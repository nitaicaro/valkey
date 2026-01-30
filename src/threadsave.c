/* Threadsave can perform a consistent snapshot
 *
 * Threadsave uses the bgIteration functionality.  Keys are provided sequentially
 * to threadsave.  Threadsave is responsible for serializing the data and writing to file.
 *
 * Threadsave runs primarily in it's own thread, avoiding most impact to the Valkey main thread.
 */

#include "threadsave.h"
#include "server.h"
#include "functions.h"
#include "bgiteration.h"
#include "monotonic.h"
#include "mutexqueue.h"
#include <assert.h>
#include "rio.h"
#include "rdb.h"
#include "bio.h"
#include "module.h"

int rdbSaveInfoAuxFields(rio *rdb, int flags, rdbSaveInfo *rsi);
ssize_t rdbSaveAuxField(rio *rdb, void *key, size_t keylen, void *val, size_t vallen);
ssize_t rdbSaveAuxFieldStrInt(rio *rdb, char *key, long long val);
ssize_t rdbSaveAuxFieldStrStr(rio *rdb, char *key, char *val);

static const void *PROCESS_COMPLETE_ITEM = (void*)-1;

static const int SNAPSHOT_FILE_CLOSE_MONITOR_INTERVAL_MS = 200;

typedef struct {
    rio save_rio;           // Must be 1st to permit cast from rio back to threasaveInfo
    int cur_db;             // Last selectDb issued
    size_t (*realRioWrite)(struct _rio *, const void *buf, size_t len); // See rioWriteWrapper
    bgIterator *iterator;
    monotime start_time;
    uint64_t bytes_written;
    int err_code;
    mutexQueue *foreground_queue;
    bool terminated;
    union {
        struct {
            sds temp_file;
            sds final_file;
        } file;
        struct {
            list *clients;
            char eofmark[RDB_EOF_MARK_SIZE];
        } repl;
    } u;
} threadsaveInfo;


/* Threadsave is designed so that multiple saves can occur in parallel.  For compatibility with
 * open-source Valkey though, there are a few issues:
 *    1) save to disk always writes to the same file and filename is maintained as a global
 *    2) save for replication hits all clients which are marked REPLICA_STATE_WAIT_BGSAVE_END
 *    3) cancellation of a save assumes a single save is running
 * So here we keep a global indicator of the current iterator (for cancellation purposes).
 */
static threadsaveInfo *currentThreadsave = NULL;

/* When saving a large object, there is no mechanism to break out and perform periodic status
 *  checks.  To get around this, the rioWrite routine is replaced with this function.  The original
 *  write routine is saved in the threadsaveInfo.
 */
static size_t rioWriteWrapper(rio *r, const void *buf, size_t len) {
    static_assert(offsetof(threadsaveInfo, save_rio) == 0, "rio must be castable to threadsaveInfo");
    threadsaveInfo *saveInfo = (threadsaveInfo*)r;

    if (saveInfo->iterator && bgIteratorIsTerminating(saveInfo->iterator)) return 0;  // indicate error

    return saveInfo->realRioWrite(r, buf, len);
}


static void installRioWriteWrapper(threadsaveInfo *saveInfo) {
    serverAssert(saveInfo->realRioWrite == NULL);
    saveInfo->realRioWrite = saveInfo->save_rio.write;
    saveInfo->save_rio.write = rioWriteWrapper;
}

static int writeRdbStartMarker(threadsaveInfo *saveInfo) {
    serverAssert(onRedisMainThread());
    serverLog(LL_DEBUG, "threadsave: writing start marker");

    rdbSetChecksumAlgorithmForSave(&saveInfo->save_rio);

    char magic[REDIS_RDB_MAGIC_SIZE + 1];
    int charsWritten;

    charsWritten = snprintf(magic, sizeof(magic), "VALKEY%03d", RDB_VERSION);
    serverAssert(charsWritten == REDIS_RDB_MAGIC_SIZE);
    if (rioWrite(&saveInfo->save_rio, magic, REDIS_RDB_MAGIC_SIZE) == 0) {
        serverLog(LL_WARNING, "threadsave: error while writing valkey magic string");
        return C_ERR;
    }

    if (rdbSaveAuxFieldStrInt(&saveInfo->save_rio, "valkey-threadsave-version", VALKEY_THREADSAVE_RDB_VERSION) == -1) {
        serverLog(LL_WARNING, "threadsave: error while writing threadsave version aux field");
        return C_ERR;
    }

    // The regular rdbSaveInfoAuxFields goes at the top of the RDB.
    //   The resulting RDB is point-in-time and does not contain replication
    //   data, so we attempt to save its replication info as of the beginning
    //   replication offset of this save session.
    rdbSaveInfo rsi, *rsiptr = NULL;
    rsiptr = rdbPopulateSaveInfo(&rsi);
    if (rdbSaveInfoAuxFields(&saveInfo->save_rio, RDBFLAGS_NONE, rsiptr) == -1) {
        serverLog(LL_WARNING, "threadsave: error while writing AUX fields");
        return C_ERR;
    }

    if (rdbSaveModulesAux(&saveInfo->save_rio, VALKEYMODULE_AUX_BEFORE_RDB) == -1) {
        serverLog(LL_WARNING, "threadsave: error while writing module AUX fields at the start");
        return C_ERR;
    }

    return C_OK;
}


static int writeRdbEndMarker(threadsaveInfo *saveInfo) {
    serverAssert(onRedisMainThread());
    serverLog(LL_DEBUG, "threadsave: writing end marker");

    if (rdbSaveModulesAux(&saveInfo->save_rio, VALKEYMODULE_AUX_AFTER_RDB) == -1) {
        serverLog(LL_WARNING, "threadsave: error while writing module AUX fields at the end");
        return C_ERR;
    }

    if (rdbSaveType(&saveInfo->save_rio, RDB_OPCODE_EOF) == -1) {
        serverLog(LL_WARNING, "threadsave: error while writing OPCODE_EOF");
        return C_ERR;
    }

    uint64_t cksum = saveInfo->save_rio.cksum;
    memrev64ifbe(&cksum);
    if (rioWrite(&saveInfo->save_rio, &cksum, 8) == 0) {
        serverLog(LL_WARNING, "threadsave: error while writing checksum");
        return C_ERR;
    }

    return C_OK;
}


static int writeSelectDb(threadsaveInfo *saveInfo, int new_db) {
    if (new_db != saveInfo->cur_db) {
        serverLog(LL_DEBUG, "threadsave: selecting db %d", new_db);

        if (rdbSaveType(&saveInfo->save_rio, RDB_OPCODE_SELECTDB) == -1) {
            serverLog(LL_WARNING, "threadsave: error while writing OPCODE_SELECTDB");
            return C_ERR;
        }
        if (rdbSaveLen(&saveInfo->save_rio, new_db) == -1) {
            serverLog(LL_WARNING, "threadsave: error while writing selectDb value");
            return C_ERR;
        }
        saveInfo->cur_db = new_db;
    }
    return C_OK;
}


static int writeDbSizeHint(threadsaveInfo *saveInfo) {
    if (server.cluster_enabled) {
        serverDb *db = server.db[0];
        int slot = kvstoreGetFirstNonEmptyHashtableIndex(db->keys);
        while (slot != -1) {
            sds slot_info = sdscatprintf(sdsempty(), "%i,%lu,%lu", slot, kvstoreHashtableSize(db->keys, slot),
                    kvstoreHashtableSize(db->expires, slot));
            if (rdbSaveAuxFieldStrStr(&saveInfo->save_rio, "slot-info", slot_info) < 0) {
                sdsfree(slot_info);
                serverLog(LL_WARNING, "threadsave: error error while writing slot-info");
                return C_ERR;
            }
            sdsfree(slot_info);
            slot = kvstoreGetNextNonEmptyHashtableIndex(db->keys, slot);
        }
    } else {
        for (int dbid = 0;  dbid < server.dbnum;  dbid++) {
            serverDb *db = server.db[dbid];
            if (db == NULL) continue;
            unsigned long dictSize = dbSize(db);
            if (dictSize > 0) {
                serverLog(LL_DEBUG, "threadsave: writing db size hint for DB %d", dbid);

                unsigned long expireSize = kvstoreSize(db->expires);

                if (writeSelectDb(saveInfo, dbid) != C_OK) return C_ERR;

                uint32_t dictHint = (dictSize <= UINT32_MAX) ? (uint32_t)dictSize : UINT32_MAX;
                uint32_t expireHint = (expireSize <= UINT32_MAX) ? (uint32_t)expireSize : UINT32_MAX;

                if (rdbSaveType(&saveInfo->save_rio, RDB_OPCODE_RESIZEDB) == -1) {
                    serverLog(LL_WARNING, "threadsave: error while writing OPCODE_RESIZEDB");
                    return C_ERR;
                }
                if (rdbSaveLen(&saveInfo->save_rio, dictHint) == -1) {
                    serverLog(LL_WARNING, "threadsave: error while writing dictHint");
                    return C_ERR;
                }
                if (rdbSaveLen(&saveInfo->save_rio, expireHint) == -1) {
                    serverLog(LL_WARNING, "threadsave: error while writing expireHint");
                    return C_ERR;
                }
            }
        }
    }
    return C_OK;
}

// ENTRY POINT for BACKGROUND THREAD
static void * threadsaveProcessor(void *arg) {
    serverAssert(!onRedisMainThread());
    threadsaveInfo *saveInfo = arg;

    /* Upon entering:
     *   * the start marker has been written
     *   * the script cache has been written
     * This function is responsible for:
     *   * writing all of the dictionary entries
     *   * writing the closing marker
     *   * flushing/closing of output
     *
     * Additionally, this function is responsible for flush/close/rename of the output file
     */
    serverLog(LL_NOTICE, "threadsave: background processor started");
    int err = C_OK;

    installRioWriteWrapper(saveInfo);

    const unsigned progressIntervalSecs = 120;
    monotime lastStatusTime;
    elapsedStart(&lastStatusTime);

    bool done = false;
    bool terminated = false;
    long items = 0;
    while (!done && err == C_OK) {
        bgIteratorItem *item = bgIteratorRead(saveInfo->iterator);

        switch (item->type) {
            case BGITERATOR_ITEM_COMPLETE:
                done = true;
                break;

            case BGITERATOR_ITEM_TERMINATED:
                terminated = true;
                done = true;
                break;

            case BGITERATOR_ITEM_DBENTRY:
                if ((err = writeSelectDb(saveInfo, item->dbid)) == C_ERR) break;
                items++;

                robj key;
                initStaticStringObject(key, objectGetKey(item->u.dbe.de));
                robj *o = item->u.dbe.de;

                long long expire = objectGetExpire(item->u.dbe.de);
                if (rdbSaveKeyValuePair(&saveInfo->save_rio, &key, o, expire, item->dbid, RDB_VERSION) == -1) {
                    serverLog(LL_WARNING, "threadsave: error writing KV pair");
                    err = C_ERR;
                }
                break;
        }

        if (elapsedSec(lastStatusTime) >= progressIntervalSecs) {
            elapsedStart(&lastStatusTime);
            serverLog(LL_NOTICE, "threadsave: progress - keys processed: %ld", items);
        }
    }

    if (err != C_OK && bgIteratorIsTerminating(saveInfo->iterator)) {
        // We recognized termination before receiving the TERMINATED event
        terminated = true;
    }

    char *message = "";
    if (terminated) message = "TERMINATED";
    else if (err != C_OK) message = "***ERROR***";
    serverLog(LL_NOTICE, "threadsave: background processor finished.  %ld items processed.  %s",
            items, message);

    currentThreadsave = NULL;
    saveInfo->err_code = err;
    bgIteratorClose(saveInfo->iterator);
    return NULL;
}


static void emitStartThreadSaveMetrics(threadsaveInfo *saveInfo) {
    serverAssert(onRedisMainThread());

    saveInfo->start_time = getMonotonicUs();

    serverLog(LL_NOTICE, "Using Threadsave for next backup");

    server.cur_bgsave_time_start = time(NULL);

    server.save_iterator_epoch = server.iterator_epoch;
    server.rdb_child_type = RDB_CHILD_TYPE_DISK;
    server.dirty_before_bgsave = server.dirty;
    server.rdb_save_time_start = time(NULL);
    server.lastbgsave_try = server.rdb_save_time_start;

    updateDictResizePolicy();

    // moduleFireServerEvent(VALKEYMODULE_EVENT_AMAZON, VALKEYMODULE_SUBEVENT_AMAZON_THREADSAVE_START, NULL);
}


static void emitEndThreadSaveMetrics(threadsaveInfo *saveInfo, bool terminated) {
    serverAssert(onRedisMainThread());

    if (server.cur_bgsave_failure_reason == SAVE_FAILURE_NONE && saveInfo->err_code != C_OK) {
        server.cur_bgsave_failure_reason = SAVE_FAILURE_DISK;
    }


    time_t save_end = time(NULL);
    /* Record Redis disk metrics */
    server.lastbgsave_status = saveInfo->err_code;
    server.rdb_save_time_last = save_end - server.rdb_save_time_start;

    if (saveInfo->err_code == C_OK) {
        server.dirty = server.dirty - server.dirty_before_bgsave;
        server.lastsave = save_end;
    }

    updateDictResizePolicy();

    if (saveInfo->err_code == C_OK) {
        /* Generate metrics on success */
        server.last_save_iterator_epoch = server.save_iterator_epoch;
        server.last_save_time_usec = server.cur_save_time_usec;
        server.last_save_serial_time_usec  = server.cur_save_serial_time_usec;
    }
    server.last_bgsave_size_bytes = saveInfo->bytes_written;

    server.rdb_child_type = RDB_CHILD_TYPE_NONE;
    server.rdb_save_time_start = -1;

    server.save_iterator_epoch = 0;
    server.cur_save_time_usec = -1;
    server.cur_save_serial_time_usec = -1;


    double duration = elapsedMs(saveInfo->start_time) / 1000.0;

    if (saveInfo->err_code == C_OK) {
        serverLog(LL_NOTICE, "threadsave: threadsave complete.  %f seconds.", duration);
    } else if (terminated) {
        serverLog(LL_WARNING, "threadsave: threadsave terminated.  %f seconds.", duration);
    } else if (errno) {
        serverLog(LL_WARNING, "threadsave: threadsave failed: %s.  %f seconds.",
                strerror(errno), duration);
    } else {
        serverLog(LL_WARNING, "threadsave: threadsave failed.  %f seconds.", duration);
    }

    // moduleFireServerEvent(VALKEYMODULE_EVENT_AMAZON,
    //                       (saveInfo->err_code == C_OK)
    //                         ? VALKEYMODULE_SUBEVENT_AMAZON_THREADSAVE_SUCCESS
    //                         : VALKEYMODULE_SUBEVENT_AMAZON_THREADSAVE_FAILED,
    //                        NULL);

    currentThreadsave = NULL;
}

static void cleanupSaveInfoAndEmitEndMetrics(threadsaveInfo *saveInfo) {
    if (saveInfo->terminated && saveInfo->err_code == C_OK) saveInfo->err_code = C_ERR;
    emitEndThreadSaveMetrics(saveInfo, saveInfo->terminated);

    serverAssert(saveInfo->u.file.temp_file == NULL);
    serverAssert(saveInfo->u.repl.clients == NULL);
    zfree(saveInfo);
}

// Routine for background thread to close and rename the threadsave snapshot file.
// Closing the file requires synchronously flushing the content to disk, which can
// take some time. 
// In addition, we also write the MD5 file if required.
static void threadsaveCloseSnapshotFile(void *args[]) {
    serverAssert(!onRedisMainThread());
    threadsaveInfo *saveInfo = (threadsaveInfo*)args[0];
    // Error or not, close the file...
    if (fclose(saveInfo->save_rio.io.file.fp) != 0) {
        serverLog(LL_WARNING, "threadsave: error closing temp file [%s]: %s",
               saveInfo->u.file.temp_file, strerror(errno));
        saveInfo->err_code = C_ERR;
    }

    if (!saveInfo->terminated && saveInfo->err_code == C_OK) {
        if (!rdbTryWriteMd5File(&saveInfo->save_rio, saveInfo->u.file.final_file)) {
            saveInfo->err_code = C_ERR;
        }
        if (saveInfo->err_code == C_OK && rename(saveInfo->u.file.temp_file, saveInfo->u.file.final_file) != 0) {
            serverLog(LL_WARNING, "threadsave: error moving temp file [%s] to destination [%s]: %s",
                    saveInfo->u.file.temp_file, saveInfo->u.file.final_file, strerror(errno));
            saveInfo->err_code = C_ERR;
        }
    }

    if (saveInfo->terminated || saveInfo->err_code != C_OK) {
        rdbRemoveTempFilesForRDB(saveInfo->u.file.final_file, getpid(), 0);
    }
    sdsfree(saveInfo->u.file.temp_file);
    sdsfree(saveInfo->u.file.final_file);
    saveInfo->u.file.temp_file = NULL;
    saveInfo->u.file.final_file = NULL;
    // Notify the main thread that I am done closing the file.
    mutexQueueAdd(saveInfo->foreground_queue, (void *)PROCESS_COMPLETE_ITEM);
}

// Timer proc which runs in the main redis event loop. It monitors to see when the background thread
// completes the action to close and rename the snapshot file at the end of disk based threadsave, 
// and performs the final clean-up actions.
static long long snapshotEndMonitorTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    serverAssert(onRedisMainThread());

    threadsaveInfo *saveInfo = (threadsaveInfo*)clientData;

    // I own this mutex queue from the main thread, check to see if the background
    // job is done or not. Note we only expect a single notification event here.
    if (mutexQueuePop(saveInfo->foreground_queue, false) != NULL) {
        serverLog(LL_DEBUG, "threadsave: snapshot end timer proc completing");
        mutexQueueRelease(saveInfo->foreground_queue);
        saveInfo->foreground_queue = NULL;
        cleanupSaveInfoAndEmitEndMetrics(saveInfo);
        return AE_NOMORE;
    }
    return SNAPSHOT_FILE_CLOSE_MONITOR_INTERVAL_MS;
}

void threadsaveComplete(bool terminated, void *privdata) {
    serverAssert(onRedisMainThread());
    serverLog(LL_NOTICE, "threadsave: completion proc - %s", (terminated) ? "terminated" : "ok");

    if (terminated && server.cur_bgsave_failure_reason == SAVE_FAILURE_NONE) {
        server.cur_bgsave_failure_reason = SAVE_FAILURE_CANCELED;
    }

    threadsaveInfo *saveInfo = privdata;
    saveInfo->terminated = terminated;
    // The save iterator should be terminated and freed at this point in time.
    saveInfo->iterator = NULL;
    // For file based threadsave, we need to generate the RDB end marker. and complete the save
    if (!saveInfo->terminated && saveInfo->err_code == C_OK) {
        saveInfo->err_code = writeRdbEndMarker(saveInfo);
    }

    // Done writing, capture bytes written (regardless of pass/fail)
    saveInfo->bytes_written = saveInfo->save_rio.processed_bytes;

    // Start a cron job to check for the background job completion
    int snapshotEndProcId = aeCreateTimeEvent(server.el, SNAPSHOT_FILE_CLOSE_MONITOR_INTERVAL_MS,
                    snapshotEndMonitorTimeProc, saveInfo, NULL);
    if (snapshotEndProcId != AE_ERR) {
        // Submit a background job to close and rename the snapshot file
        saveInfo->foreground_queue = mutexQueueCreate(); // The monitor proc will delete this
        bioCreateLazyFreeJob(threadsaveCloseSnapshotFile, 1, saveInfo);
        serverLog(LL_NOTICE, "threadsave: created background thread to perform snapshot file close and rename");
        // We will now wait for the background closeSnapshotFile job to complete.
        // The remainder of the cleanup will be performed in the snapshotEndMonitorTimeProc.
        return;
    }

    serverLog(LL_WARNING, "threadsave: error creating snapshotEndMonitorTimeProc");
    saveInfo->err_code = C_ERR;

    cleanupSaveInfoAndEmitEndMetrics(saveInfo);
}


static int threadsaveCommonStart(threadsaveInfo *saveInfo) {
    serverAssert(onRedisMainThread());

    // Note current position in the replication stream.
    saveInfo->cur_db = -1;

    emitStartThreadSaveMetrics(saveInfo);

    if (writeRdbStartMarker(saveInfo) == C_ERR) return C_ERR;

    if (rdbSaveFunctions(&(saveInfo->save_rio)) == -1) return C_ERR;

    if (writeDbSizeHint(saveInfo) == C_ERR) return C_ERR;

    return C_OK;
}


static void startBackgroundThread(threadsaveInfo *saveInfo) {
    serverAssert(onRedisMainThread());

    pthread_t thread_id;
    pthread_attr_t attr;
    int pthread_rc;
    pthread_rc = pthread_attr_init(&attr);
    serverAssert(pthread_rc == 0);  // Can't fail in linux
    pthread_rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    serverAssert(pthread_rc == 0);  // Can't fail in linux
    pthread_rc = pthread_create(&thread_id, &attr, &threadsaveProcessor, saveInfo);
    serverAssert(pthread_rc == 0);  // Very unlikely to fail (big issues if we can't start a thread)
    pthread_rc = pthread_attr_destroy(&attr);
    serverAssert(pthread_rc == 0);  // Can't fail in linux
}

/* Save to the given filename for the given purpose.
 *
 * @param filename Prospect final filename of the resulting RDB.
 *        Must be under Redis current working directory.
 * @param purpose Purpose of this save session.
 *
 * Save to disk is usually performed for the purpose of snapshot.  This creates a point-in-time
 *  backup.  The process writes to a temp file and when complete, the file is renamed to the name
 *  given in filename.
 * Alternatively (not preferred) save to disk can be used for the purpose of replication.  After the
 *  save is complete, the file is transferred to the replica(s).
 */
int threadsaveToDisk(const char *filename) {
    serverAssert(onRedisMainThread());
    serverAssert(currentThreadsave == NULL);
    serverAssert(filename);
    serverLog(LL_NOTICE, "Beginning threadsaveToDisk");

    threadsaveInfo *saveInfo = zmalloc(sizeof(threadsaveInfo));
    memset(saveInfo, 0, sizeof(threadsaveInfo));
    saveInfo->terminated = false;
    saveInfo->foreground_queue = NULL;

    saveInfo->u.file.temp_file = rdbTempRdbFilename(filename, getpid());
    saveInfo->u.file.final_file = sdsnew(filename);

    rdbRemoveMd5FileForRDB(filename);

    FILE *file = fopen(saveInfo->u.file.temp_file, "wb");
    if (file == NULL) {
        serverLog(LL_WARNING, "threadsave: failed to open temp file [%s] for threadsave: %s",
                saveInfo->u.file.temp_file, strerror(errno));
        goto werr;
    }

    rioInitWithFile(&saveInfo->save_rio, file);

    int rc = threadsaveCommonStart(saveInfo);
    if (rc != C_OK) goto werr;

    // Saving to a file indicates a consistent snapshot (a backup at a point in time)
    saveInfo->iterator = bgIteratorCreateFullScanIter(THREADSAVE_FILE_ITER_NAME,
            BGITERATOR_FLAG_CONSISTENT, NULL, threadsaveComplete, saveInfo);
    if (saveInfo->iterator == NULL) {
        serverLog(LL_WARNING, "threadsave: error creating iterator");
        goto werr;
    }
    currentThreadsave = saveInfo;

    startBackgroundThread(saveInfo);

    // at this point, background iteration has started (saveInfo will be freed later)
    return C_OK;

werr:
    saveInfo->err_code = C_ERR;
    emitEndThreadSaveMetrics(saveInfo, false);

    if (file != NULL) {
        if (fclose(file) != 0) {
            serverLog(LL_WARNING, "threadsave: Could not close temp file [%s]: %s",
                    saveInfo->u.file.temp_file, strerror(errno));
        }
        if (unlink(saveInfo->u.file.temp_file) != 0) {
            serverLog(LL_WARNING, "threadsave: Could not delete temp file [%s]: %s",
                    saveInfo->u.file.temp_file, strerror(errno));
        }
    }
    sdsfree(saveInfo->u.file.temp_file);
    sdsfree(saveInfo->u.file.final_file);
    zfree(saveInfo);
    serverLog(LL_WARNING, "Error in threadsaveToDisk before starting thread");
    return C_ERR;
}
