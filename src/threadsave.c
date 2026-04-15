#include "threadsave.h"
#include "server.h"
#include "bgiteration.h"
#include "mutexqueue.h"
#include <assert.h>
#include "rdb.h"
#include "bio.h"

static const void *PROCESS_COMPLETE_ITEM = (void *)-1;
static const int SNAPSHOT_FILE_CLOSE_MONITOR_INTERVAL_MS = 200;
static const int REPLICATION_MONITOR_INTERVAL_MS = 100;

typedef struct {
    rio save_rio;                                                       /* Must be 1st to permit cast from rio back to threadsaveInfo */
    int cur_db;                                                         /* Last selectDb issued */
    size_t (*realRioWrite)(struct _rio *, const void *buf, size_t len); /* See rioWriteWrapper */
    bgIterator *iterator;
    uint64_t bytes_written;
    int err_code;
    mutexQueue *foreground_queue;
    bool terminated;
    int write_target;
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

/* Keep a global indicator of the current iterator (for cancellation purposes). */
static threadsaveInfo *currentThreadsave = NULL;

bool isThreadsaveActive(void) {
    return currentThreadsave != NULL;
}

bool isThreadsaveToSocketActive(void) {
    return currentThreadsave != NULL && currentThreadsave->write_target == RDB_WRITE_TARGET_SOCKET;
}

/* When saving a large object, there is no mechanism to break out and perform periodic status
 * checks. To get around this, the rioWrite routine is replaced with this function. The original
 * write routine is saved in the threadsaveInfo. */
static size_t rioWriteWrapper(rio *r, const void *buf, size_t len) {
    static_assert(offsetof(threadsaveInfo, save_rio) == 0, "rio must be castable to threadsaveInfo");
    threadsaveInfo *saveInfo = (threadsaveInfo *)r;

    if (saveInfo->iterator && bgIteratorIsTerminating(saveInfo->iterator)) return 0; // indicate error

    return saveInfo->realRioWrite(r, buf, len);
}

static int connSetBlocking(connection *conn, bool blocking) {
    int rc = (blocking) ? connBlock(conn) : connNonBlock(conn);
    if (rc == ANET_ERR) {
        serverLog(LL_WARNING, "threadsave: error setting FD blocking(%d)", blocking);
        return C_ERR;
    }

    if (blocking) {
        if (connSendTimeout(conn, server.repl_timeout * 1000) == ANET_ERR) {
            serverLog(LL_WARNING, "threadsave: error setting send timeout");
            return C_ERR;
        }
        if (connRecvTimeout(conn, server.repl_timeout * 1000) == ANET_ERR) {
            serverLog(LL_WARNING, "threadsave: error setting send timeout");
            return C_ERR;
        }
    }

    return C_OK;
}


static void installRioWriteWrapper(threadsaveInfo *saveInfo) {
    serverAssert(saveInfo->realRioWrite == NULL);
    saveInfo->realRioWrite = saveInfo->save_rio.write;
    saveInfo->save_rio.write = rioWriteWrapper;
}

static int writeSelectDb(threadsaveInfo *saveInfo, int new_db) {
    if (new_db == saveInfo->cur_db) return C_OK;

    if (rdbSaveType(&saveInfo->save_rio, RDB_OPCODE_SELECTDB) == -1) {
        serverLog(LL_WARNING, "threadsave: error while writing OPCODE_SELECTDB");
        return C_ERR;
    }
    if (rdbSaveLen(&saveInfo->save_rio, new_db) == -1) {
        serverLog(LL_WARNING, "threadsave: error while writing selectDb value");
        return C_ERR;
    }
    saveInfo->cur_db = new_db;
    return C_OK;
}

static int writeDbSizeHints(threadsaveInfo *saveInfo) {
    for (int dbid = 0; dbid < server.dbnum; dbid++) {
        serverDb *db = server.db[dbid];
        if (db == NULL || dbSize(db) == 0) continue;
        if (writeSelectDb(saveInfo, dbid) != C_OK) return C_ERR;
        if (rdbSaveDbSizeHints(&saveInfo->save_rio, db, 0) < 0) return C_ERR;
    }
    return C_OK;
}


/* Forward declarations for helper functions */
static void abandonClient(threadsaveInfo *saveInfo, client *c);
static int pruneDisconnectedReplicas(threadsaveInfo *saveInfo);
static void waitForBuffersToDrain(threadsaveInfo *saveInfo);
static int transitionRioReplicaCobToRioConnset(threadsaveInfo *saveInfo);

/* Write an inline replication command into the RDB stream using RDB_OPCODE_UPDATE.
 * The command is serialized as RESP (multi-bulk) so the replica can replay it during RDB load. */
static int writeReplicationData(threadsaveInfo *saveInfo, bgIteratorItem *item) {
    serverAssert(item->type == BGITERATOR_ITEM_REPLICATION);

    if (rdbSaveType(&saveInfo->save_rio, RDB_OPCODE_UPDATE) == -1) return C_ERR;

    char llaux[LONG_STR_SIZE + 3];
    char llstr[LONG_STR_SIZE];
    int auxlen;

    /* Multi bulk length */
    auxlen = 0;
    llaux[auxlen++] = '*';
    auxlen += ll2string(llaux + auxlen, LONG_STR_SIZE, item->u.repl.argc);
    llaux[auxlen++] = '\r';
    llaux[auxlen++] = '\n';
    if (!rioWrite(&saveInfo->save_rio, llaux, auxlen)) return C_ERR;

    for (int i = 0; i < item->u.repl.argc; i++) {
        robj *o = item->u.repl.argv[i];
        int objlen;
        char *p;

        if (sdsEncodedObject(o)) {
            p = objectGetVal(o);
            objlen = sdslen(objectGetVal(o));
        } else {
            p = llstr;
            objlen = ll2string(p, LONG_STR_SIZE, (long)objectGetVal(o));
        }

        auxlen = 0;
        llaux[auxlen++] = '$';
        auxlen += ll2string(llaux + auxlen, LONG_STR_SIZE, objlen);
        llaux[auxlen++] = '\r';
        llaux[auxlen++] = '\n';
        if (!rioWrite(&saveInfo->save_rio, llaux, auxlen)) return C_ERR;
        if (!rioWrite(&saveInfo->save_rio, p, objlen)) return C_ERR;
        if (!rioWrite(&saveInfo->save_rio, "\r\n", 2)) return C_ERR;
    }

    return C_OK;
}

/* Entry point for background thread.
 * Upon entering:
 *  - The RDB header has been written (magic, aux fields, functions)
 *  - The DB size hints have been written
 * This function is responsible for writing all of the dictionary entries. 
 * Additionally:
 *  - For file-based: flush/close/rename of the output file
 *  - For socket-based: transition from COB to socket IO
 * */
static void *threadsaveProcessor(void *arg) {
    serverAssert(!onValkeyMainThread());
    threadsaveInfo *saveInfo = arg;

    serverLog(LL_NOTICE, "threadsave: background processor started");
    int err = C_OK;

    bool rioIsConnset = false;
    if (saveInfo->write_target == RDB_WRITE_TARGET_SOCKET) {
        err = transitionRioReplicaCobToRioConnset(saveInfo);
        rioIsConnset = (err == C_OK);
        serverLog(LL_NOTICE, "threadsave: transition to connset %s, %d clients remain",
                  rioIsConnset ? "OK" : "FAILED", (int)listLength(saveInfo->u.repl.clients));
    }

    installRioWriteWrapper(saveInfo);

    const unsigned progressIntervalSecs = 120;
    monotime lastStatusTime;
    elapsedStart(&lastStatusTime);

    bool done = false;
    bool terminated = false;
    long items = 0;
    while (!done && err == C_OK) {
        bgIteratorItem *item = bgIteratorRead(saveInfo->iterator);

        if (saveInfo->write_target == RDB_WRITE_TARGET_SOCKET)
            if (pruneDisconnectedReplicas(saveInfo) <= 0) {
                serverLog(LL_WARNING, "threadsave: all replicas disconnected, aborting");
                err = C_ERR;
                break;
            }

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

            case BGITERATOR_ITEM_REPLICATION:
                serverAssert(saveInfo->write_target == RDB_WRITE_TARGET_SOCKET);
                if ((err = writeSelectDb(saveInfo, item->dbid)) == C_ERR) break;
                err = writeReplicationData(saveInfo, item);
                break;

            case BGITERATOR_ITEM_SWAPDB:
                /* Iterator tracks swapdb internally; no special action needed. */
                break;

            case BGITERATOR_ITEM_FLUSHDB:
                /* Flush command will be replicated via the replication stream. */
                break;
            
            default:
                /* bgIteration may deliver item types that are not necessarily relevant to us.
                * New types may also be added in the future. It is the client's responsibility
                * to filter out irrelevant types, so we simply ignore them here. */
                break;
        }

        if (elapsedMs(lastStatusTime) >= progressIntervalSecs * 1000) {
            elapsedStart(&lastStatusTime);
            serverLog(LL_NOTICE, "threadsave: progress - keys processed: %ld", items);
        }
    }

    if (err != C_OK && bgIteratorIsTerminating(saveInfo->iterator)) {
        /* We recognized termination before receiving the TERMINATED event */
        terminated = true;
    }

    char *message = "";
    if (terminated)
        message = "TERMINATED";
    else if (err != C_OK)
        message = "***ERROR***";

    /* For socket saves, transition back from connset to COB so the main thread
     * can write the end marker into the COB via finishSocketBasedThreadsaveUsingCob. */
    if (saveInfo->write_target == RDB_WRITE_TARGET_SOCKET) {
        if (!terminated && err == C_OK) {
            rioFlush(&saveInfo->save_rio);
        }

        /* Capture bytes written so far (will be updated on main thread after EOF). */
        saveInfo->bytes_written = saveInfo->save_rio.processed_bytes;

        /* Return clients to non-blocking IO. */
        listNode *ln;
        listIter li;
        listRewind(saveInfo->u.repl.clients, &li);
        while ((ln = listNext(&li)) != NULL) {
            client *c = listNodeValue(ln);
            if (connSetBlocking(c->conn, false) == C_ERR) {
                serverLog(LL_WARNING, "threadsave: error returning client to non-blocking.");
                abandonClient(saveInfo, c);
            }
        }

        if (rioIsConnset) {
            if (terminated || err != C_OK) {
                rioFreeConnset(&saveInfo->save_rio);
            } else {
                /* Success: swap back to COB. End marker will be written on main thread. */
                uint64_t cksum = saveInfo->save_rio.cksum;
                size_t processed = saveInfo->save_rio.processed_bytes;
                rioFreeConnset(&saveInfo->save_rio);
                rioInitWithReplicaCOB(&saveInfo->save_rio);
                saveInfo->save_rio.cksum = cksum;
                saveInfo->save_rio.processed_bytes = processed;
                if (server.rdb_checksum) {
                    saveInfo->save_rio.update_cksum = rioGenericUpdateChecksum;
                }
            }
        } else {
            /* Error before transitioning to connset — still a COB rio. */
            rioFreeReplicaCOB(&saveInfo->save_rio);
        }
    }

    serverLog(LL_NOTICE, "threadsave: background processor finished. %ld items processed. %s",
            items, message);

    currentThreadsave = NULL;
    saveInfo->err_code = err;
    bgIteratorClose(saveInfo->iterator);
    return NULL;
}

/* After the bg thread finishes, some clients may have been marked for close
 * (e.g., by the main thread's freeClient path). Remove them from the client
 * list so we don't try to finish the RDB stream to dead connections. */
static void freeRecentlyTerminatedClients(threadsaveInfo *saveInfo) {
    serverAssert(onValkeyMainThread());

    listNode *ln;
    listIter li;
    listRewind(saveInfo->u.repl.clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        client *c = listNodeValue(ln);
        if (c->flag.close_asap || c->flag.close_after_reply) {
            listDelNode(saveInfo->u.repl.clients, ln);
            c->flag.threadsave_managed = 0;
            if (c->repl_data) c->repl_data->using_cob = 0;
            freeClient(c);
        }
    }
}

/* If a client is unresponsive or is being closed by the main thread, we might have to drop it
 * from the current replication activity.
 */
static void abandonClient(threadsaveInfo *saveInfo, client *c) {
    serverAssert(saveInfo->write_target == RDB_WRITE_TARGET_SOCKET);
    listNode *ln = listSearchKey(saveInfo->u.repl.clients, c);
    serverAssert(ln != NULL);
    listDelNode(saveInfo->u.repl.clients, ln);

    int remaining = listLength(saveInfo->u.repl.clients);
    serverLog(LL_WARNING, "threadsave: client(%llu) unresponsive.  %d clients remain.",
            (unsigned long long)c->id, remaining);

    // If the client connection is part of a connection set, remove it
    if (rioCheckType(&saveInfo->save_rio) == RIO_TYPE_CONNSET) {
        rioFreeConnectionFromConnset(&saveInfo->save_rio, c->conn);
    }

    // Before passing it back to the main thread, set the connection back to non-blocking
    if (connSetBlocking(c->conn, false) == C_ERR) {
        serverLog(LL_WARNING, "threadsave: error returning client(%llu) to non-blocking.", (unsigned long long)c->id);
    }

    c->flag.threadsave_managed = 0;
    if (c->repl_data) c->repl_data->using_cob = 0;
    mutexQueueAdd(saveInfo->foreground_queue, c);
}

/* Returns the number of remaining connected replicas. */
static int pruneDisconnectedReplicas(threadsaveInfo *saveInfo) {
    serverAssert(saveInfo->write_target == RDB_WRITE_TARGET_SOCKET);
    listNode *ln;
    listIter li;
    listRewind(saveInfo->u.repl.clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        client *c = listNodeValue(ln);
        waitForClientIO(c);
        // Check if client should be abandoned (simplified - no ElastiCache flags)
    }
    return listLength(saveInfo->u.repl.clients);
}

// Before writing directly to the connection, we need to wait for various buffers to drain.
static void waitForBuffersToDrain(threadsaveInfo *saveInfo) {
    serverAssert(!onValkeyMainThread());
    serverAssert(saveInfo->write_target == RDB_WRITE_TARGET_SOCKET);

    listNode *ln;
    listIter li;

    monotime startTimeMono;
    elapsedStart(&startTimeMono);

    const unsigned long loopDelayUs = 100000; // 100ms

    // Give clients a chance to flush COBs
    while (elapsedUs(startTimeMono) < (unsigned long long)server.repl_timeout * 1000000) {
        usleep(loopDelayUs);
        atomic_thread_fence(__ATOMIC_ACQUIRE);

        pruneDisconnectedReplicas(saveInfo);

        listRewind(saveInfo->u.repl.clients, &li);
        bool allFlushed = true;
        while ((ln = listNext(&li)) != NULL) {
            client *c = listNodeValue(ln);
            // Check for pending data in the COB
            if (clientHasPendingReplies(c)) {
                allFlushed = false;
                break;
            }
        }
        if (allFlushed) break;
    }

    // Kill off clients which still have COB data
    listRewind(saveInfo->u.repl.clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        client *c = listNodeValue(ln);
        if (clientHasPendingReplies(c)) {
            serverLog(LL_WARNING, "threadsave: socket not draining (COB), client(%llu)", (unsigned long long)c->id);
            abandonClient(saveInfo, c);
        }
    }

    pruneDisconnectedReplicas(saveInfo);
}

/* This function waits until all of the COBs have drained and transitions RIO to a CONNSET.
 * Returns:  C_OK - successful transition, saveInfo->save_rio is now a CONNSET
 *           C_ERR - failure, saveInfo->save_rio is still ReplicaCOB
 */
static int transitionRioReplicaCobToRioConnset(threadsaveInfo *saveInfo) {
    serverAssert(!onValkeyMainThread());
    serverAssert(saveInfo->write_target == RDB_WRITE_TARGET_SOCKET);

    waitForBuffersToDrain(saveInfo);

    listNode *ln;
    listIter li;

    // Set remaining clients to blocking
    listRewind(saveInfo->u.repl.clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        client *c = listNodeValue(ln);
        if (connBlock(c->conn) == C_ERR) {
            serverLog(LL_WARNING, "threadsave: unable to set blocking on client(%llu)", (unsigned long long)c->id);
            abandonClient(saveInfo, c);
        }
    }

    // Hopefully we still have some clients left
    int numConns = listLength(saveInfo->u.repl.clients);
    if (numConns == 0) return C_ERR;

    // At this point, we are done with ReplicaCOB RIO.
    uint64_t current_cksum = saveInfo->save_rio.cksum;
    size_t current_processed_bytes = saveInfo->save_rio.processed_bytes;
    rioFreeReplicaCOB(&saveInfo->save_rio);

    connection **conns = zmalloc(sizeof(connection*) * numConns);
    listRewind(saveInfo->u.repl.clients, &li);
    int pos = 0;
    while ((ln = listNext(&li)) != NULL) {
        client *c = listNodeValue(ln);
        conns[pos++] = c->conn;
    }

    rioInitWithConnset(&saveInfo->save_rio, conns, numConns);
    saveInfo->save_rio.cksum = current_cksum;
    saveInfo->save_rio.processed_bytes = current_processed_bytes;
    if (server.rdb_checksum) {
        saveInfo->save_rio.update_cksum = rioGenericUpdateChecksum;
    }

    zfree(conns);
    return C_OK;
}

static void resumeRegularReplicaActivity(client *c) {
    serverAssert(onValkeyMainThread());

    // Set the connection back to non-block to make sure
    // don't hang the main thread.
    if (connSetBlocking(c->conn, false) == C_ERR) {
        serverLog(LL_WARNING, "threadsave: error returning client(%llu) to non-blocking.", (unsigned long long)c->id);
    }

    c->flag.threadsave_managed = 0;

    // Since this is a replica client, re-register with priority
    connSetReadHandler(c->conn, readQueryFromClient);
    connSetPrivateData(c->conn, c);
}

static void resumeClientsAndFreeClientList(threadsaveInfo *saveInfo, bool successful) {
    serverAssert(onValkeyMainThread());

    listNode *ln;
    listIter li;
    listRewind(saveInfo->u.repl.clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        client *c = listNodeValue(ln);
        if (successful) {
            serverLog(LL_NOTICE, "threadsave: resuming regular activity for client(%llu)", (unsigned long long)c->id);
            resumeRegularReplicaActivity(c);
        } else {
            serverLog(LL_NOTICE, "threadsave: error or canceled, terminating client(%llu)", (unsigned long long)c->id);
            c->flag.threadsave_managed = 0;
            if (c->repl_data) c->repl_data->using_cob = 0;
            freeClient(c);
        }
    }

    listRelease(saveInfo->u.repl.clients);
    saveInfo->u.repl.clients = NULL;
}

static void cleanupSaveInfoAndEmitEndMetrics(threadsaveInfo *saveInfo) {
    if (saveInfo->terminated && saveInfo->err_code == C_OK) saveInfo->err_code = C_ERR;
    rdbRecordEndMetrics(RDB_BGSAVE_TYPE_THREAD, saveInfo->err_code, time(NULL));
    if (saveInfo->err_code == C_OK) {
        serverLog(LL_NOTICE, "threadsave: threadsave complete.  %lld seconds.", (long long)server.rdb_save_time_last);
    } else if (saveInfo->terminated) {
        serverLog(LL_WARNING, "threadsave: threadsave terminated.  %lld seconds.", (long long)server.rdb_save_time_last);
    } else {
        serverLog(LL_WARNING, "threadsave: threadsave failed.  %lld seconds.", (long long)server.rdb_save_time_last);
    }
    stopSaving(saveInfo->err_code == C_OK);
    currentThreadsave = NULL;

    /* Notify replicas waiting for BGSAVE to complete. */
    updateReplicasWaitingBgsave(saveInfo->err_code, saveInfo->write_target);

    serverAssert(saveInfo->u.file.temp_file == NULL);
    zfree(saveInfo);
}

/* Routine for background thread to close and rename the threadsave snapshot file.
 * Closing the file requires synchronously flushing the content to disk, which can
 * take some time. */
static void threadsaveCloseSnapshotFile(void *args[]) {
    serverAssert(!onValkeyMainThread());
    threadsaveInfo *saveInfo = (threadsaveInfo *)args[0];
    /* Error or not, close the file... */
    if (fsync(fileno(saveInfo->save_rio.io.file.fp)) != 0) {
        serverLog(LL_WARNING, "threadsave: error fsyncing temp file [%s]: %s",
                  saveInfo->u.file.temp_file, strerror(errno));
        saveInfo->err_code = C_ERR;
    }
    if (fclose(saveInfo->save_rio.io.file.fp) != 0) {
        serverLog(LL_WARNING, "threadsave: error closing temp file [%s]: %s",
                  saveInfo->u.file.temp_file, strerror(errno));
        saveInfo->err_code = C_ERR;
    }

    if (!saveInfo->terminated && saveInfo->err_code == C_OK) {
        if (rename(saveInfo->u.file.temp_file, saveInfo->u.file.final_file) != 0) {
            serverLog(LL_WARNING, "threadsave: error moving temp file [%s] to destination [%s]: %s",
                      saveInfo->u.file.temp_file, saveInfo->u.file.final_file, strerror(errno));
            saveInfo->err_code = C_ERR;
        }
    }

    if (saveInfo->terminated || saveInfo->err_code != C_OK) {
        rdbRemoveTempFile(getpid(), 0);
    }
    sdsfree(saveInfo->u.file.temp_file);
    sdsfree(saveInfo->u.file.final_file);
    saveInfo->u.file.temp_file = NULL;
    saveInfo->u.file.final_file = NULL;
    /* Notify the main thread that I am done closing the file. */
    mutexQueueAdd(saveInfo->foreground_queue, (void *)PROCESS_COMPLETE_ITEM);
}

/* Timer proc which runs in the main redis event loop. It monitors to see when the background thread
 * completes the action to close and rename the snapshot file at the end of disk based threadsave,
 * and performs the final clean-up actions. */
static long long snapshotEndMonitorTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    serverAssert(onValkeyMainThread());

    threadsaveInfo *saveInfo = (threadsaveInfo *)clientData;

    /* I own this mutex queue from the main thread, check to see if the background
       job is done or not. Note we only expect a single notification event here. */
    if (mutexQueuePop(saveInfo->foreground_queue, false) != NULL) {
        mutexQueueRelease(saveInfo->foreground_queue);
        saveInfo->foreground_queue = NULL;
        cleanupSaveInfoAndEmitEndMetrics(saveInfo);
        return AE_NOMORE;
    }
    return SNAPSHOT_FILE_CLOSE_MONITOR_INTERVAL_MS;
}

/* After a socket-based threadsave, set each replica's ref_repl_buf_node to the
 * tail of the shared replication buffer so that future replication data
 * (starting at server.primary_repl_offset+1) will be sent to the replica. */
static void fixReplicationOffset(threadsaveInfo *saveInfo) {
    serverAssert(onValkeyMainThread());

    serverLog(LL_NOTICE, "threadsave: queuing repl_offset: %lld (on COB)", server.primary_repl_offset);

    listNode *repl_node = listLast(server.repl_buffer_blocks);
    replBufBlock *tail = repl_node ? listNodeValue(repl_node) : NULL;
    listNode *target_node = NULL;
    size_t target_pos = 0;

    if (tail != NULL) {
        target_node = repl_node;
        target_pos = tail->used;
    }

    listNode *ln;
    listIter li;
    listRewind(saveInfo->u.repl.clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        client *c = listNodeValue(ln);

        if (target_node != NULL) {
            ((replBufBlock *)listNodeValue(target_node))->refcount++;
            serverAssert(c->repl_data);
            if (c->repl_data->ref_repl_buf_node != NULL) {
                ((replBufBlock *)listNodeValue(c->repl_data->ref_repl_buf_node))->refcount--;
            }
            c->repl_data->ref_repl_buf_node = target_node;
            c->repl_data->ref_block_pos = target_pos;
        }

        /* Tell the replica the final replication offset so it can continue
         * replicating from the correct point after loading the RDB. */
        addReplyArrayLen(c, 3);
        addReplyBulkCString(c, "REPLCONF");
        addReplyBulkCString(c, "psync-offset");
        addReplyBulkLongLong(c, server.primary_repl_offset);
    }
}

/* After the background thread finishes writing keys via the connset, the main
 * thread finishes the RDB stream using the COB:
 *   1) Update the RDB checksum with any data already in the COB
 *   2) Write the EOF, checksum, and eofmark into the COB
 *   3) Suspend writes until ACK, then resume so the COB drains
 *   4) Fix the replication offset so future repl data flows correctly */
static int finishSocketBasedThreadsaveUsingCob(threadsaveInfo *saveInfo) {
    serverAssert(onValkeyMainThread());
    serverAssert(listLength(saveInfo->u.repl.clients) > 0);

    /* Update checksum with any replication data already in the COB.
     * All threadsave clients share the same COB content, so process once. */
    client *c = listNodeValue(listFirst(saveInfo->u.repl.clients));
    serverAssert(c->io_last_written.data_len == 0);

    if (c->bufpos > 0) {
        if (server.rdb_checksum) {
            saveInfo->save_rio.update_cksum(&saveInfo->save_rio, c->buf, c->bufpos);
        }
        saveInfo->save_rio.processed_bytes += c->bufpos;
    }

    listNode *ln;
    listIter li;
    listRewind(c->reply, &li);
    while ((ln = listNext(&li)) != NULL) {
        clientReplyBlock *replyBlock = listNodeValue(ln);
        if (server.rdb_checksum) {
            saveInfo->save_rio.update_cksum(&saveInfo->save_rio, replyBlock->buf, replyBlock->used);
        }
        saveInfo->save_rio.processed_bytes += replyBlock->used;
    }

    /* Write EOF, checksum, and eofmark into the COB */
    // After loading a for-sync save, the replica needs to continue replicating from the
    //  correct point in the replication stream.
    // If using socket based replication, the replication stream is included with the
    //  snapshot data.  The replica will continue after the last item seen.  Since this end
    //  marker is being written synchronously on the main thread, we could simply save replication
    //  AUX fields based on the latest replication staus on the main thread.
    rdbSaveInfo rsi, *rsiptr;
    rsiptr = rdbPopulateSaveInfo(&rsi);
    serverAssert(rsiptr);
    if (rdbSaveInfoReplAuxFields(&saveInfo->save_rio, rsiptr) == -1) {
        serverLog(LL_WARNING, "threadsave: error while writing AUX fields for replication, err=%s",
                strerror(errno));
        return C_ERR;
    }
    int err = rdbWriteFooter(&saveInfo->save_rio, REPLICA_REQ_NONE);
    if (rioWrite(&saveInfo->save_rio, saveInfo->u.repl.eofmark, VALKEY_EOF_MARK_SIZE) == 0) {
            serverLog(LL_WARNING, "threadsave: error while writing valkey end eof string");
            return C_ERR;
        }
    rioFlush(&saveInfo->save_rio);

    saveInfo->bytes_written = saveInfo->save_rio.processed_bytes;

    if (err == C_OK) {
        // The COB is currently not sending.  At this point, we set a STOP position after the end
        //  marker and re-enable COB writes.
        listRewind(saveInfo->u.repl.clients, &li);
        while ((ln = listNext(&li)) != NULL) {
            client *c = listNodeValue(ln);

            // Don't write past the current point in the COB
            pauseCobSendAtCurrentPositionForAck(c);

            // Start sending
            resumeReplicaWrites(c);
        }

        // REPLCONF will be sent immediately after ACK is received
        fixReplicationOffset(saveInfo);
    }

    rioFlush(&saveInfo->save_rio);  // Force from RIO buffer into COB
    rioFreeReplicaCOB(&saveInfo->save_rio);
    return err;
}

void threadsaveComplete(bool terminated, void *privdata) {
    serverAssert(onValkeyMainThread());
    serverLog(LL_NOTICE, "threadsave: completion proc - %s", (terminated) ? "terminated" : "ok");

    threadsaveInfo *saveInfo = privdata;
    saveInfo->terminated = terminated;
    /* The save iterator should be terminated and freed at this point in time. */
    saveInfo->iterator = NULL;

    if (saveInfo->write_target == RDB_WRITE_TARGET_SOCKET) {
        /* Get rid of any clients which may have been closed after the bg thread completed. */
        freeRecentlyTerminatedClients(saveInfo);
        if (listLength(saveInfo->u.repl.clients) == 0) saveInfo->terminated = true;

        if (!saveInfo->terminated && saveInfo->err_code == C_OK) {
            /* Finish the RDB stream on the main thread using the COB. */
            int err = finishSocketBasedThreadsaveUsingCob(saveInfo);
            if (err != C_OK) saveInfo->terminated = true;
        } else {
            /* Error/terminated path: still need to free the COB rio. */
            rioFreeReplicaCOB(&saveInfo->save_rio);
        }

        resumeClientsAndFreeClientList(saveInfo, !saveInfo->terminated && saveInfo->err_code == C_OK);

        /* Shut down the replication monitor timer */
        mutexQueueAdd(saveInfo->foreground_queue, (void *)PROCESS_COMPLETE_ITEM);
        saveInfo->foreground_queue = NULL;

        cleanupSaveInfoAndEmitEndMetrics(saveInfo);
    } else {
        // For file based threadsave, we need to generate the RDB end marker. and complete the save
        if (!saveInfo->terminated && saveInfo->err_code == C_OK) {
            saveInfo->err_code = rdbWriteFooter(&saveInfo->save_rio, REPLICA_REQ_NONE) == C_ERR ? C_ERR : C_OK;
        }

        // Done writing, capture bytes written (regardless of pass/fail)
        saveInfo->bytes_written = saveInfo->save_rio.processed_bytes;

        // Start a cron job to check for the background job completion
        aeCreateTimeEvent(server.el, SNAPSHOT_FILE_CLOSE_MONITOR_INTERVAL_MS, snapshotEndMonitorTimeProc, saveInfo, NULL);
        // Submit a background job to close and rename the snapshot file
        saveInfo->foreground_queue = mutexQueueCreate(); // The monitor proc will delete this
        bioCreateLazyFreeJob(threadsaveCloseSnapshotFile, 1, saveInfo);
        serverLog(LL_NOTICE, "threadsave: created background thread to perform snapshot file close and rename");
        /* We will now wait for the background closeSnapshotFile job to complete.
         * The remainder of the cleanup will be performed in the snapshotEndMonitorTimeProc. */
    }
}

static int threadsaveCommonStart(threadsaveInfo *saveInfo) {
    serverAssert(onValkeyMainThread());

    saveInfo->cur_db = -1;

    serverLog(LL_NOTICE, "Using Threadsave for next backup");
    rdbRecordStartMetrics(RDB_BGSAVE_TYPE_THREAD);
    startSaving(RDBFLAGS_THREADSAVE);

    rdbSaveInfo rsi, *rsiptr = rdbPopulateSaveInfo(&rsi);
    if (rdbWriteHeader(&saveInfo->save_rio, REPLICA_REQ_NONE, RDB_VERSION, RDBFLAGS_NONE, rsiptr) == C_ERR) return C_ERR;

    if (writeDbSizeHints(saveInfo) == C_ERR) return C_ERR;

    return C_OK;
}

static void startBackgroundThread(threadsaveInfo *saveInfo) {
    serverAssert(onValkeyMainThread());

    pthread_t thread_id;
    pthread_attr_t attr;
    int pthread_rc;
    pthread_rc = pthread_attr_init(&attr);
    serverAssert(pthread_rc == 0);
    pthread_rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    serverAssert(pthread_rc == 0);
    pthread_rc = pthread_create(&thread_id, &attr, &threadsaveProcessor, saveInfo);
    serverAssert(pthread_rc == 0);
    pthread_rc = pthread_attr_destroy(&attr);
    serverAssert(pthread_rc == 0);
}

/* Save a point-in-time snapshot to the given filename.
 * The filename must be under the server's current working directory.
 * Writes to a temp file and renames to the final filename on completion. */
int threadsaveToDisk(const char *filename) {
    serverAssert(onValkeyMainThread());
    serverAssert(currentThreadsave == NULL);
    serverAssert(!isSaveInProgress());
    serverAssert(filename);
    serverLog(LL_NOTICE, "Beginning threadsaveToDisk");

    threadsaveInfo *saveInfo = zcalloc(sizeof(threadsaveInfo));
    saveInfo->write_target = RDB_WRITE_TARGET_DISK;

    char tmpfile[256];
    snprintf(tmpfile, sizeof(tmpfile), "temp-%d.rdb", (int)getpid());
    saveInfo->u.file.temp_file = sdsnew(tmpfile);
    saveInfo->u.file.final_file = sdsnew(filename);

    FILE *file = fopen(saveInfo->u.file.temp_file, "wb");
    if (file == NULL) {
        serverLog(LL_WARNING, "threadsave: failed to open temp file [%s] for threadsave: %s",
                  saveInfo->u.file.temp_file, strerror(errno));
        goto werr;
    }

    rioInitWithFile(&saveInfo->save_rio, file);
    if (server.rdb_save_incremental_fsync) {
        rioSetAutoSync(&saveInfo->save_rio, REDIS_AUTOSYNC_BYTES);
        rioSetReclaimCache(&saveInfo->save_rio, 1);
    }

    int rc = threadsaveCommonStart(saveInfo);
    if (rc != C_OK) goto werr;

    /* Saving to a file indicates a consistent snapshot (a backup at a point in time) */
    saveInfo->iterator = bgIteratorCreateFullScanIter(THREADSAVE_FILE_ITER_NAME,
                                                      BGITERATOR_FLAG_CONSISTENT, NULL, threadsaveComplete, saveInfo);
    if (saveInfo->iterator == NULL) {
        serverLog(LL_WARNING, "threadsave: error creating iterator");
        goto werr;
    }
    currentThreadsave = saveInfo;

    startBackgroundThread(saveInfo);

    /* at this point, background iteration has started (saveInfo will be freed later) */
    return C_OK;

werr:
    saveInfo->err_code = C_ERR;
    rdbRecordEndMetrics(RDB_BGSAVE_TYPE_THREAD, C_ERR, time(NULL));
    serverLog(LL_WARNING, "threadsave: threadsave failed.  %lld seconds.", (long long)server.rdb_save_time_last);
    stopSaving(0);
    currentThreadsave = NULL;

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
    return C_ERR;
}

/* Timer proc that runs on the main thread during socket-based threadsave.
 * The bg thread may need to abandon unresponsive replicas, but can't free
 * clients from a non-main thread. It queues them to foreground_queue, and
 * this timer frees them. Also handles the PROCESS_COMPLETE_ITEM sentinel
 * which signals the timer to stop. */
static long long replicationMonitorTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    serverAssert(onValkeyMainThread());

    mutexQueue *monitorQueue = clientData;

    void *item;
    while ((item = mutexQueuePop(monitorQueue, false)) != NULL) {
        if (item == PROCESS_COMPLETE_ITEM) {
            serverLog(LL_DEBUG, "threadsave: replication monitor timer proc completing");
            mutexQueueRelease(monitorQueue);
            return AE_NOMORE;
        }

        client *c = item;
        serverLog(LL_WARNING, "threadsave: client(%llu) ended replication early",
                  (unsigned long long)c->id);
        c->flag.threadsave_managed = 0;
        if (c->repl_data) c->repl_data->using_cob = 0;
        if (c->repl_data) c->repl_data->repl_state = REPL_STATE_NONE;
        freeClient(c);
    }
    return REPLICATION_MONITOR_INTERVAL_MS;
}

/* Called on the main thread when the bg iterator finishes iterating all keys.
 * This is the point where the bg thread is done writing key data, but the
 * end marker hasn't been written yet (that happens in threadsaveComplete via
 * finishSocketBasedThreadsaveUsingCob). */
static bool threadsaveReplDone(void *privdata) {
    serverAssert(onValkeyMainThread());

    threadsaveInfo *saveInfo = privdata;
    serverAssert(saveInfo->write_target == RDB_WRITE_TARGET_SOCKET);

    serverLog(LL_NOTICE, "threadsave: replication done - primary_repl_offset: %lld",
              server.primary_repl_offset);

    listNode *ln;
    listIter li;
    listRewind(saveInfo->u.repl.clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        client *c = listNodeValue(ln);
        if (c->flag.close_asap) {
            serverLog(LL_NOTICE, "threadsave: skipping client(%llu) marked for closure in repl done",
                      (unsigned long long)c->id);
            continue;
        }
        suspendReplicaWritesUntilAck(c);
    }

    return true;
}

int threadsaveToSockets(void) {
    serverAssert(onValkeyMainThread());
    serverAssert(currentThreadsave == NULL);
    serverLog(LL_NOTICE, "Beginning threadsaveToSockets");

    threadsaveInfo *saveInfo = zmalloc(sizeof(threadsaveInfo));
    memset(saveInfo, 0, sizeof(threadsaveInfo));
    saveInfo->terminated = false;
    saveInfo->write_target = RDB_WRITE_TARGET_SOCKET;

    /* Collect replicas in WAIT_BGSAVE_END state (already set up by startBgsaveForReplication) */
    saveInfo->u.repl.clients = listCreate();
    listNode *ln;
    listIter li;
    listRewind(server.replicas, &li);
    while((ln = listNext(&li)) != NULL) {
        client *c = listNodeValue(ln);
        serverAssert(c->repl_data);
        if (c->repl_data->repl_state == REPLICA_STATE_WAIT_BGSAVE_END) {
            serverLog(LL_NOTICE, "threadsave: beginning save to client ID: %llu", (unsigned long long)c->id);
            listAddNodeTail(saveInfo->u.repl.clients, c);
            c->flag.threadsave_managed = 1;
            c->repl_data->using_cob = 1;
        }
    }
    serverAssert(listLength(saveInfo->u.repl.clients) > 0);
    saveInfo->foreground_queue = NULL;

    /* Generate EOF marker for diskless sync */
    getRandomHexChars(saveInfo->u.repl.eofmark, RDB_EOF_MARK_SIZE);

    /* Initialize RIO with ReplicaCOB. This lets the main thread write sync metadata
     * to all pending replicas through a single rioWrite() call in a non-blocking
     * manner — it places the data into each replica's Client Output Buffer (COB),
     * and the event loop flushes it to the network asynchronously.
     *
     * Later, when the background thread takes over, it waits for the COBs to drain,
     * switches the connections to blocking mode, and begins writing directly to the
     * sockets — blocking is acceptable since it's no longer on the main thread. */
    rioInitWithReplicaCOB(&saveInfo->save_rio);

    int rc = threadsaveCommonStart(saveInfo);
    if (rc != C_OK) goto werr;

    /* Flush the ReplicaCOB intermediate buffer into the actual COBs so the
     * background thread only needs to wait for COBs to drain. */
    if (!rioFlush(&saveInfo->save_rio)) {
        serverLog(LL_WARNING, "threadsave: unable to flush replica COB before transition");
        goto werr;
    }

    /* Disable read handlers on replica connections. We don't want the main
     * thread processing REPLCONF ACK or other commands from replicas while
     * the bg thread is writing to their sockets. Read handlers are restored
     * in resumeRegularReplicaActivity(). */
    listRewind(saveInfo->u.repl.clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        client *c = listNodeValue(ln);
        connSetReadHandler(c->conn, NULL);
    }

    /* Create a timer to process abandoned clients from the bg thread.
     * The bg thread can't free clients directly — it queues them to
     * foreground_queue, and this timer frees them on the main thread. */
    saveInfo->foreground_queue = mutexQueueCreate();
    long long timeProcId = aeCreateTimeEvent(server.el, REPLICATION_MONITOR_INTERVAL_MS,
                       replicationMonitorTimeProc, saveInfo->foreground_queue, NULL);
    if (timeProcId == AE_ERR) {
        mutexQueueRelease(saveInfo->foreground_queue);
        saveInfo->foreground_queue = NULL;
        serverLog(LL_WARNING, "threadsave: error creating replicationMonitorTimeProc");
        goto werr;
    }

    /* Create iterator with REPLICATION flag (no consistent snapshot needed).
     * threadsaveReplDone is called on the main thread when iteration finishes. */
    saveInfo->iterator = bgIteratorCreateFullScanIter(THREADSAVE_SOCKET_ITER_NAME,
            BGITERATOR_FLAG_REPLICATION, threadsaveReplDone, threadsaveComplete, saveInfo);
    if (saveInfo->iterator == NULL) {
        serverLog(LL_WARNING, "threadsave: error creating iterator");
        goto werr;
    }
    currentThreadsave = saveInfo;

    startBackgroundThread(saveInfo);

    return C_OK;

werr:
    currentThreadsave = NULL;
    if (saveInfo->foreground_queue) {
        /* Signal the monitor timer to stop */
        mutexQueueAdd(saveInfo->foreground_queue, (void *)PROCESS_COMPLETE_ITEM);
        saveInfo->foreground_queue = NULL;
    }
    if (saveInfo->u.repl.clients) {
        resumeClientsAndFreeClientList(saveInfo, false);
    }
    saveInfo->err_code = C_ERR;
    rdbRecordEndMetrics(RDB_BGSAVE_TYPE_THREAD, C_ERR, time(NULL));
    serverLog(LL_WARNING, "threadsave: threadsave failed.  %lld seconds.", (long long)server.rdb_save_time_last);
    stopSaving(0);
    zfree(saveInfo);
    serverLog(LL_WARNING, "Error in threadsaveToSockets before starting thread");
    return C_ERR;
}

/* Cancels the currently running threadsave, if one is in progress. */
void threadsaveCancel(void) {
    serverAssert(onValkeyMainThread());
    if (currentThreadsave == NULL) return;
    bgIteratorTerminate(currentThreadsave->iterator);
}
