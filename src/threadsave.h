#ifndef __THREADSAVE_H__
#define __THREADSAVE_H__

#include "server.h"

#define THREADSAVE_FILE_ITER_NAME "threadsave_file"

#define REDIS_RDB_MAGIC_SIZE 9

#define VALKEY_THREADSAVE_RDB_VERSION 4

/* If a BGSAVE fails, we attempt to categorize its failure cause. */
typedef enum {SAVE_FAILURE_NONE=-1, SAVE_FAILURE_CANCELED, SAVE_FAILURE_DISK} saveFailureReason;

#endif
