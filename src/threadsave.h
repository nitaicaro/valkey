#ifndef __THREADSAVE_H__
#define __THREADSAVE_H__

#include "server.h"

#define THREADSAVE_FILE_ITER_NAME "threadsave_file"
#define THREADSAVE_SOCKET_ITER_NAME "threadsave_socket"
#define VALKEY_EOF_MARK_SIZE 40

int threadsaveToDisk(const char *filename);
int threadsaveToSockets(void);
void threadsaveCancel(void);
bool isThreadsaveActive(void);
bool isThreadsaveToSocketActive(void);
int threadsaveActiveClientCount(void);

#endif
