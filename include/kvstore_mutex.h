/*
** Mutex implementation for KVStore (SQLite v3.3.0 compatible)
** 
** This provides thread-safety for the key-value store implementation.
** Since SQLite v3.3.0 doesn't have built-in mutex support, we implement
** our own based on the platform.
*/

#ifndef KVSTORE_MUTEX_H
#define KVSTORE_MUTEX_H

/*
** Platform-specific includes
*/
#if defined(_WIN32) || defined(WIN32)
# include <windows.h>
#else
# include <pthread.h>
#endif

/*
** Mutex handle type
*/
typedef struct kvstore_mutex kvstore_mutex;

struct kvstore_mutex {
#if defined(_WIN32) || defined(WIN32)
  CRITICAL_SECTION cs;
#else
  pthread_mutex_t mutex;
#endif
  int isValid;
};

/*
** Mutex API
*/
kvstore_mutex* kvstore_mutex_alloc(void);
void kvstore_mutex_free(kvstore_mutex *p);
void kvstore_mutex_enter(kvstore_mutex *p);
void kvstore_mutex_leave(kvstore_mutex *p);
int kvstore_mutex_try(kvstore_mutex *p);

#endif /* KVSTORE_MUTEX_H */
