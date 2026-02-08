/* SPDX-License-Identifier: Apache-2.0 */
/*
** Mutex implementation for KVStore
*/

#include "kvstore_mutex.h"
#include <stdlib.h>
#include <string.h>

/*
** Allocate and initialize a new mutex
*/
kvstore_mutex* kvstore_mutex_alloc(void){
  kvstore_mutex *p;
  
  p = (kvstore_mutex*)malloc(sizeof(kvstore_mutex));
  if( p == NULL ){
    return NULL;
  }
  
  memset(p, 0, sizeof(kvstore_mutex));
  
#if defined(_WIN32) || defined(WIN32)
  InitializeCriticalSection(&p->cs);
  p->isValid = 1;
#else
  if( pthread_mutex_init(&p->mutex, NULL) == 0 ){
    p->isValid = 1;
  }else{
    free(p);
    return NULL;
  }
#endif
  
  return p;
}

/*
** Free a mutex
*/
void kvstore_mutex_free(kvstore_mutex *p){
  if( p == NULL || !p->isValid ){
    return;
  }
  
#if defined(_WIN32) || defined(WIN32)
  DeleteCriticalSection(&p->cs);
#else
  pthread_mutex_destroy(&p->mutex);
#endif
  
  p->isValid = 0;
  free(p);
}

/*
** Enter a mutex (blocking)
*/
void kvstore_mutex_enter(kvstore_mutex *p){
  if( p == NULL || !p->isValid ){
    return;
  }
  
#if defined(_WIN32) || defined(WIN32)
  EnterCriticalSection(&p->cs);
#else
  pthread_mutex_lock(&p->mutex);
#endif
}

/*
** Leave a mutex
*/
void kvstore_mutex_leave(kvstore_mutex *p){
  if( p == NULL || !p->isValid ){
    return;
  }
  
#if defined(_WIN32) || defined(WIN32)
  LeaveCriticalSection(&p->cs);
#else
  pthread_mutex_unlock(&p->mutex);
#endif
}

