/* SPDX-License-Identifier: Apache-2.0 */
/*
** Sharing One Handle Between Threads
** Demonstrates: one KVStore* used by several threads, kvstore_errmsg_copy,
**               and the transaction rule for shared handles
**
** concurrent_rw.c gives each thread its own handle (fastest: readers run in
** parallel).  Sharing one handle is simpler and also safe: every handle has
** one lock, so calls on it run one at a time.  Two rules apply:
**
**   1. Use kvstore_errmsg_copy, not kvstore_errmsg.  kvstore_errmsg returns
**      the handle's own error buffer, which another thread's error can
**      overwrite while you read it; kvstore_errmsg_copy copies it under the
**      lock into your buffer.
**   2. kvstore_begin(kv, 1) keeps the handle locked until kvstore_commit /
**      kvstore_rollback, which the SAME thread must call.  Other threads'
**      calls wait until then (they never join that transaction).
**
** Platform: pthreads (Linux, macOS, MinGW).  Native MSVC builds skip the
** threaded part.
*/

#define SNKV_IMPLEMENTATION
#include "snkv.h"
#include <stdio.h>
#include <string.h>

#if !defined(_WIN32) && !defined(_WIN64)
#  include <pthread.h>
#  define HAVE_PTHREADS 1
#elif defined(__MINGW32__) || defined(__MINGW64__)
#  include <pthread.h>
#  define HAVE_PTHREADS 1
#endif

#define DB_FILE   "shared_handle.db"
#define N_THREADS 4
#define N_KEYS    200

static void cleanup(void) {
    remove(DB_FILE);
    remove(DB_FILE "-wal");
    remove(DB_FILE "-shm");
}

#ifdef HAVE_PTHREADS
typedef struct { KVStore *pKV; int id; int nErrors; char lastErr[128]; } Worker;

static void *worker(void *arg) {
    Worker *w = (Worker *)arg;
    char key[32];

    /* Rule 2: this thread's transaction; other threads wait for it. */
    kvstore_begin(w->pKV, 1);
    for (int i = 0; i < N_KEYS; i++) {
        snprintf(key, sizeof key, "t%d:k%03d", w->id, i);
        kvstore_put(w->pKV, key, (int)strlen(key), "v", 1);
    }
    kvstore_commit(w->pKV);

    /* Provoke an error (empty key) and read the message safely. */
    if (kvstore_put(w->pKV, "", 0, "v", 1) != KVSTORE_OK) {
        w->nErrors++;
        kvstore_errmsg_copy(w->pKV, w->lastErr, (int)sizeof w->lastErr);   /* rule 1 */
    }
    return NULL;
}
#endif

int main(void) {
    KVStore *pKV = NULL;
    cleanup();
    kvstore_open(DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);

#ifdef HAVE_PTHREADS
    pthread_t th[N_THREADS];
    Worker w[N_THREADS];
    printf("=== %d threads sharing one KVStore* ===\n", N_THREADS);
    for (int i = 0; i < N_THREADS; i++) {
        memset(&w[i], 0, sizeof w[i]);
        w[i].pKV = pKV;
        w[i].id = i;
        pthread_create(&th[i], NULL, worker, &w[i]);
    }
    for (int i = 0; i < N_THREADS; i++) pthread_join(th[i], NULL);

    int64_t count = 0;
    KVColumnFamily *pDefault = NULL;
    kvstore_cf_get_default(pKV, &pDefault);
    kvstore_cf_count(pDefault, &count);
    kvstore_cf_close(pDefault);
    printf("keys written: %lld (expected %d)\n", (long long)count, N_THREADS * N_KEYS);
    for (int i = 0; i < N_THREADS; i++)
        printf("thread %d: %d error(s), copied message: \"%s\"\n", i, w[i].nErrors, w[i].lastErr);
#else
    printf("No pthreads on this platform: showing kvstore_errmsg_copy only.\n");
#endif

    /* kvstore_errmsg_copy always NUL-terminates, truncating to fit. */
    char small[12];
    kvstore_put(pKV, "", 0, "v", 1);
    kvstore_errmsg_copy(pKV, small, (int)sizeof small);
    printf("\ninto a %d-byte buffer (truncated, always terminated): \"%s\"\n",
           (int)sizeof small, small);

    kvstore_close(pKV);
    cleanup();
    printf("\n[OK] shared_handle.c example complete.\n");
    return 0;
}
