/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright 2025 SNKV Contributors */
/*
** examples/concurrent_rw.c
**
** Demonstrates multi-reader / single-writer concurrency with fullMutex=0.
**
** HOW CONCURRENCY WORKS IN SNKV / WAL MODE
** ------------------------------------------
** Each thread opens its OWN KVStore handle.  No handle is shared between
** threads.  fullMutex=0 (the default) is correct and safe in this pattern:
** the application-level recursive mutex (pKV->pMutex) is skipped entirely,
** eliminating ~20 ns of overhead per API call.
**
** Thread safety comes from WAL byte-range locks in the -shm file:
**   WRITE_LOCK  (byte 120) — at most one writer holds this at a time.
**   READ_LOCK   (bytes 0-4) — up to 5 concurrent readers, each in its own
**                              slot, locking a different byte range.
** Writers and readers lock DIFFERENT byte ranges so they never block each
** other.  A second writer trying to grab WRITE_LOCK while one is held gets
** SQLITE_BUSY, surfaced as KVSTORE_BUSY.
**
** WHEN fullMutex=1 IS NEEDED
** --------------------------
** Only when a SINGLE KVStore* is shared across threads.  In that case the
** in-memory struct (inTrans, stats, …) would race without the mutex.
** The recommended pattern — one handle per thread — never needs fullMutex.
**
** PART 1: 1 writer + N readers run concurrently.
**   The writer commits N keys.  Each reader verifies every key it can find
**   has the correct value.  Missing keys (not yet committed) are acceptable;
**   wrong values are a failure.
**
** PART 2: Second writer rejected with KVSTORE_BUSY.
**   Writer 1 holds an open write transaction.  Writer 2 attempts to begin
**   its own write transaction with busyTimeout=0 (fail immediately).
**   WAL refuses the second writer and returns KVSTORE_BUSY — no application
**   mutex needed to enforce the single-writer rule.
*/

#define SNKV_IMPLEMENTATION
#include "snkv.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Part 1 uses POSIX threads.
** Linux / macOS        — pthreads native.
** MSYS2 / MinGW        — winpthreads ships with the toolchain; linked via
**                        -lpthread added to LDFLAGS in the Makefile.
** Native Windows (MSVC) — no pthreads; Part 1 is skipped at runtime. */
#if !defined(_WIN32) && !defined(_WIN64)
   /* Linux / macOS */
#  include <pthread.h>
#  define HAVE_PTHREADS 1
#elif defined(__MINGW32__) || defined(__MINGW64__)
   /* MSYS2 / MinGW — winpthreads */
#  include <pthread.h>
#  define HAVE_PTHREADS 1
#endif

#define DB_FILE     "concurrent_rw.db"
#define NUM_KEYS    200
#define NUM_READERS 5

/* ---- helpers ------------------------------------------------------------ */

static void db_cleanup(void) {
    remove(DB_FILE);
    remove(DB_FILE "-wal");
    remove(DB_FILE "-shm");
}

static KVStore *open_store(int busy_timeout_ms) {
    KVStoreConfig cfg = {0};
    cfg.journalMode = KVSTORE_JOURNAL_WAL;
    cfg.busyTimeout = busy_timeout_ms;
    /* fullMutex stays 0 — each thread has its own handle */
    KVStore *kv = NULL;
    int rc = kvstore_open_v2(DB_FILE, &kv, &cfg);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "open_store failed: rc=%d\n", rc);
        return NULL;
    }
    return kv;
}

/* =========================================================================
** PART 1 — multi-reader / single-writer  (POSIX only)
** ========================================================================= */

#ifdef HAVE_PTHREADS

static void *reader_thread(void *arg) {
    int id = *(int *)arg;
    KVStore *kv = open_store(5000);
    if (!kv) return (void *)(intptr_t)(-1);

    int wrong = 0;
    for (int i = 0; i < NUM_KEYS; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key%04d", i);
        void *val = NULL; int vlen = 0;
        int rc = kvstore_get(kv, key, (int)strlen(key), &val, &vlen);
        if (rc == KVSTORE_OK) {
            char expected[32];
            snprintf(expected, sizeof(expected), "val%04d", i);
            if (vlen != (int)strlen(expected) ||
                memcmp(val, expected, (size_t)vlen) != 0) {
                wrong++;
                fprintf(stderr, "  reader %d: wrong value for %s\n", id, key);
            }
            snkv_free(val);
        }
        /* KVSTORE_NOTFOUND is fine — writer may not have committed yet */
    }

    kvstore_close(kv);
    printf("  reader %d done — %d wrong values\n", id, wrong);
    return (void *)(intptr_t)wrong;
}

static void *writer_thread(void *arg) {
    (void)arg;
    KVStore *kv = open_store(5000);
    if (!kv) return (void *)(intptr_t)(-1);

    int rc = kvstore_begin(kv, 1);
    if (rc == KVSTORE_OK) {
        for (int i = 0; i < NUM_KEYS; i++) {
            char key[32], val[32];
            snprintf(key, sizeof(key), "key%04d", i);
            snprintf(val, sizeof(val), "val%04d", i);
            kvstore_put(kv, key, (int)strlen(key), val, (int)strlen(val));
        }
        rc = kvstore_commit(kv);
    }
    kvstore_close(kv);
    return (void *)(intptr_t)(rc != KVSTORE_OK ? -1 : 0);
}

static void part1_concurrent(void) {
    printf("\n=== Part 1: 1 writer + %d readers (fullMutex=0) ===\n",
           NUM_READERS);

    pthread_t wt, rt[NUM_READERS];
    int reader_ids[NUM_READERS];

    pthread_create(&wt, NULL, writer_thread, NULL);
    for (int i = 0; i < NUM_READERS; i++) {
        reader_ids[i] = i;
        pthread_create(&rt[i], NULL, reader_thread, &reader_ids[i]);
    }

    void *wret = NULL;
    pthread_join(wt, &wret);
    int writer_ok = ((int)(intptr_t)wret == 0);

    int total_wrong = 0;
    for (int i = 0; i < NUM_READERS; i++) {
        void *ret = NULL;
        pthread_join(rt[i], &ret);
        int w = (int)(intptr_t)ret;
        if (w > 0) total_wrong += w;
    }

    printf("  Writer: %s\n", writer_ok ? "OK" : "FAILED");
    printf("  Total wrong values across all readers: %d\n", total_wrong);
    printf("  %s\n\n",
           (writer_ok && total_wrong == 0) ? "PASS" : "FAIL");
}

#endif /* HAVE_PTHREADS */

/* =========================================================================
** PART 2 — second writer rejected (KVSTORE_BUSY)
**
** Writer 1 holds an open write transaction (begun before the second writer
** tries).  Writer 2 uses busyTimeout=0 so it fails immediately instead of
** retrying.  WAL's WRITE_LOCK enforces single-writer — no app mutex needed.
** ========================================================================= */

static void part2_second_writer_rejected(void) {
    printf("=== Part 2: second writer rejected with KVSTORE_BUSY ===\n");

    /* Writer 1: acquire and hold the write lock */
    KVStore *kv1 = open_store(0);
    if (!kv1) { printf("  FAIL: could not open writer 1\n"); return; }

    int rc = kvstore_begin(kv1, 1);
    if (rc != KVSTORE_OK) {
        printf("  FAIL: writer 1 could not begin (rc=%d)\n", rc);
        kvstore_close(kv1);
        return;
    }
    kvstore_put(kv1, "sentinel", 8, "1", 1);
    printf("  Writer 1 holds WRITE_LOCK\n");

    /* Writer 2: attempt to grab the write lock with no retry (busyTimeout=0).
    ** WAL returns SQLITE_BUSY immediately because WRITE_LOCK is held. */
    KVStore *kv2 = open_store(0);
    int rc2 = kv2 ? kvstore_begin(kv2, 1) : KVSTORE_ERROR;
    if (kv2) kvstore_close(kv2);

    int is_busy = ((rc2 & 0xFF) == KVSTORE_BUSY ||
                   (rc2 & 0xFF) == KVSTORE_LOCKED);
    printf("  Writer 2 rc=%d — %s\n", rc2,
           is_busy ? "PASS: correctly rejected (KVSTORE_BUSY)"
                   : "FAIL: expected KVSTORE_BUSY");

    /* Release writer 1 */
    kvstore_rollback(kv1);
    kvstore_close(kv1);

    /* Confirm writer 2 can now succeed */
    KVStore *kv3 = open_store(0);
    int rc3 = kv3 ? kvstore_begin(kv3, 1) : KVSTORE_ERROR;
    if (rc3 == KVSTORE_OK) kvstore_rollback(kv3);
    if (kv3) kvstore_close(kv3);
    printf("  Writer 2 retry after release: %s\n\n",
           rc3 == KVSTORE_OK ? "PASS: succeeded" : "FAIL");
}

/* =========================================================================
** main
** ========================================================================= */

int main(void) {
    db_cleanup();

    /* Create the WAL database once before spawning threads so they don't
    ** race on SHM initialisation. */
    KVStore *kv = NULL;
    int rc = kvstore_open(DB_FILE, &kv, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Failed to create DB: rc=%d\n", rc);
        return 1;
    }
    kvstore_close(kv);

#ifdef HAVE_PTHREADS
    part1_concurrent();
#else
    printf("(Part 1 skipped — pthreads not available on this platform)\n\n");
#endif
    part2_second_writer_rejected();

    db_cleanup();
    printf("Done.\n");
    return 0;
}
