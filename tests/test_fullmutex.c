/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright 2025 SNKV Contributors */
/*
** test_fullmutex.c
**
** Tests for the fullMutex configuration flag.
**
** Covers:
**  1. fullMutex=0 via NULL config  — basic roundtrip, no mutex allocated
**  2. fullMutex=0 explicit         — same, via KVStoreConfig
**  3. fullMutex=1 explicit         — recursive mutex serialises all ops
**  4. fullMutex=1 shared handle    — N threads share one KVStore; no corruption
**  5. Second writer rejected       — KVSTORE_BUSY when write lock already held
**  6. Per-thread handles           — N threads each open their own handle (fullMutex=0)
*/

#include "kvstore.h"
#include "platform_compat.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* ---- helpers ------------------------------------------------------------ */

static int g_passed = 0;
static int g_failed = 0;

#define CHECK(cond, msg) do {                           \
    if (cond) {                                         \
        printf("  [PASS] %s\n", msg);                   \
        g_passed++;                                     \
    } else {                                            \
        printf("  [FAIL] %s\n", msg);                   \
        g_failed++;                                     \
    }                                                   \
} while(0)

static void cleanup(const char *db) {
    char buf[512];
    remove(db);
    snprintf(buf, sizeof(buf), "%s-wal", db);  remove(buf);
    snprintf(buf, sizeof(buf), "%s-shm", db);  remove(buf);
}

static KVStore *open_cfg(const char *path, int fullMutex, int busyTimeout) {
    KVStoreConfig cfg = {0};
    cfg.journalMode = KVSTORE_JOURNAL_WAL;
    cfg.fullMutex   = fullMutex;
    cfg.busyTimeout = busyTimeout;
    KVStore *kv = NULL;
    kvstore_open_v2(path, &kv, &cfg);
    return kv;
}

/* ---- Test 1: fullMutex=0 via NULL config -------------------------------- */

static void test_null_config_no_mutex(void) {
    printf("\n--- Test 1: NULL config (fullMutex defaults to 0) ---\n");
    const char *db = "fm_null.db";
    cleanup(db);

    KVStore *kv = NULL;
    int rc = kvstore_open_v2(db, &kv, NULL);
    CHECK(rc == KVSTORE_OK && kv != NULL, "open with NULL config");

    if (kv) {
        rc = kvstore_put(kv, "k", 1, "v", 1);
        CHECK(rc == KVSTORE_OK, "put OK");

        void *val; int len;
        rc = kvstore_get(kv, "k", 1, &val, &len);
        CHECK(rc == KVSTORE_OK && len == 1 && *(char *)val == 'v',
              "get returns correct value");
        if (rc == KVSTORE_OK) sqliteFree(val);

        kvstore_close(kv);
    }
    cleanup(db);
}

/* ---- Test 2: fullMutex=0 explicit --------------------------------------- */

static void test_fullmutex_zero(void) {
    printf("\n--- Test 2: fullMutex=0 explicit ---\n");
    const char *db = "fm_zero.db";
    cleanup(db);

    KVStore *kv = open_cfg(db, 0, 0);
    CHECK(kv != NULL, "open fullMutex=0");

    if (kv) {
        int rc = kvstore_put(kv, "zero", 4, "val", 3);
        CHECK(rc == KVSTORE_OK, "put OK");

        void *val; int len;
        rc = kvstore_get(kv, "zero", 4, &val, &len);
        CHECK(rc == KVSTORE_OK && len == 3, "get OK");
        if (rc == KVSTORE_OK) sqliteFree(val);

        rc = kvstore_delete(kv, "zero", 4);
        CHECK(rc == KVSTORE_OK, "delete OK");

        rc = kvstore_get(kv, "zero", 4, &val, &len);
        CHECK(rc == KVSTORE_NOTFOUND, "key gone after delete");

        kvstore_close(kv);
    }
    cleanup(db);
}

/* ---- Test 3: fullMutex=1 explicit --------------------------------------- */

static void test_fullmutex_one(void) {
    printf("\n--- Test 3: fullMutex=1 explicit ---\n");
    const char *db = "fm_one.db";
    cleanup(db);

    KVStore *kv = open_cfg(db, 1, 0);
    CHECK(kv != NULL, "open fullMutex=1");

    if (kv) {
        int rc = kvstore_put(kv, "one", 3, "val", 3);
        CHECK(rc == KVSTORE_OK, "put OK");

        void *val; int len;
        rc = kvstore_get(kv, "one", 3, &val, &len);
        CHECK(rc == KVSTORE_OK && len == 3, "get OK");
        if (rc == KVSTORE_OK) sqliteFree(val);

        rc = kvstore_delete(kv, "one", 3);
        CHECK(rc == KVSTORE_OK, "delete OK");

        rc = kvstore_get(kv, "one", 3, &val, &len);
        CHECK(rc == KVSTORE_NOTFOUND, "key gone after delete");

        kvstore_close(kv);
    }
    cleanup(db);
}

/* ---- Test 4: fullMutex=1 shared handle across threads ------------------- */

#define T4_THREADS   8
#define T4_OPS       50

typedef struct {
    KVStore *kv;
    int      id;
    int      errors;
} T4Args;

static void *t4_worker(void *arg) {
    T4Args *a = (T4Args *)arg;
    int i;
    for (i = 0; i < T4_OPS; i++) {
        char key[32], val[32];
        snprintf(key, sizeof(key), "t%d_k%d", a->id, i);
        snprintf(val, sizeof(val), "t%d_v%d", a->id, i);

        if (kvstore_put(a->kv, key, (int)strlen(key),
                        val,  (int)strlen(val)) != KVSTORE_OK) {
            a->errors++;
            continue;
        }

        void *got = NULL; int glen = 0;
        int rc = kvstore_get(a->kv, key, (int)strlen(key), &got, &glen);
        if (rc != KVSTORE_OK) {
            a->errors++;
        } else {
            if (glen != (int)strlen(val) || memcmp(got, val, glen) != 0)
                a->errors++;
            sqliteFree(got);
        }
    }
    return NULL;
}

static void test_shared_handle_fullmutex(void) {
    printf("\n--- Test 4: fullMutex=1 shared handle (%d threads) ---\n",
           T4_THREADS);
    const char *db = "fm_shared.db";
    cleanup(db);

    KVStore *kv = open_cfg(db, 1, 5000);
    if (!kv) {
        CHECK(0, "open shared handle");
        cleanup(db);
        return;
    }

    pthread_t threads[T4_THREADS];
    T4Args    args[T4_THREADS];
    int i;

    for (i = 0; i < T4_THREADS; i++) {
        args[i].kv = kv; args[i].id = i; args[i].errors = 0;
        pthread_create(&threads[i], NULL, t4_worker, &args[i]);
    }
    for (i = 0; i < T4_THREADS; i++)
        pthread_join(threads[i], NULL);

    int total_errors = 0;
    for (i = 0; i < T4_THREADS; i++)
        total_errors += args[i].errors;

    CHECK(total_errors == 0,
          "no errors across all threads sharing one handle (fullMutex=1)");

    /* Post-run integrity check */
    char *errmsg = NULL;
    int rc = kvstore_integrity_check(kv, &errmsg);
    CHECK(rc == KVSTORE_OK, "integrity_check passes after shared-handle writes");
    if (errmsg) sqliteFree(errmsg);

    kvstore_close(kv);
    cleanup(db);
}

/* ---- Test 5: Second writer rejected with KVSTORE_BUSY ------------------- */

static void test_second_writer_busy(void) {
    printf("\n--- Test 5: Second writer rejected (KVSTORE_BUSY) ---\n");
    const char *db = "fm_busy.db";
    cleanup(db);

    /* Writer 1 opens and begins a write transaction */
    KVStore *kv1 = open_cfg(db, 0, 0);
    CHECK(kv1 != NULL, "writer 1 opens");
    if (!kv1) { cleanup(db); return; }

    int rc = kvstore_begin(kv1, 1);
    CHECK(rc == KVSTORE_OK, "writer 1 begins write transaction");
    if (rc != KVSTORE_OK) {
        kvstore_close(kv1);
        cleanup(db);
        return;
    }
    kvstore_put(kv1, "sentinel", 8, "1", 1);

    /* Writer 2: busyTimeout=0 — fail immediately */
    KVStore *kv2 = open_cfg(db, 0, 0);
    int rc2 = kv2 ? kvstore_begin(kv2, 1) : KVSTORE_ERROR;
    int is_busy = ((rc2 & 0xFF) == KVSTORE_BUSY ||
                   (rc2 & 0xFF) == KVSTORE_LOCKED);
    CHECK(is_busy, "writer 2 gets KVSTORE_BUSY while writer 1 holds write lock");
    if (kv2) kvstore_close(kv2);

    /* Release writer 1 */
    kvstore_rollback(kv1);
    kvstore_close(kv1);

    /* Writer 2 can now succeed */
    KVStore *kv3 = open_cfg(db, 0, 0);
    int rc3 = kv3 ? kvstore_begin(kv3, 1) : KVSTORE_ERROR;
    CHECK(rc3 == KVSTORE_OK, "writer 2 succeeds after writer 1 releases lock");
    if (rc3 == KVSTORE_OK) kvstore_rollback(kv3);
    if (kv3) kvstore_close(kv3);

    cleanup(db);
}

/* ---- Test 6: Per-thread handles, fullMutex=0 ---------------------------- */

#define T6_THREADS 8
#define T6_KEYS    20

typedef struct {
    const char *db;
    int         id;
    int         errors;
} T6Args;

static void *t6_worker(void *arg) {
    T6Args *a = (T6Args *)arg;
    KVStore *kv = open_cfg(a->db, 0, 5000);
    if (!kv) { a->errors++; return NULL; }

    kvstore_begin(kv, 1);
    int i;
    for (i = 0; i < T6_KEYS; i++) {
        char key[32], val[32];
        snprintf(key, sizeof(key), "t%d_k%d", a->id, i);
        snprintf(val, sizeof(val), "t%d_v%d", a->id, i);
        if (kvstore_put(kv, key, (int)strlen(key),
                        val,  (int)strlen(val)) != KVSTORE_OK)
            a->errors++;
    }
    if (kvstore_commit(kv) != KVSTORE_OK)
        a->errors++;

    /* Verify own keys */
    for (i = 0; i < T6_KEYS; i++) {
        char key[32], expected[32];
        snprintf(key,      sizeof(key),      "t%d_k%d", a->id, i);
        snprintf(expected, sizeof(expected), "t%d_v%d", a->id, i);
        void *got = NULL; int glen = 0;
        int rc = kvstore_get(kv, key, (int)strlen(key), &got, &glen);
        if (rc != KVSTORE_OK) {
            a->errors++;
        } else {
            if (glen != (int)strlen(expected) ||
                memcmp(got, expected, glen) != 0)
                a->errors++;
            sqliteFree(got);
        }
    }

    kvstore_close(kv);
    return NULL;
}

static void test_per_thread_handles(void) {
    printf("\n--- Test 6: Per-thread handles, fullMutex=0 (%d threads) ---\n",
           T6_THREADS);
    const char *db = "fm_perthread.db";
    cleanup(db);

    /* Pre-create the WAL database so threads don't race on SHM init */
    KVStore *init = NULL;
    kvstore_open(db, &init, KVSTORE_JOURNAL_WAL);
    kvstore_close(init);

    pthread_t threads[T6_THREADS];
    T6Args    args[T6_THREADS];
    int i;

    for (i = 0; i < T6_THREADS; i++) {
        args[i].db = db; args[i].id = i; args[i].errors = 0;
        pthread_create(&threads[i], NULL, t6_worker, &args[i]);
    }
    for (i = 0; i < T6_THREADS; i++)
        pthread_join(threads[i], NULL);

    int total_errors = 0;
    for (i = 0; i < T6_THREADS; i++)
        total_errors += args[i].errors;

    CHECK(total_errors == 0,
          "no errors across per-thread handles (fullMutex=0)");

    /* Post-run: verify total key count */
    KVStore *verify = open_cfg(db, 0, 0);
    if (verify) {
        int count = 0;
        for (i = 0; i < T6_THREADS; i++) {
            int j;
            for (j = 0; j < T6_KEYS; j++) {
                char key[32];
                snprintf(key, sizeof(key), "t%d_k%d", i, j);
                void *v = NULL; int vl = 0;
                if (kvstore_get(verify, key, (int)strlen(key), &v, &vl)
                    == KVSTORE_OK) {
                    count++;
                    sqliteFree(v);
                }
            }
        }
        CHECK(count == T6_THREADS * T6_KEYS,
              "all keys present after per-thread writes");
        kvstore_close(verify);
    }

    cleanup(db);
}

/* ---- main --------------------------------------------------------------- */

int main(void) {
    printf("=== SNKV fullMutex Test Suite ===\n");
    enable_ansi_colors();

    test_null_config_no_mutex();
    test_fullmutex_zero();
    test_fullmutex_one();
    test_shared_handle_fullmutex();
    test_second_writer_busy();
    test_per_thread_handles();

    printf("\n=== Results: %d passed, %d failed ===\n", g_passed, g_failed);
    return g_failed ? 1 : 0;
}
