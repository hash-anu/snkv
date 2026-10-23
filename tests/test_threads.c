/* SPDX-License-Identifier: Apache-2.0 */
/*
** test_threads.c — one KVStore handle shared by many threads.
**
** Locking model under test: ONE recursive mutex per handle (no per-CF
** locks); kvstore_begin(write) keeps it held until commit/rollback, so
** other threads wait instead of joining or rolling back that transaction.
**
**   Test 1: a thread's rollback never undoes another thread's write
**   Test 2: many threads running begin/put/commit on one handle
**   Test 3: mixed load: CFs, TTL, iterators, purge, checkpoint, vacuum
**   Test 4: kvstore_errmsg_copy under concurrent errors
**   Test 5: close inside your own transaction (lock released before free)
**   Test 6: close waits for another thread's open transaction
**   Test 7: CF handles opened/closed while other threads use the handle
**
** A watchdog fails the run (exit 2) if anything deadlocks.  Run it under
** ThreadSanitizer with `make test-tsan`.
*/
#include "kvstore.h"
#include "sqlite3.h"            /* sqlite3_mutex_* for the test's own flags */
#include "platform_compat.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int g_passed = 0;
static int g_failed = 0;

#define CHECK(cond, msg) do {                           \
    if (cond) { printf("  [PASS] %s\n", msg); g_passed++; } \
    else      { printf("  [FAIL] %s\n", msg); g_failed++; } \
} while(0)

static void cleanup(const char *db) {
    char buf[512];
    remove(db);
    snprintf(buf, sizeof(buf), "%s-wal", db);  remove(buf);
    snprintf(buf, sizeof(buf), "%s-shm", db);  remove(buf);
}

static KVStore *open_shared(const char *path) {
    KVStoreConfig cfg = {0};
    cfg.journalMode = KVSTORE_JOURNAL_WAL;
    KVStore *kv = NULL;
    if (kvstore_open_v2(path, &kv, &cfg) != KVSTORE_OK) return NULL;
    return kv;
}

/* ---- Flags shared between test threads (guarded by a SQLite mutex) ---- */
static sqlite3_mutex *g_flagMutex;
static int g_flags[8];

static void flag_set(int i) {
    sqlite3_mutex_enter(g_flagMutex); g_flags[i] = 1; sqlite3_mutex_leave(g_flagMutex);
}
static int flag_get(int i) {
    sqlite3_mutex_enter(g_flagMutex); int v = g_flags[i]; sqlite3_mutex_leave(g_flagMutex);
    return v;
}
static void flags_reset(void) {
    sqlite3_mutex_enter(g_flagMutex); memset(g_flags, 0, sizeof g_flags); sqlite3_mutex_leave(g_flagMutex);
}
static void flag_wait(int i) { while (!flag_get(i)) usleep(1000); }

/* ---- Watchdog: a deadlock fails the run instead of hanging it ---- */
#define WATCHDOG_SECONDS 120
static void *watchdog(void *arg) {
    (void)arg;
    for (int i = 0; i < WATCHDOG_SECONDS * 10; i++) {
        if (flag_get(7)) return NULL;
        usleep(100000);
    }
    printf("  [FAIL] watchdog: no progress for %d s (deadlock?)\n", WATCHDOG_SECONDS);
    fflush(stdout);
    exit(2);
    return NULL;
}

/* ======================================================================
** Test 1: transaction isolation between threads on one handle.
** A begins, writes "a", waits, rolls back.  B writes "b" while A's
** transaction is open.  B must wait for A (not join it), so A's rollback
** removes "a" only and "b" survives.
** ==================================================================== */
#define F_A_BEGAN  0
#define F_A_DONE   1
#define F_B_TRYING 2   /* the other thread is about to call into the handle */

static void *t1_A(void *arg) {
    KVStore *kv = (KVStore*)arg;
    kvstore_begin(kv, 1);
    kvstore_put(kv, "a", 1, "1", 1);
    flag_set(F_A_BEGAN);
    flag_wait(F_B_TRYING);              /* B is now calling kvstore_put ... */
    usleep(200000);                     /* ... and must still be blocked */
    flag_set(F_A_DONE);
    kvstore_rollback(kv);
    return NULL;
}

static int t1_waitedForA = -1;
static int t1_putRc = -1;

static void *t1_B(void *arg) {
    KVStore *kv = (KVStore*)arg;
    flag_wait(F_A_BEGAN);
    flag_set(F_B_TRYING);
    t1_putRc = kvstore_put(kv, "b", 1, "2", 1);   /* must block until A ends */
    t1_waitedForA = flag_get(F_A_DONE);
    return NULL;
}

static void test_txn_isolation(void) {
    const char *db = "t_thr_iso.db";
    printf("\nTest 1: a thread's rollback never undoes another thread's write\n");
    cleanup(db); flags_reset();
    KVStore *kv = open_shared(db);
    CHECK(kv != NULL, "open shared handle");
    if (!kv) return;

    pthread_t a, b;
    pthread_create(&a, NULL, t1_A, kv);
    pthread_create(&b, NULL, t1_B, kv);
    pthread_join(a, NULL);
    pthread_join(b, NULL);

    void *v = NULL; int n = 0;
    CHECK(t1_putRc == KVSTORE_OK, "B's put succeeded");
    CHECK(t1_waitedForA == 1, "B's put waited until A's transaction ended");
    CHECK(kvstore_get(kv, "a", 1, &v, &n) == KVSTORE_NOTFOUND, "A's rolled-back write is gone");
    if (v) { snkv_free(v); v = NULL; }
    CHECK(kvstore_get(kv, "b", 1, &v, &n) == KVSTORE_OK && n == 1, "B's write survived A's rollback");
    if (v) snkv_free(v);
    kvstore_close(kv);
    cleanup(db);
}

/* ======================================================================
** Test 2: N threads each run M transactions of K puts on one handle.
** Before the fix a second thread's begin failed ("transaction already
** active") or its puts landed in another thread's transaction.
** ==================================================================== */
#define T2_THREADS 4
#define T2_TXNS    100
#define T2_PUTS    5

typedef struct { KVStore *kv; int id; int errors; } T2Arg;

static void *t2_worker(void *arg) {
    T2Arg *a = (T2Arg*)arg;
    char key[32];
    for (int t = 0; t < T2_TXNS; t++) {
        if (kvstore_begin(a->kv, 1) != KVSTORE_OK) { a->errors++; continue; }
        int ok = 1;
        for (int p = 0; p < T2_PUTS; p++) {
            snprintf(key, sizeof key, "t%d:x%03d:p%d", a->id, t, p);
            if (kvstore_put(a->kv, key, (int)strlen(key), "v", 1) != KVSTORE_OK) ok = 0;
        }
        if (ok && (t % 10) == 9) {                  /* every 10th: roll back */
            if (kvstore_rollback(a->kv) != KVSTORE_OK) a->errors++;
        } else if (!ok || kvstore_commit(a->kv) != KVSTORE_OK) {
            a->errors++;
            kvstore_rollback(a->kv);
        }
    }
    return NULL;
}

static void test_concurrent_txns(void) {
    const char *db = "t_thr_txn.db";
    printf("\nTest 2: %d threads x %d transactions on one handle\n", T2_THREADS, T2_TXNS);
    cleanup(db);
    KVStore *kv = open_shared(db);
    CHECK(kv != NULL, "open shared handle");
    if (!kv) return;

    pthread_t th[T2_THREADS]; T2Arg args[T2_THREADS];
    for (int i = 0; i < T2_THREADS; i++) {
        args[i].kv = kv; args[i].id = i; args[i].errors = 0;
        pthread_create(&th[i], NULL, t2_worker, &args[i]);
    }
    int errors = 0;
    for (int i = 0; i < T2_THREADS; i++) { pthread_join(th[i], NULL); errors += args[i].errors; }

    int64_t count = -1;
    kvstore_count(kv, &count);
    int64_t want = (int64_t)T2_THREADS * (T2_TXNS - T2_TXNS / 10) * T2_PUTS;
    CHECK(errors == 0, "no begin/put/commit/rollback errors");
    CHECK(count == want, "exactly the committed transactions' keys are present");
    if (count != want) printf("    count=%lld want=%lld\n", (long long)count, (long long)want);

    /* A rolled-back transaction's keys must all be absent, a committed one's present. */
    void *v = NULL; int n = 0;
    CHECK(kvstore_get(kv, "t0:x009:p0", 10, &v, &n) == KVSTORE_NOTFOUND, "rolled-back key absent");
    if (v) { snkv_free(v); v = NULL; }
    CHECK(kvstore_get(kv, "t3:x008:p4", 10, &v, &n) == KVSTORE_OK, "committed key present");
    if (v) snkv_free(v);
    CHECK(kvstore_integrity_check(kv, NULL) == KVSTORE_OK, "integrity check");
    kvstore_close(kv);
    cleanup(db);
}

/* ======================================================================
** Test 3: mixed load on one handle — every thread does something else.
** ==================================================================== */
#define T3_ITERS 300

typedef struct { KVStore *kv; KVColumnFamily *cf; int id; int errors; } T3Arg;

static void *t3_writer(void *arg) {              /* puts/deletes on its own CF */
    T3Arg *a = (T3Arg*)arg; char key[32];
    for (int i = 0; i < T3_ITERS; i++) {
        snprintf(key, sizeof key, "k%05d", i);
        if (kvstore_cf_put(a->cf, key, (int)strlen(key), key, (int)strlen(key)) != KVSTORE_OK) a->errors++;
        if (i % 3 == 0) kvstore_cf_delete(a->cf, key, (int)strlen(key));
    }
    return NULL;
}

static void *t3_ttl(void *arg) {                 /* short TTLs + purge on default CF */
    T3Arg *a = (T3Arg*)arg; char key[32];
    for (int i = 0; i < T3_ITERS; i++) {
        snprintf(key, sizeof key, "ttl%05d", i);
        if (kvstore_put_ttl(a->kv, key, (int)strlen(key), "x", 1, kvstore_now_ms() + 5) != KVSTORE_OK) a->errors++;
        if (i % 50 == 49) { int nd = 0; if (kvstore_purge_expired(a->kv, &nd) != KVSTORE_OK) a->errors++; }
    }
    return NULL;
}

static void *t3_reader(void *arg) {              /* iterators over another CF */
    T3Arg *a = (T3Arg*)arg;
    for (int r = 0; r < 20; r++) {
        KVIterator *it = NULL;
        if (kvstore_cf_iterator_create(a->cf, &it) != KVSTORE_OK) { a->errors++; continue; }
        char prev[32] = ""; int first = 1;
        for (kvstore_iterator_first(it); !kvstore_iterator_eof(it); kvstore_iterator_next(it)) {
            void *pk = NULL; int nk = 0;
            if (kvstore_iterator_key(it, &pk, &nk) != KVSTORE_OK || nk >= (int)sizeof prev) { a->errors++; break; }
            char cur[32]; memcpy(cur, pk, nk); cur[nk] = 0;
            if (!first && strcmp(prev, cur) >= 0) a->errors++;   /* order must hold */
            memcpy(prev, cur, nk + 1); first = 0;
        }
        kvstore_iterator_close(it);
    }
    return NULL;
}

static void *t3_maint(void *arg) {               /* checkpoint + vacuum + stats */
    T3Arg *a = (T3Arg*)arg;
    for (int i = 0; i < 30; i++) {
        int nl = 0, nc = 0;
        int rc = kvstore_checkpoint(a->kv, KVSTORE_CHECKPOINT_PASSIVE, &nl, &nc);
        if (rc != KVSTORE_OK && rc != KVSTORE_BUSY) a->errors++;
        rc = kvstore_incremental_vacuum(a->kv, 4);
        if (rc != KVSTORE_OK && rc != KVSTORE_BUSY) a->errors++;
        KVStoreStats st; kvstore_stats(a->kv, &st);
        usleep(2000);
    }
    return NULL;
}

static void test_mixed_load(void) {
    const char *db = "t_thr_mix.db";
    printf("\nTest 3: mixed load on one handle (CFs, TTL, iterators, maintenance)\n");
    cleanup(db);
    KVStore *kv = open_shared(db);
    CHECK(kv != NULL, "open shared handle");
    if (!kv) return;
    KVColumnFamily *cfW = NULL, *cfR = NULL;
    kvstore_cf_create(kv, "w", &cfW);
    kvstore_cf_create(kv, "r", &cfR);
    char key[32];
    for (int i = 0; i < 200; i++) {               /* data for the readers */
        snprintf(key, sizeof key, "r%05d", i);
        kvstore_cf_put(cfR, key, (int)strlen(key), "v", 1);
    }

    T3Arg aw = {kv, cfW, 0, 0}, at = {kv, NULL, 1, 0}, ar1 = {kv, cfR, 2, 0},
          ar2 = {kv, cfW, 3, 0}, am = {kv, NULL, 4, 0};
    pthread_t th[5];
    pthread_create(&th[0], NULL, t3_writer, &aw);
    pthread_create(&th[1], NULL, t3_ttl,    &at);
    pthread_create(&th[2], NULL, t3_reader, &ar1);
    pthread_create(&th[3], NULL, t3_reader, &ar2);
    pthread_create(&th[4], NULL, t3_maint,  &am);
    for (int i = 0; i < 5; i++) pthread_join(th[i], NULL);

    CHECK(aw.errors == 0, "writer thread: no errors");
    CHECK(at.errors == 0, "TTL/purge thread: no errors");
    CHECK(ar1.errors == 0 && ar2.errors == 0, "iterator threads: keys in order, no errors");
    CHECK(am.errors == 0, "checkpoint/vacuum thread: no errors");

    int64_t nW = -1, nR = -1;
    kvstore_cf_count(cfW, &nW);
    kvstore_cf_count(cfR, &nR);
    CHECK(nW == T3_ITERS - (T3_ITERS + 2) / 3, "writer CF holds exactly the undeleted keys");
    CHECK(nR == 200, "reader CF unchanged");
    CHECK(kvstore_integrity_check(kv, NULL) == KVSTORE_OK, "integrity check");
    kvstore_cf_close(cfW);
    kvstore_cf_close(cfR);
    kvstore_close(kv);
    cleanup(db);
}

/* ======================================================================
** Test 4: kvstore_errmsg_copy while other threads produce errors.
** ==================================================================== */
static void *t4_err(void *arg) {
    KVStore *kv = (KVStore*)arg;
    char buf[64];
    for (int i = 0; i < 2000; i++) {
        kvstore_commit(kv);                       /* "no active transaction to commit" */
        kvstore_errmsg_copy(kv, buf, (int)sizeof buf);
        if (strlen(buf) >= sizeof buf) return (void*)1;
    }
    return NULL;
}

static void test_errmsg_copy(void) {
    const char *db = "t_thr_err.db";
    printf("\nTest 4: kvstore_errmsg_copy under concurrent errors\n");
    cleanup(db);
    KVStore *kv = open_shared(db);
    CHECK(kv != NULL, "open shared handle");
    if (!kv) return;
    pthread_t th[4]; void *ret[4];
    for (int i = 0; i < 4; i++) pthread_create(&th[i], NULL, t4_err, kv);
    int bad = 0;
    for (int i = 0; i < 4; i++) { pthread_join(th[i], &ret[i]); if (ret[i]) bad++; }
    CHECK(bad == 0, "every copy NUL-terminated within the buffer");

    char small[8];
    kvstore_errmsg_copy(kv, small, (int)sizeof small);
    CHECK(strlen(small) == sizeof small - 1, "long message truncated to fit");
    CHECK(kvstore_errmsg_copy(kv, NULL, 10) == KVSTORE_ERROR, "NULL buffer rejected");
    CHECK(kvstore_errmsg_copy(kv, small, 0) == KVSTORE_ERROR, "zero size rejected");
    kvstore_close(kv);
    cleanup(db);
}

/* ======================================================================
** Test 5: closing inside your own transaction.  The begin's extra mutex
** hold must be released before the mutex is freed (ASan/TSan/Helgrind
** report a destroyed locked mutex otherwise).
** ==================================================================== */
static void test_close_in_own_txn(void) {
    const char *db = "t_thr_c5.db";
    printf("\nTest 5: close inside your own transaction\n");
    cleanup(db);
    KVStore *kv = open_shared(db);
    CHECK(kv != NULL, "open shared handle");
    if (!kv) return;
    kvstore_put(kv, "keep", 4, "1", 1);
    CHECK(kvstore_begin(kv, 1) == KVSTORE_OK, "begin");
    kvstore_put(kv, "lost", 4, "2", 1);
    CHECK(kvstore_close(kv) == KVSTORE_OK, "close inside the transaction returns");

    kv = open_shared(db);
    void *v = NULL; int n = 0;
    CHECK(kvstore_get(kv, "keep", 4, &v, &n) == KVSTORE_OK, "committed key kept");
    if (v) { snkv_free(v); v = NULL; }
    CHECK(kvstore_get(kv, "lost", 4, &v, &n) == KVSTORE_NOTFOUND, "uncommitted key rolled back");
    if (v) snkv_free(v);
    kvstore_close(kv);
    cleanup(db);
}

/* ======================================================================
** Test 6: close from another thread waits for an open transaction.
** ==================================================================== */
static void *t6_owner(void *arg) {
    KVStore *kv = (KVStore*)arg;
    kvstore_begin(kv, 1);
    kvstore_put(kv, "late", 4, "1", 1);
    flag_set(F_A_BEGAN);
    flag_wait(F_B_TRYING);              /* main is now calling kvstore_close */
    usleep(200000);
    flag_set(F_A_DONE);
    kvstore_commit(kv);                 /* close may proceed only after this */
    return NULL;
}

static void test_close_waits(void) {
    const char *db = "t_thr_c6.db";
    printf("\nTest 6: close waits for another thread's transaction\n");
    cleanup(db); flags_reset();
    KVStore *kv = open_shared(db);
    CHECK(kv != NULL, "open shared handle");
    if (!kv) return;
    pthread_t t;
    pthread_create(&t, NULL, t6_owner, kv);
    flag_wait(F_A_BEGAN);
    flag_set(F_B_TRYING);
    kvstore_close(kv);                  /* blocks until the owner commits */
    int waited = flag_get(F_A_DONE);
    pthread_join(t, NULL);
    CHECK(waited == 1, "close waited for the owner's commit");

    kv = open_shared(db);
    void *v = NULL; int n = 0;
    CHECK(kvstore_get(kv, "late", 4, &v, &n) == KVSTORE_OK, "owner's committed write is on disk");
    if (v) snkv_free(v);
    kvstore_close(kv);
    cleanup(db);
}

/* ======================================================================
** Test 7: column-family handles opened and closed while other threads
** use the same handle.  Closing the last handle to a CF frees its cached
** read cursor, which touches the handle's shared page cache — it must
** happen under the handle lock (run under TSan to check).
** ==================================================================== */
#define T7_ROUNDS 300

typedef struct { KVStore *kv; int errors; } T7Arg;

static void *t7_churn(void *arg) {               /* open → read → close, repeatedly */
    T7Arg *a = (T7Arg*)arg;
    for (int i = 0; i < T7_ROUNDS; i++) {
        KVColumnFamily *cf = NULL;
        if (kvstore_cf_open(a->kv, "churn", &cf) != KVSTORE_OK) { a->errors++; continue; }
        void *v = NULL; int n = 0;
        if (kvstore_cf_get(cf, "c", 1, &v, &n) != KVSTORE_OK) a->errors++;   /* caches a read cursor */
        snkv_free(v);
        if (i % 10 == 0) {                        /* sometimes an iterator too */
            KVIterator *it = NULL;
            if (kvstore_cf_iterator_create(cf, &it) == KVSTORE_OK) {
                for (kvstore_iterator_first(it); !kvstore_iterator_eof(it); kvstore_iterator_next(it)) {}
                kvstore_iterator_close(it);
            } else a->errors++;
        }
        kvstore_cf_close(cf);                     /* last handle: frees the cursor */
    }
    return NULL;
}

static void *t7_writer(void *arg) {
    T7Arg *a = (T7Arg*)arg; char key[32];
    for (int i = 0; i < T7_ROUNDS * 2; i++) {
        snprintf(key, sizeof key, "w%05d", i);
        if (kvstore_put(a->kv, key, (int)strlen(key), "v", 1) != KVSTORE_OK) a->errors++;
    }
    return NULL;
}

static void *t7_reader(void *arg) {
    T7Arg *a = (T7Arg*)arg;
    for (int i = 0; i < T7_ROUNDS; i++) {
        void *v = NULL; int n = 0;
        int rc = kvstore_get(a->kv, "w00000", 6, &v, &n);
        if (rc != KVSTORE_OK && rc != KVSTORE_NOTFOUND) a->errors++;
        snkv_free(v);
    }
    return NULL;
}

static void test_cf_churn(void) {
    const char *db = "t_thr_c7.db";
    printf("\nTest 7: CF handles opened/closed while other threads use the handle\n");
    cleanup(db);
    KVStore *kv = open_shared(db);
    CHECK(kv != NULL, "open shared handle");
    if (!kv) return;
    KVColumnFamily *cf = NULL;
    kvstore_cf_create(kv, "churn", &cf);
    kvstore_cf_put(cf, "c", 1, "x", 1);
    kvstore_cf_close(cf);

    T7Arg ac = {kv, 0}, aw = {kv, 0}, ar = {kv, 0};
    pthread_t th[3];
    pthread_create(&th[0], NULL, t7_churn,  &ac);
    pthread_create(&th[1], NULL, t7_writer, &aw);
    pthread_create(&th[2], NULL, t7_reader, &ar);
    for (int i = 0; i < 3; i++) pthread_join(th[i], NULL);

    CHECK(ac.errors == 0, "open/get/iterate/close loop: no errors");
    CHECK(aw.errors == 0 && ar.errors == 0, "writer and reader: no errors");
    int64_t n = -1; kvstore_count(kv, &n);
    CHECK(n == T7_ROUNDS * 2, "every write landed");
    CHECK(kvstore_integrity_check(kv, NULL) == KVSTORE_OK, "integrity check");
    kvstore_close(kv);
    cleanup(db);
}

int main(void) {
    printf("=== test_threads: one handle shared by many threads ===\n");
    sqlite3_initialize();
    g_flagMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
    if (!g_flagMutex) { printf("cannot allocate mutex\n"); return 1; }

    pthread_t dog;
    pthread_create(&dog, NULL, watchdog, NULL);

    test_txn_isolation();
    test_concurrent_txns();
    test_mixed_load();
    test_errmsg_copy();
    test_close_in_own_txn();
    test_close_waits();
    test_cf_churn();

    flag_set(7);                        /* stop the watchdog */
    pthread_join(dog, NULL);
    sqlite3_mutex_free(g_flagMutex);

    printf("\n=== Results: %d passed, %d failed ===\n", g_passed, g_failed);
    return g_failed ? 1 : 0;
}
