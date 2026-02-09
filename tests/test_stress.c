/*
** SQLite-Style Integration & Stress Tests for SNKV
** Windows-compatible version
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Windows-specific includes */
#ifdef _WIN32
#include <windows.h>
#include <process.h>
#define usleep(x) Sleep((x)/1000)
typedef HANDLE pthread_t;
typedef struct { int dummy; } pthread_attr_t;

/* Windows threading wrappers */
static int pthread_create(pthread_t* thread, void* attr, void* (*start_routine)(void*), void* arg) {
    *thread = (HANDLE)_beginthread((void(*)(void*))start_routine, 0, arg);
    return (*thread == (HANDLE)-1L) ? -1 : 0;
}

static int pthread_join(pthread_t thread, void** retval) {
    WaitForSingleObject(thread, INFINITE);
    CloseHandle(thread);
    return 0;
}

/* Windows timing */
static double now_sec(void) {
    LARGE_INTEGER freq, count;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&count);
    return (double)count.QuadPart / (double)freq.QuadPart;
}
#else
  /* POSIX includes */
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>

/* POSIX timing */
static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}
#endif

#include "kvstore.h"

/*
** SQLite extended error codes encode the base code in the lower 8 bits.
*/
#define IS_BUSY(rc)   (((rc)&0xFF)==SQLITE_BUSY || ((rc)&0xFF)==SQLITE_LOCKED)

/* ---- configuration ---- */
#define DB_FILE       "stress_test.db"
#define DB_WAL_FILE   "stress_wal.db"
#define NUM_THREADS   8
#define WRITE_STORM_N 100000
#define LARGE_FILL_N  50000
#define TXN_CYCLE_N   5000
#define MIXED_OPS_N   50000

/* ---- test bookkeeping ---- */
static int g_passed = 0;
static int g_failed = 0;

/* ANSI colors (works on Windows 10+ with VT100 enabled) */
#define CLR_GREEN  "\033[0;32m"
#define CLR_RED    "\033[0;31m"
#define CLR_BLUE   "\033[0;34m"
#define CLR_CYAN   "\033[0;36m"
#define CLR_RESET  "\033[0m"

static void print_result(const char* name, int ok) {
    if (ok) {
        g_passed++;
        printf(CLR_GREEN "[PASS]" CLR_RESET " %s\n", name);
    }
    else {
        g_failed++;
        printf(CLR_RED   "[FAIL]" CLR_RESET " %s\n", name);
    }
}

static void cleanup(const char* db) {
    char buf[512];
    remove(db);
    snprintf(buf, sizeof(buf), "%s-journal", db);
    remove(buf);
    snprintf(buf, sizeof(buf), "%s-wal", db);
    remove(buf);
    snprintf(buf, sizeof(buf), "%s-shm", db);
    remove(buf);
}

/* helper: generate a key like "key-00000042" */
static void make_key(char* buf, int buflen, int idx) {
    snprintf(buf, buflen, "key-%08d", idx);
}

/* helper: generate a value with repeating pattern */
static void make_value(char* buf, int buflen, int idx, int vallen) {
    int i;
    snprintf(buf, buflen, "val-%08d-", idx);
    int hdr = (int)strlen(buf);
    for (i = hdr; i < vallen - 1 && i < buflen - 1; i++) {
        buf[i] = 'A' + (i % 26);
    }
    buf[i < buflen ? i : buflen - 1] = '\0';
}

/* ================================================================
** 1. EDGE CASES & BOUNDARY VALUES
** ================================================================ */

static void test_edge_empty_value(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
    int ok = 0;

    if (rc == KVSTORE_OK) {
        const char* k = "empty_val_key";
        rc = kvstore_put(kv, k, (int)strlen(k), "", 0);
        if (rc == KVSTORE_OK) {
            void* got = NULL; int glen = 0;
            rc = kvstore_get(kv, k, (int)strlen(k), &got, &glen);
            ok = (rc == KVSTORE_OK && glen == 0);
            if (got) sqliteFree(got);
        }
        kvstore_close(kv);
    }
    print_result("Empty value round-trip", ok);
    cleanup(DB_FILE);
}

static void test_edge_binary_key_with_nulls(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
    int ok = 0;

    if (rc == KVSTORE_OK) {
        unsigned char binkey[] = { 0x01, 0x00, 0x02, 0x00, 0x03 };
        const char* val = "binary-key-value";
        rc = kvstore_put(kv, binkey, 5, val, (int)strlen(val));
        if (rc == KVSTORE_OK) {
            void* got = NULL; int glen = 0;
            rc = kvstore_get(kv, binkey, 5, &got, &glen);
            ok = (rc == KVSTORE_OK && glen == (int)strlen(val)
                && memcmp(got, val, glen) == 0);
            if (got) sqliteFree(got);
        }
        kvstore_close(kv);
    }
    print_result("Binary key with embedded NULs", ok);
    cleanup(DB_FILE);
}

static void test_edge_single_byte_key(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
    int ok = 0;

    if (rc == KVSTORE_OK) {
        unsigned char k = 0xFF;
        const char* val = "single-byte";
        rc = kvstore_put(kv, &k, 1, val, (int)strlen(val));
        if (rc == KVSTORE_OK) {
            void* got = NULL; int glen = 0;
            rc = kvstore_get(kv, &k, 1, &got, &glen);
            ok = (rc == KVSTORE_OK && glen == (int)strlen(val)
                && memcmp(got, val, glen) == 0);
            if (got) sqliteFree(got);
        }
        kvstore_close(kv);
    }
    print_result("Single-byte key (0xFF)", ok);
    cleanup(DB_FILE);
}

static void test_edge_large_key_value(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
    int ok = 0;

    if (rc == KVSTORE_OK) {
        int klen = 1024;
        int vlen = 1024 * 1024;
        char* bigkey = (char*)malloc(klen);
        char* bigval = (char*)malloc(vlen);
        if (bigkey && bigval) {
            int i;
            for (i = 0; i < klen; i++) bigkey[i] = 'K' + (i % 26);
            for (i = 0; i < vlen; i++) bigval[i] = 'V' + (i % 26);

            rc = kvstore_put(kv, bigkey, klen, bigval, vlen);
            if (rc == KVSTORE_OK) {
                void* got = NULL; int glen = 0;
                rc = kvstore_get(kv, bigkey, klen, &got, &glen);
                ok = (rc == KVSTORE_OK && glen == vlen
                    && memcmp(got, bigval, vlen) == 0);
                if (got) sqliteFree(got);
            }
        }
        free(bigkey);
        free(bigval);
        kvstore_close(kv);
    }
    print_result("Large key (1KB) + large value (1MB)", ok);
    cleanup(DB_FILE);
}

static void test_edge_overwrite_same_key(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
    int ok = 0;

    if (rc == KVSTORE_OK) {
        const char* k = "overwrite_key";
        int i;
        char val[64];
        for (i = 0; i < 100; i++) {
            snprintf(val, sizeof(val), "version-%d", i);
            rc = kvstore_put(kv, k, (int)strlen(k), val, (int)strlen(val));
            if (rc != KVSTORE_OK) break;
        }
        if (rc == KVSTORE_OK) {
            void* got = NULL; int glen = 0;
            rc = kvstore_get(kv, k, (int)strlen(k), &got, &glen);
            ok = (rc == KVSTORE_OK && glen == (int)strlen("version-99")
                && memcmp(got, "version-99", glen) == 0);
            if (got) sqliteFree(got);
        }
        kvstore_close(kv);
    }
    print_result("100x overwrite same key", ok);
    cleanup(DB_FILE);
}

static void test_edge_get_nonexistent(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
    int ok = 0;

    if (rc == KVSTORE_OK) {
        void* got = NULL; int glen = 0;
        rc = kvstore_get(kv, "no_such_key", 11, &got, &glen);
        ok = (rc == KVSTORE_NOTFOUND && got == NULL);
        kvstore_close(kv);
    }
    print_result("Get non-existent key returns NOTFOUND", ok);
    cleanup(DB_FILE);
}

static void test_edge_delete_nonexistent(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
    int ok = 0;

    if (rc == KVSTORE_OK) {
        rc = kvstore_delete(kv, "no_such_key", 11);
        ok = (rc == KVSTORE_NOTFOUND);
        kvstore_close(kv);
    }
    print_result("Delete non-existent key returns NOTFOUND", ok);
    cleanup(DB_FILE);
}

static void test_edge_put_after_delete(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
    int ok = 0;

    if (rc == KVSTORE_OK) {
        const char* k = "reinsert";
        rc = kvstore_put(kv, k, (int)strlen(k), "first", 5);
        if (rc == KVSTORE_OK) rc = kvstore_delete(kv, k, (int)strlen(k));
        if (rc == KVSTORE_OK) rc = kvstore_put(kv, k, (int)strlen(k), "second", 6);
        if (rc == KVSTORE_OK) {
            void* got = NULL; int glen = 0;
            rc = kvstore_get(kv, k, (int)strlen(k), &got, &glen);
            ok = (rc == KVSTORE_OK && glen == 6 && memcmp(got, "second", 6) == 0);
            if (got) sqliteFree(got);
        }
        kvstore_close(kv);
    }
    print_result("Put after delete (re-insert same key)", ok);
    cleanup(DB_FILE);
}

/* ================================================================
** 2. HIGH-VOLUME WRITE STORM
** ================================================================ */

static void test_write_storm(int journalMode, const char* label) {
    const char* db = (journalMode == KVSTORE_JOURNAL_WAL) ? DB_WAL_FILE : DB_FILE;
    cleanup(db);
    KVStore* kv = NULL;
    int rc = kvstore_open(db, &kv, 0, journalMode);
    int ok = 0;

    if (rc == KVSTORE_OK) {
        double t0 = now_sec();
        int i;
        char key[32], val[128];
        int batchSize = 1000;
        int integrityChecks = 0;
        int integrityOk = 1;

        for (i = 0; i < WRITE_STORM_N; i++) {
            if ((i % batchSize) == 0) {
                if (i > 0) kvstore_commit(kv);
                kvstore_begin(kv, 1);
            }
            make_key(key, sizeof(key), i);
            make_value(val, sizeof(val), i, 80);
            rc = kvstore_put(kv, key, (int)strlen(key), val, (int)strlen(val));
            if (rc != KVSTORE_OK) break;

            if ((i % 25000) == 0 && i > 0) {
                kvstore_commit(kv);
                char* errMsg = NULL;
                int ic = kvstore_integrity_check(kv, &errMsg);
                if (ic != KVSTORE_OK) {
                    printf("    Integrity FAILED at row %d: %s\n", i,
                        errMsg ? errMsg : "unknown");
                    integrityOk = 0;
                }
                if (errMsg) sqliteFree(errMsg);
                integrityChecks++;
                kvstore_begin(kv, 1);
            }
        }
        if (rc == KVSTORE_OK) kvstore_commit(kv);

        double elapsed = now_sec() - t0;
        printf("    %d writes in %.3f sec (%d ops/sec), %d integrity checks\n",
            WRITE_STORM_N, elapsed,
            (int)(WRITE_STORM_N / elapsed), integrityChecks);

        char* errMsg = NULL;
        int ic = kvstore_integrity_check(kv, &errMsg);
        if (ic != KVSTORE_OK) {
            printf("    Final integrity FAILED: %s\n", errMsg ? errMsg : "unknown");
            integrityOk = 0;
        }
        if (errMsg) sqliteFree(errMsg);

        int spotOk = 1;
        int spots[] = { 0, 1, WRITE_STORM_N / 2, WRITE_STORM_N - 1 };
        for (i = 0; i < 4; i++) {
            make_key(key, sizeof(key), spots[i]);
            make_value(val, sizeof(val), spots[i], 80);
            void* got = NULL; int glen = 0;
            rc = kvstore_get(kv, key, (int)strlen(key), &got, &glen);
            if (rc != KVSTORE_OK || glen != (int)strlen(val)
                || memcmp(got, val, glen) != 0) {
                spotOk = 0;
            }
            if (got) sqliteFree(got);
        }

        ok = (rc == KVSTORE_OK && integrityOk && spotOk);
        kvstore_close(kv);
    }
    print_result(label, ok);
    cleanup(db);
}

/* ================================================================
** 3. LARGE DATASET
** ================================================================ */

static void test_large_dataset(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_WAL);
    int ok = 0;

    if (rc == KVSTORE_OK) {
        double t0 = now_sec();
        int i;
        char key[32], val[128];
        kvstore_begin(kv, 1);
        for (i = 0; i < LARGE_FILL_N; i++) {
            if ((i % 5000) == 0 && i > 0) {
                kvstore_commit(kv);
                kvstore_begin(kv, 1);
            }
            make_key(key, sizeof(key), i);
            make_value(val, sizeof(val), i, 100);
            rc = kvstore_put(kv, key, (int)strlen(key), val, (int)strlen(val));
            if (rc != KVSTORE_OK) break;
        }
        kvstore_commit(kv);
        double fillTime = now_sec() - t0;
        printf("    Fill %d keys: %.3f sec\n", LARGE_FILL_N, fillTime);

        t0 = now_sec();
        KVIterator* iter = NULL;
        rc = kvstore_iterator_create(kv, &iter);
        int count = 0;
        if (rc == KVSTORE_OK) {
            rc = kvstore_iterator_first(iter);
            while (!kvstore_iterator_eof(iter)) {
                count++;
                kvstore_iterator_next(iter);
            }
            kvstore_iterator_close(iter);
        }
        double scanTime = now_sec() - t0;
        printf("    Full scan: %d keys in %.3f sec\n", count, scanTime);
        int scanOk = (count == LARGE_FILL_N);

        int verifyOk = 1;
        srand(42);
        for (i = 0; i < 100; i++) {
            int idx = rand() % LARGE_FILL_N;
            make_key(key, sizeof(key), idx);
            make_value(val, sizeof(val), idx, 100);
            void* got = NULL; int glen = 0;
            rc = kvstore_get(kv, key, (int)strlen(key), &got, &glen);
            if (rc != KVSTORE_OK || glen != (int)strlen(val)
                || memcmp(got, val, glen) != 0) {
                verifyOk = 0;
            }
            if (got) sqliteFree(got);
        }

        t0 = now_sec();
        kvstore_begin(kv, 1);
        for (i = 0; i < LARGE_FILL_N / 2; i++) {
            make_key(key, sizeof(key), i);
            kvstore_delete(kv, key, (int)strlen(key));
        }
        kvstore_commit(kv);
        double delTime = now_sec() - t0;
        printf("    Bulk delete %d keys: %.3f sec\n", LARGE_FILL_N / 2, delTime);

        int halfOk = 1;
        for (i = 0; i < 50; i++) {
            int idx = LARGE_FILL_N / 2 + (rand() % (LARGE_FILL_N / 2));
            make_key(key, sizeof(key), idx);
            make_value(val, sizeof(val), idx, 100);
            void* got = NULL; int glen = 0;
            rc = kvstore_get(kv, key, (int)strlen(key), &got, &glen);
            if (rc != KVSTORE_OK || glen != (int)strlen(val)
                || memcmp(got, val, glen) != 0) {
                halfOk = 0;
            }
            if (got) sqliteFree(got);
        }

        int goneOk = 1;
        for (i = 0; i < 50; i++) {
            int idx = rand() % (LARGE_FILL_N / 2);
            make_key(key, sizeof(key), idx);
            int exists = 0;
            kvstore_exists(kv, key, (int)strlen(key), &exists);
            if (exists) goneOk = 0;
        }

        char* errMsg = NULL;
        int ic = kvstore_integrity_check(kv, &errMsg);
        int intOk = (ic == KVSTORE_OK);
        if (errMsg) sqliteFree(errMsg);

        ok = (scanOk && verifyOk && halfOk && goneOk && intOk);
        kvstore_close(kv);
    }
    print_result("Large dataset: fill/scan/verify/delete (50K keys)", ok);
    cleanup(DB_FILE);
}

/* ================================================================
** 4. CRASH-RECOVERY SIMULATION
** ================================================================ */

static void test_crash_recovery_uncommitted(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc;

    rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
    if (rc != KVSTORE_OK) { print_result("Crash recovery: uncommitted txn", 0); return; }
    kvstore_put(kv, "safe", 4, "committed_data", 14);
    kvstore_close(kv);

    rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
    if (rc != KVSTORE_OK) { print_result("Crash recovery: uncommitted txn", 0); return; }
    kvstore_begin(kv, 1);
    kvstore_put(kv, "unsafe", 6, "uncommitted_data", 16);
    kvstore_close(kv);

    rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
    if (rc != KVSTORE_OK) { print_result("Crash recovery: uncommitted txn", 0); return; }

    void* got = NULL; int glen = 0;
    rc = kvstore_get(kv, "safe", 4, &got, &glen);
    int safeOk = (rc == KVSTORE_OK && glen == 14 && memcmp(got, "committed_data", 14) == 0);
    if (got) sqliteFree(got);

    int unsafeExists = 0;
    kvstore_exists(kv, "unsafe", 6, &unsafeExists);

    char* errMsg = NULL;
    int ic = kvstore_integrity_check(kv, &errMsg);
    int intOk = (ic == KVSTORE_OK);
    if (errMsg) sqliteFree(errMsg);

    kvstore_close(kv);
    print_result("Crash recovery: uncommitted txn rolled back", safeOk && !unsafeExists && intOk);
    cleanup(DB_FILE);
}

static void test_crash_recovery_wal(void) {
    cleanup(DB_WAL_FILE);
    KVStore* kv = NULL;
    int rc;

    rc = kvstore_open(DB_WAL_FILE, &kv, 0, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) { print_result("Crash recovery WAL: uncommitted txn", 0); return; }
    kvstore_put(kv, "wal_safe", 8, "wal_committed", 13);
    kvstore_close(kv);

    rc = kvstore_open(DB_WAL_FILE, &kv, 0, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) { print_result("Crash recovery WAL: uncommitted txn", 0); return; }
    kvstore_begin(kv, 1);
    kvstore_put(kv, "wal_unsafe", 10, "wal_uncommitted", 15);
    kvstore_close(kv);

    rc = kvstore_open(DB_WAL_FILE, &kv, 0, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) { print_result("Crash recovery WAL: uncommitted txn", 0); return; }

    void* got = NULL; int glen = 0;
    rc = kvstore_get(kv, "wal_safe", 8, &got, &glen);
    int safeOk = (rc == KVSTORE_OK && glen == 13);
    if (got) sqliteFree(got);

    int unsafeExists = 0;
    kvstore_exists(kv, "wal_unsafe", 10, &unsafeExists);

    char* errMsg = NULL;
    int ic = kvstore_integrity_check(kv, &errMsg);
    int intOk = (ic == KVSTORE_OK);
    if (errMsg) sqliteFree(errMsg);

    kvstore_close(kv);
    print_result("Crash recovery WAL: uncommitted txn rolled back", safeOk && !unsafeExists && intOk);
    cleanup(DB_WAL_FILE);
}

/* ================================================================
** 5. RAPID OPEN/CLOSE CYCLE
** ================================================================ */

static void test_rapid_open_close(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc;
    int ok = 1;
    int cycles = 200;

    rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
    if (rc != KVSTORE_OK) { print_result("Rapid open/close (200 cycles)", 0); return; }
    kvstore_put(kv, "persist", 7, "survives", 8);
    kvstore_close(kv);

    int i;
    for (i = 0; i < cycles; i++) {
        rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
        if (rc != KVSTORE_OK) { ok = 0; break; }

        void* got = NULL; int glen = 0;
        rc = kvstore_get(kv, "persist", 7, &got, &glen);
        if (rc != KVSTORE_OK || glen != 8 || memcmp(got, "survives", 8) != 0) {
            ok = 0;
            if (got) sqliteFree(got);
            kvstore_close(kv);
            break;
        }
        if (got) sqliteFree(got);

        if ((i % 50) == 0) {
            char val[32];
            snprintf(val, sizeof(val), "cycle-%d", i);
            kvstore_put(kv, "cycle_key", 9, val, (int)strlen(val));
        }

        kvstore_close(kv);
    }

    if (ok) {
        rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
        if (rc == KVSTORE_OK) {
            char* errMsg = NULL;
            int ic = kvstore_integrity_check(kv, &errMsg);
            if (ic != KVSTORE_OK) ok = 0;
            if (errMsg) sqliteFree(errMsg);
            kvstore_close(kv);
        }
        else {
            ok = 0;
        }
    }

    printf("    %d open/close cycles completed\n", ok ? cycles : i);
    print_result("Rapid open/close (200 cycles)", ok);
    cleanup(DB_FILE);
}

/* ================================================================
** 6. CROSS-CONFIGURATION
** ================================================================ */

static void test_cross_config(void) {
    int modes[] = { KVSTORE_JOURNAL_DELETE, KVSTORE_JOURNAL_WAL };
    const char* dbs[] = { DB_FILE, DB_WAL_FILE };
    const char* names[] = { "DELETE", "WAL" };
    int allOk = 1;
    int m;

    for (m = 0; m < 2; m++) {
        cleanup(dbs[m]);
        KVStore* kv = NULL;
        int rc = kvstore_open(dbs[m], &kv, 0, modes[m]);
        if (rc != KVSTORE_OK) { allOk = 0; continue; }

        const char* k = "xconfig";
        rc = kvstore_put(kv, k, 7, "v1", 2);
        if (rc != KVSTORE_OK) { allOk = 0; kvstore_close(kv); continue; }

        void* got = NULL; int glen = 0;
        rc = kvstore_get(kv, k, 7, &got, &glen);
        if (rc != KVSTORE_OK || glen != 2 || memcmp(got, "v1", 2) != 0) allOk = 0;
        if (got) { sqliteFree(got); } got = NULL;

        rc = kvstore_put(kv, k, 7, "v2-updated", 10);
        if (rc != KVSTORE_OK) allOk = 0;

        rc = kvstore_get(kv, k, 7, &got, &glen);
        if (rc != KVSTORE_OK || glen != 10 || memcmp(got, "v2-updated", 10) != 0) allOk = 0;
        if (got) { sqliteFree(got); } got = NULL;

        rc = kvstore_delete(kv, k, 7);
        if (rc != KVSTORE_OK) allOk = 0;

        int exists = 0;
        kvstore_exists(kv, k, 7, &exists);
        if (exists) allOk = 0;

        rc = kvstore_put(kv, k, 7, "v3-reinsert", 11);
        if (rc != KVSTORE_OK) allOk = 0;

        kvstore_begin(kv, 1);
        kvstore_put(kv, "txn1", 4, "committed", 9);
        kvstore_commit(kv);

        rc = kvstore_get(kv, "txn1", 4, &got, &glen);
        if (rc != KVSTORE_OK || glen != 9) allOk = 0;
        if (got) { sqliteFree(got); } got = NULL;

        kvstore_begin(kv, 1);
        kvstore_put(kv, "txn2", 4, "rolled_back", 11);
        kvstore_rollback(kv);

        rc = kvstore_get(kv, "txn2", 4, &got, &glen);
        if (rc != KVSTORE_NOTFOUND) allOk = 0;
        if (got) { sqliteFree(got); } got = NULL;

        char* errMsg = NULL;
        int ic = kvstore_integrity_check(kv, &errMsg);
        if (ic != KVSTORE_OK) {
            printf("    %s mode integrity FAILED: %s\n", names[m], errMsg ? errMsg : "?");
            allOk = 0;
        }
        if (errMsg) sqliteFree(errMsg);

        kvstore_close(kv);
        cleanup(dbs[m]);
    }

    print_result("Cross-config: CRUD + txn in DELETE & WAL modes", allOk);
}

/* ================================================================
** 7. CONCURRENT STRESS (SIMPLIFIED FOR WINDOWS)
** ================================================================ */

typedef struct {
    const char* db;
    int id;
    int ops;
    int errors;
    int busySkips;
    int succeeded;
    int journalMode;
    int lastErrCode;
} ThreadArg;

static int open_with_retry(const char* db, KVStore** ppKV, int journalMode) {
    int rc, retries = 0;
    do {
        rc = kvstore_open(db, ppKV, 0, journalMode);
        if (rc == KVSTORE_OK) return KVSTORE_OK;
        if (IS_BUSY(rc)) {
            usleep(5000 + (rand() % 10000));
            retries++;
        }
        else {
            return rc;
        }
    } while (retries < 20);
    return rc;
}

static void* writer_thread(void* arg) {
    ThreadArg* ta = (ThreadArg*)arg;
    KVStore* kv = NULL;
    int rc = open_with_retry(ta->db, &kv, ta->journalMode);
    if (rc != KVSTORE_OK) { ta->errors++; ta->lastErrCode = rc; return NULL; }

    int i;
    char key[32], val[64];
    for (i = 0; i < ta->ops; i++) {
        snprintf(key, sizeof(key), "t%d-key-%d", ta->id, i);
        snprintf(val, sizeof(val), "t%d-val-%d", ta->id, i);

        int retries = 0;
        int ok = 0;
        do {
            rc = kvstore_put(kv, key, (int)strlen(key), val, (int)strlen(val));
            if (rc == KVSTORE_OK) { ok = 1; break; }
            if (IS_BUSY(rc) || rc == KVSTORE_LOCKED || rc == SQLITE_PROTOCOL) {
                usleep(1000 + (rand() % 4000));
                retries++;
            }
            else {
                ta->errors++;
                ta->lastErrCode = rc;
                break;
            }
        } while (retries < 100);
        if (ok) {
            ta->succeeded++;
        }
        else if (retries >= 100) {
            ta->busySkips++;
        }
    }
    kvstore_close(kv);
    return NULL;
}

static void* reader_thread(void* arg) {
    ThreadArg* ta = (ThreadArg*)arg;
    KVStore* kv = NULL;
    int rc = open_with_retry(ta->db, &kv, ta->journalMode);
    if (rc != KVSTORE_OK) { ta->errors++; ta->lastErrCode = rc; return NULL; }

    int i;
    for (i = 0; i < ta->ops; i++) {
        KVIterator* iter = NULL;
        rc = kvstore_iterator_create(kv, &iter);
        if (rc == KVSTORE_OK) {
            kvstore_iterator_first(iter);
            int n = 0;
            while (!kvstore_iterator_eof(iter) && n < 10) {
                kvstore_iterator_next(iter);
                n++;
            }
            kvstore_iterator_close(iter);
            ta->succeeded++;
        }
        else if (IS_BUSY(rc) || rc == SQLITE_PROTOCOL) {
            ta->busySkips++;
        }
        else {
            ta->errors++;
            ta->lastErrCode = rc;
        }
    }
    kvstore_close(kv);
    return NULL;
}

static void test_concurrent_stress(void) {
    cleanup(DB_WAL_FILE);

    KVStore* kv = NULL;
    int rc = kvstore_open(DB_WAL_FILE, &kv, 0, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) { print_result("Concurrent stress (WAL, 8 threads)", 0); return; }
    kvstore_put(kv, "seed", 4, "initial", 7);
    kvstore_close(kv);

    pthread_t threads[NUM_THREADS];
    ThreadArg args[NUM_THREADS];
    int i;
    int writerOps = 200;
    int readerOps = 500;

    for (i = 0; i < NUM_THREADS; i++) {
        args[i].db = DB_WAL_FILE;
        args[i].id = i;
        args[i].errors = 0;
        args[i].busySkips = 0;
        args[i].succeeded = 0;
        args[i].lastErrCode = 0;
        args[i].journalMode = KVSTORE_JOURNAL_WAL;
        if (i < 2) {
            args[i].ops = writerOps;
            pthread_create(&threads[i], NULL, writer_thread, &args[i]);
        }
        else {
            args[i].ops = readerOps;
            pthread_create(&threads[i], NULL, reader_thread, &args[i]);
        }
    }
    for (i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    int totalErrors = 0, totalBusy = 0, totalOk = 0;
    for (i = 0; i < NUM_THREADS; i++) {
        totalErrors += args[i].errors;
        totalBusy += args[i].busySkips;
        totalOk += args[i].succeeded;
    }

    usleep(50000);
    rc = open_with_retry(DB_WAL_FILE, &kv, KVSTORE_JOURNAL_WAL);
    int intOk = 0;
    if (rc == KVSTORE_OK) {
        char* errMsg = NULL;
        int ic = kvstore_integrity_check(kv, &errMsg);
        intOk = (ic == KVSTORE_OK);
        if (!intOk) printf("    Integrity FAILED after concurrent stress: %s\n",
            errMsg ? errMsg : "?");
        if (errMsg) sqliteFree(errMsg);

        int verifyOk = 1;
        for (i = 0; i < 2 && verifyOk; i++) {
            char key[32];
            snprintf(key, sizeof(key), "t%d-key-0", i);
            int exists = 0;
            kvstore_exists(kv, key, (int)strlen(key), &exists);
            if (args[i].succeeded > 0 && !exists) verifyOk = 0;
        }

        kvstore_close(kv);
        if (!verifyOk) intOk = 0;
    }

    printf("    %d threads: %d ok, %d busy-skipped, %d real errors, integrity %s\n",
        NUM_THREADS, totalOk, totalBusy, totalErrors, intOk ? "OK" : "FAIL");
    if (totalErrors > 0) {
        for (i = 0; i < NUM_THREADS; i++) {
            if (args[i].errors > 0)
                printf("    thread %d: %d errors (last rc=%d)\n",
                    i, args[i].errors, args[i].lastErrCode);
        }
    }

    int pass = (totalErrors == 0 && intOk && totalOk > 0);
    print_result("Concurrent stress (WAL, 8 threads)", pass);
    cleanup(DB_WAL_FILE);
}

/* ================================================================
** 8. ITERATOR CORRECTNESS
** ================================================================ */

static void test_iterator_correctness(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
    int ok = 0;
    int N = 1000;

    if (rc == KVSTORE_OK) {
        int i;
        char key[32], val[64];
        kvstore_begin(kv, 1);
        for (i = 0; i < N; i++) {
            make_key(key, sizeof(key), i);
            make_value(val, sizeof(val), i, 40);
            kvstore_put(kv, key, (int)strlen(key), val, (int)strlen(val));
        }
        kvstore_commit(kv);

        KVIterator* iter = NULL;
        rc = kvstore_iterator_create(kv, &iter);
        if (rc == KVSTORE_OK) {
            rc = kvstore_iterator_first(iter);
            int count = 0;
            int keyErr = 0;
            while (!kvstore_iterator_eof(iter)) {
                void* ik = NULL; int ikn = 0;
                void* iv = NULL; int ivn = 0;
                kvstore_iterator_key(iter, &ik, &ikn);
                kvstore_iterator_value(iter, &iv, &ivn);
                if (!ik || ikn <= 0) keyErr++;
                if (!iv || ivn <= 0) keyErr++;
                count++;
                kvstore_iterator_next(iter);
            }
            kvstore_iterator_close(iter);
            ok = (count == N && keyErr == 0);
            if (!ok) printf("    Expected %d keys, got %d (errors: %d)\n", N, count, keyErr);
        }

        kvstore_close(kv);
    }
    print_result("Iterator full-scan correctness (1000 keys)", ok);
    cleanup(DB_FILE);
}

static void test_iterator_empty_db(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
    int ok = 0;

    if (rc == KVSTORE_OK) {
        KVIterator* iter = NULL;
        rc = kvstore_iterator_create(kv, &iter);
        if (rc == KVSTORE_OK) {
            kvstore_iterator_first(iter);
            ok = (kvstore_iterator_eof(iter) == 1);
            kvstore_iterator_close(iter);
        }
        kvstore_close(kv);
    }
    print_result("Iterator on empty database", ok);
    cleanup(DB_FILE);
}

/* ================================================================
** 9. TRANSACTION CYCLING
** ================================================================ */

static void test_transaction_cycling(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
    int ok = 0;

    if (rc == KVSTORE_OK) {
        double t0 = now_sec();
        int i;
        char key[32], val[64];
        int errors = 0;

        for (i = 0; i < TXN_CYCLE_N; i++) {
            rc = kvstore_begin(kv, 1);
            if (rc != KVSTORE_OK) { errors++; continue; }

            make_key(key, sizeof(key), i);
            snprintf(val, sizeof(val), "txn-%d", i);
            rc = kvstore_put(kv, key, (int)strlen(key), val, (int)strlen(val));
            if (rc != KVSTORE_OK) { errors++; kvstore_rollback(kv); continue; }

            rc = kvstore_commit(kv);
            if (rc != KVSTORE_OK) errors++;
        }

        double elapsed = now_sec() - t0;
        printf("    %d txn cycles in %.3f sec (%d txn/sec), %d errors\n",
            TXN_CYCLE_N, elapsed, (int)(TXN_CYCLE_N / elapsed), errors);

        make_key(key, sizeof(key), TXN_CYCLE_N - 1);
        void* got = NULL; int glen = 0;
        rc = kvstore_get(kv, key, (int)strlen(key), &got, &glen);
        int lastOk = (rc == KVSTORE_OK && glen > 0);
        if (got) sqliteFree(got);

        char* errMsg = NULL;
        int ic = kvstore_integrity_check(kv, &errMsg);
        int intOk = (ic == KVSTORE_OK);
        if (errMsg) sqliteFree(errMsg);

        ok = (errors == 0 && lastOk && intOk);
        kvstore_close(kv);
    }
    print_result("Transaction cycling stress (5000 begin/commit)", ok);
    cleanup(DB_FILE);
}

static void test_rollback_cycling(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
    int ok = 0;

    if (rc == KVSTORE_OK) {
        kvstore_put(kv, "anchor", 6, "stable", 6);

        int i;
        int errors = 0;
        for (i = 0; i < 1000; i++) {
            rc = kvstore_begin(kv, 1);
            if (rc != KVSTORE_OK) { errors++; continue; }
            kvstore_put(kv, "ephemeral", 9, "gone", 4);
            rc = kvstore_rollback(kv);
            if (rc != KVSTORE_OK) errors++;
        }

        void* got = NULL; int glen = 0;
        rc = kvstore_get(kv, "anchor", 6, &got, &glen);
        int anchorOk = (rc == KVSTORE_OK && glen == 6);
        if (got) sqliteFree(got);

        int ephExists = 0;
        kvstore_exists(kv, "ephemeral", 9, &ephExists);

        char* errMsg = NULL;
        int ic = kvstore_integrity_check(kv, &errMsg);
        int intOk = (ic == KVSTORE_OK);
        if (errMsg) sqliteFree(errMsg);

        ok = (errors == 0 && anchorOk && !ephExists && intOk);
        kvstore_close(kv);
    }
    print_result("Rollback cycling stress (1000 begin/rollback)", ok);
    cleanup(DB_FILE);
}

/* ================================================================
** 10. MIXED WORKLOAD
** ================================================================ */

static void test_mixed_workload(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_WAL);
    int ok = 0;

    if (rc == KVSTORE_OK) {
        srand(12345);
        double t0 = now_sec();
        int i;
        int puts = 0, gets = 0, dels = 0, exists_ops = 0;
        int errors = 0;
        int keySpace = 10000;
        char key[32], val[128];

        kvstore_begin(kv, 1);
        for (i = 0; i < MIXED_OPS_N; i++) {
            int idx = rand() % keySpace;
            int op = rand() % 100;
            make_key(key, sizeof(key), idx);

            if (op < 40) {
                make_value(val, sizeof(val), idx, 60);
                rc = kvstore_put(kv, key, (int)strlen(key), val, (int)strlen(val));
                if (rc != KVSTORE_OK) errors++;
                puts++;
            }
            else if (op < 70) {
                void* got = NULL; int glen = 0;
                rc = kvstore_get(kv, key, (int)strlen(key), &got, &glen);
                if (rc != KVSTORE_OK && rc != KVSTORE_NOTFOUND) errors++;
                if (got) sqliteFree(got);
                gets++;
            }
            else if (op < 85) {
                rc = kvstore_delete(kv, key, (int)strlen(key));
                if (rc != KVSTORE_OK && rc != KVSTORE_NOTFOUND) errors++;
                dels++;
            }
            else {
                int e = 0;
                rc = kvstore_exists(kv, key, (int)strlen(key), &e);
                if (rc != KVSTORE_OK) errors++;
                exists_ops++;
            }

            if ((i % 5000) == 4999) {
                kvstore_commit(kv);
                kvstore_begin(kv, 1);
            }
        }
        kvstore_commit(kv);

        double elapsed = now_sec() - t0;
        printf("    %d ops in %.3f sec (%d ops/sec)\n",
            MIXED_OPS_N, elapsed, (int)(MIXED_OPS_N / elapsed));
        printf("    puts=%d gets=%d dels=%d exists=%d errors=%d\n",
            puts, gets, dels, exists_ops, errors);

        char* errMsg = NULL;
        int ic = kvstore_integrity_check(kv, &errMsg);
        int intOk = (ic == KVSTORE_OK);
        if (errMsg) sqliteFree(errMsg);

        ok = (errors == 0 && intOk);
        kvstore_close(kv);
    }
    print_result("Mixed workload stress (50K random ops, WAL)", ok);
    cleanup(DB_FILE);
}

/* ================================================================
** 11. COLUMN FAMILY STRESS
** ================================================================ */

static void test_cf_stress(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_WAL);
    int ok = 0;

    if (rc == KVSTORE_OK) {
        int nCF = 10;
        KVColumnFamily* cfs[10];
        int i, j;
        char cfname[32], key[32], val[64];

        for (i = 0; i < nCF; i++) {
            snprintf(cfname, sizeof(cfname), "stress_cf_%d", i);
            rc = kvstore_cf_create(kv, cfname, &cfs[i]);
            if (rc != KVSTORE_OK) { print_result("Column family stress (10 CFs)", 0); kvstore_close(kv); return; }
        }

        kvstore_begin(kv, 1);
        for (i = 0; i < nCF; i++) {
            for (j = 0; j < 500; j++) {
                snprintf(key, sizeof(key), "cf%d-key-%d", i, j);
                snprintf(val, sizeof(val), "cf%d-val-%d", i, j);
                rc = kvstore_cf_put(cfs[i], key, (int)strlen(key), val, (int)strlen(val));
                if (rc != KVSTORE_OK) break;
            }
            if (rc != KVSTORE_OK) break;
        }
        kvstore_commit(kv);

        int allCountOk = 1;
        for (i = 0; i < nCF && rc == KVSTORE_OK; i++) {
            KVIterator* iter = NULL;
            rc = kvstore_cf_iterator_create(cfs[i], &iter);
            if (rc != KVSTORE_OK) { allCountOk = 0; break; }
            kvstore_iterator_first(iter);
            int count = 0;
            while (!kvstore_iterator_eof(iter)) {
                count++;
                kvstore_iterator_next(iter);
            }
            kvstore_iterator_close(iter);
            if (count != 500) {
                printf("    CF %d: expected 500, got %d\n", i, count);
                allCountOk = 0;
            }
        }

        int isolOk = 1;
        void* got = NULL; int glen = 0;
        rc = kvstore_cf_get(cfs[1], "cf0-key-0", 9, &got, &glen);
        if (rc != KVSTORE_NOTFOUND) isolOk = 0;
        if (got) sqliteFree(got);

        for (i = 0; i < nCF; i++) kvstore_cf_close(cfs[i]);

        char* errMsg = NULL;
        int ic = kvstore_integrity_check(kv, &errMsg);
        int intOk = (ic == KVSTORE_OK);
        if (errMsg) sqliteFree(errMsg);

        ok = (allCountOk && isolOk && intOk);
        kvstore_close(kv);
    }
    print_result("Column family stress (10 CFs x 500 keys)", ok);
    cleanup(DB_FILE);
}

/* ================================================================
** 12. GROWING VALUE SIZE
** ================================================================ */

static void test_growing_values(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_WAL);
    int ok = 0;

    if (rc == KVSTORE_OK) {
        int sizes[] = { 1, 10, 100, 1000, 4096, 10000, 65536, 262144, 524288 };
        int nSizes = sizeof(sizes) / sizeof(sizes[0]);
        int i;
        int allOk = 1;

        for (i = 0; i < nSizes; i++) {
            int vlen = sizes[i];
            char* val = (char*)malloc(vlen);
            if (!val) { allOk = 0; break; }

            int j;
            for (j = 0; j < vlen; j++) val[j] = 'A' + (j % 26);

            char key[32];
            snprintf(key, sizeof(key), "grow-%d", vlen);
            rc = kvstore_put(kv, key, (int)strlen(key), val, vlen);
            if (rc != KVSTORE_OK) { allOk = 0; free(val); break; }

            void* got = NULL; int glen = 0;
            rc = kvstore_get(kv, key, (int)strlen(key), &got, &glen);
            if (rc != KVSTORE_OK || glen != vlen || memcmp(got, val, vlen) != 0) {
                printf("    FAIL at value size %d\n", vlen);
                allOk = 0;
            }
            if (got) sqliteFree(got);
            free(val);
            if (!allOk) break;
        }

        char* errMsg = NULL;
        int ic = kvstore_integrity_check(kv, &errMsg);
        if (ic != KVSTORE_OK) allOk = 0;
        if (errMsg) sqliteFree(errMsg);

        ok = allOk;
        kvstore_close(kv);
    }
    print_result("Growing value sizes (1B to 512KB)", ok);
    cleanup(DB_FILE);
}

/* ================================================================
** 13. MODE SWITCH PERSISTENCE
** ================================================================ */

static void test_mode_switch_persistence(void) {
    cleanup(DB_FILE);
    KVStore* kv = NULL;
    int rc;
    int ok = 0;

    rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
    if (rc != KVSTORE_OK) { print_result("Mode switch persistence (DELETE<->WAL)", 0); return; }
    kvstore_put(kv, "del_key", 7, "del_value", 9);
    kvstore_close(kv);

    rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) { print_result("Mode switch persistence (DELETE<->WAL)", 0); return; }

    void* got = NULL; int glen = 0;
    rc = kvstore_get(kv, "del_key", 7, &got, &glen);
    int delOk = (rc == KVSTORE_OK && glen == 9 && memcmp(got, "del_value", 9) == 0);
    if (got) { sqliteFree(got); } got = NULL;

    kvstore_put(kv, "wal_key", 7, "wal_value", 9);
    kvstore_close(kv);

    rc = kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_DELETE);
    if (rc != KVSTORE_OK) { print_result("Mode switch persistence (DELETE<->WAL)", 0); return; }

    rc = kvstore_get(kv, "del_key", 7, &got, &glen);
    int dk = (rc == KVSTORE_OK && glen == 9);
    if (got) { sqliteFree(got); } got = NULL;

    rc = kvstore_get(kv, "wal_key", 7, &got, &glen);
    int wk = (rc == KVSTORE_OK && glen == 9 && memcmp(got, "wal_value", 9) == 0);
    if (got) { sqliteFree(got); } got = NULL;

    char* errMsg = NULL;
    int ic = kvstore_integrity_check(kv, &errMsg);
    int intOk = (ic == KVSTORE_OK);
    if (errMsg) sqliteFree(errMsg);

    kvstore_close(kv);
    ok = (delOk && dk && wk && intOk);
    print_result("Mode switch persistence (DELETE<->WAL<->DELETE)", ok);
    cleanup(DB_FILE);
}

/* ================================================================
** MAIN
** ================================================================ */

int main(void) {
#ifdef _WIN32
    /* Enable VT100 color support on Windows 10+ */
    HANDLE hOut = GetStdHandle(STD_OUTPUT_HANDLE);
    DWORD dwMode = 0;
    GetConsoleMode(hOut, &dwMode);
    SetConsoleMode(hOut, dwMode | 0x0004);  /* ENABLE_VIRTUAL_TERMINAL_PROCESSING */
#endif

    printf("\n" CLR_CYAN
        "╔══════════════════════════════════════════════════════════════╗\n"
        "║       SNKV Integration & Stress Test Suite                  ║\n"
        "║                                                              ║\n"
        "║  Modeled after SQLite's storage-engine validation:          ║\n"
        "║   • Edge cases & boundary values                            ║\n"
        "║   • High-volume write storms + integrity checks             ║\n"
        "║   • Large dataset fill/scan/delete                          ║\n"
        "║   • Crash-recovery simulation                               ║\n"
        "║   • Concurrent reader/writer stress                         ║\n"
        "║   • Cross-configuration (DELETE + WAL)                      ║\n"
        "╚══════════════════════════════════════════════════════════════╝\n"
        CLR_RESET "\n");

    printf(CLR_BLUE "=== 1. Edge Cases & Boundary Values ===" CLR_RESET "\n");
    test_edge_empty_value();
    test_edge_binary_key_with_nulls();
    test_edge_single_byte_key();
    test_edge_large_key_value();
    test_edge_overwrite_same_key();
    test_edge_get_nonexistent();
    test_edge_delete_nonexistent();
    test_edge_put_after_delete();

    printf("\n" CLR_BLUE "=== 2. High-Volume Write Storm ===" CLR_RESET "\n");
    test_write_storm(KVSTORE_JOURNAL_DELETE, "Write storm DELETE mode (100K writes + integrity)");
    test_write_storm(KVSTORE_JOURNAL_WAL, "Write storm WAL mode (100K writes + integrity)");

    printf("\n" CLR_BLUE "=== 3. Large Dataset ===" CLR_RESET "\n");
    test_large_dataset();

    printf("\n" CLR_BLUE "=== 4. Crash Recovery Simulation ===" CLR_RESET "\n");
    test_crash_recovery_uncommitted();
    test_crash_recovery_wal();

    printf("\n" CLR_BLUE "=== 5. Open/Close Resilience ===" CLR_RESET "\n");
    test_rapid_open_close();

    printf("\n" CLR_BLUE "=== 6. Cross-Configuration ===" CLR_RESET "\n");
    test_cross_config();
    test_mode_switch_persistence();

    printf("\n" CLR_BLUE "=== 7. Concurrent Stress ===" CLR_RESET "\n");
    test_concurrent_stress();

    printf("\n" CLR_BLUE "=== 8. Iterator Correctness ===" CLR_RESET "\n");
    test_iterator_correctness();
    test_iterator_empty_db();

    printf("\n" CLR_BLUE "=== 9. Transaction Cycling ===" CLR_RESET "\n");
    test_transaction_cycling();
    test_rollback_cycling();

    printf("\n" CLR_BLUE "=== 10. Mixed Workload ===" CLR_RESET "\n");
    test_mixed_workload();

    printf("\n" CLR_BLUE "=== 11. Column Family Stress ===" CLR_RESET "\n");
    test_cf_stress();

    printf("\n" CLR_BLUE "=== 12. Value Size Boundaries ===" CLR_RESET "\n");
    test_growing_values();

    printf("\n========================================\n");
    printf("Test Summary\n");
    printf("========================================\n");
    printf("Total tests:  %d\n", g_passed + g_failed);
    printf(CLR_GREEN "Passed:       %d\n" CLR_RESET, g_passed);
    if (g_failed) printf(CLR_RED);
    printf("Failed:       %d\n" CLR_RESET, g_failed);
    printf("Success rate: %.1f%%\n", g_passed * 100.0 / (g_passed + g_failed));
    printf("========================================\n");

    return g_failed ? 1 : 0;
}