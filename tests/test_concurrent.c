/*
** test_concurrent.c
**
** Concurrent read/write correctness test for SNKV in WAL mode.
**
** 10 writer threads each insert unique key-value pairs.
** 10 reader threads verify that every committed key has the correct value.
**
** Each writer owns a unique key range so there are no write conflicts
** between writers — only the WAL-level serialisation of commits.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "kvstore.h"
#include "platform_compat.h"

#define DB_FILE         "test_concurrent.db"
#define NUM_WRITERS     10
#define NUM_READERS     10
#define KEYS_PER_WRITER 10000
#define BATCH_SIZE      1000     /* commit every N puts */
#define READER_ROUNDS   5        /* each reader scans the full DB this many times */

#define IS_BUSY(rc)  (((rc)&0xFF)==SQLITE_BUSY || ((rc)&0xFF)==SQLITE_LOCKED)

#define CLR_GREEN  "\033[0;32m"
#define CLR_RED    "\033[0;31m"
#define CLR_BLUE   "\033[0;34m"
#define CLR_CYAN   "\033[0;36m"
#define CLR_RESET  "\033[0m"

static int g_passed = 0;
static int g_failed = 0;

static void print_result(const char *name, int ok) {
    if (ok) {
        g_passed++;
        printf(CLR_GREEN "[PASS]" CLR_RESET " %s\n", name);
    } else {
        g_failed++;
        printf(CLR_RED   "[FAIL]" CLR_RESET " %s\n", name);
    }
}

static void cleanup(const char *db) {
    char buf[512];
    remove(db);
    snprintf(buf, sizeof(buf), "%s-journal", db);
    remove(buf);
    snprintf(buf, sizeof(buf), "%s-wal", db);
    remove(buf);
    snprintf(buf, sizeof(buf), "%s-shm", db);
    remove(buf);
}

/* ---- key / value helpers ---- */

static void make_key(char *buf, int buflen, int writer_id, int seq) {
    snprintf(buf, buflen, "w%02d-key-%06d", writer_id, seq);
}

static void make_value(char *buf, int buflen, int writer_id, int seq) {
    snprintf(buf, buflen, "w%02d-val-%06d-payload", writer_id, seq);
}

/* ---- open with busy retry ---- */

static int open_with_retry(const char *db, KVStore **ppKV) {
    int rc, retries = 0;
    do {
        rc = kvstore_open(db, ppKV, KVSTORE_JOURNAL_WAL);
        if (rc == KVSTORE_OK) return KVSTORE_OK;
        if (IS_BUSY(rc)) {
            usleep(5000 + (rand() % 10000));
            retries++;
        } else {
            return rc;
        }
    } while (retries < 30);
    return rc;
}

/* ================================================================
** Writer thread
**
** Each writer inserts KEYS_PER_WRITER unique keys in batches.
** Keys are prefixed with the writer id so no two writers collide.
** ================================================================ */

typedef struct {
    int id;
    int keys_written;
    int busy_retries;
    int errors;
    int last_err;
} WriterResult;

static void *writer_thread(void *arg) {
    WriterResult *res = (WriterResult *)arg;
    KVStore *kv = NULL;
    int rc, i;
    char key[64], value[64];

    rc = open_with_retry(DB_FILE, &kv);
    if (rc != KVSTORE_OK) {
        res->errors++;
        res->last_err = rc;
        return NULL;
    }

    for (i = 0; i < KEYS_PER_WRITER; i++) {
        /* Begin a transaction at the start of each batch */
        if (i % BATCH_SIZE == 0) {
            int retries = 0;
            do {
                rc = kvstore_begin(kv, 1);
                if (rc == KVSTORE_OK) break;
                if (IS_BUSY(rc) || rc == KVSTORE_LOCKED || rc == SQLITE_PROTOCOL) {
                    usleep(5000 + (rand() % 20000));
                    retries++;
                    res->busy_retries++;
                } else {
                    res->errors++;
                    res->last_err = rc;
                    goto done;
                }
            } while (retries < 500);
            if (rc != KVSTORE_OK) {
                res->errors++;
                res->last_err = rc;
                goto done;
            }
        }

        make_key(key, sizeof(key), res->id, i);
        make_value(value, sizeof(value), res->id, i);

        rc = kvstore_put(kv, key, (int)strlen(key), value, (int)strlen(value));
        if (rc == KVSTORE_OK) {
            res->keys_written++;
        } else if (IS_BUSY(rc) || rc == KVSTORE_LOCKED) {
            /* Busy during put — rollback and retry the whole batch */
            res->busy_retries++;
            kvstore_rollback(kv);
            i = (i / BATCH_SIZE) * BATCH_SIZE - 1; /* restart batch */
            res->keys_written -= (i + 1) % BATCH_SIZE; /* undo count */
            if (res->keys_written < 0) res->keys_written = 0;
            usleep(5000 + (rand() % 20000));
            continue;
        } else {
            res->errors++;
            res->last_err = rc;
            kvstore_rollback(kv);
            goto done;
        }

        /* Commit at the end of each batch or at the very end */
        if ((i + 1) % BATCH_SIZE == 0 || i == KEYS_PER_WRITER - 1) {
            int retries = 0;
            do {
                rc = kvstore_commit(kv);
                if (rc == KVSTORE_OK) break;
                if (IS_BUSY(rc) || rc == KVSTORE_LOCKED || rc == SQLITE_PROTOCOL) {
                    usleep(5000 + (rand() % 20000));
                    retries++;
                    res->busy_retries++;
                } else {
                    res->errors++;
                    res->last_err = rc;
                    kvstore_rollback(kv);
                    goto done;
                }
            } while (retries < 500);
            if (rc != KVSTORE_OK) {
                res->errors++;
                res->last_err = rc;
                goto done;
            }
        }
    }

done:
    kvstore_close(kv);
    return NULL;
}

/* ================================================================
** Reader thread
**
** Reads random keys and verifies the value matches.
** Only checks keys that should have been written (by any writer).
** A missing key is acceptable (writer may not have committed yet),
** but a wrong value is a failure.
** ================================================================ */

typedef struct {
    int id;
    int reads_ok;
    int reads_missing;   /* key not yet written — acceptable */
    int reads_wrong;     /* value mismatch — failure */
    int busy_retries;
    int errors;
    int last_err;
} ReaderResult;

static void *reader_thread(void *arg) {
    ReaderResult *res = (ReaderResult *)arg;
    KVStore *kv = NULL;
    int rc, r, i;
    char key[64], expected_value[64];

    rc = open_with_retry(DB_FILE, &kv);
    if (rc != KVSTORE_OK) {
        res->errors++;
        res->last_err = rc;
        return NULL;
    }

    for (r = 0; r < READER_ROUNDS; r++) {
        for (i = 0; i < NUM_WRITERS * KEYS_PER_WRITER; i++) {
            int writer_id = i / KEYS_PER_WRITER;
            int seq = i % KEYS_PER_WRITER;
            void *got_val = NULL;
            int got_len = 0;

            make_key(key, sizeof(key), writer_id, seq);
            make_value(expected_value, sizeof(expected_value), writer_id, seq);

            rc = kvstore_get(kv, key, (int)strlen(key), &got_val, &got_len);
            if (rc == KVSTORE_OK) {
                /* Verify value correctness */
                int exp_len = (int)strlen(expected_value);
                if (got_len == exp_len && memcmp(got_val, expected_value, exp_len) == 0) {
                    res->reads_ok++;
                } else {
                    res->reads_wrong++;
                }
                sqliteFree(got_val);
            } else if (rc == KVSTORE_NOTFOUND) {
                /* Writer hasn't committed this key yet — acceptable */
                res->reads_missing++;
            } else if (IS_BUSY(rc)) {
                res->busy_retries++;
            } else {
                res->errors++;
                res->last_err = rc;
            }
        }
    }

    kvstore_close(kv);
    return NULL;
}

/* ================================================================
** Test 1: Concurrent writers + readers, then verify all data
** ================================================================ */

static void test_concurrent_write_read(void) {
    printf("\n" CLR_BLUE "=== Concurrent Write/Read Correctness (WAL) ===" CLR_RESET "\n");
    printf("  %d writers x %d keys, %d readers x %d rounds\n\n",
           NUM_WRITERS, KEYS_PER_WRITER, NUM_READERS, READER_ROUNDS);

    cleanup(DB_FILE);

    /* Create the database in WAL mode with a seed record */
    KVStore *kv = NULL;
    int rc = kvstore_open(DB_FILE, &kv, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) {
        printf("  Failed to create DB: rc=%d\n", rc);
        print_result("Concurrent write/read", 0);
        return;
    }
    kvstore_close(kv);

    /* Spawn threads */
    int total_threads = NUM_WRITERS + NUM_READERS;
    pthread_t threads[NUM_WRITERS + NUM_READERS];
    WriterResult writers[NUM_WRITERS];
    ReaderResult readers[NUM_READERS];
    int i;

    memset(writers, 0, sizeof(writers));
    memset(readers, 0, sizeof(readers));

    for (i = 0; i < NUM_WRITERS; i++) {
        writers[i].id = i;
    }
    for (i = 0; i < NUM_READERS; i++) {
        readers[i].id = i;
    }

    /* Launch writers and readers concurrently */
    for (i = 0; i < NUM_WRITERS; i++) {
        pthread_create(&threads[i], NULL, writer_thread, &writers[i]);
    }
    for (i = 0; i < NUM_READERS; i++) {
        pthread_create(&threads[NUM_WRITERS + i], NULL, reader_thread, &readers[i]);
    }

    /* Wait for all threads */
    for (i = 0; i < total_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    /* Summarise writer results */
    int total_written = 0, total_w_errors = 0, total_w_busy = 0;
    for (i = 0; i < NUM_WRITERS; i++) {
        total_written += writers[i].keys_written;
        total_w_errors += writers[i].errors;
        total_w_busy += writers[i].busy_retries;
    }
    printf("  Writers: %d/%d keys written, %d busy retries, %d errors\n",
           total_written, NUM_WRITERS * KEYS_PER_WRITER, total_w_busy, total_w_errors);

    /* Summarise reader results */
    int total_ok = 0, total_missing = 0, total_wrong = 0;
    int total_r_errors = 0, total_r_busy = 0;
    for (i = 0; i < NUM_READERS; i++) {
        total_ok += readers[i].reads_ok;
        total_missing += readers[i].reads_missing;
        total_wrong += readers[i].reads_wrong;
        total_r_errors += readers[i].errors;
        total_r_busy += readers[i].busy_retries;
    }
    printf("  Readers: %d correct, %d not-yet-written, %d WRONG, %d busy, %d errors\n",
           total_ok, total_missing, total_wrong, total_r_busy, total_r_errors);

    /* Check 1: no writer errors */
    int writers_ok = (total_w_errors == 0 &&
                      total_written == NUM_WRITERS * KEYS_PER_WRITER);
    print_result("All writers completed without errors", writers_ok);

    /* Check 2: no value mismatches during concurrent reads */
    int readers_ok = (total_wrong == 0 && total_r_errors == 0);
    print_result("No value corruption during concurrent reads", readers_ok);

    /* ---- Post-write verification: read everything back ---- */
    printf("\n  " CLR_CYAN "Post-write verification..." CLR_RESET "\n");

    kv = NULL;
    rc = open_with_retry(DB_FILE, &kv);
    if (rc != KVSTORE_OK) {
        printf("  Failed to reopen DB: rc=%d\n", rc);
        print_result("Post-write verification", 0);
        cleanup(DB_FILE);
        return;
    }

    int verify_ok = 0, verify_wrong = 0, verify_missing = 0;
    for (i = 0; i < NUM_WRITERS * KEYS_PER_WRITER; i++) {
        int writer_id = i / KEYS_PER_WRITER;
        int seq = i % KEYS_PER_WRITER;
        char key[64], expected[64];
        void *got_val = NULL;
        int got_len = 0;

        make_key(key, sizeof(key), writer_id, seq);
        make_value(expected, sizeof(expected), writer_id, seq);

        rc = kvstore_get(kv, key, (int)strlen(key), &got_val, &got_len);
        if (rc == KVSTORE_OK) {
            int exp_len = (int)strlen(expected);
            if (got_len == exp_len && memcmp(got_val, expected, exp_len) == 0) {
                verify_ok++;
            } else {
                verify_wrong++;
                if (verify_wrong <= 3) {
                    printf("    MISMATCH key=%s expected_len=%d got_len=%d\n",
                           key, exp_len, got_len);
                }
            }
            sqliteFree(got_val);
        } else {
            verify_missing++;
            if (verify_missing <= 3) {
                printf("    MISSING key=%s rc=%d\n", key, rc);
            }
        }
    }

    printf("  Verified: %d/%d correct, %d wrong, %d missing\n",
           verify_ok, NUM_WRITERS * KEYS_PER_WRITER, verify_wrong, verify_missing);

    int all_present = (verify_ok == NUM_WRITERS * KEYS_PER_WRITER &&
                       verify_wrong == 0 && verify_missing == 0);
    print_result("All keys present and correct after writes", all_present);

    /* Integrity check */
    char *errMsg = NULL;
    rc = kvstore_integrity_check(kv, &errMsg);
    int integrity_ok = (rc == KVSTORE_OK);
    if (!integrity_ok) {
        printf("    Integrity check failed: %s\n", errMsg ? errMsg : "unknown");
    }
    if (errMsg) sqliteFree(errMsg);
    print_result("Database integrity check", integrity_ok);

    kvstore_close(kv);
    cleanup(DB_FILE);
}

/* ==================== Main ==================== */
int main(void) {
    printf("\n" CLR_CYAN);
    printf("════════════════════════════════════════════════════════\n");
    printf("  SNKV Concurrent Read/Write Test (WAL Mode)\n");
    printf("════════════════════════════════════════════════════════\n");
    printf(CLR_RESET);

    srand((unsigned)time(NULL));

    test_concurrent_write_read();

    printf("\n════════════════════════════════════════════════════════\n");
    printf("  Results: " CLR_GREEN "%d passed" CLR_RESET ", "
           CLR_RED "%d failed" CLR_RESET "\n", g_passed, g_failed);
    printf("════════════════════════════════════════════════════════\n\n");

    return g_failed > 0 ? 1 : 0;
}
