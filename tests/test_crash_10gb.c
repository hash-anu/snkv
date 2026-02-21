/*
** test_crash_10gb.c
**
** 10 GB kill-9 crash-safety stress test for SNKV.
**
** Usage:
**   test_crash_10gb write  <db>   continuous writer (safe to kill -9 anytime)
**   test_crash_10gb verify <db>   post-crash verifier
**   test_crash_10gb run    <db>   orchestrate kill cycles + final verify (POSIX only)
**
** Self-validating design -- no external truth file needed.
** Every value (data and mark) is a unique deterministic 4 KB pseudo-random
** byte sequence derived from (txid, key-index) via a Knuth LCG.
**
** Key scheme:
**   data:NNNNNNNNNN:MMMMM  ->  generateValue(txid=N, kidx=M)
**   mark:NNNNNNNNNN        ->  generateValue(txid=N, kidx=BATCH_SIZE)
**
** The mark key is written LAST inside each transaction.
** WAL atomically rolls it back if the process is killed mid-write, so its
** presence is proof that all BATCH_SIZE data keys committed atomically.
**
** Verification checks:
**   1. All mark values match the expected deterministic bytes (no corruption)
**   2. Every data key belongs to a committed transaction (no orphaned partial data)
**   3. Every data key value matches its expected bytes (no byte-level corruption)
**   4. Every committed transaction has exactly BATCH_SIZE data keys (no partial commit)
**   5. SQLite integrity check passes
**
** Disk space: TARGET_SIZE_GB + ~1 GB WAL headroom required.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "kvstore.h"
#include "platform_compat.h"

#ifndef _WIN32
#  include <signal.h>
#  include <sys/wait.h>
#endif

/* ------------------------------------------------------------------ */
/* Constants                                                            */
/* ------------------------------------------------------------------ */

#define BATCH_SIZE      100     /* data keys per transaction                  */
#define VALUE_SIZE      4096    /* bytes for every value -- data AND mark keys */
#define TARGET_SIZE_GB  10      /* write until DB file reaches this size (GB) */
#define KILL_CYCLES     5       /* kill-9 + verify cycles in run mode         */
#define KILL_SLEEP_MIN  3       /* minimum seconds before each kill           */
#define KILL_SLEEP_MAX  15      /* maximum seconds before each kill           */
#define PROGRESS_EVERY  100     /* print progress line every N transactions   */
#define MAX_TXNS        60000   /* upper bound for committed[] / perTxCount[] */
#define KEY_MAX         32      /* max key string length including NUL        */

/* ------------------------------------------------------------------ */
/* Console colors                                                       */
/* ------------------------------------------------------------------ */

#define CLR_GREEN  "\033[0;32m"
#define CLR_RED    "\033[0;31m"
#define CLR_BLUE   "\033[0;34m"
#define CLR_CYAN   "\033[0;36m"
#define CLR_RESET  "\033[0m"

/* ------------------------------------------------------------------ */
/* generateValue                                                        */
/* ------------------------------------------------------------------ */

/*
** Fill buf[0..VALUE_SIZE) with a deterministic pseudo-random sequence
** derived from (txid, kidx) using a Knuth multiplicative LCG.
**
** For data keys use kidx in [0, BATCH_SIZE).
** For mark keys use kidx == BATCH_SIZE (one slot past last data key).
**
** The same (txid, kidx) pair always produces identical bytes on any
** platform -- no seed file or shared state required.
*/
static void generateValue(char *buf, long long txid, int kidx) {
    unsigned int seed =
        (unsigned int)((txid * 2654435761ULL) ^ ((unsigned int)kidx * 40503u));
    int i;
    for (i = 0; i < VALUE_SIZE; i++) {
        seed = seed * 1664525u + 1013904223u;
        buf[i] = (char)(seed >> 24);
    }
}

/* ------------------------------------------------------------------ */
/* Small utilities                                                      */
/* ------------------------------------------------------------------ */

/* Return file size in bytes, or -1 on error. */
static long long getFileSize(const char *zPath) {
#ifdef _WIN32
    struct _stat st;
    if (_stat(zPath, &st) != 0) return -1LL;
#else
    struct stat st;
    if (stat(zPath, &st) != 0) return -1LL;
#endif
    return (long long)st.st_size;
}

/* Remove db file and its WAL/SHM/journal sidecars. */
static void cleanupDB(const char *db) {
    char buf[512];
    remove(db);
    snprintf(buf, sizeof(buf), "%s-wal",     db); remove(buf);
    snprintf(buf, sizeof(buf), "%s-shm",     db); remove(buf);
    snprintf(buf, sizeof(buf), "%s-journal", db); remove(buf);
}

/* Comparator for qsort / bsearch on long long arrays. */
static int cmpLL(const void *a, const void *b) {
    long long x = *(const long long *)a;
    long long y = *(const long long *)b;
    return (x > y) - (x < y);
}

/* Monotonic seconds. */
static double nowSec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

/* ------------------------------------------------------------------ */
/* findLastTxid                                                         */
/* ------------------------------------------------------------------ */

/*
** Scan the mark: prefix and return the highest committed txid, or -1
** if no mark keys exist (fresh database).
**
** Since mark keys are zero-padded to 10 digits they sort correctly in
** lexicographic order, so the last entry in a forward scan is also the
** numerically largest txid. We iterate all of them and keep the max
** (the extra cost is negligible -- there are at most ~26 K mark keys
** even for a 10 GB database).
*/
static long long findLastTxid(KVStore *kv) {
    KVIterator *iter = NULL;
    long long maxTxid = -1LL;
    int rc;

    rc = kvstore_prefix_iterator_create(kv, "mark:", 5, &iter);
    if (rc != KVSTORE_OK || iter == NULL) return -1LL;

    while (!kvstore_iterator_eof(iter)) {
        void *pKey = NULL;
        int   nKey = 0;
        kvstore_iterator_key(iter, &pKey, &nKey);

        /* Key format: "mark:NNNNNNNNNN" -- 5 + 10 = 15 chars minimum */
        if (pKey && nKey >= 15) {
            char digits[16];
            memcpy(digits, (char *)pKey + 5, 10);
            digits[10] = '\0';
            long long txid = atoll(digits);
            if (txid > maxTxid) maxTxid = txid;
        }
        kvstore_iterator_next(iter);
    }
    kvstore_iterator_close(iter);
    return maxTxid;
}

/* ------------------------------------------------------------------ */
/* WRITE MODE                                                           */
/* ------------------------------------------------------------------ */

/*
** Write transactions continuously until the database file reaches
** TARGET_SIZE_GB. Safe to kill -9 at any point -- WAL will roll back
** any in-flight transaction on the next open.
**
** Returns 0 on normal completion, 1 on fatal error.
*/
static int doWrite(const char *zDb) {
    KVStoreConfig cfg;
    KVStore *kv = NULL;
    int rc;

    memset(&cfg, 0, sizeof(cfg));
    cfg.journalMode = KVSTORE_JOURNAL_WAL;

    rc = kvstore_open_v2(zDb, &kv, &cfg);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "[write] ERROR: cannot open '%s' (rc=%d): %s\n",
                zDb, rc, kvstore_errmsg(kv));
        return 1;
    }

    /* Resume from the transaction after the last committed one. */
    long long startTxid = findLastTxid(kv) + 1;
    long long txid = startTxid;
    long long targetBytes =
        (long long)TARGET_SIZE_GB * 1024LL * 1024LL * 1024LL;

    printf("[write] Resuming from txid %lld  (target %.0f GB)\n",
           txid, (double)TARGET_SIZE_GB);
    fflush(stdout);

    double tStart    = nowSec();
    double tLastProg = tStart;
    long long txSinceLast = 0;

    /* Stack-allocated value buffer -- VALUE_SIZE = 4 KB, safe on stack. */
    char val[VALUE_SIZE];
    char key[KEY_MAX];

    while (1) {
        long long sz = getFileSize(zDb);
        if (sz >= targetBytes) {
            printf("[write] Target size reached (%.2f GB). Done.\n",
                   (double)sz / (1024.0 * 1024.0 * 1024.0));
            break;
        }

        rc = kvstore_begin(kv, 1);
        if (rc != KVSTORE_OK) {
            fprintf(stderr, "[write] ERROR: begin failed (rc=%d)\n", rc);
            kvstore_close(kv);
            return 1;
        }

        /* --- BATCH_SIZE data keys ----------------------------------- */
        int kidx;
        for (kidx = 0; kidx < BATCH_SIZE; kidx++) {
            int klen = snprintf(key, KEY_MAX,
                                "data:%010lld:%05d", txid, kidx);
            generateValue(val, txid, kidx);
            rc = kvstore_put(kv, key, klen, val, VALUE_SIZE);
            if (rc != KVSTORE_OK) {
                fprintf(stderr,
                        "[write] ERROR: put data:%010lld:%05d failed (rc=%d)\n",
                        txid, kidx, rc);
                kvstore_rollback(kv);
                kvstore_close(kv);
                return 1;
            }
        }

        /* --- Mark key (LAST in transaction) ------------------------- */
        /* Its presence proves all BATCH_SIZE data keys committed.      */
        {
            int klen = snprintf(key, KEY_MAX, "mark:%010lld", txid);
            generateValue(val, txid, BATCH_SIZE);   /* unique per txid */
            rc = kvstore_put(kv, key, klen, val, VALUE_SIZE);
            if (rc != KVSTORE_OK) {
                fprintf(stderr,
                        "[write] ERROR: put mark:%010lld failed (rc=%d)\n",
                        txid, rc);
                kvstore_rollback(kv);
                kvstore_close(kv);
                return 1;
            }
        }

        rc = kvstore_commit(kv);
        if (rc != KVSTORE_OK) {
            fprintf(stderr,
                    "[write] ERROR: commit txid %lld failed (rc=%d)\n",
                    txid, rc);
            kvstore_close(kv);
            return 1;
        }

        txid++;
        txSinceLast++;

        if (txSinceLast >= PROGRESS_EVERY) {
            double now  = nowSec();
            double dt   = now - tLastProg;
            double speed = (dt > 0.0) ? (double)txSinceLast / dt : 0.0;
            long long sz2 = getFileSize(zDb);
            printf("[write] txid=%-10lld  size=%6.2f GB  speed=%5.0f txn/s\n",
                   txid,
                   (sz2 >= 0) ? (double)sz2 / (1024.0 * 1024.0 * 1024.0) : 0.0,
                   speed);
            fflush(stdout);
            tLastProg     = now;
            txSinceLast   = 0;
        }
    }

    double totalElapsed = nowSec() - tStart;
    long long nWritten  = txid - startTxid;
    printf("[write] Wrote %lld transactions in %.1f sec (%.0f txn/s)\n",
           nWritten, totalElapsed,
           (totalElapsed > 0.0) ? (double)nWritten / totalElapsed : 0.0);
    fflush(stdout);

    kvstore_close(kv);
    return 0;
}

/* ------------------------------------------------------------------ */
/* VERIFY MODE                                                          */
/* ------------------------------------------------------------------ */

/*
** Open the database and run four verification passes:
**
**   Phase 1 -- scan mark: prefix, validate each mark value, build committed[]
**   Phase 2 -- scan data: prefix, validate each value, detect orphaned data
**   Phase 3 -- check every committed txid has exactly BATCH_SIZE data keys
**   Phase 4 -- SQLite integrity_check
**
** Returns 0 on full pass, 1 if any check fails.
*/
static int doVerify(const char *zDb) {
    KVStoreConfig cfg;
    KVStore *kv = NULL;
    int rc;

    memset(&cfg, 0, sizeof(cfg));
    cfg.journalMode = KVSTORE_JOURNAL_WAL;

    printf("\n[verify] Opening '%s'...\n", zDb);
    rc = kvstore_open_v2(zDb, &kv, &cfg);
    if (rc != KVSTORE_OK) {
        printf(CLR_RED "[verify] FAIL: cannot open database (rc=%d)\n"
               CLR_RESET, rc);
        return 1;
    }
    printf("[verify] Database opened successfully.\n\n");

    /* Allocate bookkeeping arrays once. */
    long long *committed  = (long long *)malloc(MAX_TXNS * sizeof(long long));
    int       *perTxCount = (int *)      malloc(MAX_TXNS * sizeof(int));
    if (!committed || !perTxCount) {
        fprintf(stderr, "[verify] ERROR: out of memory\n");
        free(committed);
        free(perTxCount);
        kvstore_close(kv);
        return 1;
    }
    memset(perTxCount, 0, MAX_TXNS * sizeof(int));

    /* Counters. */
    long long markCorrupt      = 0;
    long long orphanErrors     = 0;
    long long valueErrors      = 0;
    long long incompleteErrors = 0;
    long long totalDataKeys    = 0;
    int       nCommitted       = 0;

    /* Stack buffer for expected value generation. */
    char expected[VALUE_SIZE];

    /* ----------------------------------------------------------------
    ** Phase 1: scan mark: keys, validate values, build committed[]
    **
    ** "mark:" keys sort AFTER all "data:" keys ('m' > 'd'), so a
    ** forward prefix scan on "mark:" visits only markers.
    ** We build committed[] here; Phase 2 uses bsearch against it.
    ** ---------------------------------------------------------------- */
    printf("[verify] Phase 1: scanning commit markers...\n");
    {
        KVIterator *iter = NULL;
        rc = kvstore_prefix_iterator_create(kv, "mark:", 5, &iter);
        if (rc != KVSTORE_OK) {
            printf(CLR_RED
                   "[verify] FAIL: cannot create mark iterator (rc=%d)\n"
                   CLR_RESET, rc);
            free(committed); free(perTxCount); kvstore_close(kv);
            return 1;
        }

        while (!kvstore_iterator_eof(iter)) {
            void *pKey = NULL; int nKey = 0;
            void *pVal = NULL; int nVal = 0;

            kvstore_iterator_key  (iter, &pKey, &nKey);
            kvstore_iterator_value(iter, &pVal, &nVal);

            /* Key format: "mark:NNNNNNNNNN" = 15 chars minimum. */
            if (!pKey || nKey < 15) {
                kvstore_iterator_next(iter);
                continue;
            }

            char digits[16];
            memcpy(digits, (char *)pKey + 5, 10);
            digits[10] = '\0';
            long long txid = atoll(digits);

            /* Validate the mark value -- it must equal generateValue(txid, BATCH_SIZE). */
            generateValue(expected, txid, BATCH_SIZE);
            if (nVal != VALUE_SIZE ||
                memcmp(pVal, expected, VALUE_SIZE) != 0) {
                printf(CLR_RED
                       "[verify] CORRUPT MARK: mark:%010lld value wrong "
                       "(got %d bytes)\n" CLR_RESET, txid, nVal);
                markCorrupt++;
                /* Do not add to committed[] -- treat as uncommitted. */
            } else {
                if (nCommitted < MAX_TXNS) {
                    committed[nCommitted++] = txid;
                } else {
                    fprintf(stderr,
                            "[verify] WARNING: MAX_TXNS (%d) exceeded; "
                            "increase MAX_TXNS and recompile.\n", MAX_TXNS);
                }
            }

            kvstore_iterator_next(iter);
        }
        kvstore_iterator_close(iter);
    }

    /* Sort so bsearch works in Phase 2 and Phase 3. */
    qsort(committed, (size_t)nCommitted, sizeof(long long), cmpLL);

    printf("[verify] Phase 1 done: %d committed transactions, "
           "%lld corrupt marks.\n\n",
           nCommitted, markCorrupt);

    /* ----------------------------------------------------------------
    ** Phase 2: scan data: keys, validate values and txid membership
    **
    ** For each "data:NNNNNNNNNN:MMMMM" key:
    **   a) Regenerate expected value and compare byte-by-byte.
    **   b) Confirm txid is in committed[] (no orphaned partial data).
    **   c) Increment perTxCount[idx] so Phase 3 can check completeness.
    ** ---------------------------------------------------------------- */
    printf("[verify] Phase 2: validating %lld expected data keys...\n",
           (long long)nCommitted * BATCH_SIZE);
    {
        KVIterator *iter = NULL;
        rc = kvstore_prefix_iterator_create(kv, "data:", 5, &iter);
        if (rc != KVSTORE_OK) {
            printf(CLR_RED
                   "[verify] FAIL: cannot create data iterator (rc=%d)\n"
                   CLR_RESET, rc);
            free(committed); free(perTxCount); kvstore_close(kv);
            return 1;
        }

        while (!kvstore_iterator_eof(iter)) {
            void *pKey = NULL; int nKey = 0;
            void *pVal = NULL; int nVal = 0;

            kvstore_iterator_key  (iter, &pKey, &nKey);
            kvstore_iterator_value(iter, &pVal, &nVal);

            /*
            ** Key format: "data:NNNNNNNNNN:MMMMM"
            **              0    5          15 16  20
            ** Total length always 21 for valid keys.
            */
            if (!pKey || nKey < 21) {
                kvstore_iterator_next(iter);
                continue;
            }

            char txbuf[16], kidxbuf[8];
            memcpy(txbuf,   (char *)pKey + 5,  10); txbuf[10]  = '\0';
            memcpy(kidxbuf, (char *)pKey + 16,  5); kidxbuf[5] = '\0';
            long long txid = atoll(txbuf);
            int       kidx = atoi(kidxbuf);

            /* a) Value correctness */
            generateValue(expected, txid, kidx);
            if (nVal != VALUE_SIZE ||
                memcmp(pVal, expected, VALUE_SIZE) != 0) {
                printf(CLR_RED
                       "[verify] CORRUPT DATA: data:%010lld:%05d "
                       "value wrong (got %d bytes)\n" CLR_RESET,
                       txid, kidx, nVal);
                valueErrors++;
            }

            /* b) Orphan check -- txid must be in committed[] */
            long long *found = (long long *)bsearch(
                &txid, committed, (size_t)nCommitted,
                sizeof(long long), cmpLL);
            if (!found) {
                printf(CLR_RED
                       "[verify] ORPHAN: data:%010lld:%05d -- "
                       "txid not in committed set\n" CLR_RESET,
                       txid, kidx);
                orphanErrors++;
            } else {
                /* c) Track per-transaction key count for Phase 3 */
                int idx = (int)(found - committed);
                perTxCount[idx]++;
            }

            totalDataKeys++;
            if (totalDataKeys % 10000 == 0) {
                printf("[verify]   ... %lld data keys checked\r",
                       totalDataKeys);
                fflush(stdout);
            }

            kvstore_iterator_next(iter);
        }
        kvstore_iterator_close(iter);
    }
    /* Clear the \r progress line. */
    printf("[verify] Phase 2 done: %lld data keys checked.          \n\n",
           totalDataKeys);

    /* ----------------------------------------------------------------
    ** Phase 3: completeness -- every committed txid must have exactly
    **          BATCH_SIZE data keys.  Any deviation means a partial
    **          transaction somehow made it into the committed state.
    ** ---------------------------------------------------------------- */
    printf("[verify] Phase 3: checking transaction completeness...\n");
    {
        int i;
        for (i = 0; i < nCommitted; i++) {
            if (perTxCount[i] != BATCH_SIZE) {
                printf(CLR_RED
                       "[verify] INCOMPLETE: txid %010lld has %d/%d data keys\n"
                       CLR_RESET,
                       committed[i], perTxCount[i], BATCH_SIZE);
                incompleteErrors++;
            }
        }
    }
    printf("[verify] Phase 3 done: %lld incomplete transactions.\n\n",
           incompleteErrors);

    /* ----------------------------------------------------------------
    ** Phase 4: SQLite integrity check
    ** ---------------------------------------------------------------- */
    printf("[verify] Phase 4: running integrity check...\n");
    char *errMsg = NULL;
    int   intOk  = 0;
    rc = kvstore_integrity_check(kv, &errMsg);
    if (rc == KVSTORE_OK) {
        intOk = 1;
        printf("[verify] Phase 4: " CLR_GREEN "PASS\n" CLR_RESET);
    } else {
        printf(CLR_RED "[verify] Phase 4: FAIL -- %s\n" CLR_RESET,
               errMsg ? errMsg : "unknown error");
    }
    if (errMsg) sqliteFree(errMsg);

    /* ----------------------------------------------------------------
    ** Summary
    ** ---------------------------------------------------------------- */
    int failed = (markCorrupt || orphanErrors || valueErrors ||
                  incompleteErrors || !intOk);

    printf("\n");
    printf("==================================================\n");
    printf("  Verification Summary\n");
    printf("==================================================\n");
    printf("  Committed transactions  : %d\n",   nCommitted);
    printf("  Corrupt mark values     : %lld\n", markCorrupt);
    printf("  Total data keys checked : %lld\n", totalDataKeys);
    printf("  Orphaned data keys      : %lld\n", orphanErrors);
    printf("  Value mismatches        : %lld\n", valueErrors);
    printf("  Incomplete transactions : %lld\n", incompleteErrors);
    printf("  Integrity check         : %s\n",   intOk ? "PASS" : "FAIL");
    printf("--------------------------------------------------\n");
    if (!failed) {
        printf("  " CLR_GREEN "Result: PASS" CLR_RESET "\n");
    } else {
        printf("  " CLR_RED   "Result: FAIL" CLR_RESET "\n");
    }
    printf("==================================================\n\n");

    free(committed);
    free(perTxCount);
    kvstore_close(kv);
    return failed ? 1 : 0;
}

/* ------------------------------------------------------------------ */
/* RUN MODE -- orchestrate kill cycles (POSIX only)                     */
/* ------------------------------------------------------------------ */

#ifndef _WIN32
/*
** Fork a writer child, kill it after a random interval, then fork a
** verifier child and check its exit code. Repeat KILL_CYCLES times.
** After all cycles pass, write the database to TARGET_SIZE_GB and run
** a final full verification.
*/
static int doRun(const char *zDb, const char *argv0) {
    srand((unsigned int)time(NULL));

    printf(CLR_CYAN
           "==============================================================\n"
           "  SNKV %d GB Kill-9 Crash Safety Test -- %d Kill Cycles\n"
           "==============================================================\n"
           CLR_RESET "\n",
           TARGET_SIZE_GB, KILL_CYCLES);

    int cycle;
    for (cycle = 1; cycle <= KILL_CYCLES; cycle++) {
        int sleepSec = KILL_SLEEP_MIN +
                       (rand() % (KILL_SLEEP_MAX - KILL_SLEEP_MIN + 1));

        printf(CLR_BLUE
               "=== Cycle %d/%d: starting writer (kill in %d s) ===\n"
               CLR_RESET, cycle, KILL_CYCLES, sleepSec);
        fflush(stdout);

        /* Fork writer child. */
        pid_t wPid = fork();
        if (wPid < 0) {
            perror("[run] fork (writer)");
            return 1;
        }
        if (wPid == 0) {
            execl(argv0, argv0, "write", zDb, NULL);
            _exit(1);   /* execl should not return */
        }

        sleep((unsigned int)sleepSec);
        kill(wPid, SIGKILL);        /* simulate power failure */
        waitpid(wPid, NULL, 0);
        printf("[run]  Writer killed after %d seconds.\n\n", sleepSec);
        fflush(stdout);

        /* Fork verifier child. */
        printf(CLR_BLUE "=== Cycle %d/%d: verifying ===\n" CLR_RESET,
               cycle, KILL_CYCLES);
        fflush(stdout);

        pid_t vPid = fork();
        if (vPid < 0) {
            perror("[run] fork (verifier)");
            return 1;
        }
        if (vPid == 0) {
            execl(argv0, argv0, "verify", zDb, NULL);
            _exit(1);
        }

        int status = 0;
        waitpid(vPid, &status, 0);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            printf(CLR_RED
                   "==============================================================\n"
                   "  FAIL: Verification failed at cycle %d/%d\n"
                   "==============================================================\n"
                   CLR_RESET "\n",
                   cycle, KILL_CYCLES);
            return 1;
        }
        printf(CLR_GREEN "=== Cycle %d/%d PASSED ===\n\n" CLR_RESET,
               cycle, KILL_CYCLES);
        fflush(stdout);
    }

    /* All kill cycles passed.  Write to target size then do final verify. */
    printf(CLR_BLUE "=== Final write: filling to %d GB ===\n" CLR_RESET,
           TARGET_SIZE_GB);
    fflush(stdout);
    doWrite(zDb);

    printf(CLR_BLUE "\n=== Final verify: checking %d GB database ===\n"
           CLR_RESET "\n", TARGET_SIZE_GB);
    fflush(stdout);
    int vRc = doVerify(zDb);

    if (vRc == 0) {
        printf(CLR_GREEN
               "==============================================================\n"
               "  ALL %d KILL CYCLES PASSED -- %d GB database verified clean\n"
               "==============================================================\n"
               CLR_RESET "\n",
               KILL_CYCLES, TARGET_SIZE_GB);
    } else {
        printf(CLR_RED
               "==============================================================\n"
               "  FINAL VERIFICATION FAILED\n"
               "==============================================================\n"
               CLR_RESET "\n");
    }
    return vRc;
}
#endif /* !_WIN32 */

/* ------------------------------------------------------------------ */
/* main                                                                 */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv) {
    enable_ansi_colors();

    if (argc < 3) {
        fprintf(stderr,
                "\nSNKV 10 GB Kill-9 Crash Safety Test\n\n"
                "Usage:\n"
                "  %s write  <db>   continuous writer (kill -9 safe at any point)\n"
                "  %s verify <db>   post-crash verifier\n"
#ifndef _WIN32
                "  %s run    <db>   orchestrate %d kill cycles + final verify\n"
#endif
                "\n"
                "Notes:\n"
                "  - Requires ~%d GB of free disk space (DB + WAL).\n"
                "  - 'run' mode is POSIX-only (Linux / macOS).\n"
                "  - 'write' and 'verify' work on all platforms.\n\n",
                argv[0], argv[0],
#ifndef _WIN32
                argv[0], KILL_CYCLES,
#endif
                TARGET_SIZE_GB + 1);
        return 1;
    }

    const char *mode = argv[1];
    const char *zDb  = argv[2];

    if (strcmp(mode, "write") == 0) {
        return doWrite(zDb);

    } else if (strcmp(mode, "verify") == 0) {
        return doVerify(zDb);

    } else if (strcmp(mode, "clean") == 0) {
        /* Convenience: remove the database and sidecar files. */
        cleanupDB(zDb);
        printf("[clean] Removed '%s' and sidecars.\n", zDb);
        return 0;

#ifndef _WIN32
    } else if (strcmp(mode, "run") == 0) {
        return doRun(zDb, argv[0]);
#endif

    } else {
        fprintf(stderr, "Unknown mode '%s'. Use write, verify, or run.\n",
                mode);
        return 1;
    }
}
