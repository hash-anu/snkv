/*
** Test Suite for KVStore WAL (Write-Ahead Logging) Mode
**
** This test suite validates:
** 1. WAL mode activation and file creation (-wal, -shm)
** 2. Basic CRUD operations in WAL mode
** 3. Transaction commit and rollback under WAL
** 4. WAL recovery (uncommitted transaction rolled back on reopen)
** 5. Data persistence across close/reopen in WAL mode
** 6. Concurrent read/write access (WAL allows readers + 1 writer)
** 7. Column family operations under WAL
** 8. Large payload handling under WAL
** 9. Integrity check under WAL mode
** 10. Mixed-mode: data written in DELETE mode readable after WAL reopen
**
** Compile with the project Makefile:
**   make tests
**
** Run with:
**   ./tests/test_wal
*/

#include "kvstore.h"
#include "sqliteInt.h"
#include "platform_compat.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

/* Test configuration */
#define WAL_DB_FILE      "test_wal.db"
#define WAL_WAL_FILE     "test_wal.db-wal"
#define WAL_SHM_FILE     "test_wal.db-shm"
#define WAL_DB_FILE2     "test_wal2.db"
#define WAL_WAL_FILE2    "test_wal2.db-wal"
#define WAL_SHM_FILE2    "test_wal2.db-shm"
#define NUM_THREADS      8
#define OPS_PER_THREAD   50

/* Color output */
#define C_GREEN  "\033[0;32m"
#define C_RED    "\033[0;31m"
#define C_BLUE   "\033[0;34m"
#define C_RESET  "\033[0m"

/* Test statistics */
static int g_passed = 0;
static int g_failed = 0;
static int g_total  = 0;

static void print_result(const char *name, int passed){
  g_total++;
  if( passed ){
    g_passed++;
    printf("%s[PASS]%s %s\n", C_GREEN, C_RESET, name);
  }else{
    g_failed++;
    printf("%s[FAIL]%s %s\n", C_RED, C_RESET, name);
  }
}

static void print_section(const char *name){
  printf("\n%s=== %s ===%s\n", C_BLUE, name, C_RESET);
}

static int file_exists(const char *path){
  struct stat st;
  return (stat(path, &st) == 0);
}

static void cleanup(void){
  unlink(WAL_DB_FILE);
  unlink(WAL_WAL_FILE);
  unlink(WAL_SHM_FILE);
  unlink(WAL_DB_FILE2);
  unlink(WAL_WAL_FILE2);
  unlink(WAL_SHM_FILE2);
}

/* ================================================================
** TEST 1: WAL file creation
** Opening in WAL mode and performing a write should produce
** a -wal file (and possibly a -shm file) instead of -journal.
** ================================================================ */
static void test_wal_file_creation(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc == KVSTORE_OK ){
    rc = kvstore_begin(pKV, 1);
    if( rc == KVSTORE_OK ){
      const char *k = "wkey";
      const char *v = "wval";
      kvstore_put(pKV, k, 4, v, 4);

      /* WAL file should exist, journal file should NOT */
      int wal_ok  = file_exists(WAL_WAL_FILE);
      int jour_no = !file_exists("test_wal.db-journal");
      kvstore_commit(pKV);
      passed = wal_ok && jour_no;
    }
    kvstore_close(pKV);
  }

  cleanup();
  print_result("WAL file creation on write transaction", passed);
}

/* ================================================================
** TEST 2: Basic CRUD in WAL mode
** ================================================================ */
static void test_wal_basic_crud(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("WAL basic CRUD", 0); return; }

  /* Put */
  const char *key = "greeting";
  const char *val = "hello_wal";
  rc = kvstore_put(pKV, key, (int)strlen(key), val, (int)strlen(val));
  if( rc != KVSTORE_OK ) goto done;

  /* Get */
  void *got = NULL; int glen = 0;
  rc = kvstore_get(pKV, key, (int)strlen(key), &got, &glen);
  if( rc != KVSTORE_OK || !got ) goto done;
  if( glen != (int)strlen(val) || memcmp(got, val, glen) != 0 ){
    sqliteFree(got); goto done;
  }
  sqliteFree(got); got = NULL;

  /* Exists */
  int exists = 0;
  rc = kvstore_exists(pKV, key, (int)strlen(key), &exists);
  if( rc != KVSTORE_OK || !exists ) goto done;

  /* Update */
  const char *val2 = "updated_wal";
  rc = kvstore_put(pKV, key, (int)strlen(key), val2, (int)strlen(val2));
  if( rc != KVSTORE_OK ) goto done;

  rc = kvstore_get(pKV, key, (int)strlen(key), &got, &glen);
  if( rc != KVSTORE_OK || !got ) goto done;
  if( glen != (int)strlen(val2) || memcmp(got, val2, glen) != 0 ){
    sqliteFree(got); goto done;
  }
  sqliteFree(got); got = NULL;

  /* Delete */
  rc = kvstore_delete(pKV, key, (int)strlen(key));
  if( rc != KVSTORE_OK ) goto done;

  rc = kvstore_get(pKV, key, (int)strlen(key), &got, &glen);
  if( rc != KVSTORE_NOTFOUND ) goto done;

  passed = 1;

done:
  kvstore_close(pKV);
  cleanup();
  print_result("WAL basic CRUD operations", passed);
}

/* ================================================================
** TEST 3: Transaction commit under WAL
** ================================================================ */
static void test_wal_commit(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("WAL transaction commit", 0); return; }

  rc = kvstore_begin(pKV, 1);
  if( rc == KVSTORE_OK ){
    int i;
    for(i = 0; i < 20; i++){
      char k[32], v[32];
      snprintf(k, sizeof(k), "tkey_%d", i);
      snprintf(v, sizeof(v), "tval_%d", i);
      kvstore_put(pKV, k, (int)strlen(k), v, (int)strlen(v));
    }
    rc = kvstore_commit(pKV);
    if( rc == KVSTORE_OK ){
      /* Verify all keys are readable after commit */
      int ok = 1;
      for(i = 0; i < 20 && ok; i++){
        char k[32]; void *got = NULL; int glen = 0;
        snprintf(k, sizeof(k), "tkey_%d", i);
        rc = kvstore_get(pKV, k, (int)strlen(k), &got, &glen);
        if( rc != KVSTORE_OK || !got ){ ok = 0; }
        else{ sqliteFree(got); }
      }
      passed = ok;
    }
  }

  kvstore_close(pKV);
  cleanup();
  print_result("WAL transaction commit", passed);
}

/* ================================================================
** TEST 4: Transaction rollback under WAL
** ================================================================ */
static void test_wal_rollback(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("WAL transaction rollback", 0); return; }

  /* Write a baseline value */
  const char *key = "rb_key";
  const char *val1 = "before_rollback";
  kvstore_put(pKV, key, (int)strlen(key), val1, (int)strlen(val1));

  /* Begin txn, overwrite, rollback */
  rc = kvstore_begin(pKV, 1);
  if( rc == KVSTORE_OK ){
    const char *val2 = "after_rollback_SHOULD_NOT_PERSIST";
    kvstore_put(pKV, key, (int)strlen(key), val2, (int)strlen(val2));
    kvstore_rollback(pKV);

    /* Read back – should see original */
    void *got = NULL; int glen = 0;
    rc = kvstore_get(pKV, key, (int)strlen(key), &got, &glen);
    if( rc == KVSTORE_OK && got ){
      passed = (glen == (int)strlen(val1) &&
                memcmp(got, val1, glen) == 0);
      sqliteFree(got);
    }
  }

  kvstore_close(pKV);
  cleanup();
  print_result("WAL transaction rollback", passed);
}

/* ================================================================
** TEST 5: WAL recovery (close without commit)
** ================================================================ */
static void test_wal_recovery(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  /* Phase 1: write committed value, then start uncommitted txn */
  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("WAL recovery simulation", 0); return; }

  const char *key = "recov_key";
  const char *val1 = "committed_val";
  kvstore_put(pKV, key, (int)strlen(key), val1, (int)strlen(val1));

  rc = kvstore_begin(pKV, 1);
  if( rc == KVSTORE_OK ){
    const char *val2 = "uncommitted_val";
    kvstore_put(pKV, key, (int)strlen(key), val2, (int)strlen(val2));
    /* Don't commit – simulate crash */
  }
  kvstore_close(pKV);

  /* Phase 2: reopen – should see committed value */
  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc == KVSTORE_OK ){
    void *got = NULL; int glen = 0;
    rc = kvstore_get(pKV, key, (int)strlen(key), &got, &glen);
    if( rc == KVSTORE_OK && got ){
      passed = (glen == (int)strlen(val1) &&
                memcmp(got, val1, glen) == 0);
      sqliteFree(got);
    }
    kvstore_close(pKV);
  }

  cleanup();
  print_result("WAL recovery simulation", passed);
}

/* ================================================================
** TEST 6: Data persistence across close/reopen in WAL mode
** ================================================================ */
static void test_wal_persistence(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  /* Write data */
  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("WAL data persistence", 0); return; }

  int i;
  for(i = 0; i < 50; i++){
    char k[32], v[64];
    snprintf(k, sizeof(k), "persist_%d", i);
    snprintf(v, sizeof(v), "value_%d_abcdefghij", i);
    kvstore_put(pKV, k, (int)strlen(k), v, (int)strlen(v));
  }
  kvstore_close(pKV);

  /* Reopen and verify */
  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc == KVSTORE_OK ){
    int ok = 1;
    for(i = 0; i < 50 && ok; i++){
      char k[32], expected[64];
      snprintf(k, sizeof(k), "persist_%d", i);
      snprintf(expected, sizeof(expected), "value_%d_abcdefghij", i);
      void *got = NULL; int glen = 0;
      rc = kvstore_get(pKV, k, (int)strlen(k), &got, &glen);
      if( rc != KVSTORE_OK || !got ){
        ok = 0;
      }else{
        if( glen != (int)strlen(expected) || memcmp(got, expected, glen) != 0 ){
          ok = 0;
        }
        sqliteFree(got);
      }
    }
    passed = ok;
    kvstore_close(pKV);
  }

  cleanup();
  print_result("WAL data persistence across reopen", passed);
}

/* ================================================================
** TEST 7: Concurrent readers + writer under WAL
** WAL allows one writer and multiple concurrent readers.
** ================================================================ */

typedef struct {
  KVStore *pKV;
  int thread_id;
  int num_ops;
  int success;
  int errors;
} WalThreadData;

static void *wal_writer_thread(void *arg){
  WalThreadData *d = (WalThreadData*)arg;
  int i;
  for(i = 0; i < d->num_ops; i++){
    char k[64], v[64];
    snprintf(k, sizeof(k), "wt_%d_%d", d->thread_id, i);
    snprintf(v, sizeof(v), "wv_%d_%d", d->thread_id, i);
    int rc = kvstore_put(d->pKV, k, (int)strlen(k), v, (int)strlen(v));
    if( rc == KVSTORE_OK ) d->success++;
    else d->errors++;
  }
  return NULL;
}

static void *wal_reader_thread(void *arg){
  WalThreadData *d = (WalThreadData*)arg;
  int i;
  for(i = 0; i < d->num_ops; i++){
    char k[64];
    /* Read a key that the writer might have written */
    snprintf(k, sizeof(k), "wt_0_%d", i % d->num_ops);
    void *got = NULL; int glen = 0;
    int rc = kvstore_get(d->pKV, k, (int)strlen(k), &got, &glen);
    /* NOTFOUND is acceptable (writer hasn't written it yet) */
    if( rc == KVSTORE_OK || rc == KVSTORE_NOTFOUND ){
      d->success++;
    }else{
      d->errors++;
    }
    if( got ) sqliteFree(got);
    usleep(100);
  }
  return NULL;
}

static void test_wal_concurrent(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("WAL concurrent readers + writer", 0); return; }

  /* 1 writer thread + (NUM_THREADS-1) reader threads */
  pthread_t threads[NUM_THREADS];
  WalThreadData tdata[NUM_THREADS];
  int i;

  for(i = 0; i < NUM_THREADS; i++){
    tdata[i].pKV = pKV;
    tdata[i].thread_id = i;
    tdata[i].num_ops = OPS_PER_THREAD;
    tdata[i].success = 0;
    tdata[i].errors = 0;
  }

  /* Thread 0 is the writer */
  pthread_create(&threads[0], NULL, wal_writer_thread, &tdata[0]);
  /* Remaining are readers */
  for(i = 1; i < NUM_THREADS; i++){
    pthread_create(&threads[i], NULL, wal_reader_thread, &tdata[i]);
  }
  for(i = 0; i < NUM_THREADS; i++){
    pthread_join(threads[i], NULL);
  }

  int total_success = 0, total_errors = 0;
  for(i = 0; i < NUM_THREADS; i++){
    total_success += tdata[i].success;
    total_errors += tdata[i].errors;
  }

  printf("  Writer ops: %d ok, %d err | Reader ops: %d ok, %d err\n",
         tdata[0].success, tdata[0].errors,
         total_success - tdata[0].success, total_errors - tdata[0].errors);

  /* Pass if writer had zero errors and majority of reads succeeded */
  passed = (tdata[0].errors == 0 && total_errors == 0);

  kvstore_close(pKV);
  cleanup();
  print_result("WAL concurrent readers + writer", passed);
}

/* ================================================================
** TEST 8: Column families under WAL mode
** ================================================================ */
static void test_wal_column_families(void){
  KVStore *pKV = NULL;
  KVColumnFamily *pCF1 = NULL, *pCF2 = NULL;
  int rc, passed = 0;

  cleanup();

  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("WAL column families", 0); return; }

  rc = kvstore_cf_create(pKV, "wal_cf_a", &pCF1);
  if( rc != KVSTORE_OK ) goto cf_done;
  rc = kvstore_cf_create(pKV, "wal_cf_b", &pCF2);
  if( rc != KVSTORE_OK ) goto cf_done;

  /* Write different values to the same key in different CFs */
  const char *key = "shared_key";
  const char *va = "value_cf_a";
  const char *vb = "value_cf_b";
  rc = kvstore_cf_put(pCF1, key, (int)strlen(key), va, (int)strlen(va));
  if( rc != KVSTORE_OK ) goto cf_done;
  rc = kvstore_cf_put(pCF2, key, (int)strlen(key), vb, (int)strlen(vb));
  if( rc != KVSTORE_OK ) goto cf_done;

  /* Read back and verify isolation */
  void *g1 = NULL, *g2 = NULL;
  int l1 = 0, l2 = 0;
  rc = kvstore_cf_get(pCF1, key, (int)strlen(key), &g1, &l1);
  if( rc != KVSTORE_OK || !g1 ) goto cf_done;
  rc = kvstore_cf_get(pCF2, key, (int)strlen(key), &g2, &l2);
  if( rc != KVSTORE_OK || !g2 ){ sqliteFree(g1); goto cf_done; }

  passed = (l1 == (int)strlen(va) && memcmp(g1, va, l1) == 0 &&
            l2 == (int)strlen(vb) && memcmp(g2, vb, l2) == 0);

  sqliteFree(g1);
  sqliteFree(g2);

cf_done:
  if( pCF1 ) kvstore_cf_close(pCF1);
  if( pCF2 ) kvstore_cf_close(pCF2);
  kvstore_close(pKV);
  cleanup();
  print_result("WAL column families isolation", passed);
}

/* ================================================================
** TEST 9: Large payload under WAL mode
** ================================================================ */
static void test_wal_large_payload(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("WAL large payload", 0); return; }

  /* Create a 1 MB value */
  int sz = 1024 * 1024;
  char *big = (char*)malloc(sz);
  if( !big ){ kvstore_close(pKV); print_result("WAL large payload", 0); return; }

  int i;
  for(i = 0; i < sz; i++) big[i] = (char)('A' + (i % 26));

  const char *key = "big_wal_key";
  rc = kvstore_put(pKV, key, (int)strlen(key), big, sz);
  if( rc != KVSTORE_OK ){ free(big); kvstore_close(pKV); print_result("WAL large payload", 0); return; }

  /* Read back */
  void *got = NULL; int glen = 0;
  rc = kvstore_get(pKV, key, (int)strlen(key), &got, &glen);
  if( rc == KVSTORE_OK && got && glen == sz ){
    passed = (memcmp(got, big, sz) == 0);
    sqliteFree(got);
  }

  free(big);
  kvstore_close(pKV);
  cleanup();
  print_result("WAL large payload (1 MB)", passed);
}

/* ================================================================
** TEST 10: Integrity check under WAL mode
** ================================================================ */
static void test_wal_integrity(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("WAL integrity check", 0); return; }

  /* Write some data first */
  int i;
  for(i = 0; i < 100; i++){
    char k[32], v[32];
    snprintf(k, sizeof(k), "ic_%d", i);
    snprintf(v, sizeof(v), "iv_%d", i);
    kvstore_put(pKV, k, (int)strlen(k), v, (int)strlen(v));
  }

  char *errMsg = NULL;
  rc = kvstore_integrity_check(pKV, &errMsg);
  if( rc == KVSTORE_OK ){
    passed = 1;
  }else{
    printf("  Integrity error: %s\n", errMsg ? errMsg : "(null)");
    if( errMsg ) sqliteFree(errMsg);
  }

  kvstore_close(pKV);
  cleanup();
  print_result("WAL integrity check", passed);
}

/* ================================================================
** TEST 11: Cross-mode persistence (DELETE -> WAL)
** Data written in DELETE mode should be readable after reopening
** in WAL mode, and vice versa.
** ================================================================ */
static void test_wal_cross_mode(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  /* Phase 1: write data in DELETE (rollback journal) mode */
  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_DELETE);
  if( rc != KVSTORE_OK ){ print_result("Cross-mode DELETE -> WAL", 0); return; }

  const char *key1 = "cross_key1";
  const char *val1 = "written_in_delete_mode";
  kvstore_put(pKV, key1, (int)strlen(key1), val1, (int)strlen(val1));
  kvstore_close(pKV);

  /* Phase 2: reopen in WAL mode, verify old data, write new data */
  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("Cross-mode DELETE -> WAL", 0); return; }

  void *got = NULL; int glen = 0;
  rc = kvstore_get(pKV, key1, (int)strlen(key1), &got, &glen);
  if( rc != KVSTORE_OK || !got ){
    kvstore_close(pKV);
    print_result("Cross-mode DELETE -> WAL", 0);
    return;
  }
  int phase1_ok = (glen == (int)strlen(val1) && memcmp(got, val1, glen) == 0);
  sqliteFree(got); got = NULL;

  const char *key2 = "cross_key2";
  const char *val2 = "written_in_wal_mode";
  kvstore_put(pKV, key2, (int)strlen(key2), val2, (int)strlen(val2));
  kvstore_close(pKV);

  /* Phase 3: reopen in DELETE mode, verify both keys */
  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_DELETE);
  if( rc != KVSTORE_OK ){ print_result("Cross-mode DELETE -> WAL", 0); return; }

  rc = kvstore_get(pKV, key1, (int)strlen(key1), &got, &glen);
  int k1_ok = (rc == KVSTORE_OK && got &&
               glen == (int)strlen(val1) && memcmp(got, val1, glen) == 0);
  if( got ){ sqliteFree(got); } got = NULL;

  rc = kvstore_get(pKV, key2, (int)strlen(key2), &got, &glen);
  int k2_ok = (rc == KVSTORE_OK && got &&
               glen == (int)strlen(val2) && memcmp(got, val2, glen) == 0);
  if( got ) sqliteFree(got);

  passed = phase1_ok && k1_ok && k2_ok;

  kvstore_close(pKV);
  cleanup();
  print_result("Cross-mode persistence (DELETE <-> WAL)", passed);
}

/* ================================================================
** TEST 12: Batch insert performance in WAL mode
** ================================================================ */
static void test_wal_batch_performance(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("WAL batch insert performance", 0); return; }

  int count = 10000;
  struct timespec t0, t1;
  clock_gettime(CLOCK_MONOTONIC, &t0);

  rc = kvstore_begin(pKV, 1);
  if( rc == KVSTORE_OK ){
    int i;
    for(i = 0; i < count; i++){
      char k[32], v[64];
      snprintf(k, sizeof(k), "bp_%d", i);
      snprintf(v, sizeof(v), "batch_value_%d_xxxxxxxx", i);
      rc = kvstore_put(pKV, k, (int)strlen(k), v, (int)strlen(v));
      if( rc != KVSTORE_OK ) break;
    }
    if( rc == KVSTORE_OK ) rc = kvstore_commit(pKV);
  }

  clock_gettime(CLOCK_MONOTONIC, &t1);
  double elapsed = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec)/1e9;
  double ops_sec = (elapsed > 0) ? count / elapsed : 0;

  printf("  WAL batch: %d ops in %.3f sec (%.0f ops/sec)\n", count, elapsed, ops_sec);

  /* Verify a few records */
  if( rc == KVSTORE_OK ){
    int spot_ok = 1;
    int spots[] = {0, 100, 5000, 9999};
    int s;
    for(s = 0; s < 4 && spot_ok; s++){
      char k[32], expected[64];
      snprintf(k, sizeof(k), "bp_%d", spots[s]);
      snprintf(expected, sizeof(expected), "batch_value_%d_xxxxxxxx", spots[s]);
      void *got = NULL; int glen = 0;
      rc = kvstore_get(pKV, k, (int)strlen(k), &got, &glen);
      if( rc != KVSTORE_OK || !got || glen != (int)strlen(expected) ||
          memcmp(got, expected, glen) != 0 ){
        spot_ok = 0;
      }
      if( got ) sqliteFree(got);
    }
    passed = spot_ok;
  }

  kvstore_close(pKV);
  cleanup();
  print_result("WAL batch insert (10k records)", passed);
}

/* ================================================================
** TEST 13: Iterator under WAL mode
** ================================================================ */
static void test_wal_iterator(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("WAL iterator", 0); return; }

  /* Insert known data */
  int i;
  for(i = 0; i < 10; i++){
    char k[32], v[32];
    snprintf(k, sizeof(k), "iter_%02d", i);
    snprintf(v, sizeof(v), "ival_%02d", i);
    kvstore_put(pKV, k, (int)strlen(k), v, (int)strlen(v));
  }

  /* Iterate and count */
  KVIterator *pIter = NULL;
  rc = kvstore_iterator_create(pKV, &pIter);
  if( rc != KVSTORE_OK ){ kvstore_close(pKV); print_result("WAL iterator", 0); return; }

  rc = kvstore_iterator_first(pIter);
  int count = 0;
  while( rc == KVSTORE_OK && !kvstore_iterator_eof(pIter) ){
    void *ik = NULL, *iv = NULL;
    int ikl = 0, ivl = 0;
    kvstore_iterator_key(pIter, &ik, &ikl);
    kvstore_iterator_value(pIter, &iv, &ivl);
    if( ik && iv ) count++;
    rc = kvstore_iterator_next(pIter);
  }
  kvstore_iterator_close(pIter);

  passed = (count == 10);
  if( !passed ) printf("  Expected 10 items, got %d\n", count);

  kvstore_close(pKV);
  cleanup();
  print_result("WAL iterator traversal", passed);
}

/* ================================================================
** TEST 14: SHM file creation and lifecycle
** The -shm file should exist while a WAL connection is active.
** It is cleaned up (along with -wal) when the last connection closes
** because SQLite auto-checkpoints on close.
** ================================================================ */
static void test_wal_shm_file(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("WAL -shm file lifecycle", 0); return; }

  /* After open + WAL activation, both -wal and -shm should exist */
  int wal_after_open = file_exists(WAL_WAL_FILE);
  int shm_after_open = file_exists(WAL_SHM_FILE);

  /* Write data — files should still be present */
  const char *k = "shm_test_key";
  const char *v = "shm_test_val";
  kvstore_put(pKV, k, (int)strlen(k), v, (int)strlen(v));
  int wal_after_write = file_exists(WAL_WAL_FILE);
  int shm_after_write = file_exists(WAL_SHM_FILE);

  /* Close — auto-checkpoint should remove -wal and -shm */
  kvstore_close(pKV);
  int wal_after_close = file_exists(WAL_WAL_FILE);
  int shm_after_close = file_exists(WAL_SHM_FILE);
  int db_after_close  = file_exists(WAL_DB_FILE);

  if( !wal_after_open ) printf("  FAIL: -wal missing after open\n");
  if( !shm_after_open ) printf("  FAIL: -shm missing after open\n");
  if( !wal_after_write ) printf("  FAIL: -wal missing after write\n");
  if( !shm_after_write ) printf("  FAIL: -shm missing after write\n");
  if( wal_after_close ) printf("  FAIL: -wal still present after close\n");
  if( shm_after_close ) printf("  FAIL: -shm still present after close\n");
  if( !db_after_close ) printf("  FAIL: .db missing after close\n");

  passed = wal_after_open && shm_after_open &&
           wal_after_write && shm_after_write &&
           !wal_after_close && !shm_after_close &&
           db_after_close;

  cleanup();
  print_result("WAL -shm file lifecycle", passed);
}

/* ================================================================
** TEST 15: SHM file persists throughout transaction lifecycle
** Verify -shm exists during begin/put/commit cycle.
** ================================================================ */
static void test_wal_shm_during_transaction(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("WAL -shm during transaction", 0); return; }

  rc = kvstore_begin(pKV, 1);
  if( rc != KVSTORE_OK ){ kvstore_close(pKV); print_result("WAL -shm during transaction", 0); return; }

  int shm_after_begin = file_exists(WAL_SHM_FILE);

  int i;
  for(i = 0; i < 100; i++){
    char k[32], v[32];
    snprintf(k, sizeof(k), "shmtx_%d", i);
    snprintf(v, sizeof(v), "shmval_%d", i);
    kvstore_put(pKV, k, (int)strlen(k), v, (int)strlen(v));
  }

  int shm_after_puts = file_exists(WAL_SHM_FILE);

  rc = kvstore_commit(pKV);
  int shm_after_commit = file_exists(WAL_SHM_FILE);

  if( !shm_after_begin ) printf("  FAIL: -shm missing after begin\n");
  if( !shm_after_puts ) printf("  FAIL: -shm missing after puts\n");
  if( !shm_after_commit ) printf("  FAIL: -shm missing after commit\n");

  passed = shm_after_begin && shm_after_puts && shm_after_commit;

  kvstore_close(pKV);
  cleanup();
  print_result("WAL -shm present during transaction", passed);
}

/* ================================================================
** TEST 16: ACID - Atomicity
** All operations in a transaction either commit entirely or not at all.
** ================================================================ */
static void test_wal_acid_atomicity(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("ACID Atomicity (WAL)", 0); return; }

  /* Phase 1: Write 50 keys in a transaction, then rollback */
  rc = kvstore_begin(pKV, 1);
  if( rc != KVSTORE_OK ){ kvstore_close(pKV); print_result("ACID Atomicity (WAL)", 0); return; }

  int i;
  for(i = 0; i < 50; i++){
    char k[32], v[32];
    snprintf(k, sizeof(k), "atom_%d", i);
    snprintf(v, sizeof(v), "atomval_%d", i);
    kvstore_put(pKV, k, (int)strlen(k), v, (int)strlen(v));
  }
  kvstore_rollback(pKV);

  /* Verify NONE of the 50 keys exist */
  int none_exist = 1;
  for(i = 0; i < 50; i++){
    char k[32];
    snprintf(k, sizeof(k), "atom_%d", i);
    int exists = 0;
    kvstore_exists(pKV, k, (int)strlen(k), &exists);
    if( exists ){ none_exist = 0; break; }
  }

  /* Phase 2: Write 50 keys and commit */
  rc = kvstore_begin(pKV, 1);
  if( rc != KVSTORE_OK ){ kvstore_close(pKV); print_result("ACID Atomicity (WAL)", 0); return; }
  for(i = 0; i < 50; i++){
    char k[32], v[32];
    snprintf(k, sizeof(k), "atom_%d", i);
    snprintf(v, sizeof(v), "atomval_%d", i);
    kvstore_put(pKV, k, (int)strlen(k), v, (int)strlen(v));
  }
  rc = kvstore_commit(pKV);
  if( rc != KVSTORE_OK ){ kvstore_close(pKV); print_result("ACID Atomicity (WAL)", 0); return; }

  /* Verify ALL 50 keys exist with correct values */
  int all_exist = 1;
  for(i = 0; i < 50; i++){
    char k[32], expected[32];
    snprintf(k, sizeof(k), "atom_%d", i);
    snprintf(expected, sizeof(expected), "atomval_%d", i);
    void *got = NULL; int glen = 0;
    rc = kvstore_get(pKV, k, (int)strlen(k), &got, &glen);
    if( rc != KVSTORE_OK || !got ||
        glen != (int)strlen(expected) || memcmp(got, expected, glen) != 0 ){
      all_exist = 0;
    }
    if( got ) sqliteFree(got);
    if( !all_exist ) break;
  }

  passed = none_exist && all_exist;
  if( !none_exist ) printf("  FAIL: rolled-back keys still exist\n");
  if( !all_exist ) printf("  FAIL: committed keys not found\n");

  kvstore_close(pKV);
  cleanup();
  print_result("ACID Atomicity (WAL) - all-or-nothing", passed);
}

/* ================================================================
** TEST 17: ACID - Consistency
** After writes and reopens, the database remains internally consistent.
** ================================================================ */
static void test_wal_acid_consistency(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  /* Write data in a transaction */
  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("ACID Consistency (WAL)", 0); return; }

  rc = kvstore_begin(pKV, 1);
  if( rc == KVSTORE_OK ){
    int i;
    for(i = 0; i < 200; i++){
      char k[32], v[64];
      snprintf(k, sizeof(k), "cons_%d", i);
      snprintf(v, sizeof(v), "consistency_value_%d_padding", i);
      kvstore_put(pKV, k, (int)strlen(k), v, (int)strlen(v));
    }
    kvstore_commit(pKV);
  }
  kvstore_close(pKV);

  /* Reopen and run integrity check */
  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("ACID Consistency (WAL)", 0); return; }

  char *errMsg = NULL;
  rc = kvstore_integrity_check(pKV, &errMsg);
  if( rc == KVSTORE_OK ){
    /* Also verify data is readable */
    int ok = 1;
    int i;
    for(i = 0; i < 200 && ok; i++){
      char k[32], expected[64];
      snprintf(k, sizeof(k), "cons_%d", i);
      snprintf(expected, sizeof(expected), "consistency_value_%d_padding", i);
      void *got = NULL; int glen = 0;
      rc = kvstore_get(pKV, k, (int)strlen(k), &got, &glen);
      if( rc != KVSTORE_OK || !got ||
          glen != (int)strlen(expected) || memcmp(got, expected, glen) != 0 ){
        ok = 0;
        printf("  FAIL: key cons_%d mismatch\n", i);
      }
      if( got ) sqliteFree(got);
    }
    passed = ok;
  }else{
    printf("  Integrity error: %s\n", errMsg ? errMsg : "(null)");
    if( errMsg ) sqliteFree(errMsg);
  }

  kvstore_close(pKV);
  cleanup();
  print_result("ACID Consistency (WAL) - integrity after reopen", passed);
}

/* ================================================================
** TEST 18: ACID - Isolation
** Uncommitted writes must not be visible after rollback.
** Phantom rows added in a rolled-back txn must vanish.
** ================================================================ */
static void test_wal_acid_isolation(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("ACID Isolation (WAL)", 0); return; }

  /* Write baseline data */
  const char *key = "iso_key";
  const char *val_v1 = "isolation_v1";
  kvstore_put(pKV, key, (int)strlen(key), val_v1, (int)strlen(val_v1));

  /* Start txn, overwrite, add phantom, then rollback */
  rc = kvstore_begin(pKV, 1);
  if( rc == KVSTORE_OK ){
    const char *val_v2 = "isolation_v2_SHOULD_NOT_PERSIST";
    kvstore_put(pKV, key, (int)strlen(key), val_v2, (int)strlen(val_v2));

    const char *tmp_key = "iso_phantom";
    const char *tmp_val = "phantom_val";
    kvstore_put(pKV, tmp_key, (int)strlen(tmp_key), tmp_val, (int)strlen(tmp_val));

    kvstore_rollback(pKV);
  }

  /* After rollback: original value should be intact */
  void *got = NULL; int glen = 0;
  rc = kvstore_get(pKV, key, (int)strlen(key), &got, &glen);
  int v1_ok = (rc == KVSTORE_OK && got &&
               glen == (int)strlen(val_v1) &&
               memcmp(got, val_v1, glen) == 0);
  if( got ){ sqliteFree(got); } got = NULL;

  /* Phantom key should not exist */
  int phantom_exists = 0;
  kvstore_exists(pKV, "iso_phantom", 11, &phantom_exists);

  passed = v1_ok && !phantom_exists;
  if( !v1_ok ) printf("  FAIL: original value corrupted after rollback\n");
  if( phantom_exists ) printf("  FAIL: phantom key persists after rollback\n");

  kvstore_close(pKV);
  cleanup();
  print_result("ACID Isolation (WAL) - rollback isolation", passed);
}

/* ================================================================
** TEST 19: ACID - Durability
** Committed data must survive close/reopen.
** Uncommitted data must NOT survive close/reopen.
** ================================================================ */
static void test_wal_acid_durability(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  /* Phase 1: Write committed data */
  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("ACID Durability (WAL)", 0); return; }

  int i;
  for(i = 0; i < 100; i++){
    char k[32], v[64];
    snprintf(k, sizeof(k), "dur_%d", i);
    snprintf(v, sizeof(v), "durable_value_%d_xyz", i);
    kvstore_put(pKV, k, (int)strlen(k), v, (int)strlen(v));
  }

  /* Phase 2: Start uncommitted txn with overwrites + phantom keys */
  rc = kvstore_begin(pKV, 1);
  if( rc == KVSTORE_OK ){
    for(i = 0; i < 100; i++){
      char k[32], v[64];
      snprintf(k, sizeof(k), "dur_%d", i);
      snprintf(v, sizeof(v), "UNCOMMITTED_OVERWRITE_%d", i);
      kvstore_put(pKV, k, (int)strlen(k), v, (int)strlen(v));
    }
    for(i = 100; i < 150; i++){
      char k[32], v[32];
      snprintf(k, sizeof(k), "dur_%d", i);
      snprintf(v, sizeof(v), "phantom_%d", i);
      kvstore_put(pKV, k, (int)strlen(k), v, (int)strlen(v));
    }
    /* Close WITHOUT committing */
  }
  kvstore_close(pKV);

  /* Phase 3: Reopen and verify committed values survived */
  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("ACID Durability (WAL)", 0); return; }

  int committed_ok = 1;
  for(i = 0; i < 100; i++){
    char k[32], expected[64];
    snprintf(k, sizeof(k), "dur_%d", i);
    snprintf(expected, sizeof(expected), "durable_value_%d_xyz", i);
    void *got = NULL; int glen = 0;
    rc = kvstore_get(pKV, k, (int)strlen(k), &got, &glen);
    if( rc != KVSTORE_OK || !got ||
        glen != (int)strlen(expected) || memcmp(got, expected, glen) != 0 ){
      committed_ok = 0;
      printf("  FAIL: dur_%d mismatch after reopen\n", i);
    }
    if( got ) sqliteFree(got);
    if( !committed_ok ) break;
  }

  /* Phantom keys (100-149) should NOT exist */
  int phantoms_gone = 1;
  for(i = 100; i < 150; i++){
    char k[32];
    snprintf(k, sizeof(k), "dur_%d", i);
    int exists = 0;
    kvstore_exists(pKV, k, (int)strlen(k), &exists);
    if( exists ){
      phantoms_gone = 0;
      printf("  FAIL: phantom dur_%d survived close\n", i);
      break;
    }
  }

  passed = committed_ok && phantoms_gone;

  kvstore_close(pKV);
  cleanup();
  print_result("ACID Durability (WAL) - survive close/reopen", passed);
}

/* ================================================================
** TEST 20: ACID - Crash atomicity (close mid-transaction)
** Begin a write transaction, write data, close without commit.
** Reopen: baseline intact, uncommitted writes gone.
** ================================================================ */
static void test_wal_acid_crash_atomicity(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  /* Write baseline data */
  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("ACID Crash Atomicity (WAL)", 0); return; }

  const char *base_key = "crash_base";
  const char *base_val = "base_value";
  kvstore_put(pKV, base_key, (int)strlen(base_key), base_val, (int)strlen(base_val));
  kvstore_close(pKV);

  /* Reopen, start txn, write, close without commit */
  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("ACID Crash Atomicity (WAL)", 0); return; }

  rc = kvstore_begin(pKV, 1);
  if( rc == KVSTORE_OK ){
    const char *new_val = "CRASHED_OVERWRITE";
    kvstore_put(pKV, base_key, (int)strlen(base_key), new_val, (int)strlen(new_val));

    int i;
    for(i = 0; i < 50; i++){
      char k[32], v[32];
      snprintf(k, sizeof(k), "crash_%d", i);
      snprintf(v, sizeof(v), "crashval_%d", i);
      kvstore_put(pKV, k, (int)strlen(k), v, (int)strlen(v));
    }
    /* DO NOT commit */
  }
  kvstore_close(pKV);

  /* Reopen: baseline should be original, crash_ keys should not exist */
  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("ACID Crash Atomicity (WAL)", 0); return; }

  void *got = NULL; int glen = 0;
  rc = kvstore_get(pKV, base_key, (int)strlen(base_key), &got, &glen);
  int base_ok = (rc == KVSTORE_OK && got &&
                 glen == (int)strlen(base_val) &&
                 memcmp(got, base_val, glen) == 0);
  if( got ) sqliteFree(got);

  int crash_keys_gone = 1;
  int i;
  for(i = 0; i < 50; i++){
    char k[32];
    snprintf(k, sizeof(k), "crash_%d", i);
    int exists = 0;
    kvstore_exists(pKV, k, (int)strlen(k), &exists);
    if( exists ){
      crash_keys_gone = 0;
      printf("  FAIL: crash_%d survived uncommitted txn\n", i);
      break;
    }
  }

  passed = base_ok && crash_keys_gone;
  if( !base_ok ) printf("  FAIL: base value corrupted after crash recovery\n");

  kvstore_close(pKV);
  cleanup();
  print_result("ACID Crash Atomicity (WAL) - uncommitted data gone", passed);
}

/* ================================================================
** TEST 21: Statistics tracking under WAL
** ================================================================ */
static void test_wal_statistics(void){
  KVStore *pKV = NULL;
  int rc, passed = 0;

  cleanup();

  rc = kvstore_open(WAL_DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ print_result("WAL statistics", 0); return; }

  kvstore_put(pKV, "s1", 2, "v1", 2);
  kvstore_put(pKV, "s2", 2, "v2", 2);
  kvstore_put(pKV, "s3", 2, "v3", 2);

  void *got = NULL; int glen = 0;
  kvstore_get(pKV, "s1", 2, &got, &glen);
  if( got ) sqliteFree(got);
  kvstore_get(pKV, "s2", 2, &got, &glen);
  if( got ) sqliteFree(got);

  kvstore_delete(pKV, "s3", 2);

  KVStoreStats stats;
  rc = kvstore_stats(pKV, &stats);
  if( rc == KVSTORE_OK ){
    printf("  Stats: puts=%llu, gets=%llu, deletes=%llu\n",
           (unsigned long long)stats.nPuts,
           (unsigned long long)stats.nGets,
           (unsigned long long)stats.nDeletes);
    passed = (stats.nPuts == 3 && stats.nGets == 2 && stats.nDeletes == 1);
  }

  kvstore_close(pKV);
  cleanup();
  print_result("WAL statistics tracking", passed);
}

/* ================================================================
** Main
** ================================================================ */
int main(void){
  printf("\n");
  printf("========================================\n");
  printf("KVStore WAL Mode Test Suite\n");
  printf("========================================\n");

  srand((unsigned)time(NULL));

  print_section("WAL File Behavior");
  test_wal_file_creation();
  test_wal_shm_file();
  test_wal_shm_during_transaction();

  print_section("WAL CRUD & Transactions");
  test_wal_basic_crud();
  test_wal_commit();
  test_wal_rollback();
  test_wal_recovery();
  test_wal_persistence();

  print_section("WAL Concurrency");
  test_wal_concurrent();

  print_section("WAL Column Families");
  test_wal_column_families();

  print_section("WAL Data Handling");
  test_wal_large_payload();
  test_wal_iterator();
  test_wal_batch_performance();

  print_section("WAL Integrity & Cross-mode");
  test_wal_integrity();
  test_wal_cross_mode();
  test_wal_statistics();

  print_section("WAL ACID Compliance");
  test_wal_acid_atomicity();
  test_wal_acid_consistency();
  test_wal_acid_isolation();
  test_wal_acid_durability();
  test_wal_acid_crash_atomicity();

  printf("\n");
  printf("========================================\n");
  printf("Test Summary\n");
  printf("========================================\n");
  printf("Total tests:  %d\n", g_total);
  printf("%sPassed:       %d%s\n", C_GREEN, g_passed, C_RESET);
  printf("%sFailed:       %d%s\n", g_failed > 0 ? C_RED : C_RESET,
         g_failed, C_RESET);
  printf("Success rate: %.1f%%\n",
         g_total > 0 ? (100.0 * g_passed / g_total) : 0.0);
  printf("========================================\n\n");

  cleanup();
  return (g_failed == 0) ? 0 : 1;
}
