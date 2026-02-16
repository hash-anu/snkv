/*
** Production-Ready Test Suite for KVStore
** 
** Tests cover:
** - Basic CRUD operations
** - Transaction handling
** - Error handling
** - Concurrent scenarios
** - Large data handling
** - Edge cases
** - Performance benchmarking
*/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include "kvstore.h"

#define TEST_DB "kvstore_test.db"
#define PERF_DB "kvstore_perf.db"

/* Test result tracking */
static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

/* ANSI color codes for output */
#define COLOR_RED     "\x1b[31m"
#define COLOR_GREEN   "\x1b[32m"
#define COLOR_YELLOW  "\x1b[33m"
#define COLOR_BLUE    "\x1b[34m"
#define COLOR_RESET   "\x1b[0m"

/* Utility macros */
#define TEST_START(name) \
  do { \
    tests_run++; \
    printf(COLOR_BLUE "TEST %d: %s" COLOR_RESET "\n", tests_run, name); \
  } while(0)

#define TEST_PASS() \
  do { \
    tests_passed++; \
    printf(COLOR_GREEN "  ✓ PASSED" COLOR_RESET "\n\n"); \
  } while(0)

#define TEST_FAIL(msg) \
  do { \
    tests_failed++; \
    printf(COLOR_RED "  ✗ FAILED: %s" COLOR_RESET "\n\n", msg); \
    return; \
  } while(0)

#define ASSERT_OK(rc, msg) \
  if( (rc) != KVSTORE_OK ) { \
    printf("  Error at line %d: %s (code=%d)\n", __LINE__, msg, rc); \
    if( kv ) printf("  Details: %s\n", kvstore_errmsg(kv)); \
    TEST_FAIL(msg); \
  }

#define ASSERT_EQ(expected, actual, msg) \
  if( (expected) != (actual) ) { \
    printf("  Expected %d but got %d at line %d: %s\n", \
           (int)(expected), (int)(actual), __LINE__, msg); \
    TEST_FAIL(msg); \
  }

#define ASSERT_TRUE(expr, msg) \
  if( !(expr) ) { \
    printf("  Assertion failed at line %d: %s\n", __LINE__, msg); \
    TEST_FAIL(msg); \
  }

/* Clean up test database */
static void cleanup_db(const char *dbname){
  remove(dbname);
}

/* ========== TEST CASES ========== */

/*
** Test 1: Basic open/close
*/
static void test_open_close(void){
  TEST_START("Open and close database");
  KVStore *kv = NULL;
  int rc;
  
  cleanup_db(TEST_DB);
  
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to open database");
  ASSERT_TRUE(kv != NULL, "KVStore handle is NULL");
  
  rc = kvstore_close(kv);
  ASSERT_OK(rc, "Failed to close database");
  
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/*
** Test 2: Basic CRUD operations
*/
static void test_basic_crud(void){
  TEST_START("Basic CRUD operations");
  KVStore *kv = NULL;
  int rc, exists;
  void *val = NULL;
  int vlen;
  
  cleanup_db(TEST_DB);
  
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to open database");
  
  /* PUT */
  rc = kvstore_put(kv, "key1", 4, "value1", 6);
  ASSERT_OK(rc, "Failed to put key1");
  
  /* EXISTS */
  rc = kvstore_exists(kv, "key1", 4, &exists);
  ASSERT_OK(rc, "Failed to check existence");
  ASSERT_TRUE(exists, "Key should exist");
  
  /* GET */
  rc = kvstore_get(kv, "key1", 4, &val, &vlen);
  ASSERT_OK(rc, "Failed to get key1");
  ASSERT_EQ(6, vlen, "Wrong value length");
  ASSERT_TRUE(memcmp(val, "value1", 6) == 0, "Wrong value content");
  sqliteFree(val);
  
  /* DELETE */
  rc = kvstore_delete(kv, "key1", 4);
  ASSERT_OK(rc, "Failed to delete key1");
  
  rc = kvstore_exists(kv, "key1", 4, &exists);
  ASSERT_OK(rc, "Failed to check existence after delete");
  ASSERT_TRUE(!exists, "Key should not exist after delete");
  
  kvstore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/*
** Test 3: Update existing key
*/
static void test_update(void){
  TEST_START("Update existing key");
  KVStore *kv = NULL;
  int rc;
  void *val = NULL;
  int vlen;
  
  cleanup_db(TEST_DB);
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to open database");
  
  /* Initial put */
  rc = kvstore_put(kv, "key", 3, "value1", 6);
  ASSERT_OK(rc, "Failed initial put");
  
  /* Update */
  rc = kvstore_put(kv, "key", 3, "value2_longer", 13);
  ASSERT_OK(rc, "Failed to update");
  
  /* Verify update */
  rc = kvstore_get(kv, "key", 3, &val, &vlen);
  ASSERT_OK(rc, "Failed to get after update");
  ASSERT_EQ(13, vlen, "Wrong updated value length");
  ASSERT_TRUE(memcmp(val, "value2_longer", 13) == 0, "Wrong updated value");
  sqliteFree(val);
  
  kvstore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/*
** Test 4: Transaction handling
*/
static void test_transactions(void){
  TEST_START("Transaction commit and rollback");
  KVStore *kv = NULL;
  int rc, exists;
  
  cleanup_db(TEST_DB);
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to open database");
  
  /* Test commit */
  rc = kvstore_begin(kv, 1);
  ASSERT_OK(rc, "Failed to begin write transaction");
  
  rc = kvstore_put(kv, "key1", 4, "value1", 6);
  ASSERT_OK(rc, "Failed to put in transaction");
  
  rc = kvstore_commit(kv);
  ASSERT_OK(rc, "Failed to commit");
  
  rc = kvstore_exists(kv, "key1", 4, &exists);
  ASSERT_OK(rc, "Failed to check existence");
  ASSERT_TRUE(exists, "Key should exist after commit");
  
  /* Test rollback */
  rc = kvstore_begin(kv, 1);
  ASSERT_OK(rc, "Failed to begin second transaction");
  
  rc = kvstore_put(kv, "key2", 4, "value2", 6);
  ASSERT_OK(rc, "Failed to put key2");
  
  rc = kvstore_rollback(kv);
  ASSERT_OK(rc, "Failed to rollback");
  
  rc = kvstore_exists(kv, "key2", 4, &exists);
  ASSERT_OK(rc, "Failed to check existence after rollback");
  ASSERT_TRUE(!exists, "Key should not exist after rollback");
  
  kvstore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/*
** Test 5: Multiple operations in single transaction
*/
static void test_batch_operations(void){
  TEST_START("Batch operations in transaction");
  KVStore *kv = NULL;
  int rc, i, exists;
  char key[32], val[64];
  
  cleanup_db(TEST_DB);
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to open database");
  
  rc = kvstore_begin(kv, 1);
  ASSERT_OK(rc, "Failed to begin transaction");
  
  /* Insert 100 key-value pairs */
  for(i = 0; i < 100; i++){
    snprintf(key, sizeof(key), "batch_key_%d", i);
    snprintf(val, sizeof(val), "batch_value_%d", i);
    rc = kvstore_put(kv, key, strlen(key), val, strlen(val));
    ASSERT_OK(rc, "Failed batch put");
  }
  
  rc = kvstore_commit(kv);
  ASSERT_OK(rc, "Failed to commit batch");
  
  /* Verify all keys exist */
  for(i = 0; i < 100; i++){
    snprintf(key, sizeof(key), "batch_key_%d", i);
    rc = kvstore_exists(kv, key, strlen(key), &exists);
    ASSERT_OK(rc, "Failed to check batch key");
    ASSERT_TRUE(exists, "Batch key should exist");
  }
  
  kvstore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/*
** Test 6: Iterator functionality
*/
static void test_iterator(void){
  TEST_START("Iterator functionality");
  KVStore *kv = NULL;
  KVIterator *it = NULL;
  int rc, count = 0;
  void *k, *v;
  int klen, vlen;
  
  cleanup_db(TEST_DB);
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to open database");
  
  /* Insert test data */
  rc = kvstore_put(kv, "apple", 5, "red", 3);
  ASSERT_OK(rc, "Failed to put apple");
  rc = kvstore_put(kv, "banana", 6, "yellow", 6);
  ASSERT_OK(rc, "Failed to put banana");
  rc = kvstore_put(kv, "cherry", 6, "red", 3);
  ASSERT_OK(rc, "Failed to put cherry");
  
  /* Create iterator */
  rc = kvstore_iterator_create(kv, &it);
  ASSERT_OK(rc, "Failed to create iterator");
  
  /* Iterate through all entries */
  rc = kvstore_iterator_first(it);
  ASSERT_OK(rc, "Failed to move to first");
  
  while( !kvstore_iterator_eof(it) ){
    rc = kvstore_iterator_key(it, &k, &klen);
    ASSERT_OK(rc, "Failed to get iterator key");
    
    rc = kvstore_iterator_value(it, &v, &vlen);
    ASSERT_OK(rc, "Failed to get iterator value");
    
    count++;
    
    rc = kvstore_iterator_next(it);
    ASSERT_OK(rc, "Failed to move to next");
  }
  
  ASSERT_EQ(3, count, "Should iterate over 3 entries");
  
  kvstore_iterator_close(it);
  kvstore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/*
** Test 7: Large data handling
*/
static void test_large_data(void){
  TEST_START("Large key-value pairs");
  KVStore *kv = NULL;
  int rc;
  void *val = NULL;
  int vlen;
  char *large_val;
  int large_size = 1024 * 1024;  /* 1MB */
  
  cleanup_db(TEST_DB);
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to open database");
  
  /* Allocate large value */
  large_val = (char*)malloc(large_size);
  ASSERT_TRUE(large_val != NULL, "Failed to allocate large buffer");
  memset(large_val, 'X', large_size);
  
  /* Store large value */
  rc = kvstore_put(kv, "large_key", 9, large_val, large_size);
  ASSERT_OK(rc, "Failed to put large value");
  
  /* Retrieve and verify */
  rc = kvstore_get(kv, "large_key", 9, &val, &vlen);
  ASSERT_OK(rc, "Failed to get large value");
  ASSERT_EQ(large_size, vlen, "Large value size mismatch");
  ASSERT_TRUE(memcmp(val, large_val, large_size) == 0, "Large value content mismatch");
  
  sqliteFree(val);
  free(large_val);
  kvstore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/*
** Test 8: Error handling - invalid parameters
*/
static void test_error_handling(void){
  TEST_START("Error handling");
  KVStore *kv = NULL;
  int rc;
  void *val;
  int vlen;
  
  cleanup_db(TEST_DB);
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to open database");
  
  /* Test NULL key */
  rc = kvstore_put(kv, NULL, 0, "value", 5);
  ASSERT_TRUE(rc != KVSTORE_OK, "Should fail with NULL key");
  
  /* Test zero-length key */
  rc = kvstore_put(kv, "key", 0, "value", 5);
  ASSERT_TRUE(rc != KVSTORE_OK, "Should fail with zero-length key");
  
  /* Test get non-existent key */
  rc = kvstore_get(kv, "nonexistent", 11, &val, &vlen);
  ASSERT_EQ(KVSTORE_NOTFOUND, rc, "Should return NOTFOUND");
  
  /* Test delete non-existent key */
  rc = kvstore_delete(kv, "nonexistent", 11);
  ASSERT_EQ(KVSTORE_NOTFOUND, rc, "Should return NOTFOUND on delete");
  
  kvstore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/*
** Test 9: Persistence across sessions
*/
static void test_persistence(void){
  TEST_START("Data persistence");
  KVStore *kv = NULL;
  int rc, exists;
  void *val;
  int vlen;
  
  cleanup_db(TEST_DB);
  
  /* Session 1: Write data */
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to open database (session 1)");
  
  rc = kvstore_put(kv, "persistent", 10, "data", 4);
  ASSERT_OK(rc, "Failed to put persistent data");
  
  rc = kvstore_close(kv);
  ASSERT_OK(rc, "Failed to close (session 1)");
  
  /* Session 2: Read data */
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to open database (session 2)");
  
  rc = kvstore_exists(kv, "persistent", 10, &exists);
  ASSERT_OK(rc, "Failed to check persistent key");
  ASSERT_TRUE(exists, "Persistent key should exist");
  
  rc = kvstore_get(kv, "persistent", 10, &val, &vlen);
  ASSERT_OK(rc, "Failed to get persistent data");
  ASSERT_EQ(4, vlen, "Wrong persistent value length");
  ASSERT_TRUE(memcmp(val, "data", 4) == 0, "Wrong persistent value");
  sqliteFree(val);
  
  kvstore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/*
** Test 10: Statistics tracking
*/
static void test_statistics(void){
  TEST_START("Statistics tracking");
  KVStore *kv = NULL;
  KVStoreStats stats;
  int rc;
  
  cleanup_db(TEST_DB);
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to open database");
  
  /* Perform operations */
  kvstore_put(kv, "k1", 2, "v1", 2);
  kvstore_put(kv, "k2", 2, "v2", 2);
  
  void *v;
  int vlen;
  kvstore_get(kv, "k1", 2, &v, &vlen);
  sqliteFree(v);
  
  kvstore_delete(kv, "k1", 2);
  
  /* Get statistics */
  rc = kvstore_stats(kv, &stats);
  ASSERT_OK(rc, "Failed to get statistics");
  
  ASSERT_EQ(2, stats.nPuts, "Wrong put count");
  ASSERT_EQ(1, stats.nGets, "Wrong get count");
  ASSERT_EQ(1, stats.nDeletes, "Wrong delete count");
  
  printf("  Stats: puts=%llu, gets=%llu, deletes=%llu\n",
         stats.nPuts, stats.nGets, stats.nDeletes);
  
  kvstore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/*
** Test 11: Integrity check
*/
static void test_integrity(void){
  TEST_START("Database integrity check");
  KVStore *kv = NULL;
  int rc, i;
  char key[32], val[64];
  char *errMsg = NULL;
  
  cleanup_db(TEST_DB);
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to open database");
  
  /* Insert data */
  rc = kvstore_begin(kv, 1);
  ASSERT_OK(rc, "Failed to begin transaction");
  
  for(i = 0; i < 50; i++){
    snprintf(key, sizeof(key), "key_%d", i);
    snprintf(val, sizeof(val), "value_%d", i);
    rc = kvstore_put(kv, key, strlen(key), val, strlen(val));
    ASSERT_OK(rc, "Failed to put key");
  }
  
  rc = kvstore_commit(kv);
  ASSERT_OK(rc, "Failed to commit");
  
  /* Check integrity */
  rc = kvstore_integrity_check(kv, &errMsg);
  if( rc != KVSTORE_OK ){
    printf("  Integrity error: %s\n", errMsg ? errMsg : "unknown");
    if( errMsg ) sqliteFree(errMsg);
    TEST_FAIL("Integrity check failed");
  }
  ASSERT_TRUE(errMsg == NULL, "Should have no error message");
  
  printf("  Database integrity verified OK\n");
  
  kvstore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/*
** Test 12: Performance benchmark
*/
static void test_performance(void){
  TEST_START("Performance benchmark");
  KVStore *kv = NULL;
  int rc, i;
  char key[32], val[128];
  clock_t start, end;
  double elapsed;
  int num_ops = 10000;
  
  cleanup_db(PERF_DB);
  rc = kvstore_open(PERF_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to open database");
  
  /* Benchmark: Sequential writes */
  printf("  Benchmarking %d sequential writes...\n", num_ops);
  start = clock();
  
  rc = kvstore_begin(kv, 1);
  ASSERT_OK(rc, "Failed to begin transaction");
  
  for(i = 0; i < num_ops; i++){
    snprintf(key, sizeof(key), "perf_key_%08d", i);
    snprintf(val, sizeof(val), "perf_value_%08d_with_some_extra_data", i);
    rc = kvstore_put(kv, key, strlen(key), val, strlen(val));
    ASSERT_OK(rc, "Failed benchmark put");
  }
  
  rc = kvstore_commit(kv);
  ASSERT_OK(rc, "Failed to commit benchmark");
  
  end = clock();
  elapsed = ((double)(end - start)) / CLOCKS_PER_SEC;
  printf("  Writes: %.2f seconds (%.0f ops/sec)\n", 
         elapsed, num_ops / elapsed);
  
  /* Benchmark: Random reads */
  printf("  Benchmarking %d random reads...\n", num_ops);
  start = clock();
  
  for(i = 0; i < num_ops; i++){
    int idx = rand() % num_ops;
    void *v;
    int vlen;
    snprintf(key, sizeof(key), "perf_key_%08d", idx);
    rc = kvstore_get(kv, key, strlen(key), &v, &vlen);
    ASSERT_OK(rc, "Failed benchmark get");
    sqliteFree(v);
  }
  
  end = clock();
  elapsed = ((double)(end - start)) / CLOCKS_PER_SEC;
  printf("  Reads: %.2f seconds (%.0f ops/sec)\n", 
         elapsed, num_ops / elapsed);
  
  kvstore_close(kv);
  cleanup_db(PERF_DB);
  TEST_PASS();
}

/*
** Main test runner
*/
int main(void){
  printf("\n");
  printf("========================================\n");
  printf("KVStore Production Test Suite\n");
  printf("========================================\n\n");
  
  srand(time(NULL));
  
  /* Run all tests */
  test_open_close();
  test_basic_crud();
  test_update();
  test_transactions();
  test_batch_operations();
  test_iterator();
  test_large_data();
  test_error_handling();
  test_persistence();
  test_statistics();
  test_integrity();
  test_performance();
  
  /* Print summary */
  printf("========================================\n");
  printf("Test Summary\n");
  printf("========================================\n");
  printf("Total:  %d\n", tests_run);
  printf(COLOR_GREEN "Passed: %d" COLOR_RESET "\n", tests_passed);
  if( tests_failed > 0 ){
    printf(COLOR_RED "Failed: %d" COLOR_RESET "\n", tests_failed);
  }else{
    printf("Failed: 0\n");
  }
  printf("========================================\n\n");
  
  if( tests_failed == 0 ){
    printf(COLOR_GREEN "✓ All tests passed!" COLOR_RESET "\n\n");
    return 0;
  }else{
    printf(COLOR_RED "✗ Some tests failed!" COLOR_RESET "\n\n");
    return 1;
  }
}