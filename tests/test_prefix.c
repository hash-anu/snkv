/*
** Prefix Search Test Suite for KeyValueStore
**
** Tests cover:
** - Basic prefix search with keyvaluestore_prefix_iterator_create
** - Column family prefix search with keyvaluestore_cf_prefix_iterator_create
** - Sorted key order verification
** - Empty prefix results
** - Single-character prefixes
** - Prefix matching edge cases (exact match, partial, binary keys)
** - Prefix iterator with transactions
** - Large-scale prefix search
*/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "keyvaluestore.h"

#define TEST_DB "test_prefix.db"

/* Test result tracking */
static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

/* ANSI color codes */
#define COLOR_RED     "\x1b[31m"
#define COLOR_GREEN   "\x1b[32m"
#define COLOR_BLUE    "\x1b[34m"
#define COLOR_RESET   "\x1b[0m"

#define TEST_START(name) \
  do { \
    tests_run++; \
    printf(COLOR_BLUE "TEST %d: %s" COLOR_RESET "\n", tests_run, name); \
  } while(0)

#define TEST_PASS() \
  do { \
    tests_passed++; \
    printf(COLOR_GREEN "  [OK] PASSED" COLOR_RESET "\n\n"); \
  } while(0)

#define TEST_FAIL(msg) \
  do { \
    tests_failed++; \
    printf(COLOR_RED "  [X] FAILED: %s" COLOR_RESET "\n\n", msg); \
    return; \
  } while(0)

static void cleanup_db(const char *db){
  char buf[256];
  remove(db);
  snprintf(buf, sizeof(buf), "%s-wal", db);
  remove(buf);
  snprintf(buf, sizeof(buf), "%s-shm", db);
  remove(buf);
  snprintf(buf, sizeof(buf), "%s-journal", db);
  remove(buf);
}

/* ======================================================================
** TEST 1: Basic prefix search
** ====================================================================== */
static void test_basic_prefix_search(void){
  KeyValueStore *kv = NULL;
  KeyValueIterator *pIter = NULL;
  int rc, count;

  TEST_START("Basic prefix search");
  cleanup_db(TEST_DB);

  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("open failed");

  /* Insert keys in non-sorted order */
  const char *keys[] = {
    "user:300", "user:100", "user:200",
    "session:abc", "session:def",
    "config:timeout", "config:debug",
    "admin:root", "user:050"
  };
  int nKeys = sizeof(keys) / sizeof(keys[0]);

  keyvaluestore_begin(kv, 1);
  for(int i = 0; i < nKeys; i++){
    rc = keyvaluestore_put(kv, keys[i], (int)strlen(keys[i]), "v", 1);
    if( rc != KEYVALUESTORE_OK ) TEST_FAIL("put failed");
  }
  keyvaluestore_commit(kv);

  /* Prefix search for "user:" -- should find 4 keys */
  rc = keyvaluestore_prefix_iterator_create(kv, "user:", 5, &pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("prefix iterator create failed");

  count = 0;
  while( !keyvaluestore_iterator_eof(pIter) ){
    void *k; int nk;
    rc = keyvaluestore_iterator_key(pIter, &k, &nk);
    if( rc != KEYVALUESTORE_OK ) TEST_FAIL("iterator key failed");
    if( nk < 5 || memcmp(k, "user:", 5) != 0 ){
      TEST_FAIL("key does not start with 'user:'");
    }
    count++;
    keyvaluestore_iterator_next(pIter);
  }
  keyvaluestore_iterator_close(pIter);

  if( count != 4 ){
    printf("  Expected 4 user: keys, got %d\n", count);
    TEST_FAIL("wrong count for 'user:' prefix");
  }
  printf("  [OK] Found %d keys with prefix 'user:'\n", count);

  keyvaluestore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/* ======================================================================
** TEST 2: Sorted order verification
** ====================================================================== */
static void test_sorted_order(void){
  KeyValueStore *kv = NULL;
  KeyValueIterator *pIter = NULL;
  int rc;

  TEST_START("Sorted key order in prefix search");
  cleanup_db(TEST_DB);

  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("open failed");

  /* Insert keys in reverse order */
  keyvaluestore_begin(kv, 1);
  keyvaluestore_put(kv, "item:zzz", 8, "v", 1);
  keyvaluestore_put(kv, "item:aaa", 8, "v", 1);
  keyvaluestore_put(kv, "item:mmm", 8, "v", 1);
  keyvaluestore_put(kv, "item:bbb", 8, "v", 1);
  keyvaluestore_put(kv, "other:xxx", 9, "v", 1);
  keyvaluestore_commit(kv);

  rc = keyvaluestore_prefix_iterator_create(kv, "item:", 5, &pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("prefix iterator create failed");

  const char *expected[] = {"item:aaa", "item:bbb", "item:mmm", "item:zzz"};
  int idx = 0;
  while( !keyvaluestore_iterator_eof(pIter) ){
    void *k; int nk;
    keyvaluestore_iterator_key(pIter, &k, &nk);
    if( idx >= 4 ){
      TEST_FAIL("too many keys returned");
    }
    if( nk != (int)strlen(expected[idx]) ||
        memcmp(k, expected[idx], nk) != 0 ){
      printf("  Expected '%s', got '%.*s'\n", expected[idx], nk, (char*)k);
      TEST_FAIL("wrong key order");
    }
    printf("  [OK] Key %d: %.*s\n", idx, nk, (char*)k);
    idx++;
    keyvaluestore_iterator_next(pIter);
  }
  keyvaluestore_iterator_close(pIter);

  if( idx != 4 ){
    printf("  Expected 4 keys, got %d\n", idx);
    TEST_FAIL("wrong count");
  }

  keyvaluestore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/* ======================================================================
** TEST 3: Empty prefix results
** ====================================================================== */
static void test_empty_prefix_results(void){
  KeyValueStore *kv = NULL;
  KeyValueIterator *pIter = NULL;
  int rc, count;

  TEST_START("Empty prefix results (no matching keys)");
  cleanup_db(TEST_DB);

  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("open failed");

  keyvaluestore_begin(kv, 1);
  keyvaluestore_put(kv, "aaa", 3, "v", 1);
  keyvaluestore_put(kv, "bbb", 3, "v", 1);
  keyvaluestore_commit(kv);

  /* Search for a prefix that doesn't exist */
  rc = keyvaluestore_prefix_iterator_create(kv, "zzz", 3, &pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("prefix iterator create failed");

  count = 0;
  while( !keyvaluestore_iterator_eof(pIter) ){
    count++;
    keyvaluestore_iterator_next(pIter);
  }
  keyvaluestore_iterator_close(pIter);

  if( count != 0 ){
    printf("  Expected 0 keys, got %d\n", count);
    TEST_FAIL("should find no keys for non-existent prefix");
  }
  printf("  [OK] Correctly returned 0 keys for non-existent prefix\n");

  /* Search for prefix between existing keys */
  rc = keyvaluestore_prefix_iterator_create(kv, "abc", 3, &pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("prefix iterator create failed");

  count = 0;
  while( !keyvaluestore_iterator_eof(pIter) ){
    count++;
    keyvaluestore_iterator_next(pIter);
  }
  keyvaluestore_iterator_close(pIter);

  if( count != 0 ){
    printf("  Expected 0 keys for 'abc' prefix, got %d\n", count);
    TEST_FAIL("should find no keys for 'abc' prefix");
  }
  printf("  [OK] Correctly returned 0 keys for 'abc' prefix\n");

  /* Empty database */
  keyvaluestore_close(kv);
  cleanup_db(TEST_DB);
  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("open empty db failed");

  rc = keyvaluestore_prefix_iterator_create(kv, "any", 3, &pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("prefix iterator on empty db failed");

  if( !keyvaluestore_iterator_eof(pIter) ){
    TEST_FAIL("should be eof on empty database");
  }
  printf("  [OK] Correctly returned eof on empty database\n");
  keyvaluestore_iterator_close(pIter);

  keyvaluestore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/* ======================================================================
** TEST 4: Single-character prefix
** ====================================================================== */
static void test_single_char_prefix(void){
  KeyValueStore *kv = NULL;
  KeyValueIterator *pIter = NULL;
  int rc, count;

  TEST_START("Single-character prefix search");
  cleanup_db(TEST_DB);

  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("open failed");

  keyvaluestore_begin(kv, 1);
  keyvaluestore_put(kv, "apple", 5, "v", 1);
  keyvaluestore_put(kv, "avocado", 7, "v", 1);
  keyvaluestore_put(kv, "banana", 6, "v", 1);
  keyvaluestore_put(kv, "blueberry", 9, "v", 1);
  keyvaluestore_put(kv, "cherry", 6, "v", 1);
  keyvaluestore_commit(kv);

  /* Prefix "a" should match apple, avocado */
  rc = keyvaluestore_prefix_iterator_create(kv, "a", 1, &pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("prefix iterator create failed");

  count = 0;
  while( !keyvaluestore_iterator_eof(pIter) ){
    void *k; int nk;
    keyvaluestore_iterator_key(pIter, &k, &nk);
    if( nk < 1 || ((char*)k)[0] != 'a' ){
      TEST_FAIL("key doesn't start with 'a'");
    }
    printf("  [OK] Found: %.*s\n", nk, (char*)k);
    count++;
    keyvaluestore_iterator_next(pIter);
  }
  keyvaluestore_iterator_close(pIter);

  if( count != 2 ){
    printf("  Expected 2 keys for prefix 'a', got %d\n", count);
    TEST_FAIL("wrong count for prefix 'a'");
  }

  /* Prefix "b" should match banana, blueberry */
  rc = keyvaluestore_prefix_iterator_create(kv, "b", 1, &pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("prefix iterator create failed");

  count = 0;
  while( !keyvaluestore_iterator_eof(pIter) ){
    count++;
    keyvaluestore_iterator_next(pIter);
  }
  keyvaluestore_iterator_close(pIter);

  if( count != 2 ){
    printf("  Expected 2 keys for prefix 'b', got %d\n", count);
    TEST_FAIL("wrong count for prefix 'b'");
  }
  printf("  [OK] Prefix 'b' matched %d keys\n", count);

  keyvaluestore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/* ======================================================================
** TEST 5: Prefix with exact key match
** ====================================================================== */
static void test_exact_key_as_prefix(void){
  KeyValueStore *kv = NULL;
  KeyValueIterator *pIter = NULL;
  int rc, count;

  TEST_START("Prefix that exactly matches an existing key");
  cleanup_db(TEST_DB);

  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("open failed");

  keyvaluestore_begin(kv, 1);
  keyvaluestore_put(kv, "app", 3, "v1", 2);
  keyvaluestore_put(kv, "apple", 5, "v2", 2);
  keyvaluestore_put(kv, "application", 11, "v3", 2);
  keyvaluestore_put(kv, "apply", 5, "v4", 2);
  keyvaluestore_put(kv, "banana", 6, "v5", 2);
  keyvaluestore_commit(kv);

  /* Prefix "app" should match: app, apple, application, apply */
  rc = keyvaluestore_prefix_iterator_create(kv, "app", 3, &pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("prefix iterator create failed");

  count = 0;
  while( !keyvaluestore_iterator_eof(pIter) ){
    void *k; int nk;
    keyvaluestore_iterator_key(pIter, &k, &nk);
    printf("  [OK] Found: %.*s\n", nk, (char*)k);
    count++;
    keyvaluestore_iterator_next(pIter);
  }
  keyvaluestore_iterator_close(pIter);

  if( count != 4 ){
    printf("  Expected 4 keys for prefix 'app', got %d\n", count);
    TEST_FAIL("wrong count");
  }

  /* Prefix "apple" should match only: apple */
  rc = keyvaluestore_prefix_iterator_create(kv, "apple", 5, &pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("prefix iterator create failed");

  count = 0;
  while( !keyvaluestore_iterator_eof(pIter) ){
    void *k; int nk;
    keyvaluestore_iterator_key(pIter, &k, &nk);
    if( nk != 5 || memcmp(k, "apple", 5) != 0 ){
      printf("  Unexpected key: %.*s\n", nk, (char*)k);
      TEST_FAIL("unexpected key for prefix 'apple'");
    }
    count++;
    keyvaluestore_iterator_next(pIter);
  }
  keyvaluestore_iterator_close(pIter);

  if( count != 1 ){
    printf("  Expected 1 key for prefix 'apple', got %d\n", count);
    TEST_FAIL("wrong count for 'apple' prefix");
  }
  printf("  [OK] Prefix 'apple' matched exactly 1 key\n");

  keyvaluestore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/* ======================================================================
** TEST 6: Column family prefix search
** ====================================================================== */
static void test_cf_prefix_search(void){
  KeyValueStore *kv = NULL;
  KeyValueColumnFamily *pCF = NULL;
  KeyValueIterator *pIter = NULL;
  int rc, count;

  TEST_START("Column family prefix search");
  cleanup_db(TEST_DB);

  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("open failed");

  rc = keyvaluestore_cf_create(kv, "logs", &pCF);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("cf create failed");

  keyvaluestore_begin(kv, 1);
  keyvaluestore_cf_put(pCF, "2024-01-01:info:msg1", 20, "v", 1);
  keyvaluestore_cf_put(pCF, "2024-01-01:error:msg2", 21, "v", 1);
  keyvaluestore_cf_put(pCF, "2024-01-02:info:msg3", 20, "v", 1);
  keyvaluestore_cf_put(pCF, "2024-02-01:warn:msg4", 20, "v", 1);
  keyvaluestore_commit(kv);

  /* Search for all January logs */
  rc = keyvaluestore_cf_prefix_iterator_create(pCF, "2024-01", 7, &pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("cf prefix iterator create failed");

  count = 0;
  while( !keyvaluestore_iterator_eof(pIter) ){
    void *k; int nk;
    keyvaluestore_iterator_key(pIter, &k, &nk);
    printf("  [OK] Found: %.*s\n", nk, (char*)k);
    count++;
    keyvaluestore_iterator_next(pIter);
  }
  keyvaluestore_iterator_close(pIter);

  if( count != 3 ){
    printf("  Expected 3 January logs, got %d\n", count);
    TEST_FAIL("wrong count for January prefix");
  }

  /* Also verify default CF is not affected */
  keyvaluestore_put(kv, "2024-01-xx", 10, "default_cf_val", 14);
  rc = keyvaluestore_prefix_iterator_create(kv, "2024-01", 7, &pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("default cf prefix iterator failed");

  count = 0;
  while( !keyvaluestore_iterator_eof(pIter) ){
    count++;
    keyvaluestore_iterator_next(pIter);
  }
  keyvaluestore_iterator_close(pIter);

  if( count != 1 ){
    printf("  Expected 1 key in default CF, got %d\n", count);
    TEST_FAIL("CF isolation broken");
  }
  printf("  [OK] Column family isolation verified\n");

  keyvaluestore_cf_close(pCF);
  keyvaluestore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/* ======================================================================
** TEST 7: Prefix search with values verification
** ====================================================================== */
static void test_prefix_with_values(void){
  KeyValueStore *kv = NULL;
  KeyValueIterator *pIter = NULL;
  int rc;

  TEST_START("Prefix search with value verification");
  cleanup_db(TEST_DB);

  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("open failed");

  keyvaluestore_begin(kv, 1);
  keyvaluestore_put(kv, "color:red", 9, "FF0000", 6);
  keyvaluestore_put(kv, "color:green", 11, "00FF00", 6);
  keyvaluestore_put(kv, "color:blue", 10, "0000FF", 6);
  keyvaluestore_put(kv, "size:small", 10, "S", 1);
  keyvaluestore_put(kv, "size:large", 10, "L", 1);
  keyvaluestore_commit(kv);

  rc = keyvaluestore_prefix_iterator_create(kv, "color:", 6, &pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("prefix iterator create failed");

  int count = 0;
  while( !keyvaluestore_iterator_eof(pIter) ){
    void *k, *v;
    int nk, nv;
    keyvaluestore_iterator_key(pIter, &k, &nk);
    keyvaluestore_iterator_value(pIter, &v, &nv);
    printf("  [OK] %.*s = %.*s\n", nk, (char*)k, nv, (char*)v);

    /* Verify value is a 6-char hex color */
    if( nv != 6 ){
      TEST_FAIL("unexpected value length");
    }
    count++;
    keyvaluestore_iterator_next(pIter);
  }
  keyvaluestore_iterator_close(pIter);

  if( count != 3 ){
    printf("  Expected 3 color keys, got %d\n", count);
    TEST_FAIL("wrong count");
  }

  keyvaluestore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/* ======================================================================
** TEST 8: Binary key prefix search
** ====================================================================== */
static void test_binary_key_prefix(void){
  KeyValueStore *kv = NULL;
  KeyValueIterator *pIter = NULL;
  int rc, count;

  TEST_START("Binary key prefix search");
  cleanup_db(TEST_DB);

  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("open failed");

  /* Use binary prefixes: 0x01 0x02 ... */
  unsigned char prefix[] = {0x01, 0x02};
  unsigned char key1[] = {0x01, 0x02, 0x03};
  unsigned char key2[] = {0x01, 0x02, 0x04};
  unsigned char key3[] = {0x01, 0x03, 0x00};
  unsigned char key4[] = {0x01, 0x02, 0x00};  /* 0x00 embedded */
  unsigned char key5[] = {0x02, 0x01, 0x01};

  keyvaluestore_begin(kv, 1);
  keyvaluestore_put(kv, key1, 3, "v1", 2);
  keyvaluestore_put(kv, key2, 3, "v2", 2);
  keyvaluestore_put(kv, key3, 3, "v3", 2);
  keyvaluestore_put(kv, key4, 3, "v4", 2);
  keyvaluestore_put(kv, key5, 3, "v5", 2);
  keyvaluestore_commit(kv);

  rc = keyvaluestore_prefix_iterator_create(kv, prefix, 2, &pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("prefix iterator create failed");

  count = 0;
  while( !keyvaluestore_iterator_eof(pIter) ){
    void *k; int nk;
    keyvaluestore_iterator_key(pIter, &k, &nk);
    if( nk < 2 || memcmp(k, prefix, 2) != 0 ){
      TEST_FAIL("key doesn't start with binary prefix");
    }
    count++;
    keyvaluestore_iterator_next(pIter);
  }
  keyvaluestore_iterator_close(pIter);

  /* Should match key1, key2, key4 (all start with 0x01 0x02) */
  if( count != 3 ){
    printf("  Expected 3 keys with binary prefix, got %d\n", count);
    TEST_FAIL("wrong count for binary prefix");
  }
  printf("  [OK] Found %d keys with binary prefix [0x01, 0x02]\n", count);

  keyvaluestore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/* ======================================================================
** TEST 9: Prefix search with WAL mode
** ====================================================================== */
static void test_prefix_wal_mode(void){
  KeyValueStore *kv = NULL;
  KeyValueIterator *pIter = NULL;
  int rc, count;

  TEST_START("Prefix search in WAL mode");
  cleanup_db(TEST_DB);

  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_WAL);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("open WAL failed");

  keyvaluestore_begin(kv, 1);
  for(int i = 0; i < 100; i++){
    char key[32];
    snprintf(key, sizeof(key), "ns%d:key%03d", i % 3, i);
    keyvaluestore_put(kv, key, (int)strlen(key), "v", 1);
  }
  keyvaluestore_commit(kv);

  /* Search for "ns1:" keys */
  rc = keyvaluestore_prefix_iterator_create(kv, "ns1:", 4, &pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("prefix iterator create in WAL failed");

  count = 0;
  while( !keyvaluestore_iterator_eof(pIter) ){
    void *k; int nk;
    keyvaluestore_iterator_key(pIter, &k, &nk);
    if( nk < 4 || memcmp(k, "ns1:", 4) != 0 ){
      TEST_FAIL("key doesn't start with 'ns1:'");
    }
    count++;
    keyvaluestore_iterator_next(pIter);
  }
  keyvaluestore_iterator_close(pIter);

  /* 100 keys, modulo 3 -> ns1 gets keys 1,4,7,...,97 = 33 keys */
  if( count != 33 ){
    printf("  Expected 33 ns1: keys, got %d\n", count);
    TEST_FAIL("wrong count in WAL mode");
  }
  printf("  [OK] Found %d keys with prefix 'ns1:' in WAL mode\n", count);

  keyvaluestore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/* ======================================================================
** TEST 10: Large-scale prefix search performance
** ====================================================================== */
static void test_large_scale_prefix(void){
  KeyValueStore *kv = NULL;
  KeyValueIterator *pIter = NULL;
  int rc, count;

  TEST_START("Large-scale prefix search (10K keys)");
  cleanup_db(TEST_DB);

  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_WAL);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("open failed");

  /* Insert 10K keys across 10 namespaces */
  keyvaluestore_begin(kv, 1);
  for(int i = 0; i < 10000; i++){
    char key[64], val[32];
    snprintf(key, sizeof(key), "namespace%d:record:%05d", i % 10, i);
    snprintf(val, sizeof(val), "value_%d", i);
    rc = keyvaluestore_put(kv, key, (int)strlen(key), val, (int)strlen(val));
    if( rc != KEYVALUESTORE_OK ) TEST_FAIL("put failed during bulk insert");
  }
  keyvaluestore_commit(kv);

  /* Each namespace gets 1000 keys */
  rc = keyvaluestore_prefix_iterator_create(kv, "namespace5:", 11, &pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("prefix iterator create failed");

  count = 0;
  while( !keyvaluestore_iterator_eof(pIter) ){
    count++;
    keyvaluestore_iterator_next(pIter);
  }
  keyvaluestore_iterator_close(pIter);

  if( count != 1000 ){
    printf("  Expected 1000 keys in namespace5, got %d\n", count);
    TEST_FAIL("wrong count for large-scale prefix");
  }
  printf("  [OK] Found %d keys in namespace5 out of 10K total\n", count);

  /* Narrower prefix: "namespace5:record:050" -> 10 keys (05000-05009) */
  rc = keyvaluestore_prefix_iterator_create(kv, "namespace5:record:050", 21, &pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("narrow prefix iterator failed");

  count = 0;
  while( !keyvaluestore_iterator_eof(pIter) ){
    count++;
    keyvaluestore_iterator_next(pIter);
  }
  keyvaluestore_iterator_close(pIter);

  if( count != 10 ){
    printf("  Expected 10 keys for narrow prefix, got %d\n", count);
    TEST_FAIL("wrong count for narrow prefix");
  }
  printf("  [OK] Narrow prefix matched %d keys\n", count);

  keyvaluestore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/* ======================================================================
** TEST 11: Prefix search after updates and deletes
** ====================================================================== */
static void test_prefix_after_mutations(void){
  KeyValueStore *kv = NULL;
  KeyValueIterator *pIter = NULL;
  int rc, count;

  TEST_START("Prefix search after updates and deletes");
  cleanup_db(TEST_DB);

  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("open failed");

  keyvaluestore_begin(kv, 1);
  keyvaluestore_put(kv, "tag:alpha", 9, "v1", 2);
  keyvaluestore_put(kv, "tag:beta", 8, "v2", 2);
  keyvaluestore_put(kv, "tag:gamma", 9, "v3", 2);
  keyvaluestore_put(kv, "tag:delta", 9, "v4", 2);
  keyvaluestore_commit(kv);

  /* Delete tag:beta */
  keyvaluestore_begin(kv, 1);
  keyvaluestore_delete(kv, "tag:beta", 8);
  keyvaluestore_commit(kv);

  /* Update tag:gamma value */
  keyvaluestore_begin(kv, 1);
  keyvaluestore_put(kv, "tag:gamma", 9, "updated_v3", 10);
  keyvaluestore_commit(kv);

  rc = keyvaluestore_prefix_iterator_create(kv, "tag:", 4, &pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("prefix iterator create failed");

  count = 0;
  while( !keyvaluestore_iterator_eof(pIter) ){
    void *k, *v;
    int nk, nv;
    keyvaluestore_iterator_key(pIter, &k, &nk);
    keyvaluestore_iterator_value(pIter, &v, &nv);

    printf (" [OK] Found: %.*s = %.*s\n", nk, (char*)k, nv, (char*)v);
    /* Verify tag:beta is gone */
    if( nk == 8 && memcmp(k, "tag:beta", 8) == 0 ){
      TEST_FAIL("deleted key tag:beta should not appear");
    }

    /* Verify tag:gamma has updated value */
    if( nk == 9 && memcmp(k, "tag:gamma", 9) == 0 ){
      if( nv != 10 || memcmp(v, "updated_v3", 10) != 0 ){
        TEST_FAIL("tag:gamma should have updated value");
      }
      printf("  [OK] tag:gamma has updated value\n");
    }

    count++;
    keyvaluestore_iterator_next(pIter);
  }
  keyvaluestore_iterator_close(pIter);

  if( count != 3 ){
    printf("  Expected 3 keys after delete, got %d\n", count);
    TEST_FAIL("wrong count after mutations");
  }
  printf("  [OK] Found %d keys after delete+update\n", count);

  keyvaluestore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/* ======================================================================
** TEST 12: iterator_first re-seek on prefix iterator
** ====================================================================== */
static void test_prefix_iterator_first_reseek(void){
  KeyValueStore *kv = NULL;
  KeyValueIterator *pIter = NULL;
  int rc, count;

  TEST_START("Prefix iterator first() re-seek");
  cleanup_db(TEST_DB);

  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("open failed");

  keyvaluestore_begin(kv, 1);
  keyvaluestore_put(kv, "x:1", 3, "v", 1);
  keyvaluestore_put(kv, "x:2", 3, "v", 1);
  keyvaluestore_put(kv, "x:3", 3, "v", 1);
  keyvaluestore_put(kv, "y:1", 3, "v", 1);
  keyvaluestore_commit(kv);

  rc = keyvaluestore_prefix_iterator_create(kv, "x:", 2, &pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("prefix iterator create failed");

  /* Consume all entries */
  count = 0;
  while( !keyvaluestore_iterator_eof(pIter) ){
    count++;
    keyvaluestore_iterator_next(pIter);
  }
  if( count != 3 ) TEST_FAIL("first pass: expected 3 keys");

  /* Re-seek with first() */
  rc = keyvaluestore_iterator_first(pIter);
  if( rc != KEYVALUESTORE_OK ) TEST_FAIL("iterator first re-seek failed");

  count = 0;
  while( !keyvaluestore_iterator_eof(pIter) ){
    count++;
    keyvaluestore_iterator_next(pIter);
  }
  if( count != 3 ){
    printf("  Expected 3 keys on re-seek, got %d\n", count);
    TEST_FAIL("re-seek returned wrong count");
  }
  printf("  [OK] Re-seek with first() returned %d keys again\n", count);

  keyvaluestore_iterator_close(pIter);
  keyvaluestore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/* ======================================================================
** MAIN
** ====================================================================== */
int main(void){
  printf("\n========================================\n");
  printf("Prefix Search Test Suite\n");
  printf("========================================\n\n");

  test_basic_prefix_search();
  test_sorted_order();
  test_empty_prefix_results();
  test_single_char_prefix();
  test_exact_key_as_prefix();
  test_cf_prefix_search();
  test_prefix_with_values();
  test_binary_key_prefix();
  test_prefix_wal_mode();
  test_large_scale_prefix();
  test_prefix_after_mutations();
  test_prefix_iterator_first_reseek();

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
    printf(COLOR_GREEN "[OK] All prefix search tests passed!" COLOR_RESET "\n\n");
  }

  return tests_failed > 0 ? 1 : 0;
}
