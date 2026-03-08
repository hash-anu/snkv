/* SPDX-License-Identifier: Apache-2.0 */
/*
** tests/test_new_apis.c — Test suite for new APIs:
**   kvstore_iterator_seek, kvstore_put_if_absent / kvstore_cf_put_if_absent,
**   kvstore_clear / kvstore_cf_clear, extended KVStoreStats + kvstore_stats_reset,
**   kvstore_count / kvstore_cf_count
**
** Tests:
**   --- kvstore_iterator_seek ---
**   1.  Forward seek to existing key → positioned at that key
**   2.  Forward seek between keys → positioned at next key (>=)
**   3.  Forward seek past last key → eof immediately
**   4.  Forward seek before first key → positioned at first key
**   5.  Forward prefix iterator + seek within prefix → stays within prefix
**   6.  Forward prefix iterator + seek outside prefix → eof
**   7.  Forward seek then next() → correct traversal from seek position
**   8.  Forward seek on empty CF → eof immediately
**   9.  Reverse seek to existing key → positioned at that key
**  10.  Reverse seek between keys → positioned at nearest <= target
**  11.  Reverse seek before first key → eof immediately
**  12.  Multiple sequential seeks on the same iterator
**
**   --- kvstore_put_if_absent ---
**  13.  Key absent, no TTL → inserted=1, value readable
**  14.  Key present → inserted=0, existing value unchanged
**  15.  Key absent with TTL → inserted=1, ttl_remaining > 0
**  16.  Key present with TTL → inserted=0, existing TTL unchanged
**  17.  Expired key treated as absent → new value inserted
**  18.  CF variant: same behaviour on explicit CF handle
**  19.  pInserted=NULL → no crash, still correct
**  20.  Inside explicit write transaction → joins, no double-commit
**
**   --- kvstore_clear / kvstore_cf_clear ---
**  21.  Clear default CF → count=0, iterator immediately eof
**  22.  Clear CF with TTL keys → TTL index cleared, purge returns 0
**  23.  Clear CF with TTL from previous session (reopen) → cleared
**  24.  Clear empty CF → KVSTORE_OK
**  25.  Clear then reinsert → works correctly
**  26.  Clear one CF → other CFs unaffected
**
**   --- Extended KVStoreStats ---
**  27.  nBytesWritten increments by key+value on each put
**  28.  nBytesRead increments by value size on each get
**  29.  nWalCommits increments on each commit
**  30.  nTtlExpired increments on lazy expiry
**  31.  nTtlPurged increments after purge_expired
**  32.  nDbPages > 0 after writes
**  33.  kvstore_stats_reset → cumulative counters zero; nDbPages still > 0
**
**   --- kvstore_count / kvstore_cf_count ---
**  34.  Count on empty CF → 0
**  35.  Count after N puts → N
**  36.  Count after put + delete → N-1
**  37.  Count includes expired-but-not-purged keys
**  38.  Count after purge_expired → only live keys counted
**  39.  CF count counts only that CF (not other CFs or TTL CFs)
**  40.  kvstore_count delegates to default CF correctly
**
**   --- clear reduces page count ---
**  41.  Fill store with many keys, clear, checkpoint TRUNCATE → nDbPages decreases
*/

#include "kvstore.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>

/* ---- helpers ---- */

static int passed = 0;
static int failed = 0;

static void check(const char *name, int ok){
  if( ok ){
    printf("  PASS: %s\n", name);
    passed++;
  } else {
    printf("  FAIL: %s\n", name);
    failed++;
  }
}

#define ASSERT(name, expr) check(name, (int)(expr))

static KVStore *openFresh(const char *path){
  remove(path);
  char walPath[512]; snprintf(walPath, sizeof(walPath), "%s-wal", path); remove(walPath);
  char shmPath[512]; snprintf(shmPath, sizeof(shmPath), "%s-shm", path); remove(shmPath);
  KVStore *pKV = NULL;
  int rc = kvstore_open(path, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){ fprintf(stderr, "openFresh: open failed %d\n", rc); return NULL; }
  return pKV;
}

static void cleanup(KVStore **ppKV, const char *path){
  if( *ppKV ){ kvstore_close(*ppKV); *ppKV = NULL; }
  remove(path);
  char walPath[512]; snprintf(walPath, sizeof(walPath), "%s-wal", path); remove(walPath);
  char shmPath[512]; snprintf(shmPath, sizeof(shmPath), "%s-shm", path); remove(shmPath);
}

/* ====================================================================== */
/* --- kvstore_iterator_seek --- */
/* ====================================================================== */

static void test1_seek_existing(void){
  printf("\nTest 1: forward seek to existing key\n");
  const char *path = "test_new_apis_1.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "aaa", 3, "v1", 2);
  kvstore_put(pKV, "bbb", 3, "v2", 2);
  kvstore_put(pKV, "ccc", 3, "v3", 2);

  KVIterator *it = NULL;
  int rc = kvstore_iterator_create(pKV, &it);
  ASSERT("iterator created", rc == KVSTORE_OK && it);

  rc = kvstore_iterator_seek(it, "bbb", 3);
  ASSERT("seek returns OK", rc == KVSTORE_OK);
  ASSERT("not eof after seek", !kvstore_iterator_eof(it));

  void *pKey = NULL; int nKey = 0;
  kvstore_iterator_key(it, &pKey, &nKey);
  ASSERT("key is bbb", nKey == 3 && pKey && memcmp(pKey, "bbb", 3) == 0);
  /* pKey is an internal iterator buffer — do not free */

  kvstore_iterator_close(it);
  cleanup(&pKV, path);
}

static void test2_seek_between(void){
  printf("\nTest 2: forward seek between keys (>=)\n");
  const char *path = "test_new_apis_2.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "aaa", 3, "v1", 2);
  kvstore_put(pKV, "ccc", 3, "v3", 2);

  KVIterator *it = NULL;
  kvstore_iterator_create(pKV, &it);

  /* seek to "bbb" which doesn't exist — should land on "ccc" */
  int rc = kvstore_iterator_seek(it, "bbb", 3);
  ASSERT("seek returns OK", rc == KVSTORE_OK);
  ASSERT("not eof", !kvstore_iterator_eof(it));

  void *pKey = NULL; int nKey = 0;
  kvstore_iterator_key(it, &pKey, &nKey);
  ASSERT("positioned at ccc", nKey == 3 && pKey && memcmp(pKey, "ccc", 3) == 0);
  /* pKey is an internal iterator buffer — do not free */

  kvstore_iterator_close(it);
  cleanup(&pKV, path);
}

static void test3_seek_past_last(void){
  printf("\nTest 3: forward seek past last key → eof\n");
  const char *path = "test_new_apis_3.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "aaa", 3, "v1", 2);
  kvstore_put(pKV, "bbb", 3, "v2", 2);

  KVIterator *it = NULL;
  kvstore_iterator_create(pKV, &it);

  int rc = kvstore_iterator_seek(it, "zzz", 3);
  ASSERT("seek returns OK", rc == KVSTORE_OK);
  ASSERT("eof immediately", kvstore_iterator_eof(it));

  kvstore_iterator_close(it);
  cleanup(&pKV, path);
}

static void test4_seek_before_first(void){
  printf("\nTest 4: forward seek before first key → first key\n");
  const char *path = "test_new_apis_4.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "mmm", 3, "v1", 2);
  kvstore_put(pKV, "nnn", 3, "v2", 2);

  KVIterator *it = NULL;
  kvstore_iterator_create(pKV, &it);

  int rc = kvstore_iterator_seek(it, "aaa", 3);
  ASSERT("seek returns OK", rc == KVSTORE_OK);
  ASSERT("not eof", !kvstore_iterator_eof(it));

  void *pKey = NULL; int nKey = 0;
  kvstore_iterator_key(it, &pKey, &nKey);
  ASSERT("positioned at first key (mmm)", nKey == 3 && pKey && memcmp(pKey, "mmm", 3) == 0);
  /* pKey is an internal iterator buffer — do not free */

  kvstore_iterator_close(it);
  cleanup(&pKV, path);
}

static void test5_seek_prefix_within(void){
  printf("\nTest 5: seek on prefix iterator within prefix\n");
  const char *path = "test_new_apis_5.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "pfx:a", 5, "v1", 2);
  kvstore_put(pKV, "pfx:b", 5, "v2", 2);
  kvstore_put(pKV, "pfx:c", 5, "v3", 2);
  kvstore_put(pKV, "qfx:d", 5, "v4", 2);

  KVIterator *it = NULL;
  kvstore_prefix_iterator_create(pKV, "pfx:", 4, &it);

  /* seek to "pfx:b" — within prefix */
  int rc = kvstore_iterator_seek(it, "pfx:b", 5);
  ASSERT("seek within prefix OK", rc == KVSTORE_OK);
  ASSERT("not eof", !kvstore_iterator_eof(it));

  void *pKey = NULL; int nKey = 0;
  kvstore_iterator_key(it, &pKey, &nKey);
  ASSERT("at pfx:b", nKey == 5 && pKey && memcmp(pKey, "pfx:b", 5) == 0);
  /* pKey is an internal iterator buffer — do not free */

  /* next() should give pfx:c */
  kvstore_iterator_next(it);
  ASSERT("not eof after next", !kvstore_iterator_eof(it));
  kvstore_iterator_key(it, &pKey, &nKey);
  ASSERT("next is pfx:c", nKey == 5 && pKey && memcmp(pKey, "pfx:c", 5) == 0);
  /* pKey is an internal iterator buffer — do not free */

  /* next() should be eof (q* is outside prefix) */
  kvstore_iterator_next(it);
  ASSERT("eof at prefix boundary", kvstore_iterator_eof(it));

  kvstore_iterator_close(it);
  cleanup(&pKV, path);
}

static void test6_seek_prefix_outside(void){
  printf("\nTest 6: seek on prefix iterator outside prefix → eof\n");
  const char *path = "test_new_apis_6.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "pfx:a", 5, "v1", 2);
  kvstore_put(pKV, "pfx:b", 5, "v2", 2);

  KVIterator *it = NULL;
  kvstore_prefix_iterator_create(pKV, "pfx:", 4, &it);

  /* seek outside prefix */
  int rc = kvstore_iterator_seek(it, "zzz:a", 5);
  ASSERT("seek returns OK", rc == KVSTORE_OK);
  ASSERT("eof because outside prefix", kvstore_iterator_eof(it));

  kvstore_iterator_close(it);
  cleanup(&pKV, path);
}

static void test7_seek_then_next(void){
  printf("\nTest 7: forward seek then next() traversal\n");
  const char *path = "test_new_apis_7.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "aaa", 3, "v1", 2);
  kvstore_put(pKV, "bbb", 3, "v2", 2);
  kvstore_put(pKV, "ccc", 3, "v3", 2);
  kvstore_put(pKV, "ddd", 3, "v4", 2);

  KVIterator *it = NULL;
  kvstore_iterator_create(pKV, &it);
  kvstore_iterator_seek(it, "bbb", 3);

  void *pKey = NULL; int nKey = 0;
  kvstore_iterator_key(it, &pKey, &nKey);
  ASSERT("at bbb", nKey == 3 && memcmp(pKey, "bbb", 3) == 0);
  /* pKey is an internal iterator buffer — do not free */

  kvstore_iterator_next(it);
  kvstore_iterator_key(it, &pKey, &nKey);
  ASSERT("next is ccc", nKey == 3 && memcmp(pKey, "ccc", 3) == 0);
  /* pKey is an internal iterator buffer — do not free */

  kvstore_iterator_next(it);
  kvstore_iterator_key(it, &pKey, &nKey);
  ASSERT("next is ddd", nKey == 3 && memcmp(pKey, "ddd", 3) == 0);
  /* pKey is an internal iterator buffer — do not free */

  kvstore_iterator_next(it);
  ASSERT("eof at end", kvstore_iterator_eof(it));

  kvstore_iterator_close(it);
  cleanup(&pKV, path);
}

static void test8_seek_empty(void){
  printf("\nTest 8: forward seek on empty CF → eof\n");
  const char *path = "test_new_apis_8.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  KVIterator *it = NULL;
  kvstore_iterator_create(pKV, &it);

  int rc = kvstore_iterator_seek(it, "key", 3);
  ASSERT("seek OK on empty", rc == KVSTORE_OK);
  ASSERT("eof on empty CF", kvstore_iterator_eof(it));

  kvstore_iterator_close(it);
  cleanup(&pKV, path);
}

static void test9_reverse_seek_exact(void){
  printf("\nTest 9: reverse seek to existing key\n");
  const char *path = "test_new_apis_9.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "aaa", 3, "v1", 2);
  kvstore_put(pKV, "bbb", 3, "v2", 2);
  kvstore_put(pKV, "ccc", 3, "v3", 2);

  KVIterator *it = NULL;
  kvstore_reverse_iterator_create(pKV, &it);

  int rc = kvstore_iterator_seek(it, "bbb", 3);
  ASSERT("reverse seek OK", rc == KVSTORE_OK);
  ASSERT("not eof", !kvstore_iterator_eof(it));

  void *pKey = NULL; int nKey = 0;
  kvstore_iterator_key(it, &pKey, &nKey);
  ASSERT("at bbb", nKey == 3 && memcmp(pKey, "bbb", 3) == 0);
  /* pKey is an internal iterator buffer — do not free */

  kvstore_iterator_close(it);
  cleanup(&pKV, path);
}

static void test10_reverse_seek_between(void){
  printf("\nTest 10: reverse seek between keys → nearest <=\n");
  const char *path = "test_new_apis_10.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "aaa", 3, "v1", 2);
  kvstore_put(pKV, "ccc", 3, "v3", 2);

  KVIterator *it = NULL;
  kvstore_reverse_iterator_create(pKV, &it);

  /* seek to "bbb" → no exact match, should land on "aaa" (nearest <=) */
  int rc = kvstore_iterator_seek(it, "bbb", 3);
  ASSERT("reverse seek OK", rc == KVSTORE_OK);
  ASSERT("not eof", !kvstore_iterator_eof(it));

  void *pKey = NULL; int nKey = 0;
  kvstore_iterator_key(it, &pKey, &nKey);
  ASSERT("at aaa (nearest <=)", nKey == 3 && memcmp(pKey, "aaa", 3) == 0);
  /* pKey is an internal iterator buffer — do not free */

  kvstore_iterator_close(it);
  cleanup(&pKV, path);
}

static void test11_reverse_seek_before_first(void){
  printf("\nTest 11: reverse seek before first key → eof\n");
  const char *path = "test_new_apis_11.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "mmm", 3, "v1", 2);
  kvstore_put(pKV, "nnn", 3, "v2", 2);

  KVIterator *it = NULL;
  kvstore_reverse_iterator_create(pKV, &it);

  /* seek to "aaa" — before all keys, no key <= "aaa" */
  int rc = kvstore_iterator_seek(it, "aaa", 3);
  ASSERT("reverse seek OK", rc == KVSTORE_OK);
  ASSERT("eof (nothing <=)", kvstore_iterator_eof(it));

  kvstore_iterator_close(it);
  cleanup(&pKV, path);
}

static void test12_multiple_seeks(void){
  printf("\nTest 12: multiple sequential seeks\n");
  const char *path = "test_new_apis_12.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "aaa", 3, "v1", 2);
  kvstore_put(pKV, "bbb", 3, "v2", 2);
  kvstore_put(pKV, "ccc", 3, "v3", 2);

  KVIterator *it = NULL;
  kvstore_iterator_create(pKV, &it);

  void *pKey = NULL; int nKey = 0;

  kvstore_iterator_seek(it, "ccc", 3);
  kvstore_iterator_key(it, &pKey, &nKey);
  ASSERT("first seek at ccc", nKey == 3 && memcmp(pKey, "ccc", 3) == 0);
  /* pKey is an internal iterator buffer — do not free */

  /* seek backward to aaa */
  kvstore_iterator_seek(it, "aaa", 3);
  kvstore_iterator_key(it, &pKey, &nKey);
  ASSERT("second seek at aaa", nKey == 3 && memcmp(pKey, "aaa", 3) == 0);
  /* pKey is an internal iterator buffer — do not free */

  /* seek to bbb */
  kvstore_iterator_seek(it, "bbb", 3);
  kvstore_iterator_key(it, &pKey, &nKey);
  ASSERT("third seek at bbb", nKey == 3 && memcmp(pKey, "bbb", 3) == 0);
  /* pKey is an internal iterator buffer — do not free */

  kvstore_iterator_close(it);
  cleanup(&pKV, path);
}

/* ====================================================================== */
/* --- kvstore_put_if_absent --- */
/* ====================================================================== */

static void test13_absent_no_ttl(void){
  printf("\nTest 13: put_if_absent, key absent, no TTL\n");
  const char *path = "test_new_apis_13.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  int inserted = -1;
  int rc = kvstore_put_if_absent(pKV, "key", 3, "val", 3, 0, &inserted);
  ASSERT("rc=OK", rc == KVSTORE_OK);
  ASSERT("inserted=1", inserted == 1);

  void *pVal = NULL; int nVal = 0;
  kvstore_get(pKV, "key", 3, &pVal, &nVal);
  ASSERT("value readable", pVal && nVal == 3 && memcmp(pVal, "val", 3) == 0);
  if( pVal ) snkv_free(pVal);

  cleanup(&pKV, path);
}

static void test14_key_present(void){
  printf("\nTest 14: put_if_absent, key already present → not overwritten\n");
  const char *path = "test_new_apis_14.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "key", 3, "original", 8);

  int inserted = -1;
  int rc = kvstore_put_if_absent(pKV, "key", 3, "new_val", 7, 0, &inserted);
  ASSERT("rc=OK", rc == KVSTORE_OK);
  ASSERT("inserted=0", inserted == 0);

  void *pVal = NULL; int nVal = 0;
  kvstore_get(pKV, "key", 3, &pVal, &nVal);
  ASSERT("original value unchanged", pVal && nVal == 8 && memcmp(pVal, "original", 8) == 0);
  if( pVal ) snkv_free(pVal);

  cleanup(&pKV, path);
}

static void test15_absent_with_ttl(void){
  printf("\nTest 15: put_if_absent, key absent, with TTL\n");
  const char *path = "test_new_apis_15.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  int64_t expire_ms = kvstore_now_ms() + 10000; /* 10 s */
  int inserted = -1;
  int rc = kvstore_put_if_absent(pKV, "key", 3, "val", 3, expire_ms, &inserted);
  ASSERT("rc=OK", rc == KVSTORE_OK);
  ASSERT("inserted=1", inserted == 1);

  int64_t remaining = 0;
  rc = kvstore_ttl_remaining(pKV, "key", 3, &remaining);
  ASSERT("ttl_remaining OK", rc == KVSTORE_OK);
  ASSERT("remaining > 0", remaining > 0 && remaining <= 10000);

  cleanup(&pKV, path);
}

static void test16_present_with_ttl(void){
  printf("\nTest 16: put_if_absent, key present with TTL → not overwritten\n");
  const char *path = "test_new_apis_16.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  int64_t expire_ms1 = kvstore_now_ms() + 5000;
  kvstore_put_ttl(pKV, "key", 3, "original", 8, expire_ms1);

  int64_t expire_ms2 = kvstore_now_ms() + 20000;
  int inserted = -1;
  int rc = kvstore_put_if_absent(pKV, "key", 3, "new", 3, expire_ms2, &inserted);
  ASSERT("rc=OK", rc == KVSTORE_OK);
  ASSERT("inserted=0", inserted == 0);

  void *pVal = NULL; int nVal = 0;
  kvstore_get(pKV, "key", 3, &pVal, &nVal);
  ASSERT("original value unchanged", pVal && nVal == 8 && memcmp(pVal, "original", 8) == 0);
  if( pVal ) snkv_free(pVal);

  /* Original TTL should be intact (remaining is near expire_ms1 - now) */
  int64_t remaining = 0;
  kvstore_ttl_remaining(pKV, "key", 3, &remaining);
  ASSERT("original TTL intact (<=5s)", remaining > 0 && remaining <= 5000);

  cleanup(&pKV, path);
}

static void test17_expired_treated_absent(void){
  printf("\nTest 17: expired key treated as absent, new value inserted\n");
  const char *path = "test_new_apis_17.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  /* Write key with TTL in the past */
  int64_t past_ms = kvstore_now_ms() - 1000;
  kvstore_put_ttl(pKV, "key", 3, "old_val", 7, past_ms);

  int inserted = -1;
  int rc = kvstore_put_if_absent(pKV, "key", 3, "new_val", 7, 0, &inserted);
  ASSERT("rc=OK", rc == KVSTORE_OK);
  ASSERT("inserted=1 (expired → absent)", inserted == 1);

  void *pVal = NULL; int nVal = 0;
  kvstore_get(pKV, "key", 3, &pVal, &nVal);
  ASSERT("new value present", pVal && nVal == 7 && memcmp(pVal, "new_val", 7) == 0);
  if( pVal ) snkv_free(pVal);

  cleanup(&pKV, path);
}

static void test18_cf_variant(void){
  printf("\nTest 18: CF-level put_if_absent\n");
  const char *path = "test_new_apis_18.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  KVColumnFamily *pCF = NULL;
  kvstore_cf_create(pKV, "myCF", &pCF);

  int inserted = -1;
  int rc = kvstore_cf_put_if_absent(pCF, "k", 1, "v", 1, 0, &inserted);
  ASSERT("CF insert OK", rc == KVSTORE_OK);
  ASSERT("CF inserted=1", inserted == 1);

  rc = kvstore_cf_put_if_absent(pCF, "k", 1, "v2", 2, 0, &inserted);
  ASSERT("CF second insert OK", rc == KVSTORE_OK);
  ASSERT("CF inserted=0 (key exists)", inserted == 0);

  kvstore_cf_close(pCF);
  cleanup(&pKV, path);
}

static void test19_null_pinserted(void){
  printf("\nTest 19: pInserted=NULL → no crash\n");
  const char *path = "test_new_apis_19.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  int rc = kvstore_put_if_absent(pKV, "k", 1, "v", 1, 0, NULL);
  ASSERT("rc=OK with NULL pInserted", rc == KVSTORE_OK);

  void *pVal = NULL; int nVal = 0;
  rc = kvstore_get(pKV, "k", 1, &pVal, &nVal);
  ASSERT("value readable", rc == KVSTORE_OK && pVal && nVal == 1);
  if( pVal ) snkv_free(pVal);

  cleanup(&pKV, path);
}

static void test20_explicit_transaction(void){
  printf("\nTest 20: put_if_absent inside explicit write transaction\n");
  const char *path = "test_new_apis_20.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  int rc = kvstore_begin(pKV, 1 /*write*/);
  ASSERT("begin OK", rc == KVSTORE_OK);

  int inserted = -1;
  rc = kvstore_put_if_absent(pKV, "key1", 4, "val1", 4, 0, &inserted);
  ASSERT("pia inside txn OK", rc == KVSTORE_OK);
  ASSERT("inserted=1", inserted == 1);

  rc = kvstore_put_if_absent(pKV, "key1", 4, "val2", 4, 0, &inserted);
  ASSERT("second pia OK", rc == KVSTORE_OK);
  ASSERT("inserted=0", inserted == 0);

  rc = kvstore_commit(pKV);
  ASSERT("commit OK", rc == KVSTORE_OK);

  void *pVal = NULL; int nVal = 0;
  kvstore_get(pKV, "key1", 4, &pVal, &nVal);
  ASSERT("original val1 persisted", pVal && nVal == 4 && memcmp(pVal, "val1", 4) == 0);
  if( pVal ) snkv_free(pVal);

  cleanup(&pKV, path);
}

/* ====================================================================== */
/* --- kvstore_clear / kvstore_cf_clear --- */
/* ====================================================================== */

static void test21_clear_default(void){
  printf("\nTest 21: clear default CF → count=0, eof immediately\n");
  const char *path = "test_new_apis_21.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "a", 1, "v", 1);
  kvstore_put(pKV, "b", 1, "v", 1);
  kvstore_put(pKV, "c", 1, "v", 1);

  int rc = kvstore_clear(pKV);
  ASSERT("clear OK", rc == KVSTORE_OK);

  int64_t n = -1;
  kvstore_count(pKV, &n);
  ASSERT("count=0 after clear", n == 0);

  KVIterator *it = NULL;
  kvstore_iterator_create(pKV, &it);
  kvstore_iterator_first(it);
  ASSERT("iterator eof after clear", kvstore_iterator_eof(it));
  kvstore_iterator_close(it);

  cleanup(&pKV, path);
}

static void test22_clear_with_ttl(void){
  printf("\nTest 22: clear CF with TTL keys → TTL index empty\n");
  const char *path = "test_new_apis_22.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  int64_t expire_ms = kvstore_now_ms() - 100; /* already expired */
  kvstore_put_ttl(pKV, "k1", 2, "v1", 2, expire_ms);
  kvstore_put_ttl(pKV, "k2", 2, "v2", 2, expire_ms);

  int rc = kvstore_clear(pKV);
  ASSERT("clear OK", rc == KVSTORE_OK);

  int n_purged = -1;
  rc = kvstore_purge_expired(pKV, &n_purged);
  ASSERT("purge_expired OK", rc == KVSTORE_OK);
  ASSERT("no TTL entries remain", n_purged == 0);

  cleanup(&pKV, path);
}

static void test23_clear_after_reopen(void){
  printf("\nTest 23: clear CF with TTL from previous session\n");
  const char *path = "test_new_apis_23.db";

  /* Session 1: write TTL keys */
  {
    KVStore *pKV = openFresh(path);
    if( !pKV ) return;
    int64_t expire_ms = kvstore_now_ms() - 100; /* past */
    kvstore_put_ttl(pKV, "k1", 2, "v1", 2, expire_ms);
    kvstore_put_ttl(pKV, "k2", 2, "v2", 2, expire_ms);
    kvstore_close(pKV);
  }

  /* Session 2: reopen and clear — must clear TTL CFs even though pTtlKeyCF is NULL */
  {
    KVStore *pKV = NULL;
    int rc = kvstore_open(path, &pKV, KVSTORE_JOURNAL_WAL);
    ASSERT("reopen OK", rc == KVSTORE_OK && pKV);
    if( !pKV ) return;

    rc = kvstore_clear(pKV);
    ASSERT("clear after reopen OK", rc == KVSTORE_OK);

    int n_purged = -1;
    rc = kvstore_purge_expired(pKV, &n_purged);
    ASSERT("no TTL entries remain after clear+reopen", rc == KVSTORE_OK && n_purged == 0);

    kvstore_close(pKV);
  }

  remove(path);
  char w[512]; snprintf(w, sizeof(w), "%s-wal", path); remove(w);
  char s[512]; snprintf(s, sizeof(s), "%s-shm", path); remove(s);
}

static void test24_clear_empty(void){
  printf("\nTest 24: clear empty CF → KVSTORE_OK\n");
  const char *path = "test_new_apis_24.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  int rc = kvstore_clear(pKV);
  ASSERT("clear empty CF OK", rc == KVSTORE_OK);

  cleanup(&pKV, path);
}

static void test25_clear_then_reinsert(void){
  printf("\nTest 25: clear then reinsert works correctly\n");
  const char *path = "test_new_apis_25.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "a", 1, "va", 2);
  kvstore_put(pKV, "b", 1, "vb", 2);
  kvstore_clear(pKV);

  kvstore_put(pKV, "x", 1, "vx", 2);
  kvstore_put(pKV, "y", 1, "vy", 2);

  int64_t n = -1;
  kvstore_count(pKV, &n);
  ASSERT("count=2 after reinsert", n == 2);

  void *pVal = NULL; int nVal = 0;
  kvstore_get(pKV, "x", 1, &pVal, &nVal);
  ASSERT("x readable", pVal && nVal == 2 && memcmp(pVal, "vx", 2) == 0);
  if( pVal ) snkv_free(pVal);

  cleanup(&pKV, path);
}

static void test26_clear_one_cf(void){
  printf("\nTest 26: clear one CF, other CFs unaffected\n");
  const char *path = "test_new_apis_26.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "default_key", 11, "v", 1);

  KVColumnFamily *pCF = NULL;
  kvstore_cf_create(pKV, "other", &pCF);
  {
    int ins = 0;
    kvstore_cf_put_if_absent(pCF, "cf_key", 6, "v", 1, 0, &ins);
  }

  /* Clear only the default CF */
  int rc = kvstore_clear(pKV);
  ASSERT("clear default CF OK", rc == KVSTORE_OK);

  /* Default CF is empty */
  int exists = 0;
  kvstore_exists(pKV, "default_key", 11, &exists);
  ASSERT("default_key gone", !exists);

  /* Other CF still has its key */
  kvstore_cf_exists(pCF, "cf_key", 6, &exists);
  ASSERT("cf_key still present in other CF", exists);

  kvstore_cf_close(pCF);
  cleanup(&pKV, path);
}

/* ====================================================================== */
/* --- Extended KVStoreStats --- */
/* ====================================================================== */

static void test27_bytes_written(void){
  printf("\nTest 27: nBytesWritten increments by key+value\n");
  const char *path = "test_new_apis_27.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_stats_reset(pKV);

  /* key=5 bytes, value=10 bytes */
  kvstore_put(pKV, "hello", 5, "0123456789", 10);

  KVStoreStats st = {0};
  kvstore_stats(pKV, &st);
  ASSERT("nBytesWritten == 15", st.nBytesWritten == 15);

  cleanup(&pKV, path);
}

static void test28_bytes_read(void){
  printf("\nTest 28: nBytesRead increments by value size\n");
  const char *path = "test_new_apis_28.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "k", 1, "hello", 5);
  kvstore_stats_reset(pKV);

  void *pVal = NULL; int nVal = 0;
  kvstore_get(pKV, "k", 1, &pVal, &nVal);
  if( pVal ) snkv_free(pVal);

  KVStoreStats st = {0};
  kvstore_stats(pKV, &st);
  ASSERT("nBytesRead == 5", st.nBytesRead == 5);

  cleanup(&pKV, path);
}

static void test29_wal_commits(void){
  printf("\nTest 29: nWalCommits increments on each commit\n");
  const char *path = "test_new_apis_29.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_stats_reset(pKV);

  /* Each put in autoTrans mode issues one commit → one nWalCommits */
  kvstore_put(pKV, "k1", 2, "v", 1);
  kvstore_put(pKV, "k2", 2, "v", 1);

  KVStoreStats st = {0};
  kvstore_stats(pKV, &st);
  ASSERT("nWalCommits >= 2", st.nWalCommits >= 2);

  cleanup(&pKV, path);
}

static void test30_ttl_expired_counter(void){
  printf("\nTest 30: nTtlExpired increments on lazy expiry\n");
  const char *path = "test_new_apis_30.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  int64_t past_ms = kvstore_now_ms() - 1000;
  kvstore_put_ttl(pKV, "k", 1, "v", 1, past_ms);

  kvstore_stats_reset(pKV);

  /* get_ttl triggers lazy expiry */
  void *pVal = NULL; int nVal = 0;
  int64_t remaining = 0;
  kvstore_get_ttl(pKV, "k", 1, &pVal, &nVal, &remaining);
  if( pVal ) snkv_free(pVal);

  KVStoreStats st = {0};
  kvstore_stats(pKV, &st);
  ASSERT("nTtlExpired == 1", st.nTtlExpired == 1);

  cleanup(&pKV, path);
}

static void test31_ttl_purged_counter(void){
  printf("\nTest 31: nTtlPurged increments after purge_expired\n");
  const char *path = "test_new_apis_31.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  int64_t past_ms = kvstore_now_ms() - 1000;
  kvstore_put_ttl(pKV, "k1", 2, "v", 1, past_ms);
  kvstore_put_ttl(pKV, "k2", 2, "v", 1, past_ms);

  kvstore_stats_reset(pKV);

  int n_purged = 0;
  kvstore_purge_expired(pKV, &n_purged);

  KVStoreStats st = {0};
  kvstore_stats(pKV, &st);
  ASSERT("nTtlPurged == 2", st.nTtlPurged == 2);

  cleanup(&pKV, path);
}

static void test32_db_pages(void){
  printf("\nTest 32: nDbPages > 0 after writes\n");
  const char *path = "test_new_apis_32.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "k", 1, "v", 1);

  KVStoreStats st = {0};
  kvstore_stats(pKV, &st);
  ASSERT("nDbPages > 0", st.nDbPages > 0);

  cleanup(&pKV, path);
}

static void test33_stats_reset(void){
  printf("\nTest 33: kvstore_stats_reset zeros counters, nDbPages still accurate\n");
  const char *path = "test_new_apis_33.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "k", 1, "v", 1);
  void *pVal = NULL; int nVal = 0;
  kvstore_get(pKV, "k", 1, &pVal, &nVal);
  if( pVal ) snkv_free(pVal);

  int rc = kvstore_stats_reset(pKV);
  ASSERT("stats_reset OK", rc == KVSTORE_OK);

  KVStoreStats st = {0};
  kvstore_stats(pKV, &st);
  ASSERT("nPuts=0 after reset", st.nPuts == 0);
  ASSERT("nGets=0 after reset", st.nGets == 0);
  ASSERT("nBytesRead=0 after reset", st.nBytesRead == 0);
  ASSERT("nBytesWritten=0 after reset", st.nBytesWritten == 0);
  ASSERT("nWalCommits=0 after reset", st.nWalCommits == 0);
  ASSERT("nDbPages still > 0 after reset", st.nDbPages > 0);

  cleanup(&pKV, path);
}

/* ====================================================================== */
/* --- kvstore_count / kvstore_cf_count --- */
/* ====================================================================== */

static void test34_count_empty(void){
  printf("\nTest 34: count on empty CF → 0\n");
  const char *path = "test_new_apis_34.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  int64_t n = -1;
  int rc = kvstore_count(pKV, &n);
  ASSERT("count OK on empty", rc == KVSTORE_OK);
  ASSERT("count == 0", n == 0);

  cleanup(&pKV, path);
}

static void test35_count_after_puts(void){
  printf("\nTest 35: count after N puts → N\n");
  const char *path = "test_new_apis_35.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  for( int i = 0; i < 10; i++ ){
    char key[8]; snprintf(key, sizeof(key), "key%02d", i);
    kvstore_put(pKV, key, (int)strlen(key), "v", 1);
  }

  int64_t n = -1;
  kvstore_count(pKV, &n);
  ASSERT("count == 10", n == 10);

  cleanup(&pKV, path);
}

static void test36_count_after_delete(void){
  printf("\nTest 36: count after put + delete → N-1\n");
  const char *path = "test_new_apis_36.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "a", 1, "v", 1);
  kvstore_put(pKV, "b", 1, "v", 1);
  kvstore_put(pKV, "c", 1, "v", 1);
  kvstore_delete(pKV, "b", 1);

  int64_t n = -1;
  kvstore_count(pKV, &n);
  ASSERT("count == 2", n == 2);

  cleanup(&pKV, path);
}

static void test37_count_includes_expired(void){
  printf("\nTest 37: count includes expired-but-not-purged keys\n");
  const char *path = "test_new_apis_37.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  int64_t past_ms = kvstore_now_ms() - 1000;
  kvstore_put_ttl(pKV, "expired1", 8, "v", 1, past_ms);
  kvstore_put_ttl(pKV, "expired2", 8, "v", 1, past_ms);
  kvstore_put(pKV, "live", 4, "v", 1);

  int64_t n = -1;
  kvstore_count(pKV, &n);
  ASSERT("count includes expired keys (==3)", n == 3);

  cleanup(&pKV, path);
}

static void test38_count_after_purge(void){
  printf("\nTest 38: count after purge_expired → only live keys\n");
  const char *path = "test_new_apis_38.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  int64_t past_ms = kvstore_now_ms() - 1000;
  kvstore_put_ttl(pKV, "expired1", 8, "v", 1, past_ms);
  kvstore_put_ttl(pKV, "expired2", 8, "v", 1, past_ms);
  kvstore_put(pKV, "live", 4, "v", 1);

  kvstore_purge_expired(pKV, NULL);

  int64_t n = -1;
  kvstore_count(pKV, &n);
  ASSERT("count == 1 after purge", n == 1);

  cleanup(&pKV, path);
}

static void test39_cf_count_isolation(void){
  printf("\nTest 39: CF count counts only that CF (not TTL index or default)\n");
  const char *path = "test_new_apis_39.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  /* Default CF: 5 keys */
  for( int i = 0; i < 5; i++ ){
    char k[8]; snprintf(k, sizeof(k), "d%d", i);
    kvstore_put(pKV, k, (int)strlen(k), "v", 1);
  }

  /* Named CF: 3 keys with TTL (to make sure TTL index CFs don't inflate count) */
  KVColumnFamily *pCF = NULL;
  kvstore_cf_create(pKV, "myns", &pCF);
  int64_t expire_ms = kvstore_now_ms() + 10000;
  int ins = 0;
  kvstore_cf_put_if_absent(pCF, "a", 1, "v", 1, expire_ms, &ins);
  kvstore_cf_put_if_absent(pCF, "b", 1, "v", 1, expire_ms, &ins);
  kvstore_cf_put_if_absent(pCF, "c", 1, "v", 1, expire_ms, &ins);

  int64_t n_cf = -1;
  int rc = kvstore_cf_count(pCF, &n_cf);
  ASSERT("CF count OK", rc == KVSTORE_OK);
  ASSERT("CF count == 3 (not TTL entries)", n_cf == 3);

  int64_t n_default = -1;
  kvstore_count(pKV, &n_default);
  ASSERT("default CF count == 5", n_default == 5);

  kvstore_cf_close(pCF);
  cleanup(&pKV, path);
}

static void test40_kvstore_count_default(void){
  printf("\nTest 40: kvstore_count delegates to default CF\n");
  const char *path = "test_new_apis_40.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  kvstore_put(pKV, "one",   3, "v", 1);
  kvstore_put(pKV, "two",   3, "v", 1);
  kvstore_put(pKV, "three", 5, "v", 1);

  int64_t n = -1;
  int rc = kvstore_count(pKV, &n);
  ASSERT("kvstore_count OK", rc == KVSTORE_OK);
  ASSERT("kvstore_count == 3", n == 3);

  cleanup(&pKV, path);
}

/* ====================================================================== */
/* --- clear reduces page count --- */
/* ====================================================================== */

static void test41_clear_reduces_pages(void){
  printf("\nTest 41: fill + clear + checkpoint TRUNCATE → nDbPages decreases\n");
  const char *path = "test_new_apis_41.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ) return;

  /* Insert enough keys to force multiple B-tree pages. */
  char key[32], val[256];
  memset(val, 'X', sizeof(val));
  for( int i = 0; i < 2000; i++ ){
    int nKey = snprintf(key, sizeof(key), "key%07d", i);
    kvstore_put(pKV, key, nKey, val, (int)sizeof(val));
  }

  KVStoreStats st_before = {0};
  kvstore_stats(pKV, &st_before);

  /* Clear all keys. */
  int rc = kvstore_clear(pKV);
  ASSERT("clear OK", rc == KVSTORE_OK);

  /* Incremental vacuum reclaims the freed pages back to the OS,
     shrinking nDbPages. 0 = drain the entire free list. */
  kvstore_incremental_vacuum(pKV, 0);

  KVStoreStats st_after = {0};
  kvstore_stats(pKV, &st_after);

  ASSERT("nDbPages before clear > 0", st_before.nDbPages > 0);
  ASSERT("nDbPages after clear+vacuum < before",
         st_after.nDbPages < st_before.nDbPages);

  /* Close so the pager flushes and truncates the file, then check size. */
  kvstore_close(pKV);
  pKV = NULL;

  /* Re-use st_before.nDbPages * 4096 as a proxy for the pre-clear file size. */
  long size_before = (long)st_before.nDbPages * 4096;
  struct stat sb = {0};
  stat(path, &sb);
  long size_after = (long)sb.st_size;

  ASSERT("file size before clear > 0", size_before > 0);
  ASSERT("file size after clear+vacuum < before", size_after < size_before);

  /* Remove DB files manually since pKV is already closed. */
  remove(path);
  char wal_path[256], shm_path[256];
  snprintf(wal_path, sizeof(wal_path), "%s-wal", path);
  snprintf(shm_path, sizeof(shm_path), "%s-shm", path);
  remove(wal_path);
  remove(shm_path);
}

/* ====================================================================== */
/* --- main --- */
/* ====================================================================== */

int main(void){
  printf("=== test_new_apis: kvstore_iterator_seek, put_if_absent, clear, stats, count ===\n");

  /* seek */
  test1_seek_existing();
  test2_seek_between();
  test3_seek_past_last();
  test4_seek_before_first();
  test5_seek_prefix_within();
  test6_seek_prefix_outside();
  test7_seek_then_next();
  test8_seek_empty();
  test9_reverse_seek_exact();
  test10_reverse_seek_between();
  test11_reverse_seek_before_first();
  test12_multiple_seeks();

  /* put_if_absent */
  test13_absent_no_ttl();
  test14_key_present();
  test15_absent_with_ttl();
  test16_present_with_ttl();
  test17_expired_treated_absent();
  test18_cf_variant();
  test19_null_pinserted();
  test20_explicit_transaction();

  /* clear */
  test21_clear_default();
  test22_clear_with_ttl();
  test23_clear_after_reopen();
  test24_clear_empty();
  test25_clear_then_reinsert();
  test26_clear_one_cf();

  /* extended stats */
  test27_bytes_written();
  test28_bytes_read();
  test29_wal_commits();
  test30_ttl_expired_counter();
  test31_ttl_purged_counter();
  test32_db_pages();
  test33_stats_reset();

  /* count */
  test34_count_empty();
  test35_count_after_puts();
  test36_count_after_delete();
  test37_count_includes_expired();
  test38_count_after_purge();
  test39_cf_count_isolation();
  test40_kvstore_count_default();

  /* clear reduces page count */
  test41_clear_reduces_pages();

  printf("\n=== Results: %d passed, %d failed ===\n", passed, failed);
  return (failed > 0) ? 1 : 0;
}
