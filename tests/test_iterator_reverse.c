/* SPDX-License-Identifier: Apache-2.0 */
/*
** tests/test_iterator_reverse.c — Reverse iterator regression suite
**
** Tests:
**   1.  rev_basic           — 10 keys, reverse iter, keys in descending order
**   2.  rev_empty           — reverse iter on empty CF → eof immediately
**   3.  rev_single          — reverse iter on single key → one result, then eof
**   4.  rev_last_first      — iterator_last + iterator_prev repeatedly → all keys
**   5.  rev_cf             — reverse iterator on a named CF
**   6.  rev_multi_cf        — two CFs, independent reverse iterators
**   7.  rev_ttl_skip        — expired keys skipped transparently
**   8.  direction_mismatch  — iterator_last/prev on forward iter → KVSTORE_ERROR
**   9.  rev_with_fwd_prefix — forward prefix iterator unaffected by new field
**  10.  rev_prefix_basic    — reverse prefix iter: only matching keys, descending
**  11.  rev_prefix_empty    — reverse prefix iter, no keys match → eof immediately
**  12.  rev_prefix_all_ff   — prefix with all-0xFF bytes → BtreeLast fallback
**  13.  rev_prefix_ttl_skip — reverse prefix iter skips expired keys
*/

#include "kvstore.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

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

#define ASSERT(name, expr)  check(name, (int)(expr))

static KVStore *openFresh(const char *path){
  remove(path);
  char buf[512];
  snprintf(buf, sizeof(buf), "%s-wal", path); remove(buf);
  snprintf(buf, sizeof(buf), "%s-shm", path); remove(buf);
  KVStore *pKV = NULL;
  int rc = kvstore_open(path, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){
    fprintf(stderr, "openFresh: failed rc=%d\n", rc);
    return NULL;
  }
  return pKV;
}

static void cleanup(KVStore **ppKV, const char *path){
  if( *ppKV ){ kvstore_close(*ppKV); *ppKV = NULL; }
  remove(path);
  char buf[512];
  snprintf(buf, sizeof(buf), "%s-wal", path); remove(buf);
  snprintf(buf, sizeof(buf), "%s-shm", path); remove(buf);
}

/* ---- Test 1: basic reverse iteration ---- */
static void test1_rev_basic(void){
  printf("\nTest 1: rev_basic\n");
  const char *DB = "test_rev_basic.db";
  KVStore *kv = openFresh(DB);
  if( !kv ){ ASSERT("openFresh", 0); return; }

  /* Insert key_00 .. key_09 in forward order */
  kvstore_begin(kv, 1);
  int i;
  for( i = 0; i < 10; i++ ){
    char k[16], v[16];
    snprintf(k, sizeof(k), "key_%02d", i);
    snprintf(v, sizeof(v), "val_%02d", i);
    kvstore_put(kv, k, (int)strlen(k), v, (int)strlen(v));
  }
  kvstore_commit(kv);

  KVIterator *it = NULL;
  ASSERT("create reverse iter", kvstore_reverse_iterator_create(kv, &it) == KVSTORE_OK);
  ASSERT("last ok", kvstore_iterator_last(it) == KVSTORE_OK);
  ASSERT("not eof after last", !kvstore_iterator_eof(it));

  /* Keys must come out in descending order: key_09, key_08, ... key_00 */
  int order_ok = 1;
  int count = 0;
  for( i = 9; i >= 0; i-- ){
    if( kvstore_iterator_eof(it) ){ order_ok = 0; break; }
    void *pk; int nk;
    kvstore_iterator_key(it, &pk, &nk);
    char expected[16];
    snprintf(expected, sizeof(expected), "key_%02d", i);
    if( nk != (int)strlen(expected) || memcmp(pk, expected, nk) != 0 ){
      order_ok = 0;
    }
    count++;
    kvstore_iterator_prev(it);
  }
  ASSERT("all 10 keys in descending order", order_ok && count == 10);
  ASSERT("eof after last key", kvstore_iterator_eof(it));

  kvstore_iterator_close(it);
  cleanup(&kv, DB);
}

/* ---- Test 2: empty CF ---- */
static void test2_rev_empty(void){
  printf("\nTest 2: rev_empty\n");
  const char *DB = "test_rev_empty.db";
  KVStore *kv = openFresh(DB);
  if( !kv ){ ASSERT("openFresh", 0); return; }

  KVIterator *it = NULL;
  ASSERT("create ok", kvstore_reverse_iterator_create(kv, &it) == KVSTORE_OK);
  ASSERT("last ok on empty", kvstore_iterator_last(it) == KVSTORE_OK);
  ASSERT("eof immediately on empty", kvstore_iterator_eof(it));

  kvstore_iterator_close(it);
  cleanup(&kv, DB);
}

/* ---- Test 3: single key ---- */
static void test3_rev_single(void){
  printf("\nTest 3: rev_single\n");
  const char *DB = "test_rev_single.db";
  KVStore *kv = openFresh(DB);
  if( !kv ){ ASSERT("openFresh", 0); return; }

  kvstore_put(kv, "only", 4, "value", 5);

  KVIterator *it = NULL;
  kvstore_reverse_iterator_create(kv, &it);
  kvstore_iterator_last(it);
  ASSERT("not eof", !kvstore_iterator_eof(it));

  void *pk; int nk;
  kvstore_iterator_key(it, &pk, &nk);
  ASSERT("key is 'only'", nk == 4 && memcmp(pk, "only", 4) == 0);

  kvstore_iterator_prev(it);
  ASSERT("eof after prev on single key", kvstore_iterator_eof(it));

  kvstore_iterator_close(it);
  cleanup(&kv, DB);
}

/* ---- Test 4: iterator_last + iterator_prev covers all keys ---- */
static void test4_rev_last_first(void){
  printf("\nTest 4: rev_last_first\n");
  const char *DB = "test_rev_last_first.db";
  KVStore *kv = openFresh(DB);
  if( !kv ){ ASSERT("openFresh", 0); return; }

  int N = 50;
  kvstore_begin(kv, 1);
  int i;
  for( i = 0; i < N; i++ ){
    char k[16], v[8];
    snprintf(k, sizeof(k), "k%04d", i);
    snprintf(v, sizeof(v), "v%04d", i);
    kvstore_put(kv, k, (int)strlen(k), v, (int)strlen(v));
  }
  kvstore_commit(kv);

  KVIterator *it = NULL;
  kvstore_reverse_iterator_create(kv, &it);
  kvstore_iterator_last(it);

  int count = 0;
  while( !kvstore_iterator_eof(it) ){
    count++;
    kvstore_iterator_prev(it);
  }
  ASSERT("all 50 keys visited", count == N);

  kvstore_iterator_close(it);
  cleanup(&kv, DB);
}

/* ---- Test 5: reverse iter on named CF ---- */
static void test5_rev_cf(void){
  printf("\nTest 5: rev_cf\n");
  const char *DB = "test_rev_cf.db";
  KVStore *kv = openFresh(DB);
  if( !kv ){ ASSERT("openFresh", 0); return; }

  KVColumnFamily *cf = NULL;
  ASSERT("cf_create", kvstore_cf_create(kv, "mycf", &cf) == KVSTORE_OK);

  kvstore_begin(kv, 1);
  kvstore_cf_put(cf, "aaa", 3, "1", 1);
  kvstore_cf_put(cf, "bbb", 3, "2", 1);
  kvstore_cf_put(cf, "ccc", 3, "3", 1);
  kvstore_commit(kv);

  KVIterator *it = NULL;
  ASSERT("cf_reverse_iter create", kvstore_cf_reverse_iterator_create(cf, &it) == KVSTORE_OK);
  ASSERT("last ok", kvstore_iterator_last(it) == KVSTORE_OK);
  ASSERT("not eof", !kvstore_iterator_eof(it));

  void *pk; int nk;
  kvstore_iterator_key(it, &pk, &nk);
  ASSERT("first key is ccc", nk == 3 && memcmp(pk, "ccc", 3) == 0);

  kvstore_iterator_prev(it);
  kvstore_iterator_key(it, &pk, &nk);
  ASSERT("second key is bbb", nk == 3 && memcmp(pk, "bbb", 3) == 0);

  kvstore_iterator_prev(it);
  kvstore_iterator_key(it, &pk, &nk);
  ASSERT("third key is aaa", nk == 3 && memcmp(pk, "aaa", 3) == 0);

  kvstore_iterator_prev(it);
  ASSERT("eof after all keys", kvstore_iterator_eof(it));

  kvstore_iterator_close(it);
  kvstore_cf_close(cf);
  cleanup(&kv, DB);
}

/* ---- Test 6: two CFs with independent reverse iterators ---- */
static void test6_rev_multi_cf(void){
  printf("\nTest 6: rev_multi_cf\n");
  const char *DB = "test_rev_multi_cf.db";
  KVStore *kv = openFresh(DB);
  if( !kv ){ ASSERT("openFresh", 0); return; }

  KVColumnFamily *cf1 = NULL, *cf2 = NULL;
  kvstore_cf_create(kv, "cf1", &cf1);
  kvstore_cf_create(kv, "cf2", &cf2);

  kvstore_begin(kv, 1);
  kvstore_cf_put(cf1, "a", 1, "1", 1);
  kvstore_cf_put(cf1, "b", 1, "2", 1);
  kvstore_cf_put(cf2, "x", 1, "X", 1);
  kvstore_cf_put(cf2, "y", 1, "Y", 1);
  kvstore_commit(kv);

  KVIterator *it1 = NULL, *it2 = NULL;
  kvstore_cf_reverse_iterator_create(cf1, &it1);
  kvstore_cf_reverse_iterator_create(cf2, &it2);
  kvstore_iterator_last(it1);
  kvstore_iterator_last(it2);

  void *pk; int nk;

  kvstore_iterator_key(it1, &pk, &nk);
  ASSERT("cf1 last key is b", nk == 1 && *(char*)pk == 'b');
  kvstore_iterator_key(it2, &pk, &nk);
  ASSERT("cf2 last key is y", nk == 1 && *(char*)pk == 'y');

  kvstore_iterator_prev(it1);
  kvstore_iterator_key(it1, &pk, &nk);
  ASSERT("cf1 prev key is a", nk == 1 && *(char*)pk == 'a');

  kvstore_iterator_prev(it2);
  kvstore_iterator_key(it2, &pk, &nk);
  ASSERT("cf2 prev key is x", nk == 1 && *(char*)pk == 'x');

  kvstore_iterator_close(it1);
  kvstore_iterator_close(it2);
  kvstore_cf_close(cf1);
  kvstore_cf_close(cf2);
  cleanup(&kv, DB);
}

/* ---- Test 7: expired keys skipped (TTL) ---- */
static void test7_rev_ttl_skip(void){
  printf("\nTest 7: rev_ttl_skip\n");
  const char *DB = "test_rev_ttl_skip.db";
  KVStore *kv = openFresh(DB);
  if( !kv ){ ASSERT("openFresh", 0); return; }

  /* Insert live keys and one already-expired key */
  kvstore_put(kv, "aaa", 3, "live1", 5);
  int64_t already_past = kvstore_now_ms() - 5000;
  kvstore_put_ttl(kv, "bbb", 3, "expired", 7, already_past);
  kvstore_put(kv, "ccc", 3, "live2", 5);

  KVIterator *it = NULL;
  kvstore_reverse_iterator_create(kv, &it);
  kvstore_iterator_last(it);

  int count = 0;
  int saw_expired = 0;
  while( !kvstore_iterator_eof(it) ){
    void *pk; int nk;
    kvstore_iterator_key(it, &pk, &nk);
    if( nk == 3 && memcmp(pk, "bbb", 3) == 0 ) saw_expired = 1;
    count++;
    kvstore_iterator_prev(it);
  }
  ASSERT("expired key not yielded", !saw_expired);
  ASSERT("exactly 2 live keys seen", count == 2);

  kvstore_iterator_close(it);
  cleanup(&kv, DB);
}

/* ---- Test 8: direction mismatch ---- */
static void test8_direction_mismatch(void){
  printf("\nTest 8: direction_mismatch\n");
  const char *DB = "test_rev_mismatch.db";
  KVStore *kv = openFresh(DB);
  if( !kv ){ ASSERT("openFresh", 0); return; }

  kvstore_put(kv, "k", 1, "v", 1);

  /* Forward iterator — last/prev must return KVSTORE_ERROR */
  KVIterator *fwd = NULL;
  kvstore_iterator_create(kv, &fwd);
  ASSERT("last on forward iter returns ERROR",
         kvstore_iterator_last(fwd) == KVSTORE_ERROR);
  ASSERT("prev on forward iter returns ERROR",
         kvstore_iterator_prev(fwd) == KVSTORE_ERROR);
  kvstore_iterator_close(fwd);

  /* Reverse iterator — first/next still work (no check in those functions) */
  KVIterator *rev = NULL;
  kvstore_reverse_iterator_create(kv, &rev);
  ASSERT("reverse iter create ok", rev != NULL);
  kvstore_iterator_close(rev);

  cleanup(&kv, DB);
}

/* ---- Test 9: forward prefix iter unaffected by reverse field ---- */
static void test9_rev_with_fwd_prefix(void){
  printf("\nTest 9: rev_with_fwd_prefix\n");
  const char *DB = "test_rev_fwd_prefix.db";
  KVStore *kv = openFresh(DB);
  if( !kv ){ ASSERT("openFresh", 0); return; }

  kvstore_put(kv, "pfx:a", 5, "1", 1);
  kvstore_put(kv, "pfx:b", 5, "2", 1);
  kvstore_put(kv, "other", 5, "3", 1);

  KVIterator *it = NULL;
  kvstore_prefix_iterator_create(kv, "pfx:", 4, &it);

  int count = 0;
  while( !kvstore_iterator_eof(it) ){
    count++;
    kvstore_iterator_next(it);
  }
  ASSERT("forward prefix iter sees exactly 2 keys", count == 2);

  kvstore_iterator_close(it);
  cleanup(&kv, DB);
}

/* ---- Test 10: reverse prefix basic ---- */
static void test10_rev_prefix_basic(void){
  printf("\nTest 10: rev_prefix_basic\n");
  const char *DB = "test_rev_prefix_basic.db";
  KVStore *kv = openFresh(DB);
  if( !kv ){ ASSERT("openFresh", 0); return; }

  kvstore_begin(kv, 1);
  kvstore_put(kv, "user:001", 8, "alice",  5);
  kvstore_put(kv, "user:002", 8, "bob",    3);
  kvstore_put(kv, "user:003", 8, "carol",  5);
  kvstore_put(kv, "zzzother", 8, "ignore", 6);
  kvstore_put(kv, "aaaother", 8, "ignore", 6);
  kvstore_commit(kv);

  KVIterator *it = NULL;
  ASSERT("rev prefix create ok",
         kvstore_reverse_prefix_iterator_create(kv, "user:", 5, &it) == KVSTORE_OK);
  ASSERT("not eof", !kvstore_iterator_eof(it));

  /* Expect user:003, user:002, user:001 in that order */
  const char *expected[] = { "user:003", "user:002", "user:001" };
  int order_ok = 1;
  int count = 0;
  while( !kvstore_iterator_eof(it) ){
    void *pk; int nk;
    kvstore_iterator_key(it, &pk, &nk);
    if( count >= 3 || nk != 8 || memcmp(pk, expected[count], 8) != 0 ){
      order_ok = 0;
    }
    count++;
    kvstore_iterator_prev(it);
  }
  ASSERT("3 keys in descending order", order_ok && count == 3);

  kvstore_iterator_close(it);
  cleanup(&kv, DB);
}

/* ---- Test 11: reverse prefix — no match ---- */
static void test11_rev_prefix_empty(void){
  printf("\nTest 11: rev_prefix_empty\n");
  const char *DB = "test_rev_prefix_empty.db";
  KVStore *kv = openFresh(DB);
  if( !kv ){ ASSERT("openFresh", 0); return; }

  kvstore_put(kv, "alpha:1", 7, "v", 1);
  kvstore_put(kv, "alpha:2", 7, "v", 1);

  KVIterator *it = NULL;
  kvstore_reverse_prefix_iterator_create(kv, "beta:", 5, &it);
  ASSERT("eof immediately when prefix not found", kvstore_iterator_eof(it));

  kvstore_iterator_close(it);
  cleanup(&kv, DB);
}

/* ---- Test 12: reverse prefix — all-0xFF prefix ---- */
static void test12_rev_prefix_all_ff(void){
  printf("\nTest 12: rev_prefix_all_ff\n");
  const char *DB = "test_rev_prefix_ff.db";
  KVStore *kv = openFresh(DB);
  if( !kv ){ ASSERT("openFresh", 0); return; }

  /* Insert a key whose first byte is 0xFF */
  unsigned char ffkey[] = { 0xFF, 'a' };
  unsigned char ffkey2[] = { 0xFF, 'b' };
  unsigned char prefix[] = { 0xFF };

  kvstore_put(kv, ffkey,  2, "v1", 2);
  kvstore_put(kv, ffkey2, 2, "v2", 2);
  kvstore_put(kv, "normal", 6, "v3", 2);

  KVIterator *it = NULL;
  /* Prefix = {0xFF} — kvstorePrefixSuccessor returns 0 (all-0xFF path) */
  kvstore_reverse_prefix_iterator_create(kv, prefix, 1, &it);

  int count = 0;
  while( !kvstore_iterator_eof(it) ){
    void *pk; int nk;
    kvstore_iterator_key(it, &pk, &nk);
    /* Each key must start with 0xFF */
    unsigned char *pu = (unsigned char*)pk;
    if( nk < 1 || pu[0] != 0xFF ) count = -999;
    else count++;
    kvstore_iterator_prev(it);
  }
  ASSERT("both 0xFF-prefix keys found via BtreeLast fallback", count == 2);

  kvstore_iterator_close(it);
  cleanup(&kv, DB);
}

/* ---- Test 13: reverse prefix with TTL skip ---- */
static void test13_rev_prefix_ttl_skip(void){
  printf("\nTest 13: rev_prefix_ttl_skip\n");
  const char *DB = "test_rev_prefix_ttl.db";
  KVStore *kv = openFresh(DB);
  if( !kv ){ ASSERT("openFresh", 0); return; }

  int64_t past = kvstore_now_ms() - 10000;

  kvstore_put(kv, "evt:001", 7, "live1",   5);
  kvstore_put_ttl(kv, "evt:002", 7, "dead",  4, past);  /* expired */
  kvstore_put(kv, "evt:003", 7, "live2",   5);
  kvstore_put_ttl(kv, "evt:004", 7, "dead2", 5, past);  /* expired */
  kvstore_put(kv, "evt:005", 7, "live3",   5);
  kvstore_put(kv, "other:x", 7, "ignore",  6);

  KVIterator *it = NULL;
  kvstore_reverse_prefix_iterator_create(kv, "evt:", 4, &it);

  int count = 0, saw_dead = 0;
  while( !kvstore_iterator_eof(it) ){
    void *pk; int nk;
    kvstore_iterator_key(it, &pk, &nk);
    if( nk >= 4 && memcmp(pk, "evt:", 4) == 0 ){
      if( (nk == 7 && memcmp(pk, "evt:002", 7) == 0) ||
          (nk == 7 && memcmp(pk, "evt:004", 7) == 0) ){
        saw_dead = 1;
      }
    }
    count++;
    kvstore_iterator_prev(it);
  }
  ASSERT("no expired keys yielded", !saw_dead);
  ASSERT("exactly 3 live evt: keys", count == 3);

  kvstore_iterator_close(it);
  cleanup(&kv, DB);
}

/* ---- main ---- */
int main(void){
  printf("=== Reverse Iterator Test Suite ===\n");

  test1_rev_basic();
  test2_rev_empty();
  test3_rev_single();
  test4_rev_last_first();
  test5_rev_cf();
  test6_rev_multi_cf();
  test7_rev_ttl_skip();
  test8_direction_mismatch();
  test9_rev_with_fwd_prefix();
  test10_rev_prefix_basic();
  test11_rev_prefix_empty();
  test12_rev_prefix_all_ff();
  test13_rev_prefix_ttl_skip();

  printf("\n=== Results: %d passed, %d failed ===\n", passed, failed);
  return failed ? 1 : 0;
}
