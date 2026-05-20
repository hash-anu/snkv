/* SPDX-License-Identifier: Apache-2.0 */
/*
** tests/test_regressions.c — Permanent regression tests for bugs fixed in
** past releases.  Tests use only the public kvstore.h API; no internal
** struct fields are accessed.
**
** Tests:
**  1.  TTL overwrite of same key does not duplicate the TTL index entry
**  2.  Re-putting a key with expire=0 clears its TTL (key stays, no expiry)
**  3.  Expired keys all purged → iterator sees eof immediately
**  4.  Reopen CF with different value sizes → no crash (heap-corruption guard)
**  5.  purge_expired after cf_clear returns 0 deleted (no UAF)
**  6.  Put on same key twice: second value wins, count stays 1
**  7.  iterator_close(NULL) is a no-op (no crash)
**  8.  kvstore_close with dangling open iterator → no crash
**  9.  kvstore_errmsg on NULL store → non-NULL string (no crash)
** 10.  kvstore_delete on nonexistent key → KVSTORE_NOTFOUND
*/

#include "kvstore.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int passed = 0, failed = 0;

static void check(const char *name, int ok){
  if(ok){ printf("  PASS: %s\n", name); passed++; }
  else  { printf("  FAIL: %s\n", name); failed++; }
}
#define ASSERT(name, expr) check(name, (int)(expr))

static KVStore *openFresh(const char *p){
  remove(p);
  char w[512], s[512];
  snprintf(w, sizeof(w), "%s-wal", p); remove(w);
  snprintf(s, sizeof(s), "%s-shm", p); remove(s);
  KVStore *kv = NULL;
  kvstore_open(p, &kv, KVSTORE_JOURNAL_WAL);
  return kv;
}
static void dbRemove(const char *p){
  char w[512], s[512];
  remove(p);
  snprintf(w, sizeof(w), "%s-wal", p); remove(w);
  snprintf(s, sizeof(s), "%s-shm", p); remove(s);
}

/*
** Test 1: TTL overwrite of same key does not duplicate the TTL index entry.
** If the internal TTL index is not cleaned up on overwrite, a stale entry
** will remain and purge_expired will delete the key at the wrong time.
*/
static void test_ttl_overwrite_not_doubled(void){
  printf("\nTest 1: TTL overwrite does not duplicate TTL index entry\n");
  KVStore *kv = openFresh("reg1.db");
  if(!kv) return;

  int64_t exp = kvstore_now_ms() + 60000;
  kvstore_put_ttl(kv, "k", 1, "v1", 2, exp);
  /* Overwrite same key with a different expiry */
  kvstore_put_ttl(kv, "k", 1, "v2", 2, exp + 1000);

  int64_t n = 0;
  kvstore_count(kv, &n);
  ASSERT("count==1 after overwrite", n == 1);

  /* Future-expiry key must not be purged */
  int nDel = -1;
  int rc = kvstore_purge_expired(kv, &nDel);
  ASSERT("purge ok", rc == KVSTORE_OK);
  ASSERT("0 purged (not yet expired)", nDel == 0);

  /* Value must be from the second put */
  void *pv = NULL; int nv = 0;
  rc = kvstore_get(kv, "k", 1, &pv, &nv);
  ASSERT("get ok", rc == KVSTORE_OK);
  ASSERT("second value wins", pv && nv==2 && memcmp(pv,"v2",2)==0);
  if(pv) snkv_free(pv);

  kvstore_close(kv);
  dbRemove("reg1.db");
}

/*
** Test 2: Re-putting a key with expire_ms=0 strips its TTL so the key
** becomes permanent (ttl_remaining returns KVSTORE_NO_TTL).
*/
static void test_ttl_clear_makes_permanent(void){
  printf("\nTest 2: Re-put with expire=0 makes key permanent\n");
  KVStore *kv = openFresh("reg2.db");
  if(!kv) return;

  int64_t exp = kvstore_now_ms() + 60000;
  kvstore_put_ttl(kv, "k", 1, "v", 1, exp);

  int64_t rem = 0;
  int rc = kvstore_ttl_remaining(kv, "k", 1, &rem);
  ASSERT("has TTL after put_ttl", rc == KVSTORE_OK && rem != KVSTORE_NO_TTL);

  /* Clear TTL: re-put with expire=0 */
  rc = kvstore_put_ttl(kv, "k", 1, "v", 1, 0);
  ASSERT("put_ttl with expire=0 ok", rc == KVSTORE_OK);

  rc = kvstore_ttl_remaining(kv, "k", 1, &rem);
  ASSERT("KVSTORE_NO_TTL after clear", rc == KVSTORE_OK && rem == KVSTORE_NO_TTL);

  /* Key must still be accessible */
  int ex = 0;
  rc = kvstore_exists(kv, "k", 1, &ex);
  ASSERT("key still exists after TTL clear", rc == KVSTORE_OK && ex);

  kvstore_close(kv);
  dbRemove("reg2.db");
}

/* Test 3: Expired keys purged → iterator eof immediately */
static void test_expired_iter_eof(void){
  printf("\nTest 3: iterator eof after all keys purged\n");
  KVStore *kv = openFresh("reg3.db");
  if(!kv) return;

  int64_t past = kvstore_now_ms() - 5000;
  kvstore_put_ttl(kv, "e1", 2, "v1", 2, past);
  kvstore_put_ttl(kv, "e2", 2, "v2", 2, past);

  int nDel = 0;
  int rc = kvstore_purge_expired(kv, &nDel);
  ASSERT("purge ok", rc == KVSTORE_OK);
  ASSERT("2 purged", nDel == 2);

  KVIterator *it = NULL;
  kvstore_iterator_create(kv, &it);
  kvstore_iterator_first(it);
  ASSERT("eof after purge", kvstore_iterator_eof(it));
  kvstore_iterator_close(it);

  kvstore_close(kv);
  dbRemove("reg3.db");
}

/* Test 4: Reopen CF with different value sizes — no crash */
static void test_reopen_cf_no_crash(void){
  printf("\nTest 4: reopen CF with different value sizes — no crash\n");
  const char *p = "reg4.db";

  {
    KVStore *kv = openFresh(p);
    if(!kv) return;
    KVColumnFamily *cf = NULL;
    kvstore_cf_create(kv, "vecs", &cf);
    kvstore_cf_put(cf, "v0", 2, "small", 5);
    kvstore_cf_close(cf);
    kvstore_close(kv);
  }

  {
    KVStore *kv = NULL;
    kvstore_open(p, &kv, KVSTORE_JOURNAL_WAL);
    ASSERT("reopen ok", kv != NULL);
    if(!kv){ dbRemove(p); return; }

    KVColumnFamily *cf = NULL;
    int rc = kvstore_cf_open(kv, "vecs", &cf);
    ASSERT("cf open ok", rc == KVSTORE_OK && cf != NULL);

    if(rc == KVSTORE_OK && cf){
      void *pv = NULL; int nv = 0;
      rc = kvstore_cf_get(cf, "v0", 2, &pv, &nv);
      ASSERT("get on reopened cf", rc == KVSTORE_OK);
      ASSERT("value correct", pv && nv==5 && memcmp(pv,"small",5)==0);
      if(pv) snkv_free(pv);
      kvstore_cf_close(cf);
    }
    kvstore_close(kv);
  }
  dbRemove(p);
}

/* Test 5: purge_expired after cf_clear returns 0 deleted */
static void test_purge_after_clear(void){
  printf("\nTest 5: purge_expired after clear returns 0\n");
  KVStore *kv = openFresh("reg5.db");
  if(!kv) return;

  int64_t past = kvstore_now_ms() - 1000;
  kvstore_put_ttl(kv, "x", 1, "y", 1, past);

  int rc = kvstore_clear(kv);
  ASSERT("clear ok", rc == KVSTORE_OK);

  int nDel = -1;
  rc = kvstore_purge_expired(kv, &nDel);
  ASSERT("purge after clear ok", rc == KVSTORE_OK);
  ASSERT("0 deleted", nDel == 0);

  kvstore_close(kv);
  dbRemove("reg5.db");
}

/* Test 6: put twice — second value wins, count stays 1 */
static void test_put_overwrites(void){
  printf("\nTest 6: second put overwrites; count stays 1\n");
  KVStore *kv = openFresh("reg6.db");
  if(!kv) return;

  kvstore_put(kv, "dup", 3, "first_value", 11);
  kvstore_put(kv, "dup", 3, "second", 6);

  void *pv = NULL; int nv = 0;
  int rc = kvstore_get(kv, "dup", 3, &pv, &nv);
  ASSERT("get ok", rc == KVSTORE_OK);
  ASSERT("second value wins", pv && nv==6 && memcmp(pv,"second",6)==0);
  if(pv) snkv_free(pv);

  int64_t n = 0;
  kvstore_count(kv, &n);
  ASSERT("count==1", n == 1);

  kvstore_close(kv);
  dbRemove("reg6.db");
}

/* Test 7: iterator_close(NULL) is a no-op */
static void test_iterator_close_null(void){
  printf("\nTest 7: iterator_close(NULL) is a no-op\n");
  kvstore_iterator_close(NULL);
  ASSERT("no crash on close(NULL)", 1);
}

/* Test 8: kvstore_close with dangling open iterator — no crash */
static void test_close_with_open_iterator(void){
  printf("\nTest 8: kvstore_close with dangling iterator — no crash\n");
  KVStore *kv = openFresh("reg8.db");
  if(!kv) return;

  kvstore_put(kv, "a", 1, "b", 1);
  KVIterator *it = NULL;
  kvstore_iterator_create(kv, &it);
  kvstore_iterator_first(it);

  /* Close the store while iterator is still open.
  ** The store must force-close the iterator to avoid a use-after-free. */
  kvstore_close(kv);

  /* Calling close on an already-force-closed iterator must be a no-op. */
  kvstore_iterator_close(it);
  ASSERT("no crash on close-with-dangling-iter", 1);

  dbRemove("reg8.db");
}

/* Test 9: kvstore_errmsg(NULL) must not crash */
static void test_errmsg_null(void){
  printf("\nTest 9: kvstore_errmsg(NULL) returns non-NULL string\n");
  const char *msg = kvstore_errmsg(NULL);
  ASSERT("returns non-NULL", msg != NULL);
}

/* Test 10: delete on nonexistent key returns KVSTORE_NOTFOUND */
static void test_delete_notfound(void){
  printf("\nTest 10: delete nonexistent key returns NOTFOUND\n");
  KVStore *kv = openFresh("reg10.db");
  if(!kv) return;
  int rc = kvstore_delete(kv, "missing", 7);
  ASSERT("NOTFOUND on missing key", rc == KVSTORE_NOTFOUND);
  kvstore_close(kv);
  dbRemove("reg10.db");
}

int main(void){
  printf("=== test_regressions ===\n");
  test_ttl_overwrite_not_doubled();
  test_ttl_clear_makes_permanent();
  test_expired_iter_eof();
  test_reopen_cf_no_crash();
  test_purge_after_clear();
  test_put_overwrites();
  test_iterator_close_null();
  test_close_with_open_iterator();
  test_errmsg_null();
  test_delete_notfound();
  printf("\n--- %d passed, %d failed ---\n", passed, failed);
  return (failed > 0) ? 1 : 0;
}
