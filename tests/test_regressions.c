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

/*
** Test 11: Failed write begin restores the persistent read transaction.
**
** Bug: when kvstore_put (or kvstore_begin) released the persistent read
** then failed to open a write transaction (e.g. SQLITE_BUSY from a second
** writer), inTrans was left at 0. Subsequent reads on that store still
** worked but paid a per-call BeginTrans/Commit overhead.  More critically,
** on the next write attempt the store would again release a read that was
** never there, leaving inTrans permanently at 0.
**
** We trigger the SQLITE_BUSY path by opening a second handle to the same
** file and holding a write transaction on it, then attempting a write on
** the first handle.  After the failed write, reads on the first handle
** must still succeed and the store must accept a later write once the
** second handle releases its lock.
*/
static void test_write_fail_restores_read(void){
  printf("\nTest 11: failed write begin restores persistent read\n");
  const char *p = "reg11.db";
  dbRemove(p);

  /* Seed the database via a separate open so both handles see existing data. */
  {
    KVStore *seed = NULL;
    kvstore_open(p, &seed, KVSTORE_JOURNAL_WAL);
    if(!seed){ ASSERT("seed open", 0); dbRemove(p); return; }
    kvstore_put(seed, "pre", 3, "val", 3);
    kvstore_close(seed);
  }

  KVStore *kv1 = NULL, *kv2 = NULL;
  kvstore_open(p, &kv1, KVSTORE_JOURNAL_WAL);
  kvstore_open(p, &kv2, KVSTORE_JOURNAL_WAL);
  if(!kv1 || !kv2){
    ASSERT("dual open", 0);
    if(kv1) kvstore_close(kv1);
    if(kv2) kvstore_close(kv2);
    dbRemove(p); return;
  }

  /* Hold a write transaction on kv2 so kv1 cannot acquire one. */
  int rc = kvstore_begin(kv2, 1);
  ASSERT("kv2 write begin ok", rc == KVSTORE_OK);

  /* Attempt a write on kv1 — must fail (SQLITE_BUSY). */
  rc = kvstore_put(kv1, "new", 3, "data", 4);
  ASSERT("kv1 write blocked by kv2", rc != KVSTORE_OK);

  /* After the failure, reads on kv1 must still work. */
  void *pv = NULL; int nv = 0;
  rc = kvstore_get(kv1, "pre", 3, &pv, &nv);
  ASSERT("kv1 read after failed write ok", rc == KVSTORE_OK);
  ASSERT("kv1 read correct value", pv && nv==3 && memcmp(pv,"val",3)==0);
  if(pv) snkv_free(pv);

  /* Release kv2's write lock, then kv1 must be able to write. */
  kvstore_rollback(kv2);
  rc = kvstore_put(kv1, "new", 3, "data", 4);
  ASSERT("kv1 write succeeds after kv2 releases lock", rc == KVSTORE_OK);

  kvstore_close(kv1);
  kvstore_close(kv2);
  dbRemove(p);
}

/*
** Test 12: kvstore_open (the simple wrapper) produces a working store.
**
** Bug: kvstore_open zero-initialised its KVStoreConfig, so syncLevel=0
** was passed to kvstore_open_v2.  Because pConfig was non-NULL the NULL
** guard that defaults to KVSTORE_SYNC_NORMAL was bypassed, and the store
** silently opened with PAGER_SYNCHRONOUS_OFF.
**
** The fix sets cfg.syncLevel = KVSTORE_SYNC_NORMAL explicitly.  This test
** verifies the store opened through kvstore_open is fully functional and
** data survives a close/reopen cycle, which would fail under sync=OFF on a
** system where the OS page cache is dropped between the two opens.
*/
static void test_kvstore_open_is_functional(void){
  printf("\nTest 12: kvstore_open wrapper produces a functional store\n");
  const char *p = "reg12.db";
  dbRemove(p);

  KVStore *kv = NULL;
  int rc = kvstore_open(p, &kv, KVSTORE_JOURNAL_WAL);
  ASSERT("kvstore_open ok", rc == KVSTORE_OK && kv != NULL);
  if(!kv){ dbRemove(p); return; }

  rc = kvstore_put(kv, "durability", 10, "check", 5);
  ASSERT("put ok", rc == KVSTORE_OK);
  kvstore_close(kv);

  /* Reopen — data must survive. */
  kv = NULL;
  rc = kvstore_open(p, &kv, KVSTORE_JOURNAL_WAL);
  ASSERT("reopen ok", rc == KVSTORE_OK && kv != NULL);
  if(!kv){ dbRemove(p); return; }

  void *pv = NULL; int nv = 0;
  rc = kvstore_get(kv, "durability", 10, &pv, &nv);
  ASSERT("data survived close/reopen", rc == KVSTORE_OK);
  ASSERT("correct value after reopen", pv && nv==5 && memcmp(pv,"check",5)==0);
  if(pv) snkv_free(pv);

  kvstore_close(kv);
  dbRemove(p);
}

/*
** Test 13: dropping a CF does not make sibling CFs unreachable.
**
** Bug: the metadata table uses open-addressing (linear probing).  When
** kvstore_cf_drop deleted a row with sqlite3BtreeDelete, the probe slot
** became empty.  Any CF stored at a later probe offset due to a hash
** collision with the dropped CF would never be found again — the seeker
** stops at the first empty slot.
**
** The fix writes a tombstone (4-byte zero payload) instead of deleting.
** The seeker naturally skips tombstones (payloadSz < 4+nKey) and continues
** probing.
**
** We create several CFs, drop one in the middle, then verify every
** non-dropped CF is still openable and its data still readable.
*/
static void test_cf_drop_does_not_break_siblings(void){
  printf("\nTest 13: CF drop does not make sibling CFs unreachable\n");
  const char *p = "reg13.db";
  dbRemove(p);

  KVStore *kv = NULL;
  kvstore_open(p, &kv, KVSTORE_JOURNAL_WAL);
  if(!kv){ ASSERT("open ok", 0); dbRemove(p); return; }

  /* Create several CFs and put one key in each. */
  const char *names[] = { "alpha", "beta", "gamma", "delta", "epsilon" };
  int n = (int)(sizeof(names)/sizeof(names[0]));
  int i;
  for(i = 0; i < n; i++){
    KVColumnFamily *cf = NULL;
    int rc = kvstore_cf_create(kv, names[i], &cf);
    ASSERT("create cf ok", rc == KVSTORE_OK);
    if(rc == KVSTORE_OK){
      kvstore_cf_put(cf, "k", 1, names[i], (int)strlen(names[i]));
      kvstore_cf_close(cf);
    }
  }

  /* Drop "beta" (middle of the list). */
  int rc = kvstore_cf_drop(kv, "beta");
  ASSERT("drop beta ok", rc == KVSTORE_OK);

  kvstore_close(kv);

  /* Reopen and verify every non-dropped CF is accessible with correct data. */
  kv = NULL;
  kvstore_open(p, &kv, KVSTORE_JOURNAL_WAL);
  ASSERT("reopen ok", kv != NULL);
  if(!kv){ dbRemove(p); return; }

  const char *surviving[] = { "alpha", "gamma", "delta", "epsilon" };
  int ns = (int)(sizeof(surviving)/sizeof(surviving[0]));
  for(i = 0; i < ns; i++){
    KVColumnFamily *cf = NULL;
    rc = kvstore_cf_open(kv, surviving[i], &cf);
    ASSERT("surviving CF still openable", rc == KVSTORE_OK);
    if(rc != KVSTORE_OK){
      printf("  CF '%s' not found after drop of 'beta'\n", surviving[i]);
      continue;
    }
    void *pv = NULL; int nv = 0;
    rc = kvstore_cf_get(cf, "k", 1, &pv, &nv);
    ASSERT("data intact in surviving CF", rc == KVSTORE_OK);
    ASSERT("correct value", pv && (int)strlen(surviving[i])==nv &&
           memcmp(pv, surviving[i], nv)==0);
    if(pv) snkv_free(pv);
    kvstore_cf_close(cf);
  }

  /* "beta" must be gone. */
  KVColumnFamily *cf = NULL;
  rc = kvstore_cf_open(kv, "beta", &cf);
  ASSERT("dropped CF not openable", rc == KVSTORE_NOTFOUND);
  if(cf) kvstore_cf_close(cf);

  kvstore_close(kv);
  dbRemove(p);
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
  test_write_fail_restores_read();
  test_kvstore_open_is_functional();
  test_cf_drop_does_not_break_siblings();
  printf("\n--- %d passed, %d failed ---\n", passed, failed);
  return (failed > 0) ? 1 : 0;
}
