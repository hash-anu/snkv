/*
** test_review_fixes.c — regression tests for the stability patch
** (0001-stability-fixes). Drop into tests/ and build like the other tests:
**
**   cc -Wall -Iinclude -I. -o tests/test_review_fixes \
**      tests/test_review_fixes.c libsnkv.a -lpthread -lm
**
** Run against the UNPATCHED tree first: Test 1 FAILS there (encrypted open
** bricks a plain store whose data lives only in a user CF). After the patch
** all tests pass.
*/
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "kvstore.h"
#include "sqlite3.h"   /* VFS wrapper in Test 13 */

static int nPass = 0, nFail = 0;
#define CHECK(cond, msg) do{ \
  if( cond ){ printf("  [PASS] %s\n", msg); nPass++; } \
  else      { printf("  [FAIL] %s\n", msg); nFail++; } \
}while(0)

static void cleanup(const char *base){
  char buf[256];
  snprintf(buf, sizeof buf, "%s", base);        remove(buf);
  snprintf(buf, sizeof buf, "%s-wal", base);    remove(buf);
  snprintf(buf, sizeof buf, "%s-shm", base);    remove(buf);
  snprintf(buf, sizeof buf, "%s-journal", base);remove(buf);
}

/* ----------------------------------------------------------------------
** Test 1 (fix: kvstore_open_encrypted / isPlainReencrypt):
** A plain store whose ONLY data lives in a user CF must survive
** encrypt-on-open. Before the fix, plain-data detection counted only the
** default CF; bEncrypted was set without re-encrypting the user CF, so
** every subsequent read returned AUTH_FAILED/CORRUPT — store bricked.
** -------------------------------------------------------------------- */
static void test_encrypt_user_cf_only(void){
  const char *db = "t_rf_enc.db";
  printf("Test 1: encrypt-on-open of plain store with user-CF-only data\n");
  cleanup(db);

  KVStore *kv = NULL;
  KVColumnFamily *cf = NULL;
  CHECK(kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL) == KVSTORE_OK, "plain open");
  CHECK(kvstore_cf_create(kv, "users", &cf) == KVSTORE_OK, "create user CF");
  CHECK(kvstore_cf_put(cf, "u1", 2, "alice", 5) == KVSTORE_OK, "put in user CF only");
  kvstore_cf_close(cf);
  kvstore_close(kv);

  /* Re-open with a password: must encrypt ALL CFs, not just default. */
  kv = NULL;
  int rc = kvstore_open_encrypted(db, "pw123", 5, &kv, NULL);
  CHECK(rc == KVSTORE_OK, "open_encrypted on plain store succeeds");
  if( rc == KVSTORE_OK ){
    cf = NULL;
    CHECK(kvstore_cf_open(kv, "users", &cf) == KVSTORE_OK, "re-open user CF");
    void *v = NULL; int n = 0;
    rc = kvstore_cf_get(cf, "u1", 2, &v, &n);
    CHECK(rc == KVSTORE_OK && n == 5 && memcmp(v, "alice", 5) == 0,
          "user-CF value readable after encryption (was AUTH_FAILED before fix)");
    if( v ) snkv_free(v);
    if( cf ) kvstore_cf_close(cf);
    kvstore_close(kv);
  }
  cleanup(db);
}

/* ----------------------------------------------------------------------
** Test 2 (fix: cached read-cursor invalidation):
** get() opens a cached read cursor; checkpoint TRUNCATE must still fully
** truncate the WAL, and a get() after the checkpoint must reuse/reopen
** the cursor correctly across the transaction boundary.
** -------------------------------------------------------------------- */
static void test_checkpoint_after_get(void){
  const char *db = "t_rf_ckpt.db";
  printf("Test 2: checkpoint truncation + cursor reuse across commits\n");
  cleanup(db);

  KVStore *kv = NULL;
  CHECK(kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL) == KVSTORE_OK, "open");
  CHECK(kvstore_put(kv, "k1", 2, "v1", 2) == KVSTORE_OK, "put k1");

  void *v = NULL; int n = 0;
  CHECK(kvstore_get(kv, "k1", 2, &v, &n) == KVSTORE_OK, "get k1 (opens cached cursor)");
  if( v ){ snkv_free(v); v = NULL; }

  for( int i = 0; i < 100; i++ ){
    char k[16]; snprintf(k, sizeof k, "key%05d", i);
    kvstore_put(kv, k, 8, "payload-payload-payload", 23);
  }

  int nLog = -1, nCkpt = -1;
  int rc = kvstore_checkpoint(kv, KVSTORE_CHECKPOINT_TRUNCATE, &nLog, &nCkpt);
  CHECK(rc == KVSTORE_OK, "checkpoint TRUNCATE returns OK");
  CHECK(nLog == 0, "WAL fully truncated (no frames left)");

  /* Cursor must survive (be re-seeked or re-opened) after the checkpoint
  ** committed and re-opened the persistent read transaction. */
  rc = kvstore_get(kv, "key00042", 8, &v, &n);
  CHECK(rc == KVSTORE_OK && n == 23, "get after checkpoint works");
  if( v ) snkv_free(v);

  kvstore_close(kv);
  cleanup(db);
}

/* ----------------------------------------------------------------------
** Test 3 (fix: skip-only TTL expiry in iterators):
** Iterating with expired keys present must (a) not yield expired keys,
** (b) not invalidate a second concurrently open iterator, and
** (c) purge_expired must still reclaim the entries afterwards.
** -------------------------------------------------------------------- */
static void test_iterator_ttl_skip(void){
  const char *db = "t_rf_ttl.db";
  printf("Test 3: TTL iterator skips expired without breaking other iterators\n");
  cleanup(db);

  KVStore *kv = NULL;
  CHECK(kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL) == KVSTORE_OK, "open");

  int64_t now = kvstore_now_ms();
  /* 3 live keys, 3 keys expiring in 150 ms */
  CHECK(kvstore_put(kv, "live1", 5, "x", 1) == KVSTORE_OK, "put live1");
  CHECK(kvstore_put(kv, "live2", 5, "x", 1) == KVSTORE_OK, "put live2");
  CHECK(kvstore_put(kv, "live3", 5, "x", 1) == KVSTORE_OK, "put live3");
  CHECK(kvstore_put_ttl(kv, "tmp1", 4, "y", 1, now + 150) == KVSTORE_OK, "put tmp1 ttl");
  CHECK(kvstore_put_ttl(kv, "tmp2", 4, "y", 1, now + 150) == KVSTORE_OK, "put tmp2 ttl");
  CHECK(kvstore_put_ttl(kv, "tmp3", 4, "y", 1, now + 150) == KVSTORE_OK, "put tmp3 ttl");

  usleep(300 * 1000);  /* let the tmp* keys expire */

  /* Two iterators open at once over the same CF. */
  KVIterator *itA = NULL, *itB = NULL;
  CHECK(kvstore_iterator_create(kv, &itA) == KVSTORE_OK, "iterator A create");
  CHECK(kvstore_iterator_create(kv, &itB) == KVSTORE_OK, "iterator B create");

  int seenA = 0;
  int rc = kvstore_iterator_first(itA);
  while( rc == KVSTORE_OK && !kvstore_iterator_eof(itA) ){
    seenA++;
    rc = kvstore_iterator_next(itA);
  }
  CHECK(seenA == 3, "iterator A yields only the 3 live keys");

  /* Iterator B must still be fully usable after A's pass (before the fix,
  ** A's lazy deletes committed the shared transaction mid-iteration and
  ** invalidated B's cursor). */
  int seenB = 0;
  rc = kvstore_iterator_first(itB);
  while( rc == KVSTORE_OK && !kvstore_iterator_eof(itB) ){
    seenB++;
    rc = kvstore_iterator_next(itB);
  }
  CHECK(seenB == 3, "iterator B unaffected, also yields 3 live keys");

  kvstore_iterator_close(itA);
  kvstore_iterator_close(itB);

  /* Reclamation is purge's job now. */
  int nDeleted = 0;
  CHECK(kvstore_purge_expired(kv, &nDeleted) == KVSTORE_OK, "purge_expired OK");
  CHECK(nDeleted == 3, "purge reclaims exactly the 3 expired keys");

  kvstore_close(kv);
  cleanup(db);
}

/* ----------------------------------------------------------------------
** Test 4 (fix: zErrMsg fixed buffer):
** The pointer returned by kvstore_errmsg() must remain dereferenceable
** after a later operation overwrites the error. Before the fix the old
** heap buffer was freed by the next kvstoreSetError() — a use-after-free
** that ASAN flags (plain builds may read stale-but-mapped memory, so run
** this under -fsanitize=address for a hard guarantee).
** -------------------------------------------------------------------- */
static void test_errmsg_stability(void){
  const char *db = "t_rf_err.db";
  printf("Test 4: errmsg pointer stable across subsequent errors\n");
  cleanup(db);

  KVStore *kv = NULL;
  CHECK(kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL) == KVSTORE_OK, "open");

  /* Trigger error #1: oversized key */
  char big[70 * 1024];
  memset(big, 'a', sizeof big);
  kvstore_put(kv, big, (int)sizeof big, "v", 1);
  const char *e1 = kvstore_errmsg(kv);
  char saved[64];
  snprintf(saved, sizeof saved, "%.40s", e1);

  /* Trigger error #2: begin twice */
  kvstore_begin(kv, 1);
  kvstore_begin(kv, 1);

  /* e1 must still be readable (same fixed buffer, new contents are fine —
  ** the old pointer just must not be freed memory). */
  volatile char probe = e1[0];
  (void)probe;
  CHECK(1, "old errmsg pointer still dereferenceable (verify under ASAN)");
  CHECK(strlen(kvstore_errmsg(kv)) > 0, "current errmsg non-empty");
  (void)saved;

  kvstore_rollback(kv);
  kvstore_close(kv);
  cleanup(db);
}

/* ----------------------------------------------------------------------
** Test 5 (fix: caller-owned write transaction protected from internal
** commits):
** get()/exists()/get_ttl()'s lazy TTL-expiry cleanup, and kvstore_sync(),
** used to manage pKV->inTrans as if they always owned it. If the CALLER
** had an explicit write transaction open (kvstore_begin(kv,1)), touching
** an expired key — or just calling sync() — silently committed the
** caller's in-flight, uncommitted writes and reset the transaction state
** out from under them. A later kvstore_rollback() then had nothing left
** to undo. Verify: uncommitted writes made alongside an expired-key
** touch, or alongside a sync() call, are still rolled back correctly.
** -------------------------------------------------------------------- */
static void test_txn_owner_protected(void){
  const char *db = "t_rf_txn.db";
  printf("Test 5: expired-key access / sync() must not commit caller's open txn\n");
  cleanup(db);

  KVStore *kv = NULL;
  CHECK(kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL) == KVSTORE_OK, "open");

  int64_t now = kvstore_now_ms();
  CHECK(kvstore_put_ttl(kv, "expA", 4, "old", 3, now - 5000) == KVSTORE_OK,
        "seed key A already expired");
  CHECK(kvstore_put_ttl(kv, "expB", 4, "old", 3, now - 5000) == KVSTORE_OK,
        "seed key B already expired");
  CHECK(kvstore_put_ttl(kv, "expC", 4, "old", 3, now - 5000) == KVSTORE_OK,
        "seed key C already expired");

  /* --- 5a: get() touching an expired key must not commit the txn --- */
  CHECK(kvstore_begin(kv, 1) == KVSTORE_OK, "5a begin write txn");
  CHECK(kvstore_put(kv, "dirtyA", 6, "uncommitted", 11) == KVSTORE_OK,
        "5a put uncommitted value");
  void *v = NULL; int n = 0;
  int rc = kvstore_get(kv, "expA", 4, &v, &n);
  CHECK(rc == KVSTORE_NOTFOUND, "5a get(expired) returns NOTFOUND");
  if( v ) snkv_free(v);
  CHECK(kvstore_rollback(kv) == KVSTORE_OK, "5a rollback");
  rc = kvstore_get(kv, "dirtyA", 6, &v, &n);
  CHECK(rc == KVSTORE_NOTFOUND, "5a rollback discarded the uncommitted put");
  if( v ) snkv_free(v);

  /* --- 5b: exists() touching an expired key must not commit the txn --- */
  CHECK(kvstore_begin(kv, 1) == KVSTORE_OK, "5b begin write txn");
  CHECK(kvstore_put(kv, "dirtyB", 6, "uncommitted", 11) == KVSTORE_OK,
        "5b put uncommitted value");
  int exists = -1;
  rc = kvstore_exists(kv, "expB", 4, &exists);
  CHECK(rc == KVSTORE_OK && exists == 0, "5b exists(expired) returns false");
  CHECK(kvstore_rollback(kv) == KVSTORE_OK, "5b rollback");
  rc = kvstore_get(kv, "dirtyB", 6, &v, &n);
  CHECK(rc == KVSTORE_NOTFOUND, "5b rollback discarded the uncommitted put");
  if( v ) snkv_free(v);

  /* --- 5c: get_ttl() touching an expired key must not commit the txn --- */
  CHECK(kvstore_begin(kv, 1) == KVSTORE_OK, "5c begin write txn");
  CHECK(kvstore_put(kv, "dirtyC", 6, "uncommitted", 11) == KVSTORE_OK,
        "5c put uncommitted value");
  int64_t remaining = -2;
  rc = kvstore_get_ttl(kv, "expC", 4, &v, &n, &remaining);
  CHECK(rc == KVSTORE_NOTFOUND, "5c get_ttl(expired) returns NOTFOUND");
  if( v ) snkv_free(v);
  CHECK(kvstore_rollback(kv) == KVSTORE_OK, "5c rollback");
  rc = kvstore_get(kv, "dirtyC", 6, &v, &n);
  CHECK(rc == KVSTORE_NOTFOUND, "5c rollback discarded the uncommitted put");
  if( v ) snkv_free(v);

  /* --- 5d: sync() must refuse instead of committing the caller's txn --- */
  CHECK(kvstore_begin(kv, 1) == KVSTORE_OK, "5d begin write txn");
  CHECK(kvstore_put(kv, "dirtyD", 6, "uncommitted", 11) == KVSTORE_OK,
        "5d put uncommitted value");
  rc = kvstore_sync(kv);
  CHECK(rc != KVSTORE_OK, "5d sync() refuses while an explicit txn is open");
  CHECK(kvstore_rollback(kv) == KVSTORE_OK, "5d rollback");
  rc = kvstore_get(kv, "dirtyD", 6, &v, &n);
  CHECK(rc == KVSTORE_NOTFOUND, "5d rollback discarded the uncommitted put");
  if( v ) snkv_free(v);

  kvstore_close(kv);
  cleanup(db);
}

/* ----------------------------------------------------------------------
** Test 6 (fix: TTL count drift after rollback):
** The in-memory count of TTL keys (nTtlActive) gates every expiry check.
** Deleting a TTL key, or overwriting it with a plain put, lowers the count
** at once; if that transaction is rolled back, the keys come back on disk
** but the count stayed lowered. At 0, get / exists / iterators skipped the
** TTL check and returned expired keys as live until the store was reopened.
** -------------------------------------------------------------------- */
static void test_ttl_count_after_rollback(void){
  const char *db = "t_rf_ttlrb.db";
  printf("Test 6: TTL keys still expire after a rolled-back delete/overwrite\n");
  cleanup(db);
  KVStore *kv = NULL;
  CHECK(kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL) == KVSTORE_OK, "open");

  int64_t exp = kvstore_now_ms() + 300;
  kvstore_put_ttl(kv, "ta", 2, "va", 2, exp);
  kvstore_put_ttl(kv, "tc", 2, "vc", 2, exp);
  kvstore_put(kv, "keep", 4, "vk", 2);

  /* Drop both TTL entries inside a transaction, then roll it back. */
  CHECK(kvstore_begin(kv, 1) == KVSTORE_OK, "begin");
  kvstore_delete(kv, "ta", 2);
  kvstore_put(kv, "tc", 2, "new", 3);       /* plain put clears the TTL */
  CHECK(kvstore_rollback(kv) == KVSTORE_OK, "rollback restores both TTL keys");

  while( kvstore_now_ms() < exp + 50 ){ }    /* wait for the expiry */

  /* Iterator: expired keys must be skipped, only "keep" remains. */
  KVIterator *it = NULL; int n = 0, sawExpired = 0;
  kvstore_iterator_create(kv, &it);
  for( kvstore_iterator_first(it); !kvstore_iterator_eof(it); kvstore_iterator_next(it) ){
    void *k; int nk;
    kvstore_iterator_key(it, &k, &nk);
    if( nk == 2 && k && ((char*)k)[0] == 't' ) sawExpired = 1;
    n++;
  }
  kvstore_iterator_close(it);
  CHECK(!sawExpired && n == 1, "iterator skips the expired keys");

  int ex = 1;
  kvstore_exists(kv, "ta", 2, &ex);
  CHECK(ex == 0, "exists reports the expired key as absent");

  void *v = NULL; int nv = 0;
  int rc = kvstore_get(kv, "tc", 2, &v, &nv);
  CHECK(rc == KVSTORE_NOTFOUND, "get returns NOTFOUND for the expired key");
  if( rc == KVSTORE_OK ) snkv_free(v);

  v = NULL;
  rc = kvstore_get(kv, "keep", 4, &v, &nv);
  CHECK(rc == KVSTORE_OK && nv == 2, "permanent key is still readable");
  if( rc == KVSTORE_OK ) snkv_free(v);

  kvstore_close(kv);
  cleanup(db);
}

/* ----------------------------------------------------------------------
** Test 7 (fix: drop moves another table's root page):
** With auto-vacuum, dropping a CF moves the table with the highest root page
** into the freed page. Open handles (and the TTL index CFs they hold) kept the
** old root number, so their next operation returned KVSTORE_CORRUPT and marked
** the whole store corrupted.
** -------------------------------------------------------------------- */
static void test_drop_keeps_open_handles(void){
  const char *db = "t_rf_drop.db";
  printf("Test 7: dropping a CF does not break other open handles\n");
  cleanup(db);
  KVStore *kv = NULL;
  KVColumnFamily *a = NULL, *b = NULL, *c = NULL;
  void *v = NULL; int n = 0;
  CHECK(kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL) == KVSTORE_OK, "open");
  kvstore_cf_create(kv, "a", &a);
  kvstore_cf_create(kv, "b", &b);
  kvstore_cf_put(a, "ka", 2, "va", 2);
  kvstore_cf_put(b, "kb", 2, "vb", 2);
  CHECK(kvstore_cf_get(b, "kb", 2, &v, &n) == KVSTORE_OK, "b readable (cached cursor opened)");
  snkv_free(v); v = NULL;
  kvstore_cf_close(a);
  CHECK(kvstore_cf_drop(kv, "a") == KVSTORE_OK, "drop a (b's root page is moved)");
  CHECK(kvstore_cf_get(b, "kb", 2, &v, &n) == KVSTORE_OK && n == 2,
        "get through the still-open b handle");
  snkv_free(v); v = NULL;
  CHECK(kvstore_cf_put(b, "kb2", 3, "vb2", 3) == KVSTORE_OK, "put through the b handle");

  /* b's TTL index CFs are created after "c", so __snkv_ttl_e__b holds the
  ** highest root page and is the table moved when "c" is dropped. */
  kvstore_cf_create(kv, "c", &c);
  kvstore_cf_put_ttl(b, "t", 1, "x", 1, kvstore_now_ms() - 1);   /* already expired */
  kvstore_cf_close(c); c = NULL;
  CHECK(kvstore_cf_drop(kv, "c") == KVSTORE_OK, "drop c (a TTL index root is moved)");
  int del = 0;
  CHECK(kvstore_cf_purge_expired(b, &del) == KVSTORE_OK && del == 1,
        "purge through b's moved TTL index removes the expired key");
  kvstore_cf_close(b);

  KVColumnFamily *b2 = NULL;
  CHECK(kvstore_cf_open(kv, "b", &b2) == KVSTORE_OK, "reopen b");
  v = NULL;
  CHECK(kvstore_cf_get(b2, "kb2", 3, &v, &n) == KVSTORE_OK, "data written through the moved handle is there");
  snkv_free(v);
  kvstore_cf_close(b2);
  char *err = NULL;
  CHECK(kvstore_integrity_check(kv, &err) == KVSTORE_OK, "integrity check passes");
  if( err ) snkv_free(err);
  kvstore_close(kv);
  cleanup(db);
}

/* ----------------------------------------------------------------------
** Test 8: a drop is refused while an iterator is open (its cursor could not
** follow a moved root page); it works once the iterator is closed.
** -------------------------------------------------------------------- */
static void test_drop_refused_with_iterator(void){
  const char *db = "t_rf_dropit.db";
  printf("Test 8: drop is refused while an iterator is open\n");
  cleanup(db);
  KVStore *kv = NULL;
  KVColumnFamily *a = NULL, *b = NULL;
  KVIterator *it = NULL;
  kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL);
  kvstore_cf_create(kv, "a", &a);
  kvstore_cf_create(kv, "b", &b);
  kvstore_cf_put(b, "k", 1, "v", 1);
  kvstore_cf_close(a);
  kvstore_cf_iterator_create(b, &it);
  kvstore_iterator_first(it);
  CHECK(kvstore_cf_drop(kv, "a") == KVSTORE_BUSY, "drop returns BUSY with an iterator open");
  CHECK(!kvstore_iterator_eof(it), "iterator still usable");
  kvstore_iterator_close(it);
  CHECK(kvstore_cf_drop(kv, "a") == KVSTORE_OK, "drop works after closing the iterator");
  void *v = NULL; int n = 0;
  CHECK(kvstore_cf_get(b, "k", 1, &v, &n) == KVSTORE_OK, "b still readable");
  snkv_free(v);
  kvstore_cf_close(b);
  kvstore_close(kv);
  cleanup(db);
}

/* ----------------------------------------------------------------------
** Test 9 (fix: purge inside the caller's transaction):
** kvstore_purge_expired used to commit the caller's write transaction, so
** writes made earlier in it could no longer be rolled back.
** -------------------------------------------------------------------- */
static void test_purge_in_caller_txn(void){
  const char *db = "t_rf_purge.db";
  printf("Test 9: purge_expired inside kvstore_begin keeps the transaction\n");
  cleanup(db);
  KVStore *kv = NULL;
  void *v = NULL; int n = 0, del = 0;
  kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL);
  kvstore_put_ttl(kv, "old", 3, "x", 1, kvstore_now_ms() - 1);   /* expired */
  kvstore_put(kv, "keep", 4, "y", 1);

  CHECK(kvstore_begin(kv, 1) == KVSTORE_OK, "begin");
  kvstore_put(kv, "mine", 4, "uncommitted", 11);
  CHECK(kvstore_purge_expired(kv, &del) == KVSTORE_OK && del == 1, "purge inside the transaction");
  CHECK(kvstore_rollback(kv) == KVSTORE_OK, "rollback");
  CHECK(kvstore_get(kv, "mine", 4, &v, &n) == KVSTORE_NOTFOUND,
        "write made before the purge was rolled back");
  if( v ){ snkv_free(v); v = NULL; }

  /* Commit path: the purge's deletes become permanent with the caller's commit. */
  kvstore_put_ttl(kv, "old2", 4, "x", 1, kvstore_now_ms() - 1);
  CHECK(kvstore_begin(kv, 1) == KVSTORE_OK, "begin again");
  kvstore_purge_expired(kv, &del);
  CHECK(kvstore_commit(kv) == KVSTORE_OK, "commit");
  int ex = 1;
  kvstore_exists(kv, "old2", 4, &ex);
  CHECK(ex == 0, "purged key stays gone after commit");
  CHECK(kvstore_get(kv, "keep", 4, &v, &n) == KVSTORE_OK, "untouched key still there");
  snkv_free(v);
  kvstore_close(kv);
  cleanup(db);
}

/* ----------------------------------------------------------------------
** Shared helper for Tests 10-12: fill default CF and a user CF with values
** of many sizes (0 bytes, inline, and multi-page overflow), then check them.
** -------------------------------------------------------------------- */
#define ENC_N 120
static int enc_val_len(int i){ return (i * 97) % 6000; }   /* 0 .. 5999 */
static void enc_make_val(char *buf, int i){
  int n = enc_val_len(i);
  for( int j = 0; j < n; j++ ) buf[j] = (char)('a' + (i + j) % 26);
}
static void enc_fill(KVStore *kv, KVColumnFamily *cf){
  static char val[6000];
  char key[16];
  for( int i = 0; i < ENC_N; i++ ){
    snprintf(key, sizeof key, "k%03d", i);
    enc_make_val(val, i);
    kvstore_put(kv, key, (int)strlen(key), val, enc_val_len(i));
    if( cf ) kvstore_cf_put(cf, key, (int)strlen(key), val, enc_val_len(i));
  }
}
/* Returns the number of keys whose value is wrong or unreadable. */
static int enc_verify(KVStore *kv, KVColumnFamily *cf){
  static char want[6000];
  char key[16];
  int bad = 0;
  for( int i = 0; i < ENC_N; i++ ){
    snprintf(key, sizeof key, "k%03d", i);
    enc_make_val(want, i);
    for( int pass = 0; pass < (cf ? 2 : 1); pass++ ){
      void *v = NULL; int n = 0;
      int rc = pass ? kvstore_cf_get(cf, key, (int)strlen(key), &v, &n)
                    : kvstore_get(kv, key, (int)strlen(key), &v, &n);
      if( rc != KVSTORE_OK || n != enc_val_len(i) || (n && memcmp(v, want, n)) ) bad++;
      if( v ) snkv_free(v);
    }
  }
  return bad;
}

/* ----------------------------------------------------------------------
** Test 10: a failed reencrypt / remove_encryption changes nothing
** Bug: reencrypt switched the handle to the new key before the commit, and
** remove_encryption cleared the key and bEncrypted before the commit.  When
** the commit failed (here: SQLITE_BUSY because another connection holds a
** read lock on a rollback-journal database) the handle no longer matched
** the data.  A failed auto-commit also left the btree inside its write
** transaction, so the half-done rewrite could be committed later.
** -------------------------------------------------------------------- */
static void test_enc_failure_keeps_key(void){
  const char *db = "t_rf_encfail.db";
  printf("Test 10: failed reencrypt / remove_encryption keep the old key\n");
  cleanup(db);

  KVStoreConfig cfg; memset(&cfg, 0, sizeof cfg);
  cfg.journalMode = KVSTORE_JOURNAL_DELETE;
  KVStore *kv = NULL, *kv2 = NULL;
  CHECK(kvstore_open_encrypted(db, "old", 3, &kv, &cfg) == KVSTORE_OK, "open encrypted (DELETE journal)");
  enc_fill(kv, NULL);

  /* A second connection with an open read transaction blocks the commit. */
  CHECK(kvstore_open_encrypted(db, "old", 3, &kv2, &cfg) == KVSTORE_OK, "second connection");
  void *v = NULL; int n = 0;
  kvstore_get(kv2, "k001", 4, &v, &n); if( v ){ snkv_free(v); v = NULL; }

  int rc = kvstore_reencrypt(kv, "new", 3);
  CHECK(rc != KVSTORE_OK, "reencrypt fails while another connection reads");
  CHECK(enc_verify(kv, NULL) == 0, "all values still readable with the old key");

  rc = kvstore_remove_encryption(kv);
  CHECK(rc != KVSTORE_OK, "remove_encryption fails while another connection reads");
  CHECK(kvstore_is_encrypted(kv) == 1, "store still reported as encrypted");
  CHECK(enc_verify(kv, NULL) == 0, "all values still readable after failed remove");

  /* Nothing may have been written: the blocker leaves, the store reopens. */
  kvstore_close(kv2);
  CHECK(kvstore_put(kv, "after", 5, "ok", 2) == KVSTORE_OK, "handle still writable");
  kvstore_close(kv);
  kv = NULL;
  CHECK(kvstore_open_encrypted(db, "new", 3, &kv, &cfg) == KVSTORE_AUTH_FAILED,
        "new password was never committed");
  kv = NULL;
  CHECK(kvstore_open_encrypted(db, "old", 3, &kv, &cfg) == KVSTORE_OK, "old password still opens");
  if( kv ){
    CHECK(enc_verify(kv, NULL) == 0, "values intact after reopen");
    CHECK(kvstore_integrity_check(kv, NULL) == KVSTORE_OK, "integrity check");
    kvstore_close(kv);
  }
  cleanup(db);
}

/* ----------------------------------------------------------------------
** Test 11: reencrypt / remove_encryption refuse to run inside the caller's
** transaction.  A later kvstore_rollback would restore the old ciphertext
** while the handle kept the new key (or no key).
** -------------------------------------------------------------------- */
static void test_enc_refused_in_txn(void){
  const char *db = "t_rf_enctxn.db";
  printf("Test 11: reencrypt / remove_encryption refused inside a transaction\n");
  cleanup(db);
  KVStore *kv = NULL;
  CHECK(kvstore_open_encrypted(db, "pw", 2, &kv, NULL) == KVSTORE_OK, "open encrypted");
  kvstore_put(kv, "a", 1, "alpha", 5);
  CHECK(kvstore_begin(kv, 1) == KVSTORE_OK, "begin");
  CHECK(kvstore_reencrypt(kv, "pw2", 3) == KVSTORE_ERROR, "reencrypt refused");
  CHECK(kvstore_remove_encryption(kv) == KVSTORE_ERROR, "remove_encryption refused");
  CHECK(kvstore_rollback(kv) == KVSTORE_OK, "rollback");
  void *v = NULL; int n = 0;
  CHECK(kvstore_get(kv, "a", 1, &v, &n) == KVSTORE_OK && n == 5 && memcmp(v, "alpha", 5) == 0,
        "value readable after rollback");
  if( v ) snkv_free(v);
  CHECK(kvstore_reencrypt(kv, "pw2", 3) == KVSTORE_OK, "reencrypt works outside a transaction");
  kvstore_close(kv);
  kv = NULL;
  CHECK(kvstore_open_encrypted(db, "pw2", 3, &kv, NULL) == KVSTORE_OK, "reopen with new password");
  kvstore_close(kv);
  cleanup(db);
}

/* ----------------------------------------------------------------------
** Test 12: re-encrypt and decrypt rewrite every value, in every CF
** Covers the shared rewrite helper with empty, small and overflow values.
** -------------------------------------------------------------------- */
static void test_enc_rewrite_roundtrip(void){
  const char *db = "t_rf_encrt.db";
  printf("Test 12: reencrypt / remove_encryption round trip, all value sizes\n");
  cleanup(db);
  KVStore *kv = NULL; KVColumnFamily *cf = NULL;
  CHECK(kvstore_open_encrypted(db, "one", 3, &kv, NULL) == KVSTORE_OK, "open encrypted");
  CHECK(kvstore_cf_create(kv, "u", &cf) == KVSTORE_OK, "create CF u");
  enc_fill(kv, cf);
  CHECK(kvstore_reencrypt(kv, "two", 3) == KVSTORE_OK, "reencrypt");
  CHECK(enc_verify(kv, cf) == 0, "all values correct after reencrypt");
  kvstore_cf_close(cf); kvstore_close(kv);

  kv = NULL; cf = NULL;
  CHECK(kvstore_open_encrypted(db, "two", 3, &kv, NULL) == KVSTORE_OK, "reopen with new password");
  kvstore_cf_open(kv, "u", &cf);
  CHECK(enc_verify(kv, cf) == 0, "all values correct after reopen");
  CHECK(kvstore_remove_encryption(kv) == KVSTORE_OK, "remove_encryption");
  CHECK(enc_verify(kv, cf) == 0, "all values correct after decrypt");
  kvstore_cf_close(cf); kvstore_close(kv);

  kv = NULL; cf = NULL;
  CHECK(kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL) == KVSTORE_OK, "plain reopen");
  kvstore_cf_open(kv, "u", &cf);
  CHECK(enc_verify(kv, cf) == 0, "all values correct as plaintext");
  CHECK(kvstore_integrity_check(kv, NULL) == KVSTORE_OK, "integrity check");
  kvstore_cf_close(cf); kvstore_close(kv);
  cleanup(db);
}

/* ----------------------------------------------------------------------
** Test 13: kvstore_sync really syncs
** Bug: kvstore_sync returned KVSTORE_OK without touching the disk, so with
** syncLevel OFF (common on flash-wear-sensitive devices) nothing was ever
** made durable.  A VFS wrapper counts xSync calls.
** -------------------------------------------------------------------- */
static sqlite3_vfs *g_realVfs;
static int g_nSync;
typedef struct CntFile { sqlite3_file base; sqlite3_file *pReal; } CntFile;
#define REAL(f) (((CntFile*)(f))->pReal)
static int cntClose(sqlite3_file *f){ return REAL(f)->pMethods->xClose(REAL(f)); }
static int cntRead(sqlite3_file *f, void *p, int n, sqlite3_int64 o){ return REAL(f)->pMethods->xRead(REAL(f), p, n, o); }
static int cntWrite(sqlite3_file *f, const void *p, int n, sqlite3_int64 o){ return REAL(f)->pMethods->xWrite(REAL(f), p, n, o); }
static int cntTruncate(sqlite3_file *f, sqlite3_int64 s){ return REAL(f)->pMethods->xTruncate(REAL(f), s); }
static int cntSync(sqlite3_file *f, int fl){ g_nSync++; return REAL(f)->pMethods->xSync(REAL(f), fl); }
static int cntFileSize(sqlite3_file *f, sqlite3_int64 *p){ return REAL(f)->pMethods->xFileSize(REAL(f), p); }
static int cntLock(sqlite3_file *f, int l){ return REAL(f)->pMethods->xLock(REAL(f), l); }
static int cntUnlock(sqlite3_file *f, int l){ return REAL(f)->pMethods->xUnlock(REAL(f), l); }
static int cntCheckRes(sqlite3_file *f, int *p){ return REAL(f)->pMethods->xCheckReservedLock(REAL(f), p); }
static int cntFileCtl(sqlite3_file *f, int op, void *p){ return REAL(f)->pMethods->xFileControl(REAL(f), op, p); }
static int cntSector(sqlite3_file *f){ return REAL(f)->pMethods->xSectorSize(REAL(f)); }
static int cntDevChar(sqlite3_file *f){ return REAL(f)->pMethods->xDeviceCharacteristics(REAL(f)); }
static int cntShmMap(sqlite3_file *f, int i, int sz, int b, void volatile **pp){ return REAL(f)->pMethods->xShmMap(REAL(f), i, sz, b, pp); }
static int cntShmLock(sqlite3_file *f, int o, int n, int fl){ return REAL(f)->pMethods->xShmLock(REAL(f), o, n, fl); }
static void cntShmBarrier(sqlite3_file *f){ REAL(f)->pMethods->xShmBarrier(REAL(f)); }
static int cntShmUnmap(sqlite3_file *f, int d){ return REAL(f)->pMethods->xShmUnmap(REAL(f), d); }
static const sqlite3_io_methods cntIo = {
  2, cntClose, cntRead, cntWrite, cntTruncate, cntSync, cntFileSize, cntLock,
  cntUnlock, cntCheckRes, cntFileCtl, cntSector, cntDevChar,
  cntShmMap, cntShmLock, cntShmBarrier, cntShmUnmap, 0, 0
};
static int cntOpen(sqlite3_vfs *v, const char *z, sqlite3_file *f, int fl, int *pOut){
  (void)v;
  CntFile *p = (CntFile*)f;
  p->pReal = (sqlite3_file*)&p[1];
  int rc = g_realVfs->xOpen(g_realVfs, z, p->pReal, fl, pOut);
  p->base.pMethods = (rc == SQLITE_OK && p->pReal->pMethods) ? &cntIo : 0;
  return rc;
}
static int cntDelete(sqlite3_vfs *v, const char *z, int d){ (void)v; return g_realVfs->xDelete(g_realVfs, z, d); }
static int cntAccess(sqlite3_vfs *v, const char *z, int fl, int *p){ (void)v; return g_realVfs->xAccess(g_realVfs, z, fl, p); }
static int cntFullPath(sqlite3_vfs *v, const char *z, int n, char *o){ (void)v; return g_realVfs->xFullPathname(g_realVfs, z, n, o); }
static void *cntDlOpen(sqlite3_vfs *v, const char *z){ (void)v; return g_realVfs->xDlOpen(g_realVfs, z); }
static void cntDlError(sqlite3_vfs *v, int n, char *z){ (void)v; g_realVfs->xDlError(g_realVfs, n, z); }
static void (*cntDlSym(sqlite3_vfs *v, void *p, const char *z))(void){ (void)v; return g_realVfs->xDlSym(g_realVfs, p, z); }
static void cntDlClose(sqlite3_vfs *v, void *p){ (void)v; g_realVfs->xDlClose(g_realVfs, p); }
static int cntRandom(sqlite3_vfs *v, int n, char *z){ (void)v; return g_realVfs->xRandomness(g_realVfs, n, z); }
static int cntSleep(sqlite3_vfs *v, int us){ (void)v; return g_realVfs->xSleep(g_realVfs, us); }
static int cntTime(sqlite3_vfs *v, double *p){ (void)v; return g_realVfs->xCurrentTime(g_realVfs, p); }
static int cntLastErr(sqlite3_vfs *v, int n, char *z){ (void)v; return g_realVfs->xGetLastError(g_realVfs, n, z); }
static int cntTime64(sqlite3_vfs *v, sqlite3_int64 *p){ (void)v; return g_realVfs->xCurrentTimeInt64(g_realVfs, p); }
static sqlite3_vfs cntVfs;

static void sync_case(const char *db, int journalMode){
  KVStoreConfig cfg; memset(&cfg, 0, sizeof cfg);
  cfg.journalMode = journalMode;
  cfg.syncLevel   = KVSTORE_SYNC_OFF;          /* commits never fsync */
  KVStore *kv = NULL;
  cleanup(db);
  CHECK(kvstore_open_v2(db, &kv, &cfg) == KVSTORE_OK, "open with syncLevel OFF");
  kvstore_put(kv, "a", 1, "1", 1);
  kvstore_put(kv, "b", 1, "2", 1);
  int before = g_nSync;
  CHECK(kvstore_sync(kv) == KVSTORE_OK, "kvstore_sync returns OK");
  CHECK(g_nSync > before, "kvstore_sync issued an fsync");
  CHECK(kvstore_begin(kv, 1) == KVSTORE_OK, "begin");
  CHECK(kvstore_sync(kv) == KVSTORE_ERROR, "sync refused inside a transaction");
  kvstore_rollback(kv);
  kvstore_close(kv);
  cleanup(db);
}

static void test_sync_fsyncs(void){
  printf("Test 13: kvstore_sync forces data to disk\n");
  g_realVfs = sqlite3_vfs_find(0);
  memset(&cntVfs, 0, sizeof cntVfs);
  cntVfs.iVersion = 2;
  cntVfs.szOsFile = (int)sizeof(CntFile) + g_realVfs->szOsFile;
  cntVfs.mxPathname = g_realVfs->mxPathname;
  cntVfs.zName = "cntvfs";
  cntVfs.xOpen = cntOpen; cntVfs.xDelete = cntDelete; cntVfs.xAccess = cntAccess;
  cntVfs.xFullPathname = cntFullPath; cntVfs.xDlOpen = cntDlOpen;
  cntVfs.xDlError = cntDlError; cntVfs.xDlSym = cntDlSym; cntVfs.xDlClose = cntDlClose;
  cntVfs.xRandomness = cntRandom; cntVfs.xSleep = cntSleep;
  cntVfs.xCurrentTime = cntTime; cntVfs.xGetLastError = cntLastErr;
  cntVfs.xCurrentTimeInt64 = cntTime64;
  sqlite3_vfs_register(&cntVfs, 1);

  printf("  -- WAL\n");
  sync_case("t_rf_sync_wal.db", KVSTORE_JOURNAL_WAL);
  printf("  -- rollback journal\n");
  sync_case("t_rf_sync_del.db", KVSTORE_JOURNAL_DELETE);

  sqlite3_vfs_unregister(&cntVfs);
  sqlite3_vfs_register(g_realVfs, 1);
}

/* ----------------------------------------------------------------------
** Test 14: incremental vacuum with cached read cursors and iterators
** Bug: vacuum upgraded the persistent read transaction in place, so the
** handles' cached read cursors stayed open while pages were moved, and the
** read transaction was not restored afterwards.  Vacuum is allowed while an
** iterator is open: cursor positions are saved before pages move.
** -------------------------------------------------------------------- */
static void test_vacuum_cursors(void){
  const char *db = "t_rf_vac.db";
  printf("Test 14: incremental vacuum with cached cursors / iterators\n");
  cleanup(db);
  KVStore *kv = NULL; KVColumnFamily *cf = NULL;
  kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL);
  kvstore_cf_create(kv, "c", &cf);
  char key[16], val[300];
  memset(val, 'v', sizeof val);
  for( int i = 0; i < 2000; i++ ){
    snprintf(key, sizeof key, "k%05d", i);
    kvstore_cf_put(cf, key, (int)strlen(key), val, (int)sizeof val);
    kvstore_put(kv, key, (int)strlen(key), val, 50);
  }
  for( int i = 0; i < 2000; i++ ){
    if( i % 10 == 0 ) continue;                 /* keep every 10th key */
    snprintf(key, sizeof key, "k%05d", i);
    kvstore_cf_delete(cf, key, (int)strlen(key));
  }
  /* Cache read cursors on both CFs. */
  void *v = NULL; int n = 0;
  kvstore_cf_get(cf, "k01990", 6, &v, &n); if( v ){ snkv_free(v); v = NULL; }
  kvstore_get(kv, "k00005", 6, &v, &n);    if( v ){ snkv_free(v); v = NULL; }

  /* Vacuum in the middle of an iteration: the iterator must carry on from
  ** its key and see every kept key exactly once, in order. */
  KVIterator *it = NULL;
  CHECK(kvstore_cf_iterator_create(cf, &it) == KVSTORE_OK, "open an iterator");
  int seen = 0, order = 1, vacRc = -1;
  char prev[16] = "";
  for( kvstore_iterator_first(it); !kvstore_iterator_eof(it); kvstore_iterator_next(it) ){
    void *pk = NULL; int nk = 0;
    if( kvstore_iterator_key(it, &pk, &nk) != KVSTORE_OK || nk >= (int)sizeof prev ){ order = 0; break; }
    char cur[16]; memcpy(cur, pk, nk); cur[nk] = 0;
    if( seen > 0 && strcmp(prev, cur) >= 0 ) order = 0;
    memcpy(prev, cur, nk + 1);
    if( ++seen == 100 ) vacRc = kvstore_incremental_vacuum(kv, 0);
  }
  kvstore_iterator_close(it);
  CHECK(vacRc == KVSTORE_OK, "vacuum ok while an iterator is open");
  CHECK(seen == 200 && order, "iterator sees every kept key once, in order, across the vacuum");

  CHECK(kvstore_incremental_vacuum(kv, 0) == KVSTORE_OK, "vacuum ok");
  int bad = 0;
  for( int i = 0; i < 2000; i += 10 ){
    snprintf(key, sizeof key, "k%05d", i);
    if( kvstore_cf_get(cf, key, (int)strlen(key), &v, &n) != KVSTORE_OK || n != (int)sizeof val ) bad++;
    if( v ){ snkv_free(v); v = NULL; }
  }
  CHECK(bad == 0, "every kept key readable through the cached handle");
  int ex = 1;
  kvstore_cf_exists(cf, "k00001", 6, &ex);
  CHECK(ex == 0, "deleted key stays deleted");
  CHECK(kvstore_get(kv, "k01999", 6, &v, &n) == KVSTORE_OK && n == 50, "default CF readable");
  if( v ) snkv_free(v);
  CHECK(kvstore_integrity_check(kv, NULL) == KVSTORE_OK, "integrity check");
  kvstore_cf_close(cf);
  kvstore_close(kv);
  cleanup(db);
}

/* ----------------------------------------------------------------------
** Test 15: KVSTORE_MAX_COLUMN_FAMILIES is enforced
** Bug: the documented limit (64, counting the default CF) was never
** checked.  Hidden TTL CFs must not count against it.
** -------------------------------------------------------------------- */
static void test_cf_limit(void){
  const char *db = "t_rf_cflimit.db";
  printf("Test 15: column family limit\n");
  cleanup(db);
  KVStore *kv = NULL; KVColumnFamily *cf = NULL;
  kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL);
  char name[32];
  int ok = 0;
  for( int i = 0; i < KVSTORE_MAX_COLUMN_FAMILIES - 1; i++ ){
    snprintf(name, sizeof name, "cf%02d", i);
    if( kvstore_cf_create(kv, name, &cf) == KVSTORE_OK ){
      ok++;
      if( i == 0 ) kvstore_cf_put_ttl(cf, "t", 1, "x", 1, kvstore_now_ms() + 60000);
      kvstore_cf_close(cf);
    }
  }
  CHECK(ok == KVSTORE_MAX_COLUMN_FAMILIES - 1, "63 named CFs + default created (TTL CFs not counted)");
  cf = NULL;
  CHECK(kvstore_cf_create(kv, "one_too_many", &cf) == KVSTORE_ERROR && cf == NULL,
        "64th named CF refused");
  CHECK(kvstore_cf_drop(kv, "cf05") == KVSTORE_OK, "drop one");
  CHECK(kvstore_cf_create(kv, "one_too_many", &cf) == KVSTORE_OK, "create succeeds after drop");
  kvstore_cf_close(cf);
  kvstore_close(kv);
  cleanup(db);
}

/* ----------------------------------------------------------------------
** Test 16: rolling back a drop puts moved handles back
** Bug: a drop moves the highest table into the freed root page and the open
** handles are re-pointed.  If the transaction was then rolled back (by the
** caller, or because a later step of the drop failed) the move was undone
** on disk but the handles kept the new root, so they read the dropped CF.
** -------------------------------------------------------------------- */
static void test_drop_rollback_restores_handles(void){
  const char *db = "t_rf_droprb.db";
  printf("Test 16: rollback of a drop restores open handles\n");
  cleanup(db);
  KVStore *kv = NULL; KVColumnFamily *a = NULL, *b = NULL;
  void *v = NULL; int n = 0;
  kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL);
  kvstore_cf_create(kv, "b", &b);                 /* lower root */
  kvstore_cf_put(b, "kb", 2, "bee", 3);
  kvstore_cf_close(b); b = NULL;
  kvstore_cf_create(kv, "a", &a);                 /* highest root: moves */
  kvstore_cf_put(a, "ka", 2, "ay", 2);

  CHECK(kvstore_begin(kv, 1) == KVSTORE_OK, "begin");
  CHECK(kvstore_cf_drop(kv, "b") == KVSTORE_OK, "drop b inside the transaction");
  CHECK(kvstore_cf_get(a, "ka", 2, &v, &n) == KVSTORE_OK && n == 2, "a readable after the move");
  if( v ){ snkv_free(v); v = NULL; }
  CHECK(kvstore_rollback(kv) == KVSTORE_OK, "rollback");

  CHECK(kvstore_cf_get(a, "ka", 2, &v, &n) == KVSTORE_OK && n == 2 && memcmp(v, "ay", 2) == 0,
        "a reads its own data after the rollback");
  if( v ){ snkv_free(v); v = NULL; }
  CHECK(kvstore_cf_put(a, "ka2", 3, "x", 1) == KVSTORE_OK, "write through a after the rollback");
  CHECK(kvstore_cf_open(kv, "b", &b) == KVSTORE_OK, "b exists again");
  if( b ){
    CHECK(kvstore_cf_get(b, "kb", 2, &v, &n) == KVSTORE_OK && n == 3, "b keeps its data");
    if( v ){ snkv_free(v); v = NULL; }
    int ex = 1;
    kvstore_cf_exists(b, "ka2", 3, &ex);
    CHECK(ex == 0, "a's write did not land in b");
    kvstore_cf_close(b);
  }
  CHECK(kvstore_integrity_check(kv, NULL) == KVSTORE_OK, "integrity check");
  kvstore_cf_close(a);
  kvstore_close(kv);
  cleanup(db);
}

/* ----------------------------------------------------------------------
** Test 17: Argon2 cost read from the file is bounds-checked
** Bug: the memory and pass counts stored in an encrypted file were used
** as-is.  On a 32-bit target mCost * 1024 wrapped around, a small buffer
** was allocated and Argon2 wrote past it; a huge pass count hung the
** device.  Now out-of-range values make the open fail with
** KVSTORE_CORRUPT before anything is allocated.
** -------------------------------------------------------------------- */
/* Overwrite the 8 bytes after every "params" key (the stored mCost/tCost).
** Returns how many places were patched. */
static int patch_params(const char *db, const unsigned char aNew[8]){
  FILE *f = fopen(db, "r+b");
  if( !f ) return 0;
  fseek(f, 0, SEEK_END); long sz = ftell(f); fseek(f, 0, SEEK_SET);
  unsigned char *buf = (unsigned char*)malloc((size_t)sz);
  int n = 0;
  if( buf && fread(buf, 1, (size_t)sz, f) == (size_t)sz ){
    static const unsigned char pat[] = { 0,0,0,6, 'p','a','r','a','m','s' };
    for( long i = 0; i + (long)sizeof pat + 8 <= sz; i++ ){
      if( memcmp(buf + i, pat, sizeof pat) == 0 ){
        fseek(f, i + (long)sizeof pat, SEEK_SET);
        fwrite(aNew, 1, 8, f);
        n++;
      }
    }
  }
  free(buf);
  fclose(f);
  return n;
}

static void test_enc_params_checked(void){
  const char *db = "t_rf_params.db";
  printf("Test 17: out-of-range Argon2 cost in the file is rejected\n");
  KVStoreConfig cfg; memset(&cfg, 0, sizeof cfg);
  cfg.journalMode = KVSTORE_JOURNAL_DELETE;      /* everything in the main file */
  static const unsigned char aHugeMem[8]  = { 0xFF,0xFF,0xFF,0xFF, 0,0,0,3 };
  static const unsigned char aTinyMem[8]  = { 0,0,0,1,             0,0,0,3 };
  static const unsigned char aHugePass[8] = { 0,0,1,0,             0xFF,0xFF,0xFF,0xFF };
  const unsigned char *aCase[3] = { aHugeMem, aTinyMem, aHugePass };
  const char *zCase[3] = { "memory cost 4 TiB", "memory cost 1 KiB", "4 billion passes" };

  for( int c = 0; c < 3; c++ ){
    char msg[96];
    KVStore *kv = NULL;
    cleanup(db);
    kvstore_open_encrypted(db, "pw", 2, &kv, &cfg);
    kvstore_put(kv, "k", 1, "v", 1);
    kvstore_close(kv);
    snprintf(msg, sizeof msg, "%s written into the file", zCase[c]);
    CHECK(patch_params(db, aCase[c]) >= 1, msg);
    kv = NULL;
    int rc = kvstore_open_encrypted(db, "pw", 2, &kv, &cfg);
    snprintf(msg, sizeof msg, "%s: open fails with KVSTORE_CORRUPT", zCase[c]);
    CHECK(rc == KVSTORE_CORRUPT && kv == NULL, msg);
    if( kv ) kvstore_close(kv);
  }
  cleanup(db);

  /* A negative cacheSize is a budget in KiB (small devices). */
  cfg.journalMode = KVSTORE_JOURNAL_WAL;
  cfg.cacheSize   = -32;
  KVStore *kv = NULL;
  CHECK(kvstore_open_v2(db, &kv, &cfg) == KVSTORE_OK, "open with cacheSize = -32 (32 KiB)");
  char key[16], val[200]; memset(val, 'x', sizeof val);
  int bad = 0;
  for( int i = 0; i < 2000; i++ ){
    snprintf(key, sizeof key, "k%05d", i);
    if( kvstore_put(kv, key, (int)strlen(key), val, (int)sizeof val) != KVSTORE_OK ) bad++;
  }
  for( int i = 0; i < 2000; i += 37 ){
    void *v = NULL; int n = 0;
    snprintf(key, sizeof key, "k%05d", i);
    if( kvstore_get(kv, key, (int)strlen(key), &v, &n) != KVSTORE_OK || n != (int)sizeof val ) bad++;
    if( v ) snkv_free(v);
  }
  CHECK(bad == 0, "2000 puts and reads through a 32 KiB cache");
  CHECK(kvstore_integrity_check(kv, NULL) == KVSTORE_OK, "integrity check");
  kvstore_close(kv);
  cleanup(db);
}

/* ----------------------------------------------------------------------
** Test 18: page-based WAL auto-checkpoint keeps the WAL bounded
** Before: SNKV never checkpointed by default (only an opt-in commit-count
** setting existed), so the WAL
** grew to the size of the database and every page-cache miss had to search
** an ever larger WAL index.  Now a PASSIVE checkpoint runs whenever the WAL
** reaches walAutoCheckpoint pages (default 1000, as in SQLite).
** The WAL size is read through kvstore_checkpoint()'s pnLog (frames in the
** WAL), which works on Windows too (stat() sizes of open files do not).
** -------------------------------------------------------------------- */
/* Write nCommit transactions of 100 x 1 KB values; return the WAL size in
** frames (measured by a PASSIVE checkpoint) and the auto-checkpoint count. */
static void walauto_run(const char *db, int walAutoCheckpoint, int nCommit,
                        int *pnLog, long long *pnAuto){
  KVStoreConfig cfg; memset(&cfg, 0, sizeof cfg);
  cfg.journalMode = KVSTORE_JOURNAL_WAL;
  cfg.walAutoCheckpoint = walAutoCheckpoint;
  KVStore *kv = NULL;
  cleanup(db);
  *pnLog = -1; *pnAuto = -1;
  if( kvstore_open_v2(db, &kv, &cfg) != KVSTORE_OK ) return;
  static char val[1024];
  memset(val, 'w', sizeof val);
  char key[32];
  for( int c = 0; c < nCommit; c++ ){
    kvstore_begin(kv, 1);
    for( int i = 0; i < 100; i++ ){
      snprintf(key, sizeof key, "k%06d", c * 100 + i);
      kvstore_put(kv, key, (int)strlen(key), val, (int)sizeof val);
    }
    kvstore_commit(kv);
  }
  KVStoreStats st;
  kvstore_stats(kv, &st);
  *pnAuto = (long long)st.nCheckpoints;
  int nLog = -1, nCkpt = -1;
  kvstore_checkpoint(kv, KVSTORE_CHECKPOINT_PASSIVE, &nLog, &nCkpt);
  *pnLog = nLog;
  /* The data must be intact whatever the checkpoint setting. */
  void *v = NULL; int n = 0;
  snprintf(key, sizeof key, "k%06d", nCommit * 100 - 1);
  if( kvstore_get(kv, key, (int)strlen(key), &v, &n) != KVSTORE_OK || n != (int)sizeof val ) *pnLog = -2;
  if( v ) snkv_free(v);
  if( kvstore_integrity_check(kv, NULL) != KVSTORE_OK ) *pnLog = -3;
  kvstore_close(kv);
  cleanup(db);
}

static void test_wal_auto_checkpoint(void){
  const char *db = "t_rf_walauto.db";
  int nLog; long long nAuto;
  char msg[128];
  printf("Test 18: page-based WAL auto-checkpoint\n");

  /* 80 commits x ~30 pages each ≈ 2400 WAL pages written in total. */
  walauto_run(db, 0, 80, &nLog, &nAuto);           /* default: 1000 pages */
  snprintf(msg, sizeof msg, "default: WAL stays bounded (%d frames, limit 1000 + one commit)", nLog);
  CHECK(nLog >= 0 && nLog < KVSTORE_DEFAULT_WAL_AUTOCHECKPOINT + 200, msg);
  CHECK(nAuto >= 1, "default: auto-checkpoint ran");

  walauto_run(db, 200, 80, &nLog, &nAuto);         /* custom: 200 pages */
  snprintf(msg, sizeof msg, "walAutoCheckpoint=200: WAL stays bounded (%d frames)", nLog);
  CHECK(nLog >= 0 && nLog < 200 + 200, msg);
  CHECK(nAuto >= 5, "walAutoCheckpoint=200: checkpoints ran repeatedly");

  walauto_run(db, -1, 80, &nLog, &nAuto);          /* disabled */
  snprintf(msg, sizeof msg, "walAutoCheckpoint=-1: WAL grows (%d frames)", nLog);
  CHECK(nLog > KVSTORE_DEFAULT_WAL_AUTOCHECKPOINT + 200, msg);
  CHECK(nAuto == 0, "walAutoCheckpoint=-1: no auto-checkpoint");
}

/* ----------------------------------------------------------------------
** Test 19: an orphan TTL index is emptied on open
** A CF's TTL indexes come in pairs.  When only the expiry index exists, open
** must empty it — even with an iterator open — or its stale rows purge the
** CF's keys once a new key index is created.  The old cleanup dropped the
** orphan, skipped that when an iterator was open, and a drop could move the
** table of the CF being opened.
** -------------------------------------------------------------------- */
int kvstoreDropInternalCFs(KVStore *pKV, const char **azName, int nName);

static void test_orphan_ttl_cleared(void){
  const char *db = "t_rf_orphan.db";
  printf("Test 19: orphan TTL index emptied on open\n");
  cleanup(db);
  KVStore *kv = NULL; KVColumnFamily *cf = NULL;
  void *v = NULL; int n = 0, nDel = -1;
  const char *zKeyIdx[] = { "__snkv_ttl_k__o" };
  const char *zDefIdx[] = { "__snkv_ttl_k__default" };

  /* User CF: leave only the expiry index, with a row due in 300 ms. */
  kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL);
  kvstore_cf_create(kv, "o", &cf);
  kvstore_cf_put_ttl(cf, "a", 1, "1", 1, kvstore_now_ms() + 300);
  kvstore_cf_close(cf); cf = NULL;
  CHECK(kvstoreDropInternalCFs(kv, zKeyIdx, 1) == KVSTORE_OK, "drop the key index (orphan the expiry index)");

  /* Open the CF with an iterator open (the old cleanup was skipped here). */
  KVIterator *it = NULL;
  kvstore_iterator_create(kv, &it);
  CHECK(kvstore_cf_open(kv, "o", &cf) == KVSTORE_OK, "cf_open ok with an iterator open");
  kvstore_iterator_close(it);
  CHECK(kvstore_cf_put_ttl(cf, "b", 1, "2", 1, kvstore_now_ms() + 3600000) == KVSTORE_OK, "new TTL key (recreates the key index)");
  usleep(400000);
  CHECK(kvstore_cf_purge_expired(cf, &nDel) == KVSTORE_OK && nDel == 0, "purge deletes nothing: stale expiry row gone");
  CHECK(kvstore_cf_get(cf, "a", 1, &v, &n) == KVSTORE_OK, "key whose TTL index was lost is still readable");
  if( v ){ snkv_free(v); v = NULL; }
  CHECK(kvstore_cf_put(cf, "c", 1, "3", 1) == KVSTORE_OK, "handle stays usable");
  kvstore_cf_close(cf); cf = NULL;

  /* Default CF: the orphan is found by kvstore_open itself. */
  kvstore_put_ttl(kv, "d", 1, "4", 1, kvstore_now_ms() + 300);
  kvstore_close(kv); kv = NULL;
  kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL);
  CHECK(kvstoreDropInternalCFs(kv, zDefIdx, 1) == KVSTORE_OK, "drop the default key index");
  kvstore_close(kv); kv = NULL;
  CHECK(kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL) == KVSTORE_OK, "reopen ok");
  kvstore_put_ttl(kv, "e", 1, "5", 1, kvstore_now_ms() + 3600000);
  usleep(400000);
  nDel = -1;
  CHECK(kvstore_purge_expired(kv, &nDel) == KVSTORE_OK && nDel == 0, "default CF: stale expiry row gone");
  CHECK(kvstore_get(kv, "d", 1, &v, &n) == KVSTORE_OK, "default CF key still readable");
  if( v ) snkv_free(v);
  CHECK(kvstore_integrity_check(kv, NULL) == KVSTORE_OK, "integrity check");
  kvstore_close(kv);
  cleanup(db);
}

/* ----------------------------------------------------------------------
** Test 20: creating an internal CF (vector store) inside kvstore_begin
** must not commit the caller's transaction.
** -------------------------------------------------------------------- */
int kvstoreGetOrCreateInternalCF(KVStore *pKV, const char *zName, KVColumnFamily **ppCF);

static void test_internal_cf_in_txn(void){
  const char *db = "t_rf_icf.db";
  printf("Test 20: internal CF created inside a caller transaction\n");
  cleanup(db);
  KVStore *kv = NULL; KVColumnFamily *icf = NULL;
  void *v = NULL; int n = 0;
  kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL);
  CHECK(kvstore_begin(kv, 1) == KVSTORE_OK, "begin");
  kvstore_put(kv, "x", 1, "1", 1);
  CHECK(kvstoreGetOrCreateInternalCF(kv, "__snkv_test_icf", &icf) == KVSTORE_OK, "create internal CF inside the transaction");
  kvstore_cf_put(icf, "y", 1, "2", 1);
  CHECK(kvstore_rollback(kv) == KVSTORE_OK, "rollback");
  CHECK(kvstore_get(kv, "x", 1, &v, &n) == KVSTORE_NOTFOUND, "caller's write rolled back (was not committed early)");
  if( v ){ snkv_free(v); v = NULL; }
  kvstore_cf_close(icf);
  CHECK(kvstore_integrity_check(kv, NULL) == KVSTORE_OK, "integrity check");
  kvstore_close(kv);
  cleanup(db);
}

int main(void){
  printf("=== test_review_fixes ===\n");
  test_encrypt_user_cf_only();
  test_checkpoint_after_get();
  test_iterator_ttl_skip();
  test_errmsg_stability();
  test_txn_owner_protected();
  test_ttl_count_after_rollback();
  test_drop_keeps_open_handles();
  test_drop_refused_with_iterator();
  test_purge_in_caller_txn();
  test_enc_failure_keeps_key();
  test_enc_refused_in_txn();
  test_enc_rewrite_roundtrip();
  test_sync_fsyncs();
  test_vacuum_cursors();
  test_cf_limit();
  test_drop_rollback_restores_handles();
  test_enc_params_checked();
  test_wal_auto_checkpoint();
  test_orphan_ttl_cleared();
  test_internal_cf_in_txn();
  printf("=== Results: %d passed, %d failed ===\n", nPass, nFail);
  return nFail ? 1 : 0;
}
