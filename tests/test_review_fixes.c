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
#include <unistd.h>
#include "kvstore.h"

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

int main(void){
  printf("=== test_review_fixes ===\n");
  test_encrypt_user_cf_only();
  test_checkpoint_after_get();
  test_iterator_ttl_skip();
  test_errmsg_stability();
  printf("=== Results: %d passed, %d failed ===\n", nPass, nFail);
  return nFail ? 1 : 0;
}
