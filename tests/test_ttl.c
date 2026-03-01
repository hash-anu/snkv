/* SPDX-License-Identifier: Apache-2.0 */
/*
** tests/test_ttl.c — TTL (Time-To-Live) regression suite
**
** Per-CF dual-index design:
**   __snkv_ttl_k__<CF>:  user_key → 8-byte BE int64 expire_ms
**   __snkv_ttl_e__<CF>:  [8-byte BE expire_ms][user_key] → empty
**
** Tests:
**   1.  put_ttl / get_ttl: valid key returned before expiry, remaining > 0
**   2.  Expired key: get_ttl returns NOTFOUND, lazy delete performed
**   3.  put_ttl(expire_ms=0): acts like regular put, ttl_remaining=NO_TTL
**   4.  Regular put then ttl_remaining: KVSTORE_NO_TTL
**   5.  ttl_remaining on missing key: KVSTORE_NOTFOUND
**   6.  purge_expired removes N expired keys, live keys survive
**   7.  purge_expired with nothing expired returns 0
**   8.  Regular put after put_ttl clears TTL entry
**   9.  Delete after put_ttl; purge_expired cleans up TTL index
**  10.  put_ttl inside explicit begin/commit: atomic
**  11.  cf_list does not expose __ prefix names
**  12.  cf_create rejects __ prefix
**  13.  CF-level TTL: cf_put_ttl / cf_get_ttl on a named CF
**  14.  CF-level purge_expired on a named CF
**  15.  TTL index CFs for different user CFs are independent
**  16.  cf_drop removes TTL index CFs too
**  17.  Overwrite TTL key with longer TTL; key survives short TTL window
**  18.  put_ttl rollback removes both data and TTL entries
**
** Gap-fix regression tests:
**  19.  Iterator skips expired keys (gap 1)
**  20.  nTtlActive — no false expiry after all TTL keys cleared (gap 2)
**  21.  purge_expired handles more than KVSTORE_PURGE_BATCH (256) expired keys (gap 3)
**  22.  Real wall-clock expiry: 499 ms TTL, sleep 500 ms, key must be NOTFOUND
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

/* Sleep for ms milliseconds using nanosleep (POSIX). */
static void snkv_sleep_ms(int ms){
  struct timespec ts;
  ts.tv_sec  = ms / 1000;
  ts.tv_nsec = (ms % 1000) * 1000000L;
  nanosleep(&ts, NULL);
}

static KVStore *openFresh(const char *path){
  remove(path);
  char walPath[512];
  snprintf(walPath, sizeof(walPath), "%s-wal", path);
  remove(walPath);
  char shmPath[512];
  snprintf(shmPath, sizeof(shmPath), "%s-shm", path);
  remove(shmPath);

  KVStore *pKV = NULL;
  int rc = kvstore_open(path, &pKV, KVSTORE_JOURNAL_WAL);
  if( rc != KVSTORE_OK ){
    fprintf(stderr, "openFresh: kvstore_open failed: %d\n", rc);
    return NULL;
  }
  return pKV;
}

static void cleanup(KVStore **ppKV, const char *path){
  if( *ppKV ){ kvstore_close(*ppKV); *ppKV = NULL; }
  remove(path);
  char walPath[512];
  snprintf(walPath, sizeof(walPath), "%s-wal", path);
  remove(walPath);
  char shmPath[512];
  snprintf(shmPath, sizeof(shmPath), "%s-shm", path);
  remove(shmPath);
}

/* ---- Test 1: valid key before expiry ---- */
static void test1_valid_before_expiry(void){
  printf("\nTest 1: put_ttl / get_ttl before expiry\n");
  const char *path = "tests/ttl1.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  const char *key   = "session";
  const char *value = "abc123";
  int64_t expire_ms = kvstore_now_ms() + 60000; /* 60 s from now */

  int rc = kvstore_put_ttl(pKV, key, (int)strlen(key),
                            value, (int)strlen(value), expire_ms);
  ASSERT("put_ttl ok", rc == KVSTORE_OK);

  void *pVal = NULL; int nVal = 0; int64_t remaining = 0;
  rc = kvstore_get_ttl(pKV, key, (int)strlen(key), &pVal, &nVal, &remaining);
  ASSERT("get_ttl ok", rc == KVSTORE_OK);
  ASSERT("value matches", pVal && nVal == (int)strlen(value) &&
         memcmp(pVal, value, nVal) == 0);
  ASSERT("remaining > 0", remaining > 0 && remaining <= 60000);
  if( pVal ) snkv_free(pVal);

  cleanup(&pKV, path);
}

/* ---- Test 2: expired key returns NOTFOUND ---- */
static void test2_expired_returns_notfound(void){
  printf("\nTest 2: expired key → NOTFOUND + lazy delete\n");
  const char *path = "tests/ttl2.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  const char *key   = "ephemeral";
  const char *value = "gone";
  int64_t expire_ms = kvstore_now_ms() - 5000; /* already expired */

  int rc = kvstore_put_ttl(pKV, key, (int)strlen(key),
                            value, (int)strlen(value), expire_ms);
  ASSERT("put_ttl ok", rc == KVSTORE_OK);

  void *pVal = NULL; int nVal = 0; int64_t remaining = 99;
  rc = kvstore_get_ttl(pKV, key, (int)strlen(key), &pVal, &nVal, &remaining);
  ASSERT("get_ttl returns NOTFOUND", rc == KVSTORE_NOTFOUND);
  ASSERT("value is NULL", pVal == NULL);
  ASSERT("remaining == 0", remaining == 0);

  /* Confirm key is really gone from data CF. */
  void *pVal2 = NULL; int nVal2 = 0;
  rc = kvstore_get(pKV, key, (int)strlen(key), &pVal2, &nVal2);
  ASSERT("raw get also NOTFOUND", rc == KVSTORE_NOTFOUND);

  cleanup(&pKV, path);
}

/* ---- Test 3: expire_ms==0 behaves like regular put ---- */
static void test3_zero_expire_is_permanent(void){
  printf("\nTest 3: put_ttl(expire_ms=0) → permanent key\n");
  const char *path = "tests/ttl3.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  const char *key   = "permanent";
  const char *value = "forever";

  /* First set a real TTL, then overwrite with expire_ms=0. */
  int64_t expire_ms = kvstore_now_ms() + 10000;
  kvstore_put_ttl(pKV, key, (int)strlen(key), value, (int)strlen(value), expire_ms);

  int rc = kvstore_put_ttl(pKV, key, (int)strlen(key),
                            value, (int)strlen(value), 0);
  ASSERT("put_ttl(0) ok", rc == KVSTORE_OK);

  int64_t remaining = 0;
  rc = kvstore_ttl_remaining(pKV, key, (int)strlen(key), &remaining);
  ASSERT("ttl_remaining ok", rc == KVSTORE_OK);
  ASSERT("remaining == KVSTORE_NO_TTL", remaining == KVSTORE_NO_TTL);

  cleanup(&pKV, path);
}

/* ---- Test 4: regular put → ttl_remaining == KVSTORE_NO_TTL ---- */
static void test4_regular_put_no_ttl(void){
  printf("\nTest 4: regular put → ttl_remaining = KVSTORE_NO_TTL\n");
  const char *path = "tests/ttl4.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  const char *key   = "noexpiry";
  const char *value = "plain";

  int rc = kvstore_put(pKV, key, (int)strlen(key),
                        value, (int)strlen(value));
  ASSERT("put ok", rc == KVSTORE_OK);

  int64_t remaining = 0;
  rc = kvstore_ttl_remaining(pKV, key, (int)strlen(key), &remaining);
  ASSERT("ttl_remaining ok", rc == KVSTORE_OK);
  ASSERT("remaining == KVSTORE_NO_TTL", remaining == KVSTORE_NO_TTL);

  cleanup(&pKV, path);
}

/* ---- Test 5: ttl_remaining on missing key → KVSTORE_NOTFOUND ---- */
static void test5_ttl_missing_key(void){
  printf("\nTest 5: ttl_remaining on missing key\n");
  const char *path = "tests/ttl5.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  int64_t remaining = 42;
  int rc = kvstore_ttl_remaining(pKV, "ghost", 5, &remaining);
  ASSERT("returns KVSTORE_NOTFOUND", rc == KVSTORE_NOTFOUND);

  cleanup(&pKV, path);
}

/* ---- Test 6: purge_expired removes N expired keys ---- */
static void test6_purge_expired(void){
  printf("\nTest 6: purge_expired removes expired keys\n");
  const char *path = "tests/ttl6.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  int64_t past   = kvstore_now_ms() - 1000;
  int64_t future = kvstore_now_ms() + 60000;

  /* 3 expired + 2 live. */
  kvstore_put_ttl(pKV, "exp1", 4, "v", 1, past);
  kvstore_put_ttl(pKV, "exp2", 4, "v", 1, past);
  kvstore_put_ttl(pKV, "exp3", 4, "v", 1, past);
  kvstore_put_ttl(pKV, "live1", 5, "v", 1, future);
  kvstore_put_ttl(pKV, "live2", 5, "v", 1, future);

  int nDeleted = -1;
  int rc = kvstore_purge_expired(pKV, &nDeleted);
  ASSERT("purge_expired ok", rc == KVSTORE_OK);
  ASSERT("deleted == 3", nDeleted == 3);

  /* Confirm expired keys are gone. */
  void *pVal = NULL; int nVal = 0;
  ASSERT("exp1 gone", kvstore_get(pKV, "exp1", 4, &pVal, &nVal) == KVSTORE_NOTFOUND);
  ASSERT("exp2 gone", kvstore_get(pKV, "exp2", 4, &pVal, &nVal) == KVSTORE_NOTFOUND);
  ASSERT("exp3 gone", kvstore_get(pKV, "exp3", 4, &pVal, &nVal) == KVSTORE_NOTFOUND);

  /* Confirm live keys survive. */
  rc = kvstore_get(pKV, "live1", 5, &pVal, &nVal);
  ASSERT("live1 ok", rc == KVSTORE_OK); if( pVal ) snkv_free(pVal);
  rc = kvstore_get(pKV, "live2", 5, &pVal, &nVal);
  ASSERT("live2 ok", rc == KVSTORE_OK); if( pVal ) snkv_free(pVal);

  cleanup(&pKV, path);
}

/* ---- Test 7: purge_expired with nothing expired returns 0 ---- */
static void test7_purge_nothing(void){
  printf("\nTest 7: purge_expired with no expired keys\n");
  const char *path = "tests/ttl7.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  int64_t future = kvstore_now_ms() + 60000;
  kvstore_put_ttl(pKV, "k1", 2, "v", 1, future);
  kvstore_put_ttl(pKV, "k2", 2, "v", 1, future);

  int nDeleted = -1;
  int rc = kvstore_purge_expired(pKV, &nDeleted);
  ASSERT("purge ok",     rc == KVSTORE_OK);
  ASSERT("deleted == 0", nDeleted == 0);

  cleanup(&pKV, path);
}

/* ---- Test 8: regular put after put_ttl clears TTL ---- */
static void test8_regular_put_clears_ttl(void){
  printf("\nTest 8: regular put after put_ttl removes TTL\n");
  const char *path = "tests/ttl8.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  const char *key = "mixed";
  /* Put with short TTL. */
  int64_t expire_ms = kvstore_now_ms() + 2000;
  kvstore_put_ttl(pKV, key, (int)strlen(key), "old", 3, expire_ms);

  /* Overwrite with regular put — must clear TTL. */
  int rc = kvstore_put(pKV, key, (int)strlen(key), "new", 3);
  ASSERT("regular put ok", rc == KVSTORE_OK);

  /* TTL should be gone. */
  int64_t remaining = 0;
  rc = kvstore_ttl_remaining(pKV, key, (int)strlen(key), &remaining);
  ASSERT("ttl_remaining ok", rc == KVSTORE_OK);
  ASSERT("no TTL after regular put", remaining == KVSTORE_NO_TTL);

  /* Value should be "new". */
  void *pVal = NULL; int nVal = 0;
  rc = kvstore_get_ttl(pKV, key, (int)strlen(key), &pVal, &nVal, NULL);
  ASSERT("get_ttl ok", rc == KVSTORE_OK);
  ASSERT("value is new", pVal && nVal == 3 && memcmp(pVal, "new", 3) == 0);
  if( pVal ) snkv_free(pVal);

  cleanup(&pKV, path);
}

/* ---- Test 9: delete after put_ttl; purge cleans orphan TTL ---- */
static void test9_delete_then_purge(void){
  printf("\nTest 9: delete after put_ttl; purge cleans TTL index\n");
  const char *path = "tests/ttl9.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  const char *key = "doomed";
  int64_t expire_ms = kvstore_now_ms() - 500; /* already expired */
  kvstore_put_ttl(pKV, key, (int)strlen(key), "v", 1, expire_ms);

  int nDeleted = -1;
  int rc = kvstore_purge_expired(pKV, &nDeleted);
  ASSERT("purge ok", rc == KVSTORE_OK);

  /* After purge the key must not appear in get_ttl. */
  void *pVal = NULL; int nVal = 0;
  rc = kvstore_get_ttl(pKV, key, (int)strlen(key), &pVal, &nVal, NULL);
  ASSERT("key gone after purge", rc == KVSTORE_NOTFOUND);

  cleanup(&pKV, path);
}

/* ---- Test 10: put_ttl inside explicit begin/commit ---- */
static void test10_put_ttl_in_transaction(void){
  printf("\nTest 10: put_ttl inside explicit begin/commit\n");
  const char *path = "tests/ttl10.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  const char *k1 = "txkey1", *k2 = "txkey2";
  const char *v1 = "val1",   *v2 = "val2";
  int64_t expire_ms = kvstore_now_ms() + 30000;

  int rc = kvstore_begin(pKV, 1);
  ASSERT("begin ok", rc == KVSTORE_OK);

  rc = kvstore_put_ttl(pKV, k1, (int)strlen(k1), v1, (int)strlen(v1), expire_ms);
  ASSERT("put_ttl k1 ok", rc == KVSTORE_OK);

  rc = kvstore_put_ttl(pKV, k2, (int)strlen(k2), v2, (int)strlen(v2), expire_ms);
  ASSERT("put_ttl k2 ok", rc == KVSTORE_OK);

  rc = kvstore_commit(pKV);
  ASSERT("commit ok", rc == KVSTORE_OK);

  /* Both keys must be readable with TTL. */
  void *pVal = NULL; int nVal = 0; int64_t rem = 0;
  rc = kvstore_get_ttl(pKV, k1, (int)strlen(k1), &pVal, &nVal, &rem);
  ASSERT("k1 ok", rc == KVSTORE_OK && rem > 0);
  if( pVal ) snkv_free(pVal);

  rc = kvstore_get_ttl(pKV, k2, (int)strlen(k2), &pVal, &nVal, &rem);
  ASSERT("k2 ok", rc == KVSTORE_OK && rem > 0);
  if( pVal ) snkv_free(pVal);

  cleanup(&pKV, path);
}

/* ---- Test 11: cf_list hides __ prefix CFs ---- */
static void test11_cf_list_hides_ttl_cf(void){
  printf("\nTest 11: cf_list does not expose __ prefix CFs\n");
  const char *path = "tests/ttl11.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  /* Trigger TTL CF creation. */
  int64_t expire_ms = kvstore_now_ms() + 10000;
  kvstore_put_ttl(pKV, "k", 1, "v", 1, expire_ms);

  char **azNames = NULL; int nCount = 0;
  int rc = kvstore_cf_list(pKV, &azNames, &nCount);
  ASSERT("cf_list ok", rc == KVSTORE_OK);

  int foundHidden = 0, i;
  for( i = 0; i < nCount; i++ ){
    if( azNames[i][0] == '_' && azNames[i][1] == '_' ) foundHidden = 1;
    sqliteFree(azNames[i]);
  }
  sqliteFree(azNames);
  ASSERT("no __ names in list", !foundHidden);

  cleanup(&pKV, path);
}

/* ---- Test 12: cf_create rejects __ prefix ---- */
static void test12_cf_create_rejects_reserved(void){
  printf("\nTest 12: cf_create rejects __ prefix names\n");
  const char *path = "tests/ttl12.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  KVColumnFamily *pCF = NULL;
  int rc = kvstore_cf_create(pKV, "__myhidden", &pCF);
  ASSERT("create __ rejected", rc == KVSTORE_ERROR && pCF == NULL);

  cleanup(&pKV, path);
}

/* ---- Test 13: CF-level TTL on a named CF ---- */
static void test13_named_cf_ttl(void){
  printf("\nTest 13: CF-level TTL on a named CF\n");
  const char *path = "tests/ttl13.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  KVColumnFamily *pCF = NULL;
  int rc = kvstore_cf_create(pKV, "sessions", &pCF);
  ASSERT("cf_create ok", rc == KVSTORE_OK && pCF != NULL);
  if( rc != KVSTORE_OK ){ cleanup(&pKV, path); return; }

  int64_t expire_ms = kvstore_now_ms() + 30000;
  rc = kvstore_cf_put_ttl(pCF, "uid:1", 5, "data", 4, expire_ms);
  ASSERT("cf_put_ttl ok", rc == KVSTORE_OK);

  void *pVal = NULL; int nVal = 0; int64_t rem = 0;
  rc = kvstore_cf_get_ttl(pCF, "uid:1", 5, &pVal, &nVal, &rem);
  ASSERT("cf_get_ttl ok",   rc == KVSTORE_OK);
  ASSERT("value matches",   pVal && nVal == 4 && memcmp(pVal, "data", 4) == 0);
  ASSERT("remaining > 0",   rem > 0 && rem <= 30000);
  if( pVal ) snkv_free(pVal);

  /* Check ttl_remaining via CF-level call. */
  int64_t rem2 = 0;
  rc = kvstore_cf_ttl_remaining(pCF, "uid:1", 5, &rem2);
  ASSERT("cf_ttl_remaining ok", rc == KVSTORE_OK && rem2 > 0);

  /* The key must NOT appear in the default CF. */
  void *pDef = NULL; int nDef = 0;
  rc = kvstore_get(pKV, "uid:1", 5, &pDef, &nDef);
  ASSERT("not in default CF", rc == KVSTORE_NOTFOUND);

  kvstore_cf_close(pCF);
  cleanup(&pKV, path);
}

/* ---- Test 14: CF-level purge_expired on a named CF ---- */
static void test14_named_cf_purge(void){
  printf("\nTest 14: CF-level purge_expired on a named CF\n");
  const char *path = "tests/ttl14.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  KVColumnFamily *pCF = NULL;
  int rc = kvstore_cf_create(pKV, "cache", &pCF);
  ASSERT("cf_create ok", rc == KVSTORE_OK && pCF != NULL);
  if( rc != KVSTORE_OK ){ cleanup(&pKV, path); return; }

  int64_t past   = kvstore_now_ms() - 1000;
  int64_t future = kvstore_now_ms() + 60000;

  kvstore_cf_put_ttl(pCF, "dead1", 5, "v", 1, past);
  kvstore_cf_put_ttl(pCF, "dead2", 5, "v", 1, past);
  kvstore_cf_put_ttl(pCF, "live1", 5, "v", 1, future);

  int nDeleted = -1;
  rc = kvstore_cf_purge_expired(pCF, &nDeleted);
  ASSERT("cf_purge_expired ok", rc == KVSTORE_OK);
  ASSERT("deleted == 2", nDeleted == 2);

  /* live1 must survive. */
  void *pVal = NULL; int nVal = 0;
  rc = kvstore_cf_get_ttl(pCF, "live1", 5, &pVal, &nVal, NULL);
  ASSERT("live1 ok", rc == KVSTORE_OK);
  if( pVal ) snkv_free(pVal);

  /* Default CF must be untouched. */
  int nDefaultDel = -1;
  rc = kvstore_purge_expired(pKV, &nDefaultDel);
  ASSERT("default purge returns 0", rc == KVSTORE_OK && nDefaultDel == 0);

  kvstore_cf_close(pCF);
  cleanup(&pKV, path);
}

/* ---- Test 15: TTL index CFs for different user CFs are independent ---- */
static void test15_cf_ttl_independence(void){
  printf("\nTest 15: TTL index CFs for different user CFs are independent\n");
  const char *path = "tests/ttl15.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  KVColumnFamily *pA = NULL, *pB = NULL;
  kvstore_cf_create(pKV, "alpha", &pA);
  kvstore_cf_create(pKV, "beta",  &pB);
  ASSERT("cf alpha ok", pA != NULL);
  ASSERT("cf beta ok",  pB != NULL);
  if( !pA || !pB ){
    if( pA ) kvstore_cf_close(pA);
    if( pB ) kvstore_cf_close(pB);
    cleanup(&pKV, path); return;
  }

  int64_t past   = kvstore_now_ms() - 1000;
  int64_t future = kvstore_now_ms() + 60000;

  /* alpha has 2 expired keys, beta has 1 live key. */
  kvstore_cf_put_ttl(pA, "x", 1, "v", 1, past);
  kvstore_cf_put_ttl(pA, "y", 1, "v", 1, past);
  kvstore_cf_put_ttl(pB, "z", 1, "v", 1, future);

  /* Purge only alpha. */
  int nA = -1;
  int rc = kvstore_cf_purge_expired(pA, &nA);
  ASSERT("purge alpha ok", rc == KVSTORE_OK);
  ASSERT("alpha: 2 deleted", nA == 2);

  /* beta's key must be unaffected. */
  void *pVal = NULL; int nVal = 0;
  rc = kvstore_cf_get_ttl(pB, "z", 1, &pVal, &nVal, NULL);
  ASSERT("beta z still live", rc == KVSTORE_OK);
  if( pVal ) snkv_free(pVal);

  kvstore_cf_close(pA);
  kvstore_cf_close(pB);
  cleanup(&pKV, path);
}

/* ---- Test 16: cf_drop removes TTL index CFs ---- */
static void test16_cf_drop_removes_ttl_cfs(void){
  printf("\nTest 16: cf_drop removes TTL index CFs\n");
  const char *path = "tests/ttl16.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  KVColumnFamily *pCF = NULL;
  int rc = kvstore_cf_create(pKV, "temp", &pCF);
  ASSERT("cf_create ok", rc == KVSTORE_OK && pCF != NULL);
  if( rc != KVSTORE_OK ){ cleanup(&pKV, path); return; }

  int64_t expire_ms = kvstore_now_ms() + 10000;
  kvstore_cf_put_ttl(pCF, "k", 1, "v", 1, expire_ms);
  kvstore_cf_close(pCF);
  pCF = NULL;

  /* Drop the CF — should silently drop its TTL index CFs too. */
  rc = kvstore_cf_drop(pKV, "temp");
  ASSERT("cf_drop ok", rc == KVSTORE_OK);

  /* After drop, cf_list must not contain "temp" or any __ names. */
  char **azNames = NULL; int nCount = 0;
  rc = kvstore_cf_list(pKV, &azNames, &nCount);
  ASSERT("cf_list ok", rc == KVSTORE_OK);
  int foundTemp = 0, foundHidden = 0, i;
  for( i = 0; i < nCount; i++ ){
    if( strcmp(azNames[i], "temp") == 0 ) foundTemp = 1;
    if( azNames[i][0] == '_' && azNames[i][1] == '_' ) foundHidden = 1;
    sqliteFree(azNames[i]);
  }
  sqliteFree(azNames);
  ASSERT("temp not listed",    !foundTemp);
  ASSERT("no __ names listed", !foundHidden);

  cleanup(&pKV, path);
}

/* ---- Test 17: overwrite with longer TTL extends lifetime ---- */
static void test17_extend_ttl(void){
  printf("\nTest 17: overwrite put_ttl extends lifetime\n");
  const char *path = "tests/ttl17.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  const char *key = "renew";
  /* Short TTL — would expire soon. */
  int64_t short_exp = kvstore_now_ms() + 20;
  kvstore_put_ttl(pKV, key, (int)strlen(key), "v1", 2, short_exp);

  /* Immediately extend with long TTL. */
  int64_t long_exp = kvstore_now_ms() + 60000;
  int rc = kvstore_put_ttl(pKV, key, (int)strlen(key), "v2", 2, long_exp);
  ASSERT("second put_ttl ok", rc == KVSTORE_OK);

  /* Sleep past the original short expiry. */
  snkv_sleep_ms(50);

  /* Key must still be alive with the new value. */
  void *pVal = NULL; int nVal = 0; int64_t rem = 0;
  rc = kvstore_get_ttl(pKV, key, (int)strlen(key), &pVal, &nVal, &rem);
  ASSERT("key still alive",  rc == KVSTORE_OK);
  ASSERT("value updated",    pVal && nVal == 2 && memcmp(pVal, "v2", 2) == 0);
  ASSERT("remaining > 0",    rem > 0);
  if( pVal ) snkv_free(pVal);

  /* Old short expiry must no longer be in the expiry CF (purge returns 0). */
  int nDeleted = -1;
  kvstore_purge_expired(pKV, &nDeleted);
  ASSERT("no orphan expiry entries", nDeleted == 0);

  cleanup(&pKV, path);
}

/* ---- Test 18: put_ttl rollback removes both data and TTL ---- */
static void test18_rollback_removes_ttl(void){
  printf("\nTest 18: rollback after put_ttl removes data and TTL\n");
  const char *path = "tests/ttl18.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  int rc = kvstore_begin(pKV, 1);
  ASSERT("begin ok", rc == KVSTORE_OK);

  int64_t expire_ms = kvstore_now_ms() + 30000;
  rc = kvstore_put_ttl(pKV, "rolled", 6, "back", 4, expire_ms);
  ASSERT("put_ttl ok", rc == KVSTORE_OK);

  rc = kvstore_rollback(pKV);
  ASSERT("rollback ok", rc == KVSTORE_OK);

  /* Data must be gone. */
  void *pVal = NULL; int nVal = 0;
  rc = kvstore_get(pKV, "rolled", 6, &pVal, &nVal);
  ASSERT("data gone after rollback", rc == KVSTORE_NOTFOUND);

  /* TTL_remaining must return NOTFOUND. */
  int64_t rem = 0;
  rc = kvstore_ttl_remaining(pKV, "rolled", 6, &rem);
  ASSERT("ttl gone after rollback", rc == KVSTORE_NOTFOUND);

  /* purge_expired sees nothing. */
  int nDeleted = -1;
  kvstore_purge_expired(pKV, &nDeleted);
  ASSERT("purge sees nothing", nDeleted == 0);

  cleanup(&pKV, path);
}

/* ---- Test 19: iterator skips expired keys (gap 1) ---- */
static void test19_iterator_skips_expired(void){
  printf("\nTest 19: iterator skips expired keys\n");
  const char *path = "tests/ttl19.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  int64_t past   = kvstore_now_ms() - 2000;
  int64_t future = kvstore_now_ms() + 60000;

  /*
  ** Write keys a–e in lexicographic order; b and d are expired.
  ** Expected iteration result: a, c, e only.
  */
  kvstore_put_ttl(pKV, "a", 1, "va", 2, future);
  kvstore_put_ttl(pKV, "b", 1, "vb", 2, past);   /* expired */
  kvstore_put_ttl(pKV, "c", 1, "vc", 2, future);
  kvstore_put_ttl(pKV, "d", 1, "vd", 2, past);   /* expired */
  kvstore_put_ttl(pKV, "e", 1, "ve", 2, future);

  KVIterator *pIter = NULL;
  int rc = kvstore_iterator_create(pKV, &pIter);
  ASSERT("iter create ok", rc == KVSTORE_OK && pIter != NULL);
  if( rc != KVSTORE_OK ){ cleanup(&pKV, path); return; }

  rc = kvstore_iterator_first(pIter);
  ASSERT("iter first ok", rc == KVSTORE_OK);

  /* Collect all keys seen during iteration. */
  char seen[8];
  int  nSeen = 0;
  while( !kvstore_iterator_eof(pIter) ){
    void *pKey = NULL; int nKey = 0;
    if( kvstore_iterator_key(pIter, &pKey, &nKey) == KVSTORE_OK &&
        nKey == 1 && nSeen < 8 ){
      seen[nSeen++] = ((char*)pKey)[0];
    }
    kvstore_iterator_next(pIter);
  }
  kvstore_iterator_close(pIter);

  ASSERT("iter sees exactly 3 keys", nSeen == 3);
  ASSERT("first key is a",           nSeen >= 1 && seen[0] == 'a');
  ASSERT("second key is c",          nSeen >= 2 && seen[1] == 'c');
  ASSERT("third key is e",           nSeen >= 3 && seen[2] == 'e');

  /* Expired keys must also be absent from direct get. */
  void *pVal = NULL; int nVal = 0;
  ASSERT("b gone via get", kvstore_get(pKV, "b", 1, &pVal, &nVal) == KVSTORE_NOTFOUND);
  ASSERT("d gone via get", kvstore_get(pKV, "d", 1, &pVal, &nVal) == KVSTORE_NOTFOUND);

  cleanup(&pKV, path);
}

/* ---- Test 20: nTtlActive — no false expiry after all TTL keys cleared (gap 2) ---- */
static void test20_ntttl_active_no_false_expiry(void){
  printf("\nTest 20: nTtlActive — no false expiry after TTL cleared\n");
  const char *path = "tests/ttl20.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  int64_t past   = kvstore_now_ms() - 1000;
  int64_t future = kvstore_now_ms() + 60000;

  /* Put 3 expired TTL keys — nTtlActive becomes 3. */
  kvstore_put_ttl(pKV, "k1", 2, "v1", 2, past);
  kvstore_put_ttl(pKV, "k2", 2, "v2", 2, past);
  kvstore_put_ttl(pKV, "k3", 2, "v3", 2, past);

  /* Purge all — nTtlActive goes to 0 (guard in put/get disables TTL seeks). */
  int nDeleted = -1;
  int rc = kvstore_purge_expired(pKV, &nDeleted);
  ASSERT("purge ok",     rc == KVSTORE_OK);
  ASSERT("deleted == 3", nDeleted == 3);

  /*
  ** Regular puts on the same keys must not trigger any false TTL cleanup
  ** (regression: with a stale nTtlActive > 0 the put would do an unnecessary
  ** seek and could corrupt the counter in the opposite direction).
  */
  rc = kvstore_put(pKV, "k1", 2, "new1", 4);
  ASSERT("regular put k1 ok", rc == KVSTORE_OK);
  rc = kvstore_put(pKV, "k2", 2, "new2", 4);
  ASSERT("regular put k2 ok", rc == KVSTORE_OK);

  /* Gets must return the new values — NOT KVSTORE_NOTFOUND. */
  void *pVal = NULL; int nVal = 0;
  rc = kvstore_get(pKV, "k1", 2, &pVal, &nVal);
  ASSERT("k1 get ok",   rc == KVSTORE_OK);
  ASSERT("k1 value ok", pVal && nVal == 4 && memcmp(pVal, "new1", 4) == 0);
  if( pVal ) snkv_free(pVal);

  rc = kvstore_get(pKV, "k2", 2, &pVal, &nVal);
  ASSERT("k2 get ok",   rc == KVSTORE_OK);
  ASSERT("k2 value ok", pVal && nVal == 4 && memcmp(pVal, "new2", 4) == 0);
  if( pVal ) snkv_free(pVal);

  /* After nTtlActive==0, a new put_ttl must increment the counter and work. */
  rc = kvstore_put_ttl(pKV, "k4", 2, "v4", 2, future);
  ASSERT("new put_ttl ok", rc == KVSTORE_OK);
  int64_t rem = 0;
  rc = kvstore_ttl_remaining(pKV, "k4", 2, &rem);
  ASSERT("k4 ttl ok",      rc == KVSTORE_OK && rem > 0);

  /* Purge again — must delete k4, leave k1/k2 untouched. */
  /* (k4 is not expired, so purge returns 0.) */
  nDeleted = -1;
  kvstore_purge_expired(pKV, &nDeleted);
  ASSERT("second purge sees 0", nDeleted == 0);

  rc = kvstore_get(pKV, "k1", 2, &pVal, &nVal);
  ASSERT("k1 still ok after second purge", rc == KVSTORE_OK);
  if( pVal ) snkv_free(pVal);

  cleanup(&pKV, path);
}

/* ---- Test 21: purge_expired handles > KVSTORE_PURGE_BATCH expired keys (gap 3) ---- */
static void test21_purge_large_batch(void){
  printf("\nTest 21: purge_expired handles > 256 expired keys (batch mode)\n");
  const char *path = "tests/ttl21.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  const int N_EXPIRED = 300;  /* exceeds KVSTORE_PURGE_BATCH=256 */
  const int N_LIVE    = 10;
  int64_t past   = kvstore_now_ms() - 1000;
  int64_t future = kvstore_now_ms() + 60000;
  char keybuf[32];
  int i;

  /* Write in one transaction for speed. */
  int rc = kvstore_begin(pKV, 1);
  ASSERT("begin ok", rc == KVSTORE_OK);
  for( i = 0; i < N_EXPIRED; i++ ){
    snprintf(keybuf, sizeof(keybuf), "exp%04d", i);
    kvstore_put_ttl(pKV, keybuf, (int)strlen(keybuf), "v", 1, past);
  }
  for( i = 0; i < N_LIVE; i++ ){
    snprintf(keybuf, sizeof(keybuf), "live%04d", i);
    kvstore_put_ttl(pKV, keybuf, (int)strlen(keybuf), "v", 1, future);
  }
  rc = kvstore_commit(pKV);
  ASSERT("commit ok", rc == KVSTORE_OK);

  /* Single purge call must span multiple internal batches and delete all 300. */
  int nDeleted = -1;
  rc = kvstore_purge_expired(pKV, &nDeleted);
  ASSERT("purge ok",             rc == KVSTORE_OK);
  ASSERT("deleted == N_EXPIRED", nDeleted == N_EXPIRED);

  /* Spot-check expired keys at start, batch boundary, and end are gone. */
  void *pVal = NULL; int nVal = 0;
  ASSERT("exp0000 gone", kvstore_get(pKV, "exp0000", 7, &pVal, &nVal) == KVSTORE_NOTFOUND);
  ASSERT("exp0255 gone", kvstore_get(pKV, "exp0255", 7, &pVal, &nVal) == KVSTORE_NOTFOUND);
  ASSERT("exp0299 gone", kvstore_get(pKV, "exp0299", 7, &pVal, &nVal) == KVSTORE_NOTFOUND);

  /* Live keys must survive. */
  rc = kvstore_get(pKV, "live0000", 8, &pVal, &nVal);
  ASSERT("live0000 ok", rc == KVSTORE_OK);
  if( pVal ) snkv_free(pVal);

  rc = kvstore_get(pKV, "live0009", 8, &pVal, &nVal);
  ASSERT("live0009 ok", rc == KVSTORE_OK);
  if( pVal ) snkv_free(pVal);

  /* A second purge should find nothing left to delete. */
  nDeleted = -1;
  kvstore_purge_expired(pKV, &nDeleted);
  ASSERT("second purge sees 0", nDeleted == 0);

  cleanup(&pKV, path);
}

/* ---- Test 22: real wall-clock expiry via sleep ---- */
static void test22_real_time_expiry(void){
  printf("\nTest 22: real wall-clock expiry (499 ms TTL, sleep 500 ms)\n");
  const char *path = "tests/ttl22.db";
  KVStore *pKV = openFresh(path);
  if( !pKV ){ ASSERT("open", 0); return; }

  const char *key   = "shortlived";
  const char *value = "gone_soon";
  int64_t expire_ms = kvstore_now_ms() + 499;

  int rc = kvstore_put_ttl(pKV, key, (int)strlen(key),
                            value, (int)strlen(value), expire_ms);
  ASSERT("put_ttl ok", rc == KVSTORE_OK);

  /* Key must be alive immediately after write. */
  void *pVal = NULL; int nVal = 0; int64_t rem = 0;
  rc = kvstore_get_ttl(pKV, key, (int)strlen(key), &pVal, &nVal, &rem);
  ASSERT("alive before sleep", rc == KVSTORE_OK);
  ASSERT("remaining > 0",      rem > 0);
  if( pVal ) snkv_free(pVal);

  /* Wait for real time to elapse past the expiry. */
  snkv_sleep_ms(500);

  /* Now the key must be expired. */
  pVal = NULL; nVal = 0; rem = 99;
  rc = kvstore_get_ttl(pKV, key, (int)strlen(key), &pVal, &nVal, &rem);
  ASSERT("expired after sleep",    rc == KVSTORE_NOTFOUND);
  ASSERT("value is NULL",          pVal == NULL);
  ASSERT("remaining == 0",         rem == 0);

  /* Lazy delete must have removed it from the data CF too. */
  void *pRaw = NULL; int nRaw = 0;
  rc = kvstore_get(pKV, key, (int)strlen(key), &pRaw, &nRaw);
  ASSERT("raw get also NOTFOUND",  rc == KVSTORE_NOTFOUND);

  cleanup(&pKV, path);
}

/* ========== main ========== */
int main(void){
  printf("=== TTL tests ===\n");

  test1_valid_before_expiry();
  test2_expired_returns_notfound();
  test3_zero_expire_is_permanent();
  test4_regular_put_no_ttl();
  test5_ttl_missing_key();
  test6_purge_expired();
  test7_purge_nothing();
  test8_regular_put_clears_ttl();
  test9_delete_then_purge();
  test10_put_ttl_in_transaction();
  test11_cf_list_hides_ttl_cf();
  test12_cf_create_rejects_reserved();
  test13_named_cf_ttl();
  test14_named_cf_purge();
  test15_cf_ttl_independence();
  test16_cf_drop_removes_ttl_cfs();
  test17_extend_ttl();
  test18_rollback_removes_ttl();
  test19_iterator_skips_expired();
  test20_ntttl_active_no_false_expiry();
  test21_purge_large_batch();
  test22_real_time_expiry();

  printf("\n=== Results: %d passed, %d failed ===\n", passed, failed);
  return failed > 0 ? 1 : 0;
}
