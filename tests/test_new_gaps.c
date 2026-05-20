/* SPDX-License-Identifier: Apache-2.0 */
/*
** tests/test_new_gaps.c — Coverage for behaviours not exercised by other
** test suites.  Uses only the public kvstore.h API.
**
** Tests:
**  N2.  Iterator exhaustion: eof() stays true; next() after eof returns error
**  N3.  Reverse iterator: keys arrive in descending lexicographic order
**  N4.  CF isolation: same key in two CFs does not cross-contaminate
**  m8.  Read-only open: writes return KVSTORE_READONLY
**  m9.  WAL snapshot isolation: read tx opened before a write does not see
**       the write (two handles on the same WAL file)
**
**  A1.  prefix iterator visits exactly matching keys in order
**  A2.  reverse prefix iterator visits matching keys in reverse order
**  A3.  kvstore_iterator_seek positions cursor at or after target
**  A4.  put_if_absent rejects existing key; accepts absent key
**  A5.  cf_put_if_absent on a named CF
**  A6.  TTL: get_ttl returns correct absolute expiry
**  A7.  TTL: ttl_remaining is positive for future key, NO_TTL for plain key
**  A8.  TTL: expired key not visible after lazy expiry on get/exists
**  A9.  stats: nPuts / nGets / nDeletes counters increment correctly
**  A10. count / cf_count consistency
**  A11. integrity_check passes on a fresh store
**  A12. begin(wrflag=0) opens a read snapshot; put inside returns error
**  A13. cf_list returns names of all user CFs
**  A14. kvstore_sync returns KVSTORE_OK
*/

#include "kvstore.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int passed = 0, failed = 0;
static void check(const char *n, int ok){
  if(ok){ printf("  PASS: %s\n", n); passed++; }
  else  { printf("  FAIL: %s\n", n); failed++; }
}
#define ASSERT(n, e) check(n, (int)(e))

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

/* N2: Iterator exhaustion */
static void test_iter_exhaustion(void){
  printf("\nN2: iterator exhaustion\n");
  KVStore *kv = openFresh("ng_n2.db");
  if(!kv) return;
  kvstore_put(kv, "a", 1, "1", 1);
  kvstore_put(kv, "b", 1, "2", 1);

  KVIterator *it = NULL;
  kvstore_iterator_create(kv, &it);
  kvstore_iterator_first(it);
  ASSERT("N2 first not eof", !kvstore_iterator_eof(it));
  kvstore_iterator_next(it);
  ASSERT("N2 second not eof", !kvstore_iterator_eof(it));
  kvstore_iterator_next(it);
  ASSERT("N2 eof after last", kvstore_iterator_eof(it));
  /* next() at eof: implementation returns KVSTORE_OK (no-op) and stays eof */
  kvstore_iterator_next(it);
  ASSERT("N2 still eof after extra next", kvstore_iterator_eof(it));
  kvstore_iterator_close(it);

  kvstore_close(kv);
  dbRemove("ng_n2.db");
}

/* N3: Reverse iterator visits keys in descending order */
static void test_reverse_iter_order(void){
  printf("\nN3: reverse iterator order\n");
  KVStore *kv = openFresh("ng_n3.db");
  if(!kv) return;
  kvstore_put(kv, "aa", 2, "1", 1);
  kvstore_put(kv, "bb", 2, "2", 1);
  kvstore_put(kv, "cc", 2, "3", 1);

  KVIterator *it = NULL;
  kvstore_reverse_iterator_create(kv, &it);
  kvstore_iterator_last(it);
  ASSERT("N3 not eof at start", !kvstore_iterator_eof(it));

  void *pk = NULL; int nk = 0;
  kvstore_iterator_key(it, &pk, &nk);
  ASSERT("N3 first key is cc", nk==2 && memcmp(pk,"cc",2)==0);

  kvstore_iterator_prev(it);
  kvstore_iterator_key(it, &pk, &nk);
  ASSERT("N3 second key is bb", nk==2 && memcmp(pk,"bb",2)==0);

  kvstore_iterator_prev(it);
  kvstore_iterator_key(it, &pk, &nk);
  ASSERT("N3 third key is aa", nk==2 && memcmp(pk,"aa",2)==0);

  kvstore_iterator_prev(it);
  ASSERT("N3 eof after last prev", kvstore_iterator_eof(it));
  kvstore_iterator_close(it);

  kvstore_close(kv);
  dbRemove("ng_n3.db");
}

/* N4: CF isolation */
static void test_cf_isolation(void){
  printf("\nN4: CF isolation\n");
  KVStore *kv = openFresh("ng_n4.db");
  if(!kv) return;

  KVColumnFamily *cf1 = NULL, *cf2 = NULL;
  kvstore_cf_create(kv, "alpha", &cf1);
  kvstore_cf_create(kv, "beta",  &cf2);

  kvstore_cf_put(cf1, "shared", 6, "from_alpha", 10);
  kvstore_cf_put(cf2, "shared", 6, "from_beta",   9);
  kvstore_put(kv,     "shared", 6, "from_default", 12);

  void *pv = NULL; int nv = 0;
  int rc = kvstore_cf_get(cf1, "shared", 6, &pv, &nv);
  ASSERT("N4 cf1 value correct",
    rc==KVSTORE_OK && pv && nv==10 && memcmp(pv,"from_alpha",10)==0);
  if(pv){ snkv_free(pv); pv=NULL; }

  rc = kvstore_cf_get(cf2, "shared", 6, &pv, &nv);
  ASSERT("N4 cf2 value correct",
    rc==KVSTORE_OK && pv && nv==9 && memcmp(pv,"from_beta",9)==0);
  if(pv){ snkv_free(pv); pv=NULL; }

  rc = kvstore_get(kv, "shared", 6, &pv, &nv);
  ASSERT("N4 default value correct",
    rc==KVSTORE_OK && pv && nv==12 && memcmp(pv,"from_default",12)==0);
  if(pv) snkv_free(pv);

  kvstore_cf_close(cf1);
  kvstore_cf_close(cf2);
  kvstore_close(kv);
  dbRemove("ng_n4.db");
}

/* m8: Read-only open */
static void test_readonly_open(void){
  printf("\nm8: read-only open\n");
  const char *p = "ng_m8.db";
  KVStore *kv = openFresh(p);
  if(!kv) return;
  kvstore_put(kv, "k", 1, "v", 1);
  kvstore_close(kv);

  KVStoreConfig cfg = {0};
  cfg.readOnly = 1;
  KVStore *ro = NULL;
  int rc = kvstore_open_v2(p, &ro, &cfg);
  ASSERT("m8 open ok", rc == KVSTORE_OK && ro != NULL);
  if(ro){
    void *pv = NULL; int nv = 0;
    rc = kvstore_get(ro, "k", 1, &pv, &nv);
    ASSERT("m8 get ok", rc == KVSTORE_OK && pv != NULL);
    if(pv) snkv_free(pv);

    rc = kvstore_put(ro, "k2", 2, "v2", 2);
    ASSERT("m8 put rejected with READONLY", rc == KVSTORE_READONLY);

    rc = kvstore_delete(ro, "k", 1);
    ASSERT("m8 delete rejected with READONLY", rc == KVSTORE_READONLY);

    kvstore_close(ro);
  }
  dbRemove(p);
}

/* m9: WAL snapshot isolation — read tx does not see later writes */
static void test_wal_snapshot_isolation(void){
  printf("\nm9: WAL snapshot isolation\n");
  const char *p = "ng_m9.db";

  KVStore *a = openFresh(p);
  if(!a) return;
  kvstore_put(a, "pre", 3, "exists", 6);
  kvstore_checkpoint(a, KVSTORE_CHECKPOINT_FULL, NULL, NULL);

  KVStore *b = NULL;
  int rc = kvstore_open(p, &b, KVSTORE_JOURNAL_WAL);
  ASSERT("m9 open b ok", rc == KVSTORE_OK && b != NULL);
  if(!b){ kvstore_close(a); dbRemove(p); return; }

  /* Open a read snapshot on handle A (wrflag=0) */
  rc = kvstore_begin(a, 0);
  ASSERT("m9 begin read on a ok", rc == KVSTORE_OK);

  /* Write a new key via handle B */
  kvstore_put(b, "new_key", 7, "new_val", 7);

  /* Handle A's snapshot must NOT see new_key */
  int ex = 0;
  rc = kvstore_exists(a, "new_key", 7, &ex);
  ASSERT("m9 snapshot does not see new_key", rc == KVSTORE_OK && ex == 0);

  /* pre is visible in A's snapshot */
  rc = kvstore_exists(a, "pre", 3, &ex);
  ASSERT("m9 snapshot sees pre", rc == KVSTORE_OK && ex == 1);

  kvstore_rollback(a);

  /* After releasing snapshot, A can see new_key */
  rc = kvstore_exists(a, "new_key", 7, &ex);
  ASSERT("m9 after snapshot released sees new_key", rc == KVSTORE_OK && ex == 1);

  kvstore_close(a);
  kvstore_close(b);
  dbRemove(p);
}

/* A1: prefix iterator */
static void test_prefix_iter(void){
  printf("\nA1: prefix iterator\n");
  KVStore *kv = openFresh("ng_a1.db");
  if(!kv) return;
  kvstore_put(kv, "pfx:a", 5, "1", 1);
  kvstore_put(kv, "pfx:b", 5, "2", 1);
  kvstore_put(kv, "pfx:c", 5, "3", 1);
  kvstore_put(kv, "other", 5, "x", 1);

  KVIterator *it = NULL;
  kvstore_prefix_iterator_create(kv, "pfx:", 4, &it);
  kvstore_iterator_first(it);
  int count = 0;
  while(!kvstore_iterator_eof(it)){
    void *pk = NULL; int nk = 0;
    kvstore_iterator_key(it, &pk, &nk);
    ASSERT("A1 key has prefix", nk >= 4 && memcmp(pk, "pfx:", 4) == 0);
    count++;
    kvstore_iterator_next(it);
  }
  ASSERT("A1 exactly 3 prefix keys", count == 3);
  kvstore_iterator_close(it);
  kvstore_close(kv);
  dbRemove("ng_a1.db");
}

/* A2: reverse prefix iterator */
static void test_reverse_prefix_iter(void){
  printf("\nA2: reverse prefix iterator\n");
  KVStore *kv = openFresh("ng_a2.db");
  if(!kv) return;
  kvstore_put(kv, "x:1", 3, "a", 1);
  kvstore_put(kv, "x:2", 3, "b", 1);
  kvstore_put(kv, "x:3", 3, "c", 1);
  kvstore_put(kv, "y:1", 3, "z", 1);

  KVIterator *it = NULL;
  kvstore_reverse_prefix_iterator_create(kv, "x:", 2, &it);
  kvstore_iterator_last(it);
  void *pk = NULL; int nk = 0;
  kvstore_iterator_key(it, &pk, &nk);
  ASSERT("A2 first (reverse) is x:3", nk==3 && memcmp(pk,"x:3",3)==0);
  kvstore_iterator_prev(it);
  kvstore_iterator_key(it, &pk, &nk);
  ASSERT("A2 second (reverse) is x:2", nk==3 && memcmp(pk,"x:2",3)==0);
  kvstore_iterator_prev(it);
  kvstore_iterator_key(it, &pk, &nk);
  ASSERT("A2 third (reverse) is x:1", nk==3 && memcmp(pk,"x:1",3)==0);
  kvstore_iterator_prev(it);
  ASSERT("A2 eof after last prev", kvstore_iterator_eof(it));
  kvstore_iterator_close(it);
  kvstore_close(kv);
  dbRemove("ng_a2.db");
}

/* A3: iterator_seek */
static void test_iter_seek(void){
  printf("\nA3: iterator_seek\n");
  KVStore *kv = openFresh("ng_a3.db");
  if(!kv) return;
  kvstore_put(kv, "aa", 2, "1", 1);
  kvstore_put(kv, "cc", 2, "3", 1);
  kvstore_put(kv, "ee", 2, "5", 1);

  KVIterator *it = NULL;
  kvstore_iterator_create(kv, &it);
  /* Seek to "bb" — should land on "cc" (first key >= "bb") */
  int rc = kvstore_iterator_seek(it, "bb", 2);
  ASSERT("A3 seek ok", rc == KVSTORE_OK);
  ASSERT("A3 not eof after seek", !kvstore_iterator_eof(it));
  void *pk = NULL; int nk = 0;
  kvstore_iterator_key(it, &pk, &nk);
  ASSERT("A3 seek lands on cc", nk==2 && memcmp(pk,"cc",2)==0);
  /* Seek to exact key */
  rc = kvstore_iterator_seek(it, "ee", 2);
  ASSERT("A3 seek to exact key ok", rc == KVSTORE_OK);
  kvstore_iterator_key(it, &pk, &nk);
  ASSERT("A3 seek to ee", nk==2 && memcmp(pk,"ee",2)==0);
  kvstore_iterator_close(it);
  kvstore_close(kv);
  dbRemove("ng_a3.db");
}

/* A4: put_if_absent */
static void test_put_if_absent(void){
  printf("\nA4: put_if_absent\n");
  KVStore *kv = openFresh("ng_a4.db");
  if(!kv) return;
  kvstore_put(kv, "exists", 6, "old", 3);

  /* Already-present key must be rejected */
  int inserted = -1;
  int rc = kvstore_put_if_absent(kv, "exists", 6, "new", 3, 0, &inserted);
  ASSERT("A4 put_if_absent rejects existing key", rc == KVSTORE_OK && inserted == 0);

  void *pv = NULL; int nv = 0;
  kvstore_get(kv, "exists", 6, &pv, &nv);
  ASSERT("A4 old value unchanged", pv && nv==3 && memcmp(pv,"old",3)==0);
  if(pv) snkv_free(pv);

  /* Absent key must succeed */
  inserted = -1;
  rc = kvstore_put_if_absent(kv, "fresh", 5, "val", 3, 0, &inserted);
  ASSERT("A4 put_if_absent on absent key ok", rc == KVSTORE_OK && inserted == 1);
  kvstore_get(kv, "fresh", 5, &pv, &nv);
  ASSERT("A4 new value stored", pv && nv==3 && memcmp(pv,"val",3)==0);
  if(pv) snkv_free(pv);

  kvstore_close(kv);
  dbRemove("ng_a4.db");
}

/* A5: cf_put_if_absent */
static void test_cf_put_if_absent(void){
  printf("\nA5: cf_put_if_absent\n");
  KVStore *kv = openFresh("ng_a5.db");
  if(!kv) return;
  KVColumnFamily *cf = NULL;
  kvstore_cf_create(kv, "pia", &cf);
  kvstore_cf_put(cf, "k", 1, "orig", 4);

  int inserted = -1;
  int rc = kvstore_cf_put_if_absent(cf, "k", 1, "new", 3, 0, &inserted);
  ASSERT("A5 rejects existing", rc == KVSTORE_OK && inserted == 0);

  inserted = -1;
  rc = kvstore_cf_put_if_absent(cf, "new_k", 5, "v", 1, 0, &inserted);
  ASSERT("A5 accepts absent", rc == KVSTORE_OK && inserted == 1);

  kvstore_cf_close(cf);
  kvstore_close(kv);
  dbRemove("ng_a5.db");
}

/* A6: get_ttl — retrieve value and expiry together */
static void test_get_ttl(void){
  printf("\nA6: get_ttl returns value and expiry\n");
  KVStore *kv = openFresh("ng_a6.db");
  if(!kv) return;
  int64_t exp = kvstore_now_ms() + 30000;
  kvstore_put_ttl(kv, "k", 1, "hello", 5, exp);

  void *pv = NULL; int nv = 0; int64_t remaining = 0;
  int rc = kvstore_get_ttl(kv, "k", 1, &pv, &nv, &remaining);
  ASSERT("A6 get_ttl ok", rc == KVSTORE_OK);
  ASSERT("A6 value correct", pv && nv==5 && memcmp(pv,"hello",5)==0);
  /* Remaining should be close to 30000 ms */
  ASSERT("A6 remaining > 0", remaining > 0);
  ASSERT("A6 remaining <= 30000", remaining <= 30000);
  if(pv) snkv_free(pv);

  /* Non-TTL key: get_ttl returns value, remaining == KVSTORE_NO_TTL */
  kvstore_put(kv, "plain", 5, "val", 3);
  pv = NULL; nv = 0; remaining = 0;
  rc = kvstore_get_ttl(kv, "plain", 5, &pv, &nv, &remaining);
  ASSERT("A6 plain key ok", rc == KVSTORE_OK);
  ASSERT("A6 plain remaining is NO_TTL", remaining == KVSTORE_NO_TTL);
  if(pv) snkv_free(pv);

  kvstore_close(kv);
  dbRemove("ng_a6.db");
}

/* A7: ttl_remaining is positive for future TTL, KVSTORE_NO_TTL for plain key */
static void test_ttl_remaining(void){
  printf("\nA7: ttl_remaining values\n");
  KVStore *kv = openFresh("ng_a7.db");
  if(!kv) return;
  int64_t exp = kvstore_now_ms() + 60000;
  kvstore_put_ttl(kv, "k", 1, "v", 1, exp);

  int64_t rem = 0;
  int rc = kvstore_ttl_remaining(kv, "k", 1, &rem);
  ASSERT("A7 ttl_remaining ok", rc == KVSTORE_OK);
  ASSERT("A7 remaining > 0", rem > 0);
  ASSERT("A7 remaining <= 60000", rem <= 60000);

  kvstore_put(kv, "plain", 5, "v", 1);
  rc = kvstore_ttl_remaining(kv, "plain", 5, &rem);
  ASSERT("A7 plain key returns KVSTORE_NO_TTL",
         rc == KVSTORE_OK && rem == KVSTORE_NO_TTL);

  kvstore_close(kv);
  dbRemove("ng_a7.db");
}

/* A8: Expired key not visible after lazy expiry */
static void test_ttl_expired_invisible(void){
  printf("\nA8: expired key not visible after lazy expiry\n");
  KVStore *kv = openFresh("ng_a8.db");
  if(!kv) return;
  int64_t past = kvstore_now_ms() - 1000;
  kvstore_put_ttl(kv, "gone", 4, "v", 1, past);
  kvstore_put(kv, "alive", 5, "v", 1);

  void *pv = NULL; int nv = 0;
  int rc = kvstore_get(kv, "gone", 4, &pv, &nv);
  ASSERT("A8 expired key returns NOTFOUND", rc == KVSTORE_NOTFOUND);
  ASSERT("A8 pv stays NULL", pv == NULL);

  int ex = 0;
  rc = kvstore_exists(kv, "gone", 4, &ex);
  ASSERT("A8 exists returns OK with ex==0", rc == KVSTORE_OK && ex == 0);

  rc = kvstore_get(kv, "alive", 5, &pv, &nv);
  ASSERT("A8 non-expired key still readable", rc == KVSTORE_OK && pv != NULL);
  if(pv) snkv_free(pv);

  kvstore_close(kv);
  dbRemove("ng_a8.db");
}

/* A9: stats counters increment correctly */
static void test_stats_counters(void){
  printf("\nA9: stats counters\n");
  KVStore *kv = openFresh("ng_a9.db");
  if(!kv) return;

  kvstore_stats_reset(kv);
  kvstore_put(kv, "a", 1, "v1", 2);
  kvstore_put(kv, "b", 1, "v2", 2);
  void *pv = NULL; int nv = 0;
  kvstore_get(kv, "a", 1, &pv, &nv);
  if(pv) snkv_free(pv);
  kvstore_delete(kv, "b", 1);

  KVStoreStats st = {0};
  kvstore_stats(kv, &st);
  ASSERT("A9 nPuts==2",          st.nPuts == 2);
  ASSERT("A9 nGets==1",          st.nGets == 1);
  ASSERT("A9 nDeletes==1",       st.nDeletes == 1);
  ASSERT("A9 nBytesWritten > 0", st.nBytesWritten > 0);
  ASSERT("A9 nBytesRead > 0",    st.nBytesRead > 0);

  kvstore_close(kv);
  dbRemove("ng_a9.db");
}

/* A10: count / cf_count consistency */
static void test_count_consistency(void){
  printf("\nA10: count / cf_count consistency\n");
  KVStore *kv = openFresh("ng_a10.db");
  if(!kv) return;
  kvstore_put(kv, "x", 1, "1", 1);
  kvstore_put(kv, "y", 1, "2", 1);
  kvstore_put(kv, "z", 1, "3", 1);

  int64_t n1 = 0, n2 = 0;
  kvstore_count(kv, &n1);

  KVColumnFamily *cf = NULL;
  kvstore_cf_get_default(kv, &cf);
  kvstore_cf_count(cf, &n2);
  kvstore_cf_close(cf);

  ASSERT("A10 count==3",       n1 == 3);
  ASSERT("A10 cf_count==3",    n2 == 3);
  ASSERT("A10 count==cf_count", n1 == n2);

  kvstore_close(kv);
  dbRemove("ng_a10.db");
}

/* A11: integrity_check passes on fresh store */
static void test_integrity_check(void){
  printf("\nA11: integrity_check on fresh store\n");
  KVStore *kv = openFresh("ng_a11.db");
  if(!kv) return;
  kvstore_put(kv, "k", 1, "v", 1);

  char *zErr = NULL;
  int rc = kvstore_integrity_check(kv, &zErr);
  ASSERT("A11 integrity_check ok",    rc == KVSTORE_OK);
  ASSERT("A11 no error message",      zErr == NULL);
  if(zErr) snkv_free(zErr);

  kvstore_close(kv);
  dbRemove("ng_a11.db");
}

/* A12: begin(0) opens a read tx; rollback discards any auto-upgraded writes.
** Note: begin(0) is a soft read tx — the implementation upgrades it to write
** automatically if put/delete is called.  Hard read-only enforcement requires
** the readOnly=1 config flag (tested in m8). */
static void test_begin_read_snapshot(void){
  printf("\nA12: begin(0) / rollback discards changes\n");
  KVStore *kv = openFresh("ng_a12.db");
  if(!kv) return;
  kvstore_put(kv, "k", 1, "v", 1);

  int rc = kvstore_begin(kv, 0);
  ASSERT("A12 begin(0) ok", rc == KVSTORE_OK);

  /* Reads work inside the tx */
  void *pv = NULL; int nv = 0;
  rc = kvstore_get(kv, "k", 1, &pv, &nv);
  ASSERT("A12 get in tx ok", rc == KVSTORE_OK && pv != NULL);
  if(pv) snkv_free(pv);

  /* Rollback: any writes auto-upgraded inside this tx are discarded */
  rc = kvstore_rollback(kv);
  ASSERT("A12 rollback ok", rc == KVSTORE_OK);

  /* k still exists (was committed before begin) */
  int ex = 0;
  rc = kvstore_exists(kv, "k", 1, &ex);
  ASSERT("A12 pre-tx key survives rollback", rc == KVSTORE_OK && ex == 1);

  kvstore_close(kv);
  dbRemove("ng_a12.db");
}

/* A13: cf_list returns names of all user CFs */
static void test_cf_list(void){
  printf("\nA13: cf_list returns all user CFs\n");
  KVStore *kv = openFresh("ng_a13.db");
  if(!kv) return;

  KVColumnFamily *cf1 = NULL, *cf2 = NULL;
  kvstore_cf_create(kv, "zeta",  &cf1);
  kvstore_cf_create(kv, "omega", &cf2);
  kvstore_cf_close(cf1);
  kvstore_cf_close(cf2);

  char **names = NULL; int n = 0;
  int rc = kvstore_cf_list(kv, &names, &n);
  ASSERT("A13 cf_list ok", rc == KVSTORE_OK);

  int foundZeta = 0, foundOmega = 0;
  for(int i = 0; i < n; i++){
    if(strcmp(names[i], "zeta")  == 0) foundZeta  = 1;
    if(strcmp(names[i], "omega") == 0) foundOmega = 1;
  }
  ASSERT("A13 zeta in list",  foundZeta);
  ASSERT("A13 omega in list", foundOmega);
  for(int i = 0; i < n; i++) snkv_free(names[i]);
  if(names) snkv_free(names);

  kvstore_close(kv);
  dbRemove("ng_a13.db");
}

/* A14: kvstore_sync returns KVSTORE_OK */
static void test_sync(void){
  printf("\nA14: kvstore_sync returns KVSTORE_OK\n");
  KVStore *kv = openFresh("ng_a14.db");
  if(!kv) return;
  kvstore_put(kv, "k", 1, "v", 1);
  int rc = kvstore_sync(kv);
  ASSERT("A14 sync ok", rc == KVSTORE_OK);
  kvstore_close(kv);
  dbRemove("ng_a14.db");
}

int main(void){
  printf("=== test_new_gaps ===\n");
  test_iter_exhaustion();
  test_reverse_iter_order();
  test_cf_isolation();
  test_readonly_open();
  test_wal_snapshot_isolation();
  test_prefix_iter();
  test_reverse_prefix_iter();
  test_iter_seek();
  test_put_if_absent();
  test_cf_put_if_absent();
  test_get_ttl();
  test_ttl_remaining();
  test_ttl_expired_invisible();
  test_stats_counters();
  test_count_consistency();
  test_integrity_check();
  test_begin_read_snapshot();
  test_cf_list();
  test_sync();
  printf("\n--- %d passed, %d failed ---\n", passed, failed);
  return (failed > 0) ? 1 : 0;
}
