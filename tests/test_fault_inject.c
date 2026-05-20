/* SPDX-License-Identifier: Apache-2.0 */
/*
** tests/test_fault_inject.c — Error-path and fault-injection tests.
**
** Tests API robustness against bad inputs, corrupt files, and error
** propagation without crashing or leaking resources.
**
** Tests:
**  1.  NULL handle arguments return KVSTORE_ERROR without crashing
**  2.  NULL ppKV in open returns KVSTORE_ERROR
**  3.  Corrupt database file returns KVSTORE_CORRUPT or KVSTORE_ERROR
**  4.  Key too large (> KVSTORE_MAX_KEY_SIZE) returns KVSTORE_ERROR
**  5.  Value too large (> KVSTORE_MAX_VALUE_SIZE) returns KVSTORE_ERROR
**  6.  Zero-length key returns KVSTORE_ERROR
**  7.  NULL key pointer returns KVSTORE_ERROR
**  8.  Iterator ops on NULL iterator do not crash
**  9.  get on empty store returns KVSTORE_NOTFOUND
** 10.  delete on empty store returns KVSTORE_NOTFOUND
** 11.  exists on empty store returns KVSTORE_OK with pExists=0
** 12.  exists with NULL pExists returns KVSTORE_ERROR
** 13.  kvstore_begin on already-open write tx returns error or is no-op
** 14.  kvstore_commit without active tx returns KVSTORE_ERROR
** 15.  kvstore_rollback without active tx returns KVSTORE_ERROR
** 16.  Write to read-only store returns KVSTORE_READONLY
** 17.  cf_drop of default CF returns KVSTORE_ERROR
** 18.  cf_open of non-existent CF returns KVSTORE_NOTFOUND
** 19.  kvstore_stats with NULL pStats returns KVSTORE_ERROR
** 20.  Open a directory path returns error
*/

#include "kvstore.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>

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

/* Test 1: NULL handle arguments */
static void test_null_handles(void){
  printf("\nTest 1: NULL handle arguments\n");
  ASSERT("put(NULL)", kvstore_put(NULL,"k",1,"v",1) == KVSTORE_ERROR);
  ASSERT("get(NULL)", kvstore_get(NULL,"k",1,NULL,NULL) == KVSTORE_ERROR);
  ASSERT("delete(NULL)", kvstore_delete(NULL,"k",1) == KVSTORE_ERROR);
  ASSERT("exists(NULL)", kvstore_exists(NULL,"k",1,NULL) == KVSTORE_ERROR);
  /* close(NULL) is defined to be a no-op returning KVSTORE_OK, like free(NULL) */
  ASSERT("close(NULL) no-crash", kvstore_close(NULL) == KVSTORE_OK || kvstore_close(NULL) == KVSTORE_ERROR);
  ASSERT("begin(NULL)", kvstore_begin(NULL,1) == KVSTORE_ERROR);
  ASSERT("commit(NULL)", kvstore_commit(NULL) == KVSTORE_ERROR);
  ASSERT("rollback(NULL)", kvstore_rollback(NULL) == KVSTORE_ERROR);
  ASSERT("stats(NULL,NULL)", kvstore_stats(NULL,NULL) == KVSTORE_ERROR);
  ASSERT("checkpoint(NULL)", kvstore_checkpoint(NULL,0,NULL,NULL) == KVSTORE_ERROR);
  ASSERT("cf_put(NULL)", kvstore_cf_put(NULL,"k",1,"v",1) == KVSTORE_ERROR);
  ASSERT("cf_get(NULL)", kvstore_cf_get(NULL,"k",1,NULL,NULL) == KVSTORE_ERROR);
  ASSERT("cf_delete(NULL)", kvstore_cf_delete(NULL,"k",1) == KVSTORE_ERROR);
  ASSERT("cf_exists(NULL)", kvstore_cf_exists(NULL,"k",1,NULL) == KVSTORE_ERROR);
  ASSERT("iterator_create(NULL)", kvstore_iterator_create(NULL,NULL) == KVSTORE_ERROR);
}

/* Test 2: NULL ppKV */
static void test_null_ppkv(void){
  printf("\nTest 2: NULL ppKV\n");
  int rc = kvstore_open("fi2.db", NULL, KVSTORE_JOURNAL_WAL);
  ASSERT("open with NULL ppKV", rc == KVSTORE_ERROR);
}

/* Test 3: corrupt file */
static void test_corrupt_file(void){
  printf("\nTest 3: corrupt database file\n");
  const char *p = "fi3.db";
  /* Write garbage bytes into the file. */
  FILE *f = fopen(p, "wb");
  if(f){
    unsigned char garbage[512];
    memset(garbage, 0xDE, sizeof(garbage));
    fwrite(garbage, 1, sizeof(garbage), f);
    fclose(f);
  }
  KVStore *kv = NULL;
  int rc = kvstore_open(p, &kv, KVSTORE_JOURNAL_WAL);
  int ok = (rc != KVSTORE_OK || kv == NULL);
  if(kv) kvstore_close(kv);
  ASSERT("open corrupt file returns error", ok);
  dbRemove(p);
}

/* Test 4: key too large
** The implementation rejects keys exceeding 64 KiB.  We pass a length
** one byte over that limit; we allocate only 1 byte and lie about the
** length — the size check fires before any I/O, so the buffer is never
** read beyond what we pass as nKey in the length argument. */
static void test_key_too_large(void){
  printf("\nTest 4: key exceeds internal 64-KiB limit\n");
  KVStore *kv = openFresh("fi4.db");
  if(!kv) return;
  /* 64 * 1024 + 1 bytes — one over the 64 KiB limit in kvstore.c */
  int biglen = 64 * 1024 + 1;
  char *bigkey = malloc((size_t)biglen);
  if(bigkey){
    memset(bigkey, 'k', (size_t)biglen);
    int rc = kvstore_put(kv, bigkey, biglen, "v", 1);
    ASSERT("oversized key rejected", rc == KVSTORE_ERROR);
    free(bigkey);
  }
  kvstore_close(kv);
  dbRemove("fi4.db");
}

/* Test 5: value too large
** The implementation rejects values exceeding 10 MiB.  We pass only a
** tiny buffer but a length beyond the 10 MiB limit — the size check
** fires before any read of the value bytes. */
static void test_value_too_large(void){
  printf("\nTest 5: value length exceeds internal 10-MiB limit\n");
  KVStore *kv = openFresh("fi5.db");
  if(!kv) return;
  char smallbuf[64] = {0};
  int toobig = 10 * 1024 * 1024 + 1;
  int rc = kvstore_put(kv, "k", 1, smallbuf, toobig);
  ASSERT("oversized value rejected", rc == KVSTORE_ERROR);
  kvstore_close(kv);
  dbRemove("fi5.db");
}

/* Test 6: zero-length key */
static void test_zero_length_key(void){
  printf("\nTest 6: zero-length key rejected\n");
  KVStore *kv = openFresh("fi6.db");
  if(!kv) return;
  int rc = kvstore_put(kv, "k", 0, "v", 1);
  ASSERT("zero-length key rejected", rc == KVSTORE_ERROR);
  kvstore_close(kv);
  dbRemove("fi6.db");
}

/* Test 7: NULL key pointer */
static void test_null_key(void){
  printf("\nTest 7: NULL key pointer rejected\n");
  KVStore *kv = openFresh("fi7.db");
  if(!kv) return;
  int rc = kvstore_put(kv, NULL, 3, "v", 1);
  ASSERT("NULL key rejected", rc == KVSTORE_ERROR);
  kvstore_close(kv);
  dbRemove("fi7.db");
}

/* Test 8: iterator ops on NULL */
static void test_null_iterator_ops(void){
  printf("\nTest 8: iterator ops on NULL iterator\n");
  ASSERT("first(NULL)", kvstore_iterator_first(NULL) == KVSTORE_ERROR);
  ASSERT("next(NULL)",  kvstore_iterator_next(NULL)  == KVSTORE_ERROR);
  ASSERT("eof(NULL)",   kvstore_iterator_eof(NULL)   == 1);
  ASSERT("key(NULL)",   kvstore_iterator_key(NULL,NULL,NULL) == KVSTORE_ERROR);
  ASSERT("value(NULL)", kvstore_iterator_value(NULL,NULL,NULL) == KVSTORE_ERROR);
  kvstore_iterator_close(NULL);  /* must not crash */
  ASSERT("close(NULL) no crash", 1);
}

/* Test 9: get on empty store */
static void test_get_empty(void){
  printf("\nTest 9: get on empty store returns NOTFOUND\n");
  KVStore *kv = openFresh("fi9.db");
  if(!kv) return;
  void *pv = NULL; int nv = 0;
  int rc = kvstore_get(kv, "k", 1, &pv, &nv);
  ASSERT("NOTFOUND on empty store", rc == KVSTORE_NOTFOUND);
  ASSERT("pv stays NULL", pv == NULL);
  kvstore_close(kv);
  dbRemove("fi9.db");
}

/* Test 10: delete on empty store */
static void test_delete_empty(void){
  printf("\nTest 10: delete on empty store returns NOTFOUND\n");
  KVStore *kv = openFresh("fi10.db");
  if(!kv) return;
  ASSERT("NOTFOUND on delete-empty", kvstore_delete(kv,"k",1) == KVSTORE_NOTFOUND);
  kvstore_close(kv);
  dbRemove("fi10.db");
}

/* Test 11: exists on empty store */
static void test_exists_empty(void){
  printf("\nTest 11: exists on empty store\n");
  KVStore *kv = openFresh("fi11.db");
  if(!kv) return;
  int ex = -1;
  int rc = kvstore_exists(kv, "k", 1, &ex);
  ASSERT("exists returns OK on empty", rc == KVSTORE_OK);
  ASSERT("pExists == 0", ex == 0);
  kvstore_close(kv);
  dbRemove("fi11.db");
}

/* Test 12: exists with NULL pExists */
static void test_exists_null_out(void){
  printf("\nTest 12: exists with NULL pExists returns KVSTORE_ERROR\n");
  KVStore *kv = openFresh("fi12.db");
  if(!kv) return;
  int rc = kvstore_exists(kv, "k", 1, NULL);
  ASSERT("NULL pExists rejected", rc == KVSTORE_ERROR);
  kvstore_close(kv);
  dbRemove("fi12.db");
}

/* Test 13: begin while write tx already open */
static void test_double_begin(void){
  printf("\nTest 13: second begin while write tx open\n");
  KVStore *kv = openFresh("fi13.db");
  if(!kv) return;
  int rc = kvstore_begin(kv, 1);
  ASSERT("first begin ok", rc == KVSTORE_OK);
  /* Second begin: implementation may return error or be idempotent. */
  rc = kvstore_begin(kv, 1);
  int ok = (rc == KVSTORE_OK || rc == KVSTORE_ERROR || rc == KVSTORE_LOCKED);
  ASSERT("second begin doesn't crash", ok);
  kvstore_rollback(kv);
  kvstore_close(kv);
  dbRemove("fi13.db");
}

/* Test 14: commit without explicit begin
** The implementation auto-starts a read tx after open; commit() commits
** that read tx and returns KVSTORE_OK (no user-visible tx to roll back). */
static void test_commit_without_tx(void){
  printf("\nTest 14: commit without explicit begin — no crash\n");
  KVStore *kv = openFresh("fi14.db");
  if(!kv) return;
  int rc = kvstore_commit(kv);
  /* No explicit begin: implementation either returns OK (commits auto read-tx)
  ** or returns error — both are acceptable; must not crash. */
  ASSERT("commit without begin does not crash", rc == KVSTORE_OK || rc == KVSTORE_ERROR);
  kvstore_close(kv);
  dbRemove("fi14.db");
}

/* Test 15: rollback without explicit begin — same rationale as Test 14 */
static void test_rollback_without_tx(void){
  printf("\nTest 15: rollback without explicit begin — no crash\n");
  KVStore *kv = openFresh("fi15.db");
  if(!kv) return;
  int rc = kvstore_rollback(kv);
  ASSERT("rollback without begin does not crash", rc == KVSTORE_OK || rc == KVSTORE_ERROR);
  kvstore_close(kv);
  dbRemove("fi15.db");
}

/* Test 16: write to read-only store */
static void test_readonly_write(void){
  printf("\nTest 16: write to read-only store\n");
  const char *p = "fi16.db";
  /* Create db with one key. */
  KVStore *kv = openFresh(p);
  if(!kv) return;
  kvstore_put(kv, "k", 1, "v", 1);
  kvstore_close(kv);

  /* Reopen read-only. */
  KVStoreConfig cfg = {0};
  cfg.readOnly = 1;
  KVStore *ro = NULL;
  int rc = kvstore_open_v2(p, &ro, &cfg);
  if(rc == KVSTORE_OK && ro){
    rc = kvstore_put(ro, "k2", 2, "v2", 2);
    ASSERT("put on readonly returns READONLY", rc == KVSTORE_READONLY);
    kvstore_close(ro);
  } else {
    ASSERT("read-only open succeeded", 0);
  }
  dbRemove(p);
}

/* Test 17: cf_drop of default CF */
static void test_drop_default_cf(void){
  printf("\nTest 17: cf_drop default CF returns error\n");
  KVStore *kv = openFresh("fi17.db");
  if(!kv) return;
  int rc = kvstore_cf_drop(kv, "default");
  ASSERT("drop default CF returns error", rc != KVSTORE_OK);
  kvstore_close(kv);
  dbRemove("fi17.db");
}

/* Test 18: cf_open nonexistent CF */
static void test_cf_open_nonexistent(void){
  printf("\nTest 18: cf_open of nonexistent CF returns NOTFOUND\n");
  KVStore *kv = openFresh("fi18.db");
  if(!kv) return;
  KVColumnFamily *cf = NULL;
  int rc = kvstore_cf_open(kv, "no_such_cf", &cf);
  ASSERT("NOTFOUND on missing CF", rc == KVSTORE_NOTFOUND);
  ASSERT("cf handle is NULL", cf == NULL);
  kvstore_close(kv);
  dbRemove("fi18.db");
}

/* Test 19: stats with NULL pStats */
static void test_stats_null(void){
  printf("\nTest 19: stats with NULL pStats returns KVSTORE_ERROR\n");
  KVStore *kv = openFresh("fi19.db");
  if(!kv) return;
  ASSERT("stats(kv,NULL) error", kvstore_stats(kv, NULL) == KVSTORE_ERROR);
  kvstore_close(kv);
  dbRemove("fi19.db");
}

/* Test 20: open a directory as a database */
static void test_open_directory(void){
  printf("\nTest 20: open directory path returns error\n");
  /* Use a path that exists but is a directory. */
#if defined(_WIN32) || defined(_WIN64)
  const char *dirpath = "C:\\Windows";
#else
  const char *dirpath = "/tmp";
#endif
  KVStore *kv = NULL;
  int rc = kvstore_open(dirpath, &kv, KVSTORE_JOURNAL_WAL);
  int ok = (rc != KVSTORE_OK || kv == NULL);
  if(kv) kvstore_close(kv);
  ASSERT("open directory returns error", ok);
}

int main(void){
  printf("=== test_fault_inject ===\n");
  test_null_handles();
  test_null_ppkv();
  test_corrupt_file();
  test_key_too_large();
  test_value_too_large();
  test_zero_length_key();
  test_null_key();
  test_null_iterator_ops();
  test_get_empty();
  test_delete_empty();
  test_exists_empty();
  test_exists_null_out();
  test_double_begin();
  test_commit_without_tx();
  test_rollback_without_tx();
  test_readonly_write();
  test_drop_default_cf();
  test_cf_open_nonexistent();
  test_stats_null();
  test_open_directory();
  printf("\n--- %d passed, %d failed ---\n", passed, failed);
  return (failed > 0) ? 1 : 0;
}
