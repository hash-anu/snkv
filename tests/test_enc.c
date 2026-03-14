/* SPDX-License-Identifier: Apache-2.0 */
/*
** tests/test_enc.c — Encryption regression suite
**
** Tests:
**  1.  kvstore_open_encrypted creates encrypted store; is_encrypted() == 1
**  2.  Re-open encrypted store with correct password succeeds
**  3.  Re-open encrypted store with wrong password → KVSTORE_AUTH_FAILED
**  4.  Values are NOT plaintext in file (basic ciphertext check)
**  5.  put + get round-trip preserves value on encrypted store
**  6.  get on missing key returns NOTFOUND on encrypted store
**  7.  delete works on encrypted store
**  8.  Iterator returns correct plaintext values
**  9.  Prefix iterator works on encrypted store
** 10.  Seek iterator works on encrypted store
** 11.  Reverse iterator works on encrypted store
** 12.  TTL + encryption: put_ttl / get_ttl round-trip
** 13.  TTL expiry works on encrypted store (lazy delete)
** 14.  purge_expired works on encrypted store
** 15.  kvstore_reencrypt changes the key; old password fails afterward
** 16.  After reencrypt, all values still readable with new password
** 17.  kvstore_remove_encryption; store is plaintext; kvstore_open succeeds
** 18.  is_encrypted() returns 0 on plain store
** 19.  kvstore_open_encrypted on non-encrypted file → KVSTORE_AUTH_FAILED
** 20.  Empty value round-trip on encrypted store
*/

#include "kvstore.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

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

/* Unique temp-file helper */
static char gTmpPath[256];
static int  gTmpIdx = 0;
static const char *tmpdb(void){
  snprintf(gTmpPath, sizeof(gTmpPath), "/tmp/snkv_enc_test_%d.db", gTmpIdx++);
  return gTmpPath;
}

static void rmdb(const char *path){
  char buf[300];
  remove(path);
  snprintf(buf, sizeof(buf), "%s-wal", path);  remove(buf);
  snprintf(buf, sizeof(buf), "%s-shm", path);  remove(buf);
}

/* ---- Test 1: create encrypted store ---- */
static void test_create_encrypted(void){
  printf("Test 1: create encrypted store\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  int rc = kvstore_open_encrypted(path, "password", 8, &db, NULL);
  printf("  [debug] open_encrypted rc=%d\n", rc);
  ASSERT("open_encrypted returns OK", rc == KVSTORE_OK);
  ASSERT("db handle non-NULL", db != NULL);
  ASSERT("is_encrypted == 1", kvstore_is_encrypted(db) == 1);
  if( db ) kvstore_close(db);
  rmdb(path);
}

/* ---- Test 2: re-open with correct password ---- */
static void test_reopen_correct_password(void){
  printf("Test 2: re-open correct password\n");
  const char *path = tmpdb();
  KVStore *db = NULL;

  int rc = kvstore_open_encrypted(path, "secret", 6, &db, NULL);
  ASSERT("create ok", rc == KVSTORE_OK);
  if( db ) { kvstore_close(db); db = NULL; }

  rc = kvstore_open_encrypted(path, "secret", 6, &db, NULL);
  ASSERT("reopen correct password ok", rc == KVSTORE_OK);
  ASSERT("is_encrypted after reopen", kvstore_is_encrypted(db));
  if( db ) kvstore_close(db);
  rmdb(path);
}

/* ---- Test 3: wrong password → AUTH_FAILED ---- */
static void test_wrong_password(void){
  printf("Test 3: wrong password\n");
  const char *path = tmpdb();
  KVStore *db = NULL;

  kvstore_open_encrypted(path, "correct", 7, &db, NULL);
  if( db ){ kvstore_close(db); db = NULL; }

  int rc = kvstore_open_encrypted(path, "wrong", 5, &db, NULL);
  ASSERT("wrong password → AUTH_FAILED", rc == KVSTORE_AUTH_FAILED);
  ASSERT("db is NULL on failure", db == NULL);
  rmdb(path);
}

/* ---- Test 4: ciphertext in file ---- */
static void test_ciphertext_in_file(void){
  printf("Test 4: values not plaintext in file\n");
  const char *path = tmpdb();
  KVStore *db = NULL;

  kvstore_open_encrypted(path, "pw", 2, &db, NULL);
  kvstore_put(db, "secret_key", 10, "plaintext_value", 15);
  kvstore_close(db);

  /* Read raw file and check "plaintext_value" is NOT present */
  FILE *f = fopen(path, "rb");
  ASSERT("file exists", f != NULL);
  if( f ){
    fseek(f, 0, SEEK_END);
    long sz = ftell(f); rewind(f);
    char *buf = malloc(sz);
    if( buf ){
      fread(buf, 1, sz, f);
      int found = 0;
      for( long i = 0; i < sz - 14; i++ ){
        if( memcmp(buf + i, "plaintext_value", 15) == 0 ){ found = 1; break; }
      }
      ASSERT("plaintext NOT in file", !found);
      free(buf);
    }
    fclose(f);
  }
  rmdb(path);
}

/* ---- Test 5: put/get round-trip ---- */
static void test_put_get_roundtrip(void){
  printf("Test 5: put/get round-trip\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open_encrypted(path, "pw", 2, &db, NULL);

  kvstore_put(db, "hello", 5, "world", 5);
  void *val = NULL; int nval = 0;
  int rc = kvstore_get(db, "hello", 5, &val, &nval);
  ASSERT("get returns OK", rc == KVSTORE_OK);
  ASSERT("value matches", val && nval == 5 && memcmp(val, "world", 5) == 0);
  if( val ) snkv_free(val);
  kvstore_close(db);
  rmdb(path);
}

/* ---- Test 6: get missing key → NOTFOUND ---- */
static void test_get_missing(void){
  printf("Test 6: get missing key\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open_encrypted(path, "pw", 2, &db, NULL);

  void *val = NULL; int nval = 0;
  int rc = kvstore_get(db, "nokey", 5, &val, &nval);
  ASSERT("NOTFOUND on missing key", rc == KVSTORE_NOTFOUND);
  ASSERT("val is NULL", val == NULL);
  kvstore_close(db);
  rmdb(path);
}

/* ---- Test 7: delete ---- */
static void test_delete(void){
  printf("Test 7: delete\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open_encrypted(path, "pw", 2, &db, NULL);

  kvstore_put(db, "k", 1, "v", 1);
  int rc = kvstore_delete(db, "k", 1);
  ASSERT("delete OK", rc == KVSTORE_OK);
  void *val = NULL; int nval = 0;
  rc = kvstore_get(db, "k", 1, &val, &nval);
  ASSERT("key gone after delete", rc == KVSTORE_NOTFOUND);
  kvstore_close(db);
  rmdb(path);
}

/* ---- Test 8: iterator ---- */
static void test_iterator(void){
  printf("Test 8: iterator\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open_encrypted(path, "pw", 2, &db, NULL);

  kvstore_put(db, "a", 1, "aval", 4);
  kvstore_put(db, "b", 1, "bval", 4);
  kvstore_put(db, "c", 1, "cval", 4);

  KVIterator *it = NULL;
  kvstore_iterator_create(db, &it);
  kvstore_iterator_first(it);

  int count = 0;
  int ok = 1;
  while( !kvstore_iterator_eof(it) ){
    void *v = NULL; int nv = 0;
    kvstore_iterator_value(it, &v, &nv);
    if( nv != 4 ) ok = 0;
    count++;
    kvstore_iterator_next(it);
  }
  kvstore_iterator_close(it);
  ASSERT("iterator count == 3", count == 3);
  ASSERT("all values length 4", ok);
  kvstore_close(db);
  rmdb(path);
}

/* ---- Test 9: prefix iterator ---- */
static void test_prefix_iterator(void){
  printf("Test 9: prefix iterator\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open_encrypted(path, "pw", 2, &db, NULL);

  kvstore_put(db, "user:1", 6, "alice", 5);
  kvstore_put(db, "user:2", 6, "bob",   3);
  kvstore_put(db, "other",  5, "x",     1);

  KVIterator *it = NULL;
  kvstore_prefix_iterator_create(db, "user:", 5, &it);
  kvstore_iterator_first(it);

  int count = 0;
  while( !kvstore_iterator_eof(it) ){
    count++;
    kvstore_iterator_next(it);
  }
  kvstore_iterator_close(it);
  ASSERT("prefix iterator count == 2", count == 2);
  kvstore_close(db);
  rmdb(path);
}

/* ---- Test 10: seek iterator ---- */
static void test_seek_iterator(void){
  printf("Test 10: seek iterator\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open_encrypted(path, "pw", 2, &db, NULL);

  kvstore_put(db, "apple",  5, "A", 1);
  kvstore_put(db, "banana", 6, "B", 1);
  kvstore_put(db, "cherry", 6, "C", 1);

  KVIterator *it = NULL;
  kvstore_iterator_create(db, &it);
  kvstore_iterator_seek(it, "banana", 6);

  void *k = NULL; int nk = 0;
  kvstore_iterator_key(it, &k, &nk);
  ASSERT("seek lands on banana", nk == 6 && memcmp(k, "banana", 6) == 0);
  kvstore_iterator_close(it);
  kvstore_close(db);
  rmdb(path);
}

/* ---- Test 11: reverse iterator ---- */
static void test_reverse_iterator(void){
  printf("Test 11: reverse iterator\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open_encrypted(path, "pw", 2, &db, NULL);

  kvstore_put(db, "a", 1, "1", 1);
  kvstore_put(db, "b", 1, "2", 1);
  kvstore_put(db, "c", 1, "3", 1);

  KVIterator *it = NULL;
  kvstore_reverse_iterator_create(db, &it);
  kvstore_iterator_last(it);

  void *k = NULL; int nk = 0;
  kvstore_iterator_key(it, &k, &nk);
  ASSERT("reverse starts at 'c'", nk == 1 && memcmp(k, "c", 1) == 0);

  kvstore_iterator_prev(it);
  kvstore_iterator_key(it, &k, &nk);
  ASSERT("reverse second is 'b'", nk == 1 && memcmp(k, "b", 1) == 0);

  kvstore_iterator_close(it);
  kvstore_close(db);
  rmdb(path);
}

/* ---- Test 12: TTL + encryption round-trip ---- */
static void test_ttl_roundtrip(void){
  printf("Test 12: TTL + encryption round-trip\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open_encrypted(path, "pw", 2, &db, NULL);

  int64_t expire = kvstore_now_ms() + 5000;  /* 5 seconds */
  int rc = kvstore_put_ttl(db, "session", 7, "tok123", 6, expire);
  ASSERT("put_ttl ok", rc == KVSTORE_OK);

  void *val = NULL; int nval = 0; int64_t remaining = 0;
  rc = kvstore_get_ttl(db, "session", 7, &val, &nval, &remaining);
  ASSERT("get_ttl ok", rc == KVSTORE_OK);
  ASSERT("value correct", val && nval == 6 && memcmp(val, "tok123", 6) == 0);
  ASSERT("remaining > 0", remaining > 0 && remaining <= 5000);
  if( val ) snkv_free(val);
  kvstore_close(db);
  rmdb(path);
}

/* ---- Test 13: TTL expiry on encrypted store ---- */
static void test_ttl_expiry(void){
  printf("Test 13: TTL expiry\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open_encrypted(path, "pw", 2, &db, NULL);

  int64_t expire = kvstore_now_ms() - 1;  /* already expired */
  kvstore_put_ttl(db, "old", 3, "data", 4, expire);

  void *val = NULL; int nval = 0; int64_t remaining = 0;
  int rc = kvstore_get_ttl(db, "old", 3, &val, &nval, &remaining);
  ASSERT("expired key returns NOTFOUND", rc == KVSTORE_NOTFOUND);
  ASSERT("val is NULL", val == NULL);
  kvstore_close(db);
  rmdb(path);
}

/* ---- Test 14: purge_expired on encrypted store ---- */
static void test_purge_expired(void){
  printf("Test 14: purge_expired\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open_encrypted(path, "pw", 2, &db, NULL);

  int64_t past = kvstore_now_ms() - 100;
  kvstore_put_ttl(db, "ex1", 3, "v1", 2, past);
  kvstore_put_ttl(db, "ex2", 3, "v2", 2, past);
  kvstore_put(db, "live", 4, "lv", 2);

  int nDeleted = 0;
  int rc = kvstore_purge_expired(db, &nDeleted);
  ASSERT("purge_expired ok", rc == KVSTORE_OK);
  ASSERT("purged 2 keys", nDeleted == 2);

  void *val = NULL; int nval = 0;
  rc = kvstore_get(db, "live", 4, &val, &nval);
  ASSERT("live key still readable", rc == KVSTORE_OK);
  if( val ) snkv_free(val);
  kvstore_close(db);
  rmdb(path);
}

/* ---- Test 15: reencrypt changes key ---- */
static void test_reencrypt(void){
  printf("Test 15: reencrypt changes key\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open_encrypted(path, "oldpass", 7, &db, NULL);
  kvstore_put(db, "k", 1, "v", 1);
  int rc = kvstore_reencrypt(db, "newpass", 7);
  ASSERT("reencrypt ok", rc == KVSTORE_OK);
  kvstore_close(db);

  /* Old password should fail */
  db = NULL;
  rc = kvstore_open_encrypted(path, "oldpass", 7, &db, NULL);
  ASSERT("old password fails", rc == KVSTORE_AUTH_FAILED);
  if( db ){ kvstore_close(db); db = NULL; }

  /* New password should work */
  rc = kvstore_open_encrypted(path, "newpass", 7, &db, NULL);
  ASSERT("new password works", rc == KVSTORE_OK);
  if( db ) kvstore_close(db);
  rmdb(path);
}

/* ---- Test 16: values readable after reencrypt ---- */
static void test_reencrypt_data_intact(void){
  printf("Test 16: data intact after reencrypt\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open_encrypted(path, "p1", 2, &db, NULL);
  kvstore_put(db, "fruit", 5, "mango", 5);
  kvstore_reencrypt(db, "p2", 2);
  kvstore_close(db);

  db = NULL;
  kvstore_open_encrypted(path, "p2", 2, &db, NULL);
  void *val = NULL; int nval = 0;
  int rc = kvstore_get(db, "fruit", 5, &val, &nval);
  ASSERT("data readable after reencrypt", rc == KVSTORE_OK);
  ASSERT("value correct after reencrypt", val && nval == 5 && memcmp(val, "mango", 5) == 0);
  if( val ) snkv_free(val);
  if( db ) kvstore_close(db);
  rmdb(path);
}

/* ---- Test 17: remove_encryption ---- */
static void test_remove_encryption(void){
  printf("Test 17: remove_encryption\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open_encrypted(path, "pw", 2, &db, NULL);
  kvstore_put(db, "key", 3, "val", 3);
  int rc = kvstore_remove_encryption(db);
  ASSERT("remove_encryption ok", rc == KVSTORE_OK);
  kvstore_close(db);

  /* Now open plain — should work */
  db = NULL;
  rc = kvstore_open(path, &db, KVSTORE_JOURNAL_WAL);
  ASSERT("plain open after remove_enc ok", rc == KVSTORE_OK);
  void *val = NULL; int nval = 0;
  rc = kvstore_get(db, "key", 3, &val, &nval);
  ASSERT("value readable as plaintext", rc == KVSTORE_OK);
  ASSERT("value correct", val && nval == 3 && memcmp(val, "val", 3) == 0);
  if( val ) snkv_free(val);
  if( db ) kvstore_close(db);
  rmdb(path);
}

/* ---- Test 18: is_encrypted on plain store ---- */
static void test_is_encrypted_plain(void){
  printf("Test 18: is_encrypted on plain store\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open(path, &db, KVSTORE_JOURNAL_WAL);
  ASSERT("is_encrypted == 0 on plain store", kvstore_is_encrypted(db) == 0);
  kvstore_close(db);
  rmdb(path);
}

/* ---- Test 19: open_encrypted on plain file → AUTH_FAILED ---- */
static void test_open_enc_on_plain(void){
  printf("Test 19: open_encrypted on plain file → AUTH_FAILED\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open(path, &db, KVSTORE_JOURNAL_WAL);
  kvstore_put(db, "k", 1, "v", 1);
  kvstore_close(db);

  db = NULL;
  int rc = kvstore_open_encrypted(path, "pw", 2, &db, NULL);
  ASSERT("open_encrypted on plain → AUTH_FAILED", rc == KVSTORE_AUTH_FAILED);
  if( db ){ kvstore_close(db); db = NULL; }
  rmdb(path);
}

/* ---- Test 20: empty value round-trip ---- */
static void test_empty_value(void){
  printf("Test 20: empty value round-trip\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open_encrypted(path, "pw", 2, &db, NULL);
  kvstore_put(db, "empty", 5, "", 0);
  void *val = NULL; int nval = 0;
  int rc = kvstore_get(db, "empty", 5, &val, &nval);
  ASSERT("empty value get ok", rc == KVSTORE_OK);
  ASSERT("empty value nval == 0", nval == 0);
  if( val ) snkv_free(val);
  kvstore_close(db);
  rmdb(path);
}

/* ---- Test 21: open encrypted store without password → garbled values ---- */
/*
** Encrypted stores opened via plain kvstore_open() (bEncrypted=0) return
** raw ciphertext blobs instead of plaintext.  This test verifies that the
** returned bytes do NOT match the original value — i.e. there is no
** accidental plaintext leak when the encryption layer is bypassed.
*/
static void test_plain_open_returns_garbage(void){
  printf("Test 21: plain open of encrypted store → garbled values\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  const char *plaintext = "supersecretvalue";
  int ptlen = (int)strlen(plaintext);

  /* Write encrypted */
  kvstore_open_encrypted(path, "pw", 2, &db, NULL);
  kvstore_put(db, "k", 1, plaintext, ptlen);
  kvstore_close(db);

  /* Re-open WITHOUT a password — encryption layer is off */
  db = NULL;
  int rc = kvstore_open(path, &db, KVSTORE_JOURNAL_WAL);
  ASSERT("plain open of enc store ok", rc == KVSTORE_OK);

  void *val = NULL; int nval = 0;
  rc = kvstore_get(db, "k", 1, &val, &nval);
  /* The get succeeds (the key exists), but the value is ciphertext */
  ASSERT("plain open returns a value", rc == KVSTORE_OK);
  /* Ciphertext is always longer than plaintext (nonce+mac overhead = 40 B) */
  ASSERT("returned blob is longer than plaintext", nval > ptlen);
  /* Raw bytes must NOT equal the original plaintext */
  int matches = (nval == ptlen && memcmp(val, plaintext, ptlen) == 0);
  ASSERT("returned value is NOT the original plaintext", !matches);

  if( val ) snkv_free(val);
  kvstore_close(db);
  rmdb(path);
}

/* ---- Main ---- */
int main(void){
  printf("=== SNKV Encryption Tests ===\n\n");

  test_create_encrypted();
  test_reopen_correct_password();
  test_wrong_password();
  test_ciphertext_in_file();
  test_put_get_roundtrip();
  test_get_missing();
  test_delete();
  test_iterator();
  test_prefix_iterator();
  test_seek_iterator();
  test_reverse_iterator();
  test_ttl_roundtrip();
  test_ttl_expiry();
  test_purge_expired();
  test_reencrypt();
  test_reencrypt_data_intact();
  test_remove_encryption();
  test_is_encrypted_plain();
  test_open_enc_on_plain();
  test_empty_value();
  test_plain_open_returns_garbage();

  printf("\n=== Results: %d passed, %d failed ===\n", passed, failed);
  return failed > 0 ? 1 : 0;
}
