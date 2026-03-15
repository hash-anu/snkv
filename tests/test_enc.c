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
** 21.  Plain open of encrypted store returns garbled values
** 22.  Full functionality after kvstore_reencrypt (put/get/delete/exists/
**      iterator/reverse/prefix/seek/TTL/purge/CF/put_if_absent/count)
** 23.  Full functionality after kvstore_remove_encryption (same coverage)
** 24.  remove_encryption → kvstore_open_encrypted (re-encrypt) → full
**      functionality (put/get/delete/exists/iterator/reverse/prefix/seek/
**      TTL/purge/CF/put_if_absent/count + old password fails/new works)
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
  snprintf(gTmpPath, sizeof(gTmpPath), "tests/enc_test_%d.db", gTmpIdx++);
  /* Pre-remove any leftover files from a previous crashed run */
  char buf[300];
  remove(gTmpPath);
  snprintf(buf, sizeof(buf), "%s-wal", gTmpPath); remove(buf);
  snprintf(buf, sizeof(buf), "%s-shm", gTmpPath); remove(buf);
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

/* ---- Test 19: open_encrypted on plain file → encrypts the store ---- */
static void test_open_enc_on_plain(void){
  printf("Test 19: open_encrypted on plain file → encrypts store\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open(path, &db, KVSTORE_JOURNAL_WAL);
  kvstore_put(db, "k", 1, "v", 1);
  kvstore_close(db);

  /* open_encrypted on a plain store: encrypts the existing data */
  db = NULL;
  int rc = kvstore_open_encrypted(path, "pw", 2, &db, NULL);
  ASSERT("open_encrypted on plain store ok", rc == KVSTORE_OK);
  ASSERT("is_encrypted == 1", kvstore_is_encrypted(db) == 1);
  void *val = NULL; int nval = 0;
  rc = kvstore_get(db, "k", 1, &val, &nval);
  ASSERT("pre-existing key readable after encrypt", rc == KVSTORE_OK);
  ASSERT("pre-existing value correct", val && nval == 1 && ((char*)val)[0] == 'v');
  if( val ){ snkv_free(val); val = NULL; }
  kvstore_close(db); db = NULL;

  /* old password must fail */
  rc = kvstore_open_encrypted(path, "wrong", 5, &db, NULL);
  ASSERT("wrong password fails", rc == KVSTORE_AUTH_FAILED);
  if( db ){ kvstore_close(db); db = NULL; }

  /* correct password reopens fine */
  rc = kvstore_open_encrypted(path, "pw", 2, &db, NULL);
  ASSERT("correct password reopens ok", rc == KVSTORE_OK);
  kvstore_close(db); db = NULL;
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

  /* Re-open WITHOUT a password — must be rejected with AUTH_FAILED */
  db = NULL;
  int rc = kvstore_open(path, &db, KVSTORE_JOURNAL_WAL);
  ASSERT("plain open of enc store returns AUTH_FAILED", rc == KVSTORE_AUTH_FAILED);
  ASSERT("db handle is NULL on AUTH_FAILED", db == NULL);
  rmdb(path);
}

/* ---- Test 22: full functionality after reencrypt ---- */
static void test_reencrypt_full_functionality(void){
  printf("Test 22: full functionality after reencrypt\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open_encrypted(path, "pass1", 5, &db, NULL);

  /* Write data before reencrypt */
  kvstore_put(db, "k1", 2, "v1", 2);
  kvstore_put(db, "k2", 2, "v2", 2);
  kvstore_put(db, "k3", 2, "v3", 2);

  /* TTL before reencrypt */
  int64_t expire = kvstore_now_ms() + 10000;
  kvstore_put_ttl(db, "ttlkey", 6, "ttlval", 6, expire);

  /* Column family before reencrypt */
  KVColumnFamily *cf = NULL;
  kvstore_cf_create(db, "myCF", &cf);
  kvstore_cf_put(cf, "cfk", 3, "cfv", 3);

  /* Reencrypt with new password */
  int rc = kvstore_reencrypt(db, "pass2", 5);
  ASSERT("reencrypt ok", rc == KVSTORE_OK);

  /* --- put / get --- */
  rc = kvstore_put(db, "k4", 2, "v4", 2);
  ASSERT("put after reencrypt ok", rc == KVSTORE_OK);

  void *val = NULL; int nval = 0;
  rc = kvstore_get(db, "k1", 2, &val, &nval);
  ASSERT("get pre-reencrypt key ok", rc == KVSTORE_OK);
  ASSERT("pre-reencrypt value correct", val && nval == 2 && memcmp(val, "v1", 2) == 0);
  if( val ){ snkv_free(val); val = NULL; }

  rc = kvstore_get(db, "k4", 2, &val, &nval);
  ASSERT("get post-reencrypt key ok", rc == KVSTORE_OK);
  ASSERT("post-reencrypt value correct", val && nval == 2 && memcmp(val, "v4", 2) == 0);
  if( val ){ snkv_free(val); val = NULL; }

  /* --- delete --- */
  rc = kvstore_delete(db, "k2", 2);
  ASSERT("delete after reencrypt ok", rc == KVSTORE_OK);
  rc = kvstore_get(db, "k2", 2, &val, &nval);
  ASSERT("deleted key gone", rc == KVSTORE_NOTFOUND);

  /* --- exists --- */
  int exists = 0;
  kvstore_exists(db, "k1", 2, &exists);
  ASSERT("exists ok after reencrypt", exists == 1);
  kvstore_exists(db, "k2", 2, &exists);
  ASSERT("deleted key not exists", exists == 0);

  /* --- iterator --- */
  KVIterator *it = NULL;
  kvstore_iterator_create(db, &it);
  kvstore_iterator_first(it);
  int count = 0;
  while( !kvstore_iterator_eof(it) ){ count++; kvstore_iterator_next(it); }
  kvstore_iterator_close(it);
  ASSERT("iterator count correct after reencrypt", count == 4); /* k1,k3,k4,ttlkey */

  /* --- reverse iterator --- */
  kvstore_reverse_iterator_create(db, &it);
  kvstore_iterator_last(it);
  void *rk = NULL; int nrk = 0;
  kvstore_iterator_key(it, &rk, &nrk);
  ASSERT("reverse iterator ok after reencrypt", nrk > 0);
  kvstore_iterator_close(it);

  /* --- prefix iterator --- */
  kvstore_put(db, "pfx:a", 5, "pa", 2);
  kvstore_put(db, "pfx:b", 5, "pb", 2);
  kvstore_prefix_iterator_create(db, "pfx:", 4, &it);
  kvstore_iterator_first(it);
  int pfxCount = 0;
  while( !kvstore_iterator_eof(it) ){ pfxCount++; kvstore_iterator_next(it); }
  kvstore_iterator_close(it);
  ASSERT("prefix iterator ok after reencrypt", pfxCount == 2);

  /* --- seek --- */
  kvstore_iterator_create(db, &it);
  kvstore_iterator_seek(it, "k3", 2);
  void *sk = NULL; int nsk = 0;
  kvstore_iterator_key(it, &sk, &nsk);
  ASSERT("seek ok after reencrypt", nsk == 2 && memcmp(sk, "k3", 2) == 0);
  kvstore_iterator_close(it);

  /* --- TTL: pre-reencrypt key still readable --- */
  int64_t remaining = 0;
  rc = kvstore_get_ttl(db, "ttlkey", 6, &val, &nval, &remaining);
  ASSERT("ttl get after reencrypt ok", rc == KVSTORE_OK);
  ASSERT("ttl value correct after reencrypt", val && nval == 6 && memcmp(val, "ttlval", 6) == 0);
  ASSERT("ttl remaining > 0 after reencrypt", remaining > 0);
  if( val ){ snkv_free(val); val = NULL; }

  /* --- TTL: new key with TTL after reencrypt --- */
  int64_t expire2 = kvstore_now_ms() + 8000;
  rc = kvstore_put_ttl(db, "newttl", 6, "nv", 2, expire2);
  ASSERT("put_ttl after reencrypt ok", rc == KVSTORE_OK);
  rc = kvstore_get_ttl(db, "newttl", 6, &val, &nval, &remaining);
  ASSERT("get_ttl new key after reencrypt ok", rc == KVSTORE_OK);
  if( val ){ snkv_free(val); val = NULL; }

  /* --- TTL: expired key --- */
  kvstore_put_ttl(db, "expk", 4, "ev", 2, kvstore_now_ms() - 1);
  rc = kvstore_get_ttl(db, "expk", 4, &val, &nval, &remaining);
  ASSERT("expired key NOTFOUND after reencrypt", rc == KVSTORE_NOTFOUND);

  /* --- purge_expired --- */
  kvstore_put_ttl(db, "p1", 2, "pv1", 3, kvstore_now_ms() - 100);
  kvstore_put_ttl(db, "p2", 2, "pv2", 3, kvstore_now_ms() - 100);
  int nDeleted = 0;
  rc = kvstore_purge_expired(db, &nDeleted);
  ASSERT("purge_expired after reencrypt ok", rc == KVSTORE_OK);
  ASSERT("purge_expired count >= 2 after reencrypt", nDeleted >= 2);

  /* --- count --- */
  int64_t cnt = 0;
  kvstore_count(db, &cnt);
  ASSERT("count > 0 after reencrypt", cnt > 0);

  /* --- column family: pre-reencrypt CF data readable --- */
  rc = kvstore_cf_get(cf, "cfk", 3, &val, &nval);
  ASSERT("cf get after reencrypt ok", rc == KVSTORE_OK);
  ASSERT("cf value correct after reencrypt", val && nval == 3 && memcmp(val, "cfv", 3) == 0);
  if( val ){ snkv_free(val); val = NULL; }

  /* --- column family: new write after reencrypt --- */
  rc = kvstore_cf_put(cf, "cfk2", 4, "cfv2", 4);
  ASSERT("cf put after reencrypt ok", rc == KVSTORE_OK);
  rc = kvstore_cf_get(cf, "cfk2", 4, &val, &nval);
  ASSERT("cf get new key after reencrypt ok", rc == KVSTORE_OK);
  ASSERT("cf new value correct after reencrypt", val && nval == 4 && memcmp(val, "cfv2", 4) == 0);
  if( val ){ snkv_free(val); val = NULL; }

  /* --- put_if_absent after reencrypt --- */
  int inserted = 0;
  rc = kvstore_put_if_absent(db, "k1", 2, "new", 3, 0, &inserted);
  ASSERT("put_if_absent existing key returns KVSTORE_OK (not inserted)", rc == KVSTORE_OK);
  ASSERT("put_if_absent existing key not inserted", inserted == 0);
  rc = kvstore_get(db, "k1", 2, &val, &nval);
  ASSERT("existing key unchanged by put_if_absent", val && nval == 2 && memcmp(val, "v1", 2) == 0);
  if( val ){ snkv_free(val); val = NULL; }

  rc = kvstore_put_if_absent(db, "brand_new", 9, "bv", 2, 0, &inserted);
  ASSERT("put_if_absent new key ok", rc == KVSTORE_OK);
  ASSERT("put_if_absent new key inserted", inserted == 1);
  rc = kvstore_get(db, "brand_new", 9, &val, &nval);
  ASSERT("put_if_absent new key readable", rc == KVSTORE_OK);
  if( val ){ snkv_free(val); val = NULL; }

  /* --- is_encrypted still 1 after reencrypt --- */
  ASSERT("is_encrypted still 1 after reencrypt", kvstore_is_encrypted(db) == 1);

  kvstore_cf_close(cf);
  kvstore_close(db);

  /* Reopen with new password and verify a sample key */
  db = NULL;
  rc = kvstore_open_encrypted(path, "pass2", 5, &db, NULL);
  ASSERT("reopen with new password ok", rc == KVSTORE_OK);
  rc = kvstore_get(db, "k1", 2, &val, &nval);
  ASSERT("k1 readable after reopen with new password", rc == KVSTORE_OK);
  if( val ){ snkv_free(val); val = NULL; }
  kvstore_close(db);

  rmdb(path);
}

/* ---- Test 23: full functionality after remove_encryption ---- */
static void test_remove_encryption_full_functionality(void){
  printf("Test 23: full functionality after remove_encryption\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open_encrypted(path, "pw", 2, &db, NULL);

  /* Write data before remove_encryption */
  kvstore_put(db, "k1", 2, "v1", 2);
  kvstore_put(db, "k2", 2, "v2", 2);
  kvstore_put(db, "k3", 2, "v3", 2);

  /* TTL before remove_encryption */
  int64_t expire = kvstore_now_ms() + 10000;
  kvstore_put_ttl(db, "ttlkey", 6, "ttlval", 6, expire);

  /* Column family before remove_encryption */
  KVColumnFamily *cf = NULL;
  kvstore_cf_create(db, "myCF", &cf);
  kvstore_cf_put(cf, "cfk", 3, "cfv", 3);

  /* Remove encryption */
  int rc = kvstore_remove_encryption(db);
  ASSERT("remove_encryption ok", rc == KVSTORE_OK);
  ASSERT("is_encrypted == 0 after remove", kvstore_is_encrypted(db) == 0);

  /* --- put / get --- */
  rc = kvstore_put(db, "k4", 2, "v4", 2);
  ASSERT("put after remove_encryption ok", rc == KVSTORE_OK);

  void *val = NULL; int nval = 0;
  rc = kvstore_get(db, "k1", 2, &val, &nval);
  ASSERT("get pre-remove key ok", rc == KVSTORE_OK);
  ASSERT("pre-remove value correct", val && nval == 2 && memcmp(val, "v1", 2) == 0);
  if( val ){ snkv_free(val); val = NULL; }

  rc = kvstore_get(db, "k4", 2, &val, &nval);
  ASSERT("get post-remove key ok", rc == KVSTORE_OK);
  ASSERT("post-remove value correct", val && nval == 2 && memcmp(val, "v4", 2) == 0);
  if( val ){ snkv_free(val); val = NULL; }

  /* --- delete --- */
  rc = kvstore_delete(db, "k2", 2);
  ASSERT("delete after remove_encryption ok", rc == KVSTORE_OK);
  rc = kvstore_get(db, "k2", 2, &val, &nval);
  ASSERT("deleted key gone after remove_encryption", rc == KVSTORE_NOTFOUND);

  /* --- exists --- */
  int exists = 0;
  kvstore_exists(db, "k1", 2, &exists);
  ASSERT("exists ok after remove_encryption", exists == 1);

  /* --- iterator --- */
  KVIterator *it = NULL;
  kvstore_iterator_create(db, &it);
  kvstore_iterator_first(it);
  int count = 0;
  while( !kvstore_iterator_eof(it) ){ count++; kvstore_iterator_next(it); }
  kvstore_iterator_close(it);
  ASSERT("iterator count correct after remove_encryption", count == 4); /* k1,k3,k4,ttlkey */

  /* --- reverse iterator --- */
  kvstore_reverse_iterator_create(db, &it);
  kvstore_iterator_last(it);
  void *rk = NULL; int nrk = 0;
  kvstore_iterator_key(it, &rk, &nrk);
  ASSERT("reverse iterator ok after remove_encryption", nrk > 0);
  kvstore_iterator_close(it);

  /* --- prefix iterator --- */
  kvstore_put(db, "pfx:a", 5, "pa", 2);
  kvstore_put(db, "pfx:b", 5, "pb", 2);
  kvstore_prefix_iterator_create(db, "pfx:", 4, &it);
  kvstore_iterator_first(it);
  int pfxCount = 0;
  while( !kvstore_iterator_eof(it) ){ pfxCount++; kvstore_iterator_next(it); }
  kvstore_iterator_close(it);
  ASSERT("prefix iterator ok after remove_encryption", pfxCount == 2);

  /* --- seek --- */
  kvstore_iterator_create(db, &it);
  kvstore_iterator_seek(it, "k3", 2);
  void *sk = NULL; int nsk = 0;
  kvstore_iterator_key(it, &sk, &nsk);
  ASSERT("seek ok after remove_encryption", nsk == 2 && memcmp(sk, "k3", 2) == 0);
  kvstore_iterator_close(it);

  /* --- TTL: pre-remove key still readable --- */
  int64_t remaining = 0;
  rc = kvstore_get_ttl(db, "ttlkey", 6, &val, &nval, &remaining);
  ASSERT("ttl get after remove_encryption ok", rc == KVSTORE_OK);
  ASSERT("ttl value correct after remove_encryption", val && nval == 6 && memcmp(val, "ttlval", 6) == 0);
  ASSERT("ttl remaining > 0 after remove_encryption", remaining > 0);
  if( val ){ snkv_free(val); val = NULL; }

  /* --- TTL: new key with TTL after remove_encryption --- */
  int64_t expire2 = kvstore_now_ms() + 8000;
  rc = kvstore_put_ttl(db, "newttl", 6, "nv", 2, expire2);
  ASSERT("put_ttl after remove_encryption ok", rc == KVSTORE_OK);
  rc = kvstore_get_ttl(db, "newttl", 6, &val, &nval, &remaining);
  ASSERT("get_ttl new key after remove_encryption ok", rc == KVSTORE_OK);
  if( val ){ snkv_free(val); val = NULL; }

  /* --- TTL: expired key --- */
  kvstore_put_ttl(db, "expk", 4, "ev", 2, kvstore_now_ms() - 1);
  rc = kvstore_get_ttl(db, "expk", 4, &val, &nval, &remaining);
  ASSERT("expired key NOTFOUND after remove_encryption", rc == KVSTORE_NOTFOUND);

  /* --- purge_expired --- */
  kvstore_put_ttl(db, "p1", 2, "pv1", 3, kvstore_now_ms() - 100);
  kvstore_put_ttl(db, "p2", 2, "pv2", 3, kvstore_now_ms() - 100);
  int nDeleted = 0;
  rc = kvstore_purge_expired(db, &nDeleted);
  ASSERT("purge_expired after remove_encryption ok", rc == KVSTORE_OK);
  ASSERT("purge_expired count >= 2 after remove_encryption", nDeleted >= 2);

  /* --- count --- */
  int64_t cnt = 0;
  kvstore_count(db, &cnt);
  ASSERT("count > 0 after remove_encryption", cnt > 0);

  /* --- column family: pre-remove CF data readable --- */
  rc = kvstore_cf_get(cf, "cfk", 3, &val, &nval);
  ASSERT("cf get after remove_encryption ok", rc == KVSTORE_OK);
  ASSERT("cf value correct after remove_encryption", val && nval == 3 && memcmp(val, "cfv", 3) == 0);
  if( val ){ snkv_free(val); val = NULL; }

  /* --- column family: new write after remove_encryption --- */
  rc = kvstore_cf_put(cf, "cfk2", 4, "cfv2", 4);
  ASSERT("cf put after remove_encryption ok", rc == KVSTORE_OK);
  rc = kvstore_cf_get(cf, "cfk2", 4, &val, &nval);
  ASSERT("cf get new key after remove_encryption ok", rc == KVSTORE_OK);
  ASSERT("cf new value correct after remove_encryption", val && nval == 4 && memcmp(val, "cfv2", 4) == 0);
  if( val ){ snkv_free(val); val = NULL; }

  /* --- put_if_absent after remove_encryption --- */
  int inserted = 0;
  rc = kvstore_put_if_absent(db, "k1", 2, "new", 3, 0, &inserted);
  ASSERT("put_if_absent existing key unchanged", rc == KVSTORE_OK);
  ASSERT("put_if_absent existing key not inserted", inserted == 0);
  rc = kvstore_get(db, "k1", 2, &val, &nval);
  ASSERT("existing key not overwritten by put_if_absent", val && nval == 2 && memcmp(val, "v1", 2) == 0);
  if( val ){ snkv_free(val); val = NULL; }

  rc = kvstore_put_if_absent(db, "brand_new", 9, "bv", 2, 0, &inserted);
  ASSERT("put_if_absent new key ok after remove_encryption", rc == KVSTORE_OK);
  ASSERT("put_if_absent new key inserted", inserted == 1);
  rc = kvstore_get(db, "brand_new", 9, &val, &nval);
  ASSERT("put_if_absent new key readable after remove_encryption", rc == KVSTORE_OK);
  if( val ){ snkv_free(val); val = NULL; }

  kvstore_cf_close(cf);
  kvstore_close(db);

  /* Reopen as plain store and verify key is readable */
  db = NULL;
  rc = kvstore_open(path, &db, KVSTORE_JOURNAL_WAL);
  ASSERT("plain reopen after remove_encryption ok", rc == KVSTORE_OK);
  rc = kvstore_get(db, "k1", 2, &val, &nval);
  ASSERT("k1 readable on plain reopen", rc == KVSTORE_OK);
  ASSERT("k1 value correct on plain reopen", val && nval == 2 && memcmp(val, "v1", 2) == 0);
  if( val ){ snkv_free(val); val = NULL; }
  kvstore_close(db);

  rmdb(path);
}

/* ---- Test 24: remove_encryption then re-encrypt with open_encrypted ---- */
static void test_remove_then_reencrypt_full_functionality(void){
  printf("Test 24: remove_encryption → open_encrypted → full functionality\n");
  const char *path = tmpdb();
  KVStore *db = NULL;
  kvstore_open_encrypted(path, "first", 5, &db, NULL);

  /* Write data in first encrypted session */
  kvstore_put(db, "k1", 2, "v1", 2);
  kvstore_put(db, "k2", 2, "v2", 2);
  kvstore_put(db, "k3", 2, "v3", 2);

  int64_t expire = kvstore_now_ms() + 10000;
  kvstore_put_ttl(db, "ttlkey", 6, "ttlval", 6, expire);

  KVColumnFamily *cf = NULL;
  kvstore_cf_create(db, "myCF", &cf);
  kvstore_cf_put(cf, "cfk", 3, "cfv", 3);
  kvstore_cf_close(cf); cf = NULL;

  /* Step 1: remove encryption → plaintext */
  int rc = kvstore_remove_encryption(db);
  ASSERT("remove_encryption ok", rc == KVSTORE_OK);
  ASSERT("is_encrypted == 0 after remove", kvstore_is_encrypted(db) == 0);
  kvstore_close(db); db = NULL;

  /* Step 2: re-open with open_encrypted → re-encrypts the store */
  rc = kvstore_open_encrypted(path, "second", 6, &db, NULL);
  ASSERT("open_encrypted on plain store ok", rc == KVSTORE_OK);
  ASSERT("is_encrypted == 1 after re-encrypt", kvstore_is_encrypted(db) == 1);

  /* --- put / get --- */
  rc = kvstore_put(db, "k4", 2, "v4", 2);
  ASSERT("put after re-encrypt ok", rc == KVSTORE_OK);

  void *val = NULL; int nval = 0;
  rc = kvstore_get(db, "k1", 2, &val, &nval);
  ASSERT("get pre-remove key ok", rc == KVSTORE_OK);
  ASSERT("pre-remove value correct", val && nval == 2 && memcmp(val, "v1", 2) == 0);
  if( val ){ snkv_free(val); val = NULL; }

  rc = kvstore_get(db, "k4", 2, &val, &nval);
  ASSERT("get post-re-encrypt key ok", rc == KVSTORE_OK);
  ASSERT("post-re-encrypt value correct", val && nval == 2 && memcmp(val, "v4", 2) == 0);
  if( val ){ snkv_free(val); val = NULL; }

  /* --- delete --- */
  rc = kvstore_delete(db, "k2", 2);
  ASSERT("delete after re-encrypt ok", rc == KVSTORE_OK);
  rc = kvstore_get(db, "k2", 2, &val, &nval);
  ASSERT("deleted key gone", rc == KVSTORE_NOTFOUND);

  /* --- exists --- */
  int exists = 0;
  kvstore_exists(db, "k1", 2, &exists);
  ASSERT("exists ok after re-encrypt", exists == 1);
  kvstore_exists(db, "k2", 2, &exists);
  ASSERT("deleted key not exists", exists == 0);

  /* --- iterator --- */
  KVIterator *it = NULL;
  kvstore_iterator_create(db, &it);
  kvstore_iterator_first(it);
  int count = 0;
  while( !kvstore_iterator_eof(it) ){ count++; kvstore_iterator_next(it); }
  kvstore_iterator_close(it);
  ASSERT("iterator count correct after re-encrypt", count == 4); /* k1,k3,k4,ttlkey */

  /* --- reverse iterator --- */
  kvstore_reverse_iterator_create(db, &it);
  kvstore_iterator_last(it);
  void *rk = NULL; int nrk = 0;
  kvstore_iterator_key(it, &rk, &nrk);
  ASSERT("reverse iterator ok after re-encrypt", nrk > 0);
  kvstore_iterator_close(it);

  /* --- prefix iterator --- */
  kvstore_put(db, "pfx:a", 5, "pa", 2);
  kvstore_put(db, "pfx:b", 5, "pb", 2);
  kvstore_prefix_iterator_create(db, "pfx:", 4, &it);
  kvstore_iterator_first(it);
  int pfxCount = 0;
  while( !kvstore_iterator_eof(it) ){ pfxCount++; kvstore_iterator_next(it); }
  kvstore_iterator_close(it);
  ASSERT("prefix iterator ok after re-encrypt", pfxCount == 2);

  /* --- seek --- */
  kvstore_iterator_create(db, &it);
  kvstore_iterator_seek(it, "k3", 2);
  void *sk = NULL; int nsk = 0;
  kvstore_iterator_key(it, &sk, &nsk);
  ASSERT("seek ok after re-encrypt", nsk == 2 && memcmp(sk, "k3", 2) == 0);
  kvstore_iterator_close(it);

  /* --- TTL: key written before remove_encryption still readable --- */
  int64_t remaining = 0;
  rc = kvstore_get_ttl(db, "ttlkey", 6, &val, &nval, &remaining);
  ASSERT("ttl get after re-encrypt ok", rc == KVSTORE_OK);
  ASSERT("ttl value correct after re-encrypt", val && nval == 6 && memcmp(val, "ttlval", 6) == 0);
  ASSERT("ttl remaining > 0 after re-encrypt", remaining > 0);
  if( val ){ snkv_free(val); val = NULL; }

  /* --- TTL: new key --- */
  int64_t expire2 = kvstore_now_ms() + 8000;
  rc = kvstore_put_ttl(db, "newttl", 6, "nv", 2, expire2);
  ASSERT("put_ttl after re-encrypt ok", rc == KVSTORE_OK);
  rc = kvstore_get_ttl(db, "newttl", 6, &val, &nval, &remaining);
  ASSERT("get_ttl new key after re-encrypt ok", rc == KVSTORE_OK);
  if( val ){ snkv_free(val); val = NULL; }

  /* --- TTL: expired key --- */
  kvstore_put_ttl(db, "expk", 4, "ev", 2, kvstore_now_ms() - 1);
  rc = kvstore_get_ttl(db, "expk", 4, &val, &nval, &remaining);
  ASSERT("expired key NOTFOUND after re-encrypt", rc == KVSTORE_NOTFOUND);

  /* --- purge_expired --- */
  kvstore_put_ttl(db, "p1", 2, "pv1", 3, kvstore_now_ms() - 100);
  kvstore_put_ttl(db, "p2", 2, "pv2", 3, kvstore_now_ms() - 100);
  int nDeleted = 0;
  rc = kvstore_purge_expired(db, &nDeleted);
  ASSERT("purge_expired after re-encrypt ok", rc == KVSTORE_OK);
  ASSERT("purge_expired count >= 2 after re-encrypt", nDeleted >= 2);

  /* --- count --- */
  int64_t cnt = 0;
  kvstore_count(db, &cnt);
  ASSERT("count > 0 after re-encrypt", cnt > 0);

  /* --- column family: data written before remove_encryption readable --- */
  rc = kvstore_cf_open(db, "myCF", &cf);
  ASSERT("cf reopen after re-encrypt ok", rc == KVSTORE_OK);
  rc = kvstore_cf_get(cf, "cfk", 3, &val, &nval);
  ASSERT("cf get after re-encrypt ok", rc == KVSTORE_OK);
  ASSERT("cf value correct after re-encrypt", val && nval == 3 && memcmp(val, "cfv", 3) == 0);
  if( val ){ snkv_free(val); val = NULL; }

  rc = kvstore_cf_put(cf, "cfk2", 4, "cfv2", 4);
  ASSERT("cf put after re-encrypt ok", rc == KVSTORE_OK);
  rc = kvstore_cf_get(cf, "cfk2", 4, &val, &nval);
  ASSERT("cf get new key after re-encrypt ok", rc == KVSTORE_OK);
  ASSERT("cf new value correct after re-encrypt", val && nval == 4 && memcmp(val, "cfv2", 4) == 0);
  if( val ){ snkv_free(val); val = NULL; }
  kvstore_cf_close(cf); cf = NULL;

  /* --- put_if_absent --- */
  int inserted = 0;
  rc = kvstore_put_if_absent(db, "k1", 2, "new", 3, 0, &inserted);
  ASSERT("put_if_absent existing key not inserted", inserted == 0);
  rc = kvstore_get(db, "k1", 2, &val, &nval);
  ASSERT("existing key unchanged", val && nval == 2 && memcmp(val, "v1", 2) == 0);
  if( val ){ snkv_free(val); val = NULL; }

  rc = kvstore_put_if_absent(db, "brand_new", 9, "bv", 2, 0, &inserted);
  ASSERT("put_if_absent new key inserted", inserted == 1);
  rc = kvstore_get(db, "brand_new", 9, &val, &nval);
  ASSERT("put_if_absent new key readable", rc == KVSTORE_OK);
  if( val ){ snkv_free(val); val = NULL; }

  /* --- old password fails, new password works --- */
  kvstore_close(db); db = NULL;

  rc = kvstore_open_encrypted(path, "first", 5, &db, NULL);
  ASSERT("old password fails after re-encrypt", rc == KVSTORE_AUTH_FAILED);
  if( db ){ kvstore_close(db); db = NULL; }

  rc = kvstore_open_encrypted(path, "second", 6, &db, NULL);
  ASSERT("new password works after re-encrypt", rc == KVSTORE_OK);
  rc = kvstore_get(db, "k1", 2, &val, &nval);
  ASSERT("k1 readable on final reopen", rc == KVSTORE_OK);
  ASSERT("k1 value correct on final reopen", val && nval == 2 && memcmp(val, "v1", 2) == 0);
  if( val ){ snkv_free(val); val = NULL; }
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
  test_reencrypt_full_functionality();
  test_remove_encryption_full_functionality();
  test_remove_then_reencrypt_full_functionality();

  printf("\n=== Results: %d passed, %d failed ===\n", passed, failed);
  return failed > 0 ? 1 : 0;
}
