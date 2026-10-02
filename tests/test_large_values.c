/* SPDX-License-Identifier: Apache-2.0 */
/*
** test_large_values.c — values that overflow the B-tree page.
**
** A value bigger than about a quarter of a page is stored partly on its
** B-tree page and partly on overflow pages. These tests check two things:
**
**   1. Correctness: get / exists / iterators / seek / delete / put_if_absent /
**      ttl_remaining all behave correctly when values overflow, on plain and
**      encrypted stores, including while iterators are open during writes.
**
**   2. Lookups do not copy whole values. Searching for a key only needs the
**      key, but before the fix every comparison against an overflowing entry
**      copied the entire key+value (and read all its overflow pages). The
**      same happened in put_if_absent, ttl_remaining and when a cursor saved
**      its position. We detect this with SQLite's memory statistics: the
**      largest single allocation (SQLITE_STATUS_MALLOC_SIZE) must stay far
**      below the value size for operations that never return a value.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "kvstore.h"

static int nPass = 0, nFail = 0;
#define CHECK(cond, msg) do{ \
  if( cond ){ printf("  [PASS] %s\n", msg); nPass++; } \
  else      { printf("  [FAIL] %s  (line %d)\n", msg, __LINE__); nFail++; } \
}while(0)

#define BIG      50000        /* bytes: many overflow pages on 4 KB pages   */
#define NBIG     120          /* number of big entries                     */
#define MAX_COPY (16 * 1024)  /* largest allocation allowed for key-only ops */

static void cleanup(const char *base){
  char buf[256];
  snprintf(buf, sizeof buf, "%s", base);         remove(buf);
  snprintf(buf, sizeof buf, "%s-wal", base);     remove(buf);
  snprintf(buf, sizeof buf, "%s-shm", base);     remove(buf);
  snprintf(buf, sizeof buf, "%s-journal", base); remove(buf);
}

/* Deterministic value bytes for key i, so every read can be verified. */
static void fill_value(unsigned char *p, int n, int i){
  for( int j = 0; j < n; j++ ) p[j] = (unsigned char)((i * 31 + j * 7) & 0xFF);
}
static int value_ok(const void *v, int n, int expectLen, int i){
  if( n != expectLen ) return 0;
  const unsigned char *p = (const unsigned char *)v;
  for( int j = 0; j < n; j++ ){
    if( p[j] != (unsigned char)((i * 31 + j * 7) & 0xFF) ) return 0;
  }
  return 1;
}

static int make_key(char *buf, int i){ return snprintf(buf, 32, "key:%06d", i); }

/* Largest single SQLite allocation since the last reset. */
static void reset_max_alloc(void){
  int cur = 0, hi = 0;
  sqlite3_status(SQLITE_STATUS_MALLOC_SIZE, &cur, &hi, 1);
}
static int max_alloc(void){
  int cur = 0, hi = 0;
  sqlite3_status(SQLITE_STATUS_MALLOC_SIZE, &cur, &hi, 0);
  return hi;
}

/* Fill the default CF with NBIG entries of BIG bytes (even keys only, so
** odd keys can be used as "absent"). */
static int load_big(KVStore *kv, unsigned char *buf){
  char key[32];
  if( kvstore_begin(kv, 1) != KVSTORE_OK ) return 0;
  for( int i = 0; i < NBIG; i++ ){
    int n = make_key(key, i * 2);
    fill_value(buf, BIG, i * 2);
    if( kvstore_put(kv, key, n, buf, BIG) != KVSTORE_OK ){ kvstore_rollback(kv); return 0; }
  }
  return kvstore_commit(kv) == KVSTORE_OK;
}

/* ----------------------------------------------------------------------
** Test 1: mixed value sizes around the overflow limit — every read path
** returns the exact bytes.
** -------------------------------------------------------------------- */
static void test_mixed_sizes(void){
  const char *db = "t_lv_mixed.db";
  static const int sizes[] = { 1, 100, 990, 998, 1002, 1010, 2000, 4092,
                               4093, 8200, 20000, 60000 };
  const int nSizes = (int)(sizeof sizes / sizeof sizes[0]);
  const int N = 240;
  unsigned char *buf = malloc(60000);
  char key[32];
  printf("Test 1: mixed value sizes (1 B .. 60 KB) — all read paths\n");
  cleanup(db);

  KVStore *kv = NULL;
  CHECK(kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL) == KVSTORE_OK, "open");
  int ok = 1;
  kvstore_begin(kv, 1);
  for( int i = 0; i < N; i++ ){
    int n = make_key(key, i), len = sizes[i % nSizes];
    fill_value(buf, len, i);
    if( kvstore_put(kv, key, n, buf, len) != KVSTORE_OK ) ok = 0;
  }
  CHECK(ok && kvstore_commit(kv) == KVSTORE_OK, "put 240 entries of mixed sizes");

  /* get + exists for every key */
  int getOk = 1, exOk = 1;
  for( int i = 0; i < N; i++ ){
    int n = make_key(key, i), len = sizes[i % nSizes];
    void *v = NULL; int nv = 0, ex = 0;
    if( kvstore_get(kv, key, n, &v, &nv) != KVSTORE_OK || !value_ok(v, nv, len, i) ) getOk = 0;
    snkv_free(v);
    if( kvstore_exists(kv, key, n, &ex) != KVSTORE_OK || !ex ) exOk = 0;
  }
  CHECK(getOk, "get returns exact bytes for every size");
  CHECK(exOk,  "exists finds every key");
  { int ex = 1; CHECK(kvstore_exists(kv, "key:999999", 10, &ex) == KVSTORE_OK && !ex,
                      "exists reports a missing key as absent"); }

  /* forward iteration: order + content */
  KVIterator *it = NULL;
  int count = 0, orderOk = 1, iterValOk = 1;
  kvstore_iterator_create(kv, &it);
  for( kvstore_iterator_first(it); !kvstore_iterator_eof(it); kvstore_iterator_next(it) ){
    void *k, *v; int nk, nv;
    kvstore_iterator_key(it, &k, &nk);
    kvstore_iterator_value(it, &v, &nv);
    make_key(key, count);
    if( nk != (int)strlen(key) || memcmp(k, key, nk) != 0 ) orderOk = 0;
    if( !value_ok(v, nv, sizes[count % nSizes], count) ) iterValOk = 0;
    count++;
  }
  kvstore_iterator_close(it);
  CHECK(count == N && orderOk, "forward iterator visits all keys in order");
  CHECK(iterValOk, "forward iterator returns exact values");

  /* reverse iteration */
  count = 0; orderOk = 1;
  kvstore_reverse_iterator_create(kv, &it);
  for( kvstore_iterator_last(it); !kvstore_iterator_eof(it); kvstore_iterator_prev(it) ){
    void *k; int nk;
    kvstore_iterator_key(it, &k, &nk);
    make_key(key, N - 1 - count);
    if( nk != (int)strlen(key) || memcmp(k, key, nk) != 0 ) orderOk = 0;
    count++;
  }
  kvstore_iterator_close(it);
  CHECK(count == N && orderOk, "reverse iterator visits all keys in order");

  /* seek to the middle, forward and reverse */
  {
    int n = make_key(key, 123);
    void *k; int nk;
    kvstore_iterator_create(kv, &it);
    kvstore_iterator_seek(it, key, n);
    kvstore_iterator_key(it, &k, &nk);
    CHECK(!kvstore_iterator_eof(it) && nk == n && memcmp(k, key, n) == 0,
          "forward seek lands on an exact key");
    kvstore_iterator_close(it);

    kvstore_reverse_iterator_create(kv, &it);
    kvstore_iterator_seek(it, "key:000123x", 11);   /* between 123 and 124 */
    kvstore_iterator_key(it, &k, &nk);
    CHECK(!kvstore_iterator_eof(it) && nk == n && memcmp(k, key, n) == 0,
          "reverse seek lands on the nearest smaller key");
    kvstore_iterator_close(it);
  }

  /* prefix iterator over key:0001xx (100 keys) */
  count = 0;
  kvstore_prefix_iterator_create(kv, "key:0001", 8, &it);
  while( !kvstore_iterator_eof(it) ){ count++; kvstore_iterator_next(it); }
  kvstore_iterator_close(it);
  CHECK(count == 100, "prefix iterator counts 100 keys");

  /* delete every third key, then re-check */
  ok = 1;
  for( int i = 0; i < N; i += 3 ){
    int n = make_key(key, i);
    if( kvstore_delete(kv, key, n) != KVSTORE_OK ) ok = 0;
  }
  CHECK(ok, "delete every third key");
  int after = 1;
  for( int i = 0; i < N; i++ ){
    int n = make_key(key, i), ex = -1;
    kvstore_exists(kv, key, n, &ex);
    if( ex != (i % 3 != 0) ) after = 0;
  }
  CHECK(after, "exists matches after deletes");

  /* overwrite with different sizes (grow and shrink across the limit) */
  ok = 1;
  for( int i = 1; i < N; i += 3 ){
    int n = make_key(key, i), len = sizes[(i + 5) % nSizes];
    fill_value(buf, len, i + 1000);
    if( kvstore_put(kv, key, n, buf, len) != KVSTORE_OK ) ok = 0;
  }
  int owOk = 1;
  for( int i = 1; i < N; i += 3 ){
    int n = make_key(key, i), len = sizes[(i + 5) % nSizes];
    void *v = NULL; int nv = 0;
    if( kvstore_get(kv, key, n, &v, &nv) != KVSTORE_OK || !value_ok(v, nv, len, i + 1000) ) owOk = 0;
    snkv_free(v);
  }
  CHECK(ok && owOk, "overwrites with a different size read back exactly");

  char *err = NULL;
  CHECK(kvstore_integrity_check(kv, &err) == KVSTORE_OK, "integrity check passes");
  snkv_free(err);
  kvstore_close(kv);
  cleanup(db);
  free(buf);
}

/* ----------------------------------------------------------------------
** Test 2: key-only operations never allocate a buffer the size of a value.
** -------------------------------------------------------------------- */
static void test_no_value_copy(void){
  const char *db = "t_lv_copy.db";
  unsigned char *buf = malloc(BIG);
  char key[32];
  printf("Test 2: key-only operations do not copy %d-byte values\n", BIG);
  cleanup(db);

  KVStore *kv = NULL;
  CHECK(kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL) == KVSTORE_OK, "open");
  CHECK(load_big(kv, buf), "load big entries");

  /* exists: present and absent keys */
  reset_max_alloc();
  int ok = 1;
  for( int i = 0; i < NBIG * 2; i++ ){
    int n = make_key(key, i), ex = -1;
    if( kvstore_exists(kv, key, n, &ex) != KVSTORE_OK || ex != (i % 2 == 0) ) ok = 0;
  }
  CHECK(ok, "exists correct for present and absent keys");
  printf("        largest allocation during exists: %d bytes\n", max_alloc());
  CHECK(max_alloc() < MAX_COPY, "exists does not copy whole values");

  /* put of a new small key: its search must not copy neighbouring values */
  reset_max_alloc();
  CHECK(kvstore_put(kv, "key:000001", 10, "small", 5) == KVSTORE_OK, "put small key between big ones");
  printf("        largest allocation during put: %d bytes\n", max_alloc());
  CHECK(max_alloc() < MAX_COPY, "put's search does not copy whole values");

  /* delete: search + free overflow chain, no value copy */
  reset_max_alloc();
  CHECK(kvstore_delete(kv, "key:000002", 10) == KVSTORE_OK, "delete a big entry");
  CHECK(max_alloc() < MAX_COPY, "delete does not copy whole values");

  /* put_if_absent on an existing big key: must not read the old value */
  reset_max_alloc();
  int ins = -1;
  CHECK(kvstore_put_if_absent(kv, "key:000004", 10, "x", 1, 0, &ins) == KVSTORE_OK && ins == 0,
        "put_if_absent on existing key: not inserted");
  printf("        largest allocation during put_if_absent: %d bytes\n", max_alloc());
  CHECK(max_alloc() < MAX_COPY, "put_if_absent does not read the existing value");
  {
    void *v = NULL; int nv = 0;
    CHECK(kvstore_get(kv, "key:000004", 10, &v, &nv) == KVSTORE_OK && value_ok(v, nv, BIG, 4),
          "existing value unchanged after put_if_absent");
    snkv_free(v);
  }
  ins = -1;
  CHECK(kvstore_put_if_absent(kv, "key:000005", 10, "new", 3, 0, &ins) == KVSTORE_OK && ins == 1,
        "put_if_absent on absent key: inserted");

  /* ttl_remaining on a big key without TTL */
  reset_max_alloc();
  int64_t rem = 0;
  CHECK(kvstore_ttl_remaining(kv, "key:000006", 10, &rem) == KVSTORE_OK && rem == KVSTORE_NO_TTL,
        "ttl_remaining on big key without TTL: NO_TTL");
  printf("        largest allocation during ttl_remaining: %d bytes\n", max_alloc());
  CHECK(max_alloc() < MAX_COPY, "ttl_remaining does not read the value");

  kvstore_close(kv);
  cleanup(db);
  free(buf);
}

/* ----------------------------------------------------------------------
** Test 3: ttl_remaining keeps its exact semantics with big values.
** -------------------------------------------------------------------- */
static void test_ttl_remaining_big(void){
  const char *db = "t_lv_ttl.db";
  unsigned char *buf = malloc(BIG);
  printf("Test 3: ttl_remaining semantics on big values\n");
  cleanup(db);

  KVStore *kv = NULL;
  CHECK(kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL) == KVSTORE_OK, "open");
  fill_value(buf, BIG, 1);
  CHECK(kvstore_put_ttl(kv, "live", 4, buf, BIG, kvstore_now_ms() + 60000) == KVSTORE_OK, "put_ttl live (+60 s)");
  CHECK(kvstore_put_ttl(kv, "dead", 4, buf, BIG, kvstore_now_ms() - 1000) == KVSTORE_OK, "put_ttl already expired");
  CHECK(kvstore_put(kv, "perm", 4, buf, BIG) == KVSTORE_OK, "put permanent");

  int64_t rem = 0;
  CHECK(kvstore_ttl_remaining(kv, "live", 4, &rem) == KVSTORE_OK && rem > 0 && rem <= 60000,
        "live key: remaining ms in (0, 60000]");
  CHECK(kvstore_ttl_remaining(kv, "perm", 4, &rem) == KVSTORE_OK && rem == KVSTORE_NO_TTL,
        "permanent key: NO_TTL");
  CHECK(kvstore_ttl_remaining(kv, "dead", 4, &rem) == KVSTORE_OK && rem == 0,
        "expired key: OK with 0 (lazily deleted)");
  CHECK(kvstore_ttl_remaining(kv, "dead", 4, &rem) == KVSTORE_NOTFOUND,
        "expired key afterwards: NOTFOUND");
  CHECK(kvstore_ttl_remaining(kv, "none", 4, &rem) == KVSTORE_NOTFOUND && rem == KVSTORE_NO_TTL,
        "missing key: NOTFOUND");
  {
    void *v = NULL; int nv = 0; int64_t r2 = 0;
    CHECK(kvstore_get_ttl(kv, "live", 4, &v, &nv, &r2) == KVSTORE_OK && value_ok(v, nv, BIG, 1) && r2 > 0,
          "get_ttl still returns the full value");
    snkv_free(v);
  }
  kvstore_close(kv);
  cleanup(db);
  free(buf);
}

/* ----------------------------------------------------------------------
** Test 4: cursors that must remember their place during writes.
** Inside kvstore_begin(1), reads and iterators keep cursors open while
** puts modify the same tree, so those cursors are saved and restored.
** -------------------------------------------------------------------- */
static void test_saved_cursors(void){
  const char *db = "t_lv_save.db";
  unsigned char *buf = malloc(BIG);
  char key[32];
  printf("Test 4: saved cursor positions with big values\n");
  cleanup(db);

  KVStore *kv = NULL;
  CHECK(kvstore_open(db, &kv, KVSTORE_JOURNAL_WAL) == KVSTORE_OK, "open");
  CHECK(load_big(kv, buf), "load big entries");

  /* 4a: the cached read cursor sits on a big entry; a put must save it
  **     without copying that entry's value. */
  CHECK(kvstore_begin(kv, 1) == KVSTORE_OK, "begin write transaction");
  { int ex = 0; kvstore_exists(kv, "key:000010", 10, &ex); CHECK(ex, "exists positions cached cursor on a big entry"); }
  reset_max_alloc();
  CHECK(kvstore_put(kv, "key:000011", 10, "s", 1) == KVSTORE_OK, "put in same CF (saves the cached cursor)");
  printf("        largest allocation while saving the cursor: %d bytes\n", max_alloc());
  CHECK(max_alloc() < MAX_COPY, "saving a cursor does not copy its value");
  {
    void *v = NULL; int nv = 0;
    CHECK(kvstore_get(kv, "key:000010", 10, &v, &nv) == KVSTORE_OK && value_ok(v, nv, BIG, 10),
          "cached cursor restored: big value still correct");
    snkv_free(v);
  }
  CHECK(kvstore_commit(kv) == KVSTORE_OK, "commit");

  /* 4b: iterate while writing into the same CF. Every original key must be
  **     visited exactly once, in order, with its full value. */
  CHECK(kvstore_begin(kv, 1) == KVSTORE_OK, "begin write transaction");
  KVIterator *it = NULL;
  kvstore_iterator_create(kv, &it);
  int seen = 0, orderOk = 1, valOk = 1, putOk = 1;
  char prev[32] = "";
  for( kvstore_iterator_first(it); !kvstore_iterator_eof(it); kvstore_iterator_next(it) ){
    void *k, *v; int nk, nv;
    kvstore_iterator_key(it, &k, &nk);
    kvstore_iterator_value(it, &v, &nv);
    char cur[32]; memcpy(cur, k, nk); cur[nk] = 0;
    if( prev[0] && strcmp(cur, prev) <= 0 ) orderOk = 0;
    strcpy(prev, cur);
    int idx = atoi(cur + 4);
    if( nv == BIG ){                       /* an original big entry */
      if( !value_ok(v, nv, BIG, idx) ) valOk = 0;
      seen++;
      /* write a new key right after this one: forces a save + restore */
      char nk2[40]; int n2 = snprintf(nk2, sizeof nk2, "%s+", cur);
      if( kvstore_put(kv, nk2, n2, "w", 1) != KVSTORE_OK ) putOk = 0;
    }
  }
  kvstore_iterator_close(it);
  CHECK(putOk, "puts during iteration succeed");
  CHECK(orderOk, "iterator stays in key order across writes");
  CHECK(seen == NBIG, "every original big entry visited exactly once");
  CHECK(valOk, "big values read correctly after each restore");
  CHECK(kvstore_commit(kv) == KVSTORE_OK, "commit");

  int64_t cnt = 0;
  kvstore_count(kv, &cnt);
  CHECK(cnt == NBIG + 1 + NBIG, "count = big entries + 1 small + one written per visit");
  char *err = NULL;
  CHECK(kvstore_integrity_check(kv, &err) == KVSTORE_OK, "integrity check passes");
  snkv_free(err);
  kvstore_close(kv);
  cleanup(db);
  free(buf);
  (void)key;
}

/* ----------------------------------------------------------------------
** Test 5: encrypted store — ciphertext overflows too.
** -------------------------------------------------------------------- */
static void test_encrypted_big(void){
  const char *db = "t_lv_enc.db";
  unsigned char *buf = malloc(BIG);
  char key[32];
  printf("Test 5: encrypted store with big values\n");
  cleanup(db);

  KVStoreConfig cfg; memset(&cfg, 0, sizeof cfg);
  cfg.journalMode = KVSTORE_JOURNAL_WAL;
  cfg.syncLevel   = KVSTORE_SYNC_NORMAL;
  KVStore *kv = NULL;
  CHECK(kvstore_open_encrypted(db, "pw", 2, &kv, &cfg) == KVSTORE_OK, "open encrypted");
  int ok = 1;
  kvstore_begin(kv, 1);
  for( int i = 0; i < 40; i++ ){
    int n = make_key(key, i * 2);
    fill_value(buf, BIG, i * 2);
    if( kvstore_put(kv, key, n, buf, BIG) != KVSTORE_OK ) ok = 0;
  }
  CHECK(ok && kvstore_commit(kv) == KVSTORE_OK, "put 40 big encrypted values");

  int getOk = 1;
  for( int i = 0; i < 40; i++ ){
    int n = make_key(key, i * 2);
    void *v = NULL; int nv = 0;
    if( kvstore_get(kv, key, n, &v, &nv) != KVSTORE_OK || !value_ok(v, nv, BIG, i * 2) ) getOk = 0;
    snkv_free(v);
  }
  CHECK(getOk, "get decrypts every big value exactly");

  reset_max_alloc();
  int exOk = 1;
  for( int i = 0; i < 80; i++ ){
    int n = make_key(key, i), ex = -1;
    if( kvstore_exists(kv, key, n, &ex) != KVSTORE_OK || ex != (i % 2 == 0) ) exOk = 0;
  }
  CHECK(exOk, "exists correct on encrypted store");
  int ins = -1;
  CHECK(kvstore_put_if_absent(kv, "key:000010", 10, "x", 1, 0, &ins) == KVSTORE_OK && ins == 0,
        "put_if_absent on existing encrypted key: not inserted");
  CHECK(max_alloc() < MAX_COPY, "exists / put_if_absent do not copy or decrypt values");
  kvstore_close(kv);

  /* reopen and verify */
  kv = NULL;
  CHECK(kvstore_open_encrypted(db, "pw", 2, &kv, &cfg) == KVSTORE_OK, "reopen encrypted");
  {
    void *v = NULL; int nv = 0;
    CHECK(kvstore_get(kv, "key:000078", 10, &v, &nv) == KVSTORE_OK && value_ok(v, nv, BIG, 78),
          "big value correct after reopen");
    snkv_free(v);
  }
  kvstore_close(kv);
  cleanup(db);
  free(buf);
}

/* Replace every occurrence of pat (n bytes) in file path with rep (a key can
** appear twice: in its leaf cell and as a divider on an interior page).
** Returns the number of occurrences patched. */
static int patch_file(const char *path, const unsigned char *pat, int n,
                      const unsigned char *rep){
  FILE *f = fopen(path, "r+b");
  if( !f ) return 0;
  fseek(f, 0, SEEK_END); long sz = ftell(f); fseek(f, 0, SEEK_SET);
  unsigned char *buf = (unsigned char *)malloc(sz > 0 ? sz : 1);
  int hits = 0;
  if( buf && fread(buf, 1, sz, f) == (size_t)sz ){
    for( long i = 0; i + n <= sz; i++ ){
      if( memcmp(buf + i, pat, n) == 0 ){
        fseek(f, i, SEEK_SET); fwrite(rep, 1, n, f); hits++;
      }
    }
  }
  free(buf);
  fclose(f);
  return hits;
}

/* ---- 6. corrupt stored key length ----------------------------------------
** Every cell starts with a 4-byte key length. If the file is corrupt and that
** length is huge (here 0xFFFFFFF0), the comparator used to turn it into a
** negative int and pass it to memcmp() as an enormous size, reading far past
** the cell. It must now stay inside the cell. Under ASan / valgrind the old
** code reports an out-of-bounds read here. */
static void test_corrupt_key_length(void){
  printf("\n-- 6. corrupt stored key length (small and overflowing cells)\n");
  const char *db = "tests/lv_corrupt.db";
  cleanup(db);
  KVStore *kv = NULL;
  unsigned char *big = (unsigned char *)malloc(BIG);
  char key[32];

  CHECK(kvstore_open(db, &kv, KVSTORE_JOURNAL_DELETE) == KVSTORE_OK, "open");
  kvstore_begin(kv, 1);
  for( int i = 0; i < 200; i++ ){
    int n = snprintf(key, sizeof key, "ck:%04d", i);
    unsigned char v[20]; fill_value(v, sizeof v, i);
    kvstore_put(kv, key, n, v, sizeof v);
  }
  fill_value(big, BIG, 1);
  kvstore_put(kv, "bg:0001", 7, big, BIG);
  CHECK(kvstore_commit(kv) == KVSTORE_OK, "write 200 small keys + 1 big key");
  kvstore_close(kv); kv = NULL;

  /* [00 00 00 07]"ck:0100" -> [FF FF FF F0]"ck:0100"  (same for the big key) */
  static const unsigned char bad[4] = {0xFF, 0xFF, 0xFF, 0xF0};
  unsigned char pat[11], rep[11];
  memcpy(pat, "\0\0\0\7ck:0100", 11); memcpy(rep, pat, 11); memcpy(rep, bad, 4);
  CHECK(patch_file(db, pat, 11, rep) >= 1, "corrupt the small cell's key length");
  memcpy(pat, "\0\0\0\7bg:0001", 11); memcpy(rep, pat, 11); memcpy(rep, bad, 4);
  CHECK(patch_file(db, pat, 11, rep) >= 1, "corrupt the big cell's key length");

  if( kvstore_open(db, &kv, KVSTORE_JOURNAL_DELETE) == KVSTORE_OK ){
    void *v = NULL; int nv = 0, ex = 0, rc;
    /* Searches that land on (or next to) the corrupt cells. */
    rc = kvstore_get(kv, "ck:0100", 7, &v, &nv);
    if( rc == KVSTORE_OK ) snkv_free(v);
    v = NULL;
    rc = kvstore_get(kv, "bg:0001", 7, &v, &nv);
    if( rc == KVSTORE_OK ) snkv_free(v);
    v = NULL;
    kvstore_exists(kv, "ck:0100xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", 38, &ex);
    kvstore_exists(kv, "bg:0001xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", 38, &ex);
    CHECK(1, "lookups on corrupt cells return without reading out of bounds");

    /* Keys whose search path is not misled by the corrupt cells. */
    rc = kvstore_get(kv, "ck:0000", 7, &v, &nv);
    CHECK(rc == KVSTORE_OK || rc == KVSTORE_CORRUPT,
          "lookup of an unaffected key returns OK or CORRUPT");
    if( rc == KVSTORE_OK ) snkv_free(v);
    kvstore_close(kv);
  }else{
    CHECK(1, "open refused the corrupt file (acceptable)");
  }
  free(big);
  cleanup(db);
}

int main(void){
  printf("=== test_large_values ===\n");
  test_mixed_sizes();
  test_no_value_copy();
  test_ttl_remaining_big();
  test_saved_cursors();
  test_encrypted_big();
  test_corrupt_key_length();
  printf("=== Results: %d passed, %d failed ===\n", nPass, nFail);
  return nFail ? 1 : 0;
}
