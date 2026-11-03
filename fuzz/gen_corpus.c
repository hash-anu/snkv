/* SPDX-License-Identifier: Apache-2.0 */
/*
** fuzz/gen_corpus.c — write the seed corpus for fuzz_open.
**
** Usage: gen_corpus <dir>
** Builds small real SNKV stores (1 KB pages) and writes each as one seed:
** one mode byte (see fuzz_open.c) followed by the database file.  Seeds
** cover column families, TTL keys (live and expired), overflow values,
** deleted rows (free pages), a DELETE-journal store and an encrypted store.
** Generated at build time so no binary files live in the repository.
*/
#include "kvstore.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *g_dir;

static void rmDb(const char *p){
  char b[600];
  remove(p);
  snprintf(b, sizeof b, "%s-wal", p); remove(b);
  snprintf(b, sizeof b, "%s-shm", p); remove(b);
  snprintf(b, sizeof b, "%s-journal", p); remove(b);
}

static void fill(KVStore *kv, int big){
  char key[32], val[3000];
  KVColumnFamily *users = NULL;
  memset(val, 'v', sizeof val);
  kvstore_begin(kv, 1);
  for(int i = 0; i < 60; i++){
    snprintf(key, sizeof key, "key%03d", i);
    kvstore_put(kv, key, (int)strlen(key), val, big && i % 12 == 0 ? 2500 : 20 + i);
  }
  for(int i = 0; i < 10; i++){
    snprintf(key, sizeof key, "ttl%02d", i);
    int64_t exp = i < 5 ? kvstore_now_ms() - 1000 : kvstore_now_ms() + 3600000;
    kvstore_put_ttl(kv, key, (int)strlen(key), val, 12, exp);
  }
  kvstore_commit(kv);
  if( kvstore_cf_create(kv, "users", &users) == KVSTORE_OK ){
    for(int i = 0; i < 30; i++){
      snprintf(key, sizeof key, "u%02d", i);
      kvstore_cf_put(users, key, (int)strlen(key), val, 40);
    }
    kvstore_cf_put_ttl(users, "u-ttl", 5, val, 8, kvstore_now_ms() + 3600000);
    kvstore_cf_close(users);
  }
  for(int i = 0; i < 60; i += 3){                 /* free pages */
    snprintf(key, sizeof key, "key%03d", i);
    kvstore_delete(kv, key, (int)strlen(key));
  }
}

/* Write <dir>/<name>: mode byte + the database file. */
static void emit(const char *name, int mode, const char *db){
  char out[600];
  snprintf(out, sizeof out, "%s/%s", g_dir, name);
  FILE *in = fopen(db, "rb"), *o = fopen(out, "wb");
  if( !in || !o ){ fprintf(stderr, "gen_corpus: cannot write %s\n", out); exit(1); }
  fputc(mode, o);
  char buf[4096]; size_t n;
  while( (n = fread(buf, 1, sizeof buf, in)) > 0 ) fwrite(buf, 1, n, o);
  fclose(in); fclose(o);
}

static void make(const char *name, int mode, int journal, int enc, int big, int empty){
  char db[600];
  snprintf(db, sizeof db, "%s/gen_tmp.db", g_dir);
  rmDb(db);
  KVStoreConfig c; memset(&c, 0, sizeof c);
  c.journalMode = journal;
  c.pageSize = 1024;
  KVStore *kv = NULL;
  int rc = enc ? kvstore_open_encrypted(db, "fuzz-pw", 7, &kv, &c) : kvstore_open_v2(db, &kv, &c);
  if( rc != KVSTORE_OK ){ fprintf(stderr, "gen_corpus: open %s rc=%d\n", name, rc); exit(1); }
  if( !empty ) fill(kv, big);
  kvstore_close(kv);                              /* checkpoints: all in the main file */
  emit(name, mode, db);
  rmDb(db);
}

int main(int argc, char **argv){
  if( argc != 2 ){ fprintf(stderr, "usage: gen_corpus <dir>\n"); return 1; }
  g_dir = argv[1];
  make("wal_plain",    0, KVSTORE_JOURNAL_WAL,    0, 1, 0);
  make("delete_plain", 1, KVSTORE_JOURNAL_DELETE, 0, 1, 0);
  make("wal_small",    4, KVSTORE_JOURNAL_WAL,    0, 0, 0);
  make("encrypted",    2, KVSTORE_JOURNAL_WAL,    1, 1, 0);
  make("empty",        0, KVSTORE_JOURNAL_WAL,    0, 0, 1);
  printf("gen_corpus: wrote 5 seeds to %s\n", g_dir);
  return 0;
}
