/* SPDX-License-Identifier: Apache-2.0 */
/*
** fuzz/fuzz_open.c — libFuzzer target: open and use a damaged database file.
**
** The input is a database file (libFuzzer mutates the seed corpus that
** fuzz/gen_corpus.c writes: real SNKV stores with column families, TTL keys,
** overflow values, free pages, encryption).  SNKV must open it, or refuse
** it, then read every column family, look up TTLs, purge, write, check
** integrity and close, without crashing, reading out of bounds or leaking.
** Any error code is fine; a crash, sanitizer report or leak is a bug.
**
** The first input byte picks the mode (journal, plain / encrypted open); the
** rest is the file.  Encrypted opens use the password the corpus uses, with
** the Argon2 cost at its minimum (see the Makefile) so each input is fast.
*/
#include "kvstore.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char g_path[256];

static void rmDb(void){
  char b[300];
  remove(g_path);
  snprintf(b, sizeof b, "%s-wal", g_path); remove(b);
  snprintf(b, sizeof b, "%s-shm", g_path); remove(b);
  snprintf(b, sizeof b, "%s-journal", g_path); remove(b);
}

static void useCf(KVColumnFamily *cf){
  KVIterator *it = NULL;
  int n = 0;
  if( kvstore_cf_iterator_create(cf, &it) == KVSTORE_OK ){
    for(kvstore_iterator_first(it); !kvstore_iterator_eof(it) && n < 2000; kvstore_iterator_next(it), n++){
      void *k = NULL, *v = NULL; int nk = 0, nv = 0;
      if( kvstore_iterator_key(it, &k, &nk) != KVSTORE_OK ) break;
      kvstore_iterator_value(it, &v, &nv);
      if( n % 7 == 0 ){                       /* point lookups of real keys */
        void *g = NULL; int ng = 0; int64_t rem = 0;
        char kc[64]; int nkc = nk < (int)sizeof kc ? nk : (int)sizeof kc;
        memcpy(kc, k, (size_t)nkc);
        if( kvstore_cf_get_ttl(cf, kc, nkc, &g, &ng, &rem) == KVSTORE_OK ) snkv_free(g);
      }
    }
    kvstore_iterator_close(it);
  }
  KVIterator *rit = NULL;
  if( kvstore_cf_reverse_iterator_create(cf, &rit) == KVSTORE_OK ){
    n = 0;
    for(kvstore_iterator_last(rit); !kvstore_iterator_eof(rit) && n < 50; kvstore_iterator_prev(rit), n++){
      void *k = NULL; int nk = 0;
      kvstore_iterator_key(rit, &k, &nk);
    }
    kvstore_iterator_close(rit);
  }
  int64_t cnt = 0;
  kvstore_cf_count(cf, &cnt);
  int nDel = 0;
  kvstore_cf_purge_expired(cf, &nDel);
  kvstore_cf_put(cf, "fz", 2, "value", 5);
  kvstore_cf_put_ttl(cf, "fz-ttl", 6, "v", 1, kvstore_now_ms() + 60000);
  kvstore_cf_delete(cf, "fz", 2);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size){
  if( size < 1 ) return 0;
  if( !g_path[0] ){
    const char *d = getenv("FUZZ_DB_DIR");
    snprintf(g_path, sizeof g_path, "%s/fuzz_open.db", d ? d : ".");
  }
  rmDb();
  FILE *f = fopen(g_path, "wb");
  if( !f ) return 0;
  if( size > 1 ) fwrite(data + 1, 1, size - 1, f);
  fclose(f);

  KVStoreConfig c; memset(&c, 0, sizeof c);
  c.journalMode = (data[0] & 1) ? KVSTORE_JOURNAL_DELETE : KVSTORE_JOURNAL_WAL;
  c.syncLevel = KVSTORE_SYNC_OFF;
  c.cacheSize = 32;
  KVStore *kv = NULL;
  int rc = (data[0] & 2) ? kvstore_open_encrypted(g_path, "fuzz-pw", 7, &kv, &c)
                         : kvstore_open_v2(g_path, &kv, &c);
  if( rc != KVSTORE_OK || !kv ){
    if( kv ) kvstore_close(kv);
    rmDb();
    return 0;
  }

  char *zErr = NULL;
  kvstore_integrity_check(kv, &zErr);
  snkv_free(zErr);

  KVColumnFamily *cf = NULL;
  if( kvstore_cf_get_default(kv, &cf) == KVSTORE_OK ){ useCf(cf); kvstore_cf_close(cf); }

  char **azName = NULL; int nName = 0;
  if( kvstore_cf_list(kv, &azName, &nName) == KVSTORE_OK ){
    for(int i = 0; i < nName && i < 16; i++){
      KVColumnFamily *c2 = NULL;
      if( kvstore_cf_open(kv, azName[i], &c2) == KVSTORE_OK ){ useCf(c2); kvstore_cf_close(c2); }
    }
    for(int i = 0; i < nName; i++) snkv_free(azName[i]);
    snkv_free(azName);
  }

  if( data[0] & 4 ) kvstore_incremental_vacuum(kv, 4);
  kvstore_checkpoint(kv, KVSTORE_CHECKPOINT_PASSIVE, NULL, NULL);
  kvstore_close(kv);
  rmDb();
  return 0;
}
