/* SPDX-License-Identifier: Apache-2.0 */
/*
** fuzz/fuzz_api.c — libFuzzer target: random API sequences vs. a model.
**
** Each input is read as a sequence of operations on two column families
** (default + "cf1"): put / delete / get / exists / put_if_absent / put_ttl,
** explicit transactions (commit, rollback), forward / reverse / prefix
** scans, seek, writes while an iterator is open, cf_clear, count,
** checkpoint, and close + reopen.  The same operations run on a simple
** in-memory model; any difference, and any unexpected error code, aborts
** (libFuzzer reports it with the input that caused it).  At the end of each
** input the store is closed, reopened and compared again, and must pass
** kvstore_integrity_check.
**
** Keys are 1-5 bytes from a tiny alphabet (so they collide, share prefixes
** and include 0x00 / 0xff); values are 0-63 bytes or overflow-page sized.
**
** Build and run: make fuzz   (needs clang with libFuzzer)
*/
#include "kvstore.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FAIL(...) do{ fprintf(stderr, "fuzz_api: " __VA_ARGS__); fputc('\n', stderr); abort(); }while(0)

/* ------------------------------------------------------------- input */
typedef struct In { const uint8_t *p; size_t n, i; } In;
static int inByte(In *in){ return in->i < in->n ? in->p[in->i++] : -1; }
static int inEnd(In *in){ return in->i >= in->n; }

/* ------------------------------------------------------------- model */
#define MAXK  5
#define MAXE  20000            /* 400 ops x up to 20 writes each */
typedef struct Ent { uint8_t k[MAXK]; int nk; uint8_t *v; int nv; int ttl; } Ent;
typedef struct Model { Ent e[2][MAXE]; int n[2]; } Model;

static int keyCmp(const uint8_t *a, int na, const uint8_t *b, int nb){
  int c = memcmp(a, b, (size_t)(na < nb ? na : nb));
  return c ? c : na - nb;
}
static int mFind(Model *m, int cf, const uint8_t *k, int nk){
  for(int i = 0; i < m->n[cf]; i++){
    if( keyCmp(m->e[cf][i].k, m->e[cf][i].nk, k, nk) == 0 ) return i;
  }
  return -1;
}
static void mPut(Model *m, int cf, const uint8_t *k, int nk, const uint8_t *v, int nv, int ttl){
  int i = mFind(m, cf, k, nk);
  if( i < 0 ){
    if( m->n[cf] >= MAXE ) FAIL("model full");
    i = m->n[cf]++;
    memcpy(m->e[cf][i].k, k, (size_t)nk); m->e[cf][i].nk = nk;
  }else{
    free(m->e[cf][i].v);
  }
  m->e[cf][i].v = (uint8_t*)malloc(nv ? (size_t)nv : 1);
  if( nv ) memcpy(m->e[cf][i].v, v, (size_t)nv);
  m->e[cf][i].nv = nv;
  m->e[cf][i].ttl = ttl;
}
static void mDel(Model *m, int cf, const uint8_t *k, int nk){
  int i = mFind(m, cf, k, nk);
  if( i < 0 ) return;
  free(m->e[cf][i].v);
  m->e[cf][i] = m->e[cf][--m->n[cf]];
}
static void mClear(Model *m, int cf){
  for(int i = 0; i < m->n[cf]; i++) free(m->e[cf][i].v);
  m->n[cf] = 0;
}
static void mFree(Model *m){ mClear(m, 0); mClear(m, 1); }
static void mCopy(Model *dst, const Model *src){
  for(int cf = 0; cf < 2; cf++){
    dst->n[cf] = src->n[cf];
    for(int i = 0; i < src->n[cf]; i++){
      dst->e[cf][i] = src->e[cf][i];
      dst->e[cf][i].v = (uint8_t*)malloc(src->e[cf][i].nv ? (size_t)src->e[cf][i].nv : 1);
      if( src->e[cf][i].nv ) memcpy(dst->e[cf][i].v, src->e[cf][i].v, (size_t)src->e[cf][i].nv);
    }
  }
}
static int entCmp(const void *a, const void *b){
  const Ent *x = (const Ent*)a, *y = (const Ent*)b;
  return keyCmp(x->k, x->nk, y->k, y->nk);
}
static void mSort(Model *m, int cf){ qsort(m->e[cf], (size_t)m->n[cf], sizeof(Ent), entCmp); }

/* ---------------------------------------------------- input decoding */
static const uint8_t kAlpha[6] = { 'a', 'b', 'c', 'd', 0x00, 0xff };
static int readKey(In *in, uint8_t *k){
  int b = inByte(in); if( b < 0 ) b = 0;
  int nk = 1 + b % MAXK;
  for(int i = 0; i < nk; i++){ int c = inByte(in); k[i] = kAlpha[(c < 0 ? 0 : c) % 6]; }
  return nk;
}
static uint8_t g_vbuf[4200];
static int readVal(In *in){
  int b = inByte(in); if( b < 0 ) b = 0;
  int nv = b < 200 ? b % 64 : 1000 + (b - 200) * 60;     /* up to ~4300 */
  if( nv > (int)sizeof g_vbuf ) nv = (int)sizeof g_vbuf;
  int s = inByte(in); if( s < 0 ) s = 7;
  for(int i = 0; i < nv; i++) g_vbuf[i] = (uint8_t)(s + i * 31);
  return nv;
}

/* ----------------------------------------------------------- checks */
#define TTL_FAR ((int64_t)4102444800000LL)       /* year 2100 */

static void checkScan(KVColumnFamily *cf, Model *m, int ci, int reverse,
                      const uint8_t *pfx, int npfx){
  KVIterator *it = NULL;
  int rc;
  if( npfx ){
    rc = reverse ? kvstore_cf_reverse_prefix_iterator_create(cf, pfx, npfx, &it)
                 : kvstore_cf_prefix_iterator_create(cf, pfx, npfx, &it);
  }else{
    rc = reverse ? kvstore_cf_reverse_iterator_create(cf, &it)
                 : kvstore_cf_iterator_create(cf, &it);
    if( rc == KVSTORE_OK ) rc = reverse ? kvstore_iterator_last(it) : kvstore_iterator_first(it);
  }
  if( rc != KVSTORE_OK ) FAIL("iterator create/first rc=%d", rc);
  mSort(m, ci);
  int want = reverse ? m->n[ci] - 1 : 0, step = reverse ? -1 : 1;
  for(; !kvstore_iterator_eof(it); rc = reverse ? kvstore_iterator_prev(it) : kvstore_iterator_next(it)){
    if( rc != KVSTORE_OK ) FAIL("iterator step rc=%d", rc);
    void *k = NULL, *v = NULL; int nk = 0, nv = 0;
    if( kvstore_iterator_key(it, &k, &nk) != KVSTORE_OK ) FAIL("iterator key");
    if( kvstore_iterator_value(it, &v, &nv) != KVSTORE_OK ) FAIL("iterator value");
    while( want >= 0 && want < m->n[ci] && npfx
        && (m->e[ci][want].nk < npfx || memcmp(m->e[ci][want].k, pfx, (size_t)npfx) != 0) ) want += step;
    if( want < 0 || want >= m->n[ci] ) FAIL("scan returned an extra key (cf %d)", ci);
    Ent *e = &m->e[ci][want];
    if( keyCmp(k, nk, e->k, e->nk) != 0 ) FAIL("scan key mismatch (cf %d)", ci);
    if( nv != e->nv || (nv && memcmp(v, e->v, (size_t)nv) != 0) ) FAIL("scan value mismatch (cf %d)", ci);
    want += step;
  }
  while( want >= 0 && want < m->n[ci] && npfx
      && (m->e[ci][want].nk < npfx || memcmp(m->e[ci][want].k, pfx, (size_t)npfx) != 0) ) want += step;
  if( want >= 0 && want < m->n[ci] ) FAIL("scan missed a key (cf %d)", ci);
  kvstore_iterator_close(it);
}

static void checkAll(KVColumnFamily *cfs[2], Model *m){
  for(int ci = 0; ci < 2; ci++){
    checkScan(cfs[ci], m, ci, 0, NULL, 0);
    int64_t cnt = -1;
    if( kvstore_cf_count(cfs[ci], &cnt) != KVSTORE_OK || cnt != m->n[ci] ) FAIL("count %lld != %d", (long long)cnt, m->n[ci]);
    for(int i = 0; i < m->n[ci]; i++){
      Ent *e = &m->e[ci][i];
      void *v = NULL; int nv = 0; int64_t rem = 0;
      int rc = kvstore_cf_get_ttl(cfs[ci], e->k, e->nk, &v, &nv, &rem);
      if( rc != KVSTORE_OK ) FAIL("get_ttl of a present key rc=%d", rc);
      if( nv != e->nv || (nv && memcmp(v, e->v, (size_t)nv) != 0) ) FAIL("get_ttl value mismatch");
      if( e->ttl ? rem <= 0 : rem != KVSTORE_NO_TTL ) FAIL("ttl flag mismatch (rem %lld, want ttl %d)", (long long)rem, e->ttl);
      snkv_free(v);
    }
  }
}

/* ------------------------------------------------------------- store */
static char g_path[256];
static void rmDb(void){
  char b[300];
  remove(g_path);
  snprintf(b, sizeof b, "%s-wal", g_path); remove(b);
  snprintf(b, sizeof b, "%s-shm", g_path); remove(b);
  snprintf(b, sizeof b, "%s-journal", g_path); remove(b);
}
static KVStore *openStore(int journal, KVColumnFamily *cfs[2]){
  KVStoreConfig c; memset(&c, 0, sizeof c);
  c.journalMode = journal;
  c.syncLevel = KVSTORE_SYNC_OFF;              /* speed: durability is test_powerloss's job */
  c.pageSize = 1024;
  c.cacheSize = 16;
  c.walAutoCheckpoint = 8;
  KVStore *kv = NULL;
  int rc = kvstore_open_v2(g_path, &kv, &c);
  if( rc != KVSTORE_OK ) FAIL("open rc=%d", rc);
  if( kvstore_cf_get_default(kv, &cfs[0]) != KVSTORE_OK ) FAIL("get default cf");
  rc = kvstore_cf_open(kv, "cf1", &cfs[1]);
  if( rc == KVSTORE_NOTFOUND ) rc = kvstore_cf_create(kv, "cf1", &cfs[1]);
  if( rc != KVSTORE_OK ) FAIL("cf1 rc=%d", rc);
  return kv;
}
static void closeStore(KVStore *kv, KVColumnFamily *cfs[2]){
  kvstore_cf_close(cfs[1]);
  kvstore_cf_close(cfs[0]);
  if( kvstore_close(kv) != KVSTORE_OK ) FAIL("close");
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size){
  if( !g_path[0] ){
    const char *d = getenv("FUZZ_DB_DIR");
    snprintf(g_path, sizeof g_path, "%s/fuzz_api.db", d ? d : ".");
  }
  In in = { data, size, 0 };
  static Model m, snap;
  KVColumnFamily *cfs[2];
  int journal = (size && (data[0] & 1)) ? KVSTORE_JOURNAL_DELETE : KVSTORE_JOURNAL_WAL;
  int inTxn = 0;
  uint8_t k[MAXK];

  rmDb();
  memset(&m, 0, sizeof m);
  KVStore *kv = openStore(journal, cfs);
  inByte(&in);                                   /* the journal byte */

  static int trace = -1;
  if( trace < 0 ) trace = getenv("FUZZ_TRACE") != NULL;   /* print each op */
  for(int nOps = 0; !inEnd(&in) && nOps < 400; nOps++){
    int op = inByte(&in) % 19;
    int ci = inByte(&in) & 1;
    if( trace ) fprintf(stderr, "op %2d cf %d (in txn %d)\n", op, ci, inTxn);
    KVColumnFamily *cf = cfs[ci];
    int nk, nv, rc, ex;
    void *v = NULL;
    switch( op ){
      case 0: case 1: case 2:                    /* put (common) */
        nk = readKey(&in, k); nv = readVal(&in);
        if( kvstore_cf_put(cf, k, nk, g_vbuf, nv) != KVSTORE_OK ) FAIL("put");
        mPut(&m, ci, k, nk, g_vbuf, nv, 0);
        break;
      case 3:                                    /* delete */
        nk = readKey(&in, k);
        rc = kvstore_cf_delete(cf, k, nk);
        if( rc != (mFind(&m, ci, k, nk) >= 0 ? KVSTORE_OK : KVSTORE_NOTFOUND) ) FAIL("delete rc=%d", rc);
        mDel(&m, ci, k, nk);
        break;
      case 4: {                                  /* get */
        nk = readKey(&in, k);
        int i = mFind(&m, ci, k, nk);
        rc = kvstore_cf_get(cf, k, nk, &v, &nv);
        if( i < 0 ){ if( rc != KVSTORE_NOTFOUND ) FAIL("get of absent key rc=%d", rc); }
        else if( rc != KVSTORE_OK || nv != m.e[ci][i].nv
              || (nv && memcmp(v, m.e[ci][i].v, (size_t)nv) != 0) ) FAIL("get mismatch rc=%d", rc);
        snkv_free(v);
        break;
      }
      case 5:                                    /* exists */
        nk = readKey(&in, k);
        if( kvstore_cf_exists(cf, k, nk, &ex) != KVSTORE_OK || ex != (mFind(&m, ci, k, nk) >= 0) ) FAIL("exists");
        break;
      case 6: {                                  /* put_if_absent */
        nk = readKey(&in, k); nv = readVal(&in);
        int ins = -1, had = mFind(&m, ci, k, nk) >= 0;
        if( kvstore_cf_put_if_absent(cf, k, nk, g_vbuf, nv, 0, &ins) != KVSTORE_OK || ins != !had ) FAIL("put_if_absent");
        if( !had ) mPut(&m, ci, k, nk, g_vbuf, nv, 0);
        break;
      }
      case 7:                                    /* put with a TTL far in the future */
        nk = readKey(&in, k); nv = readVal(&in);
        if( kvstore_cf_put_ttl(cf, k, nk, g_vbuf, nv, TTL_FAR) != KVSTORE_OK ) FAIL("put_ttl");
        mPut(&m, ci, k, nk, g_vbuf, nv, 1);
        break;
      case 8:                                    /* begin */
        rc = kvstore_begin(kv, 1);
        if( inTxn ){ if( rc == KVSTORE_OK ) FAIL("nested begin accepted"); }
        else{ if( rc != KVSTORE_OK ) FAIL("begin rc=%d", rc); inTxn = 1; mFree(&snap); memset(&snap, 0, sizeof snap); mCopy(&snap, &m); }
        break;
      case 9:                                    /* commit */
        rc = kvstore_commit(kv);
        if( inTxn && rc != KVSTORE_OK ) FAIL("commit rc=%d", rc);
        inTxn = 0;
        break;
      case 10:                                   /* rollback */
        if( kvstore_rollback(kv) != KVSTORE_OK ) FAIL("rollback");
        if( inTxn ){ mFree(&m); memset(&m, 0, sizeof m); mCopy(&m, &snap); inTxn = 0; }
        break;
      case 11:                                   /* forward / reverse scan */
        checkScan(cf, &m, ci, inByte(&in) & 1, NULL, 0);
        break;
      case 12: {                                 /* prefix scan, both directions */
        uint8_t p[MAXK]; int np = readKey(&in, p);
        if( np > 2 ) np = 2;
        checkScan(cf, &m, ci, inByte(&in) & 1, p, np);
        break;
      }
      case 13: {                                 /* seek: first key >= target */
        nk = readKey(&in, k);
        KVIterator *it = NULL;
        if( kvstore_cf_iterator_create(cf, &it) != KVSTORE_OK ) FAIL("iter create");
        if( kvstore_iterator_seek(it, k, nk) != KVSTORE_OK ) FAIL("seek");
        mSort(&m, ci);
        int want = -1;
        for(int i = 0; i < m.n[ci]; i++){ if( keyCmp(m.e[ci][i].k, m.e[ci][i].nk, k, nk) >= 0 ){ want = i; break; } }
        if( want < 0 ){ if( !kvstore_iterator_eof(it) ) FAIL("seek past the end not eof"); }
        else{
          void *kk = NULL; int nkk = 0;
          if( kvstore_iterator_eof(it) || kvstore_iterator_key(it, &kk, &nkk) != KVSTORE_OK
           || keyCmp(kk, nkk, m.e[ci][want].k, m.e[ci][want].nk) != 0 ) FAIL("seek landed on the wrong key");
        }
        kvstore_iterator_close(it);
        break;
      }
      case 14: {                                 /* writes while an iterator is open */
        KVIterator *it = NULL;
        if( kvstore_cf_iterator_create(cf, &it) != KVSTORE_OK || kvstore_iterator_first(it) != KVSTORE_OK ) FAIL("iter");
        int steps = inByte(&in) % 4;
        for(int s = 0; s < steps && !kvstore_iterator_eof(it); s++) kvstore_iterator_next(it);
        int nw = 1 + inByte(&in) % 20;           /* enough to split pages */
        for(int w = 0; w < nw; w++){
          nk = readKey(&in, k); nv = readVal(&in);
          if( inByte(&in) & 1 ){ kvstore_cf_delete(cf, k, nk); mDel(&m, ci, k, nk); }
          else{ if( kvstore_cf_put(cf, k, nk, g_vbuf, nv) != KVSTORE_OK ) FAIL("put during scan"); mPut(&m, ci, k, nk, g_vbuf, nv, 0); }
        }
        /* The scan must carry on without errors, in key order. */
        uint8_t prev[MAXK]; int nprev = -1;
        for(int s = 0; s < 300 && !kvstore_iterator_eof(it); s++){
          void *kk = NULL, *vv = NULL; int nkk = 0, nvv = 0;
          rc = kvstore_iterator_key(it, &kk, &nkk);
          if( rc != KVSTORE_OK && rc != KVSTORE_NOTFOUND ) FAIL("key during scan rc=%d", rc);
          rc = kvstore_iterator_value(it, &vv, &nvv);
          if( rc != KVSTORE_OK && rc != KVSTORE_NOTFOUND ) FAIL("value during scan rc=%d", rc);
          if( kvstore_iterator_key(it, &kk, &nkk) == KVSTORE_OK ){
            if( nprev >= 0 && keyCmp(kk, nkk, prev, nprev) <= 0 ) FAIL("scan went backwards after a write");
            if( nkk <= MAXK ){ memcpy(prev, kk, (size_t)nkk); nprev = nkk; }
          }
          rc = kvstore_iterator_next(it);
          if( rc != KVSTORE_OK ) FAIL("next during scan rc=%d", rc);
        }
        kvstore_iterator_close(it);
        break;
      }
      case 15:                                   /* clear the column family */
        rc = kvstore_cf_clear(cf);
        if( rc != KVSTORE_OK ) FAIL("cf_clear rc=%d (cf %d, in txn %d): %s", rc, ci, inTxn, kvstore_errmsg(kv));
        mClear(&m, ci);
        break;
      case 16: {                                 /* count */
        int64_t cnt = -1;
        if( kvstore_cf_count(cf, &cnt) != KVSTORE_OK || cnt != m.n[ci] ) FAIL("count");
        break;
      }
      case 17:                                   /* checkpoint (outside txns) */
        if( !inTxn && kvstore_checkpoint(kv, KVSTORE_CHECKPOINT_PASSIVE, NULL, NULL) != KVSTORE_OK
         && journal == KVSTORE_JOURNAL_WAL ) FAIL("checkpoint");
        break;
      case 18:                                   /* close and reopen */
        if( inTxn ){ kvstore_rollback(kv); mFree(&m); memset(&m, 0, sizeof m); mCopy(&m, &snap); inTxn = 0; }
        closeStore(kv, cfs);
        kv = openStore(journal, cfs);
        checkAll(cfs, &m);
        break;
    }
  }

  if( inTxn ){ kvstore_rollback(kv); mFree(&m); memset(&m, 0, sizeof m); mCopy(&m, &snap); }
  checkAll(cfs, &m);
  closeStore(kv, cfs);
  kv = openStore(journal, cfs);                  /* everything committed persists */
  checkAll(cfs, &m);
  char *zErr = NULL;
  if( kvstore_integrity_check(kv, &zErr) != KVSTORE_OK ) FAIL("integrity: %s", zErr ? zErr : "");
  snkv_free(zErr);
  closeStore(kv, cfs);
  mFree(&m); mFree(&snap);
  memset(&snap, 0, sizeof snap);
  rmDb();
  return 0;
}
