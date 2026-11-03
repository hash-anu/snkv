/* SPDX-License-Identifier: Apache-2.0 */
/*
** tests/test_powerloss.c — simulated power loss at every write and sync.
**
** A wrapper VFS sits between SQLite and the real files.  Writes reach the
** file at once, but the wrapper remembers what each write replaced until the
** file is synced: on a real disk, data written since the last fsync can be
** lost, reordered or half-written when the power goes.
**
** For a workload of transactions (puts of small and overflow-sized values,
** deletes, TTL puts, a column family created/dropped, auto-commits and
** checkpoints) the test first counts the write/sync/truncate/delete calls.
** Then, for every crash point N, it reruns the workload with "power lost" at
** the N-th call: that call and every later one fails and changes nothing.
** The unsynced writes are then
**   - LOSE: all rolled back (the file is also cut back to its synced size),
**   - KEEP: all kept,
**   - TEAR: kept or rolled back per 512-byte sector at random (seeded), in
**     any order a disk could produce: once a newer write's sector is kept,
**     older writes to that sector are kept too,
** and the store is reopened through the real VFS and must
**   1. open, pass kvstore_integrity_check, and
**   2. hold exactly the contents after some committed transaction:
**        sync FULL  : the last commit that returned OK, or the one that was
**                     in flight when the power went;
**        sync NORMAL: (WAL) any earlier commit, or the in-flight one — WAL
**                     with NORMAL may lose recent commits but never tears one.
** Modes: WAL + FULL, WAL + NORMAL, DELETE journal + FULL, and encrypted
** (WAL + FULL, DELETE + FULL) with kvstore_reencrypt at txn 15 and
** kvstore_remove_encryption at txn 30: the store must open with exactly the
** password (or none) that matches the recovered commit.  Built with the
** minimum Argon2 cost (see the Makefile), since it opens the store thousands
** of times.
**
** Simplifications: deleting or creating a file is durable at once (no
** directory-sync model), and the -shm file is discarded on power loss (it
** is rebuilt from the WAL on open).
**
** Env: PL_STRIDE=n tests every n-th crash point (default 1 = all),
**      PL_SEEDS=n random TEAR seeds per crash point (default 2).
*/
#include "kvstore.h"
#include "sqlite3.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int g_pass = 0, g_fail = 0;
#define CHECK(cond, msg) do{ if(cond){ g_pass++; } else { g_fail++; \
  printf("  [FAIL] %s\n", msg); } }while(0)

/* =========================================================== power-loss VFS */

#define PL_SECTOR    512
#define PL_MAX_PATHS 16

typedef struct PendWrite PendWrite;
struct PendWrite {            /* a write (or truncate) not yet synced */
  sqlite3_int64 off;
  int n;
  unsigned char *pOld;        /* bytes it replaced (zeros past the old EOF) */
  PendWrite *pNext;           /* older */
};
typedef struct PlPath {
  char zName[512];
  sqlite3_int64 syncedSize;   /* file size at the last sync */
  PendWrite *pPend;           /* newest first */
} PlPath;

static sqlite3_vfs *g_real;
static sqlite3_vfs g_plVfs;
static PlPath g_paths[PL_MAX_PATHS];
static int g_nPaths;
static int g_nOps;            /* write/sync/truncate/delete calls so far */
static int g_crashAt;         /* 0 = never */
static int g_crashed;
static int g_lieSync;         /* self-check: a disk that ignores fsync */

static void plFreePend(PlPath *p){
  while( p->pPend ){
    PendWrite *w = p->pPend;
    p->pPend = w->pNext;
    free(w->pOld);
    free(w);
  }
}
static PlPath *plFind(const char *z){
  for(int i = 0; i < g_nPaths; i++){
    if( strcmp(g_paths[i].zName, z) == 0 ) return &g_paths[i];
  }
  return 0;
}
static void plReset(void){
  for(int i = 0; i < g_nPaths; i++) plFreePend(&g_paths[i]);
  g_nPaths = 0;
  g_nOps = 0;
  g_crashed = 0;
}
/* Count one mutating call; returns 1 once the power is gone. */
static int plTick(void){
  if( g_crashed ) return 1;
  g_nOps++;
  if( g_crashAt && g_nOps >= g_crashAt ){ g_crashed = 1; return 1; }
  return 0;
}

typedef struct PlFile {
  sqlite3_file base;
  PlPath *pPath;              /* NULL: temp file, not tracked */
  sqlite3_file *pReal;
} PlFile;
#define REALF(f) (((PlFile*)(f))->pReal)

static int plRecord(PlFile *p, sqlite3_int64 off, int n){
  PendWrite *w = (PendWrite*)malloc(sizeof *w);
  if( !w ) return SQLITE_NOMEM;
  w->pOld = (unsigned char*)calloc(1, n > 0 ? n : 1);
  if( !w->pOld ){ free(w); return SQLITE_NOMEM; }
  /* A short read zero-fills the rest: past the old EOF the "old" bytes are
  ** zeros, which is what an unsynced append can leave behind. */
  int rc = p->pReal->pMethods->xRead(p->pReal, w->pOld, n, off);
  if( rc != SQLITE_OK && rc != SQLITE_IOERR_SHORT_READ ){ free(w->pOld); free(w); return rc; }
  w->off = off;
  w->n = n;
  w->pNext = p->pPath->pPend;
  p->pPath->pPend = w;
  return SQLITE_OK;
}

static int plClose(sqlite3_file *f){
  int rc = REALF(f)->pMethods->xClose(REALF(f));
  return rc;
}
static int plRead(sqlite3_file *f, void *b, int n, sqlite3_int64 o){
  return REALF(f)->pMethods->xRead(REALF(f), b, n, o);
}
static int plWrite(sqlite3_file *f, const void *b, int n, sqlite3_int64 o){
  PlFile *p = (PlFile*)f;
  if( p->pPath ){
    if( plTick() ) return SQLITE_IOERR_WRITE;
    int rc = plRecord(p, o, n);
    if( rc ) return rc;
  }
  return p->pReal->pMethods->xWrite(p->pReal, b, n, o);
}
static int plTruncate(sqlite3_file *f, sqlite3_int64 sz){
  PlFile *p = (PlFile*)f;
  if( p->pPath ){
    sqlite3_int64 cur = 0;
    if( plTick() ) return SQLITE_IOERR_TRUNCATE;
    p->pReal->pMethods->xFileSize(p->pReal, &cur);
    if( cur > sz ){                  /* remember the tail it cuts off */
      int rc = plRecord(p, sz, (int)(cur - sz));
      if( rc ) return rc;
    }
  }
  return p->pReal->pMethods->xTruncate(p->pReal, sz);
}
static int plSync(sqlite3_file *f, int fl){
  PlFile *p = (PlFile*)f;
  if( p->pPath && plTick() ) return SQLITE_IOERR_FSYNC;
  int rc = p->pReal->pMethods->xSync(p->pReal, fl);
  if( rc == SQLITE_OK && p->pPath && !g_lieSync ){
    plFreePend(p->pPath);            /* everything so far is durable */
    p->pReal->pMethods->xFileSize(p->pReal, &p->pPath->syncedSize);
  }
  return rc;
}
static int plFileSize(sqlite3_file *f, sqlite3_int64 *pSz){ return REALF(f)->pMethods->xFileSize(REALF(f), pSz); }
static int plLock(sqlite3_file *f, int l){ return REALF(f)->pMethods->xLock(REALF(f), l); }
static int plUnlock(sqlite3_file *f, int l){ return REALF(f)->pMethods->xUnlock(REALF(f), l); }
static int plCheckRes(sqlite3_file *f, int *pOut){ return REALF(f)->pMethods->xCheckReservedLock(REALF(f), pOut); }
static int plFileCtl(sqlite3_file *f, int op, void *a){ return REALF(f)->pMethods->xFileControl(REALF(f), op, a); }
static int plSector(sqlite3_file *f){ (void)f; return PL_SECTOR; }
static int plDevChar(sqlite3_file *f){ return REALF(f)->pMethods->xDeviceCharacteristics(REALF(f)); }
static int plShmMap(sqlite3_file *f, int i, int sz, int b, void volatile **pp){ return REALF(f)->pMethods->xShmMap(REALF(f), i, sz, b, pp); }
static int plShmLock(sqlite3_file *f, int o, int n, int fl){ return REALF(f)->pMethods->xShmLock(REALF(f), o, n, fl); }
static void plShmBarrier(sqlite3_file *f){ REALF(f)->pMethods->xShmBarrier(REALF(f)); }
static int plShmUnmap(sqlite3_file *f, int d){ return REALF(f)->pMethods->xShmUnmap(REALF(f), d); }

static const sqlite3_io_methods plIo = {
  2, plClose, plRead, plWrite, plTruncate, plSync, plFileSize, plLock,
  plUnlock, plCheckRes, plFileCtl, plSector, plDevChar,
  plShmMap, plShmLock, plShmBarrier, plShmUnmap, 0, 0
};

static int plOpen(sqlite3_vfs *v, const char *z, sqlite3_file *f, int fl, int *pOut){
  (void)v;
  PlFile *p = (PlFile*)f;
  p->pReal = (sqlite3_file*)&p[1];
  p->pPath = 0;
  int rc = g_real->xOpen(g_real, z, p->pReal, fl, pOut);
  if( rc != SQLITE_OK || !p->pReal->pMethods ){ p->base.pMethods = 0; return rc; }
  p->base.pMethods = &plIo;
  if( z ){
    PlPath *pp = plFind(z);
    if( !pp && g_nPaths < PL_MAX_PATHS ){
      pp = &g_paths[g_nPaths++];
      memset(pp, 0, sizeof *pp);
      snprintf(pp->zName, sizeof pp->zName, "%s", z);
      /* Whatever the file holds when first opened was there before the
      ** workload started: durable. */
      p->pReal->pMethods->xFileSize(p->pReal, &pp->syncedSize);
    }
    p->pPath = pp;
  }
  return SQLITE_OK;
}
static int plDelete(sqlite3_vfs *v, const char *z, int dirSync){
  (void)v;
  if( plTick() ) return SQLITE_IOERR_DELETE;
  int rc = g_real->xDelete(g_real, z, dirSync);
  PlPath *pp = plFind(z);
  if( pp ){ plFreePend(pp); pp->syncedSize = 0; }
  return rc;
}
static int plAccess(sqlite3_vfs *v, const char *z, int fl, int *pOut){ (void)v; return g_real->xAccess(g_real, z, fl, pOut); }
static int plFullPath(sqlite3_vfs *v, const char *z, int n, char *o){ (void)v; return g_real->xFullPathname(g_real, z, n, o); }
static void *plDlOpen(sqlite3_vfs *v, const char *z){ (void)v; return g_real->xDlOpen(g_real, z); }
static void plDlError(sqlite3_vfs *v, int n, char *z){ (void)v; g_real->xDlError(g_real, n, z); }
static void (*plDlSym(sqlite3_vfs *v, void *p, const char *z))(void){ (void)v; return g_real->xDlSym(g_real, p, z); }
static void plDlClose(sqlite3_vfs *v, void *p){ (void)v; g_real->xDlClose(g_real, p); }
static int plRandom(sqlite3_vfs *v, int n, char *z){ (void)v; return g_real->xRandomness(g_real, n, z); }
static int plSleep(sqlite3_vfs *v, int us){ (void)v; return g_real->xSleep(g_real, us); }
static int plTime(sqlite3_vfs *v, double *pOut){ (void)v; return g_real->xCurrentTime(g_real, pOut); }
static int plLastErr(sqlite3_vfs *v, int n, char *z){ (void)v; return g_real->xGetLastError(g_real, n, z); }
static int plTime64(sqlite3_vfs *v, sqlite3_int64 *pOut){ (void)v; return g_real->xCurrentTimeInt64(g_real, pOut); }

static void plInstall(void){
  sqlite3_initialize();
  g_real = sqlite3_vfs_find(0);
  memset(&g_plVfs, 0, sizeof g_plVfs);
  g_plVfs.iVersion = 2;
  g_plVfs.szOsFile = (int)sizeof(PlFile) + g_real->szOsFile;
  g_plVfs.mxPathname = g_real->mxPathname;
  g_plVfs.zName = "powerloss";
  g_plVfs.xOpen = plOpen;           g_plVfs.xDelete = plDelete;
  g_plVfs.xAccess = plAccess;       g_plVfs.xFullPathname = plFullPath;
  g_plVfs.xDlOpen = plDlOpen;       g_plVfs.xDlError = plDlError;
  g_plVfs.xDlSym = plDlSym;         g_plVfs.xDlClose = plDlClose;
  g_plVfs.xRandomness = plRandom;   g_plVfs.xSleep = plSleep;
  g_plVfs.xCurrentTime = plTime;    g_plVfs.xGetLastError = plLastErr;
  g_plVfs.xCurrentTimeInt64 = plTime64;
  sqlite3_vfs_register(&g_plVfs, 0);
}
/* Which VFS kvstore_open() uses: the wrapper, or the real one to verify. */
static void plUse(int on){ sqlite3_vfs_register(on ? &g_plVfs : g_real, 1); }

/* ------------------------------------------------- apply the power loss */

enum { LOSE = 0, KEEP = 1, TEAR = 2 };
static const char *g_modeName[] = { "lose", "keep", "tear" };

static unsigned g_rng;
static unsigned plRand(void){ g_rng = g_rng * 1103515245u + 12345u; return g_rng >> 16; }

/* Roll back the unsynced writes of every tracked file (power is off: the
** workload's handles are closed).  Uses the real VFS, so it is portable. */
static void plPowerLoss(int mode, unsigned seed){
  g_rng = seed * 2654435761u + 1;
  for(int i = 0; i < g_nPaths; i++){
    PlPath *pp = &g_paths[i];
    int exists = 0;
    g_real->xAccess(g_real, pp->zName, SQLITE_ACCESS_EXISTS, &exists);
    if( !exists || (!pp->pPend && mode != LOSE) ) continue;

    sqlite3_file *f = (sqlite3_file*)calloc(1, g_real->szOsFile);
    int outFl = 0;
    if( !f || g_real->xOpen(g_real, pp->zName, f,
                  SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB, &outFl) != SQLITE_OK ){
      free(f);
      continue;
    }
    /* Sectors a newer, kept write already owns: older writes to them stay. */
    unsigned char *kept = 0; size_t nKept = 0;
    for(PendWrite *w = pp->pPend; w; w = w->pNext){
      sqlite3_int64 s0 = w->off / PL_SECTOR, s1 = (w->off + w->n - 1) / PL_SECTOR;
      for(sqlite3_int64 s = s0; s <= s1; s++){
        sqlite3_int64 lo = s * PL_SECTOR > w->off ? s * PL_SECTOR : w->off;
        sqlite3_int64 hi = (s + 1) * PL_SECTOR < w->off + w->n ? (s + 1) * PL_SECTOR : w->off + w->n;
        if( (size_t)s >= nKept ){
          size_t nNew = (size_t)s * 2 + 16;
          unsigned char *k2 = (unsigned char*)realloc(kept, nNew);
          if( !k2 ) break;
          memset(k2 + nKept, 0, nNew - nKept);
          kept = k2; nKept = nNew;
        }
        int revert;
        if( kept[s] ) revert = 0;
        else if( mode == LOSE ) revert = 1;
        else if( mode == KEEP ) revert = 0;
        else revert = plRand() & 1;
        if( revert ){
          f->pMethods->xWrite(f, w->pOld + (lo - w->off), (int)(hi - lo), lo);
        }else{
          kept[s] = 1;
        }
      }
    }
    if( mode == LOSE ){
      sqlite3_int64 sz = 0;
      f->pMethods->xFileSize(f, &sz);
      if( sz > pp->syncedSize ) f->pMethods->xTruncate(f, pp->syncedSize);
    }
    free(kept);
    f->pMethods->xClose(f);
    free(f);
  }
}

/* ============================================================ the model */

#define NS        3                   /* namespaces: default, cf1, cf2 */
#define NKEYS     40
#define MAXVAL    1600                /* > a 1 KB page: overflow pages */

typedef struct Entry { int present; int hasTtl; int ver; int len; } Entry;
typedef struct State {
  int pw;                             /* 0 plain, 1 / 2: encrypted with password 1 / 2 */
  int cf1, cf2;                       /* column family exists */
  Entry e[NS][NKEYS];                 /* value bytes derive from (key, ver) */
} State;

static void keyName(int ns, int k, int ttl, char *buf){
  (void)ns;
  snprintf(buf, 16, "%c%03d", ttl ? 't' : 'k', k);
}
/* TTL keys ('t...') live only in the default CF; plain keys in all CFs.
** Index k: 0..NKEYS/2-1 plain, NKEYS/2.. TTL (default CF only). */
static int isTtlKey(int k){ return k >= NKEYS / 2; }
static void valueBytes(int ns, int k, int ver, int len, unsigned char *out){
  for(int i = 0; i < len; i++) out[i] = (unsigned char)(ns * 31 + k * 7 + ver * 13 + i);
}
#define TTL_EXPIRE ((int64_t)4102444800000LL)   /* 2100-01-01: never expires here */

/* =========================================================== the workload */

#define MAX_STATES 128
typedef struct Run {
  State states[MAX_STATES];  /* states[i] = contents after commit i */
  int nStates;
  State inflight;            /* commit that was running when power went */
  int hasInflight;
} Run;

typedef struct Cfg { const char *name; int journal; int sync; int enc; } Cfg;
static const char *g_pw[3] = { "", "password-one", "password-two" };

static unsigned g_wl;        /* workload RNG: the same sequence every run */
static unsigned wlRand(void){ g_wl = g_wl * 1664525u + 1013904223u; return g_wl >> 8; }

static void pushState(Run *r, const State *s){
  if( r->nStates < MAX_STATES ) r->states[r->nStates++] = *s;
}

/* One random op on the tentative state; returns the kvstore rc. */
static int doOp(KVStore *kv, KVColumnFamily *cf1, KVColumnFamily *cf2, State *t){
  static unsigned char vb[MAXVAL];
  char key[16];
  int ns = (int)(wlRand() % NS);
  if( ns == 1 && !t->cf1 ) ns = 0;
  if( ns == 2 && !t->cf2 ) ns = 0;
  int k = (int)(wlRand() % NKEYS);
  if( ns != 0 && isTtlKey(k) ) k -= NKEYS / 2;
  KVColumnFamily *cf = ns == 1 ? cf1 : ns == 2 ? cf2 : 0;
  Entry *e = &t->e[ns][k];
  keyName(ns, k, isTtlKey(k), key);
  int rc;
  if( wlRand() % 4 == 0 ){                       /* delete */
    rc = cf ? kvstore_cf_delete(cf, key, 4) : kvstore_delete(kv, key, 4);
    if( rc == KVSTORE_NOTFOUND ) rc = KVSTORE_OK;
    e->present = 0; e->hasTtl = 0;
  }else{
    int len = (wlRand() % 5 == 0) ? 900 + (int)(wlRand() % (MAXVAL - 900)) : 1 + (int)(wlRand() % 60);
    int ver = e->ver + 1;
    valueBytes(ns, k, ver, len, vb);
    if( isTtlKey(k) ){
      rc = kvstore_put_ttl(kv, key, 4, vb, len, TTL_EXPIRE);
      e->hasTtl = 1;
    }else{
      rc = cf ? kvstore_cf_put(cf, key, 4, vb, len) : kvstore_put(kv, key, 4, vb, len);
    }
    e->present = 1; e->ver = ver; e->len = len;
  }
  return rc;
}

/* Run the workload; stops at the first error (the power went).  Fills r. */
static void workload(const Cfg *cfg, const char *db, Run *r){
  KVStoreConfig c; memset(&c, 0, sizeof c);
  c.journalMode = cfg->journal;
  c.syncLevel = cfg->sync;
  c.pageSize = 1024;            /* more pages, more splits from little data */
  c.cacheSize = 12;             /* tiny cache: pages spill mid-transaction */
  c.walAutoCheckpoint = 4;      /* checkpoint all the time */
  KVStore *kv = NULL;
  KVColumnFamily *cf1 = NULL, *cf2 = NULL;
  State cur; memset(&cur, 0, sizeof cur);
  int rc;

  r->nStates = 0; r->hasInflight = 0;
  pushState(r, &cur);                            /* before anything: empty */
  g_wl = 12345;

  if( cfg->enc ){
    /* Creating the encrypted store writes its salt and verify tag: until
    ** that commits, an empty plain file is a valid outcome too. */
    State t0 = cur; t0.pw = 1;
    rc = kvstore_open_encrypted(db, g_pw[1], (int)strlen(g_pw[1]), &kv, &c);
    if( rc != KVSTORE_OK ){ if( kv ) kvstore_close(kv); r->inflight = t0; r->hasInflight = 1; return; }
    cur = t0; pushState(r, &cur);
  }else if( kvstore_open_v2(db, &kv, &c) != KVSTORE_OK ){
    if( kv ) kvstore_close(kv);
    return;
  }

  /* Create cf1 (an internal transaction). */
  State t = cur; t.cf1 = 1;
  rc = kvstore_cf_create(kv, "cf1", &cf1);
  if( rc != KVSTORE_OK ){ r->inflight = t; r->hasInflight = 1; goto done; }
  cur = t; pushState(r, &cur);

  for(int txn = 0; txn < 40; txn++){
    int kind = (int)(wlRand() % 10);
    t = cur;
    if( cfg->enc && (txn == 15 || txn == 30) ){  /* re-key, then decrypt */
      if( txn == 15 ){
        t.pw = 2;
        rc = kvstore_reencrypt(kv, g_pw[2], (int)strlen(g_pw[2]));
      }else{
        t.pw = 0;
        rc = kvstore_remove_encryption(kv);
      }
      if( rc != KVSTORE_OK ){ r->inflight = t; r->hasInflight = 1; goto done; }
    }else if( kind <= 5 ){                       /* explicit transaction */
      if( kvstore_begin(kv, 1) != KVSTORE_OK ) goto done;
      int nOps = 1 + (int)(wlRand() % 6);
      for(int i = 0; i < nOps; i++){
        if( doOp(kv, cf1, cf2, &t) != KVSTORE_OK ){ kvstore_rollback(kv); goto done; }
      }
      rc = kvstore_commit(kv);
      if( rc != KVSTORE_OK ){ kvstore_rollback(kv); r->inflight = t; r->hasInflight = 1; goto done; }
    }else if( kind <= 7 ){                       /* one auto-commit op */
      rc = doOp(kv, cf1, cf2, &t);
      if( rc != KVSTORE_OK ){ r->inflight = t; r->hasInflight = 1; goto done; }
    }else if( kind == 8 ){                       /* create / drop cf2 */
      if( !t.cf2 ){
        t.cf2 = 1;
        rc = kvstore_cf_create(kv, "cf2", &cf2);
      }else{
        t.cf2 = 0;
        memset(t.e[2], 0, sizeof t.e[2]);
        kvstore_cf_close(cf2); cf2 = NULL;
        rc = kvstore_cf_drop(kv, "cf2");
      }
      if( rc != KVSTORE_OK ){ r->inflight = t; r->hasInflight = 1; goto done; }
    }else{                                       /* checkpoint: no new state */
      if( kvstore_checkpoint(kv, KVSTORE_CHECKPOINT_PASSIVE, NULL, NULL) != KVSTORE_OK
       && cfg->journal == KVSTORE_JOURNAL_WAL ) goto done;
      continue;
    }
    cur = t;
    pushState(r, &cur);
  }
done:
  if( cf2 ) kvstore_cf_close(cf2);
  if( cf1 ) kvstore_cf_close(cf1);
  kvstore_close(kv);
}

/* ============================================================ verification */

static int readState(KVStore *kv, State *s, char *why, size_t nWhy){
  static unsigned char want[MAXVAL];
  KVColumnFamily *cfs[NS] = { 0, 0, 0 };
  memset(s, 0, sizeof *s);
  s->cf1 = kvstore_cf_open(kv, "cf1", &cfs[1]) == KVSTORE_OK;
  s->cf2 = kvstore_cf_open(kv, "cf2", &cfs[2]) == KVSTORE_OK;
  for(int ns = 0; ns < NS; ns++){
    if( ns && !cfs[ns] ) continue;
    KVIterator *it = NULL;
    int rc = ns ? kvstore_cf_iterator_create(cfs[ns], &it) : kvstore_iterator_create(kv, &it);
    if( rc != KVSTORE_OK ){ snprintf(why, nWhy, "iterator create rc=%d", rc); return 0; }
    for(kvstore_iterator_first(it); !kvstore_iterator_eof(it); kvstore_iterator_next(it)){
      void *k = NULL, *v = NULL; int nk = 0, nv = 0;
      if( kvstore_iterator_key(it, &k, &nk) != KVSTORE_OK
       || kvstore_iterator_value(it, &v, &nv) != KVSTORE_OK ){
        snprintf(why, nWhy, "iterator read failed (cf %d)", ns);
        kvstore_iterator_close(it); return 0;
      }
      int idx = -1;                     /* 'k'/'t' + index (TTL keys: upper half) */
      if( nk == 4 && (((char*)k)[0] == 'k' || ((char*)k)[0] == 't') ){
        char num[4] = { ((char*)k)[1], ((char*)k)[2], ((char*)k)[3], 0 };
        idx = atoi(num);
      }
      if( idx < 0 || idx >= NKEYS ){ snprintf(why, nWhy, "unexpected key in cf %d", ns); kvstore_iterator_close(it); return 0; }
      /* Recover the version: try each version until the bytes match. */
      Entry *e = &s->e[ns][idx];
      e->present = 1; e->len = nv; e->ver = -1;
      for(int ver = 1; ver < 200; ver++){
        valueBytes(ns, idx, ver, nv, want);
        if( memcmp(v, want, nv) == 0 ){ e->ver = ver; break; }
      }
      if( e->ver < 0 ){ snprintf(why, nWhy, "cf %d key %d: value matches no version", ns, idx); kvstore_iterator_close(it); return 0; }
    }
    kvstore_iterator_close(it);
  }
  /* TTL flags of the default CF's TTL keys. */
  for(int k = NKEYS / 2; k < NKEYS; k++){
    if( !s->e[0][k].present ) continue;
    char key[16]; keyName(0, k, 1, key);
    int64_t rem = 0;
    if( kvstore_ttl_remaining(kv, key, 4, &rem) == KVSTORE_OK && rem > 0 ) s->e[0][k].hasTtl = 1;
  }
  if( cfs[1] ) kvstore_cf_close(cfs[1]);
  if( cfs[2] ) kvstore_cf_close(cfs[2]);
  return 1;
}

static int sameState(const State *a, const State *b){
  if( a->pw != b->pw || a->cf1 != b->cf1 || a->cf2 != b->cf2 ) return 0;
  for(int ns = 0; ns < NS; ns++){
    for(int k = 0; k < NKEYS; k++){
      const Entry *x = &a->e[ns][k], *y = &b->e[ns][k];
      if( x->present != y->present ) return 0;
      if( !x->present ) continue;
      if( x->ver != y->ver || x->len != y->len ) return 0;
      if( ns == 0 && isTtlKey(k) && x->hasTtl != y->hasTtl ) return 0;
    }
  }
  return 1;
}

static void cleanupDb(const char *db){
  char buf[600];
  remove(db);
  snprintf(buf, sizeof buf, "%s-wal", db); remove(buf);
  snprintf(buf, sizeof buf, "%s-shm", db); remove(buf);
  snprintf(buf, sizeof buf, "%s-journal", db); remove(buf);
}

/* Reopen through the real VFS and check the invariants.  Returns 1 if ok. */
static int verify(const Cfg *cfg, const char *db, const Run *r, char *why, size_t nWhy){
  char shm[600];
  snprintf(shm, sizeof shm, "%s-shm", db);
  remove(shm);                                /* shared memory is lost */
  KVStoreConfig c; memset(&c, 0, sizeof c);
  c.journalMode = cfg->journal;
  c.syncLevel = cfg->sync;
  c.pageSize = 1024;
  KVStore *kv = NULL;
  int pw = 0;
  int rc = kvstore_open_v2(db, &kv, &c);
  if( cfg->enc && rc == KVSTORE_AUTH_FAILED ){
    /* Encrypted: exactly one password must work.  A plain open is tried
    ** first because opening a plain store with a password encrypts it. */
    if( kv ){ kvstore_close(kv); kv = NULL; }
    for(pw = 2; pw >= 1; pw--){
      rc = kvstore_open_encrypted(db, g_pw[pw], (int)strlen(g_pw[pw]), &kv, &c);
      if( rc == KVSTORE_OK ) break;
      if( kv ){ kvstore_close(kv); kv = NULL; }
    }
    if( pw == 0 ){ snprintf(why, nWhy, "encrypted store opens with neither password (rc=%d)", rc); return 0; }
  }
  if( rc != KVSTORE_OK ){ snprintf(why, nWhy, "reopen failed rc=%d", rc); if( kv ) kvstore_close(kv); return 0; }
  char *zErr = NULL;
  rc = kvstore_integrity_check(kv, &zErr);
  if( rc != KVSTORE_OK ){
    snprintf(why, nWhy, "integrity check rc=%d: %s", rc, zErr ? zErr : "");
    if( zErr ) snkv_free(zErr);
    kvstore_close(kv); return 0;
  }
  if( zErr ) snkv_free(zErr);
  State got;
  if( !readState(kv, &got, why, nWhy) ){ kvstore_close(kv); return 0; }
  got.pw = pw;
  kvstore_close(kv);

  int last = r->nStates - 1;
  int strict = cfg->sync == KVSTORE_SYNC_FULL;
  if( r->hasInflight && sameState(&got, &r->inflight) ) return 1;
  for(int i = last; i >= (strict ? last : 0); i--){
    if( sameState(&got, &r->states[i]) ) return 1;
  }
  int older = -1;
  for(int i = last - 1; i >= 0 && strict; i--){ if( sameState(&got, &r->states[i]) ){ older = i; break; } }
  if( older >= 0 ) snprintf(why, nWhy, "lost a committed transaction: holds commit %d of %d", older, last);
  else snprintf(why, nWhy, "contents match no committed state (last commit %d)", last);
  return 0;
}

/* ================================================================= runner */

static int runCfg(const Cfg *cfg, int stride, int nSeeds, int quiet){
  const char *db = "pl.db";
  static Run base, run;
  char msg[300], why[200];

  /* Dry run: count the crash points. */
  cleanupDb(db);
  plUse(1); plReset(); g_crashAt = 0;
  workload(cfg, db, &base);
  int nOps = g_nOps;
  plUse(0);
  snprintf(msg, sizeof msg, "%s: workload completes without power loss", cfg->name);
  CHECK(!base.hasInflight && base.nStates > 30, msg);
  if( !verify(cfg, db, &base, why, sizeof why) ){
    snprintf(msg, sizeof msg, "%s: clean run verifies (%s)", cfg->name, why);
    CHECK(0, msg);
  }

  int nRuns = 0, nBad = 0;
  for(int at = 1; at <= nOps; at += stride){
    for(int mode = LOSE; mode <= TEAR; mode++){
      int seeds = mode == TEAR ? nSeeds : 1;
      for(int sd = 0; sd < seeds; sd++){
        cleanupDb(db);
        plUse(1); plReset(); g_crashAt = at;
        workload(cfg, db, &run);
        plUse(0);
        plPowerLoss(mode, (unsigned)(at * 131 + sd));
        nRuns++;
        if( !verify(cfg, db, &run, why, sizeof why) ){
          nBad++;
          if( quiet ) continue;
          if( nBad <= 10 ){
            snprintf(msg, sizeof msg, "%s: power lost at call %d/%d, %s seed %d: %s",
                     cfg->name, at, nOps, g_modeName[mode], sd, why);
            CHECK(0, msg);
          }
        }else if( !quiet ){
          g_pass++;
        }
      }
    }
  }
  plReset();
  cleanupDb(db);
  if( !quiet ){
    printf("  %-14s %5d crash points, %6d power-loss runs, %d failed\n",
           cfg->name, (nOps + stride - 1) / stride, nRuns, nBad);
  }
  return nBad;
}

int main(void){
  int stride = getenv("PL_STRIDE") ? atoi(getenv("PL_STRIDE")) : 1;
  int nSeeds = getenv("PL_SEEDS") ? atoi(getenv("PL_SEEDS")) : 2;
  if( stride < 1 ) stride = 1;
  if( nSeeds < 1 ) nSeeds = 1;
  static const Cfg cfgs[] = {
    { "WAL+FULL",     KVSTORE_JOURNAL_WAL,    KVSTORE_SYNC_FULL,   0 },
    { "WAL+NORMAL",   KVSTORE_JOURNAL_WAL,    KVSTORE_SYNC_NORMAL, 0 },
    { "DELETE+FULL",  KVSTORE_JOURNAL_DELETE, KVSTORE_SYNC_FULL,   0 },
    /* Encrypted, re-keyed at txn 15, decrypted at txn 30 (one transaction
    ** each: the store must open with exactly the matching password). */
    { "ENC WAL+FULL", KVSTORE_JOURNAL_WAL,    KVSTORE_SYNC_FULL,   1 },
    { "ENC DEL+FULL", KVSTORE_JOURNAL_DELETE, KVSTORE_SYNC_FULL,   1 },
  };
  printf("=== test_powerloss: simulated power loss at every write/sync ===\n");
  printf("  (stride %d, %d random tear seeds per crash point)\n", stride, nSeeds);
  plInstall();

  /* Self-check: the harness must catch a disk that ignores fsync (synced
  ** data can still be lost).  If it reports nothing, the test is broken. */
  g_lieSync = 1;
  int caught = runCfg(&cfgs[0], 7, 1, 1);
  g_lieSync = 0;
  printf("  self-check: disk that ignores fsync -> %d bad recoveries detected\n", caught);
  CHECK(caught > 0, "self-check: harness detects lost synced writes");

  for(size_t i = 0; i < sizeof cfgs / sizeof cfgs[0]; i++) runCfg(&cfgs[i], stride, nSeeds, 0);
  printf("=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
  return g_fail ? 1 : 0;
}
