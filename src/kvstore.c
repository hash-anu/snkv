/* SPDX-License-Identifier: Apache-2.0 */
/*
**
** Key-Value Store Implementation - Column Families
** Built on top of SQLite v3.51.200 btree implementation
**
** This implementation provides a robust key-value store
** with column family support, proper error handling, logging, safety checks,
** and thread-safety via mutexes.
**
** Storage model:
**   User data tables use BTREE_BLOBKEY.  Each entry is stored as a
**   single blob in the btree:
**     [key_len (4 bytes BE)] [key_bytes] [value_bytes]
**   A custom comparison function (sqlite3VdbeRecordCompare in btree.c)
**   compares only the key portion, so entries are kept in lexicographic
**   key order.  This enables O(log n) point lookups, efficient prefix
**   search via cursor seek, and sorted iteration.
**
**   The internal CF metadata table remains BTREE_INTKEY with FNV-1a
**   hash as rowid (only used for column family name → root page mapping).
*/

#include "kvstore.h"
#include "pager.h"
#include <string.h>
#include <assert.h>
#include <stdio.h>

/*
** Compile-time configuration
*/
#ifndef KVSTORE_MAX_KEY_SIZE
# define KVSTORE_MAX_KEY_SIZE   (64 * 1024)  /* 64KB max key size */
#endif

#ifndef KVSTORE_MAX_VALUE_SIZE
# define KVSTORE_MAX_VALUE_SIZE (10 * 1024 * 1024)  /* 10MB max value size */
#endif

#ifndef KVSTORE_DEFAULT_CACHE_SIZE
# define KVSTORE_DEFAULT_CACHE_SIZE 2000  /* 2000 pages default cache */
#endif

#ifndef KVSTORE_MAX_CF_NAME
# define KVSTORE_MAX_CF_NAME 255  /* Maximum column family name length */
#endif

/* (collision probes removed — BLOBKEY storage uses direct key comparison) */

/*
** Column family metadata stored in meta table
** Meta layout:
**   meta[1] = Default CF table root (backward compatibility)
**   meta[2] = Number of column families
**   meta[3] = CF metadata table root
*/
#define META_DEFAULT_CF_ROOT  1
#define META_CF_COUNT         2
#define META_CF_METADATA_ROOT 3

/*
** Default column family name
*/
#define DEFAULT_CF_NAME "default"

/* ======================================================================
** BLOBKEY encoding helpers.
**
** Every BLOBKEY cell stores its data as a single blob:
**     [key_len  4 bytes, big-endian] [key_bytes ...] [value_bytes ...]
**
** The btree comparison function (sqlite3VdbeRecordCompare in btree.c)
** compares only the key portion, so entries are ordered lexicographically
** by key.  Two entries with the same key compare as equal; BtreeInsert
** will overwrite the old cell automatically (upsert).
** ====================================================================== */
/*
** Encode [key_len (4 bytes BE)][key_bytes][value_bytes] into *ppOut.
**
** If the total fits within the caller-supplied stack buffer (pStack, nStack),
** *ppOut is set to pStack — no heap allocation occurs.  Otherwise a heap
** buffer is allocated and *ppOut is set to that buffer.
**
** Returns the total encoded length on success, or -1 on OOM.
** Caller must call sqlite3_free(*ppOut) only when *ppOut != pStack.
*/
static int kvstoreEncodeBlob(
  const void *pKey, int nKey,
  const void *pVal, int nVal,
  unsigned char *pStack, int nStack,
  unsigned char **ppOut
){
  int nTotal = 4 + nKey + nVal;
  unsigned char *pOut;
  if( nTotal <= nStack ){
    pOut = pStack;
  }else{
    pOut = (unsigned char *)sqlite3Malloc(nTotal);
    if( pOut == 0 ){ *ppOut = 0; return -1; }
  }
  pOut[0] = (unsigned char)((nKey >> 24) & 0xFF);
  pOut[1] = (unsigned char)((nKey >> 16) & 0xFF);
  pOut[2] = (unsigned char)((nKey >>  8) & 0xFF);
  pOut[3] = (unsigned char)( nKey        & 0xFF);
  memcpy(pOut + 4, pKey, nKey);
  if( nVal > 0 && pVal ){
    memcpy(pOut + 4 + nKey, pVal, nVal);
  }
  *ppOut = pOut;
  return nTotal;
}

/* ======================================================================
** Internal cursor helpers – wraps the new callerallocs cursor model
** ====================================================================== */
static BtCursor *kvstoreAllocCursor(void){
  int n = sqlite3BtreeCursorSize();
  BtCursor *p = (BtCursor*)sqlite3MallocZero(n);
  if( p ) sqlite3BtreeCursorZero(p);
  return p;
}

static void kvstoreFreeCursor(BtCursor *pCur){
  if( pCur ){
    sqlite3BtreeCloseCursor(pCur);
    sqlite3_free(pCur);
  }
}

/*
** Column Family structure
*/
struct KVColumnFamily {
  KVStore *pKV;      /* Parent store */
  char *zName;       /* Column family name */
  int iTable;        /* Root page of this CF's btree */
  int refCount;      /* Reference count */
  sqlite3_mutex *pMutex;  /* Recursive mutex for this CF (SQLITE_MUTEX_RECURSIVE) */
  BtCursor *pReadCur;     /* Cached read-only cursor; NULL = not open yet */
};

/*
** KVStore structure - represents an open key-value store
*/
struct KVStore {
  Btree *pBt;        /* Underlying btree handle */
  sqlite3 *db;       /* Database connection (required by btree) */
  KeyInfo *pKeyInfo; /* Shared KeyInfo for BLOBKEY cursor comparisons */
  int iMetaTable;    /* Root page of CF metadata table */
  int inTrans;       /* Transaction state: 0=none, 1=read, 2=write */
  int isCorrupted;   /* Set if database corruption is detected */
  char *zErrMsg;     /* Last error message */

  /* Thread safety */
  sqlite3_mutex *pMutex;  /* Recursive mutex protecting store operations (SQLITE_MUTEX_RECURSIVE) */
  int closing;            /* Set to 1 when kvstore_close() is in progress */

  /* WAL auto-checkpoint */
  int walSizeLimit;       /* Checkpoint every N commits (0 = disabled) */
  int walCommits;         /* Write commits since last auto-checkpoint */

  /* Column families */
  KVColumnFamily *pDefaultCF;  /* Default column family */
  KVColumnFamily **apCF;       /* Array of open column families */
  int nCF;                     /* Number of open column families */
  int nCFAlloc;                /* Allocated size of apCF array */

  /* Statistics */
  struct {
    u64 nPuts;       /* Number of put operations */
    u64 nGets;       /* Number of get operations */
    u64 nDeletes;    /* Number of delete operations */
    u64 nIterations; /* Number of iterations */
    u64 nErrors;     /* Number of errors encountered */
  } stats;
};

/*
** Iterator structure for traversing the store
*/
struct KVIterator {
  KVColumnFamily *pCF;  /* Column family being iterated */
  BtCursor *pCur;    /* Btree cursor */
  int eof;           /* End-of-file flag */
  int ownsTrans;     /* 1 if iterator started its own transaction */
  void *pKeyBuf;     /* Buffer for current key */
  int nKeyBuf;       /* Size of key buffer */
  void *pValBuf;     /* Buffer for current value */
  int nValBuf;       /* Size of value buffer */
  int isValid;       /* Iterator validity flag */
  void *pPrefix;     /* Prefix filter (NULL = no filter) */
  int nPrefix;       /* Length of prefix */
};

/*
** Return the cached read cursor for a column family, opening it on first
** use.  The cursor remains open across calls (SQLite moves it to
** CURSOR_REQUIRESSEEK when pages are modified, restoring it on next seek).
** Caller must hold both pCF->pMutex and pKV->pMutex.
*/
static BtCursor *kvstoreGetReadCursor(KVColumnFamily *pCF){
  if( pCF->pReadCur ) return pCF->pReadCur;
  BtCursor *pCur = kvstoreAllocCursor();
  if( !pCur ) return NULL;
  int rc = sqlite3BtreeCursor(pCF->pKV->pBt, pCF->iTable, 0,
                               pCF->pKV->pKeyInfo, pCur);
  if( rc != SQLITE_OK ){
    sqlite3_free(pCur);
    return NULL;
  }
  pCF->pReadCur = pCur;
  return pCur;
}

/*
** Set error message (caller must hold mutex)
*/
static void kvstoreSetError(KVStore *pKV, const char *zFormat, ...){
  va_list ap;
  if( pKV->zErrMsg ){
    sqlite3_free(pKV->zErrMsg);
    pKV->zErrMsg = 0;
  }
  if( zFormat ){
    va_start(ap, zFormat);
    pKV->zErrMsg = sqlite3VMPrintf(pKV->db, zFormat, ap);
    va_end(ap);
  }
  pKV->stats.nErrors++;
}

/*
** Get last error message
*/
const char *kvstore_errmsg(KVStore *pKV){
  const char *zMsg;

  if( !pKV ){
    return "no error";
  }

  sqlite3_mutex_enter(pKV->pMutex);
  if( pKV->zErrMsg ){
    zMsg = pKV->zErrMsg;
  }else{
    zMsg = "no error";
  }
  sqlite3_mutex_leave(pKV->pMutex);

  return zMsg;
}

/*
** Validate key and value sizes (caller must hold mutex)
*/
static int kvstoreValidateKeyValue(
  KVStore *pKV,
  const void *pKey,
  int nKey,
  const void *pValue,
  int nValue
){
  if( pKey == NULL || nKey <= 0 ){
    kvstoreSetError(pKV, "invalid key: null or zero length");
    return KVSTORE_ERROR;
  }

  if( nKey > KVSTORE_MAX_KEY_SIZE ){
    kvstoreSetError(pKV, "key too large: %d bytes (max %d)",
                    nKey, KVSTORE_MAX_KEY_SIZE);
    return KVSTORE_ERROR;
  }

  if( pValue == NULL && nValue != 0 ){
    kvstoreSetError(pKV, "invalid value: null pointer with non-zero length");
    return KVSTORE_ERROR;
  }

  if( nValue > KVSTORE_MAX_VALUE_SIZE ){
    kvstoreSetError(pKV, "value too large: %d bytes (max %d)",
                    nValue, KVSTORE_MAX_VALUE_SIZE);
    return KVSTORE_ERROR;
  }

  return KVSTORE_OK;
}

/*
** Check if store is corrupted (caller must hold mutex)
*/
static int kvstoreCheckCorruption(KVStore *pKV, int rc){
  if( rc == SQLITE_CORRUPT || rc == SQLITE_NOTADB ){
    pKV->isCorrupted = 1;
    kvstoreSetError(pKV, "database corruption detected");
    return 1;
  }
  return 0;
}

/* ======================================================================
** BLOBKEY seek helper.
**
** Positions the cursor on the entry whose key matches (pKey, nKey).
** Uses BtreeIndexMoveto which goes through our custom comparison
** function.  Returns SQLITE_OK and sets *pFound=1 if an exact
** match was found; the cursor is left pointing at the entry.
** ====================================================================== */
static int kvstoreSeekKey(
  BtCursor *pCur,
  KeyInfo *pKeyInfo,
  const void *pKey, int nKey,
  int *pFound
){
  int rc;
  int res = 0;
  UnpackedRecord idxKey;  /* stack-allocated: avoids one heap malloc+free per seek */
  Mem memField;           /* placeholder aMem[0] required by comparison function */

  *pFound = 0;

  memset(&idxKey, 0, sizeof(idxKey));
  memset(&memField, 0, sizeof(memField));

  /* Point UnpackedRecord directly at the raw key bytes (not the header) */
  idxKey.pKeyInfo   = pKeyInfo;
  idxKey.aMem       = &memField;
  idxKey.u.z        = (char *)pKey;
  idxKey.n          = nKey;
  idxKey.nField     = 1;
  idxKey.default_rc = 0;  /* exact match */

  rc = sqlite3BtreeIndexMoveto(pCur, &idxKey, &res);

  if( rc == SQLITE_OK && res == 0 ){
    *pFound = 1;
  }
  return rc;
}

/* ======================================================================
** INTKEY helpers for the CF metadata table (internal only).
** The metadata table stores CF name → root page mappings using INTKEY
** with FNV-1a hash as rowid.
** ====================================================================== */
#ifndef KVSTORE_MAX_COLLISION_PROBES
# define KVSTORE_MAX_COLLISION_PROBES 64
#endif

static i64 kvstoreMetaHashKey(const void *pKey, int nKey){
  const unsigned char *p = (const unsigned char *)pKey;
  u64 hash = (u64)14695981039346656037ULL;
  int i;
  for(i = 0; i < nKey; i++){
    hash ^= (u64)p[i];
    hash *= (u64)1099511628211ULL;
  }
  hash &= 0x7FFFFFFFFFFFFFFFULL;
  if( hash == 0 ) hash = 1;
  return (i64)hash;
}

static int kvstoreMetaSeekKey(
  BtCursor *pCur,
  const void *pKey, int nKey,
  int *pFound,
  i64 *pRowid
){
  i64 baseHash = kvstoreMetaHashKey(pKey, nKey);
  int probe;
  *pFound = 0;
  for(probe = 0; probe < KVSTORE_MAX_COLLISION_PROBES; probe++){
    i64 tryRowid = baseHash + probe;
    int res = 0;
    int rc = sqlite3BtreeTableMoveto(pCur, tryRowid, 0, &res);
    if( rc != SQLITE_OK ) return rc;
    if( res != 0 ) return SQLITE_OK;
    u32 payloadSz = sqlite3BtreePayloadSize(pCur);
    if( (int)payloadSz >= 4 + nKey ){
      unsigned char hdr[4];
      rc = sqlite3BtreePayload(pCur, 0, 4, hdr);
      if( rc != SQLITE_OK ) return rc;
      int storedKeyLen = (hdr[0]<<24)|(hdr[1]<<16)|(hdr[2]<<8)|hdr[3];
      if( storedKeyLen == nKey ){
        void *storedKey = sqlite3Malloc(nKey);
        if( storedKey == 0 ) return SQLITE_NOMEM;
        rc = sqlite3BtreePayload(pCur, 4, nKey, storedKey);
        if( rc != SQLITE_OK ){ sqlite3_free(storedKey); return rc; }
        int match = (memcmp(storedKey, pKey, nKey) == 0);
        sqlite3_free(storedKey);
        if( match ){
          *pFound = 1;
          if( pRowid ) *pRowid = tryRowid;
          return SQLITE_OK;
        }
      }
    }
  }
  return SQLITE_OK;
}

static int kvstoreMetaFindSlot(
  BtCursor *pCur,
  const void *pKey, int nKey,
  i64 *pRowid
){
  i64 baseHash = kvstoreMetaHashKey(pKey, nKey);
  int probe;
  for(probe = 0; probe < KVSTORE_MAX_COLLISION_PROBES; probe++){
    i64 tryRowid = baseHash + probe;
    int res = 0;
    int rc = sqlite3BtreeTableMoveto(pCur, tryRowid, 0, &res);
    if( rc != SQLITE_OK ) return rc;
    if( res != 0 ){
      *pRowid = tryRowid;
      return SQLITE_OK;
    }
  }
  return SQLITE_FULL;
}

/*
** Initialize CF metadata table (caller must hold mutex)
*/
static int initCFMetadataTable(KVStore *pKV){
  int rc;
  Pgno pgno;

  rc = sqlite3BtreeBeginTrans(pKV->pBt, 1, 0);
  if( rc != SQLITE_OK ) return rc;

  /* Create metadata table (INTKEY) */
  rc = sqlite3BtreeCreateTable(pKV->pBt, &pgno, BTREE_INTKEY);
  if( rc != SQLITE_OK ){
    sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
    return rc;
  }
  pKV->iMetaTable = (int)pgno;

  /* Store metadata table root in meta[3] */
  rc = sqlite3BtreeUpdateMeta(pKV->pBt, META_CF_METADATA_ROOT, pgno);
  if( rc != SQLITE_OK ){
    sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
    return rc;
  }

  /* Initialize CF count to 1 (default CF) */
  rc = sqlite3BtreeUpdateMeta(pKV->pBt, META_CF_COUNT, 1);
  if( rc != SQLITE_OK ){
    sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
    return rc;
  }

  rc = sqlite3BtreeCommit(pKV->pBt);
  return rc;
}

/*
** Create default column family (caller must hold mutex)
*/
static int createDefaultCF(KVStore *pKV){
  int rc;
  KVColumnFamily *pCF;
  Pgno pgno;

  rc = sqlite3BtreeBeginTrans(pKV->pBt, 1, 0);
  if( rc != SQLITE_OK ) return rc;

  /* Create default CF table (BLOBKEY — keys stored lexicographically) */
  rc = sqlite3BtreeCreateTable(pKV->pBt, &pgno, BTREE_BLOBKEY);
  if( rc != SQLITE_OK ){
    sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
    return rc;
  }

  /* Store in meta[1] for backward compatibility */
  rc = sqlite3BtreeUpdateMeta(pKV->pBt, META_DEFAULT_CF_ROOT, pgno);
  if( rc != SQLITE_OK ){
    sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
    return rc;
  }

  rc = sqlite3BtreeCommit(pKV->pBt);
  if( rc != SQLITE_OK ) return rc;

  /* Create CF structure */
  pCF = (KVColumnFamily*)sqlite3MallocZero(sizeof(KVColumnFamily));
  if( !pCF ) return KVSTORE_NOMEM;

  pCF->pKV = pKV;
  pCF->zName = sqliteStrDup(DEFAULT_CF_NAME);
  pCF->iTable = (int)pgno;
  pCF->refCount = 1;

  /* Create mutex for default CF */
  pCF->pMutex = sqlite3MutexAlloc(SQLITE_MUTEX_RECURSIVE);
  if( !pCF->pMutex ){
    sqlite3_free(pCF->zName);
    sqlite3_free(pCF);
    return KVSTORE_NOMEM;
  }

  pKV->pDefaultCF = pCF;
  return KVSTORE_OK;
}

/* ======================================================================
** Busy-handler callback for kvstore_open_v2(busyTimeout > 0).
**
** SQLite calls this every time a lock cannot be acquired.  We sleep 1 ms
** per call and give up once the total elapsed time exceeds busyTimeout.
**
** pArg — points to a KVBusyCtx embedded in the KVStore allocation.
** nBusy — number of previous busy calls for this operation.
** Returns 1 to retry, 0 to surface SQLITE_BUSY to the caller.
** ====================================================================== */
typedef struct KVBusyCtx KVBusyCtx;
struct KVBusyCtx {
  int timeoutMs;    /* maximum wait in milliseconds */
  sqlite3_vfs *pVfs;
};

static int kvstoreBusyHandler(void *pArg, int nBusy){
  KVBusyCtx *ctx = (KVBusyCtx *)pArg;
  /* Each call represents ~1 ms of waiting (one 1000-µs sleep below). */
  if( nBusy * 1 >= ctx->timeoutMs ) return 0;  /* give up */
  sqlite3OsSleep(ctx->pVfs, 1000);              /* sleep 1 ms */
  return 1;                                     /* retry */
}

/* ======================================================================
** Shared teardown helper — releases all resources owned by a partially or
** fully initialised KVStore.  Used by both kvstore_open_v2 error paths
** and kvstore_close.
** ====================================================================== */
static void kvstoreTeardownNoLock(KVStore *pKV){
  int i;
  if( pKV->pDefaultCF ){
    if( pKV->pDefaultCF->pReadCur ) kvstoreFreeCursor(pKV->pDefaultCF->pReadCur);
    if( pKV->pDefaultCF->pMutex )   sqlite3_mutex_free(pKV->pDefaultCF->pMutex);
    sqlite3_free(pKV->pDefaultCF->zName);
    sqlite3_free(pKV->pDefaultCF);
    pKV->pDefaultCF = 0;
  }
  for( i = 0; i < pKV->nCF; i++ ){
    if( pKV->apCF[i] ){
      if( pKV->apCF[i]->pReadCur ) kvstoreFreeCursor(pKV->apCF[i]->pReadCur);
      if( pKV->apCF[i]->pMutex )   sqlite3_mutex_free(pKV->apCF[i]->pMutex);
      sqlite3_free(pKV->apCF[i]->zName);
      sqlite3_free(pKV->apCF[i]);
    }
  }
  sqlite3_free(pKV->apCF);
  if( pKV->pBt ) sqlite3BtreeClose(pKV->pBt);
  sqlite3_free(pKV->pKeyInfo);
  if( pKV->zErrMsg ) sqlite3_free(pKV->zErrMsg);
  /* Free the busy-handler context if we allocated one */
  if( pKV->db ){
    sqlite3_free(pKV->db->busyHandler.pBusyArg);
    if( pKV->db->mutex ) sqlite3_mutex_free(pKV->db->mutex);
    sqlite3_free(pKV->db);
  }
}

/*
** Increment the commit counter and run a PASSIVE checkpoint when the
** walSizeLimit threshold is reached. Called in the TRANS_NONE window
** (after sqlite3BtreeCommit, before the read transaction is restored).
*/
static void kvstoreAutoCheckpoint(KVStore *pKV){
  if( pKV->walSizeLimit > 0 ){
    pKV->walCommits++;
    if( pKV->walCommits >= pKV->walSizeLimit ){
      pKV->walCommits = 0;
      sqlite3BtreeCheckpoint(pKV->pBt, SQLITE_CHECKPOINT_PASSIVE, NULL, NULL);
    }
  }
}

/*
** Open a key-value store with full configuration control.
*/
int kvstore_open_v2(
  const char *zFilename,
  KVStore **ppKV,
  const KVStoreConfig *pConfig
){
  KVStore *pKV;
  int rc;

  /* Resolve configuration — use defaults for any zero/unset field */
  int journalMode  = pConfig ? pConfig->journalMode  : KVSTORE_JOURNAL_WAL;
  int syncLevel    = pConfig ? pConfig->syncLevel    : KVSTORE_SYNC_NORMAL;
  int cacheSize    = pConfig ? pConfig->cacheSize    : 0;
  int pageSize     = pConfig ? pConfig->pageSize     : 0;
  int readOnly     = pConfig ? pConfig->readOnly     : 0;
  int busyTimeout  = pConfig ? pConfig->busyTimeout  : 0;

  if( cacheSize <= 0 ) cacheSize = KVSTORE_DEFAULT_CACHE_SIZE;

  if( ppKV == NULL ){
    return KVSTORE_ERROR;
  }
  *ppKV = NULL;

  /* One-time SQLite library initialisation */
  rc = sqlite3_initialize();
  if( rc != SQLITE_OK ) return rc;

  /* Allocate KVStore structure */
  pKV = (KVStore*)sqlite3MallocZero(sizeof(KVStore));
  if( pKV == NULL ){
    return KVSTORE_NOMEM;
  }

  /* Create main mutex */
  pKV->pMutex = sqlite3MutexAlloc(SQLITE_MUTEX_RECURSIVE);
  if( !pKV->pMutex ){
    sqlite3_free(pKV);
    return KVSTORE_NOMEM;
  }

  /* Create a minimal sqlite3 structure (required by btree) */
  pKV->db = (sqlite3*)sqlite3MallocZero(sizeof(sqlite3));
  if( pKV->db == NULL ){
    sqlite3_mutex_free(pKV->pMutex);
    sqlite3_free(pKV);
    return KVSTORE_NOMEM;
  }
  pKV->db->mutex = sqlite3MutexAlloc(SQLITE_MUTEX_RECURSIVE);
  pKV->db->aLimit[SQLITE_LIMIT_LENGTH] = SQLITE_MAX_LENGTH;

  /* Install busy handler if a positive timeout was requested */
  if( busyTimeout > 0 ){
    KVBusyCtx *ctx = (KVBusyCtx*)sqlite3MallocZero(sizeof(KVBusyCtx));
    if( !ctx ){
      sqlite3_mutex_free(pKV->db->mutex);
      sqlite3_free(pKV->db);
      sqlite3_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return KVSTORE_NOMEM;
    }
    ctx->timeoutMs = busyTimeout;
    ctx->pVfs      = sqlite3_vfs_find(0);
    pKV->db->busyHandler.xBusyHandler = kvstoreBusyHandler;
    pKV->db->busyHandler.pBusyArg     = ctx;
    pKV->db->busyTimeout              = busyTimeout;
  }

  /* Choose VFS flags based on readOnly */
  int vfsFlags = readOnly
    ? (SQLITE_OPEN_READONLY  | SQLITE_OPEN_MAIN_DB)
    : (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);

  /* Open the btree */
  rc = sqlite3BtreeOpen(
    sqlite3_vfs_find(0),
    zFilename,
    pKV->db,
    &pKV->pBt,
    0,           /* no btree flags */
    vfsFlags
  );
  if( rc != SQLITE_OK ){
    kvstoreSetError(pKV, "failed to open btree: error %d", rc);
    kvstoreTeardownNoLock(pKV);
    sqlite3_mutex_free(pKV->pMutex);
    sqlite3_free(pKV);
    return rc;
  }

  /* Set page size for new databases (must be before any write transaction).
  ** sqlite3BtreeSetPageSize returns SQLITE_READONLY for existing databases
  ** (page size already fixed) — silently ignore that. */
  if( pageSize > 0 ){
    int psRc = sqlite3BtreeSetPageSize(pKV->pBt, pageSize, -1, 0);
    if( psRc != SQLITE_OK && psRc != SQLITE_READONLY ){
      kvstoreSetError(pKV, "failed to set page size: error %d", psRc);
      kvstoreTeardownNoLock(pKV);
      sqlite3_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return psRc;
    }
  }

  /* Set cache size */
  sqlite3BtreeSetCacheSize(pKV->pBt, cacheSize);

  /* Set synchronous level via pager flags.
  ** KVSTORE_SYNC_* maps to PAGER_SYNCHRONOUS_* with a +1 offset:
  **   KVSTORE_SYNC_OFF    (0) → PAGER_SYNCHRONOUS_OFF    (0x01)
  **   KVSTORE_SYNC_NORMAL (1) → PAGER_SYNCHRONOUS_NORMAL (0x02)
  **   KVSTORE_SYNC_FULL   (2) → PAGER_SYNCHRONOUS_FULL   (0x03)
  ** PAGER_CACHESPILL allows dirty pages to spill under memory pressure. */
  {
    unsigned syncFlag = (unsigned)(syncLevel + 1) | PAGER_CACHESPILL;
    sqlite3_mutex_enter(pKV->db->mutex);
    sqlite3BtreeSetPagerFlags(pKV->pBt, syncFlag);
    sqlite3_mutex_leave(pKV->db->mutex);
  }

  if( !readOnly ){
    /* Enable incremental auto-vacuum.  Must happen before SetVersion.
    ** SQLITE_READONLY returned for existing databases — that is expected. */
    int avRc = sqlite3BtreeSetAutoVacuum(pKV->pBt, BTREE_AUTOVACUUM_INCR);
    if( avRc != SQLITE_OK && avRc != SQLITE_READONLY ){
      kvstoreSetError(pKV, "failed to set auto-vacuum mode: error %d", avRc);
      kvstoreTeardownNoLock(pKV);
      sqlite3_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return avRc;
    }

    /* Set journal mode (WAL or delete-journal).
    ** sqlite3BtreeSetVersion(2) writes the WAL marker into the header.
    ** sqlite3BtreeSetVersion(1) reverts to rollback-journal mode. */
    {
      int ver = (journalMode == KVSTORE_JOURNAL_WAL) ? 2 : 1;
      rc = sqlite3BtreeSetVersion(pKV->pBt, ver);
      if( rc == SQLITE_OK ){
        rc = sqlite3BtreeCommit(pKV->pBt);  /* SetVersion leaves a txn open */
      }
      if( rc != SQLITE_OK ){
        kvstoreSetError(pKV, "failed to set journal mode: error %d", rc);
        kvstoreTeardownNoLock(pKV);
        sqlite3_mutex_free(pKV->pMutex);
        sqlite3_free(pKV);
        return rc;
      }
    }
  }

  /* Check whether this is a new or existing database */
  u32 defaultCFRoot = 0;
  u32 cfMetaRoot    = 0;
  int needsInit     = 0;

  rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
  if( rc != SQLITE_OK ){
    kvstoreSetError(pKV, "failed to begin read transaction: error %d", rc);
    kvstoreTeardownNoLock(pKV);
    sqlite3_mutex_free(pKV->pMutex);
    sqlite3_free(pKV);
    return rc;
  }
  sqlite3BtreeGetMeta(pKV->pBt, META_DEFAULT_CF_ROOT, &defaultCFRoot);
  sqlite3BtreeGetMeta(pKV->pBt, META_CF_METADATA_ROOT, &cfMetaRoot);
  sqlite3BtreeCommit(pKV->pBt);

  if( defaultCFRoot == 0 ){
    needsInit = 1;
  }

  if( needsInit ){
    if( readOnly ){
      /* Cannot initialise a brand-new database in read-only mode */
      kvstoreSetError(pKV, "cannot open empty database read-only");
      kvstoreTeardownNoLock(pKV);
      sqlite3_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return KVSTORE_READONLY;
    }
    rc = createDefaultCF(pKV);
    if( rc != SQLITE_OK ){
      kvstoreTeardownNoLock(pKV);
      sqlite3_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return rc;
    }
    rc = initCFMetadataTable(pKV);
    if( rc != SQLITE_OK ){
      kvstoreTeardownNoLock(pKV);
      sqlite3_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return rc;
    }
  }else{
    /* Existing database — open the default CF */
    KVColumnFamily *pCF = (KVColumnFamily*)sqlite3MallocZero(sizeof(KVColumnFamily));
    if( !pCF ){
      kvstoreTeardownNoLock(pKV);
      sqlite3_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return KVSTORE_NOMEM;
    }
    pCF->pKV    = pKV;
    pCF->zName  = sqliteStrDup(DEFAULT_CF_NAME);
    pCF->iTable = (int)defaultCFRoot;
    pCF->refCount = 1;
    pCF->pMutex = sqlite3MutexAlloc(SQLITE_MUTEX_RECURSIVE);
    if( !pCF->pMutex ){
      sqlite3_free(pCF->zName);
      sqlite3_free(pCF);
      kvstoreTeardownNoLock(pKV);
      sqlite3_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return KVSTORE_NOMEM;
    }
    pKV->pDefaultCF = pCF;
    pKV->iMetaTable = (int)cfMetaRoot;
  }

  /* Allocate a shared KeyInfo used by all BLOBKEY data-table cursors */
  {
    KeyInfo *pKI = (KeyInfo*)sqlite3MallocZero(SZ_KEYINFO(1));
    if( !pKI ){
      kvstoreTeardownNoLock(pKV);
      sqlite3_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return KVSTORE_NOMEM;
    }
    pKI->nRef      = 1;
    pKI->enc       = SQLITE_UTF8;
    pKI->nKeyField = 1;
    pKI->nAllField = 1;
    pKI->db        = pKV->db;
    pKI->aSortFlags = 0;
    pKI->aColl[0]  = 0;
    pKV->pKeyInfo  = pKI;
  }

  /* Keep a persistent read transaction open to avoid per-operation
  ** BeginTrans/Commit overhead on the hot read path. */
  {
    int rcR = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rcR == SQLITE_OK ) pKV->inTrans = 1;
    /* Non-fatal if it fails; auto-trans will handle individual operations. */
  }

  /* WAL auto-checkpoint config (walCommits starts at 0 via sqlite3MallocZero). */
  pKV->walSizeLimit = pConfig ? pConfig->walSizeLimit : 0;

  *ppKV = pKV;
  return KVSTORE_OK;
}

/*
** Open a key-value store database file (simplified interface).
** Delegates to kvstore_open_v2 with journalMode set and all other
** fields at their defaults.
*/
int kvstore_open(
  const char *zFilename,
  KVStore **ppKV,
  int journalMode
){
  KVStoreConfig cfg;
  memset(&cfg, 0, sizeof(cfg));
  cfg.journalMode = journalMode;
  return kvstore_open_v2(zFilename, ppKV, &cfg);
}

/*
** Close a key-value store.
*/
int kvstore_close(KVStore *pKV){
  int rc, i;

  if( pKV == NULL ){
    return KVSTORE_OK;
  }

  /* Acquire mutex and signal closure so racing threads bail out cleanly */
  sqlite3_mutex_enter(pKV->pMutex);
  pKV->closing = 1;

  /* Rollback any active transaction */
  if( pKV->inTrans ){
    sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
    pKV->inTrans = 0;
  }

  /* Close all open column families (free cached cursors before struct) */
  if( pKV->pDefaultCF ){
    if( pKV->pDefaultCF->pReadCur ){
      kvstoreFreeCursor(pKV->pDefaultCF->pReadCur);
    }
    if( pKV->pDefaultCF->pMutex ){
      sqlite3_mutex_free(pKV->pDefaultCF->pMutex);
    }
    sqlite3_free(pKV->pDefaultCF->zName);
    sqlite3_free(pKV->pDefaultCF);
  }

  for(i = 0; i < pKV->nCF; i++){
    if( pKV->apCF[i] ){
      if( pKV->apCF[i]->pReadCur ){
        kvstoreFreeCursor(pKV->apCF[i]->pReadCur);
      }
      if( pKV->apCF[i]->pMutex ){
        sqlite3_mutex_free(pKV->apCF[i]->pMutex);
      }
      sqlite3_free(pKV->apCF[i]->zName);
      sqlite3_free(pKV->apCF[i]);
    }
  }
  sqlite3_free(pKV->apCF);

  /* Close the btree */
  rc = sqlite3BtreeClose(pKV->pBt);

  /* Free shared KeyInfo */
  sqlite3_free(pKV->pKeyInfo);

  /* Free error message */
  if( pKV->zErrMsg ){
    sqlite3_free(pKV->zErrMsg);
  }

  /* Free db resources (including busy-handler context if installed) */
  if( pKV->db->busyHandler.pBusyArg ){
    sqlite3_free(pKV->db->busyHandler.pBusyArg);
  }
  if( pKV->db->mutex ){
    sqlite3_mutex_free(pKV->db->mutex);
  }
  sqlite3_free(pKV->db);

  /* Release and free mutex */
  sqlite3_mutex_leave(pKV->pMutex);
  sqlite3_mutex_free(pKV->pMutex);

  sqlite3_free(pKV);

  return rc;
}

/*
** Begin a transaction.
*/
int kvstore_begin(KVStore *pKV, int wrflag){
  int rc;

  if( pKV == NULL ){
    return KVSTORE_ERROR;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot begin transaction: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }

  if( pKV->inTrans == 2 ){
    /* Write transaction already active — reject */
    kvstoreSetError(pKV, "transaction already active");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->inTrans == 1 && !wrflag ){
    /* Persistent read is already open; just reuse it */
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_OK;
  }

  /* inTrans==1 && wrflag: SQLite forbids upgrading a read that holds the
  ** WAL checkpoint slot (readLock==0) — always SQLITE_BUSY.  Release the
  ** persistent read cleanly and then open a fresh write transaction instead.
  ** inTrans==0          : open a fresh read or write transaction directly. */
  if( pKV->inTrans == 1 ){
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }

  rc = sqlite3BtreeBeginTrans(pKV->pBt, wrflag, 0);
  if( rc == SQLITE_OK ){
    pKV->inTrans = wrflag ? 2 : 1;
  }else{
    kvstoreCheckCorruption(pKV, rc);
    kvstoreSetError(pKV, "failed to begin transaction: error %d", rc);
  }

  sqlite3_mutex_leave(pKV->pMutex);
  return rc;
}

/*
** Commit the current transaction.
*/
int kvstore_commit(KVStore *pKV){
  int rc;

  if( pKV == NULL ){
    return KVSTORE_ERROR;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( !pKV->inTrans ){
    kvstoreSetError(pKV, "no active transaction to commit");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  rc = sqlite3BtreeCommit(pKV->pBt);
  if( rc == SQLITE_OK ){
    pKV->inTrans = 0;
    kvstoreAutoCheckpoint(pKV);
    /* Restore the persistent read transaction so that subsequent get/exists
    ** calls avoid per-call BeginTrans/Commit overhead. */
    if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ){
      pKV->inTrans = 1;
    }
  }else{
    kvstoreCheckCorruption(pKV, rc);
    kvstoreSetError(pKV, "failed to commit transaction: error %d", rc);
  }

  sqlite3_mutex_leave(pKV->pMutex);
  return rc;
}

/*
** Rollback the current transaction.
*/
int kvstore_rollback(KVStore *pKV){
  int rc;

  if( pKV == NULL ){
    return KVSTORE_ERROR;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( !pKV->inTrans ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_OK; /* No transaction to rollback */
  }

  rc = sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
  pKV->inTrans = 0;

  if( rc != SQLITE_OK ){
    kvstoreCheckCorruption(pKV, rc);
    kvstoreSetError(pKV, "failed to rollback transaction: error %d", rc);
  }

  /* Restore the persistent read transaction after rollback. */
  if( !pKV->closing ){
    if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ){
      pKV->inTrans = 1;
    }
  }

  sqlite3_mutex_leave(pKV->pMutex);
  return rc;
}

/*
** Run a WAL checkpoint on the database.
*/
int kvstore_checkpoint(KVStore *pKV, int mode, int *pnLog, int *pnCkpt){
  int rc;
  if( pKV == NULL ) return KVSTORE_ERROR;

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->inTrans == 2 ){
    kvstoreSetError(pKV, "commit or rollback the write transaction first");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_BUSY;
  }

  if( pnLog  ) *pnLog  = 0;
  if( pnCkpt ) *pnCkpt = 0;

  /* Release the persistent read transaction — sqlite3BtreeCheckpoint
  ** requires TRANS_NONE (returns SQLITE_LOCKED otherwise). */
  if( pKV->inTrans == 1 ){
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }

#ifndef SQLITE_OMIT_WAL
  rc = sqlite3BtreeCheckpoint(pKV->pBt, mode, pnLog, pnCkpt);
#else
  rc = SQLITE_OK;
#endif

  /* Restore the persistent read transaction. */
  if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ){
    pKV->inTrans = 1;
  }

  if( rc != SQLITE_OK ){
    kvstoreSetError(pKV, "checkpoint failed: error %d", rc);
  }

  sqlite3_mutex_leave(pKV->pMutex);
  return rc;
}

/* Forward declaration for CF operations */
static int kvstore_cf_put_internal(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  const void *pValue, int nValue
);
static int kvstore_cf_get_internal(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  void **ppValue, int *pnValue
);
static int kvstore_cf_delete_internal(
  KVColumnFamily *pCF,
  const void *pKey, int nKey
);
static int kvstore_cf_exists_internal(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  int *pExists
);

/*
** Default CF operations (wrappers)
*/
int kvstore_put(KVStore *pKV, const void *pKey, int nKey,
                const void *pValue, int nValue){
  if( !pKV || !pKV->pDefaultCF ) return KVSTORE_ERROR;
  return kvstore_cf_put_internal(pKV->pDefaultCF, pKey, nKey, pValue, nValue);
}

int kvstore_get(KVStore *pKV, const void *pKey, int nKey,
                void **ppValue, int *pnValue){
  if( !pKV || !pKV->pDefaultCF ) return KVSTORE_ERROR;
  return kvstore_cf_get_internal(pKV->pDefaultCF, pKey, nKey, ppValue, pnValue);
}

int kvstore_delete(KVStore *pKV, const void *pKey, int nKey){
  if( !pKV || !pKV->pDefaultCF ) return KVSTORE_ERROR;
  return kvstore_cf_delete_internal(pKV->pDefaultCF, pKey, nKey);
}

int kvstore_exists(KVStore *pKV, const void *pKey, int nKey, int *pExists){
  if( !pKV || !pKV->pDefaultCF ) return KVSTORE_ERROR;
  return kvstore_cf_exists_internal(pKV->pDefaultCF, pKey, nKey, pExists);
}

/*
** Get default column family
*/
int kvstore_cf_get_default(KVStore *pKV, KVColumnFamily **ppCF){
  if( !pKV || !ppCF ){
    return KVSTORE_ERROR;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( !pKV->pDefaultCF ){
    kvstoreSetError(pKV, "default column family not initialized");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  pKV->pDefaultCF->refCount++;
  *ppCF = pKV->pDefaultCF;

  sqlite3_mutex_leave(pKV->pMutex);
  return KVSTORE_OK;
}

/*
** Create a new column family
*/
int kvstore_cf_create(KVStore *pKV, const char *zName, KVColumnFamily **ppCF){
  int rc;
  BtCursor *pCur = NULL;
  KVColumnFamily *pCF = NULL;
  int autoTrans = 0;
  Pgno pgno;
  u32 cfCount;
  i64 nNameLen;
  char *zNameCopy = NULL;

  if( !pKV || !zName || !ppCF ){
    if( pKV ) kvstoreSetError(pKV, "invalid parameters to cf_create");
    return KVSTORE_ERROR;
  }

  *ppCF = NULL;

  nNameLen = strlen(zName);
  if( nNameLen == 0 || nNameLen > KVSTORE_MAX_CF_NAME ){
    sqlite3_mutex_enter(pKV->pMutex);
    kvstoreSetError(pKV, "invalid column family name length");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  /* Check if default CF */
  if( strcmp(zName, DEFAULT_CF_NAME) == 0 ){
    return kvstore_cf_get_default(pKV, ppCF);
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  /* Start write transaction (release persistent read first if active). */
  if( pKV->inTrans != 2 ){
    if( pKV->inTrans == 1 ){
      sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 1, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 2;
  }

  /* Check if CF already exists – search metadata table */
  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 0, 0, pCur);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  int found = 0;
  rc = kvstoreMetaSeekKey(pCur, zName, (int)nNameLen, &found, NULL);
  kvstoreFreeCursor(pCur);
  pCur = NULL;

  if( rc != SQLITE_OK ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }
  if( found ){
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    kvstoreSetError(pKV, "column family already exists: %s", zName);
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  /* Create new btree table for this CF (BLOBKEY) */
  rc = sqlite3BtreeCreateTable(pKV->pBt, &pgno, BTREE_BLOBKEY);
  if( rc != SQLITE_OK ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  /* Store CF metadata in the metadata table.
  ** The metadata table is also INTKEY.  We hash the CF name to get
  ** a rowid and store [name_len | name | table_root (4 bytes BE)] as data.
  */
  {
    unsigned char tableRootBytes[4];
    int iTable = (int)pgno;
    tableRootBytes[0] = (iTable >> 24) & 0xFF;
    tableRootBytes[1] = (iTable >> 16) & 0xFF;
    tableRootBytes[2] = (iTable >>  8) & 0xFF;
    tableRootBytes[3] = iTable & 0xFF;

    unsigned char metaStack[320]; /* enough for max CF name (255) + 4 + 4 + header */
    unsigned char *pEncoded;
    int nEncoded = kvstoreEncodeBlob(zName, (int)nNameLen,
                                     tableRootBytes, 4,
                                     metaStack, (int)sizeof(metaStack),
                                     &pEncoded);
    if( nEncoded < 0 ){
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      sqlite3_mutex_leave(pKV->pMutex);
      return SQLITE_NOMEM;
    }

    pCur = kvstoreAllocCursor();
    if( !pCur ){
      if( pEncoded != metaStack ) sqlite3_free(pEncoded);
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      sqlite3_mutex_leave(pKV->pMutex);
      return SQLITE_NOMEM;
    }
    rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 1, 0, pCur);
    if( rc != SQLITE_OK ){
      if( pEncoded != metaStack ) sqlite3_free(pEncoded);
      kvstoreFreeCursor(pCur);
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }

    i64 slot;
    rc = kvstoreMetaFindSlot(pCur, zName, (int)nNameLen, &slot);
    if( rc != SQLITE_OK ){
      if( pEncoded != metaStack ) sqlite3_free(pEncoded);
      kvstoreFreeCursor(pCur);
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }

    BtreePayload payload;
    memset(&payload, 0, sizeof(payload));
    payload.nKey  = slot;
    payload.pData = pEncoded;
    payload.nData = nEncoded;

    rc = sqlite3BtreeInsert(pCur, &payload, 0, 0);
    if( pEncoded != metaStack ) sqlite3_free(pEncoded);
    kvstoreFreeCursor(pCur);
    pCur = NULL;

    if( rc != SQLITE_OK ){
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }
  }

  /* Update CF count */
  sqlite3BtreeGetMeta(pKV->pBt, META_CF_COUNT, &cfCount);
  cfCount++;
  sqlite3BtreeUpdateMeta(pKV->pBt, META_CF_COUNT, cfCount);

  /* Commit if we started the transaction, then restore persistent read. */
  if( autoTrans ){
    rc = sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }
    if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ){
      pKV->inTrans = 1;
    }
  }

  /* Create CF structure */
  pCF = (KVColumnFamily*)sqlite3MallocZero(sizeof(KVColumnFamily));
  if( !pCF ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  zNameCopy = sqliteStrDup(zName);
  if( !zNameCopy ){
    sqlite3_free(pCF);
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  pCF->pKV = pKV;
  pCF->zName = zNameCopy;
  pCF->iTable = (int)pgno;
  pCF->refCount = 1;

  /* Create mutex for this CF */
  pCF->pMutex = sqlite3MutexAlloc(SQLITE_MUTEX_RECURSIVE);
  if( !pCF->pMutex ){
    sqlite3_free(zNameCopy);
    sqlite3_free(pCF);
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  sqlite3_mutex_leave(pKV->pMutex);

  *ppCF = pCF;
  return KVSTORE_OK;
}

/*
** Open existing column family
*/
int kvstore_cf_open(KVStore *pKV, const char *zName, KVColumnFamily **ppCF){
  int rc;
  BtCursor *pCur = NULL;
  KVColumnFamily *pCF = NULL;
  int autoTrans = 0;
  i64 nNameLen;
  int iTable;
  char *zNameCopy = NULL;

  if( !pKV || !zName || !ppCF ){
    if( pKV ){
      sqlite3_mutex_enter(pKV->pMutex);
      kvstoreSetError(pKV, "invalid parameters to cf_open");
      sqlite3_mutex_leave(pKV->pMutex);
    }
    return KVSTORE_ERROR;
  }

  *ppCF = NULL;
  nNameLen = strlen(zName);

  /* Check if default CF */
  if( strcmp(zName, DEFAULT_CF_NAME) == 0 ){
    return kvstore_cf_get_default(pKV, ppCF);
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  /* Start read transaction if needed */
  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 1;
  }

  /* Look up CF in metadata table */
  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 0, 0, pCur);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  int found = 0;
  i64 foundRowid = 0;
  rc = kvstoreMetaSeekKey(pCur, zName, (int)nNameLen, &found, &foundRowid);
  if( rc != SQLITE_OK || !found ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return (rc == SQLITE_OK) ? KVSTORE_NOTFOUND : rc;
  }

  /* Re-position cursor on the found rowid and read data */
  {
    int res = 0;
    rc = sqlite3BtreeTableMoveto(pCur, foundRowid, 0, &res);
    if( rc != SQLITE_OK || res != 0 ){
      kvstoreFreeCursor(pCur);
      if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
      sqlite3_mutex_leave(pKV->pMutex);
      return KVSTORE_CORRUPT;
    }
  }

  /* Read the payload: [name_len(4) | name | table_root(4)] */
  u32 payloadSz = sqlite3BtreePayloadSize(pCur);
  if( (int)payloadSz < 4 + (int)nNameLen + 4 ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }

  /* Read table root bytes (last 4 bytes of value portion) */
  unsigned char tableRootBytes[4];
  rc = sqlite3BtreePayload(pCur, 4 + (int)nNameLen, 4, tableRootBytes);
  kvstoreFreeCursor(pCur);
  pCur = NULL;

  if( autoTrans ){
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }

  if( rc != SQLITE_OK ){
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  iTable = (tableRootBytes[0] << 24) | (tableRootBytes[1] << 16) |
           (tableRootBytes[2] << 8) | tableRootBytes[3];

  /* Create CF structure */
  pCF = (KVColumnFamily*)sqlite3MallocZero(sizeof(KVColumnFamily));
  if( !pCF ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  zNameCopy = sqliteStrDup(zName);
  if( !zNameCopy ){
    sqlite3_free(pCF);
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  pCF->pKV = pKV;
  pCF->zName = zNameCopy;
  pCF->iTable = iTable;
  pCF->refCount = 1;

  /* Create mutex for this CF */
  pCF->pMutex = sqlite3MutexAlloc(SQLITE_MUTEX_RECURSIVE);
  if( !pCF->pMutex ){
    sqlite3_free(zNameCopy);
    sqlite3_free(pCF);
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  sqlite3_mutex_leave(pKV->pMutex);

  *ppCF = pCF;
  return KVSTORE_OK;
}

/*
** Close column family handle
*/
void kvstore_cf_close(KVColumnFamily *pCF){
  KVStore *pKV;
  int shouldFree = 0;

  if( !pCF ) return;

  pKV = pCF->pKV;

  sqlite3_mutex_enter(pKV->pMutex);

  pCF->refCount--;
  if( pCF->refCount <= 0 && pCF != pKV->pDefaultCF ){
    shouldFree = 1;
  }

  sqlite3_mutex_leave(pKV->pMutex);

  if( shouldFree ){
    if( pCF->pReadCur ){
      kvstoreFreeCursor(pCF->pReadCur);
    }
    if( pCF->pMutex ){
      sqlite3_mutex_free(pCF->pMutex);
    }
    sqlite3_free(pCF->zName);
    sqlite3_free(pCF);
  }
}

/*
** Internal implementation: Put to column family
*/
static int kvstore_cf_put_internal(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  const void *pValue, int nValue
){
  BtCursor *pCur = NULL;
  int rc;
  int autoTrans = 0;
  KVStore *pKV = pCF->pKV;

  sqlite3_mutex_enter(pCF->pMutex);
  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot put: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }

  rc = kvstoreValidateKeyValue(pKV, pKey, nKey, pValue, nValue);
  if( rc != KVSTORE_OK ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return rc;
  }

  if( pKV->inTrans != 2 ){
    /* Release persistent read (if any) before starting a write — SQLite
    ** forbids upgrading when readLock==0 (WAL checkpoint slot). */
    if( pKV->inTrans == 1 ){
      sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 1, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      sqlite3_mutex_leave(pCF->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 2;
  }

  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pCF->iTable, 1, pKV->pKeyInfo, pCur);
  if( rc != SQLITE_OK ){
    kvstoreCheckCorruption(pKV, rc);
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return rc;
  }

  /* Encode [key_len | key | value] as a single BLOBKEY entry.
  ** Use a stack buffer for small payloads to avoid a heap allocation. */
  {
    unsigned char stackBuf[512];
    unsigned char *pEncoded;
    int nEncoded = kvstoreEncodeBlob(pKey, nKey, pValue, nValue,
                                     stackBuf, (int)sizeof(stackBuf),
                                     &pEncoded);
    if( nEncoded < 0 ){
      kvstoreFreeCursor(pCur);
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      sqlite3_mutex_leave(pKV->pMutex);
      sqlite3_mutex_leave(pCF->pMutex);
      return SQLITE_NOMEM;
    }

    BtreePayload payload;
    memset(&payload, 0, sizeof(payload));
    payload.pKey  = pEncoded;
    payload.nKey  = nEncoded;
    /* pData and nData are 0 for index/BLOBKEY tables */

    rc = sqlite3BtreeInsert(pCur, &payload, 0, 0);
    if( pEncoded != stackBuf ) sqlite3_free(pEncoded);
  }
  kvstoreFreeCursor(pCur);

  if( rc == SQLITE_OK ){
    pKV->stats.nPuts++;
    if( autoTrans ){
      rc = sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
      /* Restore persistent read transaction */
      if( rc == SQLITE_OK ){
        kvstoreAutoCheckpoint(pKV);
        if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ){
          pKV->inTrans = 1;
        }
      }
    }
  }else{
    kvstoreCheckCorruption(pKV, rc);
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
      pKV->inTrans = 0;
      /* Restore persistent read transaction */
      if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ){
        pKV->inTrans = 1;
      }
    }
  }

  sqlite3_mutex_leave(pKV->pMutex);
  sqlite3_mutex_leave(pCF->pMutex);

  return rc;
}

/*
** Internal implementation: Get from column family
*/
static int kvstore_cf_get_internal(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  void **ppValue, int *pnValue
){
  BtCursor *pCur = NULL;
  int rc;
  KVStore *pKV = pCF->pKV;

  if( !ppValue || !pnValue ){
    sqlite3_mutex_enter(pKV->pMutex);
    kvstoreSetError(pKV, "invalid parameters to get");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  *ppValue = NULL;
  *pnValue = 0;

  sqlite3_mutex_enter(pCF->pMutex);
  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot get: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }

  if( !pKey || nKey <= 0 ){
    kvstoreSetError(pKV, "invalid key");
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }

  if( !pKV->inTrans ){
    /* Persistent read transaction normally keeps inTrans>=1; fall back if not. */
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      sqlite3_mutex_leave(pCF->pMutex);
      return rc;
    }
    pKV->inTrans = 1;
  }

  /* Use the cached read cursor — no malloc/free per call. */
  pCur = kvstoreGetReadCursor(pCF);
  if( !pCur ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return SQLITE_NOMEM;
  }

  int found = 0;
  rc = kvstoreSeekKey(pCur, pKV->pKeyInfo, pKey, nKey, &found);
  if( rc != SQLITE_OK || !found ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return (rc == SQLITE_OK) ? KVSTORE_NOTFOUND : rc;
  }

  /* Cursor is positioned on the matching entry — read the value portion. */
  u32 payloadSz = sqlite3BtreePayloadSize(pCur);
  int storedKeyLen = 0;
  {
    unsigned char hdr[4];
    rc = sqlite3BtreePayload(pCur, 0, 4, hdr);
    if( rc == SQLITE_OK ){
      storedKeyLen = (hdr[0]<<24)|(hdr[1]<<16)|(hdr[2]<<8)|hdr[3];
    }
  }

  int valueLen = (int)payloadSz - 4 - storedKeyLen;
  void *pValue = NULL;
  if( rc == SQLITE_OK && valueLen > 0 ){
    pValue = sqlite3Malloc(valueLen);
    if( !pValue ){
      rc = SQLITE_NOMEM;
    }else{
      rc = sqlite3BtreePayload(pCur, 4 + storedKeyLen, valueLen, pValue);
      if( rc != SQLITE_OK ){
        sqlite3_free(pValue);
        pValue = NULL;
      }
    }
  }

  if( rc == SQLITE_OK ){
    pKV->stats.nGets++;
    *ppValue = pValue;
    *pnValue = (valueLen > 0) ? valueLen : 0;
  }

  sqlite3_mutex_leave(pKV->pMutex);
  sqlite3_mutex_leave(pCF->pMutex);

  return rc;
}

/*
** Internal implementation: Delete from column family
*/
static int kvstore_cf_delete_internal(
  KVColumnFamily *pCF,
  const void *pKey, int nKey
){
  BtCursor *pCur = NULL;
  int rc;
  int autoTrans = 0;
  KVStore *pKV = pCF->pKV;

  sqlite3_mutex_enter(pCF->pMutex);
  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot delete: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }

  if( !pKey || nKey <= 0 ){
    kvstoreSetError(pKV, "invalid key");
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->inTrans != 2 ){
    /* Release persistent read (if any) before starting a write. */
    if( pKV->inTrans == 1 ){
      sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 1, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      sqlite3_mutex_leave(pCF->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 2;
  }

  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pCF->iTable, 1, pKV->pKeyInfo, pCur);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return rc;
  }

  int found = 0;
  rc = kvstoreSeekKey(pCur, pKV->pKeyInfo, pKey, nKey, &found);
  if( rc != SQLITE_OK || !found ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return (rc == SQLITE_OK) ? KVSTORE_NOTFOUND : rc;
  }

  /* Cursor is already positioned — delete the entry */
  rc = sqlite3BtreeDelete(pCur, 0);
  kvstoreFreeCursor(pCur);

  if( rc == SQLITE_OK ){
    pKV->stats.nDeletes++;
    if( autoTrans ){
      rc = sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
      /* Restore persistent read transaction */
      if( rc == SQLITE_OK ){
        kvstoreAutoCheckpoint(pKV);
        if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ){
          pKV->inTrans = 1;
        }
      }
    }
  }else{
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
      pKV->inTrans = 0;
      /* Restore persistent read transaction */
      if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ){
        pKV->inTrans = 1;
      }
    }
  }

  sqlite3_mutex_leave(pKV->pMutex);
  sqlite3_mutex_leave(pCF->pMutex);

  return rc;
}

/*
** Internal implementation: Check existence in column family
*/
static int kvstore_cf_exists_internal(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  int *pExists
){
  BtCursor *pCur = NULL;
  int rc;
  KVStore *pKV = pCF->pKV;

  if( !pExists ){
    sqlite3_mutex_enter(pKV->pMutex);
    kvstoreSetError(pKV, "invalid parameters to exists");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  *pExists = 0;

  sqlite3_mutex_enter(pCF->pMutex);
  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot check existence: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }

  if( !pKey || nKey <= 0 ){
    kvstoreSetError(pKV, "invalid key");
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }

  if( !pKV->inTrans ){
    /* Persistent read transaction normally keeps inTrans>=1; fall back if not. */
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      sqlite3_mutex_leave(pCF->pMutex);
      return rc;
    }
    pKV->inTrans = 1;
  }

  /* Use the cached read cursor — no malloc/free per call. */
  pCur = kvstoreGetReadCursor(pCF);
  if( !pCur ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return SQLITE_NOMEM;
  }

  int found = 0;
  rc = kvstoreSeekKey(pCur, pKV->pKeyInfo, pKey, nKey, &found);

  if( rc == SQLITE_OK ){
    *pExists = found;
  }

  sqlite3_mutex_leave(pKV->pMutex);
  sqlite3_mutex_leave(pCF->pMutex);

  return rc;
}

/*
** Public CF operations - just call internal versions
*/
int kvstore_cf_put(KVColumnFamily *pCF, const void *pKey, int nKey,
                   const void *pValue, int nValue){
  if( !pCF ) return KVSTORE_ERROR;
  return kvstore_cf_put_internal(pCF, pKey, nKey, pValue, nValue);
}

int kvstore_cf_get(KVColumnFamily *pCF, const void *pKey, int nKey,
                   void **ppValue, int *pnValue){
  if( !pCF ) return KVSTORE_ERROR;
  return kvstore_cf_get_internal(pCF, pKey, nKey, ppValue, pnValue);
}

int kvstore_cf_delete(KVColumnFamily *pCF, const void *pKey, int nKey){
  if( !pCF ) return KVSTORE_ERROR;
  return kvstore_cf_delete_internal(pCF, pKey, nKey);
}

int kvstore_cf_exists(KVColumnFamily *pCF, const void *pKey, int nKey, int *pExists){
  if( !pCF ) return KVSTORE_ERROR;
  return kvstore_cf_exists_internal(pCF, pKey, nKey, pExists);
}

/*
** Create an iterator for default CF
*/
int kvstore_iterator_create(KVStore *pKV, KVIterator **ppIter){
  if( !pKV || !pKV->pDefaultCF ){
    return KVSTORE_ERROR;
  }
  return kvstore_cf_iterator_create(pKV->pDefaultCF, ppIter);
}

/*
** Create an iterator for specific CF
*/
int kvstore_cf_iterator_create(KVColumnFamily *pCF, KVIterator **ppIter){
  KVIterator *pIter;
  int rc;
  KVStore *pKV;

  if( !pCF || !ppIter ){
    return KVSTORE_ERROR;
  }

  pKV = pCF->pKV;
  *ppIter = NULL;

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot create iterator: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }

  pIter = (KVIterator*)sqlite3MallocZero(sizeof(KVIterator));
  if( !pIter ){
    kvstoreSetError(pKV, "out of memory allocating iterator");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  pIter->pCF = pCF;
  pCF->refCount++;  /* Hold reference to CF */
  pIter->eof = 1;
  pIter->ownsTrans = 0;
  pIter->isValid = 1;

  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      sqlite3_free(pIter);
      pCF->refCount--;
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }
    pIter->ownsTrans = 1;
    pKV->inTrans = 1;
  }

  pIter->pCur = kvstoreAllocCursor();
  if( !pIter->pCur ){
    if( pIter->ownsTrans ){
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
      pKV->inTrans = 0;
    }
    pCF->refCount--;
    sqlite3_free(pIter);
    sqlite3_mutex_leave(pKV->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pCF->iTable, 0, pKV->pKeyInfo, pIter->pCur);
  if( rc != SQLITE_OK ){
    kvstoreCheckCorruption(pKV, rc);
    if( pIter->ownsTrans ){
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
      pKV->inTrans = 0;
    }
    pCF->refCount--;
    sqlite3_free(pIter->pCur);
    sqlite3_free(pIter);
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  pKV->stats.nIterations++;
  sqlite3_mutex_leave(pKV->pMutex);

  *ppIter = pIter;
  return KVSTORE_OK;
}

/*
** Create a prefix iterator for a specific column family.
** The iterator is positioned at the first key >= prefix.
** Subsequent calls to kvstore_iterator_next will stop when keys
** no longer start with the prefix.
*/
int kvstore_cf_prefix_iterator_create(
  KVColumnFamily *pCF,
  const void *pPrefix, int nPrefix,
  KVIterator **ppIter
){
  int rc;
  KVIterator *pIter;

  if( !pCF || !ppIter || !pPrefix || nPrefix <= 0 ){
    return KVSTORE_ERROR;
  }

  /* Create a normal iterator first */
  rc = kvstore_cf_iterator_create(pCF, ppIter);
  if( rc != KVSTORE_OK ) return rc;

  pIter = *ppIter;

  /* Store a copy of the prefix */
  pIter->pPrefix = sqlite3Malloc(nPrefix);
  if( !pIter->pPrefix ){
    kvstore_iterator_close(pIter);
    *ppIter = NULL;
    return KVSTORE_NOMEM;
  }
  memcpy(pIter->pPrefix, pPrefix, nPrefix);
  pIter->nPrefix = nPrefix;

  /* Position iterator at the first matching key */
  rc = kvstore_iterator_first(pIter);
  if( rc != KVSTORE_OK ){
    kvstore_iterator_close(pIter);
    *ppIter = NULL;
    return rc;
  }

  return KVSTORE_OK;
}

/*
** Create a prefix iterator for the default column family.
*/
int kvstore_prefix_iterator_create(
  KVStore *pKV,
  const void *pPrefix, int nPrefix,
  KVIterator **ppIter
){
  if( !pKV || !pKV->pDefaultCF ){
    return KVSTORE_ERROR;
  }
  return kvstore_cf_prefix_iterator_create(pKV->pDefaultCF, pPrefix, nPrefix, ppIter);
}

/*
** Check if the current cursor entry's key starts with the iterator's prefix.
** Returns 1 if match (or no prefix filter), 0 otherwise.
*/
static int kvstoreIterCheckPrefix(KVIterator *pIter){
  unsigned char hdr[4];
  int rc, keyLen;

  if( !pIter->pPrefix ) return 1;
  if( pIter->eof ) return 0;

  u32 payloadSz = sqlite3BtreePayloadSize(pIter->pCur);
  if( payloadSz < (u32)(4 + pIter->nPrefix) ) return 0;

  rc = sqlite3BtreePayload(pIter->pCur, 0, 4, hdr);
  if( rc != SQLITE_OK ) return 0;
  keyLen = (hdr[0]<<24)|(hdr[1]<<16)|(hdr[2]<<8)|hdr[3];
  if( keyLen < pIter->nPrefix ) return 0;

  /* Read just the prefix-length portion of the stored key and compare */
  {
    unsigned char stackBuf[256];
    void *pBuf = (pIter->nPrefix <= (int)sizeof(stackBuf))
                   ? stackBuf
                   : sqlite3Malloc(pIter->nPrefix);
    int match;
    if( !pBuf ) return 0;
    rc = sqlite3BtreePayload(pIter->pCur, 4, pIter->nPrefix, pBuf);
    match = (rc == SQLITE_OK && memcmp(pBuf, pIter->pPrefix, pIter->nPrefix) == 0);
    if( pBuf != stackBuf ) sqlite3_free(pBuf);
    return match;
  }
}

/*
** Move iterator to first entry
*/
int kvstore_iterator_first(KVIterator *pIter){
  int rc, res;

  if( !pIter || !pIter->isValid ){
    return KVSTORE_ERROR;
  }

  if( pIter->pPrefix ){
    /* Prefix iterator: seek to the first key >= prefix */
    KVStore *pKV = pIter->pCF->pKV;
    UnpackedRecord *pIdxKey = sqlite3VdbeAllocUnpackedRecord(pKV->pKeyInfo);
    if( !pIdxKey ) return SQLITE_NOMEM;

    pIdxKey->u.z       = (char *)pIter->pPrefix;
    pIdxKey->n         = pIter->nPrefix;
    pIdxKey->nField    = 1;
    pIdxKey->default_rc = 0;

    rc = sqlite3BtreeIndexMoveto(pIter->pCur, pIdxKey, &res);
    sqlite3DbFree(pKV->pKeyInfo->db, pIdxKey);
    if( rc != SQLITE_OK ) return rc;

    if( res < 0 ){
      /* Cursor is before the first matching key — advance */
      rc = sqlite3BtreeNext(pIter->pCur, 0);
      if( rc == SQLITE_DONE ){
        pIter->eof = 1;
        return SQLITE_OK;
      }
      if( rc != SQLITE_OK ) return rc;
    }
    pIter->eof = 0;
    if( !kvstoreIterCheckPrefix(pIter) ){
      pIter->eof = 1;
    }
  }else{
    rc = sqlite3BtreeFirst(pIter->pCur, &res);
    if( rc == SQLITE_OK ){
      pIter->eof = res;
    }
  }

  return rc;
}

/*
** Move iterator to next entry
*/
int kvstore_iterator_next(KVIterator *pIter){
  int rc;

  if( !pIter || !pIter->isValid ){
    return KVSTORE_ERROR;
  }

  if( pIter->eof ){
    return KVSTORE_OK;
  }

  rc = sqlite3BtreeNext(pIter->pCur, 0);
  if( rc == SQLITE_DONE ){
    pIter->eof = 1;
    rc = SQLITE_OK;
  }else if( rc == SQLITE_OK && pIter->pPrefix ){
    /* Check prefix bound — keys are sorted, so first mismatch means done */
    if( !kvstoreIterCheckPrefix(pIter) ){
      pIter->eof = 1;
    }
  }

  return rc;
}

/*
** Check if iterator is at end
*/
int kvstore_iterator_eof(KVIterator *pIter){
  if( !pIter || !pIter->isValid ){
    return 1;
  }
  return pIter->eof;
}

/*
** Get current key from iterator.
** Reads the encoded payload and extracts the key portion.
*/
int kvstore_iterator_key(KVIterator *pIter, void **ppKey, int *pnKey){
  int rc;

  if( !pIter || !pIter->isValid || !ppKey || !pnKey ){
    return KVSTORE_ERROR;
  }

  if( pIter->eof ){
    return KVSTORE_ERROR;
  }

  u32 payloadSz = sqlite3BtreePayloadSize(pIter->pCur);
  if( payloadSz < 4 ) return KVSTORE_CORRUPT;

  unsigned char hdr[4];
  rc = sqlite3BtreePayload(pIter->pCur, 0, 4, hdr);
  if( rc != SQLITE_OK ) return rc;

  int keyLen = (hdr[0]<<24)|(hdr[1]<<16)|(hdr[2]<<8)|hdr[3];
  if( keyLen <= 0 || 4 + keyLen > (int)payloadSz ) return KVSTORE_CORRUPT;

  if( pIter->nKeyBuf < keyLen ){
    void *pNew = sqlite3Realloc(pIter->pKeyBuf, keyLen);
    if( !pNew ) return KVSTORE_NOMEM;
    pIter->pKeyBuf = pNew;
    pIter->nKeyBuf = keyLen;
  }

  rc = sqlite3BtreePayload(pIter->pCur, 4, keyLen, pIter->pKeyBuf);
  if( rc != SQLITE_OK ) return rc;

  *ppKey = pIter->pKeyBuf;
  *pnKey = keyLen;

  return KVSTORE_OK;
}

/*
** Get current value from iterator.
** Reads the encoded payload and extracts the value portion.
*/
int kvstore_iterator_value(KVIterator *pIter, void **ppValue, int *pnValue){
  int rc;

  if( !pIter || !pIter->isValid || !ppValue || !pnValue ){
    return KVSTORE_ERROR;
  }

  if( pIter->eof ){
    return KVSTORE_ERROR;
  }

  u32 payloadSz = sqlite3BtreePayloadSize(pIter->pCur);
  if( payloadSz < 4 ) return KVSTORE_CORRUPT;

  unsigned char hdr[4];
  rc = sqlite3BtreePayload(pIter->pCur, 0, 4, hdr);
  if( rc != SQLITE_OK ) return rc;

  int keyLen = (hdr[0]<<24)|(hdr[1]<<16)|(hdr[2]<<8)|hdr[3];
  int valLen = (int)payloadSz - 4 - keyLen;
  if( valLen < 0 ) return KVSTORE_CORRUPT;

  if( valLen == 0 ){
    *ppValue = NULL;
    *pnValue = 0;
    return KVSTORE_OK;
  }

  if( pIter->nValBuf < valLen ){
    void *pNew = sqlite3Realloc(pIter->pValBuf, valLen);
    if( !pNew ) return KVSTORE_NOMEM;
    pIter->pValBuf = pNew;
    pIter->nValBuf = valLen;
  }

  rc = sqlite3BtreePayload(pIter->pCur, 4 + keyLen, valLen, pIter->pValBuf);
  if( rc != SQLITE_OK ) return rc;

  *ppValue = pIter->pValBuf;
  *pnValue = valLen;

  return KVSTORE_OK;
}

/*
** Close an iterator
*/
void kvstore_iterator_close(KVIterator *pIter){
  KVStore *pKV;

  if( !pIter ){
    return;
  }

  pIter->isValid = 0;
  pKV = pIter->pCF ? pIter->pCF->pKV : NULL;

  if( pIter->pCur ){
    kvstoreFreeCursor(pIter->pCur);
    pIter->pCur = NULL;
  }

  if( pIter->ownsTrans && pKV ){
    sqlite3_mutex_enter(pKV->pMutex);
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
    sqlite3_mutex_leave(pKV->pMutex);
  }

  if( pIter->pCF ){
    kvstore_cf_close(pIter->pCF);
  }

  if( pIter->pKeyBuf ){
    sqlite3_free(pIter->pKeyBuf);
  }

  if( pIter->pValBuf ){
    sqlite3_free(pIter->pValBuf);
  }

  if( pIter->pPrefix ){
    sqlite3_free(pIter->pPrefix);
  }

  sqlite3_free(pIter);
}

/*
** Get statistics
*/
int kvstore_stats(KVStore *pKV, KVStoreStats *pStats){
  if( !pKV || !pStats ){
    return KVSTORE_ERROR;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  pStats->nPuts = pKV->stats.nPuts;
  pStats->nGets = pKV->stats.nGets;
  pStats->nDeletes = pKV->stats.nDeletes;
  pStats->nIterations = pKV->stats.nIterations;
  pStats->nErrors = pKV->stats.nErrors;

  sqlite3_mutex_leave(pKV->pMutex);

  return KVSTORE_OK;
}

/*
** Perform database integrity check
*/
int kvstore_integrity_check(KVStore *pKV, char **pzErrMsg){
  int rc;
  Pgno *aRoot;
  int nRoot;
  int hadTrans = 0;
  int nErr = 0;
  char *zErr = NULL;

  if( !pKV ){
    return KVSTORE_ERROR;
  }

  if( pzErrMsg ){
    *pzErrMsg = NULL;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }
    hadTrans = 1;
    pKV->inTrans = 1;
  }

  /* Build array of all root pages to check.
  ** Include page 1 (master/schema root), default CF root, metadata table root,
  ** plus every CF root page stored in the metadata table.  This prevents
  ** false "Page N: never used" warnings from the integrity checker.
  */
  {
    int nAlloc = 16;
    nRoot = 0;
    aRoot = (Pgno*)sqlite3MallocZero(nAlloc * sizeof(Pgno));
    if( !aRoot ){
      if( hadTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
      sqlite3_mutex_leave(pKV->pMutex);
      return KVSTORE_NOMEM;
    }

    /* Page 1 is always used (file header / master page) */
    aRoot[nRoot++] = 1;
    aRoot[nRoot++] = (Pgno)pKV->pDefaultCF->iTable;
    if( pKV->iMetaTable ){
      aRoot[nRoot++] = (Pgno)pKV->iMetaTable;
    }

    /* Scan metadata table to find all CF root pages */
    if( pKV->iMetaTable ){
      BtCursor *pCur = kvstoreAllocCursor();
      if( pCur ){
        rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 0, 0, pCur);
        if( rc == SQLITE_OK ){
          int res;
          rc = sqlite3BtreeFirst(pCur, &res);
          while( rc == SQLITE_OK && !res ){
            u32 payloadSz = sqlite3BtreePayloadSize(pCur);
            if( payloadSz >= 8 ){
              unsigned char hdr[4];
              rc = sqlite3BtreePayload(pCur, 0, 4, hdr);
              if( rc == SQLITE_OK ){
                int nameLen = (hdr[0]<<24)|(hdr[1]<<16)|(hdr[2]<<8)|hdr[3];
                int rootOff = 4 + nameLen;
                if( rootOff + 4 <= (int)payloadSz ){
                  unsigned char rootBytes[4];
                  rc = sqlite3BtreePayload(pCur, rootOff, 4, rootBytes);
                  if( rc == SQLITE_OK ){
                    Pgno cfRoot = (rootBytes[0]<<24)|(rootBytes[1]<<16)
                                 |(rootBytes[2]<<8)|rootBytes[3];
                    if( cfRoot > 0 ){
                      if( nRoot >= nAlloc ){
                        nAlloc *= 2;
                        Pgno *aNew = sqlite3Realloc(aRoot, nAlloc*sizeof(Pgno));
                        if( !aNew ){ rc = SQLITE_NOMEM; break; }
                        aRoot = aNew;
                      }
                      aRoot[nRoot++] = cfRoot;
                    }
                  }
                }
              }
            }
            rc = sqlite3BtreeNext(pCur, 0);
            if( rc == SQLITE_DONE ){ rc = SQLITE_OK; break; }
          }
        }
        kvstoreFreeCursor(pCur);
      }
    }
  }

  /* Run the integrity check */
  rc = sqlite3BtreeIntegrityCheck(
    pKV->db,
    pKV->pBt,
    aRoot,       /* root pages */
    0,           /* aCnt (entry count output – not needed) */
    nRoot,       /* number of roots */
    100,         /* max errors to report */
    &nErr,       /* OUT: error count */
    &zErr        /* OUT: error message */
  );

  sqlite3_free(aRoot);

  if( hadTrans ){
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }

  if( nErr > 0 && zErr ){
    pKV->isCorrupted = 1;
    kvstoreSetError(pKV, "integrity check failed: %s", zErr);
    if( pzErrMsg ){
      *pzErrMsg = zErr;
    }else{
      sqlite3_free(zErr);
    }
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }

  if( zErr ) sqlite3_free(zErr);

  sqlite3_mutex_leave(pKV->pMutex);
  return KVSTORE_OK;
}

/*
** Synchronize database to disk.
** In SQLite 3.51.200 there is no sqlite3BtreeSync.
** We do a commit+begin cycle to flush to disk if a write transaction
** is active, otherwise this is a no-op (reads are already consistent).
*/
int kvstore_sync(KVStore *pKV){
  int rc = SQLITE_OK;

  if( !pKV ){
    return KVSTORE_ERROR;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot sync: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }

  if( pKV->inTrans == 2 ){
    /* Write transaction active – commit and re-open */
    rc = sqlite3BtreeCommit(pKV->pBt);
    if( rc == SQLITE_OK ){
      pKV->inTrans = 0;
    }else{
      kvstoreCheckCorruption(pKV, rc);
      kvstoreSetError(pKV, "failed to sync database: error %d", rc);
    }
  }

  sqlite3_mutex_leave(pKV->pMutex);
  return rc;
}

/*
** Run incremental vacuum, freeing up to nPage pages.
** Pass nPage=0 to free all unused pages.
*/
int kvstore_incremental_vacuum(KVStore *pKV, int nPage){
  int rc;

  if( !pKV ){
    return KVSTORE_ERROR;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot vacuum: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }

  /* Begin a write transaction if none is active */
  int autoTrans = 0;
  if( pKV->inTrans == 0 ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 2, 0);
    if( rc != SQLITE_OK ){
      kvstoreCheckCorruption(pKV, rc);
      kvstoreSetError(pKV, "failed to begin transaction for vacuum: error %d", rc);
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }
    pKV->inTrans = 2;
    autoTrans = 1;
  }else if( pKV->inTrans == 1 ){
    /* Upgrade read transaction to write */
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 2, 0);
    if( rc != SQLITE_OK ){
      kvstoreCheckCorruption(pKV, rc);
      kvstoreSetError(pKV, "failed to upgrade transaction for vacuum: error %d", rc);
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }
    pKV->inTrans = 2;
    autoTrans = 1;
  }

  /* Run incremental vacuum steps */
  if( nPage <= 0 ){
    /* Free all unused pages */
    do {
      rc = sqlite3BtreeIncrVacuum(pKV->pBt);
    } while( rc == SQLITE_OK );
    if( rc == SQLITE_DONE ) rc = SQLITE_OK;
  }else{
    int i;
    for( i = 0; i < nPage; i++ ){
      rc = sqlite3BtreeIncrVacuum(pKV->pBt);
      if( rc != SQLITE_OK ) break;
    }
    if( rc == SQLITE_DONE ) rc = SQLITE_OK;
  }

  if( rc != SQLITE_OK ){
    kvstoreCheckCorruption(pKV, rc);
    kvstoreSetError(pKV, "incremental vacuum failed: error %d", rc);
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
      pKV->inTrans = 0;
    }
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  if( autoTrans ){
    rc = sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
    if( rc != SQLITE_OK ){
      kvstoreCheckCorruption(pKV, rc);
      kvstoreSetError(pKV, "failed to commit vacuum: error %d", rc);
    }
  }

  sqlite3_mutex_leave(pKV->pMutex);
  return rc;
}

/*
** List all column families
*/
int kvstore_cf_list(KVStore *pKV, char ***pazNames, int *pnCount){
  int rc;
  BtCursor *pCur = NULL;
  int autoTrans = 0;
  char **azNames = NULL;
  int nCount = 0;
  int nAlloc = 8;

  if( !pKV || !pazNames || !pnCount ){
    return KVSTORE_ERROR;
  }

  *pazNames = NULL;
  *pnCount = 0;

  azNames = (char**)sqlite3MallocZero(nAlloc * sizeof(char*));
  if( !azNames ){
    return KVSTORE_NOMEM;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_free(azNames);
    return KVSTORE_ERROR;
  }

  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      sqlite3_free(azNames);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 1;
  }

  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_free(azNames);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 0, 0, pCur);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_free(azNames);
    return rc;
  }

  int res;
  rc = sqlite3BtreeFirst(pCur, &res);

  while( rc == SQLITE_OK && !res ){
    /* Read payload to extract name */
    u32 payloadSz = sqlite3BtreePayloadSize(pCur);
    if( payloadSz >= 4 ){
      unsigned char hdr[4];
      rc = sqlite3BtreePayload(pCur, 0, 4, hdr);
      if( rc != SQLITE_OK ) break;

      int nameLen = (hdr[0]<<24)|(hdr[1]<<16)|(hdr[2]<<8)|hdr[3];
      if( nameLen > 0 && 4 + nameLen <= (int)payloadSz ){
        char *name = (char*)sqlite3Malloc(nameLen + 1);
        if( !name ){ rc = KVSTORE_NOMEM; break; }

        rc = sqlite3BtreePayload(pCur, 4, nameLen, name);
        if( rc != SQLITE_OK ){ sqlite3_free(name); break; }
        name[nameLen] = '\0';

        if( nCount >= nAlloc ){
          nAlloc *= 2;
          char **azNew = (char**)sqlite3Realloc(azNames, nAlloc * sizeof(char*));
          if( !azNew ){ sqlite3_free(name); rc = KVSTORE_NOMEM; break; }
          azNames = azNew;
        }

        azNames[nCount++] = name;
      }
    }

    rc = sqlite3BtreeNext(pCur, 0);
    if( rc == SQLITE_DONE ){
      rc = SQLITE_OK;
      break;
    }
  }

  kvstoreFreeCursor(pCur);

  if( autoTrans ){
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }

  sqlite3_mutex_leave(pKV->pMutex);

  if( rc != KVSTORE_OK ){
    int i;
    for(i = 0; i < nCount; i++){
      sqlite3_free(azNames[i]);
    }
    sqlite3_free(azNames);
    return rc;
  }

  *pazNames = azNames;
  *pnCount = nCount;

  return KVSTORE_OK;
}

/*
** Drop a column family
*/
int kvstore_cf_drop(KVStore *pKV, const char *zName){
  int rc;
  BtCursor *pCur = NULL;
  int autoTrans = 0;
  i64 nNameLen;
  u32 cfCount;
  int iTable;
  int iMoved = 0;

  if( !pKV || !zName ){
    if( pKV ){
      sqlite3_mutex_enter(pKV->pMutex);
      kvstoreSetError(pKV, "invalid parameters to cf_drop");
      sqlite3_mutex_leave(pKV->pMutex);
    }
    return KVSTORE_ERROR;
  }

  /* Cannot drop default CF */
  if( strcmp(zName, DEFAULT_CF_NAME) == 0 ){
    sqlite3_mutex_enter(pKV->pMutex);
    kvstoreSetError(pKV, "cannot drop default column family");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  nNameLen = strlen(zName);

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  /* Start write transaction (release persistent read first if active). */
  if( pKV->inTrans != 2 ){
    if( pKV->inTrans == 1 ){
      sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 1, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 2;
  }

  /* Find CF in metadata table */
  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 1, 0, pCur);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  int found = 0;
  i64 foundRowid = 0;
  rc = kvstoreMetaSeekKey(pCur, zName, (int)nNameLen, &found, &foundRowid);
  if( rc != SQLITE_OK || !found ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return (rc == SQLITE_OK) ? KVSTORE_NOTFOUND : rc;
  }

  /* Read the table root from payload */
  {
    int res = 0;
    rc = sqlite3BtreeTableMoveto(pCur, foundRowid, 0, &res);
    if( rc != SQLITE_OK || res != 0 ){
      kvstoreFreeCursor(pCur);
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      sqlite3_mutex_leave(pKV->pMutex);
      return KVSTORE_CORRUPT;
    }
  }

  u32 payloadSz = sqlite3BtreePayloadSize(pCur);
  if( (int)payloadSz < 4 + (int)nNameLen + 4 ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }

  unsigned char tableRootBytes[4];
  rc = sqlite3BtreePayload(pCur, 4 + (int)nNameLen, 4, tableRootBytes);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  iTable = (tableRootBytes[0] << 24) | (tableRootBytes[1] << 16) |
           (tableRootBytes[2] << 8) | tableRootBytes[3];

  /* Delete from metadata table */
  rc = sqlite3BtreeDelete(pCur, 0);
  kvstoreFreeCursor(pCur);
  pCur = NULL;

  if( rc != SQLITE_OK ){
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
      pKV->inTrans = 0;
      if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ) pKV->inTrans = 1;
    }
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  /* Drop the actual table */
  rc = sqlite3BtreeDropTable(pKV->pBt, iTable, &iMoved);
  if( rc != SQLITE_OK ){
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
      pKV->inTrans = 0;
      if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ) pKV->inTrans = 1;
    }
    kvstoreSetError(pKV, "failed to drop table: error %d", rc);
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  /* Update CF count */
  sqlite3BtreeGetMeta(pKV->pBt, META_CF_COUNT, &cfCount);
  if( cfCount > 0 ) cfCount--;
  sqlite3BtreeUpdateMeta(pKV->pBt, META_CF_COUNT, cfCount);

  if( autoTrans ){
    rc = sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
    if( rc == SQLITE_OK && sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ){
      pKV->inTrans = 1;
    }
  }

  sqlite3_mutex_leave(pKV->pMutex);

  return rc;
}

/* ===== SNKV compatibility functions ===== */

int sqlite3CorruptError(int lineno){
  sqlite3_log(SQLITE_CORRUPT,
    "database corruption at line %d of [%.10s]",
    lineno, SQLITE_SOURCE_ID);
  return SQLITE_CORRUPT;
}

int sqlite3CantopenError(int lineno){
  sqlite3_log(SQLITE_CANTOPEN,
    "cannot open file at line %d of [%.10s]",
    lineno, SQLITE_SOURCE_ID);
  return SQLITE_CANTOPEN;
}

int sqlite3MisuseError(int lineno){
  sqlite3_log(SQLITE_MISUSE,
    "misuse at line %d of [%.10s]",
    lineno, SQLITE_SOURCE_ID);
  return SQLITE_MISUSE;
}

#if defined(SQLITE_DEBUG) || defined(SQLITE_ENABLE_CORRUPT_PGNO)
int sqlite3CorruptPgnoError(int lineno, Pgno pgno){
  sqlite3_log(SQLITE_CORRUPT,
    "database corruption page %u at line %d of [%.10s]",
    pgno, lineno, SQLITE_SOURCE_ID);
  return SQLITE_CORRUPT;
}
#endif

#ifdef SQLITE_DEBUG
int sqlite3NomemError(int lineno){
  return SQLITE_NOMEM;
}
int sqlite3IoerrnomemError(int lineno){
  return SQLITE_IOERR_NOMEM;
}
#endif

/*
** Minimal sqlite3_initialize – sets up mutexes, memory allocator,
** page cache, and OS (VFS).  Skips SQL-layer registration.
*/
int sqlite3_initialize(void){
  int rc;

  if( sqlite3GlobalConfig.isInit ){
    return SQLITE_OK;
  }
  if( sqlite3GlobalConfig.inProgress ){
    return SQLITE_OK;
  }
  sqlite3GlobalConfig.inProgress = 1;

  rc = sqlite3MutexInit();
  if( rc ) goto done;

  sqlite3GlobalConfig.isMutexInit = 1;
  if( !sqlite3GlobalConfig.isMallocInit ){
    rc = sqlite3MallocInit();
    if( rc ) goto done;
    sqlite3GlobalConfig.isMallocInit = 1;
  }

  if( !sqlite3GlobalConfig.isPCacheInit ){
    rc = sqlite3PcacheInitialize();
    if( rc ) goto done;
    sqlite3GlobalConfig.isPCacheInit = 1;
  }

  rc = sqlite3OsInit();
  if( rc ) goto done;

  sqlite3PCacheBufferSetup(
    sqlite3GlobalConfig.pPage,
    sqlite3GlobalConfig.szPage,
    sqlite3GlobalConfig.nPage
  );

  sqlite3MemoryBarrier();
  sqlite3GlobalConfig.isInit = 1;

done:
  sqlite3GlobalConfig.inProgress = 0;
  return rc;
}

/*
** Minimal sqlite3_config – only handle the most common options.
*/
int sqlite3_config(int op, ...){
  int rc = SQLITE_OK;
  if( sqlite3GlobalConfig.isInit ) return SQLITE_MISUSE_BKPT;

  va_list ap;
  va_start(ap, op);
  switch( op ){
    case SQLITE_CONFIG_SINGLETHREAD:
      sqlite3GlobalConfig.bCoreMutex = 0;
      sqlite3GlobalConfig.bFullMutex = 0;
      break;
    case SQLITE_CONFIG_MULTITHREAD:
      sqlite3GlobalConfig.bCoreMutex = 1;
      sqlite3GlobalConfig.bFullMutex = 0;
      break;
    case SQLITE_CONFIG_SERIALIZED:
      sqlite3GlobalConfig.bCoreMutex = 1;
      sqlite3GlobalConfig.bFullMutex = 1;
      break;
    case SQLITE_CONFIG_MEMSTATUS:
      sqlite3GlobalConfig.bMemstat = va_arg(ap, int);
      break;
    case SQLITE_CONFIG_SMALL_MALLOC:
      sqlite3GlobalConfig.bSmallMalloc = va_arg(ap, int);
      break;
    case SQLITE_CONFIG_MALLOC: {
      sqlite3_mem_methods *p = va_arg(ap, sqlite3_mem_methods*);
      if( p ) sqlite3GlobalConfig.m = *p;
      break;
    }
    case SQLITE_CONFIG_GETMALLOC: {
      sqlite3_mem_methods *p = va_arg(ap, sqlite3_mem_methods*);
      if( p ) *p = sqlite3GlobalConfig.m;
      break;
    }
    case SQLITE_CONFIG_MUTEX: {
      sqlite3_mutex_methods *p = va_arg(ap, sqlite3_mutex_methods*);
      if( p ) sqlite3GlobalConfig.mutex = *p;
      break;
    }
    case SQLITE_CONFIG_GETMUTEX: {
      sqlite3_mutex_methods *p = va_arg(ap, sqlite3_mutex_methods*);
      if( p ) *p = sqlite3GlobalConfig.mutex;
      break;
    }
    case SQLITE_CONFIG_PCACHE2: {
      sqlite3_pcache_methods2 *p = va_arg(ap, sqlite3_pcache_methods2*);
      if( p ) sqlite3GlobalConfig.pcache2 = *p;
      break;
    }
    case SQLITE_CONFIG_GETPCACHE2: {
      sqlite3_pcache_methods2 *p = va_arg(ap, sqlite3_pcache_methods2*);
      if( p ) *p = sqlite3GlobalConfig.pcache2;
      break;
    }
    default:
      rc = SQLITE_OK;  /* Silently ignore unknown options */
      break;
  }
  va_end(ap);
  return rc;
}
