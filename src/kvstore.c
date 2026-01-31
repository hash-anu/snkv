/*
**
** Key-Value Store Implementation - Production Ready with Column Families
** Built on top of SQLite v3.3.0 btree implementation
**
** This implementation provides a robust, production-ready key-value store
** with column family support, proper error handling, logging, safety checks,
** and thread-safety via mutexes.
** 
** 
*/

#include "kvstore.h"
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

/*
** Column Family structure
*/
struct KVColumnFamily {
  KVStore *pKV;      /* Parent store */
  char *zName;       /* Column family name */
  int iTable;        /* Root page of this CF's btree */
  int refCount;      /* Reference count */
  kvstore_mutex *pMutex; /* Mutex for this CF */
};

/*
** KVStore structure - represents an open key-value store
*/
struct KVStore {
  Btree *pBt;        /* Underlying btree handle */
  sqlite3 *db;       /* Database connection (required by btree) */
  int iMetaTable;    /* Root page of CF metadata table */
  int inTrans;       /* Transaction state: 0=none, 1=read, 2=write */
  int isCorrupted;   /* Set if database corruption is detected */
  char *zErrMsg;     /* Last error message */
  
  /* Thread safety */
  kvstore_mutex *pMutex;  /* Main mutex protecting store operations */
  
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
};

/*
** Set error message (caller must hold mutex)
*/
static void kvstoreSetError(KVStore *pKV, const char *zFormat, ...){
  va_list ap;
  if( pKV->zErrMsg ){
    sqliteFree(pKV->zErrMsg);
    pKV->zErrMsg = 0;
  }
  if( zFormat ){
    va_start(ap, zFormat);
    pKV->zErrMsg = sqlite3VMPrintf(zFormat, ap);
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
  
  kvstore_mutex_enter(pKV->pMutex);
  if( pKV->zErrMsg ){
    zMsg = pKV->zErrMsg;
  }else{
    zMsg = "no error";
  }
  kvstore_mutex_leave(pKV->pMutex);
  
  return zMsg;
}

/*
** Memory comparison function for key ordering
*/
static int keyCompare(
  void *NotUsed,
  int n1, const void *p1,
  int n2, const void *p2
){
  int c = memcmp(p1, p2, n1 < n2 ? n1 : n2);
  if( c == 0 ){
    c = n1 - n2;
  }
  return c;
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

/*
** Initialize CF metadata table (caller must hold mutex)
*/
static int initCFMetadataTable(KVStore *pKV){
  int rc;
  
  rc = sqlite3BtreeBeginTrans(pKV->pBt, 1);
  if( rc != SQLITE_OK ) return rc;
  
  /* Create metadata table */
  rc = sqlite3BtreeCreateTable(pKV->pBt, &pKV->iMetaTable, 0);
  if( rc != SQLITE_OK ){
    sqlite3BtreeRollback(pKV->pBt);
    return rc;
  }
  
  /* Store metadata table root in meta[3] */
  rc = sqlite3BtreeUpdateMeta(pKV->pBt, META_CF_METADATA_ROOT, pKV->iMetaTable);
  if( rc != SQLITE_OK ){
    sqlite3BtreeRollback(pKV->pBt);
    return rc;
  }
  
  /* Initialize CF count to 1 (default CF) */
  rc = sqlite3BtreeUpdateMeta(pKV->pBt, META_CF_COUNT, 1);
  if( rc != SQLITE_OK ){
    sqlite3BtreeRollback(pKV->pBt);
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
  
  rc = sqlite3BtreeBeginTrans(pKV->pBt, 1);
  if( rc != SQLITE_OK ) return rc;
  
  /* Create default CF table */
  int iTable;
  rc = sqlite3BtreeCreateTable(pKV->pBt, &iTable, 0);
  if( rc != SQLITE_OK ){
    sqlite3BtreeRollback(pKV->pBt);
    return rc;
  }
  
  /* Store in meta[1] for backward compatibility */
  rc = sqlite3BtreeUpdateMeta(pKV->pBt, META_DEFAULT_CF_ROOT, iTable);
  if( rc != SQLITE_OK ){
    sqlite3BtreeRollback(pKV->pBt);
    return rc;
  }
  
  rc = sqlite3BtreeCommit(pKV->pBt);
  if( rc != SQLITE_OK ) return rc;
  
  /* Create CF structure */
  pCF = (KVColumnFamily*)sqliteMalloc(sizeof(KVColumnFamily));
  if( !pCF ) return KVSTORE_NOMEM;
  
  memset(pCF, 0, sizeof(KVColumnFamily));
  pCF->pKV = pKV;
  pCF->zName = sqliteStrDup(DEFAULT_CF_NAME);
  pCF->iTable = iTable;
  pCF->refCount = 1;
  
  /* Create mutex for default CF */
  pCF->pMutex = kvstore_mutex_alloc();
  if( !pCF->pMutex ){
    sqliteFree(pCF->zName);
    sqliteFree(pCF);
    return KVSTORE_NOMEM;
  }
  
  pKV->pDefaultCF = pCF;
  return KVSTORE_OK;
}

/*
** Open a key-value store database file.
*/
int kvstore_open(
  const char *zFilename,
  KVStore **ppKV,
  int flags
){
  KVStore *pKV;
  int rc;
  
  if( ppKV == NULL ){
    return KVSTORE_ERROR;
  }
  *ppKV = NULL;
  
  /* Allocate KVStore structure */
  pKV = (KVStore*)sqliteMalloc(sizeof(KVStore));
  if( pKV == NULL ){
    return KVSTORE_NOMEM;
  }
  memset(pKV, 0, sizeof(KVStore));
  
  /* Create main mutex first */
  pKV->pMutex = kvstore_mutex_alloc();
  if( !pKV->pMutex ){
    sqliteFree(pKV);
    return KVSTORE_NOMEM;
  }
  
  /* Create a minimal sqlite3 structure (required by btree) */
  pKV->db = (sqlite3*)sqliteMalloc(sizeof(sqlite3));
  if( pKV->db == NULL ){
    kvstore_mutex_free(pKV->pMutex);
    sqliteFree(pKV);
    return KVSTORE_NOMEM;
  }
  memset(pKV->db, 0, sizeof(sqlite3));
  
  /* Open the btree */
  rc = sqlite3BtreeOpen(zFilename, pKV->db, &pKV->pBt, flags);
  if( rc != SQLITE_OK ){
    kvstoreSetError(pKV, "failed to open btree: error %d", rc);
    sqliteFree(pKV->db);
    kvstore_mutex_free(pKV->pMutex);
    sqliteFree(pKV);
    return rc;
  }
  
  /* Set cache size for better performance */
  sqlite3BtreeSetCacheSize(pKV->pBt, KVSTORE_DEFAULT_CACHE_SIZE);
  
  /* Check if this is a new database or existing one */
  u32 defaultCFRoot = 0;
  u32 cfMetaRoot = 0;
  int needsInit = 0;
  
  /* Try to read existing metadata */
  rc = sqlite3BtreeGetMeta(pKV->pBt, META_DEFAULT_CF_ROOT, &defaultCFRoot);
  if( rc == SQLITE_OK ){
    sqlite3BtreeGetMeta(pKV->pBt, META_CF_METADATA_ROOT, &cfMetaRoot);
  }
  
  if( defaultCFRoot == 0 ){
    needsInit = 1;
  }
  
  if( needsInit ){
    /* New database - initialize */
    rc = createDefaultCF(pKV);
    if( rc != SQLITE_OK ){
      sqlite3BtreeClose(pKV->pBt);
      sqliteFree(pKV->db);
      kvstore_mutex_free(pKV->pMutex);
      sqliteFree(pKV);
      return rc;
    }
    
    rc = initCFMetadataTable(pKV);
    if( rc != SQLITE_OK ){
      if( pKV->pDefaultCF ){
        if( pKV->pDefaultCF->pMutex ) kvstore_mutex_free(pKV->pDefaultCF->pMutex);
        sqliteFree(pKV->pDefaultCF->zName);
        sqliteFree(pKV->pDefaultCF);
      }
      sqlite3BtreeClose(pKV->pBt);
      sqliteFree(pKV->db);
      kvstore_mutex_free(pKV->pMutex);
      sqliteFree(pKV);
      return rc;
    }
  }else{
    /* Existing database - open default CF */
    KVColumnFamily *pCF = (KVColumnFamily*)sqliteMalloc(sizeof(KVColumnFamily));
    if( !pCF ){
      sqlite3BtreeClose(pKV->pBt);
      sqliteFree(pKV->db);
      kvstore_mutex_free(pKV->pMutex);
      sqliteFree(pKV);
      return KVSTORE_NOMEM;
    }
    
    memset(pCF, 0, sizeof(KVColumnFamily));
    pCF->pKV = pKV;
    pCF->zName = sqliteStrDup(DEFAULT_CF_NAME);
    pCF->iTable = (int)defaultCFRoot;
    pCF->refCount = 1;
    
    /* Create mutex for default CF */
    pCF->pMutex = kvstore_mutex_alloc();
    if( !pCF->pMutex ){
      sqliteFree(pCF->zName);
      sqliteFree(pCF);
      sqlite3BtreeClose(pKV->pBt);
      sqliteFree(pKV->db);
      kvstore_mutex_free(pKV->pMutex);
      sqliteFree(pKV);
      return KVSTORE_NOMEM;
    }
    
    pKV->pDefaultCF = pCF;
    pKV->iMetaTable = (int)cfMetaRoot;
  }
  
  pKV->inTrans = 0;
  *ppKV = pKV;
  return KVSTORE_OK;
}

/*
** Close a key-value store.
*/
int kvstore_close(KVStore *pKV){
  int rc, i;
  
  if( pKV == NULL ){
    return KVSTORE_OK;
  }
  
  /* Acquire mutex before closing */
  kvstore_mutex_enter(pKV->pMutex);
  
  /* Rollback any active transaction */
  if( pKV->inTrans ){
    sqlite3BtreeRollback(pKV->pBt);
    pKV->inTrans = 0;
  }
  
  /* Close all open column families */
  if( pKV->pDefaultCF ){
    if( pKV->pDefaultCF->pMutex ){
      kvstore_mutex_free(pKV->pDefaultCF->pMutex);
    }
    sqliteFree(pKV->pDefaultCF->zName);
    sqliteFree(pKV->pDefaultCF);
  }
  
  for(i = 0; i < pKV->nCF; i++){
    if( pKV->apCF[i] ){
      if( pKV->apCF[i]->pMutex ){
        kvstore_mutex_free(pKV->apCF[i]->pMutex);
      }
      sqliteFree(pKV->apCF[i]->zName);
      sqliteFree(pKV->apCF[i]);
    }
  }
  sqliteFree(pKV->apCF);
  
  /* Close the btree */
  rc = sqlite3BtreeClose(pKV->pBt);
  
  /* Free error message */
  if( pKV->zErrMsg ){
    sqliteFree(pKV->zErrMsg);
  }
  
  /* Free resources */
  sqliteFree(pKV->db);
  
  /* Release and free mutex */
  kvstore_mutex_leave(pKV->pMutex);
  kvstore_mutex_free(pKV->pMutex);
  
  sqliteFree(pKV);
  
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
  
  kvstore_mutex_enter(pKV->pMutex);
  
  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot begin transaction: database is corrupted");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }
  
  if( pKV->inTrans ){
    kvstoreSetError(pKV, "transaction already active");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }
  
  rc = sqlite3BtreeBeginTrans(pKV->pBt, wrflag);
  if( rc == SQLITE_OK ){
    pKV->inTrans = wrflag ? 2 : 1;
  }else{
    kvstoreCheckCorruption(pKV, rc);
    kvstoreSetError(pKV, "failed to begin transaction: error %d", rc);
  }
  
  kvstore_mutex_leave(pKV->pMutex);
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
  
  kvstore_mutex_enter(pKV->pMutex);
  
  if( !pKV->inTrans ){
    kvstoreSetError(pKV, "no active transaction to commit");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }
  
  rc = sqlite3BtreeCommit(pKV->pBt);
  if( rc == SQLITE_OK ){
    pKV->inTrans = 0;
  }else{
    kvstoreCheckCorruption(pKV, rc);
    kvstoreSetError(pKV, "failed to commit transaction: error %d", rc);
  }
  
  kvstore_mutex_leave(pKV->pMutex);
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
  
  kvstore_mutex_enter(pKV->pMutex);
  
  if( !pKV->inTrans ){
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_OK; /* No transaction to rollback */
  }
  
  rc = sqlite3BtreeRollback(pKV->pBt);
  pKV->inTrans = 0;
  
  if( rc != SQLITE_OK ){
    kvstoreCheckCorruption(pKV, rc);
    kvstoreSetError(pKV, "failed to rollback transaction: error %d", rc);
  }
  
  kvstore_mutex_leave(pKV->pMutex);
  return rc;
}

/* Forward declaration for CF operations */
static int kvstore_cf_put_internal(
  KVColumnFamily *pCF,
  const void *pKey,
  int nKey,
  const void *pValue,
  int nValue
);

static int kvstore_cf_get_internal(
  KVColumnFamily *pCF,
  const void *pKey,
  int nKey,
  void **ppValue,
  int *pnValue
);

static int kvstore_cf_delete_internal(
  KVColumnFamily *pCF,
  const void *pKey,
  int nKey
);

static int kvstore_cf_exists_internal(
  KVColumnFamily *pCF,
  const void *pKey,
  int nKey,
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
  
  kvstore_mutex_enter(pKV->pMutex);
  
  if( !pKV->pDefaultCF ){
    kvstoreSetError(pKV, "default column family not initialized");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }
  
  pKV->pDefaultCF->refCount++;
  *ppCF = pKV->pDefaultCF;
  
  kvstore_mutex_leave(pKV->pMutex);
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
  int iTable;
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
    kvstore_mutex_enter(pKV->pMutex);
    kvstoreSetError(pKV, "invalid column family name length");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }
  
  /* Check if default CF */
  if( strcmp(zName, DEFAULT_CF_NAME) == 0 ){
    return kvstore_cf_get_default(pKV, ppCF);
  }
  
  kvstore_mutex_enter(pKV->pMutex);
  
  /* Start transaction */
  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 1);
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 2;
  }else if( pKV->inTrans != 2 ){
    kvstoreSetError(pKV, "cannot create CF: read-only transaction");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_READONLY;
  }
  
  /* Check if CF already exists */
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 0, keyCompare, NULL, &pCur);
  if( rc != SQLITE_OK ){
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }
  
  int loc;
  rc = sqlite3BtreeMoveto(pCur, zName, nNameLen, &loc);
  if( rc != SQLITE_OK ){
    sqlite3BtreeCloseCursor(pCur);
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }
  
  if( loc == 0 ){
    /* CF already exists */
    sqlite3BtreeCloseCursor(pCur);
    if( autoTrans ){
      sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstoreSetError(pKV, "column family already exists: %s", zName);
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }
  
  sqlite3BtreeCloseCursor(pCur);
  
  /* Create new btree table for this CF */
  rc = sqlite3BtreeCreateTable(pKV->pBt, &iTable, 0);
  if( rc != SQLITE_OK ){
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }
  
  /* Store CF metadata (name -> table root) */
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 1, keyCompare, NULL, &pCur);
  if( rc != SQLITE_OK ){
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }
  
  /* Value is just the table root as 4-byte integer */
  unsigned char tableRootBytes[4];
  tableRootBytes[0] = (iTable >> 24) & 0xFF;
  tableRootBytes[1] = (iTable >> 16) & 0xFF;
  tableRootBytes[2] = (iTable >> 8) & 0xFF;
  tableRootBytes[3] = iTable & 0xFF;
  
  rc = sqlite3BtreeInsert(pCur, zName, nNameLen, tableRootBytes, 4);
  sqlite3BtreeCloseCursor(pCur);
  
  if( rc != SQLITE_OK ){
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }
  
  /* Update CF count */
  sqlite3BtreeGetMeta(pKV->pBt, META_CF_COUNT, &cfCount);
  cfCount++;
  sqlite3BtreeUpdateMeta(pKV->pBt, META_CF_COUNT, cfCount);
  
  /* Commit if we started the transaction */
  if( autoTrans ){
    rc = sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      return rc;
    }
  }
  
  /* Create CF structure */
  pCF = (KVColumnFamily*)sqliteMalloc(sizeof(KVColumnFamily));
  if( !pCF ){
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }
  
  zNameCopy = sqliteStrDup(zName);
  if( !zNameCopy ){
    sqliteFree(pCF);
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }
  
  memset(pCF, 0, sizeof(KVColumnFamily));
  pCF->pKV = pKV;
  pCF->zName = zNameCopy;
  pCF->iTable = iTable;
  pCF->refCount = 1;
  
  /* Create mutex for this CF */
  pCF->pMutex = kvstore_mutex_alloc();
  if( !pCF->pMutex ){
    sqliteFree(zNameCopy);
    sqliteFree(pCF);
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }
  
  kvstore_mutex_leave(pKV->pMutex);
  
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
  u32 dataSize;
  unsigned char tableRootBytes[4];
  int iTable;
  char *zNameCopy = NULL;
  
  if( !pKV || !zName || !ppCF ){
    if( pKV ){
      kvstore_mutex_enter(pKV->pMutex);
      kvstoreSetError(pKV, "invalid parameters to cf_open");
      kvstore_mutex_leave(pKV->pMutex);
    }
    return KVSTORE_ERROR;
  }
  
  *ppCF = NULL;
  nNameLen = strlen(zName);
  
  /* Check if default CF */
  if( strcmp(zName, DEFAULT_CF_NAME) == 0 ){
    return kvstore_cf_get_default(pKV, ppCF);
  }
  
  kvstore_mutex_enter(pKV->pMutex);
  
  /* Start read transaction if needed */
  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0);
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 1;
  }
  
  /* Look up CF in metadata table */
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 0, keyCompare, NULL, &pCur);
  if( rc != SQLITE_OK ){
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }
  
  int loc;
  rc = sqlite3BtreeMoveto(pCur, zName, nNameLen, &loc);
  if( rc != SQLITE_OK || loc != 0 || sqlite3BtreeEof(pCur) ){
    sqlite3BtreeCloseCursor(pCur);
    if( autoTrans ){
      sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_NOTFOUND;
  }
  
  /* Read table root */
  rc = sqlite3BtreeDataSize(pCur, &dataSize);
  if( rc != SQLITE_OK || dataSize != 4 ){
    sqlite3BtreeCloseCursor(pCur);
    if( autoTrans ){
      sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }
  
  rc = sqlite3BtreeData(pCur, 0, 4, tableRootBytes);
  sqlite3BtreeCloseCursor(pCur);
  
  if( autoTrans ){
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }
  
  if( rc != SQLITE_OK ){
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }
  
  iTable = (tableRootBytes[0] << 24) | (tableRootBytes[1] << 16) |
           (tableRootBytes[2] << 8) | tableRootBytes[3];
  
  /* Create CF structure */
  pCF = (KVColumnFamily*)sqliteMalloc(sizeof(KVColumnFamily));
  if( !pCF ){
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }
  
  zNameCopy = sqliteStrDup(zName);
  if( !zNameCopy ){
    sqliteFree(pCF);
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }
  
  memset(pCF, 0, sizeof(KVColumnFamily));
  pCF->pKV = pKV;
  pCF->zName = zNameCopy;
  pCF->iTable = iTable;
  pCF->refCount = 1;
  
  /* Create mutex for this CF */
  pCF->pMutex = kvstore_mutex_alloc();
  if( !pCF->pMutex ){
    sqliteFree(zNameCopy);
    sqliteFree(pCF);
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }
  
  kvstore_mutex_leave(pKV->pMutex);
  
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
  
  kvstore_mutex_enter(pKV->pMutex);
  
  pCF->refCount--;
  if( pCF->refCount <= 0 && pCF != pKV->pDefaultCF ){
    shouldFree = 1;
  }
  
  kvstore_mutex_leave(pKV->pMutex);
  
  if( shouldFree ){
    if( pCF->pMutex ){
      kvstore_mutex_free(pCF->pMutex);
    }
    sqliteFree(pCF->zName);
    sqliteFree(pCF);
  }
}

/*
** Internal implementation: Put to column family
*/
static int kvstore_cf_put_internal(
  KVColumnFamily *pCF,
  const void *pKey,
  int nKey,
  const void *pValue,
  int nValue
){
  BtCursor *pCur = NULL;
  int rc;
  int autoTrans = 0;
  i64 nKey64;
  KVStore *pKV = pCF->pKV;
  
  kvstore_mutex_enter(pCF->pMutex);
  kvstore_mutex_enter(pKV->pMutex);
  
  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot put: database is corrupted");
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }
  
  rc = kvstoreValidateKeyValue(pKV, pKey, nKey, pValue, nValue);
  if( rc != KVSTORE_OK ){
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return rc;
  }
  
  nKey64 = nKey;
  
  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 1);
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      kvstore_mutex_leave(pCF->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 2;
  }else if( pKV->inTrans != 2 ){
    kvstoreSetError(pKV, "cannot put: read-only transaction active");
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return KVSTORE_READONLY;
  }
  
  rc = sqlite3BtreeCursor(pKV->pBt, pCF->iTable, 1, keyCompare, NULL, &pCur);
  if( rc != SQLITE_OK ){
    kvstoreCheckCorruption(pKV, rc);
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return rc;
  }
  
  rc = sqlite3BtreeInsert(pCur, pKey, nKey64, pValue, nValue);
  sqlite3BtreeCloseCursor(pCur);
  
  if( rc == SQLITE_OK ){
    pKV->stats.nPuts++;
    if( autoTrans ){
      rc = sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
  }else{
    kvstoreCheckCorruption(pKV, rc);
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
  }
  
  kvstore_mutex_leave(pKV->pMutex);
  kvstore_mutex_leave(pCF->pMutex);
  
  return rc;
}

/*
** Internal implementation: Get from column family
*/
static int kvstore_cf_get_internal(
  KVColumnFamily *pCF,
  const void *pKey,
  int nKey,
  void **ppValue,
  int *pnValue
){
  BtCursor *pCur = NULL;
  int rc, loc;
  u32 dataSize;
  void *pValue = NULL;
  int autoTrans = 0;
  i64 nKey64 = nKey;
  KVStore *pKV = pCF->pKV;
  
  if( !ppValue || !pnValue ){
    kvstore_mutex_enter(pKV->pMutex);
    kvstoreSetError(pKV, "invalid parameters to get");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }
  
  *ppValue = NULL;
  *pnValue = 0;
  
  kvstore_mutex_enter(pCF->pMutex);
  kvstore_mutex_enter(pKV->pMutex);
  
  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot get: database is corrupted");
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }
  
  if( !pKey || nKey <= 0 ){
    kvstoreSetError(pKV, "invalid key");
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }
  
  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0);
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      kvstore_mutex_leave(pCF->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 1;
  }
  
  rc = sqlite3BtreeCursor(pKV->pBt, pCF->iTable, 0, keyCompare, NULL, &pCur);
  if( rc != SQLITE_OK ){
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return rc;
  }
  
  rc = sqlite3BtreeMoveto(pCur, pKey, nKey64, &loc);
  if( rc != SQLITE_OK || loc != 0 || sqlite3BtreeEof(pCur) ){
    sqlite3BtreeCloseCursor(pCur);
    if( autoTrans ){
      sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return (rc == SQLITE_OK) ? KVSTORE_NOTFOUND : rc;
  }
  
  rc = sqlite3BtreeDataSize(pCur, &dataSize);
  if( rc != SQLITE_OK ){
    sqlite3BtreeCloseCursor(pCur);
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return rc;
  }
  
  if( dataSize > 0 ){
    pValue = sqliteMalloc(dataSize);
    if( !pValue ){
      sqlite3BtreeCloseCursor(pCur);
      if( autoTrans ){
        sqlite3BtreeRollback(pKV->pBt);
        pKV->inTrans = 0;
      }
      kvstore_mutex_leave(pKV->pMutex);
      kvstore_mutex_leave(pCF->pMutex);
      return KVSTORE_NOMEM;
    }
    
    rc = sqlite3BtreeData(pCur, 0, dataSize, pValue);
    if( rc != SQLITE_OK ){
      sqliteFree(pValue);
      sqlite3BtreeCloseCursor(pCur);
      if( autoTrans ){
        sqlite3BtreeRollback(pKV->pBt);
        pKV->inTrans = 0;
      }
      kvstore_mutex_leave(pKV->pMutex);
      kvstore_mutex_leave(pCF->pMutex);
      return rc;
    }
  }
  
  sqlite3BtreeCloseCursor(pCur);
  if( autoTrans ){
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }
  
  pKV->stats.nGets++;
  *ppValue = pValue;
  *pnValue = (int)dataSize;
  
  kvstore_mutex_leave(pKV->pMutex);
  kvstore_mutex_leave(pCF->pMutex);
  
  return KVSTORE_OK;
}

/*
** Internal implementation: Delete from column family
*/
static int kvstore_cf_delete_internal(
  KVColumnFamily *pCF,
  const void *pKey,
  int nKey
){
  BtCursor *pCur = NULL;
  int rc, loc;
  int autoTrans = 0;
  i64 nKey64 = nKey;
  KVStore *pKV = pCF->pKV;
  
  kvstore_mutex_enter(pCF->pMutex);
  kvstore_mutex_enter(pKV->pMutex);
  
  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot delete: database is corrupted");
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }
  
  if( !pKey || nKey <= 0 ){
    kvstoreSetError(pKV, "invalid key");
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }
  
  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 1);
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      kvstore_mutex_leave(pCF->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 2;
  }else if( pKV->inTrans != 2 ){
    kvstoreSetError(pKV, "cannot delete: read-only transaction");
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return KVSTORE_READONLY;
  }
  
  rc = sqlite3BtreeCursor(pKV->pBt, pCF->iTable, 1, keyCompare, NULL, &pCur);
  if( rc != SQLITE_OK ){
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return rc;
  }
  
  rc = sqlite3BtreeMoveto(pCur, pKey, nKey64, &loc);
  if( rc != SQLITE_OK || loc != 0 || sqlite3BtreeEof(pCur) ){
    sqlite3BtreeCloseCursor(pCur);
    if( autoTrans ){
      sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return (rc == SQLITE_OK) ? KVSTORE_NOTFOUND : rc;
  }
  
  rc = sqlite3BtreeDelete(pCur);
  sqlite3BtreeCloseCursor(pCur);
  
  if( rc == SQLITE_OK ){
    pKV->stats.nDeletes++;
    if( autoTrans ){
      rc = sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
  }else{
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
  }
  
  kvstore_mutex_leave(pKV->pMutex);
  kvstore_mutex_leave(pCF->pMutex);
  
  return rc;
}

/*
** Internal implementation: Check existence in column family
*/
static int kvstore_cf_exists_internal(
  KVColumnFamily *pCF,
  const void *pKey,
  int nKey,
  int *pExists
){
  BtCursor *pCur = NULL;
  int rc, loc, eof;
  int autoTrans = 0;
  i64 nKey64 = nKey;
  KVStore *pKV = pCF->pKV;
  
  if( !pExists ){
    kvstore_mutex_enter(pKV->pMutex);
    kvstoreSetError(pKV, "invalid parameters to exists");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }
  
  *pExists = 0;
  
  kvstore_mutex_enter(pCF->pMutex);
  kvstore_mutex_enter(pKV->pMutex);
  
  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot check existence: database is corrupted");
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }
  
  if( !pKey || nKey <= 0 ){
    kvstoreSetError(pKV, "invalid key");
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }
  
  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0);
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      kvstore_mutex_leave(pCF->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 1;
  }
  
  rc = sqlite3BtreeCursor(pKV->pBt, pCF->iTable, 0, keyCompare, NULL, &pCur);
  if( rc != SQLITE_OK ){
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return rc;
  }
  
  rc = sqlite3BtreeMoveto(pCur, pKey, nKey64, &loc);
  eof = sqlite3BtreeEof(pCur);
  
  sqlite3BtreeCloseCursor(pCur);
  if( autoTrans ){
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }
  
  if( rc == SQLITE_OK ){
    *pExists = (loc == 0 && !eof) ? 1 : 0;
  }
  
  kvstore_mutex_leave(pKV->pMutex);
  kvstore_mutex_leave(pCF->pMutex);
  
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
  
  kvstore_mutex_enter(pKV->pMutex);
  
  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot create iterator: database is corrupted");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }
  
  pIter = (KVIterator*)sqliteMalloc(sizeof(KVIterator));
  if( !pIter ){
    kvstoreSetError(pKV, "out of memory allocating iterator");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }
  memset(pIter, 0, sizeof(KVIterator));
  
  pIter->pCF = pCF;
  pCF->refCount++;  /* Hold reference to CF */
  pIter->eof = 1;
  pIter->ownsTrans = 0;
  pIter->isValid = 1;
  
  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0);
    if( rc != SQLITE_OK ){
      sqliteFree(pIter);
      pCF->refCount--;
      kvstore_mutex_leave(pKV->pMutex);
      return rc;
    }
    pIter->ownsTrans = 1;
    pKV->inTrans = 1;
  }
  
  rc = sqlite3BtreeCursor(pKV->pBt, pCF->iTable, 0, keyCompare, NULL, &pIter->pCur);
  if( rc != SQLITE_OK ){
    kvstoreCheckCorruption(pKV, rc);
    if( pIter->ownsTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
    pCF->refCount--;
    sqliteFree(pIter);
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }
  
  pKV->stats.nIterations++;
  kvstore_mutex_leave(pKV->pMutex);
  
  *ppIter = pIter;
  return KVSTORE_OK;
}

/*
** Move iterator to first entry
*/
int kvstore_iterator_first(KVIterator *pIter){
  int rc, res;
  
  if( !pIter || !pIter->isValid ){
    return KVSTORE_ERROR;
  }
  
  /* No mutex needed - iterator operations are single-threaded per iterator */
  rc = sqlite3BtreeFirst(pIter->pCur, &res);
  if( rc == SQLITE_OK ){
    pIter->eof = res;
  }
  
  return rc;
}

/*
** Move iterator to next entry
*/
int kvstore_iterator_next(KVIterator *pIter){
  int rc, res;
  
  if( !pIter || !pIter->isValid ){
    return KVSTORE_ERROR;
  }
  
  if( pIter->eof ){
    return KVSTORE_OK;
  }
  
  rc = sqlite3BtreeNext(pIter->pCur, &res);
  if( rc == SQLITE_OK ){
    pIter->eof = res;
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
** Get current key from iterator
*/
int kvstore_iterator_key(KVIterator *pIter, void **ppKey, int *pnKey){
  int rc;
  i64 keySize;
  
  if( !pIter || !pIter->isValid || !ppKey || !pnKey ){
    return KVSTORE_ERROR;
  }
  
  if( pIter->eof ){
    return KVSTORE_ERROR;
  }
  
  rc = sqlite3BtreeKeySize(pIter->pCur, &keySize);
  if( rc != SQLITE_OK ){
    return rc;
  }
  
  if( pIter->nKeyBuf < (int)keySize ){
    void *pNew = sqliteRealloc(pIter->pKeyBuf, (int)keySize);
    if( !pNew ){
      return KVSTORE_NOMEM;
    }
    pIter->pKeyBuf = pNew;
    pIter->nKeyBuf = (int)keySize;
  }
  
  rc = sqlite3BtreeKey(pIter->pCur, 0, (int)keySize, pIter->pKeyBuf);
  if( rc != SQLITE_OK ){
    return rc;
  }
  
  *ppKey = pIter->pKeyBuf;
  *pnKey = (int)keySize;
  
  return KVSTORE_OK;
}

/*
** Get current value from iterator
*/
int kvstore_iterator_value(KVIterator *pIter, void **ppValue, int *pnValue){
  int rc;
  u32 dataSize;
  
  if( !pIter || !pIter->isValid || !ppValue || !pnValue ){
    return KVSTORE_ERROR;
  }
  
  if( pIter->eof ){
    return KVSTORE_ERROR;
  }
  
  rc = sqlite3BtreeDataSize(pIter->pCur, &dataSize);
  if( rc != SQLITE_OK ){
    return rc;
  }
  
  if( pIter->nValBuf < (int)dataSize ){
    void *pNew = sqliteRealloc(pIter->pValBuf, (int)dataSize);
    if( !pNew ){
      return KVSTORE_NOMEM;
    }
    pIter->pValBuf = pNew;
    pIter->nValBuf = (int)dataSize;
  }
  
  if( dataSize > 0 ){
    rc = sqlite3BtreeData(pIter->pCur, 0, dataSize, pIter->pValBuf);
    if( rc != SQLITE_OK ){
      return rc;
    }
  }
  
  *ppValue = pIter->pValBuf;
  *pnValue = (int)dataSize;
  
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
    sqlite3BtreeCloseCursor(pIter->pCur);
  }
  
  if( pIter->ownsTrans && pKV ){
    kvstore_mutex_enter(pKV->pMutex);
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
    kvstore_mutex_leave(pKV->pMutex);
  }
  
  if( pIter->pCF ){
    kvstore_cf_close(pIter->pCF);
  }
  
  if( pIter->pKeyBuf ){
    sqliteFree(pIter->pKeyBuf);
  }
  
  if( pIter->pValBuf ){
    sqliteFree(pIter->pValBuf);
  }
  
  sqliteFree(pIter);
}

/*
** Get statistics
*/
int kvstore_stats(KVStore *pKV, KVStoreStats *pStats){
  if( !pKV || !pStats ){
    return KVSTORE_ERROR;
  }
  
  kvstore_mutex_enter(pKV->pMutex);
  
  pStats->nPuts = pKV->stats.nPuts;
  pStats->nGets = pKV->stats.nGets;
  pStats->nDeletes = pKV->stats.nDeletes;
  pStats->nIterations = pKV->stats.nIterations;
  pStats->nErrors = pKV->stats.nErrors;
  
  kvstore_mutex_leave(pKV->pMutex);
  
  return KVSTORE_OK;
}

/*
** Perform database integrity check
*/
int kvstore_integrity_check(KVStore *pKV, char **pzErrMsg){
  int rc;
  int *aiRoot;
  int nRoot;
  char *zErr;
  int hadTrans = 0;
  
  if( !pKV ){
    return KVSTORE_ERROR;
  }
  
  if( pzErrMsg ){
    *pzErrMsg = NULL;
  }
  
  kvstore_mutex_enter(pKV->pMutex);
  
  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0);
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      return rc;
    }
    hadTrans = 1;
    pKV->inTrans = 1;
  }
  
  /* Build array of all root pages to check */
  /* We need: default CF table + metadata table = 2 roots */
  nRoot = 2;
  aiRoot = (int*)sqliteMalloc(nRoot * sizeof(int));
  if( !aiRoot ){
    if( hadTrans ){
      sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }
  
  /* Add default CF table root */
  aiRoot[0] = pKV->pDefaultCF->iTable;
  /* Add metadata table root */
  aiRoot[1] = pKV->iMetaTable;
  
  /* Perform integrity check on all roots */
  zErr = sqlite3BtreeIntegrityCheck(pKV->pBt, aiRoot, nRoot);
  
  sqliteFree(aiRoot);
  
  if( hadTrans ){
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }
  
  if( zErr ){
    /* Filter out "Page X is never used" false positives for page 1 */
    /* Page 1 is the database header page and is expected to not be in use */
    if( strstr(zErr, "Page 1 is never used") != NULL ){
      /* Check if there are other errors besides Page 1 */
      char *otherErr = strstr(zErr, "\n");
      if( otherErr && otherErr[1] != '\0' ){
        /* Check if remaining errors are also just "Page X is never used" */
        char *checkErr = otherErr + 1;
        int hasRealErrors = 0;
        
        /* Simple check: if the error contains something other than "is never used", it's real */
        if( strstr(checkErr, "is never used") == NULL || 
            (strstr(checkErr, "corrupt") != NULL) ||
            (strstr(checkErr, "wrong") != NULL) ){
          hasRealErrors = 1;
        }
        
        if( hasRealErrors ){
          pKV->isCorrupted = 1;
          kvstoreSetError(pKV, "integrity check failed: %s", checkErr);
          if( pzErrMsg ){
            *pzErrMsg = sqlite3MPrintf("%s", checkErr);
          }
          sqliteFree(zErr);
          kvstore_mutex_leave(pKV->pMutex);
          return KVSTORE_CORRUPT;
        }
      }
      /* Only page 1 or "never used" warnings, database is OK */
      sqliteFree(zErr);
      kvstore_mutex_leave(pKV->pMutex);
      return KVSTORE_OK;
    }else{
      /* Real corruption detected */
      pKV->isCorrupted = 1;
      kvstoreSetError(pKV, "integrity check failed: %s", zErr);
      if( pzErrMsg ){
        *pzErrMsg = zErr;
      }else{
        sqliteFree(zErr);
      }
      kvstore_mutex_leave(pKV->pMutex);
      return KVSTORE_CORRUPT;
    }
  }
  
  kvstore_mutex_leave(pKV->pMutex);
  return KVSTORE_OK;
}

/*
** Synchronize database to disk
*/
int kvstore_sync(KVStore *pKV){
  int rc;
  
  if( !pKV ){
    return KVSTORE_ERROR;
  }
  
  kvstore_mutex_enter(pKV->pMutex);
  
  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot sync: database is corrupted");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }
  
  rc = sqlite3BtreeSync(pKV->pBt, NULL);
  if( rc != SQLITE_OK ){
    kvstoreCheckCorruption(pKV, rc);
    kvstoreSetError(pKV, "failed to sync database: error %d", rc);
  }
  
  kvstore_mutex_leave(pKV->pMutex);
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
  void *pKey;
  
  if( !pKV || !pazNames || !pnCount ){
    return KVSTORE_ERROR;
  }
  
  *pazNames = NULL;
  *pnCount = 0;
  
  azNames = (char**)sqliteMalloc(nAlloc * sizeof(char*));
  if( !azNames ){
    return KVSTORE_NOMEM;
  }
  
  kvstore_mutex_enter(pKV->pMutex);
  
  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0);
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      sqliteFree(azNames);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 1;
  }
  
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 0, keyCompare, NULL, &pCur);
  if( rc != SQLITE_OK ){
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    sqliteFree(azNames);
    return rc;
  }
  
  int res;
  rc = sqlite3BtreeFirst(pCur, &res);
  
  while( rc == SQLITE_OK && !res ){
    i64 keySize;
    rc = sqlite3BtreeKeySize(pCur, &keySize);
    if( rc != SQLITE_OK ) break;
    
    pKey = sqliteMalloc((int)keySize + 1);
    if( !pKey ){
      rc = KVSTORE_NOMEM;
      break;
    }
    
    rc = sqlite3BtreeKey(pCur, 0, (int)keySize, pKey);
    if( rc != SQLITE_OK ){
      sqliteFree(pKey);
      break;
    }
    
    ((char*)pKey)[keySize] = '\0';
    
    if( nCount >= nAlloc ){
      nAlloc *= 2;
      char **azNew = (char**)sqliteRealloc(azNames, nAlloc * sizeof(char*));
      if( !azNew ){
        sqliteFree(pKey);
        rc = KVSTORE_NOMEM;
        break;
      }
      azNames = azNew;
    }
    
    azNames[nCount++] = (char*)pKey;
    
    rc = sqlite3BtreeNext(pCur, &res);
  }
  
  sqlite3BtreeCloseCursor(pCur);
  
  if( autoTrans ){
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }
  
  kvstore_mutex_leave(pKV->pMutex);
  
  if( rc != KVSTORE_OK ){
    for(int i = 0; i < nCount; i++){
      sqliteFree(azNames[i]);
    }
    sqliteFree(azNames);
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
  int loc;
  u32 cfCount;
  u32 dataSize;
  unsigned char tableRootBytes[4];
  int iTable;
  int iMoved = 0;  // For sqlite3BtreeDropTable
  
  if( !pKV || !zName ){
    if( pKV ){
      kvstore_mutex_enter(pKV->pMutex);
      kvstoreSetError(pKV, "invalid parameters to cf_drop");
      kvstore_mutex_leave(pKV->pMutex);
    }
    return KVSTORE_ERROR;
  }
  
  /* Cannot drop default CF */
  if( strcmp(zName, DEFAULT_CF_NAME) == 0 ){
    kvstore_mutex_enter(pKV->pMutex);
    kvstoreSetError(pKV, "cannot drop default column family");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }
  
  nNameLen = strlen(zName);
  
  kvstore_mutex_enter(pKV->pMutex);
  
  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 1);
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 2;
  }else if( pKV->inTrans != 2 ){
    kvstoreSetError(pKV, "cannot drop CF: read-only transaction");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_READONLY;
  }
  
  /* Find CF in metadata table and get its table root */
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 1, keyCompare, NULL, &pCur);
  if( rc != SQLITE_OK ){
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }
  
  rc = sqlite3BtreeMoveto(pCur, zName, nNameLen, &loc);
  if( rc != SQLITE_OK || loc != 0 || sqlite3BtreeEof(pCur) ){
    sqlite3BtreeCloseCursor(pCur);
    if( autoTrans ){
      sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    return (rc == SQLITE_OK) ? KVSTORE_NOTFOUND : rc;
  }
  
  /* Read the table root page number */
  rc = sqlite3BtreeDataSize(pCur, &dataSize);
  if( rc != SQLITE_OK || dataSize != 4 ){
    sqlite3BtreeCloseCursor(pCur);
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }
  
  rc = sqlite3BtreeData(pCur, 0, 4, tableRootBytes);
  if( rc != SQLITE_OK ){
    sqlite3BtreeCloseCursor(pCur);
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }
  
  iTable = (tableRootBytes[0] << 24) | (tableRootBytes[1] << 16) |
           (tableRootBytes[2] << 8) | tableRootBytes[3];
  
  /* Delete from metadata table */
  rc = sqlite3BtreeDelete(pCur);
  sqlite3BtreeCloseCursor(pCur);
  
  if( rc != SQLITE_OK ){
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }
  
  /* Drop the actual table */
  rc = sqlite3BtreeDropTable(pKV->pBt, iTable, &iMoved);
  if( rc != SQLITE_OK ){
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt);
      pKV->inTrans = 0;
    }
    kvstoreSetError(pKV, "failed to drop table: error %d", rc);
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }
  
  /* Update CF count */
  sqlite3BtreeGetMeta(pKV->pBt, META_CF_COUNT, &cfCount);
  if( cfCount > 0 ) cfCount--;
  sqlite3BtreeUpdateMeta(pKV->pBt, META_CF_COUNT, cfCount);
  
  if( autoTrans ){
    rc = sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }
  
  kvstore_mutex_leave(pKV->pMutex);
  
  return rc;
}