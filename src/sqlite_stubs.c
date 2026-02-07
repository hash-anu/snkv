/*
** Stub implementations for SNKV.
**
** SNKV uses only the btree -> pager -> os layers of SQLite.
** This file provides minimal implementations of functions from the
** upper SQL layer that the btree/pager/os code references.
**
** Categories:
**   1. Error-reporting functions (real implementations)
**   2. sqlite3_initialize / sqlite3_config (minimal functional implementations)
**   3. VDBE record comparison stubs (never called for INTKEY tables)
**   4. sqlite3_value accessor stubs (for printf.c)
**   5. Upper-layer no-op stubs (build.c, trigger.c, etc.)
**   6. Backup stubs (for pager.c -> backup.c dependency)
*/
#include "sqliteInt.h"

/* ===== 1. Error-reporting functions ===== */

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

/* ===== 2. sqlite3_initialize / sqlite3_config ===== */

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

/* ===== 3. Core infrastructure stubs ===== */

int sqlite3TempInMemory(const sqlite3 *db){
#if SQLITE_TEMP_STORE>=2
  (void)db;
  return 1;
#else
  (void)db;
  return 0;
#endif
}

int sqlite3InvokeBusyHandler(BusyHandler *p){
  int rc;
  if( p->xBusyHandler==0 || p->nBusy<0 ) return 0;
  rc = p->xBusyHandler(p->pBusyArg, p->nBusy);
  if( rc==0 ){
    p->nBusy = -1;
  }else{
    p->nBusy++;
  }
  return rc;
}

int sqlite3WritableSchema(sqlite3 *db){
  (void)db;
  return 0;
}

int sqlite3IsMemdb(const sqlite3_vfs *pVfs){
  (void)pVfs;
  return 0;
}

/*
** Global temp directory variable (used by os_unix.c).
*/
SQLITE_API char *sqlite3_temp_directory = 0;
/*
** URI parameter helpers – return defaults.
** In a btree-only build, URI parameters are not used.
*/
const char *sqlite3_uri_parameter(const char *zFilename, const char *zParam){
  (void)zFilename; (void)zParam;
  return 0;
}

int sqlite3_uri_boolean(const char *zFilename, const char *zParam, int bDflt){
  (void)zFilename; (void)zParam;
  return bDflt;
}

/*
** sqlite3_exec – runs SQL.  Should not be called in btree-only build,
** but pager.c references it in sqlite3PagerCheckpoint for PRAGMA calls.
** Return error if ever called.
*/
int sqlite3_exec(
  sqlite3 *db,
  const char *zSql,
  int (*xCallback)(void*,int,char**,char**),
  void *pArg,
  char **pzErrMsg
){
  (void)db; (void)zSql; (void)xCallback; (void)pArg; (void)pzErrMsg;
  return SQLITE_ERROR;
}

/* ===== 4. VDBE record comparison stubs ===== */

UnpackedRecord *sqlite3VdbeAllocUnpackedRecord(KeyInfo *pKeyInfo){
  (void)pKeyInfo;
  return 0;
}

void sqlite3VdbeRecordUnpack(int nKey, const void *pKey, UnpackedRecord *p){
  (void)nKey; (void)pKey; (void)p;
}

RecordCompare sqlite3VdbeFindCompare(UnpackedRecord *p){
  (void)p;
  return sqlite3VdbeRecordCompare;
}

int sqlite3VdbeRecordCompare(int nKey, const void *pKey, UnpackedRecord *p){
  (void)nKey; (void)pKey; (void)p;
  return 0;
}

void sqlite3MemSetArrayInt64(sqlite3_value *aMem, int iIdx, i64 val){
  if( aMem ){
    aMem[iIdx].u.i = val;
    aMem[iIdx].flags = MEM_Int;
  }
}

void sqlite3VdbeDelete(Vdbe *p){ (void)p; }

/* ===== 5. sqlite3_value / Mem accessors (for printf.c, util.c) ===== */

sqlite3_int64 sqlite3_value_int64(sqlite3_value *pVal){
  if( pVal && (pVal->flags & MEM_Int) ) return pVal->u.i;
  return 0;
}

double sqlite3_value_double(sqlite3_value *pVal){
  if( pVal && (pVal->flags & MEM_Real) ) return pVal->u.r;
  if( pVal && (pVal->flags & MEM_Int) ) return (double)pVal->u.i;
  return 0.0;
}

const unsigned char *sqlite3_value_text(sqlite3_value *pVal){
  if( pVal && (pVal->flags & MEM_Str) ) return (const unsigned char*)pVal->z;
  return 0;
}

sqlite3_value *sqlite3ValueNew(sqlite3 *db){
  sqlite3_value *p = sqlite3DbMallocZero(db, sizeof(*p));
  if( p ){
    p->flags = MEM_Null;
    p->db = db;
  }
  return p;
}

void sqlite3ValueSetNull(sqlite3_value *p){
  if( p ){
    p->flags = MEM_Null;
    if( p->zMalloc ){
      sqlite3DbFree(p->db, p->zMalloc);
      p->zMalloc = 0;
    }
    p->z = 0;
    p->n = 0;
  }
}

void sqlite3ValueSetStr(
  sqlite3_value *p,
  int n,
  const void *z,
  u8 enc,
  void (*xDel)(void*)
){
  if( !p ) return;
  sqlite3ValueSetNull(p);
  if( z ){
    int nByte = (n>0 ? n : (int)strlen((const char*)z)) + 1;
    p->z = sqlite3DbMallocRaw(p->db, nByte);
    if( p->z ){
      memcpy(p->z, z, nByte-1);
      p->z[nByte-1] = 0;
      p->n = nByte - 1;
      p->flags = MEM_Str;
      p->enc = enc;
    }
    if( xDel && xDel!=SQLITE_STATIC && xDel!=SQLITE_TRANSIENT ){
      xDel((void*)z);
    }
  }
}

void sqlite3_result_error_code(sqlite3_context *pCtx, int errCode){
  (void)pCtx; (void)errCode;
}

void sqlite3_result_text(
  sqlite3_context *pCtx,
  const char *z,
  int n,
  void (*xDel)(void*)
){
  (void)pCtx; (void)z; (void)n; (void)xDel;
}

/* ===== 6. UTF-8 helper (for printf.c) ===== */

int sqlite3AppendOneUtf8Character(char *zOut, u32 c){
  if( c<0x80 ){
    zOut[0] = (char)c;
    return 1;
  }else if( c<0x800 ){
    zOut[0] = (char)(0xC0 | (c>>6));
    zOut[1] = (char)(0x80 | (c&0x3F));
    return 2;
  }else if( c<0x10000 ){
    zOut[0] = (char)(0xE0 | (c>>12));
    zOut[1] = (char)(0x80 | ((c>>6)&0x3F));
    zOut[2] = (char)(0x80 | (c&0x3F));
    return 3;
  }else{
    zOut[0] = (char)(0xF0 | (c>>18));
    zOut[1] = (char)(0x80 | ((c>>12)&0x3F));
    zOut[2] = (char)(0x80 | ((c>>6)&0x3F));
    zOut[3] = (char)(0x80 | (c&0x3F));
    return 4;
  }
}

/* ===== 7. Upper-layer SQL stubs ===== */

void sqlite3DeleteTable(sqlite3 *db, Table *pTable){
  (void)db; (void)pTable;
}

void sqlite3DeleteTrigger(sqlite3 *db, Trigger *pTrigger){
  (void)db; (void)pTrigger;
}

/* ===== 8. Backup stubs (pager.c calls these) ===== */

void sqlite3BackupUpdate(sqlite3_backup *p, Pgno iPage, const u8 *aData){
  (void)p; (void)iPage; (void)aData;
  /* pager.c guards these calls with if(pBackup), so this is never reached
  ** unless a backup is actually active (which won't happen in SNKV). */
}

void sqlite3BackupRestart(sqlite3_backup *p){
  (void)p;
}
