/* SPDX-License-Identifier: Apache-2.0 */
/*
**
** Key-Value Store API Header
** Built on top of SQLite v3.51.200 btree implementation
**
** This header provides a simple key-value store interface using
** the underlying btree structure from SQLite, with support for
** column families (multiple logical namespaces).
*/

#ifndef _KVSTORE_H_
#define _KVSTORE_H_

#include "sqliteInt.h"
#include "btree.h"

#include <stdint.h>
#include <inttypes.h>

/*
** Compatibility macros: map old SQLite 3.3.0 memory function names
** to their SQLite 3.51.200 equivalents so tests and examples compile
** without changes.
*/
#define sqliteMalloc(n)     sqlite3MallocZero((n))
#define sqliteFree(p)       sqlite3_free((p))
#define sqliteRealloc(p,n)  sqlite3Realloc((p),(uint64_t)(n))
#define sqliteStrDup(s)     sqlite3_mprintf("%s",(s))

#ifdef __cplusplus
extern "C" {
#endif

/*
** KVStore handle - represents an open key-value store
*/
typedef struct KVStore KVStore;

/*
** Column Family handle - represents a logical namespace
*/
typedef struct KVColumnFamily KVColumnFamily;

/*
** Error codes — numeric values match the corresponding SQLITE_* codes so
** that KVSTORE_* and SQLITE_* comparisons are always consistent.
*/
#define KVSTORE_OK        0   /* success */
#define KVSTORE_ERROR     1   /* generic error */
#define KVSTORE_BUSY      5   /* database locked by another connection */
#define KVSTORE_LOCKED    6   /* database locked within same connection */
#define KVSTORE_NOMEM     7   /* malloc() failed */
#define KVSTORE_READONLY  8   /* attempt to write a read-only database */
#define KVSTORE_CORRUPT   11  /* database file is malformed */
#define KVSTORE_NOTFOUND  12  /* key or column family not found */
#define KVSTORE_PROTOCOL  15  /* database lock protocol error */

/*
** Maximum number of column families
*/
#define KVSTORE_MAX_COLUMN_FAMILIES 64

/*
** Journal modes for kvstore_open / KVStoreConfig.journalMode
*/
#define KVSTORE_JOURNAL_DELETE  0   /* Delete rollback journal on commit */
#define KVSTORE_JOURNAL_WAL     1   /* Write-Ahead Logging mode */

/*
** Sync levels for KVStoreConfig.syncLevel
**
** KVSTORE_SYNC_OFF    — no fsync; fastest, but data may be lost on power
**                       failure (process crash is still safe in WAL mode).
** KVSTORE_SYNC_NORMAL — (default) WAL checkpoint syncs once; survives process
**                       crash, not necessarily power loss.
** KVSTORE_SYNC_FULL   — fsync on every commit; power-safe, slower writes.
*/
#define KVSTORE_SYNC_OFF     0
#define KVSTORE_SYNC_NORMAL  1
#define KVSTORE_SYNC_FULL    2

/*
** Checkpoint modes for kvstore_checkpoint().
** These map directly to SQLITE_CHECKPOINT_* values.
*/
#define KVSTORE_CHECKPOINT_PASSIVE   0  /* Copy frames w/o blocking; may not flush all */
#define KVSTORE_CHECKPOINT_FULL      1  /* Wait for writers, then copy all frames       */
#define KVSTORE_CHECKPOINT_RESTART   2  /* Like FULL, then reset WAL write position     */
#define KVSTORE_CHECKPOINT_TRUNCATE  3  /* Like RESTART, then truncate the WAL file     */

/*
** Configuration structure for kvstore_open_v2.
**
** Zero-initialize and set only the fields you need; unset fields use the
** documented defaults.
**
**   KVStoreConfig cfg = {0};
**   cfg.journalMode = KVSTORE_JOURNAL_WAL;  // already the default
**   cfg.busyTimeout = 5000;                 // retry up to 5 seconds
**   kvstore_open_v2("mydb.db", &kv, &cfg);
*/
typedef struct KVStoreConfig KVStoreConfig;
struct KVStoreConfig {
  /*
  ** journalMode — KVSTORE_JOURNAL_WAL (default) or KVSTORE_JOURNAL_DELETE.
  ** WAL mode allows concurrent readers with a single writer and is strongly
  ** recommended for most workloads.
  */
  int journalMode;

  /*
  ** syncLevel — KVSTORE_SYNC_NORMAL (default), KVSTORE_SYNC_OFF, or
  ** KVSTORE_SYNC_FULL.  Controls how aggressively the pager fsyncs.
  ** In WAL mode NORMAL and FULL have nearly identical performance.
  */
  int syncLevel;

  /*
  ** cacheSize — page cache size in pages (0 = use built-in default: 2000
  ** pages ≈ 8 MB with 4096-byte pages).  Larger caches improve read-heavy
  ** workloads at the cost of RSS.
  */
  int cacheSize;

  /*
  ** pageSize — database page size in bytes (0 = use built-in default: 4096).
  ** Must be a power of two between 512 and 65536.
  ** Ignored for existing databases (the stored page size wins).
  ** Must be set before the first write; has no effect on existing databases.
  */
  int pageSize;

  /*
  ** readOnly — set to 1 to open the database read-only.  All write
  ** operations (put, delete, begin(wrflag=1), etc.) will return
  ** KVSTORE_READONLY.  Default: 0 (read-write).
  */
  int readOnly;

  /*
  ** busyTimeout — milliseconds to keep retrying when the database is locked
  ** by another connection (SQLITE_BUSY).  0 (default) means fail immediately.
  ** Useful for multi-process access patterns.
  */
  int busyTimeout;

  /*
  ** walSizeLimit — WAL auto-checkpoint threshold in committed write transactions.
  **   0 (default) — no auto-checkpoint.
  **   N > 0       — after every N committed write transactions a PASSIVE checkpoint
  **                 is attempted automatically. Only effective in WAL journal mode.
  */
  int walSizeLimit;
};

/*
** Memory helpers — wrappers around the SQLite allocator.
**
** snkv_malloc(n) — allocate n bytes, zero-initialised.
** snkv_free(p)   — free a pointer returned by snkv_malloc or kvstore_get.
**
** When including snkv.h without SNKV_IMPLEMENTATION (header-only use),
** the sqlite3 allocator functions are forward-declared here so the
** macros compile without the full implementation present.
*/
#ifndef SNKV_IMPLEMENTATION
void *sqlite3MallocZero(unsigned long long);
void  sqlite3_free(void *);
#endif

#define snkv_malloc(n)  sqlite3MallocZero((unsigned long long)(n))
#define snkv_free(p)    sqlite3_free((p))

/*
** Open a key-value store with full configuration control.
**
** Parameters:
**   zFilename - Path to the database file (NULL for in-memory)
**   ppKV      - Output pointer to KVStore handle
**   pConfig   - Configuration (NULL uses all defaults, same as kvstore_open
**               with KVSTORE_JOURNAL_WAL)
**
** Default values when pConfig is NULL or a field is 0:
**   journalMode  KVSTORE_JOURNAL_WAL
**   syncLevel    KVSTORE_SYNC_NORMAL
**   cacheSize    2000 pages (~8 MB)
**   pageSize     4096 bytes (new databases only)
**   readOnly     0 (read-write)
**   busyTimeout  0 ms (fail immediately on lock)
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_open_v2(
  const char *zFilename,
  KVStore **ppKV,
  const KVStoreConfig *pConfig
);

/*
** Open a key-value store database file (simplified interface).
**
** Equivalent to kvstore_open_v2 with pConfig->journalMode = journalMode
** and all other fields at their defaults.
**
** Parameters:
**   zFilename   - Path to the database file (NULL for in-memory)
**   ppKV        - Output pointer to KVStore handle
**   journalMode - KVSTORE_JOURNAL_DELETE or KVSTORE_JOURNAL_WAL
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
**
** Note: The database is always opened with incremental auto-vacuum enabled.
**       Call kvstore_incremental_vacuum() to reclaim unused space on demand.
*/
int kvstore_open(
  const char *zFilename,
  KVStore **ppKV,
  int journalMode
);

/*
** Close a key-value store and free all associated resources.
**
** Parameters:
**   pKV - KVStore handle to close
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_close(KVStore *pKV);

/* ========== COLUMN FAMILY OPERATIONS ========== */

/*
** Create a new column family.
**
** Parameters:
**   pKV    - KVStore handle
**   zName  - Column family name (must be unique)
**   ppCF   - Output pointer to column family handle
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
**
** Note: Column family names are limited to 255 characters
*/
int kvstore_cf_create(
  KVStore *pKV,
  const char *zName,
  KVColumnFamily **ppCF
);

/*
** Open an existing column family.
**
** Parameters:
**   pKV    - KVStore handle
**   zName  - Column family name
**   ppCF   - Output pointer to column family handle
**
** Returns:
**   KVSTORE_OK on success
**   KVSTORE_NOTFOUND if column family doesn't exist
*/
int kvstore_cf_open(
  KVStore *pKV,
  const char *zName,
  KVColumnFamily **ppCF
);

/*
** Get the default column family (always exists).
**
** Parameters:
**   pKV  - KVStore handle
**   ppCF - Output pointer to column family handle
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
**
** Note: The default column family is created automatically on database open
*/
int kvstore_cf_get_default(
  KVStore *pKV,
  KVColumnFamily **ppCF
);

/*
** Drop a column family (delete all data and metadata).
**
** Parameters:
**   pKV   - KVStore handle
**   zName - Column family name
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
**
** Note: Cannot drop the default column family
*/
int kvstore_cf_drop(
  KVStore *pKV,
  const char *zName
);

/*
** List all column families in the database.
**
** Parameters:
**   pKV      - KVStore handle
**   pazNames - Output array of column family names (caller must free)
**   pnCount  - Output count of column families
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
**
** Note: Caller must free each name with sqliteFree(), then free the array
*/
int kvstore_cf_list(
  KVStore *pKV,
  char ***pazNames,
  int *pnCount
);

/*
** Close a column family handle (does not delete the column family).
**
** Parameters:
**   pCF - Column family handle
*/
void kvstore_cf_close(KVColumnFamily *pCF);

/* ========== KEY-VALUE OPERATIONS (DEFAULT CF) ========== */

/*
** Insert or update a key-value pair in the default column family.
**
** Parameters:
**   pKV      - KVStore handle
**   pKey     - Pointer to key data
**   nKey     - Length of key in bytes
**   pValue   - Pointer to value data
**   nValue   - Length of value in bytes
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
**
** Note: If the key already exists, its value will be updated.
*/
int kvstore_put(
  KVStore *pKV,
  const void *pKey,
  int nKey,
  const void *pValue,
  int nValue
);

/*
** Retrieve a value by key from the default column family.
**
** Parameters:
**   pKV      - KVStore handle
**   pKey     - Pointer to key data
**   nKey     - Length of key in bytes
**   ppValue  - Output pointer to value data (caller must free)
**   pnValue  - Output pointer to value length
**
** Returns:
**   KVSTORE_OK on success
**   KVSTORE_NOTFOUND if key doesn't exist
**   Other error codes on failure
**
** Note: Caller is responsible for freeing *ppValue with sqliteFree()
*/
int kvstore_get(
  KVStore *pKV,
  const void *pKey,
  int nKey,
  void **ppValue,
  int *pnValue
);

/*
** Delete a key-value pair from the default column family.
**
** Parameters:
**   pKV  - KVStore handle
**   pKey - Pointer to key data
**   nKey - Length of key in bytes
**
** Returns:
**   KVSTORE_OK on success
**   KVSTORE_NOTFOUND if key doesn't exist
**   Other error codes on failure
*/
int kvstore_delete(
  KVStore *pKV,
  const void *pKey,
  int nKey
);

/*
** Check if a key exists in the default column family.
**
** Parameters:
**   pKV     - KVStore handle
**   pKey    - Pointer to key data
**   nKey    - Length of key in bytes
**   pExists - Output: 1 if exists, 0 if not
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_exists(
  KVStore *pKV,
  const void *pKey,
  int nKey,
  int *pExists
);

/* ========== KEY-VALUE OPERATIONS (SPECIFIC CF) ========== */

/*
** Insert or update a key-value pair in a specific column family.
**
** Parameters:
**   pCF      - Column family handle
**   pKey     - Pointer to key data
**   nKey     - Length of key in bytes
**   pValue   - Pointer to value data
**   nValue   - Length of value in bytes
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_cf_put(
  KVColumnFamily *pCF,
  const void *pKey,
  int nKey,
  const void *pValue,
  int nValue
);

/*
** Retrieve a value by key from a specific column family.
**
** Parameters:
**   pCF      - Column family handle
**   pKey     - Pointer to key data
**   nKey     - Length of key in bytes
**   ppValue  - Output pointer to value data (caller must free)
**   pnValue  - Output pointer to value length
**
** Returns:
**   KVSTORE_OK on success
**   KVSTORE_NOTFOUND if key doesn't exist
*/
int kvstore_cf_get(
  KVColumnFamily *pCF,
  const void *pKey,
  int nKey,
  void **ppValue,
  int *pnValue
);

/*
** Delete a key-value pair from a specific column family.
**
** Parameters:
**   pCF  - Column family handle
**   pKey - Pointer to key data
**   nKey - Length of key in bytes
**
** Returns:
**   KVSTORE_OK on success
**   KVSTORE_NOTFOUND if key doesn't exist
*/
int kvstore_cf_delete(
  KVColumnFamily *pCF,
  const void *pKey,
  int nKey
);

/*
** Check if a key exists in a specific column family.
**
** Parameters:
**   pCF     - Column family handle
**   pKey    - Pointer to key data
**   nKey    - Length of key in bytes
**   pExists - Output: 1 if exists, 0 if not
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_cf_exists(
  KVColumnFamily *pCF,
  const void *pKey,
  int nKey,
  int *pExists
);

/*
** Iterator structure for traversing the key-value store
*/
typedef struct KVIterator KVIterator;

/*
** Create an iterator for the default column family.
**
** Parameters:
**   pKV   - KVStore handle
**   ppIter - Output pointer to iterator
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_iterator_create(
  KVStore *pKV,
  KVIterator **ppIter
);

/*
** Create an iterator for a specific column family.
**
** Parameters:
**   pCF    - Column family handle
**   ppIter - Output pointer to iterator
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_cf_iterator_create(
  KVColumnFamily *pCF,
  KVIterator **ppIter
);

/*
** Create a prefix iterator for the default column family.
** The iterator is pre-positioned at the first key whose bytes start
** with (pPrefix, nPrefix).  Subsequent kvstore_iterator_next() calls
** automatically stop when keys no longer match the prefix.
**
** Parameters:
**   pKV      - KVStore handle
**   pPrefix  - Prefix bytes to search for
**   nPrefix  - Length of prefix in bytes
**   ppIter   - Output pointer to iterator
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
**
** Note: The iterator is already positioned; do NOT call
**       kvstore_iterator_first() — just read key/value directly.
*/
int kvstore_prefix_iterator_create(
  KVStore *pKV,
  const void *pPrefix, int nPrefix,
  KVIterator **ppIter
);

/*
** Create a prefix iterator for a specific column family.
**
** Parameters:
**   pCF      - Column family handle
**   pPrefix  - Prefix bytes to search for
**   nPrefix  - Length of prefix in bytes
**   ppIter   - Output pointer to iterator
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_cf_prefix_iterator_create(
  KVColumnFamily *pCF,
  const void *pPrefix, int nPrefix,
  KVIterator **ppIter
);

/*
** Move iterator to the first key-value pair.
**
** Parameters:
**   pIter - Iterator handle
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_iterator_first(KVIterator *pIter);

/*
** Move iterator to the next key-value pair.
**
** Parameters:
**   pIter - Iterator handle
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_iterator_next(KVIterator *pIter);

/*
** Check if iterator has reached the end.
**
** Parameters:
**   pIter - Iterator handle
**
** Returns:
**   1 if at end, 0 otherwise
*/
int kvstore_iterator_eof(KVIterator *pIter);

/*
** Get current key from iterator.
**
** Parameters:
**   pIter   - Iterator handle
**   ppKey   - Output pointer to key data (do not free)
**   pnKey   - Output pointer to key length
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_iterator_key(
  KVIterator *pIter,
  void **ppKey,
  int *pnKey
);

/*
** Get current value from iterator.
**
** Parameters:
**   pIter    - Iterator handle
**   ppValue  - Output pointer to value data (do not free)
**   pnValue  - Output pointer to value length
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_iterator_value(
  KVIterator *pIter,
  void **ppValue,
  int *pnValue
);

/*
** Close and free an iterator.
**
** Parameters:
**   pIter - Iterator handle
*/
void kvstore_iterator_close(KVIterator *pIter);

/*
** Begin a transaction.
**
** Parameters:
**   pKV      - KVStore handle
**   wrflag   - 1 for write transaction, 0 for read-only
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_begin(KVStore *pKV, int wrflag);

/*
** Commit the current transaction.
**
** Parameters:
**   pKV - KVStore handle
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_commit(KVStore *pKV);

/*
** Rollback the current transaction.
**
** Parameters:
**   pKV - KVStore handle
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_rollback(KVStore *pKV);

/*
** Get the last error message from a KVStore handle.
**
** Parameters:
**   pKV - KVStore handle
**
** Returns:
**   Error message string (do not free)
*/
const char *kvstore_errmsg(KVStore *pKV);

/*
** Statistics structure
*/
typedef struct KVStoreStats KVStoreStats;
struct KVStoreStats {
  uint64_t nPuts;       /* Number of put operations */
  uint64_t nGets;       /* Number of get operations */
  uint64_t nDeletes;    /* Number of delete operations */
  uint64_t nIterations; /* Number of iterations created */
  uint64_t nErrors;     /* Number of errors encountered */
};

/*
** Get statistics from the key-value store.
**
** Parameters:
**   pKV    - KVStore handle
**   pStats - Output statistics structure
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_stats(KVStore *pKV, KVStoreStats *pStats);

/*
** Perform an integrity check on the database.
**
** Parameters:
**   pKV      - KVStore handle
**   pzErrMsg - Output pointer to error message (caller must free with sqliteFree)
**
** Returns:
**   KVSTORE_OK if database is ok
**   KVSTORE_CORRUPT if corruption detected
*/
int kvstore_integrity_check(KVStore *pKV, char **pzErrMsg);

/*
** Synchronize the database to disk.
**
** Parameters:
**   pKV - KVStore handle
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_sync(KVStore *pKV);

/*
** Run an incremental vacuum step, freeing up to nPage pages.
**
** Reclaims unused pages and shrinks the database file.
** Databases are always opened with incremental auto-vacuum enabled,
** so this function can be called at any time to reclaim space.
**
** Parameters:
**   pKV    - KVStore handle
**   nPage  - Maximum number of pages to free (0 = free all)
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_incremental_vacuum(KVStore *pKV, int nPage);

/*
** Run a WAL checkpoint on the database.
**
** Copies WAL frames back into the main database file. Any open write
** transaction must be committed or rolled back before calling; calling
** while a write transaction is active returns KVSTORE_BUSY.
**
** The persistent read transaction is temporarily released and restored
** around the checkpoint call (required by the btree layer).
**
** Parameters:
**   pKV    - KVStore handle
**   mode   - KVSTORE_CHECKPOINT_PASSIVE / FULL / RESTART / TRUNCATE
**   pnLog  - Output: WAL frames total after checkpoint (may be NULL)
**   pnCkpt - Output: frames successfully written to DB (may be NULL)
**
** Returns:
**   KVSTORE_OK    on success
**   KVSTORE_BUSY  if a write transaction is currently open
**   KVSTORE_ERROR on other failure
**
** Note: On non-WAL (DELETE journal) databases this is a no-op that
**       returns KVSTORE_OK with *pnLog = *pnCkpt = 0.
*/
int kvstore_checkpoint(KVStore *pKV, int mode, int *pnLog, int *pnCkpt);

#ifdef __cplusplus
}
#endif

#endif /* _KVSTORE_H_ */
