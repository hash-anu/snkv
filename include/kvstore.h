/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright 2025 SNKV Contributors */
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
#define KVSTORE_AUTH_FAILED 13 /* wrong password or corrupted encrypted store */
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
** KVSTORE_NO_TTL — sentinel returned by kvstore_ttl_remaining / kvstore_get_ttl
** when the key exists but has no expiry associated with it.
*/
#define KVSTORE_NO_TTL  ((int64_t)(-1))

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

  /* Internal flag — set by kvstore_open_encrypted to bypass the plain-open
  ** encryption guard.  Not part of the public API; zero-init leaves it 0. */
  int allowEncrypted;
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
** Insert (key, value) into the default column family only if the key does
** not already exist.
**
**   expire_ms > 0  — absolute expiry in ms (use kvstore_now_ms() + delta).
**                    Applied only when the key is newly inserted.
**                    Ignored if the key already exists.
**   expire_ms == 0 — no TTL.
**
**   *pInserted (may be NULL):
**     1 — key was absent; value was written.
**     0 — key already existed; store is unchanged.
**
** Expired keys are treated as absent: a key that exists but has expired is
** lazily deleted and the new value is inserted.
**
** The data write and the TTL entry (if any) are committed atomically in a
** single write transaction. If the caller already holds an explicit write
** transaction (kvstore_begin with wrflag=1), the operation joins it without
** issuing a nested commit.
**
** Returns KVSTORE_OK whether or not the key was inserted.
** Returns an error code only on I/O or locking failure.
*/
int kvstore_put_if_absent(
  KVStore *pKV,
  const void *pKey,   int nKey,
  const void *pValue, int nValue,
  int64_t expire_ms,
  int *pInserted
);

/*
** Insert (key, value) into a specific column family only if absent.
** Equivalent to kvstore_put_if_absent() but operates on an explicit CF handle.
*/
int kvstore_cf_put_if_absent(
  KVColumnFamily *pCF,
  const void *pKey,   int nKey,
  const void *pValue, int nValue,
  int64_t expire_ms,
  int *pInserted
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
** Create a reverse iterator for the default column family.
**
** A reverse iterator starts before the last entry.  Call
** kvstore_iterator_last() to move to the largest key, then
** kvstore_iterator_prev() to advance toward smaller keys.
**
** Returns:
**   KVSTORE_OK on success, error code otherwise.
**
** Thread safety: creation is thread-safe; the returned KVIterator *
** handle must be used by a single thread at a time.
*/
int kvstore_reverse_iterator_create(KVStore *pKV, KVIterator **ppIter);

/*
** CF variant of kvstore_reverse_iterator_create.
*/
int kvstore_cf_reverse_iterator_create(KVColumnFamily *pCF, KVIterator **ppIter);

/*
** Create a reverse prefix iterator for the default column family.
**
** Scoped to keys that begin with (pPrefix, nPrefix).
** kvstore_iterator_last() positions at the last key with the given prefix;
** kvstore_iterator_prev() walks backward and sets eof=1 when keys no longer
** match the prefix.
**
** pPrefix bytes are copied; the caller may free the buffer immediately.
**
** Returns:
**   KVSTORE_OK on success, error code otherwise.
*/
int kvstore_reverse_prefix_iterator_create(
  KVStore *pKV,
  const void *pPrefix, int nPrefix,
  KVIterator **ppIter
);

/*
** CF variant of kvstore_reverse_prefix_iterator_create.
*/
int kvstore_cf_reverse_prefix_iterator_create(
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
** Position a reverse iterator at the last (largest) key.
**
** Equivalent to kvstore_iterator_first() for forward iterators.
** Returns KVSTORE_ERROR if called on a forward iterator (direction mismatch).
**
** Parameters:
**   pIter - Reverse iterator handle
**
** Returns:
**   KVSTORE_OK on success (check kvstore_iterator_eof for emptiness).
*/
int kvstore_iterator_last(KVIterator *pIter);

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
** Advance a reverse iterator to the previous (smaller) key.
**
** Equivalent to kvstore_iterator_next() for forward iterators.
** Sets eof=1 when the beginning (or prefix boundary) is reached.
** Returns KVSTORE_ERROR if called on a forward iterator (direction mismatch).
**
** Parameters:
**   pIter - Reverse iterator handle
**
** Returns:
**   KVSTORE_OK on success, error code otherwise.
*/
int kvstore_iterator_prev(KVIterator *pIter);

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
** Position the iterator at a target key.
**
** Forward iterator (reverse=0): positions at the first key >= (pKey, nKey).
** Reverse iterator (reverse=1): positions at the last  key <= (pKey, nKey).
**
** Works on any iterator type — plain, prefix, reverse, reverse-prefix.
** If the iterator has a prefix filter the seeked position is validated
** against the prefix; if it falls outside the prefix range eof is set.
**
** Calling seek resets the iterator started state — it is safe to seek
** multiple times on the same iterator without closing and recreating it.
**
** After seek, call kvstore_iterator_eof to check for emptiness, then
** kvstore_iterator_next (forward) or kvstore_iterator_prev (reverse)
** to advance.
**
** Thread safety: the iterator handle must be used by a single thread.
**
** Parameters:
**   pIter - Iterator handle (must be open)
**   pKey  - Target key bytes
**   nKey  - Length of target key in bytes
**
** Returns:
**   KVSTORE_OK    — positioned successfully (check kvstore_iterator_eof)
**   KVSTORE_ERROR — iterator not open or internal error
*/
int kvstore_iterator_seek(KVIterator *pIter, const void *pKey, int nKey);

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
  /* --- Existing fields (unchanged, same order) --- */
  uint64_t nPuts;         /* Total put operations */
  uint64_t nGets;         /* Total get operations */
  uint64_t nDeletes;      /* Total delete operations */
  uint64_t nIterations;   /* Total iterators created */
  uint64_t nErrors;       /* Total errors encountered */

  /* --- I/O throughput --- */
  uint64_t nBytesRead;    /* Total value bytes returned by get operations */
  uint64_t nBytesWritten; /* Total (key+value) bytes written by put operations */

  /* --- WAL & transactions --- */
  uint64_t nWalCommits;   /* Write transactions successfully committed */
  uint64_t nCheckpoints;  /* WAL checkpoints performed */

  /* --- TTL activity --- */
  uint64_t nTtlExpired;   /* Keys lazily expired on get/exists */
  uint64_t nTtlPurged;    /* Keys removed by kvstore_purge_expired */

  /* --- Storage (read from B-tree at kvstore_stats() call time) --- */
  uint64_t nDbPages;      /* Total pages in database (sqlite3BtreeLastPage) */
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
** Reset all cumulative stat counters to zero.
**
** nDbPages is always read live from the B-tree and is unaffected by reset.
** Useful for measuring rates over a time window:
**
**   kvstore_stats_reset(db);
**   // ... run workload for 1 second ...
**   kvstore_stats(db, &s);
**   printf("puts/sec: %llu\n", (unsigned long long)s.nPuts);
**
** Returns:
**   KVSTORE_OK on success, error code otherwise.
*/
int kvstore_stats_reset(KVStore *pKV);

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

/* ========== BULK OPERATIONS ========== */

/*
** Remove all key-value pairs from the default column family.
**
** Also clears all associated TTL index entries, even if the TTL index CFs
** were not opened in the current session (hasTtl=1 but handles are NULL).
** The column family root page and CF metadata remain intact — the CF is
** empty and ready for new inserts after this call.
**
** Precondition: no iterators may be open on the default CF when clear is
** called. Calling clear with open iterators produces undefined behaviour.
** Close all iterators before calling clear.
**
** Returns:
**   KVSTORE_OK on success, error code otherwise.
*/
int kvstore_clear(KVStore *pKV);

/*
** Remove all key-value pairs from the given column family.
** Equivalent to kvstore_clear() but operates on an explicit CF handle.
** Same preconditions and guarantees apply.
*/
int kvstore_cf_clear(KVColumnFamily *pCF);

/* ========== COUNT OPERATIONS ========== */

/*
** Count the number of entries in the default column family.
**
** Complexity: O(pages) — visits each B-tree page once and reads nCell from
** the page header without fetching key or value payload. For a warm cache
** this is fast; for a cold large database it triggers one page read per page.
**
** The count includes keys that have expired but not yet been lazily deleted.
** Call kvstore_purge_expired() first for an accurate live count.
**
** Behaviour during an uncommitted write transaction: uses the cached
** read-only cursor which may not see uncommitted puts from the write cursor.
** Commit the transaction first for an accurate post-write count.
**
** *pnCount must not be NULL.
**
** Returns:
**   KVSTORE_OK on success, error code otherwise.
*/
int kvstore_count(KVStore *pKV, int64_t *pnCount);

/*
** Count entries in the given column family.
** Counts only the main data B-tree — TTL index CFs are excluded.
** Same complexity and caveats as kvstore_count.
*/
int kvstore_cf_count(KVColumnFamily *pCF, int64_t *pnCount);

/* ========== TTL OPERATIONS ========== */

/*
** Return the current time in milliseconds since the Unix epoch.
** Use this to compute absolute expiry values for kvstore_put_ttl:
**
**   int64_t expire_ms = kvstore_now_ms() + 30000;  // 30 seconds from now
*/
int64_t kvstore_now_ms(void);

/*
** Insert or update key/value with an expiry time.
**
**   expire_ms > 0  — absolute expiry in ms since Unix epoch.
**   expire_ms == 0 — write with no TTL (removes any existing TTL entry).
**
** Both the data write and the TTL entry are committed atomically.
**
** Returns:
**   KVSTORE_OK on success, error code otherwise.
*/
int kvstore_put_ttl(
  KVStore *pKV,
  const void *pKey, int nKey,
  const void *pValue, int nValue,
  int64_t expire_ms
);

/*
** Retrieve a value with lazy TTL expiry check.
**
** If the key has expired:
**   - The key and its TTL entry are deleted from the store.
**   - Returns KVSTORE_NOTFOUND.
**   - *pnRemaining is set to 0 (if not NULL).
**
** If the key is valid:
**   - *ppValue / *pnValue are populated. Caller must snkv_free(*ppValue).
**   - *pnRemaining is set to remaining ms, or KVSTORE_NO_TTL if no expiry.
**
** pnRemaining may be NULL.
*/
int kvstore_get_ttl(
  KVStore *pKV,
  const void *pKey, int nKey,
  void **ppValue, int *pnValue,
  int64_t *pnRemaining
);

/*
** Return the remaining TTL for a key in milliseconds.
**
**   KVSTORE_NO_TTL (-1) — key exists but has no expiry.
**   0                   — key just expired (lazy delete performed).
**   N > 0               — N ms remain.
**
** Returns KVSTORE_NOTFOUND if the key does not exist at all.
** pnRemaining must not be NULL.
*/
int kvstore_ttl_remaining(
  KVStore *pKV,
  const void *pKey, int nKey,
  int64_t *pnRemaining
);

/*
** Scan the TTL index and delete all expired key-value pairs in a single
** write transaction.  Only TTL-bearing keys are scanned — O(TTL keys),
** not O(all data keys).
**
** *pnDeleted (may be NULL) — set to the number of expired keys removed.
**
** Returns:
**   KVSTORE_OK on success (including the case where nothing was expired).
*/
int kvstore_purge_expired(KVStore *pKV, int *pnDeleted);

/* ========== CF-LEVEL TTL OPERATIONS ========== */

/*
** Insert or update key/value in the given column family with an expiry time.
** Equivalent to kvstore_put_ttl() but operates on an explicit CF handle.
*/
int kvstore_cf_put_ttl(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  const void *pValue, int nValue,
  int64_t expire_ms
);

/*
** Get value from a column family with lazy TTL expiry check.
** Equivalent to kvstore_get_ttl() but operates on an explicit CF handle.
*/
int kvstore_cf_get_ttl(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  void **ppValue, int *pnValue,
  int64_t *pnRemaining
);

/*
** Return remaining TTL in milliseconds for a key in the given column family.
** Equivalent to kvstore_ttl_remaining() but operates on an explicit CF handle.
*/
int kvstore_cf_ttl_remaining(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  int64_t *pnRemaining
);

/*
** Purge expired keys from the given column family.
** Equivalent to kvstore_purge_expired() but operates on an explicit CF handle.
*/
int kvstore_cf_purge_expired(KVColumnFamily *pCF, int *pnDeleted);


/* =========================================================================
** Encryption API — password-based authenticated encryption (XChaCha20-Poly1305)
** =========================================================================
**
** An encrypted store uses Argon2id to derive a 256-bit key from the password.
** Every value is individually wrapped: [nonce(24)][ciphertext(N)][mac(16)].
** Keys are stored in plaintext so all B-tree ordering, prefix, seek, range
** and reverse-iteration operations work exactly as on unencrypted stores.
**
** Auth metadata (salt, KDF params, verify tag) is kept in a dedicated
** internal column family "__snkv_auth__".  Opening an encrypted store with
** the wrong password returns KVSTORE_AUTH_FAILED.
*/

/*
** Open (or create) an encrypted key-value store.
**
**   zFilename  — path to the database file.
**   pPassword  — raw password bytes.
**   nPassword  — length of password in bytes.
**   ppKV       — set to the open KVStore handle on success.
**   pConfig    — optional config (may be NULL for defaults).
**
** If the file does not exist it is created and the password is written.
** If the file exists the password is verified; KVSTORE_AUTH_FAILED is
** returned on mismatch.  Returns KVSTORE_OK on success.
*/
int kvstore_open_encrypted(
  const char *zFilename,
  const void *pPassword, int nPassword,
  KVStore **ppKV,
  const KVStoreConfig *pConfig
);

/*
** Return 1 if pKV was opened with encryption, 0 otherwise.
*/
int kvstore_is_encrypted(KVStore *pKV);

/*
** Re-key an encrypted store with a new password.
** The store must currently be open with encryption (KVSTORE_AUTH_FAILED if
** the current key is wrong; KVSTORE_ERROR if the store is not encrypted).
** All data is re-encrypted in a single transaction.
*/
int kvstore_reencrypt(
  KVStore *pKV,
  const void *pNewPassword, int nNewPassword
);

/*
** Remove encryption from a store, rewriting all values in plaintext.
** The store must currently be open with encryption.
** Returns KVSTORE_ERROR if the store is not encrypted.
*/
int kvstore_remove_encryption(KVStore *pKV);

#ifdef __cplusplus
}
#endif

#endif /* _KVSTORE_H_ */
