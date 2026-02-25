# SNKV API Specification

**Version:** 1.1
**Built on:** SQLite v3.51.200 B-tree engine
**License:** Apache-2.0

## Table of Contents

- [Overview](#overview)
- [Error Codes](#error-codes)
- [Constants](#constants)
- [Configuration](#configuration)
- [Database Lifecycle](#database-lifecycle)
- [Key-Value Operations (Default Column Family)](#key-value-operations-default-column-family)
- [Column Family Management](#column-family-management)
- [Key-Value Operations (Specific Column Family)](#key-value-operations-specific-column-family)
- [Iterators](#iterators)
- [Prefix Iterators](#prefix-iterators)
- [Transactions](#transactions)
- [Diagnostics](#diagnostics)
- [Memory Management](#memory-management)
- [Thread Safety](#thread-safety)

---

## Overview

SNKV is an embedded key-value store built directly on SQLite's B-tree layer. It provides:

- Binary keys and values of arbitrary length
- Column families (multiple logical namespaces in one file)
- ACID transactions with WAL or rollback journal
- Prefix search via prefix iterators
- Thread-safe operations with mutex protection

### Types

| Type | Description |
|------|-------------|
| `KeyValueStore` | Opaque handle to an open database |
| `KeyValueColumnFamily` | Opaque handle to a column family |
| `KeyValueIterator` | Opaque handle to a key-value iterator |
| `KeyValueStoreStats` | Statistics counters (see [Diagnostics](#diagnostics)) |
| `KeyValueStoreConfig` | Configuration struct for `keyvaluestore_open_v2` (see [Configuration](#configuration)) |

---

## Error Codes

| Code | Value | Description |
|------|-------|-------------|
| `KEYVALUESTORE_OK` | 0 | Operation succeeded |
| `KEYVALUESTORE_ERROR` | 1 | Generic error |
| `KEYVALUESTORE_BUSY` | 5 | Database locked by another connection (retry or use `busyTimeout`) |
| `KEYVALUESTORE_LOCKED` | 6 | Database locked within the same connection |
| `KEYVALUESTORE_NOMEM` | 7 | Memory allocation failed |
| `KEYVALUESTORE_READONLY` | 8 | Database is read-only |
| `KEYVALUESTORE_CORRUPT` | 11 | Database file is corrupted |
| `KEYVALUESTORE_NOTFOUND` | 12 | Key or column family not found |
| `KEYVALUESTORE_PROTOCOL` | 15 | Database lock protocol error |

All error codes are aliases for the corresponding `SQLITE_*` codes.

---

## Constants

### Journal modes

| Constant | Value | Description |
|----------|-------|-------------|
| `KEYVALUESTORE_JOURNAL_DELETE` | 0 | Rollback journal mode (delete journal on commit) |
| `KEYVALUESTORE_JOURNAL_WAL` | 1 | Write-Ahead Logging mode (recommended) |

### Sync levels

| Constant | Value | Description |
|----------|-------|-------------|
| `KEYVALUESTORE_SYNC_OFF` | 0 | No fsync — fastest, data at risk on power failure |
| `KEYVALUESTORE_SYNC_NORMAL` | 1 | WAL-safe (default) — survives process crash, not power loss |
| `KEYVALUESTORE_SYNC_FULL` | 2 | Power-safe — fsync on every commit, slower writes |

> In WAL mode (`KEYVALUESTORE_JOURNAL_WAL`) there is virtually no write-throughput
> difference between `SYNC_NORMAL` and `SYNC_FULL`.

### Checkpoint modes

Used with `keyvaluestore_checkpoint()`. Map directly to `SQLITE_CHECKPOINT_*` values.

| Constant | Value | Description |
|----------|-------|-------------|
| `KEYVALUESTORE_CHECKPOINT_PASSIVE` | 0 | Copy frames without blocking; may not flush all if readers hold back frames |
| `KEYVALUESTORE_CHECKPOINT_FULL` | 1 | Wait for active writers to finish, then copy all frames |
| `KEYVALUESTORE_CHECKPOINT_RESTART` | 2 | Like FULL, then reset the WAL write position to the start |
| `KEYVALUESTORE_CHECKPOINT_TRUNCATE` | 3 | Like RESTART, then truncate the WAL file to zero bytes |

### Other constants

| Constant | Value | Description |
|----------|-------|-------------|
| `KEYVALUESTORE_MAX_COLUMN_FAMILIES` | 64 | Maximum number of column families per database |

---

## Configuration

### KeyValueStoreConfig

```c
typedef struct KeyValueStoreConfig KeyValueStoreConfig;
struct KeyValueStoreConfig {
  int journalMode;  /* KEYVALUESTORE_JOURNAL_WAL (default) or KEYVALUESTORE_JOURNAL_DELETE */
  int syncLevel;    /* KEYVALUESTORE_SYNC_NORMAL (default), KEYVALUESTORE_SYNC_OFF, or KEYVALUESTORE_SYNC_FULL */
  int cacheSize;    /* Page cache in pages (0 = default = 2000 pages ≈ 8 MB) */
  int pageSize;     /* Page size in bytes (0 = default = 4096; new databases only) */
  int readOnly;     /* 1 = open read-only; default 0 */
  int busyTimeout;   /* ms to retry on SQLITE_BUSY (0 = fail immediately; default 0) */
  int walSizeLimit;  /* auto-checkpoint every N commits in WAL mode (0 = disabled) */
};
```

**Field defaults** (applied when the field is 0 / structure is zero-initialised):

| Field | Default | Notes |
|-------|---------|-------|
| `journalMode` | `KEYVALUESTORE_JOURNAL_WAL` | WAL is strongly recommended |
| `syncLevel` | `KEYVALUESTORE_SYNC_NORMAL` | Safe against process crash |
| `cacheSize` | 2000 pages (~8 MB) | Larger cache improves read-heavy workloads |
| `pageSize` | 4096 bytes | Ignored for existing databases |
| `readOnly` | 0 (read-write) | |
| `busyTimeout` | 0 (fail immediately) | Set > 0 for multi-process workloads |
| `walSizeLimit` | 0 (disabled) | Auto-checkpoint every N commits; 0 = no auto-checkpoint |

---

## Database Lifecycle

### keyvaluestore_open_v2

Open or create a key-value store with full configuration control.

```c
int keyvaluestore_open_v2(
  const char *zFilename,
  KeyValueStore **ppKV,
  const KeyValueStoreConfig *pConfig
);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `zFilename` | `const char *` | Path to database file. `NULL` for in-memory. |
| `ppKV` | `KeyValueStore **` | Output pointer to the opened `KeyValueStore` handle. |
| `pConfig` | `const KeyValueStoreConfig *` | Configuration. `NULL` uses all defaults (same as `keyvaluestore_open` with `KEYVALUESTORE_JOURNAL_WAL`). |

**Returns:** `KEYVALUESTORE_OK` on success, error code otherwise.

**Examples:**

```c
/* Default config — WAL mode, NORMAL sync, 8 MB cache */
KeyValueStore *kv;
keyvaluestore_open_v2("mydb.db", &kv, NULL);

/* Explicitly configured */
KeyValueStoreConfig cfg = {0};
cfg.journalMode = KEYVALUESTORE_JOURNAL_WAL;
cfg.syncLevel   = KEYVALUESTORE_SYNC_FULL;     /* power-safe */
cfg.cacheSize   = 4000;                  /* ~16 MB cache */
cfg.busyTimeout = 5000;                  /* retry up to 5 s */
keyvaluestore_open_v2("mydb.db", &kv, &cfg);

/* Read-only open */
KeyValueStoreConfig ro = {0};
ro.readOnly = 1;
keyvaluestore_open_v2("mydb.db", &kv, &ro);
```

---

### keyvaluestore_open

Open or create a key-value store (simplified interface).

Equivalent to calling `keyvaluestore_open_v2` with `pConfig->journalMode = journalMode`
and all other fields at their defaults.

```c
int keyvaluestore_open(
  const char *zFilename,
  KeyValueStore **ppKV,
  int journalMode
);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `zFilename` | `const char *` | Path to database file. Pass `NULL` for in-memory database. |
| `ppKV` | `KeyValueStore **` | Output pointer to the opened KeyValueStore handle. |
| `journalMode` | `int` | `KEYVALUESTORE_JOURNAL_DELETE` or `KEYVALUESTORE_JOURNAL_WAL`. |

**Returns:** `KEYVALUESTORE_OK` on success, error code otherwise.

**Example:**
```c
KeyValueStore *kv = NULL;
int rc = keyvaluestore_open("mydata.db", &kv, KEYVALUESTORE_JOURNAL_WAL);
if (rc != KEYVALUESTORE_OK) {
    fprintf(stderr, "Failed to open database\n");
}
```

---

### keyvaluestore_close

Close a database and free all associated resources. Any uncommitted transaction is rolled back.

```c
int keyvaluestore_close(KeyValueStore *pKV);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `pKV` | `KeyValueStore *` | Handle to close. |

**Returns:** `KEYVALUESTORE_OK` on success.

---

## Key-Value Operations (Default Column Family)

### keyvaluestore_put

Insert or update a key-value pair. If the key already exists, its value is replaced.

```c
int keyvaluestore_put(
  KeyValueStore *pKV,
  const void *pKey,   int nKey,
  const void *pValue, int nValue
);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `pKV` | `KeyValueStore *` | Database handle. |
| `pKey` | `const void *` | Pointer to key data (binary-safe). |
| `nKey` | `int` | Key length in bytes. |
| `pValue` | `const void *` | Pointer to value data (binary-safe). |
| `nValue` | `int` | Value length in bytes. |

**Returns:** `KEYVALUESTORE_OK` on success.

**Note:** If no explicit transaction is active, each `keyvaluestore_put` runs in its own auto-committed transaction.

---

### keyvaluestore_get

Retrieve a value by its key.

```c
int keyvaluestore_get(
  KeyValueStore *pKV,
  const void *pKey,  int nKey,
  void **ppValue,    int *pnValue
);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `pKV` | `KeyValueStore *` | Database handle. |
| `pKey` | `const void *` | Pointer to key data. |
| `nKey` | `int` | Key length in bytes. |
| `ppValue` | `void **` | Output: pointer to allocated value data. **Caller must free with `sqliteFree()`.** |
| `pnValue` | `int *` | Output: value length in bytes. |

**Returns:**
- `KEYVALUESTORE_OK` — key found, value written to `*ppValue`.
- `KEYVALUESTORE_NOTFOUND` — key does not exist.

**Example:**
```c
void *value = NULL;
int vlen = 0;
int rc = keyvaluestore_get(kv, "user:1", 6, &value, &vlen);
if (rc == KEYVALUESTORE_OK) {
    printf("Value: %.*s\n", vlen, (char *)value);
    sqliteFree(value);
}
```

---

### keyvaluestore_delete

Delete a key-value pair.

```c
int keyvaluestore_delete(
  KeyValueStore *pKV,
  const void *pKey, int nKey
);
```

**Returns:**
- `KEYVALUESTORE_OK` — key deleted.
- `KEYVALUESTORE_NOTFOUND` — key did not exist.

---

### keyvaluestore_exists

Check if a key exists without reading its value. More efficient than `keyvaluestore_get` for existence checks.

```c
int keyvaluestore_exists(
  KeyValueStore *pKV,
  const void *pKey, int nKey,
  int *pExists
);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `pExists` | `int *` | Output: `1` if key exists, `0` if not. |

**Returns:** `KEYVALUESTORE_OK` on success.

---

## Column Family Management

Column families provide multiple logical key-value namespaces within a single database file. Each column family is stored in its own B-tree.

### keyvaluestore_cf_create

Create a new column family.

```c
int keyvaluestore_cf_create(
  KeyValueStore *pKV,
  const char *zName,
  KeyValueColumnFamily **ppCF
);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `zName` | `const char *` | Column family name (max 255 characters, must be unique). |
| `ppCF` | `KeyValueColumnFamily **` | Output: handle to the new column family. |

**Returns:** `KEYVALUESTORE_OK` on success.

---

### keyvaluestore_cf_open

Open an existing column family.

```c
int keyvaluestore_cf_open(
  KeyValueStore *pKV,
  const char *zName,
  KeyValueColumnFamily **ppCF
);
```

**Returns:**
- `KEYVALUESTORE_OK` — column family opened.
- `KEYVALUESTORE_NOTFOUND` — column family does not exist.

---

### keyvaluestore_cf_get_default

Get the default column family handle. The default column family always exists and is created automatically when the database is opened.

```c
int keyvaluestore_cf_get_default(
  KeyValueStore *pKV,
  KeyValueColumnFamily **ppCF
);
```

---

### keyvaluestore_cf_drop

Delete a column family and all its data.

```c
int keyvaluestore_cf_drop(
  KeyValueStore *pKV,
  const char *zName
);
```

**Note:** The default column family cannot be dropped.

---

### keyvaluestore_cf_list

List all column families in the database.

```c
int keyvaluestore_cf_list(
  KeyValueStore *pKV,
  char ***pazNames,
  int *pnCount
);
```

**Memory:** Caller must free each name with `sqliteFree()`, then free the array itself with `sqliteFree()`.

**Example:**
```c
char **names = NULL;
int count = 0;
keyvaluestore_cf_list(kv, &names, &count);
for (int i = 0; i < count; i++) {
    printf("CF: %s\n", names[i]);
    sqliteFree(names[i]);
}
sqliteFree(names);
```

---

### keyvaluestore_cf_close

Close a column family handle. Does not delete the column family or its data.

```c
void keyvaluestore_cf_close(KeyValueColumnFamily *pCF);
```

---

## Key-Value Operations (Specific Column Family)

These functions are identical to the default column family operations but take a `KeyValueColumnFamily *` handle instead of `KeyValueStore *`.

### keyvaluestore_cf_put

```c
int keyvaluestore_cf_put(KeyValueColumnFamily *pCF, const void *pKey, int nKey, const void *pValue, int nValue);
```

### keyvaluestore_cf_get

```c
int keyvaluestore_cf_get(KeyValueColumnFamily *pCF, const void *pKey, int nKey, void **ppValue, int *pnValue);
```

**Note:** Caller must free `*ppValue` with `sqliteFree()`.

### keyvaluestore_cf_delete

```c
int keyvaluestore_cf_delete(KeyValueColumnFamily *pCF, const void *pKey, int nKey);
```

### keyvaluestore_cf_exists

```c
int keyvaluestore_cf_exists(KeyValueColumnFamily *pCF, const void *pKey, int nKey, int *pExists);
```

---

## Iterators

Iterators traverse all key-value pairs in sorted key order. Keys are sorted lexicographically by their raw bytes.

### keyvaluestore_iterator_create

Create an iterator for the default column family.

```c
int keyvaluestore_iterator_create(KeyValueStore *pKV, KeyValueIterator **ppIter);
```

### keyvaluestore_cf_iterator_create

Create an iterator for a specific column family.

```c
int keyvaluestore_cf_iterator_create(KeyValueColumnFamily *pCF, KeyValueIterator **ppIter);
```

### keyvaluestore_iterator_first

Position the iterator at the first key-value pair.

```c
int keyvaluestore_iterator_first(KeyValueIterator *pIter);
```

### keyvaluestore_iterator_next

Advance the iterator to the next key-value pair.

```c
int keyvaluestore_iterator_next(KeyValueIterator *pIter);
```

### keyvaluestore_iterator_eof

Check if the iterator has reached the end.

```c
int keyvaluestore_iterator_eof(KeyValueIterator *pIter);
```

**Returns:** `1` if at end (no more entries), `0` otherwise.

### keyvaluestore_iterator_key

Get the key at the current iterator position.

```c
int keyvaluestore_iterator_key(KeyValueIterator *pIter, void **ppKey, int *pnKey);
```

**Note:** The returned pointer is owned by the iterator. **Do not free it.** The pointer is valid until the next `keyvaluestore_iterator_next()` or `keyvaluestore_iterator_close()` call.

### keyvaluestore_iterator_value

Get the value at the current iterator position.

```c
int keyvaluestore_iterator_value(KeyValueIterator *pIter, void **ppValue, int *pnValue);
```

**Note:** The returned pointer is owned by the iterator. **Do not free it.**

### keyvaluestore_iterator_close

Close and free an iterator.

```c
void keyvaluestore_iterator_close(KeyValueIterator *pIter);
```

### Full Iteration Example

```c
KeyValueIterator *it = NULL;
keyvaluestore_iterator_create(kv, &it);
keyvaluestore_iterator_first(it);

while (!keyvaluestore_iterator_eof(it)) {
    void *key, *value;
    int klen, vlen;

    keyvaluestore_iterator_key(it, &key, &klen);
    keyvaluestore_iterator_value(it, &value, &vlen);

    printf("Key: %.*s  Value: %.*s\n", klen, (char *)key, vlen, (char *)value);

    keyvaluestore_iterator_next(it);
}

keyvaluestore_iterator_close(it);
```

---

## Prefix Iterators

Prefix iterators efficiently scan all keys that start with a given byte prefix. They are pre-positioned at the first matching key and automatically stop when keys no longer match the prefix.

### keyvaluestore_prefix_iterator_create

```c
int keyvaluestore_prefix_iterator_create(
  KeyValueStore *pKV,
  const void *pPrefix, int nPrefix,
  KeyValueIterator **ppIter
);
```

### keyvaluestore_cf_prefix_iterator_create

```c
int keyvaluestore_cf_prefix_iterator_create(
  KeyValueColumnFamily *pCF,
  const void *pPrefix, int nPrefix,
  KeyValueIterator **ppIter
);
```

**Important:** The iterator is already positioned at the first matching key. **Do not call `keyvaluestore_iterator_first()`** — read the key/value directly, then call `keyvaluestore_iterator_next()`.

### Prefix Iterator Example

```c
KeyValueIterator *it = NULL;
keyvaluestore_prefix_iterator_create(kv, "user:", 5, &it);

while (!keyvaluestore_iterator_eof(it)) {
    void *key, *value;
    int klen, vlen;

    keyvaluestore_iterator_key(it, &key, &klen);
    keyvaluestore_iterator_value(it, &value, &vlen);

    printf("Key: %.*s\n", klen, (char *)key);

    keyvaluestore_iterator_next(it);
}

keyvaluestore_iterator_close(it);
```

---

## Transactions

SNKV supports explicit transactions for batching multiple operations atomically.

### keyvaluestore_begin

Begin a transaction.

```c
int keyvaluestore_begin(KeyValueStore *pKV, int wrflag);
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `wrflag` | `int` | `1` for write transaction, `0` for read-only. |

**Note:** Only one write transaction can be active at a time. In WAL mode, readers do not block writers and writers do not block readers.

### keyvaluestore_commit

Commit the current transaction, making all changes durable.

```c
int keyvaluestore_commit(KeyValueStore *pKV);
```

**Durability:** In WAL mode with the default `syncLevel = KEYVALUESTORE_SYNC_NORMAL`, `keyvaluestore_commit()` does not fsync on every commit but guarantees survival of process crash. Use `KEYVALUESTORE_SYNC_FULL` for power-loss safety.

### keyvaluestore_rollback

Rollback the current transaction, discarding all uncommitted changes.

```c
int keyvaluestore_rollback(KeyValueStore *pKV);
```

### Transaction Example

```c
keyvaluestore_begin(kv, 1);  /* begin write transaction */

keyvaluestore_put(kv, "key1", 4, "val1", 4);
keyvaluestore_put(kv, "key2", 4, "val2", 4);
keyvaluestore_put(kv, "key3", 4, "val3", 4);

keyvaluestore_commit(kv);    /* all three writes are atomic */
```

### Auto-commit Behavior

If no explicit transaction is active, each `keyvaluestore_put`, `keyvaluestore_delete`, etc. runs in its own auto-committed transaction. For bulk operations, wrapping multiple operations in an explicit transaction is significantly faster.

### Handling KEYVALUESTORE_BUSY

When multiple connections access the same database, write operations may return `KEYVALUESTORE_BUSY`. The simplest fix is to set `busyTimeout` in `KeyValueStoreConfig` — SNKV will automatically retry for up to that many milliseconds before returning `KEYVALUESTORE_BUSY`:

```c
KeyValueStoreConfig cfg = {0};
cfg.busyTimeout = 5000;   /* retry for up to 5 seconds */
keyvaluestore_open_v2("shared.db", &kv, &cfg);
```

For manual retry without `busyTimeout`:

```c
int rc;
int retries = 0;
do {
    rc = keyvaluestore_begin(kv, 1);
    if (rc == KEYVALUESTORE_BUSY || rc == KEYVALUESTORE_LOCKED) {
        usleep(1000 + (rand() % 5000));
        retries++;
    }
} while ((rc == KEYVALUESTORE_BUSY || rc == KEYVALUESTORE_LOCKED) && retries < 100);
```

---

## Diagnostics

### keyvaluestore_errmsg

Get the last error message.

```c
const char *keyvaluestore_errmsg(KeyValueStore *pKV);
```

**Returns:** Error message string. **Do not free.** The string is valid until the next operation on the same handle.

### keyvaluestore_stats

Get operation statistics.

```c
int keyvaluestore_stats(KeyValueStore *pKV, KeyValueStoreStats *pStats);
```

**KeyValueStoreStats structure:**

| Field | Type | Description |
|-------|------|-------------|
| `nPuts` | `u64` | Total put operations |
| `nGets` | `u64` | Total get operations |
| `nDeletes` | `u64` | Total delete operations |
| `nIterations` | `u64` | Total iterators created |
| `nErrors` | `u64` | Total errors encountered |

### keyvaluestore_integrity_check

Verify database integrity by walking the entire B-tree structure.

```c
int keyvaluestore_integrity_check(KeyValueStore *pKV, char **pzErrMsg);
```

**Returns:**
- `KEYVALUESTORE_OK` — database is consistent.
- `KEYVALUESTORE_CORRUPT` — corruption detected; `*pzErrMsg` contains details.

**Note:** Caller must free `*pzErrMsg` with `sqliteFree()`.

### keyvaluestore_sync

Force all pending changes to be written to disk.

```c
int keyvaluestore_sync(KeyValueStore *pKV);
```

If a write transaction is active, this performs a commit-and-reopen cycle to flush the WAL to disk. If no write transaction is active, this is a no-op.

### keyvaluestore_incremental_vacuum

Reclaim unused pages and shrink the database file. All databases are opened with incremental auto-vacuum enabled, so this function can be called at any time after deleting data to reduce file size.

```c
int keyvaluestore_incremental_vacuum(KeyValueStore *pKV, int nPage);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `pKV` | `KeyValueStore *` | Database handle. |
| `nPage` | `int` | Maximum number of pages to free. Pass `0` to free all unused pages. |

**Returns:** `KEYVALUESTORE_OK` on success, error code otherwise.

**Notes:**
- When data is deleted, the freed pages are added to an internal freelist but the database file does not shrink. Call `keyvaluestore_incremental_vacuum()` to actually truncate the file and reclaim disk space.
- Pass a positive `nPage` to limit the amount of work per call, which is useful for avoiding long pauses in latency-sensitive applications.
- If no explicit transaction is active, the function automatically begins and commits a write transaction for the vacuum operation.

**Example:**
```c
/* Delete a batch of records */
keyvaluestore_begin(kv, 1);
for (int i = 0; i < 1000; i++) {
    keyvaluestore_delete(kv, keys[i], key_lens[i]);
}
keyvaluestore_commit(kv);

/* Reclaim all unused space */
keyvaluestore_incremental_vacuum(kv, 0);

/* Or reclaim incrementally (e.g., 50 pages at a time) */
keyvaluestore_incremental_vacuum(kv, 50);
```

### keyvaluestore_checkpoint

Run a WAL checkpoint, copying WAL frames back into the main database file.

```c
int keyvaluestore_checkpoint(KeyValueStore *pKV, int mode, int *pnLog, int *pnCkpt);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `pKV` | `KeyValueStore *` | Database handle. |
| `mode` | `int` | Checkpoint mode — one of the `KEYVALUESTORE_CHECKPOINT_*` constants. |
| `pnLog` | `int *` | Output: total WAL frames after checkpoint. May be `NULL`. |
| `pnCkpt` | `int *` | Output: frames successfully written to the database file. May be `NULL`. |

**Returns:**

| Code | Meaning |
|------|---------|
| `KEYVALUESTORE_OK` | Checkpoint completed (or no-op on non-WAL database). |
| `KEYVALUESTORE_BUSY` | A write transaction is currently open — commit or rollback first. |
| `KEYVALUESTORE_ERROR` | Other failure (check `keyvaluestore_errmsg()`). |

**Notes:**
- The persistent read transaction is temporarily released around the checkpoint call (required by the btree layer) and is automatically restored afterward.
- On non-WAL (DELETE journal) databases this is a no-op that returns `KEYVALUESTORE_OK` with `*pnLog = *pnCkpt = 0`.
- `PASSIVE` mode is non-blocking: it copies only frames not held back by active readers and returns immediately. It is the safest choice for background use.
- If `*pnLog == *pnCkpt` after a `PASSIVE` checkpoint, all WAL frames have been successfully copied (no frames are blocked by readers).
- Use `TRUNCATE` to also shrink the WAL file to zero bytes, which frees disk space.

**WAL auto-checkpoint via `walSizeLimit`:**

As an alternative to calling `keyvaluestore_checkpoint()` manually, set `cfg.walSizeLimit = N` when opening the store. SNKV then automatically runs a `PASSIVE` checkpoint after every N committed write transactions, keeping WAL growth bounded without any application-level calls.

```c
KeyValueStoreConfig cfg = {0};
cfg.journalMode  = KEYVALUESTORE_JOURNAL_WAL;
cfg.walSizeLimit = 500;   /* PASSIVE checkpoint every 500 committed writes */
keyvaluestore_open_v2("mydb.db", &kv, &cfg);
```

**Manual checkpoint example:**
```c
/* Flush all WAL frames — call after a batch of writes */
int nLog = 0, nCkpt = 0;
int rc = keyvaluestore_checkpoint(kv, KEYVALUESTORE_CHECKPOINT_PASSIVE, &nLog, &nCkpt);
if (rc == KEYVALUESTORE_OK) {
    printf("WAL: %d frames total, %d checkpointed\n", nLog, nCkpt);
}

/* Reclaim WAL disk space (truncate WAL file to zero) */
keyvaluestore_checkpoint(kv, KEYVALUESTORE_CHECKPOINT_TRUNCATE, NULL, NULL);
```

---

## Memory Management

SNKV uses SQLite's memory allocator internally. The following macros are provided for application use:

| Macro | Equivalent | Description |
|-------|-----------|-------------|
| `sqliteMalloc(n)` | `sqlite3MallocZero(n)` | Allocate `n` zero-initialized bytes |
| `sqliteFree(p)` | `sqlite3_free(p)` | Free memory allocated by SNKV |
| `sqliteRealloc(p, n)` | `sqlite3Realloc(p, n)` | Resize allocation |
| `sqliteStrDup(s)` | `sqlite3_mprintf("%s", s)` | Duplicate a string |

When using the **single-header** (`snkv.h`) build, the following aliases are also available and preferred in application code:

| Macro | Description |
|-------|-------------|
| `snkv_malloc(n)` | Alias for `sqliteMalloc(n)` |
| `snkv_free(p)` | Alias for `sqliteFree(p)` |

**Rule:** Any pointer returned by `keyvaluestore_get`, `keyvaluestore_cf_get`, `keyvaluestore_cf_list`, or `keyvaluestore_integrity_check` must be freed with `sqliteFree()` (or `snkv_free()` in single-header builds).

Iterator key/value pointers (`keyvaluestore_iterator_key`, `keyvaluestore_iterator_value`) are **not** caller-owned and must **not** be freed.

---

## Thread Safety

- All `keyvaluestore_*` operations are protected by a mutex — a single `KeyValueStore` handle is safe to use from multiple threads.
- In WAL mode, multiple readers can proceed concurrently with a single writer.
- Only one write transaction can be active at a time. Concurrent write attempts return `SQLITE_BUSY`.
- For maximum concurrency, each thread can open its own `KeyValueStore` connection to the same database file.
- The `KeyValueIterator` handle is **not** thread-safe — do not share an iterator between threads.
