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
- [TTL / Key Expiry](#ttl--key-expiry)
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
| `KVStore` | Opaque handle to an open database |
| `KVColumnFamily` | Opaque handle to a column family |
| `KVIterator` | Opaque handle to a key-value iterator |
| `KVStoreStats` | Statistics counters (see [Diagnostics](#diagnostics)) |
| `KVStoreConfig` | Configuration struct for `kvstore_open_v2` (see [Configuration](#configuration)) |

---

## Error Codes

| Code | Value | Description |
|------|-------|-------------|
| `KVSTORE_OK` | 0 | Operation succeeded |
| `KVSTORE_ERROR` | 1 | Generic error |
| `KVSTORE_BUSY` | 5 | Database locked by another connection (retry or use `busyTimeout`) |
| `KVSTORE_LOCKED` | 6 | Database locked within the same connection |
| `KVSTORE_NOMEM` | 7 | Memory allocation failed |
| `KVSTORE_READONLY` | 8 | Database is read-only |
| `KVSTORE_CORRUPT` | 11 | Database file is corrupted |
| `KVSTORE_NOTFOUND` | 12 | Key or column family not found |
| `KVSTORE_PROTOCOL` | 15 | Database lock protocol error |

All error codes are aliases for the corresponding `SQLITE_*` codes.

---

## Constants

### Journal modes

| Constant | Value | Description |
|----------|-------|-------------|
| `KVSTORE_JOURNAL_DELETE` | 0 | Rollback journal mode (delete journal on commit) |
| `KVSTORE_JOURNAL_WAL` | 1 | Write-Ahead Logging mode (recommended) |

### Sync levels

| Constant | Value | Description |
|----------|-------|-------------|
| `KVSTORE_SYNC_OFF` | 0 | No fsync — fastest, data at risk on power failure |
| `KVSTORE_SYNC_NORMAL` | 1 | WAL-safe (default) — survives process crash, not power loss |
| `KVSTORE_SYNC_FULL` | 2 | Power-safe — fsync on every commit, slower writes |

> In WAL mode (`KVSTORE_JOURNAL_WAL`) there is virtually no write-throughput
> difference between `SYNC_NORMAL` and `SYNC_FULL`.

### Checkpoint modes

Used with `kvstore_checkpoint()`. Map directly to `SQLITE_CHECKPOINT_*` values.

| Constant | Value | Description |
|----------|-------|-------------|
| `KVSTORE_CHECKPOINT_PASSIVE` | 0 | Copy frames without blocking; may not flush all if readers hold back frames |
| `KVSTORE_CHECKPOINT_FULL` | 1 | Wait for active writers to finish, then copy all frames |
| `KVSTORE_CHECKPOINT_RESTART` | 2 | Like FULL, then reset the WAL write position to the start |
| `KVSTORE_CHECKPOINT_TRUNCATE` | 3 | Like RESTART, then truncate the WAL file to zero bytes |

### Other constants

| Constant | Value | Description |
|----------|-------|-------------|
| `KVSTORE_MAX_COLUMN_FAMILIES` | 64 | Maximum number of column families per database |

---

## Configuration

### KVStoreConfig

```c
typedef struct KVStoreConfig KVStoreConfig;
struct KVStoreConfig {
  int journalMode;  /* KVSTORE_JOURNAL_WAL (default) or KVSTORE_JOURNAL_DELETE */
  int syncLevel;    /* KVSTORE_SYNC_NORMAL (default), KVSTORE_SYNC_OFF, or KVSTORE_SYNC_FULL */
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
| `journalMode` | `KVSTORE_JOURNAL_WAL` | WAL is strongly recommended |
| `syncLevel` | `KVSTORE_SYNC_NORMAL` | Safe against process crash |
| `cacheSize` | 2000 pages (~8 MB) | Larger cache improves read-heavy workloads |
| `pageSize` | 4096 bytes | Ignored for existing databases |
| `readOnly` | 0 (read-write) | |
| `busyTimeout` | 0 (fail immediately) | Set > 0 for multi-process workloads |
| `walSizeLimit` | 0 (disabled) | Auto-checkpoint every N commits; 0 = no auto-checkpoint |

---

## Database Lifecycle

### kvstore_open_v2

Open or create a key-value store with full configuration control.

```c
int kvstore_open_v2(
  const char *zFilename,
  KVStore **ppKV,
  const KVStoreConfig *pConfig
);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `zFilename` | `const char *` | Path to database file. `NULL` for in-memory. |
| `ppKV` | `KVStore **` | Output pointer to the opened `KVStore` handle. |
| `pConfig` | `const KVStoreConfig *` | Configuration. `NULL` uses all defaults (same as `kvstore_open` with `KVSTORE_JOURNAL_WAL`). |

**Returns:** `KVSTORE_OK` on success, error code otherwise.

**Examples:**

```c
/* Default config — WAL mode, NORMAL sync, 8 MB cache */
KVStore *kv;
kvstore_open_v2("mydb.db", &kv, NULL);

/* Explicitly configured */
KVStoreConfig cfg = {0};
cfg.journalMode = KVSTORE_JOURNAL_WAL;
cfg.syncLevel   = KVSTORE_SYNC_FULL;     /* power-safe */
cfg.cacheSize   = 4000;                  /* ~16 MB cache */
cfg.busyTimeout = 5000;                  /* retry up to 5 s */
kvstore_open_v2("mydb.db", &kv, &cfg);

/* Read-only open */
KVStoreConfig ro = {0};
ro.readOnly = 1;
kvstore_open_v2("mydb.db", &kv, &ro);
```

---

### kvstore_open

Open or create a key-value store (simplified interface).

Equivalent to calling `kvstore_open_v2` with `pConfig->journalMode = journalMode`
and all other fields at their defaults.

```c
int kvstore_open(
  const char *zFilename,
  KVStore **ppKV,
  int journalMode
);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `zFilename` | `const char *` | Path to database file. Pass `NULL` for in-memory database. |
| `ppKV` | `KVStore **` | Output pointer to the opened KVStore handle. |
| `journalMode` | `int` | `KVSTORE_JOURNAL_DELETE` or `KVSTORE_JOURNAL_WAL`. |

**Returns:** `KVSTORE_OK` on success, error code otherwise.

**Example:**
```c
KVStore *kv = NULL;
int rc = kvstore_open("mydata.db", &kv, KVSTORE_JOURNAL_WAL);
if (rc != KVSTORE_OK) {
    fprintf(stderr, "Failed to open database\n");
}
```

---

### kvstore_close

Close a database and free all associated resources. Any uncommitted transaction is rolled back.

```c
int kvstore_close(KVStore *pKV);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `pKV` | `KVStore *` | Handle to close. |

**Returns:** `KVSTORE_OK` on success.

---

## Key-Value Operations (Default Column Family)

### kvstore_put

Insert or update a key-value pair. If the key already exists, its value is replaced.

```c
int kvstore_put(
  KVStore *pKV,
  const void *pKey,   int nKey,
  const void *pValue, int nValue
);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `pKV` | `KVStore *` | Database handle. |
| `pKey` | `const void *` | Pointer to key data (binary-safe). |
| `nKey` | `int` | Key length in bytes. |
| `pValue` | `const void *` | Pointer to value data (binary-safe). |
| `nValue` | `int` | Value length in bytes. |

**Returns:** `KVSTORE_OK` on success.

**Note:** If no explicit transaction is active, each `kvstore_put` runs in its own auto-committed transaction.

---

### kvstore_get

Retrieve a value by its key.

```c
int kvstore_get(
  KVStore *pKV,
  const void *pKey,  int nKey,
  void **ppValue,    int *pnValue
);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `pKV` | `KVStore *` | Database handle. |
| `pKey` | `const void *` | Pointer to key data. |
| `nKey` | `int` | Key length in bytes. |
| `ppValue` | `void **` | Output: pointer to allocated value data. **Caller must free with `sqliteFree()`.** |
| `pnValue` | `int *` | Output: value length in bytes. |

**Returns:**
- `KVSTORE_OK` — key found, value written to `*ppValue`.
- `KVSTORE_NOTFOUND` — key does not exist.

**Example:**
```c
void *value = NULL;
int vlen = 0;
int rc = kvstore_get(kv, "user:1", 6, &value, &vlen);
if (rc == KVSTORE_OK) {
    printf("Value: %.*s\n", vlen, (char *)value);
    sqliteFree(value);
}
```

---

### kvstore_delete

Delete a key-value pair.

```c
int kvstore_delete(
  KVStore *pKV,
  const void *pKey, int nKey
);
```

**Returns:**
- `KVSTORE_OK` — key deleted.
- `KVSTORE_NOTFOUND` — key did not exist.

---

### kvstore_exists

Check if a key exists without reading its value. More efficient than `kvstore_get` for existence checks.

```c
int kvstore_exists(
  KVStore *pKV,
  const void *pKey, int nKey,
  int *pExists
);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `pExists` | `int *` | Output: `1` if key exists, `0` if not. |

**Returns:** `KVSTORE_OK` on success.

---

## Column Family Management

Column families provide multiple logical key-value namespaces within a single database file. Each column family is stored in its own B-tree.

### kvstore_cf_create

Create a new column family.

```c
int kvstore_cf_create(
  KVStore *pKV,
  const char *zName,
  KVColumnFamily **ppCF
);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `zName` | `const char *` | Column family name (max 255 characters, must be unique). |
| `ppCF` | `KVColumnFamily **` | Output: handle to the new column family. |

**Returns:** `KVSTORE_OK` on success.

---

### kvstore_cf_open

Open an existing column family.

```c
int kvstore_cf_open(
  KVStore *pKV,
  const char *zName,
  KVColumnFamily **ppCF
);
```

**Returns:**
- `KVSTORE_OK` — column family opened.
- `KVSTORE_NOTFOUND` — column family does not exist.

---

### kvstore_cf_get_default

Get the default column family handle. The default column family always exists and is created automatically when the database is opened.

```c
int kvstore_cf_get_default(
  KVStore *pKV,
  KVColumnFamily **ppCF
);
```

---

### kvstore_cf_drop

Delete a column family and all its data.

```c
int kvstore_cf_drop(
  KVStore *pKV,
  const char *zName
);
```

**Note:** The default column family cannot be dropped.

---

### kvstore_cf_list

List all column families in the database.

```c
int kvstore_cf_list(
  KVStore *pKV,
  char ***pazNames,
  int *pnCount
);
```

**Memory:** Caller must free each name with `sqliteFree()`, then free the array itself with `sqliteFree()`.

**Example:**
```c
char **names = NULL;
int count = 0;
kvstore_cf_list(kv, &names, &count);
for (int i = 0; i < count; i++) {
    printf("CF: %s\n", names[i]);
    sqliteFree(names[i]);
}
sqliteFree(names);
```

---

### kvstore_cf_close

Close a column family handle. Does not delete the column family or its data.

```c
void kvstore_cf_close(KVColumnFamily *pCF);
```

---

## Key-Value Operations (Specific Column Family)

These functions are identical to the default column family operations but take a `KVColumnFamily *` handle instead of `KVStore *`.

### kvstore_cf_put

```c
int kvstore_cf_put(KVColumnFamily *pCF, const void *pKey, int nKey, const void *pValue, int nValue);
```

### kvstore_cf_get

```c
int kvstore_cf_get(KVColumnFamily *pCF, const void *pKey, int nKey, void **ppValue, int *pnValue);
```

**Note:** Caller must free `*ppValue` with `sqliteFree()`.

### kvstore_cf_delete

```c
int kvstore_cf_delete(KVColumnFamily *pCF, const void *pKey, int nKey);
```

### kvstore_cf_exists

```c
int kvstore_cf_exists(KVColumnFamily *pCF, const void *pKey, int nKey, int *pExists);
```

---

## Iterators

Iterators traverse all key-value pairs in sorted key order. Keys are sorted lexicographically by their raw bytes.

### kvstore_iterator_create

Create an iterator for the default column family.

```c
int kvstore_iterator_create(KVStore *pKV, KVIterator **ppIter);
```

### kvstore_cf_iterator_create

Create an iterator for a specific column family.

```c
int kvstore_cf_iterator_create(KVColumnFamily *pCF, KVIterator **ppIter);
```

### kvstore_iterator_first

Position the iterator at the first key-value pair.

```c
int kvstore_iterator_first(KVIterator *pIter);
```

### kvstore_iterator_next

Advance the iterator to the next key-value pair.

```c
int kvstore_iterator_next(KVIterator *pIter);
```

### kvstore_iterator_eof

Check if the iterator has reached the end.

```c
int kvstore_iterator_eof(KVIterator *pIter);
```

**Returns:** `1` if at end (no more entries), `0` otherwise.

### kvstore_iterator_key

Get the key at the current iterator position.

```c
int kvstore_iterator_key(KVIterator *pIter, void **ppKey, int *pnKey);
```

**Note:** The returned pointer is owned by the iterator. **Do not free it.** The pointer is valid until the next `kvstore_iterator_next()` or `kvstore_iterator_close()` call.

### kvstore_iterator_value

Get the value at the current iterator position.

```c
int kvstore_iterator_value(KVIterator *pIter, void **ppValue, int *pnValue);
```

**Note:** The returned pointer is owned by the iterator. **Do not free it.**

### kvstore_iterator_close

Close and free an iterator.

```c
void kvstore_iterator_close(KVIterator *pIter);
```

### Full Iteration Example

```c
KVIterator *it = NULL;
kvstore_iterator_create(kv, &it);
kvstore_iterator_first(it);

while (!kvstore_iterator_eof(it)) {
    void *key, *value;
    int klen, vlen;

    kvstore_iterator_key(it, &key, &klen);
    kvstore_iterator_value(it, &value, &vlen);

    printf("Key: %.*s  Value: %.*s\n", klen, (char *)key, vlen, (char *)value);

    kvstore_iterator_next(it);
}

kvstore_iterator_close(it);
```

---

## Prefix Iterators

Prefix iterators efficiently scan all keys that start with a given byte prefix. They are pre-positioned at the first matching key and automatically stop when keys no longer match the prefix.

### kvstore_prefix_iterator_create

```c
int kvstore_prefix_iterator_create(
  KVStore *pKV,
  const void *pPrefix, int nPrefix,
  KVIterator **ppIter
);
```

### kvstore_cf_prefix_iterator_create

```c
int kvstore_cf_prefix_iterator_create(
  KVColumnFamily *pCF,
  const void *pPrefix, int nPrefix,
  KVIterator **ppIter
);
```

**Important:** The iterator is already positioned at the first matching key. **Do not call `kvstore_iterator_first()`** — read the key/value directly, then call `kvstore_iterator_next()`.

### Prefix Iterator Example

```c
KVIterator *it = NULL;
kvstore_prefix_iterator_create(kv, "user:", 5, &it);

while (!kvstore_iterator_eof(it)) {
    void *key, *value;
    int klen, vlen;

    kvstore_iterator_key(it, &key, &klen);
    kvstore_iterator_value(it, &value, &vlen);

    printf("Key: %.*s\n", klen, (char *)key);

    kvstore_iterator_next(it);
}

kvstore_iterator_close(it);
```

---

## Transactions

SNKV supports explicit transactions for batching multiple operations atomically.

### kvstore_begin

Begin a transaction.

```c
int kvstore_begin(KVStore *pKV, int wrflag);
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `wrflag` | `int` | `1` for write transaction, `0` for read-only. |

**Note:** Only one write transaction can be active at a time. In WAL mode, readers do not block writers and writers do not block readers.

### kvstore_commit

Commit the current transaction, making all changes durable.

```c
int kvstore_commit(KVStore *pKV);
```

**Durability:** In WAL mode with the default `syncLevel = KVSTORE_SYNC_NORMAL`, `kvstore_commit()` does not fsync on every commit but guarantees survival of process crash. Use `KVSTORE_SYNC_FULL` for power-loss safety.

### kvstore_rollback

Rollback the current transaction, discarding all uncommitted changes.

```c
int kvstore_rollback(KVStore *pKV);
```

### Transaction Example

```c
kvstore_begin(kv, 1);  /* begin write transaction */

kvstore_put(kv, "key1", 4, "val1", 4);
kvstore_put(kv, "key2", 4, "val2", 4);
kvstore_put(kv, "key3", 4, "val3", 4);

kvstore_commit(kv);    /* all three writes are atomic */
```

### Auto-commit Behavior

If no explicit transaction is active, each `kvstore_put`, `kvstore_delete`, etc. runs in its own auto-committed transaction. For bulk operations, wrapping multiple operations in an explicit transaction is significantly faster.

### Handling KVSTORE_BUSY

When multiple connections access the same database, write operations may return `KVSTORE_BUSY`. The simplest fix is to set `busyTimeout` in `KVStoreConfig` — SNKV will automatically retry for up to that many milliseconds before returning `KVSTORE_BUSY`:

```c
KVStoreConfig cfg = {0};
cfg.busyTimeout = 5000;   /* retry for up to 5 seconds */
kvstore_open_v2("shared.db", &kv, &cfg);
```

For manual retry without `busyTimeout`:

```c
int rc;
int retries = 0;
do {
    rc = kvstore_begin(kv, 1);
    if (rc == KVSTORE_BUSY || rc == KVSTORE_LOCKED) {
        usleep(1000 + (rand() % 5000));
        retries++;
    }
} while ((rc == KVSTORE_BUSY || rc == KVSTORE_LOCKED) && retries < 100);
```

---

## Diagnostics

### kvstore_errmsg

Get the last error message.

```c
const char *kvstore_errmsg(KVStore *pKV);
```

**Returns:** Error message string. **Do not free.** The string is valid until the next operation on the same handle.

### kvstore_stats

Get operation statistics.

```c
int kvstore_stats(KVStore *pKV, KVStoreStats *pStats);
```

**KVStoreStats structure:**

| Field | Type | Description |
|-------|------|-------------|
| `nPuts` | `u64` | Total put operations |
| `nGets` | `u64` | Total get operations |
| `nDeletes` | `u64` | Total delete operations |
| `nIterations` | `u64` | Total iterators created |
| `nErrors` | `u64` | Total errors encountered |

### kvstore_integrity_check

Verify database integrity by walking the entire B-tree structure.

```c
int kvstore_integrity_check(KVStore *pKV, char **pzErrMsg);
```

**Returns:**
- `KVSTORE_OK` — database is consistent.
- `KVSTORE_CORRUPT` — corruption detected; `*pzErrMsg` contains details.

**Note:** Caller must free `*pzErrMsg` with `sqliteFree()`.

### kvstore_sync

Force all pending changes to be written to disk.

```c
int kvstore_sync(KVStore *pKV);
```

If a write transaction is active, this performs a commit-and-reopen cycle to flush the WAL to disk. If no write transaction is active, this is a no-op.

### kvstore_incremental_vacuum

Reclaim unused pages and shrink the database file. All databases are opened with incremental auto-vacuum enabled, so this function can be called at any time after deleting data to reduce file size.

```c
int kvstore_incremental_vacuum(KVStore *pKV, int nPage);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `pKV` | `KVStore *` | Database handle. |
| `nPage` | `int` | Maximum number of pages to free. Pass `0` to free all unused pages. |

**Returns:** `KVSTORE_OK` on success, error code otherwise.

**Notes:**
- When data is deleted, the freed pages are added to an internal freelist but the database file does not shrink. Call `kvstore_incremental_vacuum()` to actually truncate the file and reclaim disk space.
- Pass a positive `nPage` to limit the amount of work per call, which is useful for avoiding long pauses in latency-sensitive applications.
- If no explicit transaction is active, the function automatically begins and commits a write transaction for the vacuum operation.

**Example:**
```c
/* Delete a batch of records */
kvstore_begin(kv, 1);
for (int i = 0; i < 1000; i++) {
    kvstore_delete(kv, keys[i], key_lens[i]);
}
kvstore_commit(kv);

/* Reclaim all unused space */
kvstore_incremental_vacuum(kv, 0);

/* Or reclaim incrementally (e.g., 50 pages at a time) */
kvstore_incremental_vacuum(kv, 50);
```

### kvstore_checkpoint

Run a WAL checkpoint, copying WAL frames back into the main database file.

```c
int kvstore_checkpoint(KVStore *pKV, int mode, int *pnLog, int *pnCkpt);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `pKV` | `KVStore *` | Database handle. |
| `mode` | `int` | Checkpoint mode — one of the `KVSTORE_CHECKPOINT_*` constants. |
| `pnLog` | `int *` | Output: total WAL frames after checkpoint. May be `NULL`. |
| `pnCkpt` | `int *` | Output: frames successfully written to the database file. May be `NULL`. |

**Returns:**

| Code | Meaning |
|------|---------|
| `KVSTORE_OK` | Checkpoint completed (or no-op on non-WAL database). |
| `KVSTORE_BUSY` | A write transaction is currently open — commit or rollback first. |
| `KVSTORE_ERROR` | Other failure (check `kvstore_errmsg()`). |

**Notes:**
- The persistent read transaction is temporarily released around the checkpoint call (required by the btree layer) and is automatically restored afterward.
- On non-WAL (DELETE journal) databases this is a no-op that returns `KVSTORE_OK` with `*pnLog = *pnCkpt = 0`.
- `PASSIVE` mode is non-blocking: it copies only frames not held back by active readers and returns immediately. It is the safest choice for background use.
- If `*pnLog == *pnCkpt` after a `PASSIVE` checkpoint, all WAL frames have been successfully copied (no frames are blocked by readers).
- Use `TRUNCATE` to also shrink the WAL file to zero bytes, which frees disk space.

**WAL auto-checkpoint via `walSizeLimit`:**

As an alternative to calling `kvstore_checkpoint()` manually, set `cfg.walSizeLimit = N` when opening the store. SNKV then automatically runs a `PASSIVE` checkpoint after every N committed write transactions, keeping WAL growth bounded without any application-level calls.

```c
KVStoreConfig cfg = {0};
cfg.journalMode  = KVSTORE_JOURNAL_WAL;
cfg.walSizeLimit = 500;   /* PASSIVE checkpoint every 500 committed writes */
kvstore_open_v2("mydb.db", &kv, &cfg);
```

**Manual checkpoint example:**
```c
/* Flush all WAL frames — call after a batch of writes */
int nLog = 0, nCkpt = 0;
int rc = kvstore_checkpoint(kv, KVSTORE_CHECKPOINT_PASSIVE, &nLog, &nCkpt);
if (rc == KVSTORE_OK) {
    printf("WAL: %d frames total, %d checkpointed\n", nLog, nCkpt);
}

/* Reclaim WAL disk space (truncate WAL file to zero) */
kvstore_checkpoint(kv, KVSTORE_CHECKPOINT_TRUNCATE, NULL, NULL);
```

---

## TTL / Key Expiry

SNKV supports per-key Time-To-Live (TTL) with a **per-CF dual-index** design.
Every user column family `<CF>` can have two hidden sibling CFs created lazily
on the first `kvstore_cf_put_ttl` call:

| Hidden CF | Key | Value | Purpose |
|-----------|-----|-------|---------|
| `__snkv_ttl_k__<CF>` | `user_key` | 8-byte BE `expire_ms` | O(1) point lookup |
| `__snkv_ttl_e__<CF>` | `[8-byte BE expire_ms][user_key]` | empty | O(expired) range scan for purge |

CFs that never use TTL pay **zero overhead** — the hidden index CFs are never
created. Names beginning with `__` are reserved and invisible in `kvstore_cf_list`.

### TTL Sentinel

```c
#define KVSTORE_NO_TTL (-1)   /* key exists but has no expiry */
```

### Time Helper

```c
int64_t kvstore_now_ms(void);
```

Returns the current time in milliseconds since the Unix epoch.  Use this to
compute absolute expiry values:

```c
int64_t expire_ms = kvstore_now_ms() + 30000;  // 30 seconds from now
```

### Default Column Family TTL

```c
int kvstore_put_ttl(
    KVStore *pKV,
    const void *pKey, int nKey,
    const void *pValue, int nValue,
    int64_t expire_ms
);
```

Insert or update a key with an expiry time.

| `expire_ms` | Behaviour |
|-------------|-----------|
| `> 0` | Absolute expiry in ms since Unix epoch. |
| `== 0` | Write key without TTL (removes any existing TTL entry). |

Both the data write and the TTL index writes are committed atomically.

---

```c
int kvstore_get_ttl(
    KVStore *pKV,
    const void *pKey, int nKey,
    void **ppValue, int *pnValue,
    int64_t *pnRemaining       /* may be NULL */
);
```

Retrieve a value with lazy TTL expiry check.

| Outcome | Return | `*ppValue` | `*pnRemaining` |
|---------|--------|------------|----------------|
| Key valid | `KVSTORE_OK` | populated (caller frees) | remaining ms, or `KVSTORE_NO_TTL` |
| Key expired | `KVSTORE_NOTFOUND` | `NULL` | `0` |
| Key absent | `KVSTORE_NOTFOUND` | `NULL` | `KVSTORE_NO_TTL` |

On expiry the key and both TTL index entries are lazily deleted in a write
transaction before returning.

---

```c
int kvstore_ttl_remaining(
    KVStore *pKV,
    const void *pKey, int nKey,
    int64_t *pnRemaining        /* must not be NULL */
);
```

Return remaining TTL without fetching the value.

| Return code | `*pnRemaining` | Meaning |
|-------------|----------------|---------|
| `KVSTORE_OK` | `KVSTORE_NO_TTL` | Key exists, no expiry |
| `KVSTORE_OK` | `0` | Key just expired (lazy delete performed) |
| `KVSTORE_OK` | `N > 0` | N ms remain |
| `KVSTORE_NOTFOUND` | — | Key does not exist |

---

```c
int kvstore_purge_expired(KVStore *pKV, int *pnDeleted);  /* pnDeleted may be NULL */
```

Scan the expiry index CF and delete all expired entries in a single write
transaction.  Uses the 8-byte big-endian `expire_ms` prefix sort order to
stop at the first non-expired entry — **O(expired keys)**, not O(all TTL keys).

`*pnDeleted` is set to the number of data keys successfully deleted.

### Column Family–Level TTL

All four functions above have CF-level equivalents that operate on a
`KVColumnFamily` handle instead of a `KVStore` handle.  Each user CF maintains
**independent** TTL indexes; purging CF A never touches CF B.

```c
int kvstore_cf_put_ttl(
    KVColumnFamily *pCF,
    const void *pKey, int nKey,
    const void *pValue, int nValue,
    int64_t expire_ms
);

int kvstore_cf_get_ttl(
    KVColumnFamily *pCF,
    const void *pKey, int nKey,
    void **ppValue, int *pnValue,
    int64_t *pnRemaining       /* may be NULL */
);

int kvstore_cf_ttl_remaining(
    KVColumnFamily *pCF,
    const void *pKey, int nKey,
    int64_t *pnRemaining       /* must not be NULL */
);

int kvstore_cf_purge_expired(KVColumnFamily *pCF, int *pnDeleted);
```

Semantics are identical to the default-CF variants above.

### TTL Lifecycle Notes

- **Dropping a CF** (`kvstore_cf_drop`) automatically removes its two hidden
  TTL index CFs.
- **Re-opening a store** (`kvstore_open` / `kvstore_cf_open`) probes for
  existing TTL index CFs and restores the `hasTtl` flag transparently.
- **Overwriting with `kvstore_put`** removes any existing TTL entry for that
  key atomically.
- **Rolling back** a transaction containing `kvstore_put_ttl` discards both the
  data write and the TTL index writes.

### Example

```c
/* Write a session token that expires in 30 minutes. */
int64_t expire_ms = kvstore_now_ms() + 30 * 60 * 1000;
kvstore_put_ttl(pKV, "sess:abc", 8, token, token_len, expire_ms);

/* Read it back — lazy expiry check included. */
void *val = NULL; int nVal = 0; int64_t rem = 0;
int rc = kvstore_get_ttl(pKV, "sess:abc", 8, &val, &nVal, &rem);
if (rc == KVSTORE_OK) {
    /* use val, rem is remaining ms */
    snkv_free(val);
} else if (rc == KVSTORE_NOTFOUND) {
    /* session expired or never existed */
}

/* Nightly maintenance: purge expired sessions. */
int n = 0;
kvstore_purge_expired(pKV, &n);
printf("Purged %d expired sessions\n", n);
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

**Rule:** Any pointer returned by `kvstore_get`, `kvstore_cf_get`, `kvstore_cf_list`, or `kvstore_integrity_check` must be freed with `sqliteFree()` (or `snkv_free()` in single-header builds).

Iterator key/value pointers (`kvstore_iterator_key`, `kvstore_iterator_value`) are **not** caller-owned and must **not** be freed.

---

## Thread Safety

- All `kvstore_*` operations are protected by a mutex — a single `KVStore` handle is safe to use from multiple threads.
- In WAL mode, multiple readers can proceed concurrently with a single writer.
- Only one write transaction can be active at a time. Concurrent write attempts return `SQLITE_BUSY`.
- For maximum concurrency, each thread can open its own `KVStore` connection to the same database file.
- The `KVIterator` handle is **not** thread-safe — do not share an iterator between threads.
