# SNKV Python API Reference

Python bindings for [SNKV](https://github.com/hash-anu/snkv) — a lightweight, ACID-compliant
embedded key-value store built on SQLite's B-Tree engine.

---

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Constants](#constants)
  - [Journal Mode](#journal-mode)
  - [Sync Level](#sync-level)
  - [Checkpoint Mode](#checkpoint-mode)
- [Exceptions](#exceptions)
- [KVStore](#kvstore)
  - [Opening a Store](#opening-a-store)
  - [Core Operations](#core-operations)
  - [TTL / Key Expiry](#ttl--key-expiry)
  - [Dict-like Interface](#dict-like-interface)
  - [Transactions](#transactions)
  - [Column Families](#column-families)
  - [Iterators](#iterators)
  - [Maintenance](#maintenance)
  - [Lifecycle](#lifecycle)
- [ColumnFamily](#columnfamily)
  - [Core Operations](#core-operations-1)
  - [TTL / Key Expiry](#ttl--key-expiry-1)
  - [Dict-like Interface](#dict-like-interface-1)
  - [Iterators](#iterators-1)
  - [Lifecycle](#lifecycle-1)
- [Iterator](#iterator)
  - [Python Iterator Protocol](#python-iterator-protocol)
  - [Manual Control](#manual-control)
  - [Lifecycle](#lifecycle-2)
- [Type Notes](#type-notes)

---

## Installation

```bash
# Linux / macOS
cd python
python3 setup.py build_ext --inplace

# Windows (MSYS2 MinGW64 shell)
cd python
python3 setup.py build_ext --inplace
```

See the [README](../../README.md) for full platform-specific prerequisites.

---

## Quick Start

```python
from snkv import KVStore

# Open (or create) a database file
with KVStore("mydb.db") as db:
    db["hello"] = "world"
    print(db["hello"].decode())    # world
    print(db.get("missing"))       # None (no exception)

    for key, value in db:
        print(key, value)          # b'hello' b'world'
```

---

## Constants

### Journal Mode

| Constant | Description |
|----------|-------------|
| `JOURNAL_WAL` | Write-Ahead Logging — concurrent readers, sequential WAL writes **(default)** |
| `JOURNAL_DELETE` | Rollback journal — traditional SQLite journal mode |

### Sync Level

| Constant | Description |
|----------|-------------|
| `SYNC_OFF` | No fsync — fastest, not crash-safe |
| `SYNC_NORMAL` | fsync on WAL checkpoints — survives process crash **(default)** |
| `SYNC_FULL` | fsync on every commit — strongest durability guarantee |

### Checkpoint Mode

| Constant | Description |
|----------|-------------|
| `CHECKPOINT_PASSIVE` | Copy WAL frames without blocking readers or writers **(default)** |
| `CHECKPOINT_FULL` | Wait for all readers, then copy all frames |
| `CHECKPOINT_RESTART` | Like FULL, then reset the WAL write position |
| `CHECKPOINT_TRUNCATE` | Like RESTART, then truncate the WAL file to zero bytes |

### TTL

| Constant | Value | Description |
|----------|-------|-------------|
| `NO_TTL` | `-1` | Sentinel returned by `ttl()` when a key exists but has no expiry |

---

## Exceptions

```
snkv.Error          ← base class for all SNKV errors
├── snkv.NotFoundError   (also subclass of KeyError)
├── snkv.BusyError
├── snkv.LockedError
├── snkv.ReadOnlyError
└── snkv.CorruptError
```

| Exception | Raised when |
|-----------|-------------|
| `Error` | Generic SNKV error (base class) |
| `NotFoundError` | Key does not exist; also a `KeyError` so `except KeyError` works |
| `BusyError` | Database is locked by another connection and `busy_timeout` expired |
| `LockedError` | Write lock conflicts with a concurrent transaction |
| `ReadOnlyError` | Write attempted on a read-only store |
| `CorruptError` | Database integrity check detected corruption |

---

## KVStore

The main entry point. Opens or creates a key-value store at a given path.

### Opening a Store

#### `KVStore(path, *, journal_mode=JOURNAL_WAL, **config)`

```python
from snkv import KVStore, JOURNAL_WAL, SYNC_NORMAL

# Minimal — uses all defaults
db = KVStore("mydb.db")

# In-memory (no file, lost on close)
db = KVStore()         # or KVStore(None)

# With advanced options
db = KVStore(
    "mydb.db",
    journal_mode=JOURNAL_WAL,
    sync_level=SYNC_NORMAL,
    cache_size=4000,       # ~16 MB page cache
    page_size=4096,        # bytes; new databases only
    busy_timeout=5000,     # ms to retry on lock
    wal_size_limit=100,    # auto-checkpoint every 100 WAL frames
    read_only=0,           # 1 to open read-only
)
```

**Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | `str \| None` | `None` | Path to the database file. `None` opens an in-memory store. |
| `journal_mode` | `int` | `JOURNAL_WAL` | Journal mode constant. |
| `sync_level` | `int` | `SYNC_NORMAL` | Sync level constant. |
| `cache_size` | `int` | `2000` | Page cache size in pages (~4 KB each; default ~8 MB). |
| `page_size` | `int` | `4096` | Database page size in bytes. Only applied to new databases. |
| `busy_timeout` | `int` | `0` | Milliseconds to retry on `SQLITE_BUSY`. `0` = fail immediately. |
| `wal_size_limit` | `int` | `0` | Auto-checkpoint after N committed WAL frames. `0` = disabled. |
| `read_only` | `int` | `0` | `1` to open the database read-only. |

Always use as a context manager to guarantee proper cleanup:

```python
with KVStore("mydb.db") as db:
    db["key"] = "value"
# db is closed here
```

---

### Core Operations

#### `put(key, value, ttl=None) -> None`

Insert or overwrite a key-value pair in the default column family.

```python
db.put("user:1", b"\x01\x02\x03")
db.put(b"binary_key", "string_value")

# With expiry: key disappears after 3600 seconds
db.put("session:abc", "tok", ttl=3600)
db.put("cache:x", data, ttl=0.5)   # half a second
```

Both `key` and `value` accept `str` (UTF-8 encoded automatically) or `bytes` / `bytearray` / `memoryview`.

`ttl` is a number of seconds (int or float) until the key expires. `None` (default) means no expiry.
The data write and the TTL index write are committed atomically.

---

#### `get(key, default=None) -> bytes | None`

Return the stored value as `bytes`, or `default` if the key does not exist **or has expired**.

```python
val = db.get("user:1")          # b'\x01\x02\x03'  or  None
val = db.get("missing", b"")    # b''  if not found
val = db.get("session:abc")     # None if expired (lazy delete performed)
```

Never raises `NotFoundError` — use `db[key]` if you want an exception on miss.
Expired keys are lazily deleted on access and return `default`.

---

#### `delete(key) -> None`

Delete a key. Raises `NotFoundError` (a `KeyError`) if the key does not exist.

```python
db.delete("user:1")
```

---

#### `exists(key) -> bool`

Return `True` if the key exists, without fetching the value.

```python
if db.exists("user:1"):
    print("found")
```

---

### TTL / Key Expiry

SNKV supports per-key TTL with zero overhead for stores that never use it.
Expiry timestamps are stored in a hidden internal column family (`__snkv_ttl__`)
that is created lazily on first use and never appears in `list_column_families()`.

#### `put(key, value, ttl=seconds) -> None`

See [Core Operations](#core-operations). Pass `ttl=<seconds>` to set an expiry.

#### `ttl(key) -> float | None`

Return the remaining TTL for `key` in seconds.

| Return value | Meaning |
|---|---|
| Positive float | Seconds remaining |
| `0.0` | Key just expired (lazy delete performed) |
| `None` | Key exists but has no expiry |
| raises `NotFoundError` | Key does not exist |

```python
db.put("session", "tok", ttl=3600)

remaining = db.ttl("session")   # e.g. 3599.97
remaining = db.ttl("permanent") # None  (no TTL set)

try:
    db.ttl("nonexistent")
except NotFoundError:
    print("key not found")
```

#### `purge_expired() -> int`

Scan the TTL index and delete all expired keys in a single transaction.
Returns the number of keys deleted.

```python
n = db.purge_expired()
print(f"Removed {n} expired keys")
```

Lazy deletion (on `get` / `db[key]`) handles the common case automatically.
`purge_expired()` is useful for background cleanup or after a bulk load of short-lived keys.

**Full TTL example:**

```python
import time
from snkv import KVStore, NotFoundError

with KVStore("cache.db") as db:
    db.put("rate_limit:user1", b"42", ttl=60)   # expires in 60 s
    db.put("permanent_key",    b"data")          # no expiry

    # Transparent: returns None once expired (no exception)
    val = db.get("rate_limit:user1")             # b'42' while live

    # Check remaining time
    secs = db.ttl("rate_limit:user1")            # e.g. 59.9
    secs = db.ttl("permanent_key")               # None

    # Bulk purge
    deleted = db.purge_expired()
```

---

### Dict-like Interface

`KVStore` supports the standard Python mapping-style operators.
Both `db[key]` and `db.get(key)` perform lazy TTL expiry — expired keys raise
`KeyError` / return `None` just like missing keys.

| Syntax | Equivalent method | Notes |
|--------|-------------------|-------|
| `db["key"]` | `get` | Raises `NotFoundError` / `KeyError` on miss or expiry |
| `db["key"] = "val"` | `put` | No TTL — use `put(key, val, ttl=...)` for expiry |
| `del db["key"]` | `delete` | Raises `NotFoundError` / `KeyError` on miss |
| `"key" in db` | `exists` | |
| `for k, v in db` | `iterator()` | Yields `(bytes, bytes)` pairs |

```python
db["session:abc"] = "active"
print(db["session:abc"])          # b'active'
print("session:abc" in db)        # True
del db["session:abc"]
print("session:abc" in db)        # False
```

---

### Transactions

SNKV transactions are ACID-compliant. By default every individual put/delete runs in
its own auto-committed transaction. Use explicit transactions to batch multiple
operations atomically.

#### `begin(write=False) -> None`

Begin a transaction. Pass `write=True` to start a write transaction.

```python
db.begin(write=True)
```

#### `commit() -> None`

Commit the active transaction, making all changes permanent.

```python
db.commit()
```

#### `rollback() -> None`

Discard all changes since the last `begin()`.

```python
db.rollback()
```

**Example — atomic batch write:**

```python
db.begin(write=True)
try:
    db["a"] = "1"
    db["b"] = "2"
    db["c"] = "3"
    db.commit()
except Exception:
    db.rollback()
    raise
```

---

### Column Families

Column families are independent logical namespaces within a single database file.
Each family has its own key space and its own ordered B-Tree cursor.

#### `create_column_family(name) -> ColumnFamily`

Create a new column family. Raises `Error` if it already exists.

```python
cf = db.create_column_family("users")
```

#### `open_column_family(name) -> ColumnFamily`

Open an existing column family. Raises `NotFoundError` if it does not exist.

```python
cf = db.open_column_family("users")
```

#### `default_column_family() -> ColumnFamily`

Return a handle to the default (unnamed) column family used by `put`/`get`/`delete`.

```python
cf = db.default_column_family()
```

#### `list_column_families() -> list[str]`

Return a list of all user-defined column family names (excluding the default family
and internal system families such as `__snkv_ttl__`).

```python
names = db.list_column_families()   # e.g. ["users", "sessions"]
```

#### `drop_column_family(name) -> None`

Permanently delete a column family and all of its data.

```python
db.drop_column_family("users")
```

**Example — using column families:**

```python
with db.create_column_family("users") as users:
    users["alice"] = b"admin"
    users["bob"]   = b"viewer"

with db.open_column_family("users") as users:
    print(users["alice"])          # b'admin'
    for key, value in users:
        print(key, value)
```

Always close a `ColumnFamily` handle (or use it as a context manager) before closing the `KVStore`.

---

### Iterators

#### `iterator() -> Iterator`

Return an `Iterator` over all (key, value) pairs in the default column family,
in ascending key order.

```python
with db.iterator() as it:
    for key, value in it:
        print(key.decode(), value.decode())
```

#### `prefix_iterator(prefix) -> Iterator`

Return an `Iterator` over keys that start with `prefix`, in ascending key order.

```python
for key, value in db.prefix_iterator("user:"):
    print(key, value)
```

`prefix` accepts `str` or `bytes`.

---

### Maintenance

#### `sync() -> None`

Flush all pending writes to disk (equivalent to `fsync`).

```python
db.sync()
```

#### `vacuum(n_pages=0) -> None`

Run an incremental vacuum step to reclaim unused pages.
`n_pages=0` (default) frees all available unused pages.

```python
db.vacuum()       # reclaim all free pages
db.vacuum(100)    # reclaim up to 100 pages
```

#### `integrity_check() -> None`

Run a full database integrity check. Raises `CorruptError` with a description
if corruption is detected. Returns `None` on success.

```python
try:
    db.integrity_check()
    print("Database OK")
except snkv.CorruptError as e:
    print(f"Corruption detected: {e}")
```

#### `checkpoint(mode=CHECKPOINT_PASSIVE) -> tuple[int, int]`

Run a WAL checkpoint. Returns `(nLog, nCkpt)`:

- `nLog` — total WAL frames after the checkpoint
- `nCkpt` — frames successfully written to the main database file

Any active write transaction must be committed or rolled back before calling.

```python
from snkv import CHECKPOINT_TRUNCATE

nlog, nckpt = db.checkpoint(CHECKPOINT_TRUNCATE)
print(f"WAL frames remaining: {nlog}, checkpointed: {nckpt}")
```

#### `stats() -> dict`

Return operation counters as a dictionary.

```python
s = db.stats()
# {'puts': 42, 'gets': 100, 'deletes': 5, 'iterations': 3, 'errors': 0}
```

| Key | Description |
|-----|-------------|
| `puts` | Total successful `put` calls |
| `gets` | Total successful `get` calls |
| `deletes` | Total successful `delete` calls |
| `iterations` | Total iterator steps |
| `errors` | Total error occurrences |

#### `errmsg -> str`

Property. The last error message string from the underlying store.

```python
print(db.errmsg)
```

---

### Lifecycle

#### `close() -> None`

Close the store and release all resources. All column family and iterator handles
must be closed before calling this.

```python
db.close()
```

#### Context manager

```python
with KVStore("mydb.db") as db:
    ...
# Automatically calls db.close()
```

---

## ColumnFamily

A logical namespace within a `KVStore`. Obtained via
`db.create_column_family()`, `db.open_column_family()`, or `db.default_column_family()`.

### Core Operations

#### `put(key, value, ttl=None) -> None`

Insert or overwrite a key-value pair.

`ttl` — optional expiry in seconds (`int` or `float`). `None` means no expiry.
Both the data write and the TTL index write are atomic.
A plain `put()` on an existing TTL key removes the TTL entry.

```python
cf.put("alice", b"admin")              # permanent
cf.put("token", b"xyz", ttl=300)       # expires in 5 minutes
cf.put("token", b"xyz")                # overwrite — removes TTL
```

#### `get(key, default=None) -> bytes | None`

Return value bytes, or `default` if the key does not exist or has expired.
Performs lazy expiry on access.

```python
role = cf.get("alice")           # b'admin'  or  None
role = cf.get("unknown", b"")    # b''
```

#### `delete(key) -> None`

Delete a key. Raises `NotFoundError` if not found.

```python
cf.delete("alice")
```

#### `exists(key) -> bool`

Return `True` if the key exists.

```python
cf.exists("alice")    # True / False
```

---

### TTL / Key Expiry

#### `ttl(key) -> float | None`

Return remaining TTL in seconds.

| Return value | Meaning |
|-------------|---------|
| `None` | Key exists but has no expiry |
| `0.0` | Key just expired (lazy delete performed) |
| `N > 0.0` | N seconds remain |
| raises `NotFoundError` | Key does not exist at all |

```python
remaining = cf.ttl("token")   # e.g. 284.3
```

#### `purge_expired() -> int`

Scan and delete all expired keys in **this column family only** in a single
write transaction.  Uses the expiry index CF (sorted by expire time) to stop at
the first non-expired entry — O(expired keys).

Returns the number of data keys deleted.

```python
n = cf.purge_expired()
print(f"Cleaned up {n} expired entries")
```

---

### Dict-like Interface

`ColumnFamily` supports the same mapping-style operators as `KVStore`.

```python
cf["alice"] = "admin"
print(cf["alice"])             # b'admin'
print("alice" in cf)           # True
del cf["alice"]
```

---

### Iterators

#### `iterator() -> Iterator`

Return an `Iterator` over all keys in this column family in ascending order.

```python
for key, value in cf.iterator():
    print(key, value)
```

#### `prefix_iterator(prefix) -> Iterator`

Return an `Iterator` over keys starting with `prefix`.

```python
for key, value in cf.prefix_iterator("user:"):
    print(key, value)
```

---

### Lifecycle

#### `close() -> None`

Release the column family handle. Does **not** delete the column family or its data.
Must be called before the parent `KVStore` is closed.

#### Context manager

```python
with db.create_column_family("sessions") as cf:
    cf["tok:abc"] = b"uid:1"
# cf is closed here
```

---

## Iterator

Ordered key-value iterator returned by `KVStore.iterator()`,
`KVStore.prefix_iterator()`, `ColumnFamily.iterator()`, and
`ColumnFamily.prefix_iterator()`.

Iterators reflect a point-in-time snapshot of the B-Tree cursor position.

### Python Iterator Protocol

The most common usage — iterate with a `for` loop:

```python
for key, value in db.iterator():
    print(key.decode(), value.decode())

# As a context manager (auto-closes the iterator)
with db.prefix_iterator("session:") as it:
    for key, value in it:
        print(key, value)
```

Each iteration step yields a `(bytes, bytes)` tuple of `(key, value)`.

---

### Manual Control

For fine-grained cursor control:

#### `first() -> Iterator`

Seek to the first key. Returns `self` for chaining.

```python
it.first()
```

#### `next() -> None`

Advance to the next key.

```python
it.next()
```

#### `eof -> bool`

`True` when the iterator is past the last key.

```python
while not it.eof:
    print(it.key, it.value)
    it.next()
```

#### `key -> bytes`

The current key as bytes. Undefined if `eof` is `True`.

#### `value -> bytes`

The current value as bytes. Undefined if `eof` is `True`.

#### `item() -> tuple[bytes, bytes]`

Return the current `(key, value)` tuple.

```python
k, v = it.item()
```

**Full manual example:**

```python
it = db.iterator()
it.first()
while not it.eof:
    print(it.key.decode(), "->", it.value.decode())
    it.next()
it.close()
```

---

### Lifecycle

#### `close() -> None`

Release the iterator cursor. Must be called when done (or use a `with` block).

#### Context manager

```python
with db.iterator() as it:
    for key, value in it:
        ...
# it.close() called automatically
```

---

## Type Notes

**Keys and values** accept any of:

| Type | Behaviour |
|------|-----------|
| `str` | Encoded to UTF-8 bytes automatically |
| `bytes` | Passed through as-is |
| `bytearray` | Converted to `bytes` |
| `memoryview` | Converted to `bytes` |

**Return values** are always `bytes`.

**Thread safety:** The underlying C store is mutex-protected. Each Python method
acquires a mutex around the C call and releases the GIL for blocking I/O, making
it safe to share a `KVStore` across threads. Column family and iterator handles
are not independently thread-safe — access them from one thread at a time.

---

## Complete Example

```python
from snkv import (
    KVStore,
    JOURNAL_WAL, SYNC_NORMAL,
    CHECKPOINT_TRUNCATE,
    NotFoundError, CorruptError,
)

with KVStore("mydb.db",
             journal_mode=JOURNAL_WAL,
             sync_level=SYNC_NORMAL,
             cache_size=2000,
             busy_timeout=5000,
             wal_size_limit=200) as db:

    # Basic CRUD
    db["greeting"] = "hello"
    print(db["greeting"].decode())        # hello
    print(db.get("missing"))              # None
    del db["greeting"]

    # Atomic batch
    db.begin(write=True)
    db["a"] = "1"
    db["b"] = "2"
    db.commit()

    # TTL / key expiry
    db.put("session:xyz", b"tok_abc", ttl=3600)   # expires in 1 hour
    db.put("cache:foo",   b"data",    ttl=5)       # expires in 5 s
    db.put("permanent",   b"data")                 # no expiry

    print(db.ttl("session:xyz"))   # e.g. 3599.99 (seconds remaining)
    print(db.ttl("permanent"))     # None (no expiry)

    val = db.get("session:xyz")    # b'tok_abc' while live, None once expired
    n   = db.purge_expired()       # bulk-delete all expired keys

    # Column families
    with db.create_column_family("users") as users:
        users["alice"] = b"admin"
        users["bob"]   = b"viewer"

    with db.open_column_family("users") as users:
        for key, value in users.prefix_iterator("ali"):
            print(key, value)             # b'alice' b'admin'

    # Ordered full scan
    for key, value in db:
        print(key.decode(), "->", value.decode())

    # Maintenance
    nlog, nckpt = db.checkpoint(CHECKPOINT_TRUNCATE)
    print(f"WAL frames after checkpoint: {nlog}")

    try:
        db.integrity_check()
    except CorruptError as e:
        print(f"Corruption: {e}")

    print(db.stats())
```
