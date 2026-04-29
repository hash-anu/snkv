# SNKV Python Bindings

[![Build](https://github.com/hash-anu/snkv/actions/workflows/c-cpp.yml/badge.svg)](https://github.com/hash-anu/snkv/actions/workflows/c-cpp.yml)
[![PyPI](https://img.shields.io/pypi/v/snkv)](https://pypi.org/project/snkv/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/hash-anu/snkv/blob/master/LICENSE)

Idiomatic Python 3.8+ bindings for [SNKV](https://github.com/hash-anu/snkv) — a lightweight,
ACID-compliant embedded key-value store built directly on SQLite's B-Tree engine.

If you find it useful, a ⭐ on [GitHub](https://github.com/hash-anu/snkv) goes a long way!

---

## Features

- **Dict-style API** — `db["key"] = value`, `val = db["key"]`, `del db["key"]`, `"key" in db`
- **Context managers** — `with KVStore(...) as db` and `with db.create_column_family(...) as cf` for guaranteed cleanup
- **Prefix iterators** — efficient namespace scans with `db.prefix_iterator(b"user:")`
- **Reverse iterators** — walk keys in descending order with `db.reverse_iterator()` and `db.reverse_prefix_iterator(b"user:")`
- **WAL checkpoint control** — PASSIVE / FULL / RESTART / TRUNCATE modes via `db.checkpoint()`
- **Auto-checkpoint** — set `wal_size_limit=N` to checkpoint automatically after every N WAL frames
- **Typed exceptions** — `NotFoundError`, `BusyError`, `LockedError`, `ReadOnlyError`, `CorruptError` all subclass `snkv.Error`
- **No Python dependencies** — pure CPython C extension; only requires a C compiler and `python3-dev`
- **Native TTL** — per-key expiry with `put(ttl=seconds)`, dict-style `db[key, ttl] = value`, lazy expiry on get, and `purge_expired()`
- **Encryption** — per-value XChaCha20-Poly1305 encryption with Argon2id key derivation; transparent to all existing APIs
- **Seek iterators** — jump to any key in O(log N) with `it.seek(key)`, chainable and works on prefix/reverse iterators
- **Conditional insert** — atomic `put_if_absent(key, value, ttl=None)` returns `True` if inserted; safe for distributed locks and dedup
- **Bulk clear** — `db.clear()` / `cf.clear()` truncates all keys in O(pages) without dropping the store
- **Key count** — `db.count()` / `cf.count()` returns entry count in O(pages); CF counts are fully isolated
- **Extended stats** — `db.stats()` exposes 12 counters including `bytes_read`, `bytes_written`, `wal_commits`, `ttl_expired`, `db_pages`; reset with `db.stats_reset()`
- **Vector search** — integrated HNSW approximate nearest-neighbour index via `snkv[vector]`; sidecar persistence, quantization (f32/f16/i8), metadata filtering, exact rerank, TTL on vectors, and encryption support
- **471 tests** — full pytest suite covering ACID, WAL, crash recovery, concurrency, column families, TTL, encryption, and vector search

---

## Installation

### From PyPI (recommended)

Pre-built binary wheels are available for Linux, macOS, and Windows — no compiler needed.

**Windows / macOS:**
```bash
pip install snkv
```

**Linux (Debian/Ubuntu):**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install snkv
```

> Linux system Python is "externally managed" (PEP 668) and blocks
> system-wide pip installs. Use a virtual environment.

### Build from Source

```bash
# System dependencies
sudo apt-get install -y build-essential python3-dev python3-pip

# Python build dependencies
pip3 install setuptools wheel pytest

# Build
cd python
python3 setup.py build_ext --inplace
```

#### macOS

```bash
# Compiler (skip if already installed)
xcode-select --install

# Python build dependencies
pip3 install setuptools wheel pytest

# Build
cd python
python3 setup.py build_ext --inplace
```

#### Windows — Native Python (recommended)

1. Install [Python 3.8+](https://python.org/downloads) — check **"Add Python to PATH"**
2. Install [Visual Studio Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/) — select **"Desktop development with C++"**
3. Open **"x64 Native Tools Command Prompt for VS 2022"** from the Start Menu (required for 64-bit Python; "Developer PowerShell for VS" defaults to 32-bit and will fail)

```cmd
:: Python build dependencies
pip install setuptools wheel pytest

:: Build
cd python
python setup.py build_ext --inplace
```

#### Windows — MSYS2 MinGW64 shell

Open the **MSYS2 MinGW64** shell (not plain MSYS2, not cmd.exe):

```bash
# System + Python dependencies (one-time)
pacman -S --needed mingw-w64-x86_64-python \
                   mingw-w64-x86_64-python-pip \
                   mingw-w64-x86_64-python-setuptools \
                   mingw-w64-x86_64-python-pytest

# Build
cd python
python3 setup.py build_ext --inplace
```

> On all platforms, `setup.py` automatically locates `snkv.h` — no manual
> header step needed. On Linux/macOS it regenerates it via `make snkv.h`;
> on Windows it falls back to the pre-built `snkv.h` included in the repo.

---

## Quick Start

```python
from snkv import KVStore

with KVStore("mydb.db") as db:
    db["hello"] = "world"
    print(db["hello"].decode())   # world
```

---

## API Reference

### Opening a store

```python
from snkv import KVStore, JOURNAL_WAL, JOURNAL_DELETE, SYNC_NORMAL, SYNC_OFF, SYNC_FULL

with KVStore(
    "mydb.db",
    journal_mode=JOURNAL_WAL,   # JOURNAL_WAL (default) or JOURNAL_DELETE
    sync_level=SYNC_NORMAL,     # SYNC_NORMAL (default), SYNC_OFF, SYNC_FULL
    cache_size=2000,            # pages (~8 MB default)
    page_size=4096,             # bytes; new databases only
    busy_timeout=5000,          # ms to retry on SQLITE_BUSY (default 0)
    read_only=False,            # open read-only
    wal_size_limit=100,         # auto-checkpoint every 100 WAL frames (0 = off)
) as db:
    ...
```

### CRUD

```python
# Write
db["key"] = b"value"          # bytes or str keys/values are both accepted
db["key"] = "value"           # str is UTF-8 encoded automatically

# Read
val = db["key"]               # returns bytes; raises NotFoundError if missing
val = db.get("key")           # returns bytes or None
val = db.get("key", b"def")   # with default

# Check existence
exists = "key" in db
exists = db.exists(b"key")

# Delete
del db["key"]
db.delete(b"key")             # same as del; no error if key absent

# Upsert
db.put(b"key", b"value")      # identical to db["key"] = value
```

### Transactions

```python
db.begin(write=True)
db["a"] = "1"
db["b"] = "2"
db.commit()          # persist

db.begin(write=True)
db["c"] = "3"
db.rollback()        # discard — "c" is never written
```

Auto-commit is the default: each `db["key"] = value` outside an explicit transaction is
committed immediately.

### Column Families

Logical namespaces within a single database file. Always close `cf` before `db`.

```python
# Create (first use)
with db.create_column_family("users") as cf:
    cf[b"alice"] = b"admin"
    cf[b"bob"]   = b"viewer"

# Open (subsequent uses)
with db.open_column_family("users") as cf:
    print(cf[b"alice"])       # b"admin"

# List all column families
names = db.list_column_families()   # ["users", ...]

# Drop
db.drop_column_family("users")
```

### Iterators

```python
# Full scan — yields (key, value) tuples in key order
for key, value in db.iterator():
    print(key, value)

# Prefix scan
for key, value in db.prefix_iterator(b"user:"):
    print(key, value)

# Manual control
it = db.iterator()
it.first()
while not it.eof:
    print(it.key, it.value)
    it.next()
it.close()

# As a context manager
with db.iterator() as it:
    for key, value in it:
        ...
```

### Reverse Iterators

Walk keys in descending order — no full scan, no sort, pure B-tree traversal.

```python
# Full reverse scan
for key, value in db.reverse_iterator():
    print(key, value)

# Reverse prefix scan — visits only matching keys, largest first
for key, value in db.reverse_prefix_iterator(b"user:"):
    print(key, value)

# Manual control
it = db.reverse_iterator()
it.last()
while not it.eof:
    print(it.key, it.value)
    it.prev()
it.close()

# As a context manager
with db.reverse_prefix_iterator(b"log:") as it:
    for key, value in it:
        ...
```

Column families support reverse iterators identically via `cf.reverse_iterator()` and `cf.reverse_prefix_iterator()`.

### WAL Checkpoint

```python
from snkv import CHECKPOINT_PASSIVE, CHECKPOINT_FULL, CHECKPOINT_RESTART, CHECKPOINT_TRUNCATE

# Returns (nLog, nCkpt) — WAL frames total / frames written to DB
nlog, nckpt = db.checkpoint(CHECKPOINT_PASSIVE)    # copy frames without blocking
nlog, nckpt = db.checkpoint(CHECKPOINT_FULL)       # wait for writers, flush all
nlog, nckpt = db.checkpoint(CHECKPOINT_RESTART)    # like FULL, reset write position
nlog, nckpt = db.checkpoint(CHECKPOINT_TRUNCATE)   # like RESTART, truncate WAL file
```

Must be called outside an active write transaction. Use `wal_size_limit` to auto-checkpoint
instead.

### Iterator Seek

Jump to any position in O(log N) without scanning from the start.

```python
with db.iterator() as it:
    it.seek(b"user:bob")        # forward: position at first key >= target
    while not it.eof:
        print(it.key, it.value)
        it.next()

with db.iterator(reverse=True) as it:
    it.last()
    it.seek(b"user:bob")        # reverse: position at last key <= target
    while not it.eof:
        print(it.key, it.value)
        it.prev()

# Works on prefix iterators too — boundary still enforced
with db.iterator(prefix=b"user:") as it:
    it.seek(b"user:carol")      # skip straight to "user:carol"
    while not it.eof:
        print(it.key)
        it.next()

# seek() returns self for chaining
key = db.iterator().seek(b"target").key
```

### Conditional Insert

Atomically insert a key only when it is absent — safe for distributed locks and deduplication.

```python
# Returns True if inserted, False if the key already existed.
inserted = db.put_if_absent(b"lock", b"owner:alice")

# With TTL — the key auto-releases after the given number of seconds.
inserted = db.put_if_absent(b"session:42", b"token-xyz", ttl=30)

# Column families support the same method.
with db.create_column_family("dedup") as cf:
    if cf.put_if_absent(b"msg:001", b"hello"):
        process(b"msg:001")     # only the first caller reaches here
```

### Bulk Clear

Truncate all entries from a store or column family in O(pages) — no iterating, no individual deletes.

```python
db.clear()      # remove every key from the default CF

with db.create_column_family("cache") as cf:
    cf.clear()  # only this CF is affected; other CFs are untouched
```

TTL index entries are cleared atomically alongside data entries. Close all iterators before calling `clear()`.

### Key Count

Count entries without scanning individual keys.

```python
n = db.count()                           # total entries in the default CF

with db.open_column_family("users") as cf:
    n = cf.count()                       # only this CF; TTL index not counted

# count() includes expired-but-not-yet-purged keys.
# Call purge_expired() first for an accurate live count.
db.purge_expired()
n = db.count()
```

### Maintenance

```python
db.sync()                 # flush OS write buffers (fsync)
db.vacuum(100)            # reclaim up to 100 unused pages incrementally
db.integrity_check()      # raises CorruptError if database is corrupt

# Extended stats — 12 counters
stats = db.stats()
# Keys: puts, gets, deletes, iterations, errors,
#       bytes_read, bytes_written, wal_commits, checkpoints,
#       ttl_expired, ttl_purged, db_pages

# Reset all cumulative counters (db_pages is always live)
db.stats_reset()
```

### TTL — Native Key Expiry

Per-key TTL with automatic lazy expiry on read.

```python
# Put with TTL (seconds, float precision)
db.put(b"session", b"tok123", ttl=60)   # expires in 60 s
db[b"token", 30] = b"bearer-xyz"        # dict-style shorthand

# Get — expired keys are silently evicted and raise NotFoundError
val = db.get(b"session")                # returns bytes or None if expired

# Check remaining lifetime
from snkv import NotFoundError
try:
    remaining = db.ttl(b"session")      # seconds remaining (float)
except NotFoundError:
    remaining = None                    # key expired or never set

# Purge all expired keys from disk (returns count removed)
n = db.purge_expired()

# Column families support TTL identically
with db.create_column_family("cache") as cf:
    cf.put(b"item", b"data", ttl=10)
    cf[b"item2", 5] = b"data2"
    n = cf.purge_expired()
```

### Encryption

Transparent per-value encryption. All existing APIs work without modification.

```python
from snkv import KVStore, AuthError

# Create / open encrypted store
with KVStore.open_encrypted("mydb.db", b"hunter2") as db:
    db[b"secret"] = b"classified"
    print(db.is_encrypted())      # True
    print(db[b"secret"])          # b"classified" — transparent decrypt

# Wrong password raises AuthError
try:
    KVStore.open_encrypted("mydb.db", b"wrong")
except AuthError:
    print("bad password")

# Change password in-place (re-encrypts all values atomically)
with KVStore.open_encrypted("mydb.db", b"hunter2") as db:
    db.reencrypt(b"new-strong-pass")

# Remove encryption permanently
with KVStore.open_encrypted("mydb.db", b"new-strong-pass") as db:
    db.remove_encryption()
with KVStore("mydb.db") as db:    # plain open works now
    print(db[b"secret"])
```

| Method | Description |
|---|---|
| `KVStore.open_encrypted(path, password, **kwargs)` | Class method — open or create encrypted store |
| `db.is_encrypted()` | Returns `True` if store is encrypted |
| `db.reencrypt(new_password)` | Change password; re-encrypts all values atomically |
| `db.remove_encryption()` | Decrypt in-place; store becomes plain |

**Cryptographic details:** XChaCha20-Poly1305 per value · Argon2id KDF (64 MB, 3 iterations) · 40-byte overhead per value (nonce + MAC) · key wiped from memory on close.

---

## Vector Search

Integrated HNSW approximate nearest-neighbour index backed by [usearch](https://github.com/unum-cloud/usearch). All vectors and KV data live in the same `.db` file — no separate index file, no external service.

### Installation

```bash
pip install snkv[vector]
```

### Quick Start

```python
from snkv.vector import VectorStore
import numpy as np

with VectorStore("store.db", dim=128, space="cosine") as vs:
    vs.vector_put(b"doc:1", b"hello world", np.random.rand(128).astype("f4"))
    results = vs.search(np.random.rand(128).astype("f4"), top_k=5)
    for r in results:
        print(r.key, r.distance, r.value)
```

### Parameters

| Parameter | Default | Description |
|---|---|---|
| `path` | — | Path to `.db` file. `None` for in-memory. |
| `dim` | — | Vector dimension. Fixed for the lifetime of the store. |
| `space` | `"l2"` | Distance metric: `"l2"` (squared L2), `"cosine"`, or `"ip"` (inner product). |
| `connectivity` | `16` | HNSW M parameter. |
| `expansion_add` | `128` | HNSW expansion during index build. |
| `expansion_search` | `None` | HNSW expansion at query time. `None` restores the stored value (default 64). |
| `dtype` | `"f32"` | In-memory index precision: `"f32"`, `"f16"` (half RAM), or `"i8"` (quarter RAM). On-disk storage is always float32. |
| `password` | `None` | Open/create an encrypted store. Sidecar is disabled for encrypted stores. |

### Quantization

`dtype` controls the in-memory HNSW graph precision only — on-disk storage in `_snkv_vec_` is always float32.

| dtype | RAM per vector (dim=768) | Notes |
|---|---|---|
| `"f32"` | 3072 bytes | Full precision (default) |
| `"f16"` | 1536 bytes | Half RAM, negligible recall loss |
| `"i8"` | 768 bytes | Quarter RAM, small recall cost |

For 1 M vectors at dim=768: `f32` ≈ 3 GB → `f16` ≈ 1.5 GB → `i8` ≈ 768 MB.

```python
# Half RAM for the in-memory index; on-disk vectors still float32
with VectorStore("store.db", dim=768, space="cosine", dtype="f16") as vs:
    vs.vector_put(b"doc:1", b"hello", np.random.rand(768).astype("f4"))
```

### Index Persistence (Sidecar)

For unencrypted file-backed stores, the HNSW index is saved to `{path}.usearch` on `close()` and reloaded on the next open — skipping the O(n×d) CF rebuild. A companion `{path}.usearch.nid` stamp file detects any write that occurred after the last clean close (including crash scenarios). Stale or corrupt sidecars are silently discarded and the index is rebuilt from the column families.

Encrypted stores and in-memory stores always rebuild from column families.

### Key Methods

```python
# Write
vs.vector_put(b"key", b"value", vec, ttl=None, metadata=None)
vs.vector_put_batch([(b"key", b"value", vec), ...], ttl=None)

# Search
results = vs.search(query_vec, top_k=10)                           # ANN
results = vs.search(query_vec, top_k=10, filter={"topic": "ml"})  # metadata filter
results = vs.search(query_vec, top_k=10, rerank=True)             # exact rerank
results = vs.search(query_vec, top_k=10, max_distance=0.5)        # distance cutoff
pairs   = vs.search_keys(query_vec, top_k=10)                     # keys + distances only

# SearchResult fields: key, value, distance, metadata
# NOTE: result.metadata is None unless filter= is passed to search().
# To access metadata without filtering, call get_metadata(key) after the search:
for r in results:
    meta = vs.get_metadata(r.key)   # dict or None — always works

# Read
vec  = vs.vector_get(b"key")          # np.ndarray(dim,) float32
val  = vs.get(b"key")                 # value bytes from KV store
meta = vs.get_metadata(b"key")        # dict or None

# Delete / maintenance
vs.delete(b"key")
n = vs.vector_purge_expired()         # remove expired vectors from index + CFs

# Stats
stats = vs.vector_stats()
# Keys: dim, space, dtype, connectivity, expansion_add, expansion_search,
#       count, capacity, fill_ratio, vec_cf_count, has_metadata, sidecar_enabled

# Drop index (KV data preserved)
vs.drop_vector_index()
```

### Encrypted Vector Store

```python
from snkv import AuthError

with VectorStore("store.db", dim=128, password=b"secret") as vs:
    vs.vector_put(b"doc:1", b"classified", np.random.rand(128).astype("f4"))

try:
    VectorStore("store.db", dim=128, password=b"wrong")
except AuthError:
    print("bad password")
```

---

## Error Hierarchy

```
snkv.Error (base)
├── snkv.NotFoundError       (also KeyError — raised by db["missing"])
├── snkv.BusyError           (SQLITE_BUSY — another writer holds the lock)
├── snkv.LockedError         (SQLITE_LOCKED)
├── snkv.ReadOnlyError       (write attempted on read-only store)
├── snkv.CorruptError        (database file is corrupt)
└── snkv.AuthError           (wrong password or not an encrypted store)

snkv.vector.VectorIndexError  (index dropped or empty; not a subclass of snkv.Error)
```

```python
import snkv

try:
    val = db["missing_key"]
except snkv.NotFoundError:
    val = b"default"

try:
    db["key"] = b"value"
except snkv.BusyError:
    # retry after a delay
    ...
```

---

## Running Tests

**Linux / macOS**
```bash
cd python
python3 -m pytest tests/ -v
```

**Windows — Native Python (x64 Native Tools Command Prompt for VS 2022)**
```cmd
cd python
set PYTHONPATH=.
python -m pytest tests\ -v
```

**Windows — MSYS2 MinGW64 shell**
```bash
cd python
PYTHONPATH=. python3 -m pytest tests/ -v
```

All 471 tests should pass.

---

## Running Examples

**Linux / macOS**
```bash
cd python
PYTHONPATH=. python3 examples/basic.py           # CRUD, binary data, in-memory store
PYTHONPATH=. python3 examples/transactions.py    # begin/commit/rollback
PYTHONPATH=. python3 examples/column_families.py # logical namespaces
PYTHONPATH=. python3 examples/iterators.py       # ordered scan, prefix scan
PYTHONPATH=. python3 examples/config.py          # journal mode, sync, cache, WAL limit
PYTHONPATH=. python3 examples/checkpoint.py      # manual + auto WAL checkpoint
PYTHONPATH=. python3 examples/session_store.py   # real-world session store pattern
PYTHONPATH=. python3 examples/ttl.py             # TTL expiry, rate limiter demo
PYTHONPATH=. python3 examples/encryption.py  # encrypted store, wrong-password, reencrypt
PYTHONPATH=. python3 examples/iterator_reverse.py # reverse iterators, descending scans
PYTHONPATH=. python3 examples/new_apis.py        # seek, put_if_absent, clear, count, stats
PYTHONPATH=. python3 examples/multiprocess.py    # 5 concurrent processes, busy_timeout
PYTHONPATH=. python3 examples/vector.py          # vector search, quantization, sidecar, TTL, encryption
```

**Windows — Native Python (x64 Native Tools Command Prompt for VS 2022)**
```cmd
cd python
set PYTHONPATH=.
python examples\basic.py
python examples\transactions.py
python examples\column_families.py
python examples\iterators.py
python examples\config.py
python examples\checkpoint.py
python examples\session_store.py
python examples\ttl.py
python examples\encryption.py
python examples\iterator_reverse.py
python examples\new_apis.py
python examples\multiprocess.py
python examples\all_apis.py
python examples\vector.py
```

**Windows — MSYS2 MinGW64 shell**
```bash
cd python
PYTHONPATH=. python3 examples/basic.py
PYTHONPATH=. python3 examples/transactions.py
# ... same pattern for all examples
```

---

## Benchmarks

Benchmarks run on **Windows 11** comparing SNKV against [diskcache](https://pypi.org/project/diskcache/) 5.6.3.
Both libraries are SQLite-backed; diskcache adds pickle serialization overhead that SNKV avoids by operating on raw bytes.

Run it yourself:
```bash
pip install diskcache
cd python
PYTHONIOENCODING=utf-8 python examples/benchmark_diskcache.py
```

### Core Operations — N = 10,000 ops, 64-byte values

| Operation | SNKV | diskcache | Speedup |
|---|---|---|---|
| Bulk write (batched tx) | 24.4 ms | 649.6 ms | **26.6x** |
| Individual write | 189.2 ms | 2.02 s | **10.7x** |
| Read — hit (100%) | 19.5 ms | 178.7 ms | **9.2x** |
| Read — miss (100%) | 18.1 ms | 170.3 ms | **9.4x** |
| Delete (batched tx) | 25.1 ms | 250.8 ms | **10.0x** |
| Full scan (key + value) | 7.3 ms | 31.8 ms | **4.4x** |
| Write with TTL | 68.7 ms | 636.5 ms | **9.3x** |

### Prefix Scan — 10,000 keys, 1,000 matching `ns3:`

SNKV uses a native `prefix_iterator` that visits only matching keys. diskcache has no native prefix support and scans all keys in Python.

| Operation | SNKV | diskcache | Speedup |
|---|---|---|---|
| Forward prefix scan | 3.0 ms | 94.7 ms | **31.7x** |
| Reverse prefix scan | 2.8 ms | 93.7 ms | **33.4x** |

### Mixed Workload — 80% read / 20% write, 10,000 ops

| Operation | SNKV | diskcache | Speedup |
|---|---|---|---|
| 80% read / 20% write | 57.5 ms | 575.0 ms | **10.0x** |

### Value Size Scaling — N = 5,000 ops

| Size | Op | SNKV | diskcache | Speedup |
|---|---|---|---|---|
| 64 B | write | 11.5 ms | 369.7 ms | **32.2x** |
| 64 B | read | 10.7 ms | 95.3 ms | **8.9x** |
| 1 KB | write | 294.3 ms | 400.6 ms | **1.4x** |
| 1 KB | read | 288.1 ms | 103.2 ms | 0.4x |
| 10 KB | write | 769.3 ms | 914.3 ms | **1.2x** |
| 10 KB | read | 746.8 ms | 156.4 ms | 0.2x |
| 100 KB | write | 5.98 s | 7.03 s | **1.2x** |
| 100 KB | read | 5.77 s | 1.33 s | 0.2x |

> For large values (≥ 1 KB) diskcache's read path becomes competitive because pickle overhead shrinks relative to I/O cost. SNKV retains its write advantage at every size.

### Large Dataset — N = 100,000 keys, 64-byte values

| Operation | SNKV | diskcache | Speedup |
|---|---|---|---|
| Bulk write (batched tx) | 217.7 ms | 5.05 s | **23.2x** |
| Read (10,000 sampled) | 35.3 ms | 186.1 ms | **5.3x** |
| Full scan (key + value) | 50.1 ms | 199.1 ms | **4.0x** |

### Durability — Write → Close → Reopen → Verify, N = 10,000 keys

| Library | Test | Write | Reopen + Read | Verified |
|---|---|---|---|---|
| SNKV | Plain write | 31.7 ms | 22.0 ms | 10,000 / 10,000 ✓ |
| diskcache | Plain write | 741.6 ms | 175.2 ms | 10,000 / 10,000 ✓ |
| SNKV | Write with TTL | 129.6 ms | 30.1 ms | 10,000 / 10,000 ✓ |
| diskcache | Write with TTL | 717.3 ms | 177.5 ms | 10,000 / 10,000 ✓ |

Write speedup: **23.4x** (plain) · **5.5x** (TTL). Both stores verified all keys correctly after a cold reopen.

**Overall: SNKV faster in 20 / 23 tests — average speedup 11.1x**

---

## Thread Safety

Each thread must use its own `KVStore` instance. WAL mode serialises concurrent writers
at the SQLite level — a `BusyError` is raised (or retried up to `busy_timeout` ms) when
two writers collide. Multiple readers always make progress concurrently in WAL mode.

```python
import threading
from snkv import KVStore, JOURNAL_WAL

def worker(db_path, worker_id):
    # Each thread opens its own connection
    with KVStore(db_path, journal_mode=JOURNAL_WAL, busy_timeout=5000) as db:
        db[f"key_{worker_id}".encode()] = b"value"

threads = [threading.Thread(target=worker, args=("mydb.db", i)) for i in range(4)]
for t in threads: t.start()
for t in threads: t.join()
```

---

## Third-Party Licenses

The `snkv` Python package embeds the following third-party libraries compiled into its native extension:

| Library | Version | License | Notes |
|---------|---------|---------|-------|
| [SQLite](https://www.sqlite.org/) | 3.x (amalgamation subset) | [Public Domain](https://www.sqlite.org/copyright.html) | B-tree, pager, WAL, OS layer |
| [Monocypher](https://monocypher.org/) | 4.x | [CC0-1.0](https://creativecommons.org/publicdomain/zero/1.0/) (Public Domain) | XChaCha20-Poly1305 + Argon2id |
| [usearch](https://github.com/unum-cloud/usearch) | ≥ 2.9 | [Apache 2.0](https://github.com/unum-cloud/usearch/blob/main/LICENSE) | HNSW vector index (optional — `pip install snkv[vector]`) |

SQLite and Monocypher are statically linked into the extension module — no separate installation required.

SQLite and Monocypher are public domain — no attribution is legally required, but credit is given here in the spirit of good practice. usearch is an optional runtime dependency and is not bundled.

---

## License

Apache License 2.0 © 2025 Hash Anu
