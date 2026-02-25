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
- **Context managers** — `with KeyValueStore(...) as db` and `with db.create_column_family(...) as cf` for guaranteed cleanup
- **Prefix iterators** — efficient namespace scans with `db.prefix_iterator(b"user:")`
- **WAL checkpoint control** — PASSIVE / FULL / RESTART / TRUNCATE modes via `db.checkpoint()`
- **Auto-checkpoint** — set `wal_size_limit=N` to checkpoint automatically after every N WAL frames
- **Typed exceptions** — `NotFoundError`, `BusyError`, `LockedError`, `ReadOnlyError`, `CorruptError` all subclass `snkv.Error`
- **No Python dependencies** — pure CPython C extension; only requires a C compiler and `python3-dev`
- **247 tests** — full pytest suite covering ACID, WAL, crash recovery, concurrency, column families, JSON, and more

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
from snkv import KeyValueStore

with KeyValueStore("mydb.db") as db:
    db["hello"] = "world"
    print(db["hello"].decode())   # world
```

---

## API Reference

### Opening a store

```python
from snkv import KeyValueStore, JOURNAL_WAL, JOURNAL_DELETE, SYNC_NORMAL, SYNC_OFF, SYNC_FULL

with KeyValueStore(
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

### Maintenance

```python
db.sync()                 # flush OS write buffers (fsync)
db.vacuum(100)            # reclaim up to 100 unused pages incrementally
db.integrity_check()      # raises CorruptError if database is corrupt
stats = db.stats()        # dict: {"puts": N, "gets": N, "deletes": N, "iterations": N}
```

---

## Error Hierarchy

```
snkv.Error (base)
├── snkv.NotFoundError   (also KeyError — raised by db["missing"])
├── snkv.BusyError       (SQLITE_BUSY — another writer holds the lock)
├── snkv.LockedError     (SQLITE_LOCKED)
├── snkv.ReadOnlyError   (write attempted on read-only store)
└── snkv.CorruptError    (database file is corrupt)
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

All 247 tests should pass.

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
```

**Windows — MSYS2 MinGW64 shell**
```bash
cd python
PYTHONPATH=. python3 examples/basic.py
PYTHONPATH=. python3 examples/transactions.py
# ... same pattern for all examples
```

---

## Thread Safety

Each thread must use its own `KeyValueStore` instance. WAL mode serialises concurrent writers
at the SQLite level — a `BusyError` is raised (or retried up to `busy_timeout` ms) when
two writers collide. Multiple readers always make progress concurrently in WAL mode.

```python
import threading
from snkv import KeyValueStore, JOURNAL_WAL

def worker(db_path, worker_id):
    # Each thread opens its own connection
    with KeyValueStore(db_path, journal_mode=JOURNAL_WAL, busy_timeout=5000) as db:
        db[f"key_{worker_id}".encode()] = b"value"

threads = [threading.Thread(target=worker, args=("mydb.db", i)) for i in range(4)]
for t in threads: t.start()
for t in threads: t.join()
```

---

## License

Apache License 2.0 © 2025 Hash Anu
