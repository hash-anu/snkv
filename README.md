
<p align="center">
  <img src="snkv_logo.png" alt="SNKV Logo" width="220">
</p>

<h1 align="center">SNKV</h1>
<p align="center">
A simple, crash-safe embedded key-value store
</p>

<p align="center">

# SNKV — a simple, crash-safe embedded key-value store

[![Build](https://github.com/hash-anu/snkv/actions/workflows/c-cpp.yml/badge.svg)](https://github.com/hash-anu/snkv/actions/workflows/c-cpp.yml)
[![Memory Leaks](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/hash-anu/snkv/badges/valgrind.json)](https://github.com/hash-anu/snkv/actions/workflows/c-cpp.yml)
[![Tests](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/hash-anu/snkv/badges/tests.json)](https://github.com/hash-anu/snkv/actions/workflows/c-cpp.yml)
[![Peak Memory](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/hash-anu/snkv/badges/memory.json)](https://github.com/hash-anu/snkv/actions/workflows/c-cpp.yml)
[![GitHub Issues](https://img.shields.io/github/issues/hash-anu/snkv?label=open%20issues&color=orange)](https://github.com/hash-anu/snkv/issues)
[![GitHub Closed Issues](https://img.shields.io/github/issues-closed/hash-anu/snkv?label=closed%20issues&color=green)](https://github.com/hash-anu/snkv/issues?q=is%3Aissue+is%3Aclosed)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/hash-anu/snkv/blob/master/LICENSE)

---

## What is SNKV?

**SNKV** is a lightweight, **ACID-compliant embedded key-value store** built directly on SQLite's B-Tree storage engine — without SQL.

The idea: bypass the SQL layer entirely and talk directly to SQLite's storage engine. No SQL parser. No query planner. No virtual machine. Just a clean KV API on top of a proven, battle-tested storage core.

> *SQLite-grade reliability. KV-first design. Lower overhead for read-heavy and mixed key-value workloads.*

---

## Quick Start

Single-header integration — drop it in and go:

```c
#define SNKV_IMPLEMENTATION
#include "snkv.h"

int main(void) {
    KVStore *db;
    kvstore_open("mydb.db", &db, KVSTORE_JOURNAL_WAL);

    kvstore_put(db, "key", 3, "value", 5);

    void *val; int len;
    kvstore_get(db, "key", 3, &val, &len);
    printf("%.*s\n", len, (char*)val);
    snkv_free(val);

    kvstore_close(db);
}
```
**C/C++ API Reference:** [hash-anu.github.io/snkv/api.html](https://hash-anu.github.io/snkv/api.html#lang=c)

---

## TTL — Native Key Expiry

SNKV has built-in TTL (Time-To-Live) support. Keys expire automatically — no background thread, no cron job. Expiry is enforced lazily on access and can be bulk-purged on demand.

```c
/* C API */
int64_t expire_ms = kvstore_now_ms() + 60000;  /* 60 seconds from now */
kvstore_put_ttl(db, "session", 7, "tok123", 6, expire_ms);

void *val; int len; int64_t remaining_ms;
kvstore_get_ttl(db, "session", 7, &val, &len, &remaining_ms);

int n;
kvstore_purge_expired(db, &n);  /* bulk-delete all expired keys */
```

```python
# Python API
db.put(b"session", b"tok123", ttl=60)      # expires in 60 seconds
db[b"token", 30] = b"bearer-xyz"           # dict-style TTL

val = db.get(b"session")                   # None if expired
try:
    remaining = db.ttl(b"session")         # seconds remaining, or None if no expiry
except NotFoundError:
    remaining = None                        # key does not exist
n = db.purge_expired()                     # bulk-delete expired keys
```

TTL is supported on both the default store and on column families. Expired keys are lazily deleted on `get()` and `exists()` — consistent results without a background thread.

---

## Encryption

SNKV has built-in at-rest encryption. Every value is encrypted with XChaCha20-Poly1305; passwords are stretched with Argon2id. All existing APIs work transparently on encrypted stores.

```c
/* C API */
KVStore *db;
kvstore_open_encrypted("mydb.db", "hunter2", 7, &db, NULL);

kvstore_put(db, "secret", 6, "classified", 10);   /* encrypted transparently */

void *val; int len;
kvstore_get(db, "secret", 6, &val, &len);          /* decrypted on read */
snkv_free(val);

kvstore_reencrypt(db, "new-pass", 8);              /* change password in-place */
kvstore_close(db);
```

```python
# Python API
with KVStore.open_encrypted("mydb.db", b"hunter2") as db:
    db[b"secret"] = b"classified"       # encrypted transparently
    print(db[b"secret"])                # b"classified" — decrypted on read
    db.reencrypt(b"new-pass")           # change password in-place
```

**Cryptographic details:** XChaCha20-Poly1305 per value · Argon2id KDF (64 MB, 3 iterations) · 40-byte overhead per value · encryption key wiped from memory on close · wrong password returns `KVSTORE_AUTH_FAILED` / raises `AuthError`.

---

## Configuration

Use `kvstore_open_v2` to control how the store is opened. Zero-initialise the
config and set only what you need — unset fields resolve to safe defaults.

```c
KVStoreConfig cfg = {0};
cfg.journalMode = KVSTORE_JOURNAL_WAL;   /* WAL mode (default) */
cfg.syncLevel   = KVSTORE_SYNC_NORMAL;   /* survives process crash (default) */
cfg.cacheSize   = 4000;                  /* ~16 MB page cache (default 2000 ≈ 8 MB) */
cfg.pageSize    = 4096;                  /* DB page size, new DBs only (default 4096) */
cfg.busyTimeout = 5000;                  /* retry 5 s on SQLITE_BUSY (default 0) */
cfg.readOnly    = 0;                     /* read-write (default) */

KVStore *db;
kvstore_open_v2("mydb.db", &db, &cfg);
```

| Field | Default | Options |
|-------|---------|---------|
| `journalMode` | `KVSTORE_JOURNAL_WAL` | `KVSTORE_JOURNAL_DELETE` |
| `syncLevel` | `KVSTORE_SYNC_NORMAL` | `KVSTORE_SYNC_OFF`, `KVSTORE_SYNC_FULL` |
| `cacheSize` | 2000 pages (~8 MB) | Any positive integer |
| `pageSize` | 4096 bytes | Power of 2, 512–65536; new DBs only |
| `readOnly` | 0 | 1 to open read-only |
| `busyTimeout` | 0 (fail immediately) | Milliseconds; useful for multi-process use |

`kvstore_open` remains fully supported and uses all defaults except `journalMode`.

---

## Installation & Build

### Linux / macOS

```bash
make              # builds libsnkv.a
make snkv.h       # generates single-header version
make examples     # builds examples
make run-examples # run all examples
make test         # run all tests (CI suite)
make clean
```

### Windows (MSYS2 / MinGW64)

**1.** Install [MSYS2](https://www.msys2.org/).

**2.** Launch **"MSYS2 MinGW 64-bit"** from the Start menu (not the plain MSYS2 terminal).

**3.** Install the toolchain:

```bash
pacman -S --needed mingw-w64-x86_64-gcc make
```

**4.** Clone and build:

```bash
git clone https://github.com/hash-anu/snkv.git
cd snkv
make              # builds libsnkv.a
make snkv.h       # generates single-header
make examples     # builds .exe examples
make run-examples
make test
```

> All commands must be run from the **MSYS2 MinGW64 shell**. Running `mingw32-make` from
> a native `cmd.exe` or PowerShell window will not work — the Makefile relies on `sh` and
> standard Unix tools that are only available inside the MSYS2 environment.

---

### Python Bindings

Available on PyPI — no compiler needed:

```bash
pip install snkv
```

```python
from snkv import KVStore

with KVStore("mydb.db") as db:
    db["hello"] = "world"
    print(db["hello"].decode())   # world
```

Full documentation — installation, API reference, examples, and thread-safety notes — is in
**[python/README.md](python/README.md)**.

![SNKV Python API Demo](demo.gif)

---

### 10 GB Crash-Safety Stress Test

A production-scale kill-9 test is included but kept separate from the CI suite.
It writes unique deterministic key-value pairs into a 10 GB WAL-mode database,
forcibly kills the writer with `SIGKILL` during active writes, and verifies on
restart that every committed transaction is present with byte-exact values, no
partial transactions are visible, and the database has zero corruption.

```bash
make test-crash-10gb          # run full 5-cycle kill-9 + verify (Linux / macOS)

# individual modes
./tests/test_crash_10gb write  tests/crash_10gb.db   # continuous writer
./tests/test_crash_10gb verify tests/crash_10gb.db   # post-crash verifier
./tests/test_crash_10gb clean  tests/crash_10gb.db   # remove DB files
```

> Requires ~11 GB free disk. `run` mode is POSIX-only; `write` and `verify` work on all platforms.

---

## How It Works

Standard database path:

```
Application → SQL Parser → Query Planner → VDBE (VM) → B-Tree → Disk
```

SNKV path:

```
Application → KV API → B-Tree → Disk
```

By removing the layers you don't need for key-value workloads, SNKV keeps the proven storage core and cuts the overhead.

| Layer         | SQLite | SNKV |
| ------------- | ------ | ---- |
| SQL Parser    | ✅      | ❌    |
| Query Planner | ✅      | ❌    |
| VDBE (VM)     | ✅      | ❌    |
| B-Tree Engine | ✅      | ✅    |
| Pager / WAL   | ✅      | ✅    |

---

## Benchmarks

> 1M records, Linux, averaged across 3 runs.
> Both SNKV and SQLite use identical settings: WAL mode, `synchronous=NORMAL`, 2000-page (8 MB) page cache, 4096-byte pages.
>
> Benchmark source: [SNKV](https://github.com/hash-anu/snkv/blob/master/tests/test_benchmark.c) · [SQLite](https://github.com/hash-anu/sqllite-benchmark-kv)

### SNKV vs SQLite (KV workloads)

SQLite benchmark uses `WITHOUT ROWID` with a BLOB primary key — the fairest possible comparison, both using a single B-tree keyed on the same field. Both run with identical settings: WAL mode, `synchronous=NORMAL`, 2000-page (8 MB) cache, 4096-byte pages. This isolates the pure cost of the SQL layer for KV operations.

> Note: Both SNKV and SQLite (`WITHOUT ROWID`) use identical peak RSS (~10.8 MB) since they share the same underlying pager and page cache infrastructure.

| Benchmark         | SQLite       | SNKV         | Notes                        |
| ----------------- | ------------ | ------------ | ---------------------------- |
| Sequential writes | 142K ops/s   | 232K ops/s   | **SNKV 1.64x faster**        |
| Random reads      | 90K ops/s    | 160K ops/s   | **SNKV 1.77x faster**        |
| Sequential scan   | 1.56M ops/s  | 2.89M ops/s  | **SNKV 1.85x faster**        |
| Random updates    | 16K ops/s    | 31K ops/s    | **SNKV 1.9x faster**         |
| Random deletes    | 16K ops/s    | 31K ops/s    | **SNKV ~2x faster**          |
| Exists checks     | 93K ops/s    | 173K ops/s   | **SNKV 1.85x faster**        |
| Mixed workload    | 34K ops/s    | 62K ops/s    | **SNKV 1.79x faster**        |
| Bulk insert       | 211K ops/s   | 248K ops/s   | **SNKV 1.17x faster**        |

With identical storage configuration, SNKV wins across every benchmark. The gains come entirely from bypassing the SQL layer — no parsing, no query planner, no VDBE — and a per-column-family cached read cursor that eliminates repeated cursor open/close overhead on the hot read path. Updates and deletes show the biggest gains (~2x) since SQLite must parse, plan, and execute a full SQL statement per operation. Bulk insert is the closest (17%) because both commit a single large B-tree transaction with minimal per-row overhead.

---

### Running your own LMDB / RocksDB comparison

If you want to benchmark SNKV against LMDB or RocksDB, the benchmark harnesses are here:

- **LMDB** — [github.com/hash-anu/lmdb-benchmark](https://github.com/hash-anu/lmdb-benchmark)
- **RocksDB** — [github.com/hash-anu/rocksdb-benchmark](https://github.com/hash-anu/rocksdb-benchmark)

---

## When to Use SNKV

**SNKV is a good fit if:**
- Your workload is read-heavy or mixed (reads + writes)
- You need native TTL — sessions, rate limiting, caches, OTP codes, leases
- You're running in a memory-constrained or embedded environment
- You want a clean KV API without writing SQL strings, preparing statements, and binding parameters
- You need single-header C integration with no external dependencies
- You want predictable latency — no compaction stalls, no mmap tuning

**Consider alternatives if:**
- You need maximum write/update/delete throughput → **RocksDB** (LSM-tree)
- You need maximum read/scan speed and memory isn't a constraint → **LMDB** (memory-mapped)
- You already use SQL elsewhere and want to consolidate → **SQLite directly**

---

## Features

- **ACID Transactions** — commit / rollback safety
- **WAL Mode** — concurrent readers + single writer
- **Native TTL** — per-key expiry with lazy eviction and `purge_expired()`; no background thread required
- **Encryption** — per-value XChaCha20-Poly1305 with Argon2id key derivation; transparent to all existing APIs
- **Column Families** — logical namespaces within a single database
- **Iterators** — ordered key traversal
- **Thread Safe** — built-in synchronization
- **Single-header** — drop `snkv.h` into any C/C++ project
- **Zero memory leaks** — verified with Valgrind
- **SSD-friendly** — WAL appends sequentially, reducing random writes

- **Python Bindings** — idiomatic Python 3.8+ API with dict-style access, TTL support, context managers, typed exceptions, and prefix iterators — see [python/README.md](python/README.md)

---

## Backup & Tooling Compatibility

Because SNKV uses SQLite's file format and pager layer, backup tools that operate at the WAL or page level work out of the box:

- ✅ **LiteFS** — distributed SQLite replication works with SNKV databases
- ✅ **WAL-based backup tools** — any tool consuming WAL files works correctly
- ✅ **Rollback journal tools** — journal mode is fully supported

**Note:** Tools that rely on SQLite's schema layer — like the `sqlite3` CLI or DB Browser for SQLite — won't work. SNKV bypasses the schema layer entirely by design.

---

## Internals & Documentation

I documented the SQLite internals explored while building this:

- [B-Tree operations](https://github.com/hash-anu/snkv/blob/master/internal/BTREE_OPERATIONS.md)
- [Pager operations](https://github.com/hash-anu/snkv/blob/master/internal/PAGER_OPERATIONS.md)
- [OS layer operations](https://github.com/hash-anu/snkv/blob/master/internal/OS_LAYER_OPERATIONS.md)
- [KV layer design](https://github.com/hash-anu/snkv/blob/master/internal/KVSTORE_OPERATIONS.md)

---

## Design Principles

- **Minimalism wins** — fewer layers, less overhead
- **Proven foundations** — reuse battle-tested storage, don't reinvent it
- **Predictable performance** — no hidden query costs, no compaction stalls
- **Honest tradeoffs** — SNKV is not the fastest at everything; it's optimized for its target use case

---

## Third-Party Licenses

SNKV embeds the following third-party libraries:

| Library | Version | License | Notes |
|---------|---------|---------|-------|
| [SQLite](https://www.sqlite.org/) | 3.x (amalgamation subset) | [Public Domain](https://www.sqlite.org/copyright.html) | B-tree, pager, WAL, OS layer |
| [Monocypher](https://monocypher.org/) | 4.x | [CC0-1.0](https://creativecommons.org/publicdomain/zero/1.0/) (Public Domain) | XChaCha20-Poly1305 + Argon2id |
| [usearch](https://github.com/unum-cloud/usearch) | ≥ 2.9 | [Apache 2.0](https://github.com/unum-cloud/usearch/blob/main/LICENSE) | HNSW vector index (optional — `pip install snkv[vector]`) |

SQLite and Monocypher are statically compiled into `libsnkv` and `snkv.h`. No dynamic linking or separate installation is required.

SQLite and Monocypher are public domain — no attribution is legally required, but credit is given here in the spirit of good practice. usearch is an optional runtime dependency and is not bundled.

---

## License

Apache License 2.0 © 2025 Hash Anu
