
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
[![Discord](https://img.shields.io/badge/Discord-Join%20Server-5865F2?logo=discord&logoColor=white)](https://discord.gg/EUb4Y5qE)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/hash-anu/snkv/blob/master/LICENSE)

---

## What is SNKV?

**SNKV** is a lightweight, **ACID-compliant embedded key-value store** built directly on SQLite's storage engine — without SQL layer.

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

## Vector Search (C API)

SNKV includes a native C vector search layer built on [usearch](https://github.com/unum-cloud/usearch)'s HNSW index. Vectors and KV data share the same `.db` file. The index is rebuilt from the database on open and optionally saved to a `.usearch` sidecar for fast reload.

```c
#include "kvstore_vec.h"

/* Open (or create) a 128-dim cosine store */
KVVecStore *vs = NULL;
kvstore_vec_open("store.db", 128, KVVEC_SPACE_COSINE,
                 0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs);

/* Insert key + value + vector (+ optional JSON metadata) */
float vec[128] = { /* ... */ };
kvstore_vec_put(vs, "doc:1", 5, "hello world", 11,
                vec, 0, "{\"tag\":\"ai\"}", 12);

/* Approximate nearest-neighbour search */
KVVecSearchResult *res = NULL; int n = 0;
kvstore_vec_search(vs, query, /*top_k=*/5, /*rerank=*/0,
                   /*oversample=*/0, /*max_dist=*/0.0f, &res, &n);
for (int i = 0; i < n; i++)
    printf("%.*s  dist=%.4f\n", res[i].nKey, (char*)res[i].pKey, res[i].distance);
kvstore_vec_free_results(res, n);

kvstore_vec_close(vs);
```

### Distance spaces

| Constant | Description |
|----------|-------------|
| `KVVEC_SPACE_L2` | Squared Euclidean (‖a−b‖²) — **not** sqrt; distances are comparable but not metric L2 |
| `KVVEC_SPACE_COSINE` | Cosine distance (1 − dot(a,b) / (‖a‖·‖b‖)) |
| `KVVEC_SPACE_IP` | Inner product (negative dot product) |

### Index precision

| Constant | RAM usage | Notes |
|----------|-----------|-------|
| `KVVEC_DTYPE_F32` | Full | Default |
| `KVVEC_DTYPE_F16` | Half | Negligible recall loss |
| `KVVEC_DTYPE_I8` | Quarter | Cosine-like metrics only |

### Key features

- **Atomic writes** — every `put` is one KVStore transaction (5 internal CFs); usearch updated after commit
- **Exact rerank** — pass `rerank=1` to fetch `oversample×top_k` candidates and re-score with exact float32 distances
- **TTL** — pass `expire_ms > 0` to `kvstore_vec_put`; expired vectors are lazily evicted on search/get and bulk-removable via `kvstore_vec_purge_expired`
- **Sidecar persistence** — `close` saves the HNSW graph to `{path}.usearch`; `open` loads it in O(1) instead of an O(n·dim) rebuild (disabled for encrypted stores)
- **Encryption** — pass a password to `kvstore_vec_open`; values and vectors are encrypted, sidecar is disabled
- **Batch inserts** — `kvstore_vec_put_batch` writes N items in one atomic transaction

### Build

The vector layer requires g++ to compile the usearch C++ core:

```bash
make vector               # builds libsnkv_vec.a  (core + usearch)
make vector-examples      # compiles examples/vector.c
make run-vector-examples  # runs the example
make test-vector          # runs the test suite
```

See [examples/vector.c](examples/vector.c) for a complete walkthrough of every API, and [api.html](https://hash-anu.github.io/snkv/api.html#c-vec) for the full C reference.

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
make                      # builds libsnkv.a (pure gcc, no C++)
make snkv.h               # generates single-header version
make examples             # builds examples
make run-examples         # run all examples
make test                 # run all tests (CI suite)

make vector               # builds libsnkv_vec.a (core + usearch, requires g++)
make vector-examples      # builds examples/vector
make run-vector-examples  # run the vector example
make test-vector          # run the vector test suite

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
pip install snkv           # KV store, TTL, encryption, column families
pip install snkv[vector]   # + HNSW vector search (usearch + numpy)
```

```python
from snkv import KVStore

with KVStore("mydb.db") as db:
    db["hello"] = "world"
    print(db["hello"].decode())   # world
```

**Vector search** — integrated HNSW approximate nearest-neighbour index backed by [usearch](https://github.com/unum-cloud/usearch). Vectors and KV data share the same `.db` file. Supports metadata filtering, exact rerank, TTL on vectors, quantization (f32/f16/i8), sidecar index persistence, and encryption. Available in both C and Python.

```python
from snkv.vector import VectorStore
import numpy as np

with VectorStore("store.db", dim=128, space="cosine") as vs:
    vs.vector_put(b"doc:1", b"hello world", np.random.rand(128).astype("f4"))
    results = vs.search(np.random.rand(128).astype("f4"), top_k=5)
    for r in results:
        print(r.key, r.distance, r.value)
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
- **Python Bindings** — idiomatic Python 3.8+ API with dict-style access, TTL, encryption, column families, iterators, and typed exceptions — see [python/README.md](python/README.md)
- **Vector Search (C)** — native HNSW index via `make vector`; ANN search, exact rerank, TTL, sidecar persistence, encryption, batch insert — see [examples/vector.c](examples/vector.c)
- **Vector Search (Python)** — `pip install snkv[vector]`; metadata filtering, exact rerank, TTL on vectors, quantization (f32/f16/i8), sidecar persistence — see [python/README.md#vector-search](python/README.md#vector-search)
- **Can be cross compiled for Android devices** - snkv is embedded database with vector support which has all required features so it can be cross compiled for Android platforms
- **Ideal for AI workloads** - SNKV built solely on top of battle tested b-tree engine of sqlite, additionally by default read transaction is enabled, so read operations are faster as well as since it bypass sql layer it removed overhead of row metadata write operations and directly write data to b-tree, Which improves 1.5x speed improvement in mixed workloads.

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
| [usearch](https://github.com/unum-cloud/usearch) | ≥ 2.9 | [Apache 2.0](https://github.com/unum-cloud/usearch/blob/main/LICENSE) | HNSW vector index (optional — C: `make vector`, Python: `pip install snkv[vector]`) |

SQLite and Monocypher are statically compiled into `libsnkv` and `snkv.h`. No dynamic linking or separate installation is required.

SQLite and Monocypher are public domain — no attribution is legally required, but credit is given here in the spirit of good practice. usearch is an optional runtime dependency and is not bundled.

---

## Hire Me

If you're reading this, you're probably dealing with one of these:

- Your SQLite queries are too slow for a pure KV workload and you don't know where to start optimizing
- You need an embedded store on Android/iOS/edge but can't ship a heavy dependency
- You're building an AI pipeline and need fast local vector + KV storage without standing up a separate service
- You have a C/C++ performance problem and need someone who actually reads storage engine source code

I built SNKV by going deep into SQLite's B-tree and pager internals — the kind of work most engineers avoid. That's the level I operate at.

**I can help you with:**
- Diagnosing and fixing storage/performance bottlenecks in C, C++, or Python
- Integrating or adapting SNKV (or SQLite) into your embedded / mobile / edge product
- Building Python native extensions that wrap C libraries cleanly
- Setting up vector search for AI workloads without the infrastructure overhead
- Cross-compiling native libraries for Android or other constrained targets

**Availability:** ~3 hrs/day · Remote · Flexible rate — reach out and we'll find something that works

**Contact:** [hashmak.jsn@gmail.com](mailto:hashmak.jsn@gmail.com) · [GitHub](https://github.com/hash-anu)

---

## Support This Project

SNKV is free and open-source. If it's been useful to you — or you just want to support independent systems software — any amount helps keep the work going.

[![GitHub Sponsors](https://img.shields.io/badge/Sponsor-GitHub-ea4aaa?logo=github-sponsors&logoColor=white)](https://github.com/sponsors/hash-anu)

Even a one-time coffee keeps things moving — thank you.

---

## License

Apache License 2.0 © 2025 Hash Anu
