<p align="center">
  <img src="snkv_logo.png" alt="SNKV Logo" width="220">
</p>

<h1 align="center">SNKV</h1>
<p align="center">A simple, crash-safe embedded key-value store</p>

[![Build](https://github.com/hash-anu/snkv/actions/workflows/c-cpp.yml/badge.svg)](https://github.com/hash-anu/snkv/actions/workflows/c-cpp.yml)
[![Memory Leaks](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/hash-anu/snkv/badges/valgrind.json)](https://github.com/hash-anu/snkv/actions/workflows/c-cpp.yml)
[![Tests](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/hash-anu/snkv/badges/tests.json)](https://github.com/hash-anu/snkv/actions/workflows/c-cpp.yml)
[![Discord](https://img.shields.io/badge/Discord-Join%20Server-5865F2?logo=discord&logoColor=white)](https://discord.gg/EUb4Y5qE)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/hash-anu/snkv/blob/master/LICENSE)

**SNKV** is a lightweight, ACID-compliant embedded key-value store written in C. It talks directly to SQLite's B-tree storage engine and skips the SQL layer: no parser, no query planner, no VM.

```
SQLite:  Application → SQL Parser → Query Planner → VDBE → B-Tree → Disk
SNKV:    Application → KV API → B-Tree → Disk
```

> **Looking for Python?** The Python bindings are no longer maintained on `master`.
> Use the [`python_bindings`](https://github.com/hash-anu/snkv/tree/python_bindings) branch.

---

## Quick Start

Single-header integration:

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

- **API reference:** [hash-anu.github.io/snkv/api.html](https://hash-anu.github.io/snkv/api.html)
- **Examples:** [examples/](examples/) · [hash-anu.github.io/snkv/examples.html](https://hash-anu.github.io/snkv/examples.html)

---

## Features

- **ACID transactions** and **WAL mode** (concurrent readers, single writer)
- **Native TTL**: per-key expiry, lazy eviction, bulk `kvstore_purge_expired()`. No background thread.
- **Encryption**: per-value XChaCha20-Poly1305 with Argon2id key derivation, transparent to all APIs
- **Column families**: logical namespaces within one database
- **Iterators**: forward and reverse, prefix, and seek
- **Vector search**: HNSW index via [usearch](https://github.com/unum-cloud/usearch) (`make vector`), stored in the same `.db` file. See [examples/vector.c](examples/vector.c).
- **Thread-safe**, **single-header**, **Valgrind-clean**
- **Compatible with SQLite page/WAL-level tools** such as LiteFS. Tools that need SQLite's schema layer (the `sqlite3` CLI) do not work.

```c
/* TTL */
kvstore_put_ttl(db, "session", 7, "tok123", 6, kvstore_now_ms() + 60000);

/* Encryption */
kvstore_open_encrypted("secure.db", "hunter2", 7, &db, NULL);

/* Custom configuration */
KVStoreConfig cfg = {0};                  /* zero fields are NOT defaults: set journalMode */
cfg.journalMode = KVSTORE_JOURNAL_WAL;
cfg.syncLevel   = KVSTORE_SYNC_FULL;
cfg.busyTimeout = 5000;
kvstore_open_v2("mydb.db", &db, &cfg);
```

---

## Build

**Linux / macOS**

```bash
make                  # libsnkv.a
make snkv.h           # single-header version
make examples         # build examples
make test             # run test suite
make vector           # libsnkv_vec.a (core + usearch, requires g++)
make test-vector      # run vector tests
```

**Windows**: use the **MSYS2 MinGW 64-bit** shell (not cmd.exe or PowerShell):

```bash
pacman -S --needed mingw-w64-x86_64-gcc make
make && make test
```

---

## Benchmarks

1M records on Linux. Both stores use WAL, `synchronous=NORMAL`, an 8 MB cache and 4 KB pages; SQLite uses a `WITHOUT ROWID` table.
Source: [SNKV](tests/test_benchmark.c) · [SQLite](https://github.com/hash-anu/sqllite-benchmark-kv)

| Benchmark         | SQLite      | SNKV        | Speedup |
| ----------------- | ----------- | ----------- | ------- |
| Sequential writes | 142K ops/s  | 232K ops/s  | 1.64x   |
| Random reads      | 90K ops/s   | 160K ops/s  | 1.77x   |
| Sequential scan   | 1.56M ops/s | 2.89M ops/s | 1.85x   |
| Random updates    | 16K ops/s   | 31K ops/s   | 1.9x    |
| Random deletes    | 16K ops/s   | 31K ops/s   | ~2x     |
| Mixed workload    | 34K ops/s   | 62K ops/s   | 1.79x   |

Comparisons against [LMDB](https://github.com/hash-anu/lmdb-benchmark) and [RocksDB](https://github.com/hash-anu/rocksdb-benchmark) are available too.

**Good fit:** read-heavy or mixed workloads, native TTL (sessions, caches, leases), embedded or memory-constrained targets, and simple C integration.
**Consider alternatives:** RocksDB for maximum write throughput, LMDB for maximum read speed when memory is plentiful.

---

## Third-Party Licenses

| Library | License | Use |
|---------|---------|-----|
| [SQLite](https://www.sqlite.org/) | Public Domain | B-tree, pager, WAL, OS layer (bundled) |
| [Monocypher](https://monocypher.org/) | CC0-1.0 | XChaCha20-Poly1305 + Argon2id (bundled) |
| [usearch](https://github.com/unum-cloud/usearch) | Apache 2.0 | HNSW vector index (optional, `make vector`) |

## License

Apache License 2.0 © 2025 Hash Anu
