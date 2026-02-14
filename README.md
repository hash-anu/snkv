# SNKV — B-tree based Key Value store

## Overview

**SNKV** is a lightweight, high‑performance **ACID compliant key–value store** built directly on top of the **SQLite B‑Tree layer**.

Instead of using SQLite through SQL queries, SNKV **bypasses the entire SQL processing stack** and directly invokes SQLite's **production‑ready B‑Tree APIs** to perform key–value operations.

The result is a database that retains **SQLite's proven reliability and durability**, while being **~60% faster for mixed KV workloads (70% read, 20% write, 10% delete operations)** due to dramatically reduced overhead.

---

## Design Philosophy

SQLite is an excellent general‑purpose database, but for key–value workloads it carries significant overhead:

* SQL parsing and compilation
* Virtual machine execution (VDBE)
* Query optimization and schema management

SNKV removes these layers entirely and keeps only what is essential for a KV store.

## Building

SNKV can be used in two ways: as a **static library** or as a **single-header file**.

```bash
make              # builds libsnkv.a (static library)
make snkv.h       # generates single-header amalgamation
make examples     # builds example programs (uses snkv.h)
make run-examples # builds and runs all examples
make test         # runs all test suites
make clean        # removes all build artifacts
```

## Usage

SNKV exposes a simple C API for key–value operations without any SQL involvement.

### Option 1: Single-header file (recommended for new projects)

Just drop `snkv.h` into your project — no library to link, no include paths to configure.

```c
#define SNKV_IMPLEMENTATION
#include "snkv.h"

int main(void) {
    KVStore *pKV;
    kvstore_open("mydb.db", &pKV, 0, KVSTORE_JOURNAL_WAL);

    kvstore_put(pKV, "key", 3, "value", 5);

    void *pValue; int nValue;
    kvstore_get(pKV, "key", 3, &pValue, &nValue);
    printf("%.*s\n", nValue, (char*)pValue);
    snkv_free(pValue);

    kvstore_close(pKV);
    return 0;
}
```

Compile (no library needed):

```bash
gcc -o myapp myapp.c                          # Linux/macOS
gcc -o myapp.exe myapp.c                      # Windows/MSYS
```

`#define SNKV_IMPLEMENTATION` must appear in exactly **one** `.c` file. Other `.c` files can `#include "snkv.h"` without the define to get just the API declarations.

**C++ projects** — compile the implementation as C, use the API from C++:

```c
// snkv_impl.c (compile with gcc)
#define SNKV_IMPLEMENTATION
#include "snkv.h"
```

```cpp
// myapp.cpp (compile with g++)
#include "snkv.h"
int main() { /* use kvstore_open, kvstore_put, etc. */ }
```

```bash
gcc -c snkv_impl.c && g++ myapp.cpp snkv_impl.o -o myapp
```

### Option 2: Static library

```c
#include "kvstore.h"

int main(void) {
    KVStore *pKV;
    kvstore_open("mydb.db", &pKV, 0, KVSTORE_JOURNAL_WAL);

    kvstore_put(pKV, "key", 3, "value", 5);

    void *pValue; int nValue;
    kvstore_get(pKV, "key", 3, &pValue, &nValue);
    printf("%.*s\n", nValue, (char*)pValue);
    sqliteFree(pValue);

    kvstore_close(pKV);
    return 0;
}
```

Compile against the library:

```bash
gcc -Iinclude -o myapp myapp.c libsnkv.a
```

Standalone examples are provided in the `examples/` directory:

| Example | Covers |
|---|---|
| `examples/basic.c` | Hello world, CRUD, existence checks |
| `examples/transactions.c` | Atomic batch operations, rollback |
| `examples/column_families.c` | Data organization with column families |
| `examples/iterators.c` | Scanning, filtered iteration, statistics |
| `examples/session_store.c` | Real-world session management |
| `examples/benchmark.c` | Auto-commit vs batch transaction performance |

See [kvstore_example.md](kvstore_example.md) for the full API usage guide.

## Features

* **ACID transactions** — begin, commit, rollback with full durability
* **WAL mode** — Write-Ahead Logging for concurrent readers + writer
* **Column families** — multiple logical namespaces in a single database
* **Iterators** — ordered key-value traversal with cursor-based scanning
* **Thread safety** — mutex-protected operations for concurrent access
* **Zero memory leaks** — verified with Valgrind memcheck

## Tests

All unit tests and benchmarks are located in the `tests/` directory (8 test suites, 100+ tests).

## Visual Architecture Comparison

### The Stack We Removed

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         The Stack We REMOVED                                │
└─────────────────────────────────────────────────────────────────────────────┘

        REMOVED from SQLite                 Why It's Not Needed for SNKV
        ═══════════════════════             ═══════════════════════════

┌──────────────────────────┐
│   SQL Interface Layer     │                No SQL → Not required
│   - sqlite3_prepare()    │
│   - sqlite3_step()       │
│   - sqlite3_bind_*()     │
└──────────────────────────┘
           │
           ▼
┌──────────────────────────┐
│     SQL Compiler         │                No SQL → No parsing or codegen
│   - Tokenizer            │
│   - Parser               │
│   - Code Generator       │
└──────────────────────────┘
           │
           ▼
┌──────────────────────────┐
│   Virtual Machine        │                No bytecode execution
│   - VDBE Executor        │
│   - Opcode Interpreter   │
│   - 200+ opcodes         │
└──────────────────────────┘
           │
           ▼
┌──────────────────────────┐
│    Backend Layer         │                No schema or query planning
│   - Query Optimizer      │
│   - Index Manager        │
│   - Schema Manager       │
└──────────────────────────┘
           │
           ▼
    ═══════════════════════════════════════════════════
```

---

## What SNKV Keeps (Unchanged)

SNKV intentionally keeps the **most battle‑tested parts of SQLite**, unchanged:

```
┌──────────────────────────┐                ┌──────────────────────────┐
│      B-Tree Engine       │   ══════════▶  │      B-Tree Engine       │
│   (SQLite proven code)   │   KEPT THIS    │   (Same proven code)     │
└──────────────────────────┘                └──────────────────────────┘
           │                                             │
           ▼                                             ▼
┌──────────────────────────┐                ┌──────────────────────────┐
│      Pager Module        │   ══════════▶  │      Pager Module        │
│   (Cache, Journaling)    │   KEPT THIS    │   (Same code)            │
└──────────────────────────┘                └──────────────────────────┘
           │                                             │
           ▼                                             ▼
┌──────────────────────────┐                ┌──────────────────────────┐
│      OS Interface        │   ══════════▶  │      OS Interface        │
│   (File I/O, Locks)      │   KEPT THIS    │   (Same code)            │
└──────────────────────────┘                └──────────────────────────┘
```

This means SNKV benefits from:

* Crash safety (rollback journal and WAL)
* Atomic commits
* Page cache and efficient I/O
* Write-Ahead Logging for concurrent access
* Real‑world tested

## Clean Architecture Diagram


                            ┌────────────────┐
                            │  Application   │
                            └────────┬───────┘
                                     │
                                     │ kvstore_put(key, value)
                                     │ kvstore_get(key) → value
                                     │ kvstore_delete(key)
                                     │ kvstore_begin(), kvstore_commit(), kvstore_rollback(), ...
                                     ▼
                    ┌────────────────────────────────┐
                    │        KVStore Layer           │
                    │   (Thin Wrapper - ~2400 LOC)   │
                    │                                │
                    │  • Simple API                  │
                    │  • Column Families             │
                    │  • Thread Safety (Mutexes)     │
                    │  • Validation                  │
                    │  • Statistics                  │
                    └────────────┬───────────────────┘
                                 │
                                 │ Direct calls (no SQL!)
                                 │
                                 ▼
                    ┌────────────────────────────────┐
                    │       B-Tree Engine            │
                    │  (SQLite v3.51.200 - Proven Code)  │
                    │                                │
                    │  • Tree Operations             │
                    │  • Key-Value Storage           │
                    │  • Cursors & Navigation        │
                    └────────────┬───────────────────┘
                                 │
                                 │
                                 ▼
                    ┌────────────────────────────────┐
                    │       Pager Module             │
                    │  (SQLite v3.51.200 - Proven Code)  │
                    │                                │
                    │  • Transaction Management      │
                    │  • Rollback Journal            │
                    │  • ACID Guarantees             │
                    └────────────┬───────────────────┘
                                 │
                                 │
                                 ▼
                    ┌────────────────────────────────┐
                    │       OS Interface             │
                    │  (SQLite v3.51.200 - Proven Code)  │
                    │                                │
                    │  • File I/O                    │
                    │  • File Locking                │
                    │  • Crash Recovery              │
                    └────────────┬───────────────────┘
                                 │
                                 │
                                 ▼
                         ┌──────────────┐
                         ┌──────────────┐
                         │  Disk Files  │
                         │              │
                         │  • kvstore.db│
                         │  • -wal      │
                         │  • -shm      │
                         │  • -journal  │
                         └──────────────┘

## Result

**Same reliability, fewer layers, significantly faster KV performance.**

* No SQL parsing or planning
* No virtual machine execution
* Direct B‑Tree access

Typical gains:

* Lower memory usage
* Predictable latency

---

## Comparison with SQLite v3.51.200
The following table shows the **average performance across 5 runs** for both **SQLite v3.51.200 (SQL-based KV access)** and **SNKV (direct B-Tree KV access)** using identical workloads (50,000 records).

All numbers are **operations per second (ops/sec)**.

| Benchmark                    | SQLite (avg) | SNKV (avg) | Winner          |
| ---------------------------- | ------------ | ---------- | --------------- |
| Sequential Writes            | 146,727      | 128,310    | SQLite (+14%)   |
| Random Reads                 | 173,863      | 219,050    | SNKV (+26%)     |
| Sequential Scan              | 2,138,485    | 3,025,534  | SNKV (\~1.4×)   |
| Random Updates               | 116,026      | 111,054    | Tie             |
| Random Deletes               | 62,728       | 105,890    | SNKV (+69%)     |
| Exists Checks                | 263,348      | 227,897    | SQLite (+16%)   |
| Mixed Workload (70R/20W/10D) | 129,999      | 210,916    | **SNKV (+62%)** |
| Bulk Insert (single txn)     | 245,598      | 269,433    | SNKV (+10%)     |

### Key Observations

- **SNKV dominates mixed and delete-heavy workloads** due to zero SQL/VDBE overhead
- **Random reads are 26% faster** in SNKV — no SQL parsing or statement preparation per lookup
- **Sequential scans are \~1.4× faster** in SNKV due to direct cursor traversal
- **Deletes are 69% faster** — the biggest single-operation win for SNKV
- **SQLite wins sequential writes and exists checks** due to its optimized prepared-statement caching
- Update performance is effectively identical (same B-Tree + Pager path)

### Benchmark Code

- SQLite benchmark source: [https://github.com/hash-anu/sqlite-benchmark-kv](https://github.com/hash-anu/sqlite-benchmark-kv)
- SNKV benchmark source: `tests/test_benchmark.c`

## Platform Support

SNKV works on **Linux**, **macOS**, **Windows** (MSYS2/MinGW/Cygwin), and other Unix-like systems. Both **C** and **C++** are supported.

The single-header `snkv.h` contains platform-specific code for all targets — the compiler automatically selects the right code path via `SQLITE_OS_UNIX` / `SQLITE_OS_WIN` macros.

## When to Use SNKV

SNKV is ideal for:

* Embedded systems
* Low‑memory environments
* Configuration stores
* Metadata databases
* C/C++ applications needing fast KV access
* Projects that want a **single-file dependency** (just drop in `snkv.h`)
* Systems that do **not** need SQL
* Many more

If you need joins, ad‑hoc queries, or analytics — use SQLite.
If you need **fast, reliable key–value storage** — use SNKV.

---

## Summary

SNKV proves a simple idea:

> *If you don’t need SQL, don’t pay for it.*

By standing directly on SQLite’s B‑Tree engine, SNKV delivers a focused, fast, and reliable key–value database with minimal complexity.


## License

Copyright © 2025 Hash Anu

Licensed under the **Apache License, Version 2.0** (the "License");
you may not use this project except in compliance with the License.

You may obtain a copy of the License at:

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an **"AS IS" BASIS**,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
