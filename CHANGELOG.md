# Changelog

## 0.9.0 — unreleased

### Breaking changes

- **`KVStoreConfig.fullMutex` removed.** Every handle now always has one lock,
  so a `KVStore*` can always be shared between threads. Code that sets
  `cfg.fullMutex` no longer compiles: delete the line.
- **`KVStoreConfig.walSizeLimit` removed.** It checkpointed every N commits.
  Use `walAutoCheckpoint` instead, which counts WAL pages and is on by
  default (1000 pages, like SQLite).
- The size of `KVStoreConfig` changed. Rebuild everything that uses it.
- The `python_bindings` branch still uses `full_mutex` / `wal_size_limit` and
  has to be updated before it can move to this version.

### Threading: transactions now belong to one thread

`kvstore_begin(kv, 1)` keeps the handle locked until `kvstore_commit` or
`kvstore_rollback`. Other threads' calls on that handle **wait** until then.
Before, they ran inside the open transaction, so one thread's rollback could
undo another thread's writes.

Two patterns now deadlock and must be changed:

- Thread A calls `kvstore_begin`, then waits for thread B to write through
  the same handle. B waits for A's commit, and A waits for B.
- Thread A calls `kvstore_begin` and thread B calls `kvstore_commit` /
  `kvstore_rollback`. B waits for the lock that A holds.

Commit or roll back on the thread that called begin. To let several threads
write at the same time, give each its own handle: WAL lets one writer run
alongside many readers, and a second writer gets `KVSTORE_BUSY` (or waits up
to `busyTimeout`).

### Added

- `make fuzz` (clang + libFuzzer, Linux / macOS): `fuzz_api` runs random API
  sequences (puts, deletes, TTL, transactions, scans, writes during scans,
  reopen) against an in-memory model; `fuzz_open` opens and uses damaged
  database files (seeded from real stores, including encrypted ones).
  `make fuzz-regress` replays every input that once found a bug. CI runs a
  short fuzz on every PR and 30 minutes per fuzzer nightly, keeping the
  corpus between runs.
- `make test-powerloss`: simulated power loss at every write and sync
  (unsynced writes lost, kept, or torn per 512-byte sector), for WAL and
  DELETE journals, sync FULL and NORMAL, and encrypted stores through
  re-key and decryption. After every crash the store must open, pass the
  integrity check and hold exactly a committed state. A self-check proves
  the harness catches a disk that ignores fsync.
- `kvstore_errmsg_copy()`: copies the last error message under the handle
  lock. Use it instead of `kvstore_errmsg()` when threads share a handle.
- `KVSTORE_VERSION`, `KVSTORE_VERSION_MAJOR` / `_MINOR` / `_PATCH`,
  `KVSTORE_VERSION_NUMBER`, `kvstore_libversion()` and
  `kvstore_libversion_number()`.
- `KVStoreConfig.walAutoCheckpoint` and the build-time default
  `KVSTORE_DEFAULT_WAL_AUTOCHECKPOINT`.
- `cacheSize < 0` sets the page cache as a memory budget in KiB.
- Build-time Argon2 cost settings (`SNKV_ENC_M_COST` and limits) for small
  devices.

### Fixed

- Found by the new fuzzers (`make fuzz`):
  - Rolling back a transaction that held a column family's first TTL write
    left the handle pointing at the TTL index tables the rollback removed:
    every later `put_ttl`, `get_ttl`, `purge_expired` and `cf_clear` on it
    returned `KVSTORE_CORRUPT` until the store was reopened. TTL index
    tables created in a rolled-back transaction are now dropped from the
    handle; tables that existed before are kept.
  - 4-byte lengths in stored records were decoded with a signed shift (`b[0]
    << 24`): undefined behaviour for a top byte >= 0x80, and in practice a
    negative length. On a damaged file `get` and iterator `value` used it to
    size the value (one damaged byte caused a 1.6 GB allocation), and the
    integrity check and column-family walk read root pages from the wrong
    place. All 23 decodes now use one unsigned helper that rejects lengths
    that do not fit, and the sites that use them check for that.
  - Opening a damaged plain store with a password (encrypt-on-open) could
    loop forever: rewriting each value seeks past the last processed key,
    and in a B-tree with out-of-order keys the seek moved backwards,
    re-encrypting (and growing) the same rows endlessly. The rewrite now
    requires each key to be greater than the last and returns
    `KVSTORE_CORRUPT` otherwise.
- Vector store: after a power cut that followed a clean `kvstore_vec_close`,
  the index sidecar (`{db}.usearch`, written without fsync) could be torn
  while its stamp (`{db}.usearch.nid`) survived. Open accepted the torn
  file and the first search aborted the process. The stamp now also holds a
  checksum of the sidecar; a sidecar that does not match (torn, truncated,
  corrupt, or an old 8-byte stamp) is ignored and the index is rebuilt from
  the database.
- `kvstore_iterator_key` / `_value` after a write through the handle moved
  the iterator's row (for example, inserts that split its page) returned
  `KVSTORE_CORRUPT` on a healthy store: they read the B-tree cursor without
  restoring the position SQLite saved. They now restore it; if the current
  row was deleted, `value` returns `KVSTORE_NOTFOUND` and `next` / `prev`
  still continue from the right place.
- Thread safety: one lock per handle (the per-column-family locks are gone).
  Iterator calls and closing a column family or iterator now run under the
  handle lock. They used to read and release pages of the shared page cache
  without it.
- Vector store: `get`, `get_vector`, `get_metadata`, `kv_put`, `contains` and
  `count` now take the store's mutex, and the "index dropped" check runs under
  it (it raced with `drop_index`).
- `kvstore_open` no longer fails when SQLite mutexes are switched off at run
  time (`SQLITE_CONFIG_SINGLETHREAD`).
- A leftover half of a column family's TTL index is emptied on open instead
  of dropped (a drop could move the table of the column family being opened,
  and cleanup was skipped while an iterator was open).
- Creating an internal (vector store) column family inside `kvstore_begin` no
  longer commits the caller's transaction.
- Re-encryption fails instead of continuing if random nonce generation fails.
- `purge_expired` reports delete errors and counts only committed deletes.
- `kvstore_incremental_vacuum` works while iterators are open.
- `kvstore_sync` fsyncs; drops and rollbacks keep open handles correct;
  failed internal commits roll back; TTL expiry after a rollback; a crash on
  corrupt keys; key-only lookups for large values.
- WAL growth: SNKV never installed SQLite's WAL auto-checkpoint, so the WAL
  grew to the size of the database and slowed reads by 8–16%.

### Performance

- Iterators now lock the handle (in 0.8.2 they took no lock, which was the
  unsafe part), but only when they move: `first` / `next` / `last` / `prev` /
  `seek` copy the current row, and `key` / `value` return the copy without
  locking. One lock per row instead of three. A full key+value scan is 1.78x
  faster than with a lock on every call, and 1.73x faster than SQLite
  (mean of 5 alternating runs, 1M rows on tmpfs: 5.9 → 10.6 M rows/s; SQLite
  6.1 M rows/s). Point reads and writes are unchanged. Builds that never use
  threads can compile with `-DSQLITE_THREADSAFE=0` to remove all locking.
- Values up to 1 KB (`KVSTORE_ITER_PREFETCH_MAX`) are copied when the
  iterator moves; larger and encrypted values are still read when
  `kvstore_iterator_value` is called. So a write to the current key made
  after the move is not seen by `kvstore_iterator_value` for small values
  until the iterator moves again. The pointers from `key` / `value` stay
  valid until the iterator moves or is closed.

### Removed

- Python bindings moved from `master` to the `python_bindings` branch.
