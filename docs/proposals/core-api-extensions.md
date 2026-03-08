# Proposal: Core API Extensions — Seek, Put-If-Absent, Clear, Stats, Count

**Status:** Accepted
**Author:** SNKV maintainers
**Created:** 2026-03-07
**Target version:** 0.5.0

---

## Table of Contents

- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Feature 1 — kvstore_iterator_seek](#feature-1--kvstore_iterator_seek)
- [Feature 2 — kvstore_put_if_absent](#feature-2--kvstore_put_if_absent)
- [Feature 3 — kvstore_clear / kvstore_cf_clear](#feature-3--kvstore_clear--kvstore_cf_clear)
- [Feature 4 — Extended KVStoreStats](#feature-4--extended-kvstorestats)
- [Feature 5 — kvstore_count / kvstore_cf_count](#feature-5--kvstore_count--kvstore_cf_count)
- [Production Readiness Review](#production-readiness-review)
- [Files Changed](#files-changed)
- [Test Plan](#test-plan)
- [Backward Compatibility](#backward-compatibility)

---

## Motivation

SNKV provides solid CRUD, column families, TTL, and iterator primitives. However, several
fundamental operations present in every production KV store (LevelDB, RocksDB, LMDB) are
missing or cannot be emulated efficiently with the existing API:

- **Range scans** require positioning an iterator at an arbitrary key (`seek`). The existing
  prefix iterator only covers the prefix-match case; arbitrary `[start, end)` ranges are
  impossible without an O(N) full scan from the beginning.

- **Atomic conditional writes** (`put_if_absent`) currently require four manual steps
  (`begin → exists → put → commit`) and are easy to get wrong in concurrent code.

- **Clearing all keys** from a CF (especially the default CF, which cannot be dropped)
  requires an O(N) iterate-and-delete loop. The underlying B-tree supports O(pages) bulk
  clear via `sqlite3BtreeClearTable` but this is not exposed.

- **Observability** is limited to five counters. Cache behaviour, WAL health, byte
  throughput, TTL activity, and storage usage are invisible to the operator.

- **Key count** requires a full O(N) key iteration today. `sqlite3BtreeCount` provides
  an O(pages) count that is much cheaper for large stores.

---

## Goals

1. `kvstore_iterator_seek` — position any iterator at first key `>= target` (forward) or `<= target` (reverse).
2. `kvstore_put_if_absent` — atomic conditional insert with optional TTL.
3. `kvstore_clear` / `kvstore_cf_clear` — O(pages) bulk delete including TTL indexes from any session.
4. Extended `KVStoreStats` — bytes, WAL, checkpoint, TTL, and storage counters + reset API.
5. `kvstore_count` / `kvstore_cf_count` — O(pages) entry count per CF.
6. Full Python binding for all new APIs.
7. Test coverage for all new APIs.

---

## Non-Goals

- `kvstore_key_size` — marginal utility; `kvstore_get` is the natural path.
- `kvstore_cf_rename` — no user demand yet; O(N) copy is acceptable workaround.
- Encryption at rest — separate proposal.
- Watch / change notifications — separate proposal.

---

## Feature 1 — `kvstore_iterator_seek`

### Problem

The only way to position an iterator at a specific key today is:
- `kvstore_iterator_first` — always starts at the smallest key
- `kvstore_prefix_iterator_create` — starts at the first key matching a prefix AND
  restricts all subsequent `next()` calls to that prefix

There is no way to say "start at key X, iterate freely until key Y". This blocks
range scans, pagination (resume from last seen key), time-series queries, and any
workload where the scan boundary is not a key prefix.

### API

```c
/*
** Position the iterator at a target key.
**
** Forward iterator (reverse=0): positions at first key >= (pKey, nKey).
** Reverse iterator (reverse=1): positions at last  key <= (pKey, nKey).
**
** Works on any iterator type — plain, prefix, reverse, reverse-prefix.
** If the iterator has a prefix filter, the seeked position is also validated
** against the prefix: if the result falls outside the prefix range, eof is
** set immediately.
**
** After seek, kvstore_iterator_next (forward) or kvstore_iterator_prev
** (reverse) continues from the seeked position as normal.
**
** Calling seek resets the iterator's started state — it is safe to seek
** multiple times on the same iterator without closing and recreating it.
**
** Thread safety: the iterator handle must be used by a single thread.
**
** Returns:
**   KVSTORE_OK    — positioned successfully (check kvstore_iterator_eof)
**   KVSTORE_ERROR — iterator not open or internal error
*/
int kvstore_iterator_seek(KVIterator *pIter, const void *pKey, int nKey);
```

No CF-level variant needed — seek operates on the iterator handle directly.

### Internal Design

```
kvstore_iterator_seek(pIter, pKey, nKey):

1. Check pIter->isValid; return KVSTORE_ERROR if not open.

2. Branch on pIter->reverse:

   FORWARD (reverse=0) — seek to first key >= target:
     sqlite3BtreeIndexMoveto(pCur, &idxKey, &res)
       res < 0  → cursor at key < target → sqlite3BtreeNext to advance
       res >= 0 → cursor at key >= target → done

   REVERSE (reverse=1) — seek to last key <= target:
     sqlite3BtreeIndexMoveto(pCur, &idxKey, &res)
       res > 0  → cursor at key > target → sqlite3BtreePrevious to retreat
       res <= 0 → cursor at key <= target → done

3. Set pIter->eof = sqlite3BtreeEof(pCur).

4. If pIter->pPrefix set: call kvstoreIterCheckPrefix.
   If prefix does not match → pIter->eof = 1.

5. Call kvstoreIterSkipExpired (forward) or kvstoreIterSkipExpiredReverse
   (reverse) to handle lazy TTL expiry at the seeked position.

6. Return KVSTORE_OK.
```

This is distinct from `kvstoreSeekAfter` (strictly `>`) and `kvstoreSeekBefore`
(strictly `<`). Forward seek is `>=`; reverse seek is `<=`.

### Python — flag reset requirement

`IteratorObject` tracks `started` and `needs_first` flags used by `__next__`.
After `seek()` in the C binding:
- Set `self->started = 1`
- Set `self->needs_first = 0`

This prevents the next `__next__` call from overriding the seeked position by
calling `first()` or `last()` again.

### Usage Example

```c
/* Range scan: order:1000 to order:2000 (inclusive) */
KVIterator *iter;
kvstore_iterator_create(db, &iter);
kvstore_iterator_seek(iter, "order:1000", 10);
while (!kvstore_iterator_eof(iter)) {
    void *key; int nKey;
    kvstore_iterator_key(iter, &key, &nKey);
    if (nKey < 6 || memcmp(key, "order:", 6) != 0) break;
    if (nKey > 10 || memcmp(key, "order:2000", 10) > 0) break;
    /* process entry */
    kvstore_iterator_next(iter);
}
kvstore_iterator_close(iter);

/* Reverse seek: find latest key <= "ts:1000" */
KVIterator *rev;
kvstore_reverse_iterator_create(db, &rev);
kvstore_iterator_seek(rev, "ts:1000", 7);  /* positions at <= "ts:1000" */
if (!kvstore_iterator_eof(rev)) {
    /* rev is at the largest key <= "ts:1000" */
}
kvstore_iterator_close(rev);
```

### Python

```python
# Forward range scan
it = db.iterator()
it.seek(b"order:1000")
while not it.eof:
    if it.key > b"order:2000":
        break
    print(it.key, it.value)
    it.next()

# Reverse seek — last key <= target
it = db.reverse_iterator()
it.seek(b"ts:1000")   # positions at <= b"ts:1000"
if not it.eof:
    print(it.key, it.value)
```

---

## Feature 2 — `kvstore_put_if_absent`

### Problem

Atomic conditional insert (insert only if key does not already exist) requires four
manual steps today:

```c
kvstore_begin(db, 1);
kvstore_exists(db, key, nKey, &exists);
if (!exists) kvstore_put(db, key, nKey, val, nVal);
kvstore_commit(db);
```

This is verbose, easy to get wrong (forgotten begin/commit, no TTL support), and a
common enough pattern that it warrants a dedicated primitive. Use cases:

- **Distributed locks** — acquire lock only if not already held; lock must auto-expire
- **Cache-aside** — populate cache only if key is absent
- **Idempotency keys** — record a key only on first occurrence, expire after window
- **Unique slot allocation** — claim a slot atomically

### API

```c
/*
** Insert (key, value) only if key does not already exist.
**
**   expire_ms > 0  — absolute expiry in ms (use kvstore_now_ms() + delta).
**                    Applied only when the key is newly inserted.
**                    Ignored if key already exists.
**   expire_ms == 0 — no TTL.
**
**   *pInserted (may be NULL):
**     1 — key was absent; value was written.
**     0 — key already existed; store unchanged.
**
** Expired keys are treated as absent: if the key exists but has expired,
** it is lazily deleted and the new value is inserted.
**
** Both the data write and the TTL entry are committed atomically in a
** single write transaction. If the caller is already inside an explicit
** write transaction (kvstore_begin with wrflag=1), the operation joins
** that transaction without issuing a nested commit.
**
** Returns KVSTORE_OK whether or not the key was inserted.
** Returns an error code only on I/O or locking failure.
*/
int kvstore_put_if_absent(
  KVStore *pKV,
  const void *pKey,   int nKey,
  const void *pValue, int nValue,
  int64_t expire_ms,
  int *pInserted          /* may be NULL */
);

int kvstore_cf_put_if_absent(
  KVColumnFamily *pCF,
  const void *pKey,   int nKey,
  const void *pValue, int nValue,
  int64_t expire_ms,
  int *pInserted          /* may be NULL */
);
```

### Internal Design

```
kvstore_cf_put_if_absent(pCF, pKey, nKey, pValue, nValue, expire_ms, pInserted):

1. sqlite3_mutex_enter(pKV->pMutex)

2. autoTrans = 0
   if pKV->inTrans != 2:          /* not already in a write transaction */
     if pKV->inTrans == 1:
       sqlite3BtreeCommit(pKV->pBt)   /* drop read transaction */
       pKV->inTrans = 0
     sqlite3BtreeBeginTrans(pKV->pBt, 1, 0)
     pKV->inTrans = 2
     autoTrans = 1
   /* else: join caller's existing write transaction, no autoTrans */

3. kvstore_cf_exists_internal(pCF, pKey, nKey, &exists)
   Note: kvstore_cf_exists_internal performs lazy TTL expiry —
   an expired key is deleted within the current write transaction
   and reports exists=0, so expired keys are transparently treated
   as absent.

4. if !exists:
     if expire_ms > 0:
       kvstore_cf_put_ttl_internal(pCF, pKey, nKey, pValue, nValue, expire_ms)
     else:
       kvstore_cf_put_internal(pCF, pKey, nKey, pValue, nValue)
     if pInserted: *pInserted = 1
   else:
     if pInserted: *pInserted = 0

5. if autoTrans:
     if rc == KVSTORE_OK:
       sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0
       kvstoreAutoCheckpoint(pKV)
       sqlite3BtreeBeginTrans(pKV->pBt, 0, 0); pKV->inTrans = 1
     else:
       sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0
       sqlite3BtreeBeginTrans(pKV->pBt, 0, 0); pKV->inTrans = 1

6. sqlite3_mutex_leave(pKV->pMutex)
```

Steps 3–4 are within a single write transaction — fully atomic. No TOCTOU race.
Correctly handles both autoTrans and caller-managed transaction paths.

### Usage Example

```c
/* Distributed lock with 30-second auto-expiry */
int inserted;
int64_t expire = kvstore_now_ms() + 30000;
kvstore_put_if_absent(db, "lock:res1", 9, "owner-A", 7, expire, &inserted);
if (inserted) {
    /* lock acquired — will auto-expire in 30s */
} else {
    /* lock held by another owner */
}

/* No TTL — permanent if inserted */
kvstore_put_if_absent(db, "config:init", 11, "done", 4, 0, NULL);
```

### Python

```python
# No TTL
inserted = db.put_if_absent(b"key", b"value")

# With TTL — 30 second lock
inserted = db.put_if_absent(b"lock:resource", b"owner-A", ttl=30.0)

# CF variant
inserted = cf.put_if_absent(b"slot:42", b"claimed", ttl=60.0)
```

`put_if_absent` returns `True` if inserted, `False` if key already existed.

---

## Feature 3 — `kvstore_clear` / `kvstore_cf_clear`

### Problem

There is no API to remove all keys from a CF. The only workaround is O(N)
iterate-and-delete, which is:

- Slow for large CFs — requires reading every key
- Two-pass — collect keys, then delete (can't delete while iterating)
- Not applicable to the default CF — cannot drop and recreate it

SQLite B-tree provides `sqlite3BtreeClearTable(pBt, iTable, pnChange)` which removes
all entries from a B-tree table in O(pages) without dropping the table structure.

### API

```c
/*
** Remove all key-value pairs from the default column family.
**
** Also clears all TTL index entries for this CF, even if the TTL index
** CFs were not opened in the current session (hasTtl=1 but pTtlKeyCF==NULL).
** The column family root page and metadata remain intact.
**
** Precondition: no iterators may be open on this CF when clear is called.
** Calling clear with open iterators produces undefined behaviour. Close all
** iterators before calling clear.
**
** Returns:
**   KVSTORE_OK on success, error code otherwise.
*/
int kvstore_clear(KVStore *pKV);

/*
** Remove all key-value pairs from the given column family.
** Equivalent to kvstore_clear() but operates on an explicit CF handle.
*/
int kvstore_cf_clear(KVColumnFamily *pCF);
```

### Internal Design

```
kvstore_cf_clear(pCF):

1. sqlite3_mutex_enter(pKV->pMutex) + sqlite3_mutex_enter(pCF->pMutex)

2. Begin write transaction (autoTrans pattern — same as put_if_absent step 2)

3. Clear main data B-tree:
     sqlite3BtreeClearTable(pKV->pBt, pCF->iTable, NULL)

4. Clear TTL index CFs — handle two cases:

   Case A — TTL CFs already open (pCF->pTtlKeyCF != NULL):
     sqlite3BtreeClearTable(pKV->pBt, pCF->pTtlKeyCF->iTable, NULL)
     sqlite3BtreeClearTable(pKV->pBt, pCF->pTtlExpiryCF->iTable, NULL)

   Case B — TTL data exists from a previous session (pCF->hasTtl == 1
             but pCF->pTtlKeyCF == NULL):
     Open the TTL CFs via kvstoreGetOrCreateTtlCFs(pCF)
     Clear them via sqlite3BtreeClearTable
     Leave them open (pCF->pTtlKeyCF / pTtlExpiryCF now populated)

   Case C — CF never used TTL (pCF->hasTtl == 0 && pCF->pTtlKeyCF == NULL):
     Skip — no TTL tables to clear.

5. Reset pCF->nTtlActive = 0

6. Invalidate cached read cursor:
     sqlite3BtreeCloseCursor(pCF->pReadCur)
     sqlite3_free(pCF->pReadCur)
     pCF->pReadCur = NULL
   (kvstoreGetReadCursor will reallocate on next use)

7. Commit (rollback on error, same autoTrans pattern)

8. sqlite3_mutex_leave both mutexes
```

All three B-tree clears are in one write transaction — atomically consistent.
Case B ensures no stale TTL entries survive across sessions.

### Complexity

| Operation | Cost |
|---|---|
| `kvstore_cf_clear` (today — iterate+delete) | O(N keys) |
| `kvstore_cf_clear` (new — BtreeClearTable) | O(P pages) ≈ O(N / 100) |

### Usage Example

```c
/* Reset session store between test runs */
kvstore_clear(db);

/* Clear one namespace without affecting others */
kvstore_cf_clear(sessions_cf);
```

### Python

```python
db.clear()        # default CF
cf.clear()        # specific CF
```

---

## Feature 4 — Extended `KVStoreStats`

### Problem

Current `KVStoreStats` exposes only five operation counters:

```c
uint64_t nPuts, nGets, nDeletes, nIterations, nErrors;
```

This is insufficient for production operation. Missing:

- **I/O throughput** — bytes read/written (capacity planning, regression detection)
- **WAL health** — commit count, current WAL frame count (checkpoint tuning)
- **Checkpoint activity** — how often checkpoints run
- **TTL activity** — keys expired and purged (TTL correctness verification)
- **Storage** — total pages and free pages (vacuum scheduling, disk usage)
- **Reset** — no way to measure rates (puts/sec, bytes/sec) without a reset API

### API — Extended `KVStoreStats`

```c
typedef struct KVStoreStats KVStoreStats;
struct KVStoreStats {
  /* --- Existing fields (unchanged, same order) --- */
  uint64_t nPuts;         /* Total put operations */
  uint64_t nGets;         /* Total get operations */
  uint64_t nDeletes;      /* Total delete operations */
  uint64_t nIterations;   /* Total iterators created */
  uint64_t nErrors;       /* Total errors encountered */

  /* --- New: I/O throughput --- */
  uint64_t nBytesRead;    /* Total value bytes returned by get operations */
  uint64_t nBytesWritten; /* Total (key + value) bytes written by put operations */

  /* --- New: WAL & transactions --- */
  uint64_t nWalCommits;   /* Write transactions successfully committed */
  uint64_t nCheckpoints;  /* WAL checkpoints performed */

  /* --- New: TTL activity --- */
  uint64_t nTtlExpired;   /* Keys lazily expired on get/exists */
  uint64_t nTtlPurged;    /* Keys removed by kvstore_purge_expired */

  /* --- New: Storage (read from B-tree at kvstore_stats() call time) --- */
  uint64_t nDbPages;      /* Total pages in database (sqlite3BtreeLastPage) */
};

/*
** Read current statistics into *pStats.
** nDbPages is read live from the B-tree at call time.
** All other fields are cumulative counters since store open (or last reset).
*/
int kvstore_stats(KVStore *pKV, KVStoreStats *pStats);

/*
** Reset all cumulative counters to zero.
** nDbPages is always live and is unaffected by reset.
** Useful for measuring rates over a time window:
**
**   kvstore_stats_reset(db);
**   sleep(1);
**   kvstore_stats(db, &s);
**   printf("puts/sec: %llu\n", s.nPuts);
*/
int kvstore_stats_reset(KVStore *pKV);
```

### Note on `nFreePages`

`nFreePages` was considered but removed: SQLite does not expose a direct public API
for per-connection free page count without reading the B-tree header under a lock.
`sqlite3BtreeLastPage` (total pages) is reliable and sufficient — operators can
infer fragmentation from `(nDbPages * pageSize) vs. actual file size`. Free page
count may be added in a future version if a safe API is identified.

### Tracking Points in `kvstore.c`

| New field | Where incremented |
|---|---|
| `nBytesRead` | `kvstore_cf_get_internal` — add `nValue` on successful read |
| `nBytesWritten` | `kvstore_cf_put_internal` — add `nKey + nValue` on write |
| `nWalCommits` | `kvstore_commit` — increment on `KVSTORE_OK` |
| `nCheckpoints` | `kvstore_checkpoint` — increment on `KVSTORE_OK` |
| `nTtlExpired` | `kvstore_cf_get_ttl` — increment when lazy delete fires |
| `nTtlPurged` | `kvstore_cf_purge_expired` — add `nDeleted` to counter |
| `nDbPages` | `kvstore_stats` — `sqlite3BtreeLastPage(pKV->pBt)` at call time |

### Internal struct change

```c
/* In struct KVStore (src/kvstore.c) — add new fields */
struct {
  u64 nPuts;
  u64 nGets;
  u64 nDeletes;
  u64 nIterations;
  u64 nErrors;
  /* New */
  u64 nBytesRead;
  u64 nBytesWritten;
  u64 nWalCommits;
  u64 nCheckpoints;
  u64 nTtlExpired;
  u64 nTtlPurged;
} stats;
```

### Python

```python
s = db.stats()
# Existing
s["puts"], s["gets"], s["deletes"], s["iterations"], s["errors"]
# New
s["bytes_read"], s["bytes_written"]
s["wal_commits"], s["checkpoints"]
s["ttl_expired"], s["ttl_purged"]
s["db_pages"]

# Rate measurement
db.stats_reset()
time.sleep(1)
s = db.stats()
print(f"puts/sec: {s['puts']}, bytes_written/sec: {s['bytes_written']}")
```

---

## Feature 5 — `kvstore_count` / `kvstore_cf_count`

### Problem

Counting keys requires a full O(N) key iteration today. `sqlite3BtreeCount` traverses
the B-tree page by page, summing `pPage->nCell` without reading key/value payload.
For a BLOBKEY tree (which SNKV uses), `intKey = 0` on all pages, so all cells on all
pages are counted — giving the correct total in O(pages).

### API

```c
/*
** Count the number of entries in the default column family.
**
** Complexity: O(pages) — visits each B-tree page once, reads nCell from
** the page header without fetching key or value data. For a hot cache this
** is fast; for a cold large database it triggers one page read per page.
**
** The count includes keys that have expired but not yet been lazily deleted.
** Call kvstore_purge_expired() first for an accurate live count.
**
** Behaviour during an uncommitted write transaction: kvstore_count uses the
** cached read cursor (pCF->pReadCur) which is a read-only cursor. It may
** not see uncommitted puts from the same session's write cursor. For an
** accurate count after a batch of puts, commit the transaction first.
**
** *pnCount must not be NULL.
**
** Returns:
**   KVSTORE_OK on success, error code otherwise.
*/
int kvstore_count(KVStore *pKV, int64_t *pnCount);

/*
** Count entries in the given column family.
** Counts only the main data B-tree — TTL index CFs are not counted.
** Same complexity and caveats as kvstore_count.
*/
int kvstore_cf_count(KVColumnFamily *pCF, int64_t *pnCount);
```

### Internal Design

```
kvstore_cf_count(pCF, pnCount):

1. sqlite3_mutex_enter(pKV->pMutex) + sqlite3_mutex_enter(pCF->pMutex)

2. Ensure read transaction active (pKV->inTrans >= 1).
   If inTrans == 0: begin read transaction.

3. pCur = kvstoreGetReadCursor(pCF)
   Returns cached cursor (pCF->pReadCur) — no allocation.
   If cursor NULL: return KVSTORE_ERROR.

4. rc = sqlite3BtreeCount(pKV->db, pCur, pnCount)
   sqlite3BtreeCount ends by calling moveToRoot(pCur) — cursor left at root.
   Next kvstore_get/exists will re-seek from root via normal B-tree seek,
   which is correct behaviour (CURSOR_REQUIRESSEEK handles this).

5. sqlite3_mutex_leave both mutexes.
   Return KVSTORE_OK on success.
```

Uses the cached read cursor — no extra cursor allocation.
Counts only `pCF->iTable` (main data B-tree). TTL index CFs are separate
B-trees with separate root pages and are not touched.

### Complexity vs. Alternatives

| Method | Complexity | Notes |
|---|---|---|
| Iterate all keys | O(N keys) | Reads full payload per key |
| `sqlite3BtreeCount` | O(P pages) ≈ O(N/100) | Reads page header only |
| Stored counter | O(1) | Adds increment/decrement to every put/delete — rejected |

A stored counter (O(1)) was considered but rejected: it adds overhead to every
put and delete on the common path for a rare operation.

### Usage Example

```c
int64_t n;
kvstore_count(db, &n);
printf("default CF has %lld keys (including expired)\n", n);

kvstore_purge_expired(db, NULL);
kvstore_count(db, &n);
printf("live keys: %lld\n", n);
```

### Python

```python
print(db.count())          # default CF
print(cf.count())          # specific CF

# Accurate live count
db.purge_expired()
print(db.count())
```

---

## Production Readiness Review

All issues identified during design review and their resolutions:

| # | Issue | Severity | Resolution |
|---|---|---|---|
| 1 | `kvstore_cf_clear` silently skips TTL data from previous session (`hasTtl=1`, `pTtlKeyCF==NULL`) | Critical | Case B in internal design: open TTL CFs via `kvstoreGetOrCreateTtlCFs` before clearing |
| 2 | `kvstore_cf_clear` with open iterators → undefined behavior | Critical | Documented as hard precondition in API comment; caller must close all iterators first |
| 3 | `kvstore_iterator_seek` on reverse iterator — `>=` semantics wrong for backward scan | Critical | `reverse=1` path uses `kvstoreSeekBefore`-style logic to seek `<=` instead of `>=` |
| 4 | `nFreePages` — no verified B-tree API for per-connection free page count | Critical | Removed from `KVStoreStats`; documented rationale; may be added in future version |
| 5 | `put_if_absent` in caller-managed write transaction — autoTrans would double-commit | Critical | Explicit `inTrans == 2` check; joins existing transaction without issuing nested commit |
| 6 | No `kvstore_stats_reset()` — no way to measure rates over a time window | Should fix | Added `kvstore_stats_reset()` API |
| 7 | Python `Iterator.seek()` — `started`/`needs_first` flags not reset | Should fix | C binding explicitly sets `started=1`, `needs_first=0` after successful seek |
| 8 | `kvstore_count` during uncommitted write transaction may miss pending puts | Document | Documented in API comment — commit first for accurate count |

---

## Files Changed

| File | Changes |
|---|---|
| `include/kvstore.h` | New declarations for all 5 features + `kvstore_stats_reset`; extended `KVStoreStats` |
| `src/kvstore.c` | Implementations; new stat fields in `struct KVStore`; tracking at call sites |
| `python/snkv_module.c` | New C methods: `seek`, `put_if_absent`, `clear`, `count`, `stats_reset`; extended `stats` dict |
| `python/snkv/__init__.py` | `Iterator.seek()`, `KVStore/ColumnFamily.put_if_absent/clear/count`, `KVStore.stats_reset()` |
| `tests/test_new_apis.c` | New test file — 35 tests |
| `Makefile` | Add `tests/test_new_apis.c` to `TEST_SRC` |

---

## Test Plan

### `kvstore_iterator_seek`

| # | Test |
|---|---|
| 1 | Forward seek to existing key → positioned exactly at that key |
| 2 | Forward seek to non-existing key between two keys → positioned at next key |
| 3 | Forward seek past last key → eof immediately |
| 4 | Forward seek before first key → positioned at first key |
| 5 | Forward seek on prefix iterator within prefix → works, next() stays within prefix |
| 6 | Forward seek on prefix iterator outside prefix → eof immediately |
| 7 | Forward seek then next() → correct traversal from seeked position |
| 8 | Forward seek on empty CF → eof immediately |
| 9 | Reverse seek to existing key → positioned exactly at that key |
| 10 | Reverse seek to non-existing key → positioned at nearest key <= target |
| 11 | Reverse seek past first key → eof immediately |
| 12 | Seek multiple times on same iterator → each seek repositions correctly |

### `kvstore_put_if_absent`

| # | Test |
|---|---|
| 13 | Key absent, no TTL → inserted=1, value readable |
| 14 | Key present → inserted=0, existing value unchanged |
| 15 | Key absent with TTL → inserted=1, TTL readable via ttl_remaining |
| 16 | Key present with TTL → inserted=0, existing TTL unchanged |
| 17 | Expired key → treated as absent, new value inserted with new TTL |
| 18 | CF variant → same behavior on explicit CF handle |
| 19 | pInserted=NULL → no crash, operation still correct |
| 20 | Inside explicit write transaction → joins transaction, no double-commit |

### `kvstore_clear` / `kvstore_cf_clear`

| # | Test |
|---|---|
| 21 | Clear default CF → count=0, iterator immediately eof |
| 22 | Clear CF with TTL keys → TTL index empty, purge_expired returns 0 |
| 23 | Clear CF with TTL from previous session (reopen) → TTL index cleared |
| 24 | Clear empty CF → KVSTORE_OK, no error |
| 25 | Clear CF then reinsert → works correctly |
| 26 | Clear one CF → other CFs unaffected |

### Extended `KVStoreStats`

| # | Test |
|---|---|
| 27 | nBytesWritten increments by key+value size on each put |
| 28 | nBytesRead increments by value size on each get |
| 29 | nWalCommits increments on each commit |
| 30 | nCheckpoints increments after checkpoint |
| 31 | nTtlExpired increments on lazy expiry |
| 32 | nTtlPurged increments by count after purge_expired |
| 33 | nDbPages > 0 after writes |
| 34 | kvstore_stats_reset → all counters zero, nDbPages still accurate |

### `kvstore_count` / `kvstore_cf_count`

| # | Test |
|---|---|
| 35 | Count on empty CF → 0 |
| 36 | Count after N puts → N |
| 37 | Count after put + delete → N-1 |
| 38 | Count includes expired-but-not-purged keys |
| 39 | Count after purge_expired → only live keys |
| 40 | CF count counts only that CF, not others or TTL index CFs |

---

## Backward Compatibility

- `KVStoreStats` struct gains new fields appended after existing fields — no ABI break
  for callers that zero-initialize the struct before calling `kvstore_stats`.
- All new functions are additive — no existing function signatures change.
- `kvstore_stats_reset` is a new function — no existing callers affected.
- Python `stats()` dict gains new keys — existing code accessing only known keys
  is unaffected.
- `kvstore_clear` is a destructive operation with no accidental call risk — requires
  explicit invocation.
