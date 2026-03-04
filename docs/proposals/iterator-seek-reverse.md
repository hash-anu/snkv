# Proposal: Reverse Iteration for SNKV

**Status:** RFC (Request for Comments)
**Author:** SNKV maintainers
**Created:** 2026-03-03
**Target version:** 0.4.0

---

## Table of Contents

- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Background](#background)
- [Design](#design)
  - [KVIterator struct changes](#kviterator-struct-changes)
  - [API](#api)
  - [Internals: Reverse Iteration](#internals-reverse-iteration)
  - [Internals: TTL interaction](#internals-ttl-interaction)
  - [Column Family Support](#column-family-support)
- [Performance Analysis](#performance-analysis)
- [Error Handling](#error-handling)
- [Backward Compatibility](#backward-compatibility)
- [Test Plan](#test-plan)
- [Python API](#python-api)
- [Alternatives Considered](#alternatives-considered)
- [Open Questions](#open-questions)
- [Implementation Checklist](#implementation-checklist)

---

## Motivation

SNKV's current iterator supports forward-only traversal starting from the first
key. This blocks common and fundamental query patterns:

**Reverse iteration — no workaround:**
- "Latest N events" — must read all keys into memory and reverse in the caller.
- Leaderboard (top scores descending) — O(n) full scan instead of O(k).
- Time-series descending — the dominant access pattern for logs and metrics.
- Cursor-based pagination backward through results.

---

## Goals

1. Reverse iteration — iterate keys from last to first.
2. Reverse prefix iteration — iterate keys with a given prefix from last to first.
3. Works with column families — all new functions have default-CF and named-CF
   variants consistent with the existing API.
4. TTL-aware — reverse iteration correctly skips expired keys, same as forward
   iteration today.
5. Backward compatible — all existing iterator behaviour is unchanged. The new
   field on `KVIterator` is zero-initialised and inactive unless explicitly set.

---

## Non-Goals

- Bidirectional switching — an iterator created for forward traversal cannot be
  switched to reverse mid-scan. Direction is set at creation time.
- Iterator bounds — upper and lower bound support is not in scope.
- Iterator invalidation on write — same as existing: the iterator cursor is
  marked `CURSOR_REQUIRESSEEK` by the btree layer on write and re-seeks
  automatically on the next step. No new semantics.
- Count / aggregate operations — not in scope.

---

## Background

### Existing iterator internals

`KVIterator` (lines 232–244 of `kvstore.c`) holds a `BtCursor *pCur` pointing
into the B-tree for the target CF. The current implementation uses:

```
sqlite3BtreeFirst(pCur, &res)   — position at smallest key
sqlite3BtreeNext(pCur, 0)       — advance forward
sqlite3BtreeIndexMoveto(pCur, pIdxKey, &res) — seek to key (used in prefix iterator)
```

The SQLite B-tree already provides the symmetric reverse primitives, declared in
`include/btree.h` and implemented in `src/btree.c`:

```
sqlite3BtreeLast(pCur, &res)        — position at largest key
sqlite3BtreePrevious(pCur, flags)   — advance backward
```

Neither `sqlite3BtreeLast` nor `sqlite3BtreePrevious` is currently called from
`kvstore.c`. No new btree primitives are needed — this proposal wires them up.

A new `kvstoreSeekBefore` static helper (first key strictly < target) is needed
for TTL lazy-delete recovery under reverse iteration. It mirrors the existing
`kvstoreSeekAfter` helper (line 396).

---

## Design

### KVIterator struct changes

One field is added to `KVIterator` (after the existing `pPrefix`/`nPrefix`):

```c
struct KVIterator {
  KVColumnFamily *pCF;
  BtCursor *pCur;
  int eof;
  int ownsTrans;
  void *pKeyBuf;   int nKeyBuf;
  void *pValBuf;   int nValBuf;
  int isValid;
  void *pPrefix;   int nPrefix;

  /* --- new field --- */
  int reverse;              /* 1 = iterate backward (BtreeLast/BtreePrevious) */
};
```

The field is zero-initialised by `sqlite3MallocZero` in
`kvstore_cf_iterator_create`. An iterator with `reverse=0` behaves identically
to the current implementation — no existing behaviour changes.

---

### API

#### Reverse iterator creation

```c
/*
** Create a reverse iterator for the default column family.
**
** A reverse iterator starts positioned before the last entry.
** Call kvstore_iterator_last() to move to the first (largest) key,
** then kvstore_iterator_prev() to advance backward.
**
** Returns:
**   KVSTORE_OK on success, error code otherwise.
*/
int kvstore_reverse_iterator_create(KVStore *pKV, KVIterator **ppIter);

/*
** CF variant of kvstore_reverse_iterator_create.
*/
int kvstore_cf_reverse_iterator_create(KVColumnFamily *pCF, KVIterator **ppIter);

/*
** Create a reverse prefix iterator for the default column family.
**
** Identical to kvstore_reverse_iterator_create but scoped to keys that begin
** with (pPrefix, nPrefix). kvstore_iterator_last() positions at the last key
** with the given prefix; kvstore_iterator_prev() walks backward and sets eof=1
** when the key no longer matches the prefix.
**
** pPrefix bytes are copied; the caller's buffer may be freed immediately.
**
** Returns:
**   KVSTORE_OK on success, error code otherwise.
*/
int kvstore_reverse_prefix_iterator_create(KVStore *pKV,
                                           const void *pPrefix, int nPrefix,
                                           KVIterator **ppIter);

/*
** CF variant of kvstore_reverse_prefix_iterator_create.
*/
int kvstore_cf_reverse_prefix_iterator_create(KVColumnFamily *pCF,
                                              const void *pPrefix, int nPrefix,
                                              KVIterator **ppIter);
```

#### Reverse movement

```c
/*
** Position a reverse iterator at the last (largest) key.
**
** Equivalent to kvstore_iterator_first() for forward iterators.
** Must be called on an iterator created with kvstore_reverse_iterator_create()
** or kvstore_cf_reverse_iterator_create().
**
** Returns:
**   KVSTORE_OK on success (check kvstore_iterator_eof for emptiness).
*/
int kvstore_iterator_last(KVIterator *pIter);

/*
** Advance a reverse iterator to the previous (smaller) key.
**
** Equivalent to kvstore_iterator_next() for forward iterators.
** Sets eof=1 when the beginning is reached.
**
** Returns:
**   KVSTORE_OK on success.
*/
int kvstore_iterator_prev(KVIterator *pIter);
```

---

### Internals: Reverse Iteration

`kvstore_reverse_iterator_create` calls `kvstore_cf_iterator_create` and sets
`pIter->reverse = 1`. No other changes to the creation path.

`kvstore_reverse_prefix_iterator_create` follows the same pattern as
`kvstore_prefix_iterator_create`: it calls `kvstore_cf_iterator_create`, sets
`pIter->reverse = 1`, then copies `(pPrefix, nPrefix)` into heap-allocated
buffers stored in `pIter->pPrefix` / `pIter->nPrefix` using `sqlite3Malloc`.
If the allocation fails the iterator is closed and `KVSTORE_NOMEM` is returned.
The caller's `pPrefix` buffer may be freed immediately after the call returns.

`kvstore_iterator_last`:

```
1. If pIter->pPrefix is set (reverse prefix iterator):
     Compute prefix successor: copy pPrefix, increment last non-0xFF byte by 1.
     If entire prefix is 0xFF bytes (no successor exists): fall through to BtreeLast.
     Else: kvstoreSeekBefore(pCur, pSucc, nSucc, &eof)
     If eof or current key does not start with pPrefix: pIter->eof = 1, return.
2. Else (plain reverse iterator):
     sqlite3BtreeLast(pIter->pCur, &res)
     pIter->eof = res
3. If !eof: kvstoreIterSkipExpiredReverse(pIter)
```

The prefix successor `pSucc` is computed as: copy `pPrefix` bytes, scan from
the last byte backward to find the first byte that is not `0xFF`, increment it
by 1, and truncate there. Example: `"user:"` → `"user;"` (`:` + 1 = `;`).
This positions the cursor just past all keys with the given prefix, and
`kvstoreSeekBefore` backs up one step to land on the last matching key. Cost
is O(log n) — identical to a normal btree seek.

`kvstore_iterator_prev`:

```
1. If pIter->eof: return KVSTORE_OK
2. sqlite3BtreePrevious(pIter->pCur, 0)
3. If SQLITE_DONE: pIter->eof = 1, return KVSTORE_OK
4. If pIter->pPrefix: kvstoreIterCheckPrefix(pIter) — if no match, pIter->eof = 1, return KVSTORE_OK
5. kvstoreIterSkipExpiredReverse(pIter)
```

Step 4 is the prefix boundary check for non-expired keys walking past the prefix.
`kvstoreIterCheckPrefix` is the same helper used by `kvstore_iterator_next` —
it reads the current key from the cursor and checks whether it starts with
`pIter->pPrefix`.

---

### Internals: TTL interaction

`kvstoreIterSkipExpired` (line 2835) handles forward iteration lazy-delete
recovery: after deleting an expired key, it calls `kvstoreSeekAfter` to
reposition the cursor at the first entry strictly > the deleted key.

For reverse iteration, a new static helper `kvstoreIterSkipExpiredReverse` is
needed. It is structurally identical to `kvstoreIterSkipExpired` with one
difference: after the lazy delete, it calls a new `kvstoreSeekBefore` helper
(first entry strictly < deleted key) instead of `kvstoreSeekAfter`.

**`kvstoreSeekBefore`** (new static, mirrors `kvstoreSeekAfter` at line 396):

```c
/* Position pCur at the last entry strictly less than (pKey, nKey).
** Sets *pEof = 1 if no such entry exists.
*/
static int kvstoreSeekBefore(
  BtCursor *pCur,
  KeyInfo *pKeyInfo,
  const void *pKey, int nKey,
  int *pEof
){
  UnpackedRecord idxKey; Mem memField;
  int res = 0, rc;
  /* ... same UnpackedRecord setup ... */
  rc = sqlite3BtreeIndexMoveto(pCur, &idxKey, &res);
  if( rc != SQLITE_OK ){ *pEof = 1; return rc; }
  if( res >= 0 ){
    /* At or after target — back up one entry. */
    rc = sqlite3BtreePrevious(pCur, 0);
    if( rc == SQLITE_DONE ){ *pEof = 1; return SQLITE_OK; }
    if( rc != SQLITE_OK ){ *pEof = 1; return rc; }
  }
  /* res < 0: cursor is already at the last entry < target. */
  *pEof = sqlite3BtreeEof(pCur);
  return SQLITE_OK;
}
```

---

### Column Family Support

All new functions have default-CF and CF-level variants following the existing
naming convention:

| Default CF | Named CF |
|---|---|
| `kvstore_reverse_iterator_create` | `kvstore_cf_reverse_iterator_create` |
| `kvstore_reverse_prefix_iterator_create` | `kvstore_cf_reverse_prefix_iterator_create` |
| `kvstore_iterator_last` | (operates on the iterator handle — CF-agnostic) |
| `kvstore_iterator_prev` | (operates on the iterator handle — CF-agnostic) |

`kvstore_iterator_last` and `prev` operate on a `KVIterator *` handle which
already contains its CF — no CF parameter is needed.

---

## Performance Analysis

`sqlite3BtreeLast` and `sqlite3BtreePrevious` are symmetric to `sqlite3BtreeFirst`
and `sqlite3BtreeNext` at the btree level. B-trees are bidirectional by design;
the page structure supports both directions with identical page-access cost.
Each `prev` call is O(1) amortised, same as `next`.

The `reverse` field is zero-initialised. Zero overhead for all existing iterator
usage.

---

## Error Handling

| Condition | Behaviour |
|---|---|
| `kvstore_iterator_last` on forward iterator | `KVSTORE_ERROR` — direction mismatch |
| `kvstore_iterator_prev` on forward iterator | `KVSTORE_ERROR` — direction mismatch |
| `BtreePrevious` returns `SQLITE_DONE` | `pIter->eof = 1`, `KVSTORE_OK` returned |
| DB corruption mid-iteration | Returns `KVSTORE_CORRUPT`, same as existing |

---

## Thread Safety

The thread-safety model for reverse iterators is identical to forward iterators:

| Scope | Thread-safe? | Notes |
|---|---|---|
| `KVStore *` / `KVColumnFamily *` | Yes | Multiple threads may create iterators concurrently. |
| `KVIterator *` handle | No | One thread at a time per iterator handle. |
| `kvstore_iterator_last` / `prev` | No (per handle) | No mutex held during cursor movement. |
| TTL lazy-delete inside `prev` | Yes (internally) | `kvstoreIterSkipExpiredReverse` acquires `pCF->pMutex` + `pKV->pMutex` before writing, same as `kvstoreIterSkipExpired` does for forward iteration. |

The creation functions (`kvstore_reverse_iterator_create`,
`kvstore_reverse_prefix_iterator_create`) hold `pKV->pMutex` during cursor
setup and release it before returning — same as `kvstore_cf_iterator_create`.
No new locking rules are introduced.

---

## Backward Compatibility

- No existing function signatures change.
- `KVIterator` is an opaque type in `kvstore.h` — adding fields is ABI-safe
  for all callers who allocate iterators through `kvstore_iterator_create`.
- `reverse=0` produces identical behaviour to the current implementation. Zero
  new overhead.
- `kvstore_iterator_first` and `kvstore_iterator_next` are unchanged.

---

## Test Plan

### Unit tests (`tests/test_iterator_reverse.c`)

| # | Test | Verifies |
|---|---|---|
| 1 | `rev_basic` | Insert 10 keys → reverse iter → keys in descending order |
| 2 | `rev_empty` | Reverse iter on empty CF → eof immediately |
| 3 | `rev_single` | Reverse iter on single key → one result, then eof |
| 4 | `rev_last_first` | `iterator_last` + `iterator_prev` repeatedly → all keys |
| 5 | `rev_cf` | Reverse iterator on named CF |
| 6 | `rev_multi_cf` | Two CFs, independent reverse iterators |
| 7 | `rev_ttl_skip` | Reverse iter over CF with expired keys → expired keys skipped |
| 8 | `direction_mismatch` | `iterator_last` on forward iter → KVSTORE_ERROR |
| 9 | `rev_with_fwd_prefix` | Forward prefix iterator unaffected by new field |
| 10 | `rev_prefix_basic` | Reverse prefix iter → only matching keys, descending |
| 11 | `rev_prefix_empty` | Reverse prefix iter, no keys match → eof immediately |
| 12 | `rev_prefix_all_ff` | Prefix with all-0xFF bytes (no successor) → BtreeLast fallback |
| 13 | `rev_prefix_ttl_skip` | Reverse prefix iter with expired keys → expired keys skipped |

### Integration tests

- **"Latest N" query**: 1 million keys, fetch last 10 — verify O(10) behaviour
  (no full scan).
- **Pagination correctness**: paginate forward through 10,000 keys, then backward
  through same 10,000 — verify identical key set in reverse order.
- **Reverse prefix scan**: insert `user:001`–`user:999` alongside unrelated keys;
  reverse prefix iter on `"user:"` returns exactly `user:001`–`user:999` in
  descending order, no unrelated keys.
- **Python API**: end-to-end test covering all new iterator methods.

---

## Python API

The Python `snkv` package exposes reverse iteration through the existing
`KVStore.iterator()` factory method extended with `reverse` and `prefix`
parameters:

The `reverse` and `prefix` parameters map to the four C creation functions as
follows:

| `reverse` | `prefix` | C function called |
|---|---|---|
| `False` | `None` | `kvstore_iterator_create` |
| `False` | `b"..."` | `kvstore_prefix_iterator_create` |
| `True` | `None` | `kvstore_reverse_iterator_create` |
| `True` | `b"..."` | `kvstore_reverse_prefix_iterator_create` |

`cf_iterator` follows the same dispatch using the CF-level variants
(`kvstore_cf_*`).

```python
class KVStore:
    def iterator(
        self,
        *,
        reverse: bool = False,
        prefix: bytes | None = None,
    ) -> "Iterator":
        """
        Return an iterator over the default column family.

        reverse=True         — iterate from last key to first.
        prefix=b"user:"      — scope to keys starting with prefix.
        reverse=True +
          prefix=b"user:"    — reverse scan over keys with the given prefix.

        Usage (reverse / latest-N):
            with db.iterator(reverse=True) as it:
                it.last()
                for _ in range(10):
                    if it.eof(): break
                    print(it.key(), it.value())
                    it.prev()

        Usage (reverse prefix / latest N events for a user):
            with db.iterator(reverse=True, prefix=b"user:42:") as it:
                it.last()
                while not it.eof():
                    process(it.key(), it.value())
                    it.prev()
        """
        ...

    def cf_iterator(
        self,
        cf: "ColumnFamily",
        *,
        reverse: bool = False,
        prefix: bytes | None = None,
    ) -> "Iterator": ...
```

```python
class Iterator:
    """Returned by KVStore.iterator(). Use as context manager."""
    def first(self) -> None: ...          # forward: move to first key (or first with prefix)
    def last(self) -> None: ...           # reverse: move to last key (or last with prefix)
    def next(self) -> None: ...           # forward: advance
    def prev(self) -> None: ...           # reverse: advance
    def eof(self) -> bool: ...
    def key(self) -> bytes: ...
    def value(self) -> bytes: ...
    def __enter__(self) -> "Iterator": ...
    def __exit__(self, *_) -> None: ...   # calls close()
    def close(self) -> None: ...
```

### Common patterns

```python
# Latest 10 events (reverse, no prefix)
results = []
with db.iterator(reverse=True) as it:
    it.last()
    while not it.eof() and len(results) < 10:
        results.append((it.key(), it.value()))
        it.prev()

# Latest 10 events for user 42 (reverse prefix)
results = []
with db.iterator(reverse=True, prefix=b"user:42:") as it:
    it.last()
    while not it.eof() and len(results) < 10:
        results.append((it.key(), it.value()))
        it.prev()

# Full reverse prefix scan
with db.iterator(reverse=True, prefix=b"session:") as it:
    it.last()
    while not it.eof():
        process(it.key(), it.value())
        it.prev()
```

---

## Alternatives Considered

### Alternative 1: Bidirectional iterator (switch direction mid-scan)

Allow `next` and `prev` on the same iterator. RocksDB supports this.

**Rejected because:**
- Adds `reverse` state change to `iterator_next` and `iterator_prev` — every
  call must check whether the direction has changed since the last step.
- TTL skip-expired recovery must handle both directions on the same iterator.
- Doubles the number of code paths in the hot path.
- Real-world usage almost never switches direction mid-scan. The simpler
  "direction set at creation" model covers all practical cases.

### Alternative 2: Separate reverse-iterator type

Create `KVReverseIterator` as a distinct type.

**Rejected because:**
- Doubles all iterator-related code (close, key, value, TTL skip, CF ref).
- `KVIterator` already has all the state needed; a single `reverse` flag is
  sufficient to distinguish behaviour.

---

## Open Questions

**Q1. Should `kvstore_iterator_last` on a forward iterator return
`KVSTORE_ERROR` or silently work (position at last key)?**

Current proposal: `KVSTORE_ERROR` — direction mismatch is likely a bug. An
alternative: allow it to work, since the btree supports it. Explicit error is
safer.

---

## Implementation Checklist

- [ ] Add `reverse` field to `KVIterator` struct
- [ ] Add `kvstoreSeekBefore` static helper (mirrors `kvstoreSeekAfter`)
- [ ] Add `kvstorePrefixSuccessor` static helper (compute prefix + 1 for reverse prefix seek)
- [ ] Add `kvstoreIterSkipExpiredReverse` static helper (mirrors `kvstoreIterSkipExpired`)
- [ ] Implement `kvstore_cf_reverse_iterator_create` / `kvstore_reverse_iterator_create`
- [ ] Implement `kvstore_cf_reverse_prefix_iterator_create` / `kvstore_reverse_prefix_iterator_create`
- [ ] Implement `kvstore_iterator_last` (prefix-aware: uses `kvstorePrefixSuccessor` + `kvstoreSeekBefore` when `pPrefix` set)
- [ ] Implement `kvstore_iterator_prev`
- [ ] Add all new declarations to `include/kvstore.h`
- [ ] Write `tests/test_iterator_reverse.c` (13 tests)
- [ ] Add `tests/test_iterator_reverse.c` to `Makefile` TEST_SRC
- [ ] Implement Python bindings in `python/snkv_module.c`
- [ ] Add `iterator()` `reverse` + `prefix` parameters to `python/snkv/__init__.py`
- [ ] Update `docs/api/API_SPECIFICATION.md` with new iterator section
- [ ] Update `docs/python_api/API.md` with Python iterator section
- [ ] Add `examples/iterator_reverse.c` and `python/examples/iterator_reverse.py`
- [ ] Run full test suite: all existing tests pass, 13 new tests pass
- [ ] Valgrind clean on `tests/test_iterator_reverse`

---

*Feedback welcome via GitHub Discussions or as comments on the PR that introduced this file.*
