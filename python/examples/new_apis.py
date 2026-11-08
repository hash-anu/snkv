# SPDX-License-Identifier: Apache-2.0
"""
New APIs Example
Demonstrates:
  - Iterator.seek() — forward and reverse positional seek
  - KVStore/ColumnFamily.put_if_absent() — atomic conditional insert
  - KVStore/ColumnFamily.clear() — bulk truncation
  - KVStore/ColumnFamily.count() — entry count
  - KVStore.stats_reset() + extended stats() fields

Run:
    python examples/new_apis.py
"""

import os
import time

from snkv import KVStore, NotFoundError

DB_FILE = "new_apis_example.db"


# ---------------------------------------------------------------------------
# 1. Iterator seek — forward
# ---------------------------------------------------------------------------
def section_seek_forward(db: KVStore) -> None:
    print("\n--- 1. Iterator seek (forward) ---")

    db.put(b"apple",  b"fruit")
    db.put(b"banana", b"fruit")
    db.put(b"cherry", b"fruit")
    db.put(b"date",   b"fruit")

    with db.iterator() as it:
        # Seek to exact key.
        it.seek(b"banana")
        print(f"  seek(b'banana')  → {it.key!r}")

        # Seek between keys — lands on next key >= target.
        it.seek(b"bo")
        print(f"  seek(b'bo')      → {it.key!r}  (next >= target)")

        # Seek past last key → eof.
        it.seek(b"zzz")
        print(f"  seek(b'zzz')     → eof={it.eof}")

        # Seek then walk forward.
        it.seek(b"cherry")
        keys = []
        while not it.eof:
            keys.append(it.key.decode())
            it.next()
        print(f"  walk from b'cherry': {keys}")

    db.clear()


# ---------------------------------------------------------------------------
# 2. Iterator seek — reverse
# ---------------------------------------------------------------------------
def section_seek_reverse(db: KVStore) -> None:
    print("\n--- 2. Iterator seek (reverse) ---")

    db.put(b"aaa", b"v")
    db.put(b"bbb", b"v")
    db.put(b"ccc", b"v")

    with db.iterator(reverse=True) as it:
        it.last()

        # Seek to exact key.
        it.seek(b"bbb")
        print(f"  rev seek(b'bbb') → {it.key!r}")

        # Seek between keys — lands on nearest key <= target.
        it.seek(b"bbc")
        print(f"  rev seek(b'bbc') → {it.key!r}  (nearest <=)")

        # Seek before all keys → eof.
        it.seek(b"000")
        print(f"  rev seek(b'000') → eof={it.eof}")

    db.clear()


# ---------------------------------------------------------------------------
# 3. Iterator seek — prefix iterator
# ---------------------------------------------------------------------------
def section_seek_prefix(db: KVStore) -> None:
    print("\n--- 3. Seek within a prefix iterator ---")

    db.put(b"user:alice", b"1")
    db.put(b"user:bob",   b"2")
    db.put(b"user:carol", b"3")
    db.put(b"team:alpha", b"x")   # different prefix

    with db.iterator(prefix=b"user:") as it:
        it.seek(b"user:bob")
        print(f"  seek(b'user:bob') within prefix b'user:':")
        while not it.eof:
            print(f"    {it.key!r}")
            it.next()
    print("  (prefix boundary respected — b'team:alpha' not visited)")

    db.clear()


# ---------------------------------------------------------------------------
# 4. Seek chaining
# ---------------------------------------------------------------------------
def section_seek_chaining(db: KVStore) -> None:
    print("\n--- 4. Seek chaining ---")

    db.put(b"x", b"1")
    db.put(b"y", b"2")
    db.put(b"z", b"3")

    with db.iterator() as it:
        # seek() returns self, so calls can be chained.
        key = it.seek(b"y").key
        print(f"  it.seek(b'y').key = {key!r}")

    db.clear()


# ---------------------------------------------------------------------------
# 5. put_if_absent — default CF
# ---------------------------------------------------------------------------
def section_put_if_absent(db: KVStore) -> None:
    print("\n--- 5. put_if_absent (default CF) ---")

    # Key absent → inserted, returns True.
    inserted = db.put_if_absent(b"lock", b"owner:alice")
    print(f"  put_if_absent(b'lock', absent)  → {inserted}")

    # Key present → not inserted, returns False.
    inserted = db.put_if_absent(b"lock", b"owner:bob")
    print(f"  put_if_absent(b'lock', present) → {inserted}")

    print(f"  current value: {db.get(b'lock')!r}  (first write wins)")

    # put_if_absent with TTL — auto-releasing lock.
    inserted = db.put_if_absent(b"session:42", b"token-xyz", ttl=5)
    remaining = db.ttl(b"session:42")
    print(f"  put_if_absent(b'session:42', ttl=5s) → inserted={inserted}, "
          f"remaining={remaining:.2f}s")

    db.clear()


# ---------------------------------------------------------------------------
# 6. put_if_absent — named CF
# ---------------------------------------------------------------------------
def section_put_if_absent_cf(db: KVStore) -> None:
    print("\n--- 6. put_if_absent (named CF) ---")

    with db.create_column_family("dedup") as cf:
        # Deduplication: first write wins.
        print(f"  cf.put_if_absent(b'msg:001', first)  → {cf.put_if_absent(b'msg:001', b'hello')}")
        print(f"  cf.put_if_absent(b'msg:001', second) → {cf.put_if_absent(b'msg:001', b'world')}")
        print(f"  value: {cf.get(b'msg:001')!r}  (first write wins)")

        # Works correctly inside an explicit transaction.
        db.begin(write=True)
        cf.put_if_absent(b"msg:002", b"a")
        cf.put_if_absent(b"msg:002", b"b")   # same tx — should be blocked
        db.commit()
        print(f"  tx: msg:002 = {cf.get(b'msg:002')!r}")


# ---------------------------------------------------------------------------
# 7. clear
# ---------------------------------------------------------------------------
def section_clear(db: KVStore) -> None:
    print("\n--- 7. clear ---")

    for i in range(10):
        db.put(f"k{i}".encode(), b"v")

    print(f"  count before clear: {db.count()}")
    db.clear()
    print(f"  count after  clear: {db.count()}")

    # New inserts work normally after clear.
    db.put(b"fresh", b"start")
    print(f"  count after re-insert: {db.count()}")

    # CF-level clear is isolated from the default CF.
    with db.create_column_family("scratch") as cf:
        cf.put(b"tmp1", b"x")
        cf.put(b"tmp2", b"x")
        print(f"  CF \"scratch\" count before cf.clear(): {cf.count()}")
        cf.clear()
        print(f"  CF \"scratch\" count after  cf.clear(): {cf.count()}")

    print(f"  default CF count unchanged: {db.count()}")
    db.clear()


# ---------------------------------------------------------------------------
# 8. count
# ---------------------------------------------------------------------------
def section_count(db: KVStore) -> None:
    print("\n--- 8. count ---")

    print(f"  empty store: {db.count()}")

    for i in range(5):
        db.put(f"k{i}".encode(), b"v")
    print(f"  after 5 puts: {db.count()}")

    db.delete(b"k2")
    print(f"  after delete k2: {db.count()}")

    # count() includes expired-but-not-yet-purged keys.
    db._db.put_ttl(b"exp", b"v", int((time.time() - 1) * 1000))
    print(f"  with 1 expired (unpurged): {db.count()}")

    db.purge_expired()
    print(f"  after purge_expired: {db.count()}")

    # CF count is independent of default CF.
    with db.create_column_family("myCF") as cf:
        cf.put(b"a", b"v")
        cf.put(b"b", b"v")
        print(f"  CF count={cf.count()}, default CF count={db.count()} (isolated)")

    db.clear()


# ---------------------------------------------------------------------------
# 9. Extended stats + stats_reset
# ---------------------------------------------------------------------------
def section_stats(db: KVStore) -> None:
    print("\n--- 9. Extended stats + stats_reset ---")

    db.stats_reset()

    # Write: key=5 bytes, value=10 bytes → 15 bytes written.
    db.put(b"hello", b"0123456789")

    # Read: value=10 bytes → 10 bytes read.
    db.get(b"hello")

    # Trigger lazy TTL expiry.
    db._db.put_ttl(b"gone", b"x", int((time.time() - 1) * 1000))
    db.get(b"gone")   # lazy-expires on read

    st = db.stats()

    print(f"  puts          = {st['puts']}")
    print(f"  gets          = {st['gets']}")
    print(f"  bytes_written = {st['bytes_written']}")
    print(f"  bytes_read    = {st['bytes_read']}")
    print(f"  wal_commits   = {st['wal_commits']}")
    print(f"  checkpoints   = {st['checkpoints']}")
    print(f"  ttl_expired   = {st['ttl_expired']}")
    print(f"  ttl_purged    = {st['ttl_purged']}")
    print(f"  db_pages      = {st['db_pages']}")

    # Reset clears cumulative counters; db_pages remains live.
    db.stats_reset()
    st = db.stats()
    print(f"  after reset: puts={st['puts']} gets={st['gets']} "
          f"bytes_written={st['bytes_written']}")
    print(f"  db_pages after reset = {st['db_pages']}  (always live)")

    db.clear()


# ---------------------------------------------------------------------------
# 10. Real-world: idempotent job scheduler using put_if_absent
# ---------------------------------------------------------------------------
class JobScheduler:
    """
    Distributed job deduplication using put_if_absent.

    Only the first worker that claims a job ID proceeds to execute it.
    The lock expires automatically via TTL so jobs can be retried if
    the worker crashes.
    """

    def __init__(self, db: KVStore, timeout_s: float = 30) -> None:
        self._db      = db
        self._timeout = timeout_s
        try:
            self._cf = db.open_column_family("jobs")
        except NotFoundError:
            self._cf = db.create_column_family("jobs")

    def try_claim(self, job_id: str, worker_id: str) -> bool:
        """Return True if this worker claimed the job, False if already claimed."""
        key = job_id.encode()
        return self._cf.put_if_absent(key, worker_id.encode(), ttl=self._timeout)

    def release(self, job_id: str) -> None:
        self._cf.delete(job_id.encode())

    def close(self) -> None:
        self._cf.close()


def section_job_scheduler(db: KVStore) -> None:
    print("\n--- 10. Real-world: idempotent job scheduler ---")

    sched = JobScheduler(db, timeout_s=10)

    # Worker A claims job-001.
    claimed_a = sched.try_claim("job-001", "worker-A")
    print(f"  worker-A claims job-001: {claimed_a}")

    # Worker B tries the same job — should be blocked.
    claimed_b = sched.try_claim("job-001", "worker-B")
    print(f"  worker-B claims job-001: {claimed_b}  (blocked — already claimed)")

    # Worker A claims a different job.
    claimed_a2 = sched.try_claim("job-002", "worker-A")
    print(f"  worker-A claims job-002: {claimed_a2}  (different job, succeeds)")

    # After releasing, another worker can claim it.
    sched.release("job-001")
    reclaimed = sched.try_claim("job-001", "worker-B")
    print(f"  worker-B re-claims job-001 after release: {reclaimed}")

    sched.close()


# ---------------------------------------------------------------------------
def main() -> None:
    with KVStore(DB_FILE) as db:
        print("=== SNKV New APIs Example ===")

        section_seek_forward(db)
        section_seek_reverse(db)
        section_seek_prefix(db)
        section_seek_chaining(db)
        section_put_if_absent(db)
        section_put_if_absent_cf(db)
        section_clear(db)
        section_count(db)
        section_stats(db)
        section_job_scheduler(db)


if __name__ == "__main__":
    main()

    for ext in ("", "-wal", "-shm"):
        try:
            os.remove(DB_FILE + ext)
        except FileNotFoundError:
            pass

    print("\n[OK] new_apis.py example complete.")
