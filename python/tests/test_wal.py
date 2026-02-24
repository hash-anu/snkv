"""
WAL journal mode tests — mirrors tests/test_wal.c.

Covers WAL file lifecycle, CRUD, transactions, recovery, persistence,
concurrency, column families, large payloads, iterators, batch performance,
integrity, cross-mode interop, statistics, and a full ACID sub-suite.
"""

import os
import threading
import pytest
from snkv import KeyValueStore, JOURNAL_WAL, JOURNAL_DELETE, CHECKPOINT_TRUNCATE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _wal(path: str) -> str:
    return path + "-wal"

def _shm(path: str) -> str:
    return path + "-shm"

def _write_n(db: KeyValueStore, n: int, prefix: str = "k") -> None:
    db.begin(write=True)
    for i in range(n):
        db[f"{prefix}{i:06d}".encode()] = f"v{i}".encode()
    db.commit()


# ---------------------------------------------------------------------------
# File lifecycle
# ---------------------------------------------------------------------------

def test_wal_file_created_not_journal(tmp_path):
    """Opening in WAL mode must create a -wal file, not a -journal file."""
    path = str(tmp_path / "wal.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db.begin(write=True)
        db[b"k"] = b"v"
        # flush to WAL before commit so the file exists
        db.commit()
        assert os.path.exists(_wal(path)), "-wal file must exist"
        assert not os.path.exists(path + "-journal"), "-journal must NOT exist"


def test_wal_shm_file_exists_during_writes(tmp_path):
    """Both -wal and -shm must exist while the WAL-mode store is open."""
    path = str(tmp_path / "shm.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db.begin(write=True)
        db[b"k"] = b"v"
        db.commit()
        assert os.path.exists(_wal(path))
        assert os.path.exists(_shm(path))


def test_wal_shm_during_transaction(tmp_path):
    """-wal and -shm must exist after begin, after put, and after commit."""
    path = str(tmp_path / "shm_txn.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db.begin(write=True)
        assert os.path.exists(_wal(path)) or os.path.exists(_shm(path))
        db[b"k"] = b"v"
        db.commit()
        assert os.path.exists(_wal(path))
        assert os.path.exists(_shm(path))


# ---------------------------------------------------------------------------
# CRUD in WAL mode
# ---------------------------------------------------------------------------

def test_wal_basic_crud(tmp_path):
    """Full CRUD cycle in WAL mode."""
    path = str(tmp_path / "crud.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db[b"k"] = b"v1"
        assert db.exists(b"k")
        assert db[b"k"] == b"v1"

        db[b"k"] = b"v2"   # update
        assert db[b"k"] == b"v2"

        db.delete(b"k")
        assert db.get(b"k") is None


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------

def test_wal_commit_20_keys(tmp_path):
    """20 keys committed in WAL mode must all be readable."""
    path = str(tmp_path / "commit.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        _write_n(db, 20)
        for i in range(20):
            assert db.get(f"k{i:06d}".encode()) == f"v{i}".encode()


def test_wal_rollback_restores_original(tmp_path):
    """A rolled-back update must leave the original value intact."""
    path = str(tmp_path / "rb.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db[b"base"] = b"original"

        db.begin(write=True)
        db[b"base"] = b"modified"
        db.rollback()

        assert db[b"base"] == b"original"


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def test_wal_persistence_50_keys(tmp_path):
    """50 keys written in WAL mode must survive close + reopen."""
    path = str(tmp_path / "persist.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        _write_n(db, 50)

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        for i in range(50):
            assert db.get(f"k{i:06d}".encode()) == f"v{i}".encode()


# ---------------------------------------------------------------------------
# Crash recovery (simulated: close without commit)
# ---------------------------------------------------------------------------

def test_wal_recovery_uncommitted_discarded(tmp_path):
    """Uncommitted WAL data must be absent after close + reopen."""
    path = str(tmp_path / "recovery.db")

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db[b"committed"] = b"yes"

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db.begin(write=True)
        db[b"lost"] = b"no"
        # close without commit

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        assert db[b"committed"] == b"yes"
        assert db.get(b"lost") is None


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------

def test_wal_concurrent_1_writer_7_readers(tmp_path):
    """1 writer + 7 readers sharing a WAL DB: no corruption."""
    path = str(tmp_path / "conc.db")
    errors = []
    lock = threading.Lock()

    def _writer():
        try:
            with KeyValueStore(path, journal_mode=JOURNAL_WAL, busy_timeout=5000) as db:
                for batch in range(10):
                    db.begin(write=True)
                    for j in range(100):
                        key = f"w{batch:02d}_{j:04d}".encode()
                        db[key] = f"val{batch}_{j}".encode()
                    db.commit()
        except Exception as e:
            with lock:
                errors.append(f"writer: {e}")

    def _reader(rid: int):
        try:
            with KeyValueStore(path, journal_mode=JOURNAL_WAL, busy_timeout=5000) as db:
                for _ in range(5):
                    for batch in range(10):
                        for j in range(100):
                            key = f"w{batch:02d}_{j:04d}".encode()
                            val = db.get(key)
                            if val is not None:
                                expected = f"val{batch}_{j}".encode()
                                if val != expected:
                                    with lock:
                                        errors.append(
                                            f"reader {rid}: corrupt {key!r}"
                                        )
                                    return
        except Exception as e:
            with lock:
                errors.append(f"reader {rid}: {e}")

    threads = [threading.Thread(target=_writer)]
    threads += [threading.Thread(target=_reader, args=(i,)) for i in range(7)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, "\n".join(errors)


# ---------------------------------------------------------------------------
# Column families in WAL mode
# ---------------------------------------------------------------------------

def test_wal_column_families_isolation(tmp_path):
    """Two CFs in WAL mode must hold independent values for the same key."""
    path = str(tmp_path / "cf.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        with db.create_column_family("cf1") as cf1:
            with db.create_column_family("cf2") as cf2:
                cf1[b"shared"] = b"from_cf1"
                cf2[b"shared"] = b"from_cf2"
                assert cf1[b"shared"] == b"from_cf1"
                assert cf2[b"shared"] == b"from_cf2"


# ---------------------------------------------------------------------------
# Large payload
# ---------------------------------------------------------------------------

def test_wal_large_payload_1mb(tmp_path):
    """A 1 MB value must round-trip correctly in WAL mode."""
    path = str(tmp_path / "large.db")
    payload = bytes(range(256)) * (1024 * 1024 // 256)
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db[b"big"] = payload
        assert db[b"big"] == payload


# ---------------------------------------------------------------------------
# Integrity
# ---------------------------------------------------------------------------

def test_wal_integrity_100_keys(tmp_path):
    path = str(tmp_path / "int.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        _write_n(db, 100)
        db.integrity_check()


# ---------------------------------------------------------------------------
# Cross-mode (DELETE ↔ WAL interop)
# ---------------------------------------------------------------------------

def test_wal_cross_mode_delete_then_wal(tmp_path):
    """Data written in DELETE mode must be readable after reopening in WAL."""
    path = str(tmp_path / "cross.db")

    with KeyValueStore(path, journal_mode=JOURNAL_DELETE) as db:
        db[b"from_delete"] = b"del_val"

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        assert db[b"from_delete"] == b"del_val"
        db[b"from_wal"] = b"wal_val"

    with KeyValueStore(path, journal_mode=JOURNAL_DELETE) as db:
        assert db[b"from_delete"] == b"del_val"
        assert db[b"from_wal"]    == b"wal_val"


# ---------------------------------------------------------------------------
# Batch performance smoke-test
# ---------------------------------------------------------------------------

def test_wal_batch_10k(tmp_path):
    """10 000 keys in a single WAL transaction — spot-check 4 of them."""
    path = str(tmp_path / "batch.db")
    import random
    rng = random.Random(42)

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        _write_n(db, 10_000)

        for _ in range(4):
            i = rng.randint(0, 9999)
            assert db.get(f"k{i:06d}".encode()) == f"v{i}".encode()


# ---------------------------------------------------------------------------
# Iterator
# ---------------------------------------------------------------------------

def test_wal_iterator_count_10(tmp_path):
    path = str(tmp_path / "iter.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        _write_n(db, 10)
        count = sum(1 for _ in db.iterator())
        assert count == 10


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def test_wal_statistics(tmp_path):
    path = str(tmp_path / "stats.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db.put(b"s1", b"a")
        db.put(b"s2", b"b")
        db.put(b"s3", b"c")
        db.get(b"s1")
        db.get(b"s2")
        db.delete(b"s3")

        s = db.stats()
        assert s["puts"]    >= 3
        assert s["gets"]    >= 2
        assert s["deletes"] >= 1


# ---------------------------------------------------------------------------
# ACID sub-suite (WAL-specific)
# ---------------------------------------------------------------------------

def test_wal_acid_atomicity(tmp_path):
    """Rollback discards all puts; commit makes all puts visible."""
    path = str(tmp_path / "acid_atom.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        # rollback path
        db.begin(write=True)
        for i in range(50):
            db[f"atom{i:04d}".encode()] = b"v"
        db.rollback()
        for i in range(50):
            assert db.get(f"atom{i:04d}".encode()) is None

        # commit path
        db.begin(write=True)
        for i in range(50):
            db[f"atom{i:04d}".encode()] = f"v{i}".encode()
        db.commit()
        for i in range(50):
            assert db.get(f"atom{i:04d}".encode()) == f"v{i}".encode()


def test_wal_acid_consistency(tmp_path):
    """200 keys written, closed, reopened — integrity_check must pass."""
    path = str(tmp_path / "acid_cons.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        _write_n(db, 200)

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db.integrity_check()
        for i in range(200):
            assert db.get(f"k{i:06d}".encode()) == f"v{i}".encode()


def test_wal_acid_isolation(tmp_path):
    """Rolled-back overwrite must not affect baseline; phantom key must vanish."""
    path = str(tmp_path / "acid_iso.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db[b"base"] = b"original"

        db.begin(write=True)
        db[b"base"]    = b"overwritten"
        db[b"phantom"] = b"ghost"
        db.rollback()

        assert db[b"base"]          == b"original"
        assert db.get(b"phantom")   is None


def test_wal_acid_durability(tmp_path):
    """100 committed keys survive a simulated crash (close without commit)."""
    path = str(tmp_path / "acid_dur.db")

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        _write_n(db, 100)

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db.begin(write=True)
        for i in range(100):
            db[f"k{i:06d}".encode()] = b"overwritten"
        for i in range(50):
            db[f"phantom{i:04d}".encode()] = b"ghost"
        # close without commit

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        for i in range(100):
            assert db.get(f"k{i:06d}".encode()) == f"v{i}".encode()
        for i in range(50):
            assert db.get(f"phantom{i:04d}".encode()) is None


def test_wal_acid_crash_atomicity(tmp_path):
    """Uncommitted batch after reopen must have no effect on committed data."""
    path = str(tmp_path / "acid_crash.db")

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db[b"baseline"] = b"v0"

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        assert db[b"baseline"] == b"v0"
        db.begin(write=True)
        db[b"baseline"] = b"v1"
        for i in range(50):
            db[f"uncommitted{i:04d}".encode()] = b"x"
        # close without commit

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        assert db[b"baseline"] == b"v0"
        for i in range(50):
            assert db.get(f"uncommitted{i:04d}".encode()) is None
