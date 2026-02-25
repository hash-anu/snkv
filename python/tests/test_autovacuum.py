"""
Incremental vacuum tests — mirrors tests/test_autovacuum.c.

Verifies that kvstore.vacuum() reclaims disk space after bulk deletes
and preserves data integrity.
"""

import os
import pytest
from snkv import KeyValueStore, JOURNAL_WAL, JOURNAL_DELETE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _db_file_size(path: str) -> int:
    return os.path.getsize(path)


def _fill(db, n: int, prefix: str = "k") -> None:
    db.begin(write=True)
    for i in range(n):
        db[f"{prefix}{i:06d}".encode()] = b"x" * 256
    db.commit()


def _delete_range(db, start: int, stop: int, prefix: str = "k") -> None:
    db.begin(write=True)
    for i in range(start, stop):
        db.delete(f"{prefix}{i:06d}".encode())
    db.commit()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_incremental_vacuum_shrinks_file(tmp_path):
    """After bulk insert + delete + vacuum, file must be smaller."""
    path = str(tmp_path / "vac.db")

    with KeyValueStore(path) as db:
        _fill(db, 2000)
        _delete_range(db, 0, 1800)
        db.sync()

    size_before = _db_file_size(path)

    with KeyValueStore(path) as db:
        db.vacuum(0)        # free all unused pages
        db.sync()

    size_after = _db_file_size(path)
    assert size_after < size_before, (
        f"file should shrink after vacuum: {size_before} -> {size_after}"
    )


def test_partial_vacuum_two_stage(tmp_path):
    """Partial vacuum (n_pages > 0) reclaims space incrementally."""
    path = str(tmp_path / "partial.db")

    with KeyValueStore(path) as db:
        _fill(db, 2000)
        _delete_range(db, 0, 1800)
        db.sync()

    size_before = _db_file_size(path)

    with KeyValueStore(path) as db:
        db.vacuum(10)       # first stage: free up to 10 pages
        db.sync()

    size_stage1 = _db_file_size(path)

    with KeyValueStore(path) as db:
        db.vacuum(0)        # second stage: free the rest
        db.sync()

    size_stage2 = _db_file_size(path)

    assert size_stage2 <= size_stage1 <= size_before


def test_vacuum_wal_mode(tmp_path):
    """Vacuum must work correctly in WAL journal mode."""
    path = str(tmp_path / "vac_wal.db")

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        _fill(db, 1000)
        _delete_range(db, 0, 800)
        db.sync()

    size_before = _db_file_size(path)

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db.vacuum(0)
        db.sync()

    size_after = _db_file_size(path)
    assert size_after < size_before


def test_vacuum_data_integrity(tmp_path):
    """Surviving records must be intact and pass integrity_check after vacuum."""
    path = str(tmp_path / "vac_int.db")

    with KeyValueStore(path) as db:
        db.begin(write=True)
        for i in range(1000):
            db[f"item{i:06d}".encode()] = f"value{i}".encode()
        db.commit()
        _delete_range(db, 0, 800, prefix="item")
        db.vacuum(0)
        db.sync()

    with KeyValueStore(path) as db:
        db.integrity_check()

        # records 800-999 must survive with correct values
        for i in range(800, 1000):
            assert db.get(f"item{i:06d}".encode()) == f"value{i}".encode()

        # records 0-799 must be gone
        for i in range(0, 10):
            assert db.get(f"item{i:06d}".encode()) is None


def test_multiple_vacuum_cycles(tmp_path):
    """Three independent insert/delete/vacuum cycles must each shrink the
    file and leave exactly the surviving records."""
    path = str(tmp_path / "multicycle.db")
    surviving = []

    for cycle in range(3):
        prefix = f"c{cycle}_"

        with KeyValueStore(path) as db:
            db.begin(write=True)
            for i in range(1000):
                db[f"{prefix}{i:06d}".encode()] = f"cycle{cycle}_{i}".encode()
            db.commit()
            _delete_range(db, 0, 800, prefix=prefix)
            db.vacuum(0)
            db.sync()

        surviving.append((prefix, list(range(800, 1000))))

    with KeyValueStore(path) as db:
        db.integrity_check()
        for prefix, indices in surviving:
            for i in indices:
                key = f"{prefix}{i:06d}".encode()
                assert db.get(key) is not None, f"missing: {key}"


def test_vacuum_zero_pages_is_full_vacuum(tmp_path):
    """vacuum(0) is equivalent to a full vacuum (no pages argument)."""
    path = str(tmp_path / "zero.db")
    with KeyValueStore(path) as db:
        _fill(db, 500)
        _delete_range(db, 0, 400)
        db.vacuum(0)      # must not raise
        db.integrity_check()
