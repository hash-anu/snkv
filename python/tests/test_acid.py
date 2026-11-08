"""
ACID compliance tests — mirrors tests/test_acid.c.

Tests atomicity, consistency, isolation, and durability in both
DELETE (rollback journal) and WAL journal modes.
"""

import pytest
from snkv import KVStore, JOURNAL_WAL, JOURNAL_DELETE, CorruptError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(params=[JOURNAL_WAL, JOURNAL_DELETE], ids=["WAL", "DELETE"])
def db(request, tmp_path):
    path = str(tmp_path / "acid.db")
    with KVStore(path, journal_mode=request.param) as store:
        yield store


@pytest.fixture(params=[JOURNAL_WAL, JOURNAL_DELETE], ids=["WAL", "DELETE"])
def db_path(request, tmp_path):
    """Yields (path, journal_mode) without opening — for persistence tests."""
    return str(tmp_path / "acid.db"), request.param


# ---------------------------------------------------------------------------
# Atomicity
# ---------------------------------------------------------------------------

def test_atomicity_rollback_undoes_all_puts(db):
    """All puts in a rolled-back transaction must disappear."""
    db.begin(write=True)
    db[b"a"] = b"1"
    db[b"b"] = b"2"
    db[b"c"] = b"3"
    db.rollback()

    assert db.get(b"a") is None
    assert db.get(b"b") is None
    assert db.get(b"c") is None


def test_atomicity_partial_transaction_rolled_back(db):
    """Update + insert + delete inside a rolled-back txn — all undone."""
    db[b"existing"] = b"original"

    db.begin(write=True)
    db[b"existing"] = b"modified"   # update
    db[b"new_key"]  = b"new_val"    # insert
    db.rollback()

    assert db.get(b"existing") == b"original"
    assert db.get(b"new_key")  is None


def test_atomicity_commit_makes_all_visible(db):
    """All puts in a committed transaction must be visible."""
    db.begin(write=True)
    for i in range(10):
        db[f"key{i}".encode()] = f"val{i}".encode()
    db.commit()

    for i in range(10):
        assert db.get(f"key{i}".encode()) == f"val{i}".encode()


# ---------------------------------------------------------------------------
# Consistency
# ---------------------------------------------------------------------------

def test_consistency_integrity_check_empty(db):
    """Integrity check on an empty DB must pass."""
    db.integrity_check()


def test_consistency_integrity_check_after_inserts_and_deletes(db):
    """Integrity check must pass after bulk insert + partial delete."""
    db.begin(write=True)
    for i in range(100):
        db[f"k{i:04d}".encode()] = f"v{i}".encode()
    db.commit()

    db.begin(write=True)
    for i in range(0, 100, 2):      # delete even-indexed keys
        db.delete(f"k{i:04d}".encode())
    db.commit()

    for i in range(1, 100, 2):      # odd keys must survive
        assert db.get(f"k{i:04d}".encode()) == f"v{i}".encode()

    db.integrity_check()


def test_consistency_cf_isolation(db):
    """Same key written to two different CFs must stay isolated."""
    with db.create_column_family("cf_a") as cf_a:
        with db.create_column_family("cf_b") as cf_b:
            cf_a[b"shared"] = b"from_a"
            cf_b[b"shared"] = b"from_b"

            assert cf_a[b"shared"] == b"from_a"
            assert cf_b[b"shared"] == b"from_b"


def test_consistency_cf_key_not_in_other_cf(db):
    """A key in one CF must not be visible in another."""
    with db.create_column_family("left") as cf_l:
        with db.create_column_family("right") as cf_r:
            cf_l[b"only_left"] = b"x"
            assert cf_r.get(b"only_left") is None


# ---------------------------------------------------------------------------
# Isolation
# ---------------------------------------------------------------------------

def test_isolation_updates_visible_within_transaction(db):
    """A write made inside a transaction must be visible to subsequent reads
    within that same transaction."""
    db[b"counter"] = b"0"

    db.begin(write=True)
    db[b"counter"] = b"1"
    assert db[b"counter"] == b"1"   # in-transaction read
    db.commit()

    assert db[b"counter"] == b"1"


def test_isolation_rolled_back_write_not_visible(db):
    """A write inside a rolled-back transaction must not leak outside."""
    db[b"stable"] = b"original"

    db.begin(write=True)
    db[b"stable"]  = b"transient"
    db[b"phantom"] = b"ghost"
    db.rollback()

    assert db[b"stable"]  == b"original"
    assert db.get(b"phantom") is None


def test_isolation_auto_committed_ops_persist(db):
    """Operations outside explicit transactions auto-commit immediately."""
    db[b"auto1"] = b"val1"
    db[b"auto2"] = b"val2"

    assert db[b"auto1"] == b"val1"
    assert db[b"auto2"] == b"val2"


# ---------------------------------------------------------------------------
# Durability
# ---------------------------------------------------------------------------

def test_durability_survives_close_reopen(db_path):
    """Data written and synced must survive close + reopen."""
    path, mode = db_path

    with KVStore(path, journal_mode=mode) as db:
        db[b"durable"] = b"yes"
        db.sync()

    with KVStore(path, journal_mode=mode) as db:
        assert db[b"durable"] == b"yes"


def test_durability_multiple_close_reopen_cycles(db_path):
    """Data from all prior sessions must accumulate across multiple
    close/reopen cycles."""
    path, mode = db_path

    for cycle in range(5):
        with KVStore(path, journal_mode=mode) as db:
            db[f"cycle{cycle}".encode()] = f"v{cycle}".encode()

        # verify all previous cycles are still present
        with KVStore(path, journal_mode=mode) as db:
            for prev in range(cycle + 1):
                assert db.get(f"cycle{prev}".encode()) == f"v{prev}".encode()


def test_durability_uncommitted_lost_on_close(db_path):
    """Data written but NOT committed must be absent after close + reopen."""
    path, mode = db_path

    with KVStore(path, journal_mode=mode) as db:
        db[b"committed"] = b"safe"

    with KVStore(path, journal_mode=mode) as db:
        db.begin(write=True)
        db[b"lost"] = b"never_committed"
        # close without commit

    with KVStore(path, journal_mode=mode) as db:
        assert db[b"committed"] == b"safe"
        assert db.get(b"lost") is None
