"""
Production-level correctness tests — mirrors tests/test_prod.c.

Covers the full API surface: CRUD, update, transactions, batch ops,
iterators, large data, error handling, persistence, statistics,
integrity, and a performance smoke-test.
"""

import os
import time
import pytest
import snkv
from snkv import KVStore, NotFoundError, JOURNAL_WAL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "prod.db")
    with KVStore(path) as store:
        yield store


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "prod_persist.db")


# ---------------------------------------------------------------------------
# Open / close
# ---------------------------------------------------------------------------

def test_open_close(tmp_path):
    """KVStore opens and closes without error."""
    path = str(tmp_path / "oc.db")
    db = KVStore(path)
    db.close()


# ---------------------------------------------------------------------------
# Basic CRUD
# ---------------------------------------------------------------------------

def test_basic_crud(db):
    db[b"hello"] = b"world"
    assert db.exists(b"hello")
    assert db[b"hello"] == b"world"
    db.delete(b"hello")
    assert not db.exists(b"hello")


def test_update_longer_value(db):
    """Overwriting a key with a longer value must return the new value."""
    db[b"key"] = b"short"
    db[b"key"] = b"a_much_longer_value_than_before"
    assert db[b"key"] == b"a_much_longer_value_than_before"


def test_update_shorter_value(db):
    """Overwriting with a shorter value must return the shorter value."""
    db[b"key"] = b"long_initial_value"
    db[b"key"] = b"tiny"
    assert db[b"key"] == b"tiny"


def test_overwrite_100_times(db):
    """100 successive overwrites must leave only the last value."""
    for i in range(100):
        db[b"counter"] = f"version-{i}".encode()
    assert db[b"counter"] == b"version-99"


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------

def test_transaction_commit_persists(db):
    db.begin(write=True)
    db[b"tx"] = b"committed"
    db.commit()
    assert db[b"tx"] == b"committed"


def test_transaction_rollback_discards(db):
    db.begin(write=True)
    db[b"tx"] = b"not_committed"
    db.rollback()
    assert db.get(b"tx") is None


def test_batch_100_keys_in_one_transaction(db):
    """100 puts in a single transaction must all be readable after commit."""
    db.begin(write=True)
    for i in range(100):
        db[f"batch{i:04d}".encode()] = f"val{i}".encode()
    db.commit()

    for i in range(100):
        assert db.get(f"batch{i:04d}".encode()) == f"val{i}".encode()


# ---------------------------------------------------------------------------
# Iterator
# ---------------------------------------------------------------------------

def test_iterator_count(db):
    """Iterator must visit all inserted keys exactly once."""
    db.begin(write=True)
    for i in range(10):
        db[f"iter{i}".encode()] = b"v"
    db.commit()

    count = sum(1 for _ in db.iterator())
    assert count == 10


def test_iterator_key_value_accessible(db):
    """Iterator must yield correct key and value."""
    db[b"k1"] = b"v1"
    db[b"k2"] = b"v2"

    result = {k: v for k, v in db.iterator()}
    assert result[b"k1"] == b"v1"
    assert result[b"k2"] == b"v2"


def test_iterator_empty_returns_nothing(db):
    assert list(db.iterator()) == []


# ---------------------------------------------------------------------------
# Large data
# ---------------------------------------------------------------------------

def test_large_value_1mb(db):
    """A 1 MB value must round-trip byte-for-byte."""
    payload = bytes(range(256)) * (1024 * 1024 // 256)   # exactly 1 MB
    db[b"large"] = payload
    assert db[b"large"] == payload


def test_large_key(db):
    """A 1 KB key must work correctly."""
    key = b"K" * 1024
    db[key] = b"value_for_large_key"
    assert db[key] == b"value_for_large_key"


def test_large_key_and_value(db):
    """1 KB key + 64 KB value must round-trip correctly."""
    key   = bytes(range(256)) * 4          # 1 KB
    value = bytes(range(256)) * 256        # 64 KB
    db[key] = value
    assert db[key] == value


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

def test_get_nonexistent_returns_none(db):
    assert db.get(b"no_such_key") is None


def test_getitem_nonexistent_raises_key_error(db):
    with pytest.raises(KeyError):
        _ = db[b"no_such_key"]


def test_delete_nonexistent_raises(db):
    with pytest.raises(KeyError):
        db.delete(b"no_such_key")


def test_not_found_is_key_error(db):
    with pytest.raises(NotFoundError):
        _ = db[b"nope"]
    assert issubclass(NotFoundError, KeyError)


# ---------------------------------------------------------------------------
# Persistence across sessions
# ---------------------------------------------------------------------------

def test_persistence_close_reopen(db_path):
    """Data written in session 1 must be readable in session 2."""
    with KVStore(db_path) as db:
        db[b"persistent"] = b"yes_it_is"

    with KVStore(db_path) as db:
        assert db[b"persistent"] == b"yes_it_is"


def test_persistence_100_keys(db_path):
    with KVStore(db_path) as db:
        db.begin(write=True)
        for i in range(100):
            db[f"p{i:04d}".encode()] = f"val{i}".encode()
        db.commit()

    with KVStore(db_path) as db:
        for i in range(100):
            assert db.get(f"p{i:04d}".encode()) == f"val{i}".encode()


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def test_statistics_counts(db):
    """Stats must count puts, gets, and deletes accurately."""
    db.put(b"s1", b"a")
    db.put(b"s2", b"b")
    db.get(b"s1")
    db.delete(b"s2")

    s = db.stats()
    assert s["puts"]    >= 2
    assert s["gets"]    >= 1
    assert s["deletes"] >= 1


def test_statistics_iterations(db):
    for i in range(5):
        db[f"t{i}".encode()] = b"v"
    list(db.iterator())

    s = db.stats()
    assert s["iterations"] >= 1


# ---------------------------------------------------------------------------
# Integrity check
# ---------------------------------------------------------------------------

def test_integrity_check_50_keys(db):
    """integrity_check must pass after 50 puts in a transaction."""
    db.begin(write=True)
    for i in range(50):
        db[f"int{i:04d}".encode()] = f"v{i}".encode()
    db.commit()
    db.integrity_check()   # must not raise


def test_integrity_check_empty_db(db):
    db.integrity_check()   # must not raise


# ---------------------------------------------------------------------------
# Performance smoke-test (not a hard timing assertion)
# ---------------------------------------------------------------------------

def test_performance_10k_writes(tmp_path):
    """10 000 sequential writes + 10 000 random reads: just verify they run
    without error and all reads return correct values."""
    path = str(tmp_path / "perf.db")
    N = 10_000

    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        t0 = time.monotonic()
        db.begin(write=True)
        for i in range(N):
            db[f"perf{i:07d}".encode()] = f"val{i}".encode()
        db.commit()
        write_s = time.monotonic() - t0

        t1 = time.monotonic()
        import random
        rng = random.Random(42)
        for _ in range(N):
            i = rng.randint(0, N - 1)
            assert db.get(f"perf{i:07d}".encode()) == f"val{i}".encode()
        read_s = time.monotonic() - t1

    # print for informational purposes (not a pass/fail threshold)
    print(
        f"\n  write: {N / write_s:,.0f} ops/s  "
        f"  read: {N / read_s:,.0f} ops/s"
    )
