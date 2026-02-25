"""
Basic test suite for the snkv Python bindings.

Run with:
    pytest python/tests/
or from the python/ directory:
    pytest tests/
"""

import os
import pytest
import snkv
from snkv import (
    KeyValueStore,
    ColumnFamily,
    Iterator,
    NotFoundError,
    BusyError,
    LockedError,
    CorruptError,
    JOURNAL_WAL,
    JOURNAL_DELETE,
    CHECKPOINT_PASSIVE,
    CHECKPOINT_TRUNCATE,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "test.db")
    with KeyValueStore(path) as store:
        yield store


@pytest.fixture
def db_delete(tmp_path):
    """KeyValueStore opened in DELETE (rollback) journal mode."""
    path = str(tmp_path / "delete.db")
    with KeyValueStore(path, journal_mode=JOURNAL_DELETE) as store:
        yield store


# ---------------------------------------------------------------------------
# Basic put / get / delete / exists
# ---------------------------------------------------------------------------

def test_put_get_bytes(db):
    db.put(b"hello", b"world")
    assert db.get(b"hello") == b"world"


def test_put_get_str_keys(db):
    db.put("hello", "world")
    assert db.get("hello") == b"world"


def test_get_not_found_returns_default(db):
    assert db.get(b"missing") is None
    assert db.get(b"missing", b"fallback") == b"fallback"


def test_get_not_found_raises_via_index(db):
    with pytest.raises(KeyError):
        _ = db[b"missing"]


def test_delete(db):
    db.put(b"k", b"v")
    db.delete(b"k")
    assert not db.exists(b"k")


def test_delete_missing_raises(db):
    with pytest.raises(KeyError):
        db.delete(b"nope")


def test_exists_true(db):
    db.put(b"yes", b"1")
    assert db.exists(b"yes") is True


def test_exists_false(db):
    assert db.exists(b"nope") is False


def test_overwrite(db):
    db.put(b"k", b"v1")
    db.put(b"k", b"v2")
    assert db.get(b"k") == b"v2"


def test_binary_key_and_value(db):
    key   = bytes(range(256))
    value = bytes(reversed(range(256)))
    db.put(key, value)
    assert db.get(key) == value


def test_empty_value(db):
    db.put(b"empty", b"")
    assert db.get(b"empty") == b""


# ---------------------------------------------------------------------------
# dict-like interface
# ---------------------------------------------------------------------------

def test_dict_set_get_del(db):
    db["k"] = "v"
    assert db["k"] == b"v"
    assert "k" in db
    del db["k"]
    assert "k" not in db


def test_contains_str_and_bytes(db):
    db[b"key"] = b"val"
    assert b"key" in db
    assert "key" in db
    assert b"other" not in db


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------

def test_transaction_commit(db):
    db.begin(write=True)
    db.put(b"tx", b"committed")
    db.commit()
    assert db.get(b"tx") == b"committed"


def test_transaction_rollback(db):
    db.begin(write=True)
    db.put(b"tx", b"not-committed")
    db.rollback()
    assert db.get(b"tx") is None


# ---------------------------------------------------------------------------
# Iterator (default CF)
# ---------------------------------------------------------------------------

def test_iterator_basic(db):
    db.put(b"a", b"1")
    db.put(b"b", b"2")
    db.put(b"c", b"3")

    pairs = list(db.iterator())
    keys = [k for k, v in pairs]
    assert b"a" in keys
    assert b"b" in keys
    assert b"c" in keys


def test_iterator_for_loop(db):
    db["x"] = "10"
    db["y"] = "20"

    found = {}
    for k, v in db:
        found[k] = v

    assert found[b"x"] == b"10"
    assert found[b"y"] == b"20"


def test_iterator_context_manager(db):
    db.put(b"one", b"1")
    with db.iterator() as it:
        pairs = list(it)
    assert len(pairs) >= 1


def test_iterator_empty_store(db):
    assert list(db.iterator()) == []


def test_prefix_iterator(db):
    db.put(b"user:1", b"alice")
    db.put(b"user:2", b"bob")
    db.put(b"post:1", b"hello")

    pairs = list(db.prefix_iterator(b"user:"))
    assert len(pairs) == 2
    keys = {k for k, v in pairs}
    assert b"user:1" in keys
    assert b"user:2" in keys
    assert b"post:1" not in keys


def test_prefix_iterator_no_match(db):
    db.put(b"foo", b"bar")
    assert list(db.prefix_iterator(b"zzz")) == []


def test_iterator_manual_control(db):
    db.put(b"a", b"1")
    db.put(b"b", b"2")

    it = db.iterator()
    it.first()
    assert not it.eof
    assert it.key in (b"a", b"b")
    it.next()
    it.close()


# ---------------------------------------------------------------------------
# Column families
# ---------------------------------------------------------------------------

def test_cf_create_and_use(db):
    with db.create_column_family("users") as cf:
        cf.put(b"alice", b"30")
        assert cf.get(b"alice") == b"30"
        assert cf.exists(b"alice") is True


def test_cf_no_cross_contamination(db):
    cf1 = db.create_column_family("ns1")
    cf2 = db.create_column_family("ns2")

    cf1.put(b"shared_key", b"ns1_value")
    cf2.put(b"shared_key", b"ns2_value")

    assert cf1.get(b"shared_key") == b"ns1_value"
    assert cf2.get(b"shared_key") == b"ns2_value"

    cf1.close()
    cf2.close()


def test_cf_default(db):
    cf = db.default_column_family()
    cf.put(b"via_cf", b"yes")
    cf.close()
    assert db.get(b"via_cf") == b"yes"


def test_cf_list(db):
    db.create_column_family("alpha")
    db.create_column_family("beta")

    names = db.list_column_families()
    assert "alpha" in names
    assert "beta" in names


def test_cf_drop(db):
    db.create_column_family("temp")
    db.drop_column_family("temp")
    names = db.list_column_families()
    assert "temp" not in names


def test_cf_open(db):
    db.create_column_family("persistent")
    cf = db.open_column_family("persistent")
    cf.put(b"k", b"v")
    cf.close()


def test_cf_open_missing_raises(db):
    with pytest.raises(snkv.Error):
        db.open_column_family("does_not_exist")


def test_cf_dict_interface(db):
    with db.create_column_family("dict_cf") as cf:
        cf[b"key"] = b"val"
        assert cf[b"key"] == b"val"
        assert b"key" in cf
        del cf[b"key"]
        assert b"key" not in cf


def test_cf_iterator(db):
    with db.create_column_family("iter_cf") as cf:
        cf.put(b"x", b"1")
        cf.put(b"y", b"2")
        pairs = list(cf.iterator())
        keys = {k for k, v in pairs}
        assert b"x" in keys
        assert b"y" in keys


def test_cf_prefix_iterator(db):
    with db.create_column_family("pfx_cf") as cf:
        cf.put(b"tag:a", b"1")
        cf.put(b"tag:b", b"2")
        cf.put(b"other", b"3")
        pairs = list(cf.prefix_iterator(b"tag:"))
        assert len(pairs) == 2


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def test_stats(db):
    db.put(b"k", b"v")
    db.get(b"k")
    s = db.stats()
    assert s["puts"] >= 1
    assert s["gets"] >= 1
    assert s["deletes"] >= 0
    assert s["iterations"] >= 0
    assert s["errors"] >= 0


# ---------------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------------

def test_sync(db):
    db.put(b"k", b"v")
    db.sync()  # must not raise


def test_vacuum(db):
    for i in range(10):
        db.put(f"k{i}".encode(), b"v")
    for i in range(10):
        db.delete(f"k{i}".encode())
    db.vacuum()  # must not raise


def test_integrity_check_healthy(db):
    db.put(b"k", b"v")
    db.integrity_check()  # must not raise


def test_checkpoint_wal(tmp_path):
    path = str(tmp_path / "wal.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        for i in range(50):
            db.put(f"k{i}".encode(), f"v{i}".encode())
        nlog, nckpt = db.checkpoint(CHECKPOINT_PASSIVE)
        assert nlog  >= 0
        assert nckpt >= 0


def test_checkpoint_truncate(tmp_path):
    path = str(tmp_path / "trunc.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        for i in range(50):
            db.put(f"k{i}".encode(), f"v{i}".encode())
        nlog, nckpt = db.checkpoint(CHECKPOINT_TRUNCATE)
        assert nlog == 0  # WAL truncated


def test_checkpoint_on_delete_journal(db_delete):
    # No-op on non-WAL databases; must not raise
    nlog, nckpt = db_delete.checkpoint(CHECKPOINT_PASSIVE)
    assert nlog  == 0
    assert nckpt == 0


# ---------------------------------------------------------------------------
# Configuration (open_v2 / keyword args)
# ---------------------------------------------------------------------------

def test_open_v2_via_kwargs(tmp_path):
    path = str(tmp_path / "v2.db")
    with KeyValueStore(path, cache_size=500, busy_timeout=100) as db:
        db.put(b"k", b"v")
        assert db.get(b"k") == b"v"


def test_open_in_memory():
    with KeyValueStore(None) as db:
        db.put(b"ephemeral", b"data")
        assert db.get(b"ephemeral") == b"data"


def test_open_delete_mode(tmp_path):
    path = str(tmp_path / "del.db")
    with KeyValueStore(path, journal_mode=JOURNAL_DELETE) as db:
        db.put(b"key", b"value")
        assert db.get(b"key") == b"value"


# ---------------------------------------------------------------------------
# Lifecycle / safety
# ---------------------------------------------------------------------------

def test_context_manager_closes(tmp_path):
    path = str(tmp_path / "ctx.db")
    with KeyValueStore(path) as db:
        db.put(b"key", b"val")
    # Reaching here without exception means close() was called cleanly.


def test_double_close_safe(tmp_path):
    path = str(tmp_path / "dbl.db")
    db = KeyValueStore(path)
    db.close()
    db.close()  # must not crash


def test_errmsg_property(db):
    msg = db.errmsg
    assert isinstance(msg, str)


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------

def test_not_found_is_key_error(db):
    with pytest.raises(KeyError):
        _ = db[b"nope"]


def test_not_found_is_snkv_error():
    assert issubclass(NotFoundError, snkv.Error)


def test_busy_is_snkv_error():
    assert issubclass(BusyError, snkv.Error)


def test_locked_is_snkv_error():
    assert issubclass(LockedError, snkv.Error)


def test_corrupt_is_snkv_error():
    assert issubclass(CorruptError, snkv.Error)
