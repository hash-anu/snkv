"""
TTL (Time-To-Live) test suite for the snkv Python bindings.

Run with:
    pytest python/tests/test_ttl.py
or from the python/ directory:
    pytest tests/test_ttl.py
"""

import time
import pytest
import snkv
from snkv import KVStore, NotFoundError, NO_TTL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "ttl.db")
    with KVStore(path) as store:
        yield store


@pytest.fixture
def db_path(tmp_path):
    """Return a path string for tests that open/close manually."""
    return str(tmp_path / "ttl_reopen.db")


# ---------------------------------------------------------------------------
# put() with ttl=
# ---------------------------------------------------------------------------

def test_put_ttl_get_before_expiry(db):
    """Key inserted with ttl is readable while still live."""
    db.put(b"session", b"tok123", ttl=60)
    assert db.get(b"session") == b"tok123"


def test_put_ttl_getitem_before_expiry(db):
    """db[key] also works while key is live."""
    db.put(b"k", b"v", ttl=60)
    assert db[b"k"] == b"v"


def test_put_ttl_str_key(db):
    """str keys are UTF-8 encoded; TTL still works."""
    db.put("greeting", "hello", ttl=60)
    assert db.get("greeting") == b"hello"


def test_put_ttl_expiry_get_returns_none(db):
    """Expired key: get() returns None (lazy delete performed)."""
    db.put(b"short", b"lived", ttl=0.001)
    time.sleep(0.05)
    assert db.get(b"short") is None


def test_put_ttl_expiry_getitem_raises_keyerror(db):
    """Expired key: db[key] raises KeyError."""
    db.put(b"short2", b"lived", ttl=0.001)
    time.sleep(0.05)
    with pytest.raises(KeyError):
        _ = db[b"short2"]


def test_put_ttl_lazy_delete_removes_from_exists(db):
    """After expiry and a get(), the key no longer exists."""
    db.put(b"ev", b"data", ttl=0.001)
    time.sleep(0.05)
    db.get(b"ev")  # trigger lazy delete
    assert not db.exists(b"ev")


def test_put_no_ttl_behaves_as_normal(db):
    """put() without ttl= still works as before."""
    db.put(b"perm", b"forever")
    assert db.get(b"perm") == b"forever"


def test_put_ttl_overwrite_with_regular_put_removes_ttl(db):
    """Overwriting a TTL key with a plain put() removes the TTL entry."""
    db.put(b"k", b"v1", ttl=60)
    db.put(b"k", b"v2")          # plain put — should clear TTL
    assert db.get(b"k") == b"v2"
    # TTL must be gone: ttl() should return None
    assert db.ttl(b"k") is None


def test_put_ttl_overwrite_with_longer_ttl(db):
    """A second put_ttl replaces the first expiry."""
    db.put(b"k", b"v1", ttl=0.01)
    db.put(b"k", b"v2", ttl=60)   # extend lifetime
    time.sleep(0.05)
    assert db.get(b"k") == b"v2"  # must still be alive


# ---------------------------------------------------------------------------
# ttl() method
# ---------------------------------------------------------------------------

def test_ttl_positive_for_live_key(db):
    """ttl() returns a positive float for a key with active expiry."""
    db.put(b"k", b"v", ttl=10)
    remaining = db.ttl(b"k")
    assert remaining is not None
    assert 0 < remaining <= 10


def test_ttl_none_for_permanent_key(db):
    """ttl() returns None when the key has no expiry."""
    db.put(b"perm", b"data")
    assert db.ttl(b"perm") is None


def test_ttl_zero_for_just_expired_key(db):
    """ttl() returns 0.0 when the key has expired (and performs lazy delete)."""
    db.put(b"k", b"v", ttl=0.001)
    time.sleep(0.05)
    assert db.ttl(b"k") == 0.0


def test_ttl_missing_key_raises_not_found(db):
    """ttl() raises NotFoundError when the key does not exist at all."""
    with pytest.raises(NotFoundError):
        db.ttl(b"nonexistent")


def test_ttl_missing_key_is_key_error(db):
    """NotFoundError IS-A KeyError — except KeyError works."""
    with pytest.raises(KeyError):
        db.ttl(b"nonexistent")


def test_ttl_returns_float_seconds(db):
    """ttl() returns seconds as a float, not milliseconds."""
    db.put(b"k", b"v", ttl=5)
    remaining = db.ttl(b"k")
    assert isinstance(remaining, float)
    # Must be in the seconds range, not milliseconds range
    assert remaining < 10


# ---------------------------------------------------------------------------
# purge_expired()
# ---------------------------------------------------------------------------

def test_purge_expired_deletes_expired_keys(db):
    """purge_expired() removes all expired keys and returns the count."""
    for i in range(5):
        db.put(f"exp:{i}".encode(), b"val", ttl=0.001)
    # Insert a key that should survive
    db.put(b"live", b"yes", ttl=60)
    time.sleep(0.05)

    n = db.purge_expired()
    assert n == 5
    assert db.get(b"live") == b"yes"


def test_purge_expired_no_expired_returns_zero(db):
    """purge_expired() returns 0 when there is nothing to purge."""
    db.put(b"k", b"v", ttl=60)
    assert db.purge_expired() == 0


def test_purge_expired_on_store_without_ttl(db):
    """purge_expired() on a store that never used TTL returns 0, no error."""
    db.put(b"k", b"v")
    assert db.purge_expired() == 0


def test_purge_expired_orphan_ttl_cleaned(db):
    """purge_expired() removes orphaned TTL entries for expired keys."""
    db.put(b"gone", b"v", ttl=0.001)
    time.sleep(0.05)
    n = db.purge_expired()
    assert n >= 1
    assert db.get(b"gone") is None


def test_purge_expired_mixed_batch(db):
    """Only expired keys are removed; live ones survive."""
    for i in range(3):
        db.put(f"dead:{i}".encode(), b"x", ttl=0.001)
    for i in range(4):
        db.put(f"live:{i}".encode(), b"y", ttl=60)
    time.sleep(0.05)

    n = db.purge_expired()
    assert n == 3
    for i in range(4):
        assert db.get(f"live:{i}".encode()) == b"y"


# ---------------------------------------------------------------------------
# Column family / internal CF protection
# ---------------------------------------------------------------------------

def test_cf_list_does_not_show_ttl_cf(db):
    """__snkv_ttl__ never appears in list_column_families()."""
    db.put(b"k", b"v", ttl=60)   # trigger creation of __snkv_ttl__
    names = db.list_column_families()
    for name in names:
        assert not name.startswith("__"), f"Internal CF leaked: {name!r}"


def test_cf_create_reserved_prefix_raises(db):
    """Creating a column family with a __ prefix raises snkv.Error."""
    with pytest.raises(snkv.Error):
        db.create_column_family("__reserved__")


def test_cf_open_reserved_prefix_raises(db):
    """Opening a column family with a __ prefix raises snkv.Error."""
    with pytest.raises(snkv.Error):
        db.open_column_family("__snkv_ttl__")


# ---------------------------------------------------------------------------
# TTL inside explicit transactions
# ---------------------------------------------------------------------------

def test_put_ttl_inside_transaction(db):
    """put(ttl=...) inside an explicit transaction is atomic."""
    db.begin(write=True)
    db.put(b"tx_key", b"tx_val", ttl=60)
    db.commit()

    assert db.get(b"tx_key") == b"tx_val"
    remaining = db.ttl(b"tx_key")
    assert remaining is not None and remaining > 0


def test_put_ttl_rollback_removes_both(db):
    """Rolling back a transaction rolls back the data and TTL entry."""
    db.begin(write=True)
    db.put(b"rolled", b"back", ttl=60)
    db.rollback()

    assert db.get(b"rolled") is None
    with pytest.raises(NotFoundError):
        db.ttl(b"rolled")


# ---------------------------------------------------------------------------
# NO_TTL constant
# ---------------------------------------------------------------------------

def test_no_ttl_constant_value():
    """NO_TTL is -1 as documented."""
    assert NO_TTL == -1


def test_no_ttl_is_int():
    assert isinstance(NO_TTL, int)


# ---------------------------------------------------------------------------
# Persistence across reopen
# ---------------------------------------------------------------------------

def test_ttl_survives_store_reopen(db_path):
    """TTL entries are persisted and expire correctly after a store reopen."""
    with KVStore(db_path) as db:
        db.put(b"persistent", b"value", ttl=60)

    with KVStore(db_path) as db:
        assert db.get(b"persistent") == b"value"
        remaining = db.ttl(b"persistent")
        assert remaining is not None and 0 < remaining <= 60


def test_expired_key_gone_after_reopen(db_path):
    """A key that expires is not returned after the store is reopened."""
    with KVStore(db_path) as db:
        db.put(b"short", b"lived", ttl=0.001)

    time.sleep(0.05)

    with KVStore(db_path) as db:
        assert db.get(b"short") is None


def test_purge_expired_after_reopen(db_path):
    """purge_expired() works correctly on a reopened store."""
    with KVStore(db_path) as db:
        for i in range(3):
            db.put(f"e:{i}".encode(), b"v", ttl=0.001)

    time.sleep(0.05)

    with KVStore(db_path) as db:
        n = db.purge_expired()
        assert n == 3
