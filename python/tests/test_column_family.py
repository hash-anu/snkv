"""
Column family tests — mirrors tests/test_columnfamily.c.

Covers CF lifecycle, data isolation, list/drop, iterators,
cross-CF transactions, and persistence across close/reopen.
"""

import pytest
import snkv
from snkv import KVStore, NotFoundError, JOURNAL_WAL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "cf.db")
    with KVStore(path) as store:
        yield store


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "cf_persist.db")


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def test_cf_create_and_reopen(db_path):
    """CFs created in one session must be openable in the next."""
    with KVStore(db_path) as db:
        with db.create_column_family("users") as cf:
            cf[b"alice"] = b"admin"
        with db.create_column_family("sessions") as _:
            pass

    with KVStore(db_path) as db:
        names = db.list_column_families()
        assert "users" in names
        assert "sessions" in names

        with db.open_column_family("users") as cf:
            assert cf[b"alice"] == b"admin"


def test_cf_default_family(db):
    """Default CF handle shares data with the top-level KVStore API."""
    with db.default_column_family() as cf:
        cf[b"via_cf"] = b"1"

    assert db[b"via_cf"] == b"1"

    db[b"via_db"] = b"2"
    with db.default_column_family() as cf:
        assert cf[b"via_db"] == b"2"


def test_cf_duplicate_creation_raises(db):
    """Creating a CF that already exists must raise an error."""
    with db.create_column_family("dupe") as _:
        pass

    with pytest.raises(snkv.Error):
        db.create_column_family("dupe")


def test_cf_open_missing_raises(db):
    """Opening a non-existent CF must raise NotFoundError."""
    with pytest.raises(snkv.Error):
        db.open_column_family("ghost")


# ---------------------------------------------------------------------------
# Isolation
# ---------------------------------------------------------------------------

def test_cf_isolation_same_key_different_values(db):
    """The same key in two CFs must hold independent values."""
    with db.create_column_family("ns1") as cf1:
        with db.create_column_family("ns2") as cf2:
            cf1[b"key"] = b"ns1_value"
            cf2[b"key"] = b"ns2_value"

            assert cf1[b"key"] == b"ns1_value"
            assert cf2[b"key"] == b"ns2_value"


def test_cf_key_absent_from_other_cf(db):
    """A key written to cf_a must not be visible in cf_b."""
    with db.create_column_family("a") as cf_a:
        with db.create_column_family("b") as cf_b:
            cf_a[b"only_in_a"] = b"x"
            assert cf_b.get(b"only_in_a") is None


def test_cf_isolated_from_default_cf(db):
    """A key in a named CF must not appear in the default CF."""
    with db.create_column_family("named") as cf:
        cf[b"hidden"] = b"y"

    assert db.get(b"hidden") is None


# ---------------------------------------------------------------------------
# List and drop
# ---------------------------------------------------------------------------

def test_cf_list_all(db):
    """list_column_families returns all created CFs."""
    for name in ("alpha", "beta", "gamma", "delta"):
        db.create_column_family(name)

    names = db.list_column_families()
    for name in ("alpha", "beta", "gamma", "delta"):
        assert name in names


def test_cf_drop_removes_from_list(db):
    """drop_column_family must remove the CF from the list."""
    for name in ("keep1", "keep2", "drop_me"):
        db.create_column_family(name)

    db.drop_column_family("drop_me")
    names = db.list_column_families()

    assert "drop_me" not in names
    assert "keep1" in names
    assert "keep2" in names


def test_cf_drop_removes_data(db, db_path):
    """Data in a dropped CF must not be accessible after reopen."""
    path = db_path
    with KVStore(path) as d:
        with d.create_column_family("temp") as cf:
            cf[b"key"] = b"val"
        d.drop_column_family("temp")

    with KVStore(path) as d:
        assert "temp" not in d.list_column_families()


# ---------------------------------------------------------------------------
# Iterators
# ---------------------------------------------------------------------------

def test_cf_iterators_are_independent(db):
    """Iterating cf_a must not yield keys from cf_b."""
    with db.create_column_family("cf_a") as cf_a:
        with db.create_column_family("cf_b") as cf_b:
            for i in range(5):
                cf_a[f"a{i}".encode()] = b"1"
            for i in range(3):
                cf_b[f"b{i}".encode()] = b"2"

            a_keys = {k for k, _ in cf_a.iterator()}
            b_keys = {k for k, _ in cf_b.iterator()}

            assert len(a_keys) == 5
            assert len(b_keys) == 3
            assert a_keys.isdisjoint(b_keys)


def test_cf_prefix_iterator_stays_within_cf(db):
    """prefix_iterator on a CF must not cross into another CF."""
    with db.create_column_family("log") as cf:
        cf[b"2024:jan:1"] = b"a"
        cf[b"2024:jan:2"] = b"b"
        cf[b"2024:feb:1"] = b"c"

    db[b"2024:jan:x"] = b"default_cf"   # same prefix, default CF

    with db.open_column_family("log") as cf:
        jan = list(cf.prefix_iterator(b"2024:jan:"))
        assert len(jan) == 2
        keys = {k for k, _ in jan}
        assert b"2024:jan:x" not in keys


# ---------------------------------------------------------------------------
# Transactions across column families
# ---------------------------------------------------------------------------

def test_cf_transaction_commit_both_cfs(db):
    """A committed transaction must persist writes to two CFs."""
    with db.create_column_family("cf1") as cf1:
        with db.create_column_family("cf2") as cf2:
            db.begin(write=True)
            cf1[b"k"] = b"cf1_val"
            cf2[b"k"] = b"cf2_val"
            db.commit()

            assert cf1[b"k"] == b"cf1_val"
            assert cf2[b"k"] == b"cf2_val"


def test_cf_transaction_rollback_both_cfs(db):
    """A rolled-back transaction must undo writes to both CFs."""
    with db.create_column_family("cf1") as cf1:
        with db.create_column_family("cf2") as cf2:
            db.begin(write=True)
            cf1[b"k"] = b"cf1_val"
            cf2[b"k"] = b"cf2_val"
            db.rollback()

            assert cf1.get(b"k") is None
            assert cf2.get(b"k") is None


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def test_cf_data_persists_across_sessions(db_path):
    """Data written to a CF in session 1 must be readable in session 2."""
    with KVStore(db_path) as db:
        with db.create_column_family("users") as cf:
            for i in range(50):
                cf[f"user:{i}".encode()] = f"data{i}".encode()

    with KVStore(db_path) as db:
        with db.open_column_family("users") as cf:
            for i in range(50):
                assert cf.get(f"user:{i}".encode()) == f"data{i}".encode()


def test_cf_wal_mode(tmp_path):
    """Column families must work correctly in WAL journal mode."""
    path = str(tmp_path / "cf_wal.db")
    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        with db.create_column_family("wf") as cf:
            cf[b"wkey"] = b"wval"
            assert cf[b"wkey"] == b"wval"

    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        with db.open_column_family("wf") as cf:
            assert cf[b"wkey"] == b"wval"
