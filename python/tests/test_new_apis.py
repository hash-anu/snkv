"""
Tests for new SNKV Python APIs:
  Iterator.seek(), KVStore/ColumnFamily.put_if_absent, .clear(), .count(),
  KVStore.stats_reset(), extended stats dict.

Run with:
    pytest python/tests/test_new_apis.py
"""

import time
import pytest
import snkv
from snkv import KVStore, NotFoundError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "new_apis.db")
    with KVStore(path) as store:
        yield store


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "new_apis_reopen.db")


# ===========================================================================
# Iterator.seek()
# ===========================================================================

def test_seek_forward_existing(db):
    """seek() positions at exact matching key (forward iterator)."""
    db.put(b"aaa", b"v1")
    db.put(b"bbb", b"v2")
    db.put(b"ccc", b"v3")

    with db.iterator() as it:
        it.first()
        it.seek(b"bbb")
        assert not it.eof
        assert it.key == b"bbb"
        assert it.value == b"v2"


def test_seek_forward_between(db):
    """seek() lands on next key >= target when exact key absent."""
    db.put(b"aaa", b"v1")
    db.put(b"ccc", b"v3")

    with db.iterator() as it:
        it.first()
        it.seek(b"bbb")
        assert not it.eof
        assert it.key == b"ccc"


def test_seek_forward_past_last(db):
    """seek() past last key → eof."""
    db.put(b"aaa", b"v1")
    db.put(b"bbb", b"v2")

    with db.iterator() as it:
        it.first()
        it.seek(b"zzz")
        assert it.eof


def test_seek_forward_before_first(db):
    """seek() before all keys → positions at first key."""
    db.put(b"mmm", b"v1")
    db.put(b"nnn", b"v2")

    with db.iterator() as it:
        it.first()
        it.seek(b"aaa")
        assert not it.eof
        assert it.key == b"mmm"


def test_seek_then_next(db):
    """seek() then next() traverses from seeked position."""
    for k in (b"aaa", b"bbb", b"ccc", b"ddd"):
        db.put(k, b"v")

    with db.iterator() as it:
        it.seek(b"bbb")
        assert it.key == b"bbb"
        it.next()
        assert it.key == b"ccc"
        it.next()
        assert it.key == b"ddd"
        it.next()
        assert it.eof


def test_seek_empty(db):
    """seek() on empty store → eof."""
    with db.iterator() as it:
        it.seek(b"key")
        assert it.eof


def test_seek_reverse_existing(db):
    """seek() on reverse iterator → positions at exact key."""
    for k in (b"aaa", b"bbb", b"ccc"):
        db.put(k, b"v")

    with db.iterator(reverse=True) as it:
        it.last()
        it.seek(b"bbb")
        assert not it.eof
        assert it.key == b"bbb"


def test_seek_reverse_between(db):
    """reverse seek() lands on nearest key <= target."""
    db.put(b"aaa", b"v1")
    db.put(b"ccc", b"v3")

    with db.iterator(reverse=True) as it:
        it.last()
        it.seek(b"bbb")
        assert not it.eof
        assert it.key == b"aaa"


def test_seek_reverse_before_first(db):
    """reverse seek() before all keys → eof."""
    db.put(b"mmm", b"v1")
    db.put(b"nnn", b"v2")

    with db.iterator(reverse=True) as it:
        it.last()
        it.seek(b"aaa")
        assert it.eof


def test_seek_multiple_times(db):
    """Multiple sequential seeks reposition correctly."""
    for k in (b"aaa", b"bbb", b"ccc"):
        db.put(k, b"v")

    with db.iterator() as it:
        it.seek(b"ccc")
        assert it.key == b"ccc"
        it.seek(b"aaa")
        assert it.key == b"aaa"
        it.seek(b"bbb")
        assert it.key == b"bbb"


def test_seek_prefix_within(db):
    """seek() on prefix iterator within prefix stays within prefix."""
    db.put(b"pfx:a", b"v1")
    db.put(b"pfx:b", b"v2")
    db.put(b"pfx:c", b"v3")
    db.put(b"qfx:d", b"v4")

    with db.iterator(prefix=b"pfx:") as it:
        it.seek(b"pfx:b")
        assert not it.eof
        assert it.key == b"pfx:b"
        it.next()
        assert it.key == b"pfx:c"
        it.next()
        assert it.eof  # boundary respected


def test_seek_chaining(db):
    """seek() returns self for chaining."""
    db.put(b"aaa", b"v1")
    db.put(b"bbb", b"v2")

    with db.iterator() as it:
        result = it.seek(b"bbb")
        assert result is it
        assert it.key == b"bbb"


# ===========================================================================
# put_if_absent
# ===========================================================================

def test_put_if_absent_absent(db):
    """put_if_absent inserts when key is absent → returns True."""
    result = db.put_if_absent(b"key", b"val")
    assert result is True
    assert db.get(b"key") == b"val"


def test_put_if_absent_present(db):
    """put_if_absent returns False when key exists, value unchanged."""
    db.put(b"key", b"original")
    result = db.put_if_absent(b"key", b"new_val")
    assert result is False
    assert db.get(b"key") == b"original"


def test_put_if_absent_with_ttl(db):
    """put_if_absent with ttl= inserts with TTL."""
    result = db.put_if_absent(b"key", b"val", ttl=3600)
    assert result is True
    remaining = db.ttl(b"key")
    assert remaining is not None
    assert 0 < remaining <= 3600


def test_put_if_absent_existing_ttl(db):
    """put_if_absent returns False if key exists (even with TTL)."""
    db.put(b"key", b"original", ttl=3600)
    result = db.put_if_absent(b"key", b"new", ttl=7200)
    assert result is False
    assert db.get(b"key") == b"original"


def test_put_if_absent_expired_treated_absent(db):
    """put_if_absent inserts when existing key has expired."""
    # Write with past TTL via the low-level API
    past_ms = int((time.time() - 1) * 1000)
    db._db.put_ttl(b"key", b"old_val", past_ms)

    result = db.put_if_absent(b"key", b"new_val")
    assert result is True
    assert db.get(b"key") == b"new_val"


def test_put_if_absent_cf(db):
    """put_if_absent works on ColumnFamily."""
    with db.create_column_family("myCF") as cf:
        assert cf.put_if_absent(b"k", b"v") is True
        assert cf.put_if_absent(b"k", b"v2") is False
        assert cf.get(b"k") == b"v"


def test_put_if_absent_in_transaction(db):
    """put_if_absent inside explicit write transaction works correctly."""
    db.begin(write=True)
    assert db.put_if_absent(b"key1", b"val1") is True
    assert db.put_if_absent(b"key1", b"val2") is False
    db.commit()
    assert db.get(b"key1") == b"val1"


# ===========================================================================
# clear
# ===========================================================================

def test_clear_default(db):
    """clear() empties the default CF."""
    for i in range(5):
        db.put(f"key{i}".encode(), b"v")
    assert db.count() == 5
    db.clear()
    assert db.count() == 0

    # Iterator sees nothing
    with db.iterator() as it:
        items = list(it)
    assert items == []


def test_clear_with_ttl(db):
    """clear() also removes TTL entries; purge_expired returns 0 after."""
    past_ms = int((time.time() - 1) * 1000)
    db._db.put_ttl(b"k1", b"v1", past_ms)
    db._db.put_ttl(b"k2", b"v2", past_ms)
    db.clear()
    assert db.purge_expired() == 0


def test_clear_empty(db):
    """clear() on empty store succeeds."""
    db.clear()  # should not raise
    assert db.count() == 0


def test_clear_then_reinsert(db):
    """After clear(), new keys can be inserted."""
    db.put(b"old", b"val")
    db.clear()
    db.put(b"new", b"val2")
    assert db.count() == 1
    assert db.get(b"new") == b"val2"
    assert db.get(b"old") is None


def test_clear_cf_isolation(db):
    """clear() on default CF does not affect other CFs."""
    db.put(b"default_key", b"dv")

    with db.create_column_family("other") as cf:
        cf.put(b"cf_key", b"cv")
        db.clear()
        assert db.get(b"default_key") is None
        assert cf.get(b"cf_key") == b"cv"


def test_clear_cf(db):
    """ColumnFamily.clear() clears only that CF."""
    with db.create_column_family("myCF") as cf:
        for i in range(4):
            cf.put(f"k{i}".encode(), b"v")
        assert cf.count() == 4
        cf.clear()
        assert cf.count() == 0


# ===========================================================================
# count
# ===========================================================================

def test_count_empty(db):
    """count() on empty CF returns 0."""
    assert db.count() == 0


def test_count_after_puts(db):
    """count() returns correct count after N puts."""
    for i in range(7):
        db.put(f"k{i}".encode(), b"v")
    assert db.count() == 7


def test_count_after_delete(db):
    """count() decrements correctly after delete."""
    db.put(b"a", b"v")
    db.put(b"b", b"v")
    db.put(b"c", b"v")
    db.delete(b"b")
    assert db.count() == 2


def test_count_includes_expired(db):
    """count() includes expired-but-not-yet-purged keys."""
    past_ms = int((time.time() - 1) * 1000)
    db._db.put_ttl(b"exp1", b"v", past_ms)
    db._db.put_ttl(b"exp2", b"v", past_ms)
    db.put(b"live", b"v")
    assert db.count() == 3


def test_count_after_purge(db):
    """count() after purge_expired reflects only live keys."""
    past_ms = int((time.time() - 1) * 1000)
    db._db.put_ttl(b"exp1", b"v", past_ms)
    db._db.put_ttl(b"exp2", b"v", past_ms)
    db.put(b"live", b"v")
    db.purge_expired()
    assert db.count() == 1


def test_count_cf_isolation(db):
    """CF count is isolated from default CF and TTL index CFs."""
    for i in range(3):
        db.put(f"d{i}".encode(), b"v")

    with db.create_column_family("myCF") as cf:
        for i in range(5):
            cf.put(f"c{i}".encode(), b"v", ttl=3600)
        assert cf.count() == 5

    assert db.count() == 3


# ===========================================================================
# Extended stats + stats_reset
# ===========================================================================

def test_stats_bytes_written(db):
    """nBytesWritten tracks key+value bytes per put."""
    db.stats_reset()
    # key=5 bytes, value=10 bytes
    db.put(b"hello", b"0123456789")
    st = db.stats()
    assert st["bytes_written"] == 15


def test_stats_bytes_read(db):
    """nBytesRead tracks value bytes per get."""
    db.put(b"k", b"hello")
    db.stats_reset()
    db.get(b"k")
    st = db.stats()
    assert st["bytes_read"] == 5


def test_stats_wal_commits(db):
    """nWalCommits increments on each committed write."""
    db.stats_reset()
    db.put(b"k1", b"v")
    db.put(b"k2", b"v")
    st = db.stats()
    assert st["wal_commits"] >= 2


def test_stats_ttl_expired(db):
    """nTtlExpired increments on lazy expiry in get_ttl."""
    past_ms = int((time.time() - 1) * 1000)
    db._db.put_ttl(b"key", b"val", past_ms)
    db.stats_reset()
    db.get(b"key")  # triggers lazy expiry
    st = db.stats()
    assert st["ttl_expired"] == 1


def test_stats_ttl_purged(db):
    """nTtlPurged increments per key deleted by purge_expired."""
    past_ms = int((time.time() - 1) * 1000)
    db._db.put_ttl(b"k1", b"v", past_ms)
    db._db.put_ttl(b"k2", b"v", past_ms)
    db.stats_reset()
    db.purge_expired()
    st = db.stats()
    assert st["ttl_purged"] == 2


def test_stats_db_pages(db):
    """nDbPages is positive after writes."""
    db.put(b"k", b"v")
    st = db.stats()
    assert st["db_pages"] > 0


def test_stats_reset(db):
    """stats_reset() zeros cumulative counters; db_pages remains > 0."""
    db.put(b"k", b"v")
    db.get(b"k")
    db.stats_reset()
    st = db.stats()
    assert st["puts"] == 0
    assert st["gets"] == 0
    assert st["bytes_read"] == 0
    assert st["bytes_written"] == 0
    assert st["wal_commits"] == 0
    assert st["db_pages"] > 0


def test_stats_has_all_keys(db):
    """stats() dict contains all expected keys."""
    st = db.stats()
    expected = {
        "puts", "gets", "deletes", "iterations", "errors",
        "bytes_read", "bytes_written", "wal_commits", "checkpoints",
        "ttl_expired", "ttl_purged", "db_pages",
    }
    assert expected <= set(st.keys())


# ===========================================================================
# clear reduces page count
# ===========================================================================

def test_clear_reduces_db_pages(tmp_path):
    """Fill store, clear, vacuum → db_pages and file size both decrease."""
    import os
    path = str(tmp_path / "clear_pages.db")
    with KVStore(path) as db:
        val = b"X" * 256
        for i in range(2000):
            db.put(f"key{i:07d}".encode(), val)

        pages_before = db.stats()["db_pages"]

        db.clear()
        db.vacuum(0)  # 0 = free all unused pages

        pages_after = db.stats()["db_pages"]

    # File is flushed and truncated after the context manager closes.
    size_before = pages_before * 4096  # page_size default is 4096
    size_after = os.path.getsize(path)

    assert pages_before > 0
    assert pages_after < pages_before
    assert size_before > 0
    assert size_after < size_before
