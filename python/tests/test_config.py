"""
Configuration tests — mirrors tests/test_config.c.

Exercises every field of KeyValueStoreConfig (via KeyValueStore keyword args),
backward-compat open, read-only mode, and various journal/sync combos.
"""

import os
import pytest
import snkv
from snkv import (
    KeyValueStore,
    NotFoundError,
    ReadOnlyError,
    JOURNAL_WAL,
    JOURNAL_DELETE,
    SYNC_OFF,
    SYNC_NORMAL,
    SYNC_FULL,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _roundtrip(db: KeyValueStore) -> None:
    """Basic put/get round-trip to verify the store is functional."""
    db[b"cfg_key"] = b"cfg_val"
    assert db[b"cfg_key"] == b"cfg_val"


# ---------------------------------------------------------------------------
# Journal modes
# ---------------------------------------------------------------------------

def test_default_open_wal(tmp_path):
    """KeyValueStore() defaults to WAL mode and is functional."""
    path = str(tmp_path / "default.db")
    with KeyValueStore(path) as db:
        _roundtrip(db)


def test_explicit_wal_mode(tmp_path):
    """Explicitly opening in WAL mode works."""
    path = str(tmp_path / "wal.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        _roundtrip(db)


def test_delete_journal_mode(tmp_path):
    """DELETE journal mode opens and is functional."""
    path = str(tmp_path / "del.db")
    with KeyValueStore(path, journal_mode=JOURNAL_DELETE) as db:
        _roundtrip(db)


# ---------------------------------------------------------------------------
# Sync levels
# ---------------------------------------------------------------------------

def test_sync_off(tmp_path):
    path = str(tmp_path / "sync_off.db")
    with KeyValueStore(path, sync_level=SYNC_OFF) as db:
        _roundtrip(db)


def test_sync_normal(tmp_path):
    path = str(tmp_path / "sync_normal.db")
    with KeyValueStore(path, sync_level=SYNC_NORMAL) as db:
        _roundtrip(db)


def test_sync_full(tmp_path):
    path = str(tmp_path / "sync_full.db")
    with KeyValueStore(path, sync_level=SYNC_FULL) as db:
        _roundtrip(db)


# ---------------------------------------------------------------------------
# Cache size
# ---------------------------------------------------------------------------

def test_custom_cache_size(tmp_path):
    """cache_size=500 (~2 MB) must still work for 200 keys."""
    path = str(tmp_path / "cache.db")
    with KeyValueStore(path, cache_size=500) as db:
        db.begin(write=True)
        for i in range(200):
            db[f"k{i:04d}".encode()] = f"v{i}".encode()
        db.commit()
        assert db.get(b"k0099") == b"v99"


def test_large_cache_size(tmp_path):
    """cache_size=8000 (~32 MB) must open and work normally."""
    path = str(tmp_path / "bigcache.db")
    with KeyValueStore(path, cache_size=8000) as db:
        _roundtrip(db)


# ---------------------------------------------------------------------------
# Page size
# ---------------------------------------------------------------------------

def test_custom_page_size_8192(tmp_path):
    """page_size=8192 on a new DB must open and accept writes."""
    path = str(tmp_path / "page8k.db")
    with KeyValueStore(path, page_size=8192) as db:
        _roundtrip(db)


def test_custom_page_size_16384(tmp_path):
    path = str(tmp_path / "page16k.db")
    with KeyValueStore(path, page_size=16384) as db:
        _roundtrip(db)


# ---------------------------------------------------------------------------
# Busy timeout
# ---------------------------------------------------------------------------

def test_busy_timeout(tmp_path):
    """busy_timeout=500 must open successfully and function normally."""
    path = str(tmp_path / "busy.db")
    with KeyValueStore(path, busy_timeout=500) as db:
        _roundtrip(db)


# ---------------------------------------------------------------------------
# Read-only mode
# ---------------------------------------------------------------------------

def test_read_only_reads_succeed(tmp_path):
    """A DB opened read-only must allow gets."""
    path = str(tmp_path / "ro.db")
    with KeyValueStore(path) as db:
        db[b"ro_key"] = b"ro_val"

    with KeyValueStore(path, read_only=1) as db:
        assert db[b"ro_key"] == b"ro_val"


def test_read_only_write_raises(tmp_path):
    """A write attempt on a read-only store must raise an error."""
    path = str(tmp_path / "ro_write.db")
    with KeyValueStore(path) as db:
        db[b"seed"] = b"1"

    with KeyValueStore(path, read_only=1) as db:
        with pytest.raises(snkv.Error):
            db[b"new_key"] = b"blocked"


def test_read_only_nonexistent_file_raises(tmp_path):
    """Opening a non-existent file read-only must raise an error."""
    path = str(tmp_path / "nonexistent.db")
    with pytest.raises(snkv.Error):
        KeyValueStore(path, read_only=1)


# ---------------------------------------------------------------------------
# WAL size limit (auto-checkpoint)
# ---------------------------------------------------------------------------

def test_wal_size_limit_enabled(tmp_path):
    """wal_size_limit>0 must open successfully and auto-checkpoint."""
    path = str(tmp_path / "walsize.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL, wal_size_limit=50) as db:
        db.begin(write=True)
        for i in range(200):
            db[f"k{i}".encode()] = b"v" * 100
        db.commit()
        # data must still be readable after potential auto-checkpoints
        assert db.get(b"k0") == b"v" * 100


def test_wal_size_limit_zero_disabled(tmp_path):
    """wal_size_limit=0 (default) must open and work normally."""
    path = str(tmp_path / "walsize0.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL, wal_size_limit=0) as db:
        _roundtrip(db)


# ---------------------------------------------------------------------------
# Combined options
# ---------------------------------------------------------------------------

def test_wal_normal_cache_busy(tmp_path):
    """WAL + SYNC_NORMAL + custom cache + busy_timeout all together."""
    path = str(tmp_path / "combined.db")
    with KeyValueStore(
        path,
        journal_mode=JOURNAL_WAL,
        sync_level=SYNC_NORMAL,
        cache_size=2000,
        busy_timeout=1000,
    ) as db:
        db.begin(write=True)
        for i in range(50):
            db[f"k{i}".encode()] = f"v{i}".encode()
        db.commit()
        for i in range(50):
            assert db.get(f"k{i}".encode()) == f"v{i}".encode()


# ---------------------------------------------------------------------------
# In-memory store
# ---------------------------------------------------------------------------

def test_in_memory_store():
    """None path opens an in-memory store (no file created)."""
    with KeyValueStore(None) as db:
        db[b"mem"] = b"only"
        assert db[b"mem"] == b"only"
    # data is gone — nothing to assert about files


# ---------------------------------------------------------------------------
# Backward compatibility (positional journal_mode)
# ---------------------------------------------------------------------------

def test_positional_journal_mode_wal(tmp_path):
    """journal_mode passed as keyword must work the same as config."""
    path = str(tmp_path / "compat.db")
    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db[b"compat"] = b"ok"
        assert db[b"compat"] == b"ok"
