"""
snkv - Python bindings for SNKV embedded key-value store.

Quick start:

    from snkv import KVStore

    with KVStore("mydb.db") as db:
        db["hello"] = "world"
        print(db["hello"])          # b"world"
        print(db.get("hello"))      # b"world"
        print(db.get("missing"))    # None

        for key, value in db:
            print(key, value)

All keys and values can be str (UTF-8 encoded) or bytes.
The store always returns raw bytes from get/iteration.
"""

from __future__ import annotations

import time

from typing import (
    Iterator as TypingIterator,
    Optional,
    List,
    Tuple,
    Union,
)

from . import _snkv  # type: ignore[attr-defined]
from ._snkv import (
    Error,
    NotFoundError,
    BusyError,
    LockedError,
    ReadOnlyError,
    CorruptError,
    JOURNAL_DELETE,
    JOURNAL_WAL,
    SYNC_OFF,
    SYNC_NORMAL,
    SYNC_FULL,
    CHECKPOINT_PASSIVE,
    CHECKPOINT_FULL,
    CHECKPOINT_RESTART,
    CHECKPOINT_TRUNCATE,
    NO_TTL,
)

_KVStore       = _snkv.KVStore
_ColumnFamily  = _snkv.ColumnFamily
_Iterator      = _snkv.Iterator

# ---------------------------------------------------------------------------
# Encoding helper
# ---------------------------------------------------------------------------

_Encodable = Union[str, bytes, bytearray, memoryview]


def _enc(v: _Encodable) -> bytes:
    """Encode str -> bytes (UTF-8). Pass bytes/bytearray/memoryview through."""
    if isinstance(v, str):
        return v.encode("utf-8")
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v)
    raise TypeError(
        f"keys and values must be str or bytes, not {type(v).__name__!r}"
    )


# ---------------------------------------------------------------------------
# Iterator wrapper
# ---------------------------------------------------------------------------

class Iterator:
    """
    Ordered key-value iterator returned by KVStore.iterator() and
    KVStore.prefix_iterator().

    Can be used as:
      - A Python iterator:  for key, value in db.iterator(): ...
      - A context manager:  with db.iterator() as it: ...
      - Manual control:     it.first(); while not it.eof: key = it.key; ...
    """

    __slots__ = ("_it",)

    def __init__(self, raw: _Iterator) -> None:
        self._it = raw

    # --- Manual control ---

    def first(self) -> "Iterator":
        """Seek to the first key. Returns self for chaining."""
        self._it.first()
        return self

    def next(self) -> None:
        """Advance to the next key."""
        self._it.next()

    @property
    def eof(self) -> bool:
        """True if the iterator is past the last key."""
        return self._it.eof()

    @property
    def key(self) -> bytes:
        """Current key bytes."""
        return self._it.key()

    @property
    def value(self) -> bytes:
        """Current value bytes."""
        return self._it.value()

    def item(self) -> Tuple[bytes, bytes]:
        """Current (key, value) tuple."""
        return self._it.item()

    # --- Python iterator protocol ---

    def __iter__(self) -> TypingIterator[Tuple[bytes, bytes]]:
        return iter(self._it)

    def __next__(self) -> Tuple[bytes, bytes]:
        return next(self._it)  # type: ignore[call-overload]

    # --- Lifecycle ---

    def close(self) -> None:
        """Release iterator resources."""
        self._it.close()

    def __enter__(self) -> "Iterator":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


# ---------------------------------------------------------------------------
# ColumnFamily wrapper
# ---------------------------------------------------------------------------

class ColumnFamily:
    """
    A logical namespace within a SNKV store.

    Obtained via KVStore.create_column_family(), KVStore.open_column_family(),
    or KVStore.default_column_family().

    Usage:

        with db.create_column_family("users") as cf:
            cf["alice"] = "30"
            print(cf["alice"])        # b"30"
            print("alice" in cf)      # True
    """

    __slots__ = ("_cf",)

    def __init__(self, raw: _ColumnFamily) -> None:
        self._cf = raw

    # --- Core operations ---

    def put(
        self,
        key: _Encodable,
        value: _Encodable,
        ttl: Optional[float] = None,
    ) -> None:
        """
        Insert or update a key-value pair.

        ttl -- seconds until the key expires (int or float). None means no expiry.
               Both the data write and the TTL index write are atomic.
        """
        if ttl is None:
            self._cf.put(_enc(key), _enc(value))
        else:
            expire_ms = int((time.time() + float(ttl)) * 1000)
            self._cf.put_ttl(_enc(key), _enc(value), expire_ms)

    def get(
        self,
        key: _Encodable,
        default: Optional[bytes] = None,
    ) -> Optional[bytes]:
        """Return value bytes, or default if key not found or expired."""
        try:
            value, _remaining = self._cf.get_ttl(_enc(key))
            return value
        except NotFoundError:
            return default

    def delete(self, key: _Encodable) -> None:
        """Delete key. Raises NotFoundError if key does not exist."""
        self._cf.delete(_enc(key))

    def exists(self, key: _Encodable) -> bool:
        """Return True if key exists."""
        return self._cf.exists(_enc(key))

    def ttl(self, key: _Encodable) -> Optional[float]:
        """
        Return remaining TTL in seconds for key.

        Returns None if the key exists but has no expiry.
        Returns 0.0 if the key has just expired (lazy delete performed).
        Raises NotFoundError if the key does not exist at all.
        """
        remaining_ms = self._cf.ttl_remaining(_enc(key))
        if remaining_ms == _snkv.NO_TTL:
            return None
        return remaining_ms / 1000.0

    def purge_expired(self) -> int:
        """Scan and delete all expired keys in this CF. Returns count deleted."""
        return self._cf.purge_expired()

    # --- Iterators ---

    def iterator(self) -> Iterator:
        """Return an Iterator over all keys in this column family."""
        return Iterator(self._cf.iterator())

    def prefix_iterator(self, prefix: _Encodable) -> Iterator:
        """Return an Iterator over keys that start with prefix."""
        return Iterator(self._cf.prefix_iterator(_enc(prefix)))

    # --- dict-like interface ---

    def __getitem__(self, key: _Encodable) -> bytes:
        # NotFoundError IS-A KeyError — let it propagate directly.
        # Use get_ttl so expired keys raise KeyError rather than returning stale data.
        value, _remaining = self._cf.get_ttl(_enc(key))
        return value

    def __setitem__(self, key: _Encodable, value: _Encodable) -> None:
        self._cf.put(_enc(key), _enc(value))

    def __delitem__(self, key: _Encodable) -> None:
        # NotFoundError IS-A KeyError — let it propagate directly.
        self._cf.delete(_enc(key))

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, (str, bytes, bytearray, memoryview)):
            return False
        return self._cf.exists(_enc(key))  # type: ignore[arg-type]

    def __iter__(self) -> TypingIterator[Tuple[bytes, bytes]]:
        return iter(self.iterator())

    # --- Lifecycle ---

    def close(self) -> None:
        """Release column family resources (does not delete data)."""
        self._cf.close()

    def __enter__(self) -> "ColumnFamily":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


# ---------------------------------------------------------------------------
# KVStore — main entry point
# ---------------------------------------------------------------------------

class KVStore:
    """
    SNKV key-value store.

    Opens (or creates) a database at the given path.
    Use as a context manager to guarantee the store is closed.

    Parameters
    ----------
    path : str or None
        Path to the database file.  None opens an in-memory store.
    journal_mode : int
        JOURNAL_WAL (default) or JOURNAL_DELETE.
    **config
        Advanced options passed to kvstore_open_v2:
          sync_level     -- SYNC_OFF / SYNC_NORMAL (default) / SYNC_FULL
          cache_size     -- page cache size in pages (default 2000 ~= 8 MB)
          page_size      -- DB page size, new databases only (default 4096)
          read_only      -- 1 to open read-only (default 0)
          busy_timeout   -- ms to retry on lock (default 0 = fail immediately)
          wal_size_limit -- auto-checkpoint every N committed transactions (0 = off)

    Quick start
    -----------
        with KVStore("mydb.db") as db:
            db.put(b"key", b"value")
            val = db.get(b"key")    # b"value"
            db["key2"] = "hello"
            del db["key2"]
    """

    def __init__(
        self,
        path: Optional[str] = None,
        *,
        journal_mode: int = JOURNAL_WAL,
        **config: int,
    ) -> None:
        if config:
            self._db: _KVStore = _KVStore.open_v2(
                path,
                journal_mode=journal_mode,
                **config,
            )
        else:
            self._db = _KVStore(path, journal_mode)

    # --- Core KV operations ---

    def put(
        self,
        key: _Encodable,
        value: _Encodable,
        ttl: Optional[float] = None,
    ) -> None:
        """
        Insert or update a key-value pair in the default column family.

        ttl -- seconds until the key expires (int or float). None means no expiry.
               Both the data write and the TTL index write are atomic.
        """
        if ttl is None:
            self._db.put(_enc(key), _enc(value))
        else:
            expire_ms = int((time.time() + float(ttl)) * 1000)
            self._db.put_ttl(_enc(key), _enc(value), expire_ms)

    def get(
        self,
        key: _Encodable,
        default: Optional[bytes] = None,
    ) -> Optional[bytes]:
        """Return value bytes for key, or default if not found or expired."""
        try:
            value, _remaining = self._db.get_ttl(_enc(key))
            return value
        except NotFoundError:
            return default

    def delete(self, key: _Encodable) -> None:
        """Delete key. Raises NotFoundError (subclass of KeyError) if not found."""
        self._db.delete(_enc(key))

    def exists(self, key: _Encodable) -> bool:
        """Return True if key exists in the default column family."""
        return self._db.exists(_enc(key))

    def ttl(self, key: _Encodable) -> Optional[float]:
        """
        Return remaining TTL in seconds for key.

        Returns None if the key exists but has no expiry.
        Returns 0.0 if the key has just expired (lazy delete performed).
        Raises NotFoundError if the key does not exist at all.
        """
        remaining_ms = self._db.ttl_remaining(_enc(key))
        if remaining_ms == _snkv.NO_TTL:
            return None
        return remaining_ms / 1000.0

    def purge_expired(self) -> int:
        """Scan and delete all expired keys. Returns the number of keys deleted."""
        return self._db.purge_expired()

    # --- dict-like interface ---

    def __getitem__(self, key: _Encodable) -> bytes:
        # NotFoundError IS-A KeyError — let it propagate directly.
        # Use get_ttl so expired keys raise KeyError rather than returning stale data.
        value, _remaining = self._db.get_ttl(_enc(key))
        return value

    def __setitem__(self, key: _Encodable, value: _Encodable) -> None:
        self._db.put(_enc(key), _enc(value))

    def __delitem__(self, key: _Encodable) -> None:
        # NotFoundError IS-A KeyError — let it propagate directly.
        self._db.delete(_enc(key))

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, (str, bytes, bytearray, memoryview)):
            return False
        return self._db.exists(_enc(key))  # type: ignore[arg-type]

    def __iter__(self) -> TypingIterator[Tuple[bytes, bytes]]:
        """Iterate over all (key, value) pairs in the default column family."""
        return iter(self.iterator())

    # --- Transactions ---

    def begin(self, write: bool = False) -> None:
        """Begin a transaction. write=True for a write transaction."""
        self._db.begin(write)

    def commit(self) -> None:
        """Commit the current transaction."""
        self._db.commit()

    def rollback(self) -> None:
        """Roll back the current transaction."""
        self._db.rollback()

    # --- Column families ---

    def create_column_family(self, name: str) -> ColumnFamily:
        """Create a new column family and return its handle."""
        return ColumnFamily(self._db.cf_create(name))

    def open_column_family(self, name: str) -> ColumnFamily:
        """Open an existing column family. Raises NotFoundError if missing."""
        return ColumnFamily(self._db.cf_open(name))

    def default_column_family(self) -> ColumnFamily:
        """Return a handle to the default column family."""
        return ColumnFamily(self._db.cf_get_default())

    def list_column_families(self) -> List[str]:
        """Return a list of all column family names."""
        return self._db.cf_list()

    def drop_column_family(self, name: str) -> None:
        """Drop a column family and permanently delete all its data."""
        self._db.cf_drop(name)

    # --- Iterators ---

    def iterator(self) -> Iterator:
        """Return an Iterator over all keys in the default column family."""
        return Iterator(self._db.iterator())

    def prefix_iterator(self, prefix: _Encodable) -> Iterator:
        """Return an Iterator over keys that start with prefix."""
        return Iterator(self._db.prefix_iterator(_enc(prefix)))

    # --- Maintenance ---

    def sync(self) -> None:
        """Flush all pending writes to disk."""
        self._db.sync()

    def vacuum(self, n_pages: int = 0) -> None:
        """
        Run an incremental vacuum step.
        n_pages=0 (default) frees all available unused pages.
        """
        self._db.vacuum(n_pages)

    def integrity_check(self) -> None:
        """
        Run a database integrity check.
        Raises CorruptError with details if corruption is detected.
        """
        self._db.integrity_check()

    def checkpoint(self, mode: int = CHECKPOINT_PASSIVE) -> Tuple[int, int]:
        """
        Run a WAL checkpoint.

        Returns (nLog, nCkpt):
          nLog  -- total WAL frames after the checkpoint
          nCkpt -- frames successfully written to the database file

        mode is one of CHECKPOINT_PASSIVE (default), CHECKPOINT_FULL,
        CHECKPOINT_RESTART, or CHECKPOINT_TRUNCATE.
        """
        return self._db.checkpoint(mode)

    def stats(self) -> dict:
        """
        Return operation statistics as a dict with keys:
          puts, gets, deletes, iterations, errors.
        """
        return self._db.stats()

    @property
    def errmsg(self) -> str:
        """Last error message string from the store."""
        return self._db.errmsg()

    # --- Lifecycle ---

    def close(self) -> None:
        """Close the store and release all resources."""
        self._db.close()

    def __enter__(self) -> "KVStore":
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> bool:
        self.close()
        return False

    def __repr__(self) -> str:
        return "snkv.KVStore()"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    # Main class
    "KVStore",
    "ColumnFamily",
    "Iterator",
    # Exceptions
    "Error",
    "NotFoundError",
    "BusyError",
    "LockedError",
    "ReadOnlyError",
    "CorruptError",
    # Journal mode
    "JOURNAL_WAL",
    "JOURNAL_DELETE",
    # Sync level
    "SYNC_OFF",
    "SYNC_NORMAL",
    "SYNC_FULL",
    # Checkpoint mode
    "CHECKPOINT_PASSIVE",
    "CHECKPOINT_FULL",
    "CHECKPOINT_RESTART",
    "CHECKPOINT_TRUNCATE",
    # TTL
    "NO_TTL",
]
