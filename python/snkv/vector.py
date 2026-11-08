# SPDX-License-Identifier: Apache-2.0
# Copyright 2025 SNKV Contributors
"""
snkv.vector — VectorStore backed by the native SNKV vector store.

The HNSW index, storage, TTL, metadata and the sidecar index file are all
handled by the C library (kvstore_vec.c, using usearch), compiled into the
snkv extension.  This module converts between numpy arrays and the C API.

Install:
    pip install snkv[vector]      # adds numpy

Quick start:
    from snkv.vector import VectorStore
    import numpy as np

    with VectorStore("store.db", dim=128, space="cosine") as vs:
        vs.vector_put(b"doc:1", b"hello world", np.random.rand(128).astype("f4"))
        results = vs.search(np.random.rand(128).astype("f4"), top_k=5)
        for r in results:
            print(r.key, r.distance, r.value)
"""

from __future__ import annotations

try:
    import numpy as np
except ImportError as _err:
    raise ImportError(
        "Vector search requires numpy.\n"
        "Install with:  pip install snkv[vector]"
    ) from _err

import json
import math
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from snkv import NotFoundError
from snkv import _snkv

VectorIndexError = _snkv.VectorIndexError

# Internal CF names used by the C vector store (kvstore_vec.c).
_CF_VEC  = "__snkv_vec__"       # user key  → float32 bytes (raw vector)
_CF_IDK  = "__snkv_vec_idk__"   # user key  → 8-byte big-endian int64 (usearch label)
_CF_IDI  = "__snkv_vec_idi__"   # int64     → user key
_CF_META = "__snkv_vec_meta__"  # configuration
_CF_TAGS = "__snkv_vec_tags__"  # user key  → JSON metadata bytes

_SPACES = {
    "l2":     _snkv.VEC_SPACE_L2,
    "cosine": _snkv.VEC_SPACE_COSINE,
    "ip":     _snkv.VEC_SPACE_IP,
}
_DTYPES = {
    "f32": _snkv.VEC_DTYPE_F32,
    "f16": _snkv.VEC_DTYPE_F16,
    "i8":  _snkv.VEC_DTYPE_I8,
}
_SPACE_NAMES = {v: k for k, v in _SPACES.items()}
_DTYPE_NAMES = {v: k for k, v in _DTYPES.items()}


def _enc(v) -> bytes:
    if isinstance(v, str):
        return v.encode()
    return bytes(v)


def _meta_bytes(metadata: Optional[Dict[str, Any]]) -> Optional[bytes]:
    """None keeps existing metadata, {} clears it (b""), else JSON bytes."""
    if metadata is None:
        return None
    if len(metadata) == 0:
        return b""
    try:
        return json.dumps(metadata, ensure_ascii=False).encode()
    except TypeError as exc:
        raise ValueError(f"metadata must be JSON-serializable: {exc}") from exc


def _meta_dict(raw: Optional[bytes]) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    try:
        return json.loads(raw.decode())
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _check_ttl(ttl: Optional[float]) -> float:
    if ttl is None:
        return 0.0
    if not math.isfinite(ttl) or ttl <= 0:
        raise ValueError(f"ttl must be a finite positive number, got {ttl}")
    return float(ttl)


@dataclass
class SearchResult:
    key:      bytes
    value:    bytes
    distance: float
    metadata: Optional[Dict[str, Any]] = field(default=None)


class VectorStore:
    """
    SNKV key-value store with an integrated HNSW vector index.

    Parameters
    ----------
    path : str or None
        Path to the .db file. None opens an in-memory store.
    dim : int
        Vector dimension. Fixed for the lifetime of the store.
    space : str
        Distance metric: "l2" (squared L2), "cosine", or "ip".
    connectivity : int
        HNSW M parameter (default 16).
    expansion_add : int
        HNSW expansion during index build (default 128).
    expansion_search : int or None
        HNSW expansion at query time. None restores the stored value on
        reopen, falling back to 64 for new stores.
    dtype : str
        In-memory index precision: "f32" (default), "f16", or "i8".
        On-disk storage is always float32 regardless of dtype.
    password : bytes or str or None
        Opens/creates an encrypted store if given.  Encrypted stores do not
        write the sidecar index file.

    Index persistence
    -----------------
    For unencrypted, file-backed stores the index is saved to
    ``{path}.usearch`` on close and reloaded on the next open.  If the
    sidecar is missing, stale or damaged, the index is rebuilt from the
    database.

    Thread safety
    -------------
    Each call is serialised by the store's internal lock, so one instance
    can be shared between threads.  Do not call close() while another
    thread is still using the store.
    """

    def __init__(
        self,
        path: Optional[str],
        dim: int,
        *,
        space: str = "l2",
        connectivity: int = 16,
        expansion_add: int = 128,
        expansion_search: Optional[int] = None,
        dtype: str = "f32",
        password: Optional[Union[str, bytes]] = None,
    ) -> None:
        if space not in _SPACES:
            raise ValueError(
                f"space must be one of {sorted(_SPACES)!r}, got {space!r}"
            )
        if dim < 1:
            raise ValueError(f"dim must be >= 1, got {dim}")
        if dtype not in _DTYPES:
            raise ValueError(
                f"dtype must be one of {sorted(_DTYPES)!r}, got {dtype!r}"
            )
        if connectivity < 1:
            raise ValueError(f"connectivity must be >= 1, got {connectivity}")
        if expansion_add < 1:
            raise ValueError(f"expansion_add must be >= 1, got {expansion_add}")
        if expansion_search is not None and expansion_search < 1:
            raise ValueError(f"expansion_search must be >= 1, got {expansion_search}")

        pw = None
        if password is not None:
            pw = password.encode() if isinstance(password, str) else bytes(password)

        self._dim = dim
        self._vs = _snkv.VecStore(
            path, dim, _SPACES[space], connectivity, expansion_add,
            expansion_search or 0, _DTYPES[dtype], pw,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _vec(self, vector) -> bytes:
        vec = np.ascontiguousarray(vector, dtype=np.float32)
        if vec.ndim != 1 or vec.shape[0] != self._dim:
            raise ValueError(
                f"vector must have shape ({self._dim},), got {vec.shape}"
            )
        return vec.tobytes()

    def _query(self, query) -> bytes:
        q = np.asarray(query, dtype=np.float32).ravel()
        if q.shape[0] != self._dim:
            raise ValueError(
                f"query must have {self._dim} dimensions, got {q.shape[0]}"
            )
        return q.tobytes()

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def vector_put(
        self,
        key,
        value,
        vector,
        *,
        ttl:      Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Store key/value and add vector to the HNSW index, atomically.

        ttl      : float or None — seconds until expiry
        metadata : dict or None
            None  — existing metadata preserved on overwrite
            {}    — clears any existing metadata entry
            {...} — writes new metadata
        """
        self._vs.put(_enc(key), _enc(value), self._vec(vector),
                     _check_ttl(ttl), _meta_bytes(metadata))

    def vector_put_batch(
        self,
        items: Iterable,
        *,
        ttl: Optional[float] = None,
    ) -> None:
        """
        Insert or update multiple items in one transaction.

        items : iterable of (key, value, vector) or (key, value, vector, metadata)
        ttl   : uniform expiry in seconds applied to all items (optional)

        If any item has an invalid vector shape nothing is written.
        For duplicate keys the last occurrence wins.
        """
        expire = _check_ttl(ttl)
        raw: List[Tuple] = []
        for item in items:
            if len(item) == 3:
                key, value, vector = item
                meta = None
            elif len(item) == 4:
                key, value, vector, meta = item
            else:
                raise ValueError(
                    "items must be (key, value, vector) or "
                    "(key, value, vector, metadata)"
                )
            raw.append((_enc(key), _enc(value), self._vec(vector), _meta_bytes(meta)))
        if raw:
            self._vs.put_batch(raw, expire)

    # ------------------------------------------------------------------
    # KV reads and dict interface
    # ------------------------------------------------------------------

    def get(self, key, default=None) -> Optional[bytes]:
        """Return value bytes, or default if key not found or expired."""
        value = self._vs.get(_enc(key))
        return default if value is None else value

    def vector_get(self, key) -> "np.ndarray":
        """Return stored vector as np.ndarray(dim,) float32."""
        return np.frombuffer(self._vs.get_vector(_enc(key)), dtype=np.float32).copy()

    def get_metadata(self, key) -> Optional[Dict[str, Any]]:
        """Return stored metadata dict, or None if key has no metadata."""
        return _meta_dict(self._vs.get_metadata(_enc(key)))

    def __getitem__(self, key) -> bytes:
        value = self._vs.get(_enc(key))
        if value is None:
            raise NotFoundError("key not found")
        return value

    def __setitem__(self, key, value) -> None:
        """Plain KV put — does NOT add to vector index."""
        self._vs.kv_put(_enc(key), _enc(value))

    def __delitem__(self, key) -> None:
        self.delete(key)

    def __contains__(self, key) -> bool:
        return self._vs.contains(_enc(key))

    def __len__(self) -> int:
        """Active vector count — same as vector_stats()["count"]."""
        return self._vs.count()

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    def delete(self, key) -> None:
        """
        Delete key from KV store and HNSW index.
        Raises NotFoundError if key does not exist or has expired.
        """
        self._vs.delete(_enc(key))

    # ------------------------------------------------------------------
    # ANN search
    # ------------------------------------------------------------------

    def search(
        self,
        query,
        top_k: int = 10,
        *,
        filter=None,
        rerank: bool = False,
        oversample: int = 3,
        max_distance: Optional[float] = None,
    ) -> List[SearchResult]:
        """
        Approximate nearest-neighbour search.

        query        : array-like, shape (dim,)
        top_k        : int — maximum results to return
        filter       : dict, callable, or None
            dict      — equality filter; all fields must match
            callable  — predicate(metadata_dict) -> bool
            None      — no filter
        rerank       : bool — re-score candidates with exact float32 distances
        oversample   : int — candidate pool = top_k * oversample when
                       filter or rerank is active (default 3)
        max_distance : float or None — drop results with distance > threshold;
                       applied last, after rerank

        Returns at most top_k results.  SearchResult.metadata is populated
        only when filter is provided; otherwise call get_metadata(key).
        """
        if top_k <= 0:
            raise ValueError("top_k must be > 0")
        if oversample < 1:
            raise ValueError(f"oversample must be >= 1, got {oversample}")
        q = self._query(query)

        if isinstance(filter, dict):
            _fd = dict(filter)
            filter_fn: Optional[Callable] = (
                lambda m: all(m.get(k) == v for k, v in _fd.items())
            )
        elif callable(filter):
            filter_fn = filter
        else:
            filter_fn = None

        # With a filter, fetch a larger pool so enough candidates survive it.
        fetch_k = top_k
        if filter_fn is not None:
            fetch_k = max(1, min(self._vs.count(), top_k * oversample))

        rows = self._vs.search(q, fetch_k, bool(rerank), oversample, 0.0)

        results: List[SearchResult] = []
        for key_b, value, dist, meta_raw in rows:
            metadata = None
            if filter_fn is not None:
                # kvstore_vec_search does not fill metadata yet; fetch it.
                if meta_raw is None:
                    meta_raw = self._vs.get_metadata(key_b)
                metadata = _meta_dict(meta_raw)
                if not filter_fn(metadata or {}):
                    continue
            if max_distance is not None and dist > max_distance:
                continue
            results.append(
                SearchResult(key=key_b, value=value, distance=dist, metadata=metadata)
            )
            if len(results) == top_k:
                break
        return results

    def search_keys(self, query, top_k: int = 10) -> List[Tuple[bytes, float]]:
        """ANN search returning (key, distance) pairs only — no value fetch."""
        if top_k <= 0:
            raise ValueError("top_k must be > 0")
        return self._vs.search_keys(self._query(query), top_k)

    # ------------------------------------------------------------------
    # Index statistics
    # ------------------------------------------------------------------

    def vector_stats(self) -> dict:
        """
        Return index configuration and runtime state.

        Keys
        ----
        dim, space, dtype, connectivity, expansion_add, expansion_search
        count           — active (non-deleted) vectors in the index
        capacity        — allocated capacity in the index
        fill_ratio      — count / capacity
        vec_cf_count    — stored vectors (may include expired)
        has_metadata    — True if any metadata has been stored
        sidecar_enabled — True if the index is saved to {path}.usearch
        """
        st = self._vs.stats()
        st["space"] = _SPACE_NAMES.get(st["space"], st["space"])
        st["dtype"] = _DTYPE_NAMES.get(st["dtype"], st["dtype"])
        return st

    def vector_info(self) -> dict:
        """Return basic index configuration (subset of vector_stats)."""
        st = self.vector_stats()
        return {k: st[k] for k in (
            "dim", "space", "dtype", "count",
            "connectivity", "expansion_add", "expansion_search",
        )}

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def vector_purge_expired(self) -> int:
        """
        Delete expired vectors from the index and the database.
        Returns the number of vectors removed.

        Call this method (not KVStore.purge_expired) to keep vectors in sync.
        """
        return self._vs.purge_expired()

    def drop_vector_index(self) -> None:
        """
        Drop the vector index and its stored vectors.  KV data in the
        default CF is preserved and accessible via get().

        After this call, search() raises VectorIndexError.
        """
        self._vs.drop_index()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the store.  Unencrypted file-backed stores save the
        index to ``{path}.usearch`` so the next open skips the rebuild."""
        self._vs.close()

    def __enter__(self) -> "VectorStore":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def __repr__(self) -> str:
        try:
            st = self.vector_stats()
        except Exception:
            return f"VectorStore(dim={self._dim}, closed)"
        return (
            f"VectorStore(dim={self._dim}, space={st['space']!r}, "
            f"dtype={st['dtype']!r}, count={st['count']})"
        )


__all__ = [
    "VectorStore",
    "SearchResult",
    "VectorIndexError",
]
