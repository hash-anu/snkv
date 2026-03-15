# SPDX-License-Identifier: Apache-2.0
# Copyright 2025 SNKV Contributors
"""
snkv.vector — VectorStore with integrated usearch HNSW vector index.

Install:
    pip install snkv[vector]

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
    from usearch.index import Index as _Index
    import numpy as np
except ImportError as _err:
    raise ImportError(
        "Vector search requires extra dependencies.\n"
        "Install with:  pip install snkv[vector]"
    ) from _err

import json
import math
import struct
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from snkv import KVStore, NotFoundError

# ---------------------------------------------------------------------------
# Internal CF names (Decision 5)
# Note: SNKV reserves names starting with "__" for C-internal use; vector
# CFs use a single leading underscore to stay out of user namespace.
# ---------------------------------------------------------------------------
_CF_VEC  = "_snkv_vec_"       # user key  → float32 bytes (raw vector)
_CF_IDK  = "_snkv_vec_idk_"   # user key  → 8-byte big-endian int64 (usearch label)
_CF_IDI  = "_snkv_vec_idi_"   # int64     → user key
_CF_META = "_snkv_vec_meta_"  # configuration
_CF_TAGS = "_snkv_vec_tags_"  # user key  → JSON metadata bytes (lazy, Decision 21)

# Meta keys
_META_NDIM    = b"ndim"
_META_METRIC  = b"metric"
_META_CONN    = b"connectivity"
_META_EXPADD  = b"expansion_add"
_META_EXPSRCH = b"expansion_search"
_META_DTYPE   = b"dtype"
_META_NEXT_ID = b"next_id"

# Public space name → usearch metric name (Decision 12)
_METRIC_MAP  = {"l2": "l2sq", "cosine": "cos", "ip": "ip"}
_VALID_DTYPES = {"f32", "f16", "i8"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pack_i64(n: int) -> bytes:
    return struct.pack(">q", n)


def _unpack_i64(b: bytes) -> int:
    return struct.unpack(">q", b)[0]


def _enc(v) -> bytes:
    if isinstance(v, str):
        return v.encode("utf-8")
    return bytes(v)


def _exact_dist_batch(q: "np.ndarray", vecs: "np.ndarray", space: str) -> "np.ndarray":
    """Exact distances between query q (dim,) and candidates vecs (n, dim)."""
    if space == "l2":
        diff = vecs - q
        return np.sum(diff * diff, axis=1)
    elif space == "cosine":
        dots  = vecs @ q
        norms = np.linalg.norm(vecs, axis=1) * np.linalg.norm(q)
        return 1.0 - dots / (norms + 1e-10)
    else:  # ip
        return -(vecs @ q)


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

class VectorIndexError(Exception):
    """Raised when the usearch index is not initialised or in an invalid state."""


@dataclass
class SearchResult:
    key:      bytes
    value:    bytes
    distance: float
    metadata: Optional[Dict[str, Any]] = field(default=None)


# ---------------------------------------------------------------------------
# VectorStore
# ---------------------------------------------------------------------------

class VectorStore:
    """
    SNKV key-value store with an integrated usearch HNSW vector index.

    All vector data is stored inside the .db file using dedicated column
    families (Decision 4). The in-memory usearch Index is rebuilt from those
    CFs on every open (Decision 8 Phase 1).

    Parameters
    ----------
    path : str or None
        Path to the .db file. None opens an in-memory store.
    dim : int
        Vector dimension. Fixed for the lifetime of the store (Decision 13).
    space : str
        Distance metric: "l2" (squared L2), "cosine", or "ip" (Decision 12).
    connectivity : int
        HNSW M parameter (default 16).
    expansion_add : int
        HNSW expansion during index build (default 128).
    expansion_search : int or None
        HNSW expansion at query time. None (default) restores the stored value
        on reopen, falling back to 64 for new stores (Decision 25).
    dtype : str
        In-memory index precision: "f32" (default), "f16", or "i8" (Decision 20).
        On-disk storage is always float32 regardless of dtype.
    password : bytes or str or None
        Opens/creates an encrypted store if given (Decision 18).
    **kv_kwargs
        Passed to KVStore() for the plain-open path.
    """

    def __init__(
        self,
        path: Optional[str],
        dim: int,
        *,
        space: str = "l2",
        connectivity: int = 16,
        expansion_add: int = 128,
        expansion_search: Optional[int] = None,   # sentinel — Decision 25
        dtype: str = "f32",                        # Decision 20
        password: Optional[Union[str, bytes]] = None,
        **kv_kwargs,
    ) -> None:
        if space not in _METRIC_MAP:
            raise ValueError(
                f"space must be one of {sorted(_METRIC_MAP)!r}, got {space!r}"
            )
        if dim < 1:
            raise ValueError(f"dim must be >= 1, got {dim}")
        if dtype not in _VALID_DTYPES:
            raise ValueError(
                f"dtype must be one of {sorted(_VALID_DTYPES)!r}, got {dtype!r}"
            )
        if connectivity < 1:
            raise ValueError(f"connectivity must be >= 1, got {connectivity}")
        if expansion_add < 1:
            raise ValueError(f"expansion_add must be >= 1, got {expansion_add}")
        if expansion_search is not None and expansion_search < 1:
            raise ValueError(f"expansion_search must be >= 1, got {expansion_search}")

        # Open KVStore — encrypted or plain (Decision 18)
        if password is not None:
            pw = password.encode() if isinstance(password, str) else bytes(password)
            self._kv = KVStore.open_encrypted(path, pw)
        else:
            self._kv = KVStore(path, **kv_kwargs)

        self._dim              = dim
        self._space            = space
        self._connectivity     = connectivity
        self._expansion_add    = expansion_add
        self._expansion_search: Optional[int] = expansion_search  # resolved in _rebuild_index
        self._dtype            = dtype
        self._index: Optional[_Index] = None
        self._next_id: int = 0
        self._tags_cf = None  # lazy — created on first metadata write (Decision 21)

        # Open (or create) the four core internal CFs (Decision 5)
        self._vec_cf  = self._get_or_create_cf(_CF_VEC)
        self._idk_cf  = self._get_or_create_cf(_CF_IDK)
        self._idi_cf  = self._get_or_create_cf(_CF_IDI)
        self._meta_cf = self._get_or_create_cf(_CF_META)

        # Build in-memory usearch index from stored CFs (Decision 8)
        try:
            self._rebuild_index()
        except Exception:
            self._kv.close()
            raise

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_or_create_cf(self, name: str):
        try:
            return self._kv.open_column_family(name)
        except NotFoundError:
            return self._kv.create_column_family(name)

    def _rebuild_index(self) -> None:
        """Build the in-memory usearch Index from stored CFs."""
        stored_ndim   = self._meta_cf.get(_META_NDIM)
        stored_metric = self._meta_cf.get(_META_METRIC)

        if stored_ndim is not None:
            # Existing store — validate immutable fields (Decisions 12, 13, 20)
            ndim   = _unpack_i64(stored_ndim)
            metric = stored_metric.decode() if stored_metric else "l2"
            if ndim != self._dim:
                raise ValueError(
                    f"dim mismatch: store has ndim={ndim}, "
                    f"opened with dim={self._dim}"
                )
            if metric != self._space:
                raise ValueError(
                    f"space mismatch: store has space={metric!r}, "
                    f"opened with space={self._space!r}"
                )

            conn_raw    = self._meta_cf.get(_META_CONN)
            expadd_raw  = self._meta_cf.get(_META_EXPADD)
            dtype_raw   = self._meta_cf.get(_META_DTYPE)
            expsrch_raw = self._meta_cf.get(_META_EXPSRCH)
            next_id_raw = self._meta_cf.get(_META_NEXT_ID)

            # connectivity and expansion_add: stored value always wins (affect graph structure)
            if conn_raw:
                self._connectivity  = _unpack_i64(conn_raw)
            if expadd_raw:
                self._expansion_add = _unpack_i64(expadd_raw)

            # dtype: immutable — validate if stored
            if dtype_raw:
                stored_dtype = dtype_raw.decode()
                if stored_dtype != self._dtype:
                    raise ValueError(
                        f"dtype mismatch: store has dtype={stored_dtype!r}, "
                        f"opened with dtype={self._dtype!r}"
                    )

            # expansion_search: advisory — caller arg overrides stored (Decision 25)
            if self._expansion_search is None:
                self._expansion_search = (
                    _unpack_i64(expsrch_raw) if expsrch_raw else 64
                )
            # else: caller passed explicit value — keep it, ignore stored

            self._next_id = _unpack_i64(next_id_raw) if next_id_raw else 0

        else:
            # New store — resolve expansion_search sentinel
            if self._expansion_search is None:
                self._expansion_search = 64

        # Try to open tags CF if it already exists on disk (Decision 21)
        try:
            self._tags_cf = self._kv.open_column_family(_CF_TAGS)
        except NotFoundError:
            self._tags_cf = None

        count = self._vec_cf.count()

        self._index = _Index(
            ndim=self._dim,
            metric=_METRIC_MAP[self._space],
            connectivity=self._connectivity,
            expansion_add=self._expansion_add,
            dtype=self._dtype,
        )
        self._index.expansion_search = self._expansion_search

        if count > 0:
            # Batch-load all stored vectors (Decision 8 Phase 1)
            ids:  List[int]          = []
            vecs: List["np.ndarray"] = []
            with self._vec_cf.iterator() as it:
                for key_b, vec_bytes in it:
                    id_raw = self._idk_cf.get(key_b)
                    if id_raw is not None:
                        ids.append(_unpack_i64(id_raw))
                        vecs.append(np.frombuffer(vec_bytes, dtype=np.float32))
            if ids:
                self._index.add(
                    np.array(ids, dtype=np.uint64),
                    np.stack(vecs),
                )

    def _write_full_meta(self) -> None:
        """Persist all metadata. Call within an active write transaction."""
        self._meta_cf.put(_META_NDIM,    _pack_i64(self._dim))
        self._meta_cf.put(_META_METRIC,  self._space.encode())
        self._meta_cf.put(_META_CONN,    _pack_i64(self._connectivity))
        self._meta_cf.put(_META_EXPADD,  _pack_i64(self._expansion_add))
        self._meta_cf.put(_META_EXPSRCH, _pack_i64(self._expansion_search))
        self._meta_cf.put(_META_DTYPE,   self._dtype.encode())
        self._meta_cf.put(_META_NEXT_ID, _pack_i64(self._next_id))

    def _get_tags_cf(self):
        """Lazily create _snkv_vec_tags_ on first metadata write (Decision 21)."""
        if self._tags_cf is None:
            self._tags_cf = self._get_or_create_cf(_CF_TAGS)
        return self._tags_cf

    # ------------------------------------------------------------------
    # Vector write (Decision 7 — atomicity)
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
        Store key/value and add vector to the HNSW index.

        The SNKV write (default CF + all vector CFs) is one atomic transaction.
        The usearch Index update happens after commit (Decision 7).

        Parameters
        ----------
        key      : bytes or str
        value    : bytes or str
        vector   : array-like, shape (dim,)
        ttl      : float or None — seconds until expiry
        metadata : dict or None (Decision 21)
            None  — existing metadata preserved on overwrite
            {}    — clears any existing metadata entry
            {...} — writes new metadata
        """
        key_b = _enc(key)
        val_b = _enc(value)
        vec   = np.asarray(vector, dtype=np.float32)
        if vec.ndim != 1 or vec.shape[0] != self._dim:
            raise ValueError(
                f"vector must have shape ({self._dim},), got {vec.shape}"
            )
        vec_bytes = vec.tobytes()

        if ttl is not None and (not math.isfinite(ttl) or ttl <= 0):
            raise ValueError(f"ttl must be a finite positive number, got {ttl}")
        if self._idk_cf is None:
            raise VectorIndexError(
                "Vector index has been dropped; open a new VectorStore to re-insert."
            )
        old_id_raw = self._idk_cf.get(key_b)
        old_id     = _unpack_i64(old_id_raw) if old_id_raw is not None else None
        is_first   = self._meta_cf.get(_META_NDIM) is None

        # Ensure tags CF exists BEFORE opening the write transaction (DDL must not
        # be nested inside a DML transaction — Decision 21).
        if metadata is not None and len(metadata) > 0:
            self._get_tags_cf()

        new_id = self._next_id

        self._kv.begin(write=True)
        try:
            if ttl is None:
                self._kv.put(key_b, val_b)
            else:
                self._kv.put(key_b, val_b, ttl=ttl)

            self._vec_cf.put(key_b, vec_bytes)
            self._idk_cf.put(key_b, _pack_i64(new_id))
            self._idi_cf.put(_pack_i64(new_id), key_b)

            if old_id is not None:
                try:
                    self._idi_cf.delete(_pack_i64(old_id))
                except NotFoundError:
                    pass

            # Metadata handling (Decision 21)
            if metadata is not None:
                if len(metadata) == 0:
                    # Clear existing entry
                    if self._tags_cf is not None:
                        try:
                            self._tags_cf.delete(key_b)
                        except NotFoundError:
                            pass
                else:
                    try:
                        meta_bytes = json.dumps(metadata, ensure_ascii=False).encode()
                    except TypeError as exc:
                        raise ValueError(
                            f"metadata must be JSON-serializable: {exc}"
                        ) from exc
                    self._get_tags_cf().put(key_b, meta_bytes)

            self._next_id += 1
            if is_first:
                self._write_full_meta()
            else:
                self._meta_cf.put(_META_NEXT_ID, _pack_i64(self._next_id))

            self._kv.commit()
        except Exception:
            self._next_id = new_id
            self._kv.rollback()
            raise

        # Post-commit usearch update (Decision 7)
        if old_id is not None:
            try:
                self._index.remove(old_id)
            except Exception:
                pass
        # Auto-reserve when fill_ratio >= 0.9 (Decision 24)
        cap = self._index.capacity
        if cap > 0 and len(self._index) / cap >= 0.9:
            self._index.reserve(cap * 2)
        self._index.add(np.array([new_id], dtype=np.uint64), vec.reshape(1, -1))

    # ------------------------------------------------------------------
    # Batch insert (Decision 22)
    # ------------------------------------------------------------------

    def vector_put_batch(
        self,
        items: Iterable,
        *,
        ttl: Optional[float] = None,
    ) -> None:
        """
        Insert or update multiple items atomically.

        items : iterable of (key, value, vector) or (key, value, vector, metadata)
        ttl   : uniform expiry in seconds applied to all items (optional)

        Single SNKV transaction + single index.add — 5-20x faster than
        N individual vector_put calls for initial bulk load (Decision 22).
        If any item has an invalid vector shape the entire batch rolls back.
        """
        if ttl is not None and (not math.isfinite(ttl) or ttl <= 0):
            raise ValueError(f"ttl must be a finite positive number, got {ttl}")
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
            vec = np.asarray(vector, dtype=np.float32)
            if vec.ndim != 1 or vec.shape[0] != self._dim:
                raise ValueError(
                    f"vector must have shape ({self._dim},), got {vec.shape}"
                )
            raw.append((_enc(key), _enc(value), vec, meta))

        if not raw:
            return

        # Deduplicate: last occurrence wins (Decision 22)
        key_to_last: Dict[bytes, int] = {}
        for i, (key_b, _, _, _) in enumerate(raw):
            key_to_last[key_b] = i
        deduped = [raw[i] for i in sorted(key_to_last.values())]

        if self._idk_cf is None:
            raise VectorIndexError(
                "Vector index has been dropped; open a new VectorStore to re-insert."
            )

        is_first = self._meta_cf.get(_META_NDIM) is None

        # Ensure tags CF exists BEFORE opening the write transaction (DDL must not
        # be nested inside a DML transaction — Decision 21).
        for _, _, _, meta in deduped:
            if meta is not None and len(meta) > 0:
                self._get_tags_cf()
                break

        # Snapshot pre-existing IDs before the transaction
        pre_ids: Dict[bytes, Optional[int]] = {}
        for key_b, _, _, _ in deduped:
            old_raw = self._idk_cf.get(key_b)
            pre_ids[key_b] = _unpack_i64(old_raw) if old_raw is not None else None

        base_id = self._next_id
        new_id_for: Dict[bytes, int] = {
            key_b: base_id + i for i, (key_b, _, _, _) in enumerate(deduped)
        }

        self._kv.begin(write=True)
        try:
            for key_b, val_b, vec, meta in deduped:
                new_id = new_id_for[key_b]
                old_id = pre_ids[key_b]

                if ttl is None:
                    self._kv.put(key_b, val_b)
                else:
                    self._kv.put(key_b, val_b, ttl=ttl)

                self._vec_cf.put(key_b, vec.tobytes())
                self._idk_cf.put(key_b, _pack_i64(new_id))
                self._idi_cf.put(_pack_i64(new_id), key_b)

                if old_id is not None:
                    try:
                        self._idi_cf.delete(_pack_i64(old_id))
                    except NotFoundError:
                        pass

                if meta is not None:
                    if len(meta) == 0:
                        if self._tags_cf is not None:
                            try:
                                self._tags_cf.delete(key_b)
                            except NotFoundError:
                                pass
                    else:
                        try:
                            meta_bytes = json.dumps(meta, ensure_ascii=False).encode()
                        except TypeError as exc:
                            raise ValueError(
                                f"metadata must be JSON-serializable: {exc}"
                            ) from exc
                        self._get_tags_cf().put(key_b, meta_bytes)

            self._next_id = base_id + len(deduped)
            if is_first:
                self._write_full_meta()
            else:
                self._meta_cf.put(_META_NEXT_ID, _pack_i64(self._next_id))

            self._kv.commit()
        except Exception:
            self._next_id = base_id
            self._kv.rollback()
            raise

        # Post-commit: remove old usearch entries, batch-add new ones
        for key_b, _, _, _ in deduped:
            old_id = pre_ids[key_b]
            if old_id is not None:
                try:
                    self._index.remove(old_id)
                except Exception:
                    pass

        # Auto-reserve when fill_ratio >= 0.9 (Decision 24)
        cap = self._index.capacity
        if cap > 0 and len(self._index) / cap >= 0.9:
            self._index.reserve(max(cap * 2, len(self._index) + len(deduped)))

        ids_arr  = np.array(
            [new_id_for[key_b] for key_b, _, _, _ in deduped], dtype=np.uint64
        )
        vecs_arr = np.stack([vec for _, _, vec, _ in deduped])
        self._index.add(ids_arr, vecs_arr)

    # ------------------------------------------------------------------
    # KV reads and dict interface (Decision 15)
    # ------------------------------------------------------------------

    def get(self, key, default=None) -> Optional[bytes]:
        """Return value bytes, or default if key not found or expired."""
        return self._kv.get(_enc(key), default)

    def vector_get(self, key) -> "np.ndarray":
        """Return stored vector as np.ndarray(dim,) float32."""
        if self._vec_cf is None:
            raise VectorIndexError(
                "Vector index has been dropped; open a new VectorStore to re-insert."
            )
        vec_bytes = self._vec_cf.get(_enc(key))
        if vec_bytes is None:
            raise NotFoundError("no vector stored for key")
        return np.frombuffer(vec_bytes, dtype=np.float32).copy()

    def get_metadata(self, key) -> Optional[Dict[str, Any]]:
        """Return stored metadata dict, or None if key has no metadata."""
        if self._tags_cf is None:
            return None
        meta_bytes = self._tags_cf.get(_enc(key))
        if meta_bytes is None:
            return None
        try:
            return json.loads(meta_bytes.decode())
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    def __getitem__(self, key) -> bytes:
        return self._kv[_enc(key)]

    def __setitem__(self, key, value) -> None:
        """Plain KV put — does NOT add to vector index (Decision 15)."""
        self._kv[_enc(key)] = _enc(value)

    def __delitem__(self, key) -> None:
        self.delete(key)

    def __contains__(self, key) -> bool:
        return _enc(key) in self._kv

    def __len__(self) -> int:
        """Active vector count — same as vector_stats()["count"] (Decision 24)."""
        return len(self._index) if self._index else 0

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    def delete(self, key) -> None:
        """
        Delete key from KV store and HNSW index.
        Raises NotFoundError if key does not exist or has expired.
        """
        key_b  = _enc(key)
        if self._idk_cf is None:
            # Index dropped — fall back to plain KV delete
            self._kv.delete(key_b)
            return
        id_raw = self._idk_cf.get(key_b)

        if id_raw is None:
            # Key exists only in the plain KV (no vector), plain delete
            self._kv.delete(key_b)
            return

        int_id = _unpack_i64(id_raw)

        self._kv.begin(write=True)
        try:
            self._kv.delete(key_b)
            self._vec_cf.delete(key_b)
            self._idk_cf.delete(key_b)
            try:
                self._idi_cf.delete(_pack_i64(int_id))
            except NotFoundError:
                pass
            # Decision 21: remove metadata in same transaction
            if self._tags_cf is not None:
                try:
                    self._tags_cf.delete(key_b)
                except NotFoundError:
                    pass
            self._kv.commit()
        except Exception:
            self._kv.rollback()
            raise

        try:
            self._index.remove(int_id)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # ANN search (Decisions 14, 21, 23, 26)
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

        Parameters
        ----------
        query        : array-like, shape (dim,)
        top_k        : int — maximum results to return
        filter       : dict, callable, or None (Decision 21)
            dict      — equality filter; all fields must match
            callable  — predicate(metadata_dict) -> bool
            None      — no filter
        rerank       : bool — if True, re-score ANN candidates with exact
                       float32 distances from _snkv_vec_ (Decision 23)
        oversample   : int — candidate pool = top_k * oversample when
                       filter or rerank is active (default 3)
        max_distance : float or None — drop results with distance > threshold;
                       applied last after rerank (Decision 26)

        Returns
        -------
        List[SearchResult] — may be shorter than top_k (Decision 14).
        SearchResult.metadata is populated only when filter is provided.
        For metadata without filtering, call get_metadata(key) per result.
        """
        if top_k <= 0:
            raise ValueError("top_k must be > 0")
        if oversample < 1:
            raise ValueError(f"oversample must be >= 1, got {oversample}")
        if self._index is None or len(self._index) == 0:
            raise VectorIndexError(
                "Vector index is empty. Call vector_put() before search()."
            )

        q = np.asarray(query, dtype=np.float32).ravel()
        if q.shape[0] != self._dim:
            raise ValueError(
                f"query must have {self._dim} dimensions, got {q.shape[0]}"
            )

        # Determine fetch count — oversample when filter or rerank active
        needs_extra = (filter is not None) or rerank
        fetch_k = (
            min(len(self._index), top_k * oversample) if needs_extra else top_k
        )

        matches   = self._index.search(q.reshape(1, -1), fetch_k)
        # .ravel() handles both 1D (single query, usearch 2.23+) and 2D (batch) shapes
        labels    = np.asarray(matches.keys).ravel()
        ann_dists = np.asarray(matches.distances).ravel()

        # Normalise filter to callable
        if isinstance(filter, dict):
            _fd = dict(filter)
            filter_fn: Optional[Callable] = (
                lambda m: all(m.get(k) == v for k, v in _fd.items())
            )
        elif callable(filter):
            filter_fn = filter
        else:
            filter_fn = None

        # Collect valid candidates
        # Each entry: (key_b, value, ann_dist, vec_bytes_or_None, metadata_or_None)
        candidates: List[Tuple] = []
        for label, ann_dist in zip(labels, ann_dists):
            key_b = self._idi_cf.get(_pack_i64(int(label)))
            if key_b is None:
                continue
            value = self._kv.get(key_b)
            if value is None:
                continue  # expired

            vec_bytes = None
            if rerank:
                vec_bytes = self._vec_cf.get(key_b)
                if vec_bytes is None:
                    continue

            metadata = None
            if filter_fn is not None and self._tags_cf is not None:
                meta_bytes = self._tags_cf.get(key_b)
                if meta_bytes is not None:
                    try:
                        metadata = json.loads(meta_bytes.decode())
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        metadata = None

            candidates.append((key_b, value, float(ann_dist), vec_bytes, metadata))

        # Exact rerank (Decision 23): batch compute distances, sort ascending
        if rerank and candidates:
            vecs_arr = np.stack(
                [np.frombuffer(c[3], dtype=np.float32) for c in candidates]
            )
            exact = _exact_dist_batch(q, vecs_arr, self._space)
            candidates = [
                (c[0], c[1], float(exact[i]), c[3], c[4])
                for i, c in enumerate(candidates)
            ]
            candidates.sort(key=lambda c: c[2])

        # Apply filter, max_distance, top_k — max_distance applied last (Decision 26)
        results: List[SearchResult] = []
        for key_b, value, dist, _, metadata in candidates:
            if filter_fn is not None and not filter_fn(metadata or {}):
                continue
            if max_distance is not None and dist > max_distance:
                continue
            results.append(
                SearchResult(
                    key=key_b,
                    value=value,
                    distance=dist,
                    metadata=metadata if filter_fn is not None else None,
                )
            )
            if len(results) == top_k:
                break

        return results

    def search_keys(self, query, top_k: int = 10) -> List[Tuple[bytes, float]]:
        """ANN search returning (key, distance) pairs only — no value fetch."""
        if top_k <= 0:
            raise ValueError("top_k must be > 0")
        if self._index is None or len(self._index) == 0:
            raise VectorIndexError(
                "Vector index is empty. Call vector_put() before search()."
            )

        q = np.asarray(query, dtype=np.float32).ravel()
        if q.shape[0] != self._dim:
            raise ValueError(
                f"query must have {self._dim} dimensions, got {q.shape[0]}"
            )
        matches = self._index.search(q.reshape(1, -1), top_k)

        results = []
        for label, dist in zip(np.asarray(matches.keys).ravel(),
                               np.asarray(matches.distances).ravel()):
            key_b = self._idi_cf.get(_pack_i64(int(label)))
            if key_b is None:
                continue
            if self._kv.get(key_b) is None:  # expired or missing
                continue
            results.append((key_b, float(dist)))
        return results

    # ------------------------------------------------------------------
    # Index statistics (Decision 24)
    # ------------------------------------------------------------------

    def vector_stats(self) -> dict:
        """
        Return index configuration and runtime state.

        Keys
        ----
        dim, space, dtype, connectivity, expansion_add, expansion_search
        count        — active (non-deleted) vectors in usearch index
        capacity     — allocated capacity in usearch index
        fill_ratio   — count / capacity
        vec_cf_count — entries in _snkv_vec_ (may include expired)
        has_metadata — True if _snkv_vec_tags_ CF exists
        """
        count     = len(self._index) if self._index else 0
        capacity  = self._index.capacity if self._index else 0
        fill_ratio = count / capacity if capacity > 0 else 0.0
        return {
            "dim":              self._dim,
            "space":            self._space,
            "dtype":            self._dtype,
            "connectivity":     self._connectivity,
            "expansion_add":    self._expansion_add,
            "expansion_search": self._expansion_search,
            "count":            count,
            "capacity":         capacity,
            "fill_ratio":       fill_ratio,
            "vec_cf_count":     self._vec_cf.count() if self._vec_cf else 0,
            "has_metadata":     self._tags_cf is not None,
        }

    def vector_info(self) -> dict:
        """Return basic index configuration (subset of vector_stats)."""
        return {
            "dim":              self._dim,
            "space":            self._space,
            "dtype":            self._dtype,
            "count":            len(self._index) if self._index else 0,
            "connectivity":     self._connectivity,
            "expansion_add":    self._expansion_add,
            "expansion_search": self._expansion_search,
        }

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def vector_purge_expired(self) -> int:
        """
        Delete expired vectors from the HNSW index and all _snkv_vec*_ CFs.
        Returns count of vectors removed (Decision 10).

        Call this method (not KVStore.purge_expired) to keep vector CFs in sync.
        """
        if self._index is None or self._vec_cf is None:
            return 0

        expired: List[Tuple[bytes, int]] = []
        with self._vec_cf.iterator() as it:
            for key_b, _ in it:
                if self._kv.get(key_b) is None:
                    id_raw = self._idk_cf.get(key_b)
                    if id_raw is not None:
                        expired.append((key_b, _unpack_i64(id_raw)))

        if not expired:
            return 0

        self._kv.begin(write=True)
        try:
            for key_b, int_id in expired:
                for cf in (self._vec_cf, self._idk_cf):
                    try:
                        cf.delete(key_b)
                    except NotFoundError:
                        pass
                try:
                    self._idi_cf.delete(_pack_i64(int_id))
                except NotFoundError:
                    pass
                if self._tags_cf is not None:
                    try:
                        self._tags_cf.delete(key_b)
                    except NotFoundError:
                        pass
            self._kv.commit()
        except Exception:
            self._kv.rollback()
            raise

        for _, int_id in expired:
            try:
                self._index.remove(int_id)
            except Exception:
                pass

        return len(expired)

    def drop_vector_index(self) -> None:
        """
        Drop all _snkv_vec*_ CFs. KV data in the default CF is preserved
        and accessible via get() (Decision 17).

        After this call, search() raises VectorIndexError.
        """
        drop_list = [
            (self._vec_cf,  _CF_VEC),
            (self._idk_cf,  _CF_IDK),
            (self._idi_cf,  _CF_IDI),
            (self._meta_cf, _CF_META),
        ]
        if self._tags_cf is not None:
            drop_list.append((self._tags_cf, _CF_TAGS))

        for cf, name in drop_list:
            try:
                cf.close()
            except Exception:
                pass
            try:
                self._kv.drop_column_family(name)
            except NotFoundError:
                pass

        self._vec_cf  = None
        self._idk_cf  = None
        self._idi_cf  = None
        self._meta_cf = None
        self._tags_cf = None
        self._index   = None
        self._next_id = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close all CF handles and the underlying KVStore."""
        cfs = [self._vec_cf, self._idk_cf, self._idi_cf, self._meta_cf, self._tags_cf]
        for cf in cfs:
            if cf is not None:
                try:
                    cf.close()
                except Exception:
                    pass
        self._kv.close()

    def __enter__(self) -> "VectorStore":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def __repr__(self) -> str:
        count = len(self._index) if self._index else 0
        return (
            f"VectorStore(dim={self._dim}, space={self._space!r}, "
            f"dtype={self._dtype!r}, count={count})"
        )


__all__ = [
    "VectorStore",
    "SearchResult",
    "VectorIndexError",
]