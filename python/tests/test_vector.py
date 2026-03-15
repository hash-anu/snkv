"""
Vector search test suite — covers T1-T27 from the design doc plus base tests.

Run with:
    pytest python/tests/test_vector.py
or from the python/ directory:
    pytest tests/test_vector.py

Requires:
    pip install snkv[vector]
"""

import pytest

pytest.importorskip("usearch", reason="usearch not installed; skip vector tests")
pytest.importorskip("numpy",   reason="numpy not installed; skip vector tests")

import numpy as np
from snkv import NotFoundError
from snkv.vector import VectorStore, SearchResult, VectorIndexError

DIM = 16  # small dimension keeps tests fast


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_store(tmp_path, name="test.db", **kwargs):
    return VectorStore(str(tmp_path / name), dim=DIM, **kwargs)


def rand_vecs(n, dim=DIM, seed=42):
    rng = np.random.default_rng(seed)
    return rng.standard_normal((n, dim)).astype(np.float32)


def eye_vecs(dim=DIM):
    """Well-separated unit vectors — useful for deterministic nearest-neighbour tests."""
    return np.eye(dim, dtype=np.float32) * 10.0


# ---------------------------------------------------------------------------
# Base tests
# ---------------------------------------------------------------------------

class TestBase:
    def test_put_and_get(self, tmp_path):
        with make_store(tmp_path) as vs:
            vecs = rand_vecs(3)
            vs.vector_put(b"a", b"val_a", vecs[0])
            vs.vector_put(b"b", b"val_b", vecs[1])
            assert vs.get(b"a") == b"val_a"
            assert vs.get(b"b") == b"val_b"

    def test_vector_get(self, tmp_path):
        with make_store(tmp_path) as vs:
            vec = rand_vecs(1)[0]
            vs.vector_put(b"k", b"v", vec)
            np.testing.assert_allclose(vs.vector_get(b"k"), vec, rtol=1e-5)

    def test_search_returns_exact_match_first(self, tmp_path):
        with make_store(tmp_path) as vs:
            vecs = rand_vecs(10)
            for i, v in enumerate(vecs):
                vs.vector_put(f"k{i}".encode(), f"v{i}".encode(), v)
            results = vs.search(vecs[0], top_k=3)
            assert len(results) > 0
            assert results[0].key == b"k0"

    def test_delete_removes_from_kv_and_index(self, tmp_path):
        with make_store(tmp_path) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
            vs.delete(b"k")
            assert vs.get(b"k") is None
            with pytest.raises(NotFoundError):
                vs.vector_get(b"k")

    def test_overwrite_updates_vector_and_value(self, tmp_path):
        with make_store(tmp_path) as vs:
            v1, v2 = rand_vecs(2)
            vs.vector_put(b"k", b"v1", v1)
            vs.vector_put(b"k", b"v2", v2)
            assert vs.get(b"k") == b"v2"
            np.testing.assert_allclose(vs.vector_get(b"k"), v2, rtol=1e-5)

    def test_wrong_dim_raises(self, tmp_path):
        with make_store(tmp_path) as vs:
            with pytest.raises(ValueError, match="shape"):
                vs.vector_put(b"k", b"v", np.zeros(DIM + 1, dtype=np.float32))

    def test_space_mismatch_on_reopen(self, tmp_path):
        path = str(tmp_path / "store.db")
        with VectorStore(path, dim=DIM, space="l2") as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
        with pytest.raises(ValueError, match="space mismatch"):
            VectorStore(path, dim=DIM, space="cosine")

    def test_dim_mismatch_on_reopen(self, tmp_path):
        path = str(tmp_path / "store.db")
        with VectorStore(path, dim=DIM) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
        with pytest.raises(ValueError, match="dim mismatch"):
            VectorStore(path, dim=DIM + 1)

    def test_search_empty_raises_vector_index_error(self, tmp_path):
        with make_store(tmp_path) as vs:
            with pytest.raises(VectorIndexError):
                vs.search(rand_vecs(1)[0])

    def test_setitem_does_not_touch_vector_index(self, tmp_path):
        with make_store(tmp_path) as vs:
            vs[b"plain"] = b"value"
            assert vs.get(b"plain") == b"value"
            with pytest.raises(NotFoundError):
                vs.vector_get(b"plain")

    def test_drop_vector_index_keeps_kv_data(self, tmp_path):
        with make_store(tmp_path) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
            vs.drop_vector_index()
            assert vs.get(b"k") == b"v"
            with pytest.raises(VectorIndexError):
                vs.search(rand_vecs(1)[0])

    def test_survives_close_and_reopen(self, tmp_path):
        path = str(tmp_path / "store.db")
        vec = rand_vecs(1)[0]
        with VectorStore(path, dim=DIM) as vs:
            for i in range(5):
                vs.vector_put(f"k{i}".encode(), b"v", rand_vecs(1, seed=i)[0])
        with VectorStore(path, dim=DIM) as vs:
            assert len(vs) == 5
            results = vs.search(vec, top_k=1)
            assert len(results) == 1

    def test_len(self, tmp_path):
        with make_store(tmp_path) as vs:
            assert len(vs) == 0
            vs.vector_put(b"k1", b"v", rand_vecs(1, seed=0)[0])
            assert len(vs) == 1
            vs.vector_put(b"k2", b"v", rand_vecs(1, seed=1)[0])
            assert len(vs) == 2
            vs.delete(b"k1")
            assert len(vs) == 1


# ---------------------------------------------------------------------------
# T1–T4 — Vector quantization (Decision 20)
# ---------------------------------------------------------------------------

class TestQuantization:
    def test_f16_nearest_neighbour_correct(self, tmp_path):
        """T1: dtype=f16 — nearest neighbour correct."""
        with make_store(tmp_path, dtype="f16") as vs:
            vecs = rand_vecs(10)
            for i, v in enumerate(vecs):
                vs.vector_put(f"k{i}".encode(), b"v", v)
            assert vs.search(vecs[0], top_k=1)[0].key == b"k0"

    def test_i8_nearest_neighbour_correct(self, tmp_path):
        """T2: dtype=i8 — nearest neighbour correct."""
        with make_store(tmp_path, dtype="i8") as vs:
            vecs = rand_vecs(10)
            for i, v in enumerate(vecs):
                vs.vector_put(f"k{i}".encode(), b"v", v)
            assert vs.search(vecs[0], top_k=1)[0].key == b"k0"

    def test_dtype_persists_on_reopen(self, tmp_path):
        """T3: dtype stored in meta, survives close + reopen."""
        path = str(tmp_path / "store.db")
        with VectorStore(path, dim=DIM, dtype="f16") as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
        with VectorStore(path, dim=DIM, dtype="f16") as vs:
            assert vs.vector_stats()["dtype"] == "f16"

    def test_dtype_mismatch_raises(self, tmp_path):
        """T4: dtype mismatch on reopen → ValueError."""
        path = str(tmp_path / "store.db")
        with VectorStore(path, dim=DIM, dtype="f16") as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
        with pytest.raises(ValueError, match="dtype mismatch"):
            VectorStore(path, dim=DIM, dtype="f32")


# ---------------------------------------------------------------------------
# T5–T10 — Metadata filtering (Decision 21)
# ---------------------------------------------------------------------------

class TestMetadataFiltering:
    def test_put_and_get_metadata(self, tmp_path):
        """T5: vector_put(metadata=…) → get_metadata() returns same dict."""
        with make_store(tmp_path) as vs:
            meta = {"category": "science", "year": 2024}
            vs.vector_put(b"doc1", b"content", rand_vecs(1)[0], metadata=meta)
            assert vs.get_metadata(b"doc1") == meta

    def test_search_filter_dict(self, tmp_path):
        """T6: search(filter=dict) — only matching docs returned."""
        with make_store(tmp_path, space="cosine") as vs:
            vecs = rand_vecs(6)
            for i, v in enumerate(vecs):
                cat = "x" if i % 2 == 0 else "y"
                vs.vector_put(f"k{i}".encode(), b"v", v, metadata={"cat": cat})
            results = vs.search(vecs[0], top_k=10, filter={"cat": "x"})
            assert len(results) > 0
            assert all(r.metadata["cat"] == "x" for r in results)
            keys = {r.key for r in results}
            assert b"k1" not in keys and b"k3" not in keys and b"k5" not in keys

    def test_search_filter_callable(self, tmp_path):
        """T7: search(filter=callable) — predicate applied correctly."""
        with make_store(tmp_path) as vs:
            vecs = rand_vecs(6)
            for i, v in enumerate(vecs):
                vs.vector_put(f"k{i}".encode(), b"v", v, metadata={"score": i * 10})
            results = vs.search(vecs[2], top_k=10,
                                filter=lambda m: m.get("score", 0) >= 20)
            assert len(results) > 0
            assert all(r.metadata["score"] >= 20 for r in results)

    def test_overwrite_without_metadata_preserves_existing(self, tmp_path):
        """T8: overwrite without metadata= preserves existing metadata."""
        with make_store(tmp_path) as vs:
            vs.vector_put(b"k", b"v1", rand_vecs(1, seed=0)[0],
                          metadata={"tag": "keep"})
            vs.vector_put(b"k", b"v2", rand_vecs(1, seed=1)[0])  # no metadata=
            assert vs.get_metadata(b"k") == {"tag": "keep"}

    def test_delete_removes_metadata(self, tmp_path):
        """T9: delete(key) removes entry from __snkv_vec_tags__."""
        with make_store(tmp_path) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0], metadata={"tag": "x"})
            vs.delete(b"k")
            assert vs.get_metadata(b"k") is None

    def test_selective_filter_under_delivers(self, tmp_path):
        """T10: highly selective filter returns < top_k, no exception."""
        with make_store(tmp_path) as vs:
            vecs = rand_vecs(8)
            for i, v in enumerate(vecs):
                vs.vector_put(f"k{i}".encode(), b"v", v,
                              metadata={"rare": i == 0})
            results = vs.search(vecs[0], top_k=8,
                                filter=lambda m: m.get("rare") is True)
            assert 1 <= len(results) < 8

    def test_clear_metadata_with_empty_dict(self, tmp_path):
        """metadata={} clears existing metadata entry."""
        with make_store(tmp_path) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0], metadata={"x": 1})
            vs.vector_put(b"k", b"v", rand_vecs(1, seed=1)[0], metadata={})
            assert vs.get_metadata(b"k") is None

    def test_get_metadata_no_tags_cf_returns_none(self, tmp_path):
        """get_metadata returns None when no metadata was ever written."""
        with make_store(tmp_path) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])  # no metadata
            assert vs.get_metadata(b"k") is None


# ---------------------------------------------------------------------------
# T11–T14 — Batch insert (Decision 22)
# ---------------------------------------------------------------------------

class TestBatchInsert:
    def test_batch_all_searchable(self, tmp_path):
        """T11: vector_put_batch(items) — all items searchable after call."""
        with make_store(tmp_path) as vs:
            vecs = rand_vecs(20)
            items = [(f"k{i}".encode(), f"v{i}".encode(), vecs[i])
                     for i in range(20)]
            vs.vector_put_batch(items)
            assert len(vs) == 20
            assert vs.search(vecs[0], top_k=1)[0].key == b"k0"

    def test_batch_count_correct(self, tmp_path):
        """T12: correct vector_stats()["count"] after batch."""
        with make_store(tmp_path) as vs:
            vecs = rand_vecs(15)
            vs.vector_put_batch(
                [(f"k{i}".encode(), b"v", vecs[i]) for i in range(15)]
            )
            assert vs.vector_stats()["count"] == 15

    def test_batch_empty_noop(self, tmp_path):
        """T13: empty batch — no-op, no exception."""
        with make_store(tmp_path) as vs:
            vs.vector_put_batch([])
            assert len(vs) == 0

    def test_batch_rollback_on_invalid_shape(self, tmp_path):
        """T14: invalid vector shape in batch — entire batch rolls back."""
        with make_store(tmp_path) as vs:
            good = rand_vecs(1)[0]
            bad  = np.zeros(DIM + 5, dtype=np.float32)
            with pytest.raises(ValueError, match="shape"):
                vs.vector_put_batch([
                    (b"good", b"v", good),
                    (b"bad",  b"v", bad),
                ])
            assert len(vs) == 0  # nothing committed

    def test_batch_with_metadata(self, tmp_path):
        """Batch with 4-tuple (key, value, vector, metadata)."""
        with make_store(tmp_path) as vs:
            vecs = rand_vecs(4)
            items = [
                (f"k{i}".encode(), b"v", vecs[i], {"idx": i})
                for i in range(4)
            ]
            vs.vector_put_batch(items)
            assert vs.get_metadata(b"k2") == {"idx": 2}

    def test_batch_last_occurrence_wins(self, tmp_path):
        """Duplicate key in batch — last occurrence wins."""
        with make_store(tmp_path) as vs:
            vec1 = rand_vecs(1, seed=1)[0]
            vec2 = rand_vecs(1, seed=2)[0]
            vs.vector_put_batch([
                (b"k", b"first",  vec1),
                (b"k", b"second", vec2),
            ])
            assert len(vs) == 1
            assert vs.get(b"k") == b"second"


# ---------------------------------------------------------------------------
# T15–T18 — Two-stage search / rerank (Decision 23)
# ---------------------------------------------------------------------------

class TestRerank:
    def test_rerank_f32_same_top_result(self, tmp_path):
        """T15: rerank=True with dtype=f32 — same top result as rerank=False."""
        with make_store(tmp_path, dtype="f32") as vs:
            vecs = rand_vecs(20)
            for i, v in enumerate(vecs):
                vs.vector_put(f"k{i}".encode(), b"v", v)
            q = vecs[5]
            r_plain = vs.search(q, top_k=1, rerank=False)
            r_rerank = vs.search(q, top_k=1, rerank=True)
            assert r_plain[0].key == r_rerank[0].key

    def test_rerank_i8_top1_matches_brute_force(self, tmp_path):
        """T16: rerank=True with dtype=i8 — top-1 matches brute-force exact."""
        vecs = eye_vecs()  # well-separated; top-1 is unambiguous
        with make_store(tmp_path, dtype="i8", space="l2") as vs:
            for i, v in enumerate(vecs):
                vs.vector_put(f"k{i}".encode(), b"v", v)
            # Query closest to k3
            results = vs.search(vecs[3], top_k=1, rerank=True, oversample=4)
            assert results[0].key == b"k3"

    def test_rerank_result_length_le_top_k(self, tmp_path):
        """T17: rerank=True, oversample=5 — result count ≤ top_k."""
        with make_store(tmp_path) as vs:
            vecs = rand_vecs(10)
            for i, v in enumerate(vecs):
                vs.vector_put(f"k{i}".encode(), b"v", v)
            results = vs.search(vecs[0], top_k=3, rerank=True, oversample=5)
            assert len(results) <= 3

    def test_filter_and_rerank_compose(self, tmp_path):
        """T18: search(filter=…, rerank=True) — filter and rerank compose correctly."""
        with make_store(tmp_path) as vs:
            vecs = rand_vecs(10)
            for i, v in enumerate(vecs):
                cat = "a" if i % 2 == 0 else "b"
                vs.vector_put(f"k{i}".encode(), b"v", v, metadata={"cat": cat})
            results = vs.search(vecs[0], top_k=5,
                                filter={"cat": "a"}, rerank=True)
            assert all(r.metadata["cat"] == "a" for r in results)
            assert len(results) <= 5


# ---------------------------------------------------------------------------
# T19–T21 — Index statistics (Decision 24)
# ---------------------------------------------------------------------------

class TestIndexStats:
    def test_stats_count_and_dim(self, tmp_path):
        """T19: vector_stats() returns correct count and dim after inserts."""
        with make_store(tmp_path) as vs:
            vecs = rand_vecs(7)
            for i, v in enumerate(vecs):
                vs.vector_put(f"k{i}".encode(), b"v", v)
            s = vs.vector_stats()
            assert s["count"] == 7
            assert s["dim"] == DIM

    def test_fill_ratio_in_range(self, tmp_path):
        """T20: vector_stats()["fill_ratio"] between 0.0 and 1.0."""
        with make_store(tmp_path) as vs:
            for i, v in enumerate(rand_vecs(5)):
                vs.vector_put(f"k{i}".encode(), b"v", v)
            s = vs.vector_stats()
            assert 0.0 <= s["fill_ratio"] <= 1.0

    def test_has_metadata_flag(self, tmp_path):
        """T21: has_metadata False before any metadata put, True after."""
        with make_store(tmp_path) as vs:
            vs.vector_put(b"k1", b"v", rand_vecs(1, seed=0)[0])
            assert vs.vector_stats()["has_metadata"] is False
            vs.vector_put(b"k2", b"v", rand_vecs(1, seed=1)[0],
                          metadata={"x": 1})
            assert vs.vector_stats()["has_metadata"] is True

    def test_stats_all_keys_present(self, tmp_path):
        """vector_stats() returns all documented keys."""
        expected = {
            "dim", "space", "dtype", "connectivity", "expansion_add",
            "expansion_search", "count", "capacity", "fill_ratio",
            "vec_cf_count", "has_metadata",
        }
        with make_store(tmp_path) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
            assert expected == set(vs.vector_stats().keys())

    def test_len_matches_stats_count(self, tmp_path):
        """len(vs) == vector_stats()["count"]."""
        with make_store(tmp_path) as vs:
            for i, v in enumerate(rand_vecs(4)):
                vs.vector_put(f"k{i}".encode(), b"v", v)
            assert len(vs) == vs.vector_stats()["count"]


# ---------------------------------------------------------------------------
# T22–T24 — expansion_search persistence (Decision 25)
# ---------------------------------------------------------------------------

class TestExpansionSearchPersistence:
    def test_restores_on_reopen_without_arg(self, tmp_path):
        """T22: custom expansion_search restored on reopen without arg."""
        path = str(tmp_path / "store.db")
        with VectorStore(path, dim=DIM, expansion_search=128) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
        # Reopen without passing expansion_search — should restore 128
        with VectorStore(path, dim=DIM) as vs:
            assert vs.vector_stats()["expansion_search"] == 128

    def test_stats_reflect_stored_value_after_reopen(self, tmp_path):
        """T23: vector_stats()["expansion_search"] reflects stored value."""
        path = str(tmp_path / "store.db")
        with VectorStore(path, dim=DIM, expansion_search=96) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
        with VectorStore(path, dim=DIM) as vs:
            assert vs.vector_stats()["expansion_search"] == 96

    def test_explicit_arg_overrides_stored(self, tmp_path):
        """T24: explicit expansion_search=32 overrides stored value of 128."""
        path = str(tmp_path / "store.db")
        with VectorStore(path, dim=DIM, expansion_search=128) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
        with VectorStore(path, dim=DIM, expansion_search=32) as vs:
            assert vs.vector_stats()["expansion_search"] == 32

    def test_default_expansion_search_is_64_for_new_store(self, tmp_path):
        """New store with no expansion_search arg defaults to 64."""
        with make_store(tmp_path) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
            assert vs.vector_stats()["expansion_search"] == 64


# ---------------------------------------------------------------------------
# T25–T27 — max_distance filter (Decision 26)
# ---------------------------------------------------------------------------

class TestMaxDistance:
    def test_zero_distance_returns_empty(self, tmp_path):
        """T25: search(max_distance=0.0) with non-matching query → empty, no exception."""
        with make_store(tmp_path, space="l2") as vs:
            for i, v in enumerate(rand_vecs(5)):
                vs.vector_put(f"k{i}".encode(), b"v", v)
            # Query not in the index — all L2sq distances > 0
            query = rand_vecs(1, seed=999)[0]
            results = vs.search(query, top_k=5, max_distance=0.0)
            assert results == []

    def test_large_distance_same_as_no_threshold(self, tmp_path):
        """T26: search(max_distance=very_large) → same results as no threshold."""
        with make_store(tmp_path) as vs:
            vecs = rand_vecs(10)
            for i, v in enumerate(vecs):
                vs.vector_put(f"k{i}".encode(), b"v", v)
            r_none  = vs.search(vecs[0], top_k=5)
            r_large = vs.search(vecs[0], top_k=5, max_distance=1e18)
            assert [r.key for r in r_none] == [r.key for r in r_large]

    def test_filter_rerank_max_distance_compose(self, tmp_path):
        """T27: filter + rerank + max_distance all three compose correctly."""
        with make_store(tmp_path, space="l2") as vs:
            vecs = rand_vecs(15)
            for i, v in enumerate(vecs):
                cat = "target" if i < 5 else "other"
                vs.vector_put(f"k{i}".encode(), b"v", v, metadata={"cat": cat})
            results = vs.search(
                vecs[0], top_k=10,
                filter={"cat": "target"},
                rerank=True,
                max_distance=1e18,
            )
            assert all(r.metadata["cat"] == "target" for r in results)
            assert all(r.distance <= 1e18 for r in results)

    def test_max_distance_inclusive_bound(self, tmp_path):
        """distance == max_distance is kept (Decision 26: strictly greater than drops)."""
        with make_store(tmp_path, space="l2") as vs:
            vec = rand_vecs(1)[0]
            vs.vector_put(b"k", b"v", vec)
            # The same vector has distance 0.0 from itself
            results = vs.search(vec, top_k=1, max_distance=0.0)
            # 0.0 <= 0.0 → kept
            assert len(results) == 1
            assert results[0].key == b"k"


# ---------------------------------------------------------------------------
# search_keys
# ---------------------------------------------------------------------------

class TestSearchKeys:
    def test_returns_key_distance_pairs(self, tmp_path):
        with make_store(tmp_path) as vs:
            vecs = rand_vecs(5)
            for i, v in enumerate(vecs):
                vs.vector_put(f"k{i}".encode(), b"v", v)
            results = vs.search_keys(vecs[0], top_k=3)
            assert len(results) == 3
            keys, dists = zip(*results)
            assert b"k0" in keys
            assert all(isinstance(d, float) for d in dists)

    def test_returns_at_most_top_k(self, tmp_path):
        with make_store(tmp_path) as vs:
            for i, v in enumerate(rand_vecs(10)):
                vs.vector_put(f"k{i}".encode(), b"v", v)
            assert len(vs.search_keys(rand_vecs(1, seed=99)[0], top_k=4)) <= 4

    def test_empty_index_raises(self, tmp_path):
        with make_store(tmp_path) as vs:
            with pytest.raises(VectorIndexError):
                vs.search_keys(rand_vecs(1)[0])

    def test_no_value_fetched(self, tmp_path):
        """search_keys result contains (bytes, float) only — no value field."""
        with make_store(tmp_path) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
            results = vs.search_keys(rand_vecs(1, seed=7)[0], top_k=1)
            assert len(results) == 1
            key, dist = results[0]
            assert isinstance(key, bytes)
            assert isinstance(dist, float)

    def test_wrong_dim_raises(self, tmp_path):
        with make_store(tmp_path) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
            with pytest.raises(ValueError, match="dimensions"):
                vs.search_keys(np.zeros(DIM + 1, dtype=np.float32))

    def test_skips_expired_keys(self, tmp_path):
        """search_keys silently drops expired entries (Decision 10)."""
        import time
        with make_store(tmp_path) as vs:
            vec = rand_vecs(1)[0]
            vs.vector_put(b"live", b"v", rand_vecs(1, seed=1)[0])
            vs.vector_put(b"exp",  b"v", vec, ttl=0.01)
            time.sleep(0.05)
            results = vs.search_keys(vec, top_k=5)
            keys = [k for k, _ in results]
            assert b"exp" not in keys


# ---------------------------------------------------------------------------
# TTL in vector_put and vector_put_batch
# ---------------------------------------------------------------------------

class TestTTL:
    def test_expired_key_skipped_in_search(self, tmp_path):
        """Expired vector silently dropped from search results (Decision 10)."""
        import time
        with make_store(tmp_path) as vs:
            live_vec = rand_vecs(1, seed=1)[0]
            exp_vec  = rand_vecs(1, seed=2)[0]
            vs.vector_put(b"live", b"v", live_vec)
            vs.vector_put(b"exp",  b"v", exp_vec, ttl=0.01)
            time.sleep(0.05)
            results = vs.search(exp_vec, top_k=5)
            assert all(r.key != b"exp" for r in results)

    def test_expired_key_not_returned_by_get(self, tmp_path):
        import time
        with make_store(tmp_path) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0], ttl=0.01)
            time.sleep(0.05)
            assert vs.get(b"k") is None

    def test_batch_ttl_applied_to_all(self, tmp_path):
        """vector_put_batch(ttl=...) expires all items uniformly."""
        import time
        with make_store(tmp_path) as vs:
            vecs = rand_vecs(3)
            vs.vector_put_batch(
                [(f"k{i}".encode(), b"v", vecs[i]) for i in range(3)],
                ttl=0.01,
            )
            time.sleep(0.05)
            assert all(vs.get(f"k{i}".encode()) is None for i in range(3))


# ---------------------------------------------------------------------------
# vector_purge_expired
# ---------------------------------------------------------------------------

class TestPurgeExpired:
    def test_purge_removes_expired_vectors(self, tmp_path):
        import time
        with make_store(tmp_path) as vs:
            vs.vector_put(b"live", b"v", rand_vecs(1, seed=0)[0])
            vs.vector_put(b"exp1", b"v", rand_vecs(1, seed=1)[0], ttl=0.01)
            vs.vector_put(b"exp2", b"v", rand_vecs(1, seed=2)[0], ttl=0.01)
            time.sleep(0.05)
            removed = vs.vector_purge_expired()
            assert removed == 2
            assert vs.vector_stats()["vec_cf_count"] == 1

    def test_purge_removes_from_index(self, tmp_path):
        """After purge, expired labels no longer in usearch index."""
        import time
        with make_store(tmp_path) as vs:
            vs.vector_put(b"live", b"v", rand_vecs(1, seed=0)[0])
            vs.vector_put(b"exp",  b"v", rand_vecs(1, seed=1)[0], ttl=0.01)
            time.sleep(0.05)
            before = len(vs)
            vs.vector_purge_expired()
            assert len(vs) == before - 1

    def test_purge_also_removes_metadata(self, tmp_path):
        import time
        with make_store(tmp_path) as vs:
            vs.vector_put(b"exp", b"v", rand_vecs(1)[0],
                          ttl=0.01, metadata={"x": 1})
            time.sleep(0.05)
            vs.vector_purge_expired()
            assert vs.get_metadata(b"exp") is None

    def test_purge_no_expired_returns_zero(self, tmp_path):
        with make_store(tmp_path) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
            assert vs.vector_purge_expired() == 0

    def test_purge_empty_store_returns_zero(self, tmp_path):
        with make_store(tmp_path) as vs:
            assert vs.vector_purge_expired() == 0


# ---------------------------------------------------------------------------
# Encrypted store (Decision 18)
# ---------------------------------------------------------------------------

class TestEncryption:
    def test_encrypted_put_and_search(self, tmp_path):
        """Basic vector_put + search works through encryption layer."""
        path = str(tmp_path / "enc.db")
        vecs = rand_vecs(5)
        with VectorStore(path, dim=DIM, password=b"secret") as vs:
            for i, v in enumerate(vecs):
                vs.vector_put(f"k{i}".encode(), b"v", v)
            assert vs.search(vecs[0], top_k=1)[0].key == b"k0"

    def test_encrypted_survives_reopen(self, tmp_path):
        path = str(tmp_path / "enc.db")
        vec = rand_vecs(1)[0]
        with VectorStore(path, dim=DIM, password="mypass") as vs:
            vs.vector_put(b"k", b"hello", vec)
        with VectorStore(path, dim=DIM, password="mypass") as vs:
            assert vs.get(b"k") == b"hello"
            assert len(vs) == 1

    def test_wrong_password_raises(self, tmp_path):
        from snkv import AuthError
        path = str(tmp_path / "enc.db")
        with VectorStore(path, dim=DIM, password=b"correct") as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
        with pytest.raises(AuthError):
            VectorStore(path, dim=DIM, password=b"wrong")


# ---------------------------------------------------------------------------
# Dict interface (__getitem__, __contains__, __delitem__)
# ---------------------------------------------------------------------------

class TestDictInterface:
    def test_getitem_returns_value(self, tmp_path):
        with make_store(tmp_path) as vs:
            vs.vector_put(b"k", b"val", rand_vecs(1)[0])
            assert vs[b"k"] == b"val"

    def test_getitem_plain_kv_key(self, tmp_path):
        with make_store(tmp_path) as vs:
            vs[b"plain"] = b"data"
            assert vs[b"plain"] == b"data"

    def test_getitem_missing_raises(self, tmp_path):
        with make_store(tmp_path) as vs:
            with pytest.raises(Exception):  # NotFoundError / KeyError
                _ = vs[b"missing"]

    def test_contains_true_for_existing(self, tmp_path):
        with make_store(tmp_path) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
            assert b"k" in vs

    def test_contains_false_for_missing(self, tmp_path):
        with make_store(tmp_path) as vs:
            assert b"nope" not in vs

    def test_delitem_removes_key(self, tmp_path):
        with make_store(tmp_path) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
            del vs[b"k"]
            assert vs.get(b"k") is None


# ---------------------------------------------------------------------------
# connectivity and expansion_add persistence
# ---------------------------------------------------------------------------

class TestHNSWParamPersistence:
    def test_connectivity_restored_on_reopen(self, tmp_path):
        """Stored connectivity wins over constructor arg on reopen (Decision 12)."""
        path = str(tmp_path / "store.db")
        with VectorStore(path, dim=DIM, connectivity=8) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
        # Reopen with different connectivity — stored value must win
        with VectorStore(path, dim=DIM, connectivity=32) as vs:
            assert vs.vector_stats()["connectivity"] == 8

    def test_expansion_add_restored_on_reopen(self, tmp_path):
        path = str(tmp_path / "store.db")
        with VectorStore(path, dim=DIM, expansion_add=64) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
        with VectorStore(path, dim=DIM, expansion_add=256) as vs:
            assert vs.vector_stats()["expansion_add"] == 64


# ---------------------------------------------------------------------------
# vector_info
# ---------------------------------------------------------------------------

class TestVectorInfo:
    def test_vector_info_keys(self, tmp_path):
        expected = {"dim", "space", "dtype", "count",
                    "connectivity", "expansion_add", "expansion_search"}
        with make_store(tmp_path) as vs:
            vs.vector_put(b"k", b"v", rand_vecs(1)[0])
            assert set(vs.vector_info().keys()) == expected

    def test_vector_info_count_matches(self, tmp_path):
        with make_store(tmp_path) as vs:
            for i, v in enumerate(rand_vecs(3)):
                vs.vector_put(f"k{i}".encode(), b"v", v)
            assert vs.vector_info()["count"] == 3


# ---------------------------------------------------------------------------
# ip space
# ---------------------------------------------------------------------------

class TestIPSpace:
    def test_ip_search_returns_results(self, tmp_path):
        with make_store(tmp_path, space="ip") as vs:
            vecs = rand_vecs(10)
            for i, v in enumerate(vecs):
                vs.vector_put(f"k{i}".encode(), b"v", v)
            results = vs.search(vecs[0], top_k=3)
            assert len(results) > 0

    def test_ip_rerank_consistent(self, tmp_path):
        """rerank=True with ip space returns same top-1 as plain search."""
        with make_store(tmp_path, space="ip", dtype="f32") as vs:
            vecs = rand_vecs(10)
            for i, v in enumerate(vecs):
                vs.vector_put(f"k{i}".encode(), b"v", v)
            plain  = vs.search(vecs[3], top_k=1)
            rerank = vs.search(vecs[3], top_k=1, rerank=True)
            assert plain[0].key == rerank[0].key
