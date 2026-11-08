# SPDX-License-Identifier: Apache-2.0
"""
VectorStore Example
Demonstrates: HNSW vector search integrated with SNKV key-value storage.

Run:
    pip install snkv[vector]
    python examples/vector.py

Sections:
    1. Basic vector_put and search
    2. Batch insert with vector_put_batch
    3. Metadata filtering
    4. Exact rerank + max_distance
    5. search_keys — keys and distances only
    6. TTL on vectors
    7. vector_purge_expired
    8. Encrypted VectorStore
    9. drop_vector_index — preserve KV, drop vectors
    10. Real-world: semantic document store
    11. Sidecar index persistence (fast reopen)
    12. Quantization — f16 / i8 to reduce in-memory index size
"""

import os
import tempfile

import numpy as np

from snkv.vector import VectorStore, VectorIndexError

DIM = 64  # small dimension keeps the example fast


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def tmp(suffix=".db"):
    fd, path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    cleanup(path)
    return path


def cleanup(path):
    """Remove .db, .usearch sidecar, and .nid stamp if present."""
    for p in (path, path + ".usearch", path + ".usearch.nid"):
        if os.path.exists(p):
            os.unlink(p)


def rand(n=1):
    """Return n random unit vectors of shape (DIM,) each."""
    vecs = np.random.rand(n, DIM).astype("f4")
    norms = np.linalg.norm(vecs, axis=1, keepdims=True)
    return vecs / (norms + 1e-10)


# ---------------------------------------------------------------------------
# 1. Basic vector_put and search
# ---------------------------------------------------------------------------
print("── 1. Basic vector_put and search ──────────────────────────────────")

path = tmp()
with VectorStore(path, dim=DIM, space="cosine") as vs:
    docs = {
        b"doc:python":     (b"Python is a high-level programming language.",  rand()[0]),
        b"doc:rust":       (b"Rust provides memory safety without GC.",        rand()[0]),
        b"doc:databases":  (b"Databases store and retrieve structured data.",  rand()[0]),
        b"doc:ml":         (b"Machine learning trains models on data.",        rand()[0]),
        b"doc:networking": (b"Networking connects computers across the world.", rand()[0]),
    }

    for key, (value, vec) in docs.items():
        vs.vector_put(key, value, vec)

    print(f"  stored {len(vs)} vectors")

    # Search using the "python" doc vector as query
    query = docs[b"doc:python"][1]
    results = vs.search(query, top_k=3)
    print("  top-3 nearest to doc:python:")
    for r in results:
        print(f"    {r.key.decode():<20} dist={r.distance:.4f}  val={r.value.decode()[:40]}")

cleanup(path)


# ---------------------------------------------------------------------------
# 2. Batch insert with vector_put_batch
# ---------------------------------------------------------------------------
print("\n── 2. Batch insert ──────────────────────────────────────────────────")

path = tmp()
with VectorStore(path, dim=DIM, space="l2") as vs:
    n = 500
    items = [
        (f"item:{i}".encode(), f"value-{i}".encode(), rand()[0])
        for i in range(n)
    ]
    vs.vector_put_batch(items)
    print(f"  inserted {len(vs)} vectors via vector_put_batch")

    q = rand()[0]
    results = vs.search(q, top_k=5)
    print(f"  search top-5 distances: {[round(r.distance, 4) for r in results]}")

cleanup(path)


# ---------------------------------------------------------------------------
# 3. Metadata filtering
# ---------------------------------------------------------------------------
print("\n── 3. Metadata filtering ────────────────────────────────────────────")

path = tmp()
with VectorStore(path, dim=DIM, space="cosine") as vs:
    articles = [
        (b"art:1", b"Deep learning in NLP",          rand()[0], {"topic": "ml",  "year": 2023}),
        (b"art:2", b"Key-value store internals",      rand()[0], {"topic": "db",  "year": 2022}),
        (b"art:3", b"Transformer architectures",      rand()[0], {"topic": "ml",  "year": 2024}),
        (b"art:4", b"B-tree vs LSM-tree performance", rand()[0], {"topic": "db",  "year": 2023}),
        (b"art:5", b"Diffusion models explained",     rand()[0], {"topic": "ml",  "year": 2024}),
    ]
    vs.vector_put_batch(articles)

    q = rand()[0]

    # Dict filter — equality match
    ml_results = vs.search(q, top_k=5, filter={"topic": "ml"})
    print(f"  topic=ml results: {[r.key.decode() for r in ml_results]}")

    # Callable filter — predicate
    recent = vs.search(q, top_k=5, filter=lambda m: m.get("year", 0) >= 2024)
    print(f"  year>=2024 results: {[r.key.decode() for r in recent]}")

    # Metadata round-trip
    meta = vs.get_metadata(b"art:3")
    print(f"  get_metadata(art:3) = {meta}")

cleanup(path)


# ---------------------------------------------------------------------------
# 4. Exact rerank + max_distance
# ---------------------------------------------------------------------------
print("\n── 4. Exact rerank + max_distance ───────────────────────────────────")

path = tmp()
with VectorStore(path, dim=DIM, space="cosine") as vs:
    vecs = rand(100)
    vs.vector_put_batch(
        [(f"k:{i}".encode(), f"v:{i}".encode(), vecs[i]) for i in range(100)]
    )

    q = vecs[0]  # query identical to k:0 — should be distance ~0

    # ANN without rerank
    ann = vs.search(q, top_k=5)
    print(f"  ANN top-5 distances:    {[round(r.distance, 5) for r in ann]}")

    # With exact rerank
    reranked = vs.search(q, top_k=5, rerank=True)
    print(f"  Reranked top-5 dists:   {[round(r.distance, 5) for r in reranked]}")

    # Distance cutoff — only keep very close results
    close = vs.search(q, top_k=10, rerank=True, max_distance=0.05)
    print(f"  max_distance=0.05 kept: {len(close)} results")

cleanup(path)


# ---------------------------------------------------------------------------
# 5. search_keys — keys and distances only
# ---------------------------------------------------------------------------
print("\n── 5. search_keys ───────────────────────────────────────────────────")

path = tmp()
with VectorStore(path, dim=DIM, space="cosine") as vs:
    vecs = rand(50)
    vs.vector_put_batch(
        [(f"doc:{i}".encode(), f"val:{i}".encode(), vecs[i]) for i in range(50)]
    )

    q = rand()[0]
    pairs = vs.search_keys(q, top_k=5)
    print("  (key, distance) pairs:")
    for key, dist in pairs:
        print(f"    {key.decode():<12} {dist:.5f}")

cleanup(path)


# ---------------------------------------------------------------------------
# 6. TTL on vectors
# ---------------------------------------------------------------------------
print("\n── 6. TTL on vectors ────────────────────────────────────────────────")

path = tmp()
with VectorStore(path, dim=DIM, space="l2") as vs:
    permanent = rand()[0]
    expiring  = rand()[0]

    vs.vector_put(b"permanent", b"I live forever", permanent)
    vs.vector_put(b"expiring",  b"I expire soon",  expiring, ttl=0.05)  # 50 ms

    print(f"  before expiry: {len(vs)} vectors in index")

    import time
    time.sleep(0.1)

    # search transparently skips expired keys
    results = vs.search(expiring, top_k=5)
    keys_found = [r.key for r in results]
    print(f"  after expiry: b'expiring' in results = {b'expiring' in keys_found}")
    print(f"  after expiry: b'permanent' in results = {b'permanent' in keys_found}")

cleanup(path)


# ---------------------------------------------------------------------------
# 7. vector_purge_expired
# ---------------------------------------------------------------------------
print("\n── 7. vector_purge_expired ──────────────────────────────────────────")

path = tmp()
with VectorStore(path, dim=DIM, space="l2") as vs:
    vecs = rand(10)
    for i in range(10):
        ttl = 0.05 if i < 5 else None  # first 5 expire, last 5 don't
        vs.vector_put(f"k:{i}".encode(), f"v:{i}".encode(), vecs[i], ttl=ttl)

    import time
    time.sleep(0.1)

    before = vs.vector_stats()["count"]
    n = vs.vector_purge_expired()
    after = vs.vector_stats()["count"]

    print(f"  before purge: {before} in index")
    print(f"  purged: {n}")
    print(f"  after purge: {after} in index")

cleanup(path)


# ---------------------------------------------------------------------------
# 8. Encrypted VectorStore
# ---------------------------------------------------------------------------
print("\n── 8. Encrypted VectorStore ─────────────────────────────────────────")

path = tmp()
try:
    from snkv import AuthError

    vec = rand()[0]
    with VectorStore(path, dim=DIM, password=b"hunter2") as vs:
        vs.vector_put(b"secret", b"classified data", vec)
        results = vs.search(vec, top_k=1)
        print(f"  search on encrypted store: {results[0].key}")

    # Reopen with correct password
    with VectorStore(path, dim=DIM, password=b"hunter2") as vs:
        print(f"  reopened encrypted store, {len(vs)} vector(s)")

    # Wrong password raises AuthError
    try:
        with VectorStore(path, dim=DIM, password=b"wrong") as vs:
            pass
    except AuthError:
        print("  wrong password → AuthError raised correctly")

finally:
    if os.path.exists(path):
        cleanup(path)


# ---------------------------------------------------------------------------
# 9. drop_vector_index — preserve KV data
# ---------------------------------------------------------------------------
print("\n── 9. drop_vector_index ─────────────────────────────────────────────")

path = tmp()
with VectorStore(path, dim=DIM) as vs:
    vs.vector_put(b"doc:1", b"hello", rand()[0])
    vs.vector_put(b"doc:2", b"world", rand()[0])

    vs.drop_vector_index()

    # KV data still accessible
    val = vs.get(b"doc:1")
    print(f"  get after drop: {val}")

    # Vector operations raise VectorIndexError
    try:
        vs.search(rand()[0], top_k=1)
    except VectorIndexError as e:
        print(f"  search after drop → VectorIndexError: {e}")

cleanup(path)


# ---------------------------------------------------------------------------
# 10. Real-world: semantic document store
# ---------------------------------------------------------------------------
print("\n── 10. Real-world: semantic document store ──────────────────────────")

# Simulate a mini document store where vectors are embeddings and values
# are document content. Uses metadata for filtering by category.

path = tmp()
with VectorStore(path, dim=DIM, space="cosine", connectivity=16, expansion_add=128) as vs:
    # "Embed" documents (random vectors stand in for real embeddings)
    corpus = [
        ("intro-python",   "Introduction to Python programming",             "tutorial"),
        ("advanced-rust",  "Advanced Rust: lifetimes and ownership",         "tutorial"),
        ("sql-basics",     "SQL fundamentals for beginners",                 "tutorial"),
        ("ml-overview",    "Overview of machine learning algorithms",        "research"),
        ("vector-search",  "How HNSW indexes work for approximate search",   "research"),
        ("snkv-internals", "Inside SNKV: B-tree storage without SQL",        "research"),
        ("py-perf",        "Python performance tips and profiling tools",     "tutorial"),
        ("llm-fine-tune",  "Fine-tuning large language models",              "research"),
    ]

    embeddings = {slug: rand()[0] for slug, _, _ in corpus}

    vs.vector_put_batch([
        (slug.encode(), content.encode(), embeddings[slug], {"category": cat})
        for slug, content, cat in corpus
    ])

    print(f"  indexed {len(vs)} documents")

    # Query: find research papers related to "vector search"
    query_vec = embeddings["vector-search"]

    print("\n  all results near 'vector-search':")
    for r in vs.search(query_vec, top_k=4):
        print(f"    {r.key.decode():<22} dist={r.distance:.4f}")

    print("\n  research only near 'vector-search':")
    for r in vs.search(query_vec, top_k=4, filter={"category": "research"}):
        print(f"    {r.key.decode():<22} dist={r.distance:.4f}  {r.metadata}")

    # Stats
    stats = vs.vector_stats()
    print(f"\n  store stats: dim={stats['dim']}, space={stats['space']}, "
          f"count={stats['count']}, fill_ratio={stats['fill_ratio']:.2f}, "
          f"sidecar_enabled={stats['sidecar_enabled']}")

cleanup(path)

# ---------------------------------------------------------------------------
# 11. Sidecar index persistence (fast reopen)
# ---------------------------------------------------------------------------
print("\n── 11. Sidecar index persistence ────────────────────────────────────")

# For unencrypted file-backed stores, the HNSW index is saved to
# {path}.usearch on close.  The next open loads it directly — skipping
# the O(n×d) CF rebuild entirely.

path = tmp()
N = 300

with VectorStore(path, dim=DIM, space="cosine") as vs:
    vecs = rand(N)
    vs.vector_put_batch(
        [(f"item:{i}".encode(), f"val:{i}".encode(), vecs[i]) for i in range(N)]
    )
    print(f"  wrote {len(vs)} vectors, sidecar_enabled={vs.vector_stats()['sidecar_enabled']}")

# Sidecar files are now on disk alongside the .db
sidecar_exists = os.path.exists(path + ".usearch")
stamp_exists   = os.path.exists(path + ".usearch.nid")
print(f"  .usearch exists={sidecar_exists}, .nid exists={stamp_exists}")

# Reopen — loads from sidecar (no CF scan)
with VectorStore(path, dim=DIM, space="cosine") as vs:
    q = vecs[0]
    results = vs.search(q, top_k=3)
    print(f"  search after sidecar load: top key = {results[0].key.decode()}, "
          f"dist={results[0].distance:.5f}")
    print(f"  index size after reopen: {len(vs)} vectors")

# Encrypted store: sidecar is disabled (a plaintext index file would
# defeat the purpose of encryption — Decision 28)
path_enc = tmp()
try:
    from snkv import AuthError
    with VectorStore(path_enc, dim=DIM, password=b"secret", space="cosine") as vs:
        vs.vector_put_batch(
            [(f"enc:{i}".encode(), b"v", rand()[0]) for i in range(10)]
        )
        print(f"  encrypted store sidecar_enabled={vs.vector_stats()['sidecar_enabled']}")
    print(f"  .usearch absent for encrypted store: "
          f"{not os.path.exists(path_enc + '.usearch')}")
finally:
    cleanup(path_enc)

cleanup(path)


# ---------------------------------------------------------------------------
# 12. Quantization — f16 / i8 to reduce in-memory index size
# ---------------------------------------------------------------------------
print("\n── 12. Quantization ─────────────────────────────────────────────────")

# dtype controls in-memory HNSW precision only.
# On-disk storage in _snkv_vec_ is always float32 regardless of dtype.
#
# Memory per vector in the usearch index (DIM=64):
#   f32 → 64 × 4 = 256 bytes
#   f16 → 64 × 2 = 128 bytes  (½ RAM)
#   i8  → 64 × 1 =  64 bytes  (¼ RAM)
#
# Trade-off: lower precision → smaller index, slightly lower recall.

N = 500
vecs = rand(N)
items = [(f"doc:{i}".encode(), f"val:{i}".encode(), vecs[i]) for i in range(N)]
query = vecs[0]

results_by_dtype = {}
for dtype in ("f32", "f16", "i8"):
    path = tmp()
    with VectorStore(path, dim=DIM, space="cosine", dtype=dtype) as vs:
        vs.vector_put_batch(items)
        results = vs.search(query, top_k=5)
        results_by_dtype[dtype] = [r.key.decode() for r in results]
        stats = vs.vector_stats()
        print(f"  dtype={dtype}  count={stats['count']}  "
              f"capacity={stats['capacity']}  "
              f"top-1={results[0].key.decode()}  dist={results[0].distance:.5f}")
    cleanup(path)

# top-1 should be doc:0 (query == vecs[0]) for all dtypes
for dtype, keys in results_by_dtype.items():
    assert keys[0] == "doc:0", f"dtype={dtype}: expected doc:0 as top-1, got {keys[0]}"

print(f"\n  top-5 keys per dtype:")
for dtype, keys in results_by_dtype.items():
    print(f"    {dtype}: {keys}")
print("  (i8/f16 may differ from f32 due to quantization — top-1 always exact)")


print("\nAll examples completed.")
