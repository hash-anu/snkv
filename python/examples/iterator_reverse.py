# SPDX-License-Identifier: Apache-2.0
"""
Reverse Iterator Examples
Demonstrates: descending key scans on the default CF and named CFs.

Run:
    python examples/iterator_reverse.py

Sections:
    1. Full reverse scan (all keys, descending)
    2. Reverse prefix scan
    3. Column-family reverse scan
    4. Using iterator(reverse=True, prefix=...) unified API
    5. Real-world: leaderboard top-N using padded score keys
"""

import os

from snkv import KVStore

DB_FILE = "rev_iter_example.db"


# ---------------------------------------------------------------------------
# 1. Full reverse scan
# ---------------------------------------------------------------------------
def section_full_reverse(db: KVStore) -> None:
    print("\n--- 1. Full Reverse Scan ---")

    db.put("apple",      "0.50")
    db.put("banana",     "0.30")
    db.put("cherry",     "1.20")
    db.put("date",       "2.00")
    db.put("elderberry", "3.50")

    print(f"  {'Fruit':<14} {'Price':>5}")
    print("  " + "-" * 20)

    # iterator(reverse=True) — unified API
    for key, value in db.iterator(reverse=True):
        print(f"  {key.decode():<14} {value.decode():>5}")

    # Clean up for next section
    for k in [b"apple", b"banana", b"cherry", b"date", b"elderberry"]:
        db.delete(k)


# ---------------------------------------------------------------------------
# 2. Reverse prefix scan
# ---------------------------------------------------------------------------
def section_reverse_prefix(db: KVStore) -> None:
    print("\n--- 2. Reverse Prefix Scan ---")

    db.put("admin:root",    "active")
    db.put("user:alice",    "online")
    db.put("user:bob",      "offline")
    db.put("user:charlie",  "online")
    db.put("user:diana",    "away")
    db.put("user:eve",      "online")

    # Reverse prefix: eve → diana → charlie → bob → alice  (admin: excluded)
    print("  User keys in descending order:")
    for key, value in db.iterator(prefix="user:", reverse=True):
        print(f"    {key.decode()} -> {value.decode()}")

    for k in [b"admin:root", b"user:alice", b"user:bob",
              b"user:charlie", b"user:diana", b"user:eve"]:
        db.delete(k)


# ---------------------------------------------------------------------------
# 3. Column-family reverse scan
# ---------------------------------------------------------------------------
def section_cf_reverse(db: KVStore) -> None:
    print("\n--- 3. Column Family Reverse Scan ---")

    with db.create_column_family("products") as cf:
        cf.put("gizmo",     "9.99")
        cf.put("gadget",    "24.99")
        cf.put("widget",    "4.99")
        cf.put("doohickey", "14.99")

        print(f"  {'Product':<12} {'Price':>6}")
        print("  " + "-" * 20)

        # CF-level reverse_iterator()
        for key, value in cf.reverse_iterator():
            print(f"  {key.decode():<12} {value.decode():>6}")

    db.drop_column_family("products")


# ---------------------------------------------------------------------------
# 4. Unified iterator() API — all four combinations
# ---------------------------------------------------------------------------
def section_unified_api(db: KVStore) -> None:
    print("\n--- 4. Unified iterator() API ---")

    db.put("fruit:apple",  "a")
    db.put("fruit:banana", "b")
    db.put("fruit:cherry", "c")
    db.put("veg:broccoli", "d")
    db.put("veg:carrot",   "e")

    combos = [
        (dict(),                                 "forward, all"),
        (dict(reverse=True),                     "reverse, all"),
        (dict(prefix="fruit:"),                  "forward, prefix=fruit:"),
        (dict(prefix="fruit:", reverse=True),    "reverse, prefix=fruit:"),
    ]

    for kwargs, label in combos:
        keys = [k.decode() for k, _ in db.iterator(**kwargs)]
        print(f"  [{label}]")
        print(f"    {keys}")

    for k in [b"fruit:apple", b"fruit:banana", b"fruit:cherry",
              b"veg:broccoli", b"veg:carrot"]:
        db.delete(k)


# ---------------------------------------------------------------------------
# 5. Leaderboard: top-5 scores via reverse prefix scan
#    Keys: "score:<08d>:<player>" — descending lex = descending score
# ---------------------------------------------------------------------------
def section_leaderboard(db: KVStore) -> None:
    print("\n--- 5. Leaderboard: Top-5 Scores ---")

    scores = {
        "alice":   9200,
        "bob":     7800,
        "charlie": 12500,
        "diana":   3400,
        "eve":     11000,
        "frank":   8600,
        "grace":   15000,
        "hank":    4200,
        "iris":    9800,
        "jake":    6300,
    }

    for player, score in scores.items():
        key = f"score:{score:08d}:{player}"
        db.put(key, player)

    print(f"  {'Rank':<6} {'Player':<10} {'Score':>6}")
    print("  " + "-" * 24)

    for rank, (key_b, _) in enumerate(
        db.iterator(prefix="score:", reverse=True), start=1
    ):
        if rank > 5:
            break
        key = key_b.decode()  # "score:XXXXXXXX:player"
        parts = key.split(":")
        player = parts[2]
        score  = int(parts[1])
        print(f"  {rank:<6} {player:<10} {score:>6}")

    # Clean up
    for player, score in scores.items():
        db.delete(f"score:{score:08d}:{player}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> None:
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

    with KVStore(DB_FILE) as db:
        section_full_reverse(db)
        section_reverse_prefix(db)
        section_cf_reverse(db)
        section_unified_api(db)
        section_leaderboard(db)

    os.remove(DB_FILE)
    print("\nAll reverse iterator examples passed.")


if __name__ == "__main__":
    main()
