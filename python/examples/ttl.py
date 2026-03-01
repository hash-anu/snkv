# SPDX-License-Identifier: Apache-2.0
"""
TTL (Time-To-Live) Example
Demonstrates: per-key expiry on the default CF and on named CFs.

Run:
    python examples/ttl.py

Sections:
    1. Basic put/get with ttl= on the default CF
    2. Lazy expiry — expired keys are deleted on access
    3. Bulk cleanup with purge_expired()
    4. CF-level TTL on a named column family
    5. TTL inside an explicit transaction
    6. Real-world: rate limiter backed by SNKV TTL
"""

import os
import time

from snkv import KVStore, NotFoundError, NO_TTL

DB_FILE = "ttl_example.db"


# ---------------------------------------------------------------------------
# 1. Basic put / get with ttl= on the default CF
# ---------------------------------------------------------------------------
def section_basic_ttl(db: KVStore) -> None:
    print("\n--- 1. Basic TTL on default CF ---")

    # Write a key that expires in 5 seconds.
    db.put(b"token", b"abc-xyz-789", ttl=5)
    print(f"  put(b'token', ttl=5)")

    # Read it back.
    val = db.get(b"token")
    print(f"  get(b'token') = {val}")

    # Inspect remaining TTL.
    remaining = db.ttl(b"token")
    print(f"  ttl(b'token') = {remaining:.3f} s remaining")

    # Overwrite with a plain put — TTL entry is removed.
    db.put(b"token", b"overwritten")
    print(f"  after plain put: ttl(b'token') = {db.ttl(b'token')!r}  (None = permanent)")


# ---------------------------------------------------------------------------
# 2. Lazy expiry — expired keys are silently deleted on access
# ---------------------------------------------------------------------------
def section_lazy_expiry(db: KVStore) -> None:
    print("\n--- 2. Lazy expiry ---")

    db.put(b"flash", b"here today", ttl=0.05)   # 50 ms
    print("  inserted b'flash' (expires in 50 ms)")

    time.sleep(0.1)   # wait past expiry

    val = db.get(b"flash")   # triggers lazy delete
    print(f"  get(b'flash') after expiry = {val!r}  (None = expired)")

    # ttl() also performs lazy delete and returns 0.0 for a just-expired key.
    db.put(b"blink", b"gone soon", ttl=0.05)
    time.sleep(0.1)
    remaining = db.ttl(b"blink")
    print(f"  ttl(b'blink') after expiry = {remaining!r}  (0.0 = just expired)")

    # Confirm truly absent key raises NotFoundError.
    try:
        db.ttl(b"never_existed")
    except NotFoundError:
        print("  ttl(b'never_existed') raised NotFoundError  (correct)")


# ---------------------------------------------------------------------------
# 3. Bulk cleanup with purge_expired()
# ---------------------------------------------------------------------------
def section_purge(db: KVStore) -> None:
    print("\n--- 3. purge_expired ---")

    # Insert several short-lived keys and one long-lived key.
    db.put(b"old1", b"v", ttl=0.001)
    db.put(b"old2", b"v", ttl=0.001)
    db.put(b"old3", b"v", ttl=0.001)
    db.put(b"keep", b"v", ttl=60)

    time.sleep(0.05)   # let the three expire

    deleted = db.purge_expired()
    print(f"  purge_expired() removed {deleted} key(s)")

    print(f"  get(b'keep') = {db.get(b'keep')!r}  (survived)")


# ---------------------------------------------------------------------------
# 4. CF-level TTL on a named column family
# ---------------------------------------------------------------------------
def section_named_cf_ttl(db: KVStore) -> None:
    print("\n--- 4. CF-level TTL (named CF) ---")

    with db.create_column_family("rate_limits") as cf:
        # Token expires in 1 second.
        cf.put(b"user:42", b"5", ttl=1)
        print(f"  cf.put(b'user:42', ttl=1 s)")

        remaining = cf.ttl(b"user:42")
        print(f"  cf.ttl(b'user:42') = {remaining:.3f} s")

        # Extend the TTL by overwriting with a longer expiry.
        cf.put(b"user:42", b"5", ttl=30)
        print(f"  after extending TTL: cf.ttl = {cf.ttl(b'user:42'):.1f} s")

        # Make it permanent (ttl=None removes expiry).
        cf.put(b"user:42", b"5")   # plain put, no ttl
        print(f"  after plain put: cf.ttl = {cf.ttl(b'user:42')!r}  (None = permanent)")

        # Insert an already-expired key and purge.
        cf.put(b"user:99", b"0", ttl=0.001)
        time.sleep(0.05)
        n = cf.purge_expired()
        print(f"  cf.purge_expired() removed {n} key(s)")

        # Named CF TTL is isolated from the default CF.
        default_n = db.purge_expired()
        print(f"  default CF purge_expired() = {default_n} (independent)")


# ---------------------------------------------------------------------------
# 5. TTL inside an explicit transaction
# ---------------------------------------------------------------------------
def section_ttl_in_transaction(db: KVStore) -> None:
    print("\n--- 5. TTL inside explicit transaction ---")

    # Write two TTL keys, then rollback — both should vanish.
    db.begin(write=True)
    db.put(b"tx_k1", b"val1", ttl=10)
    db.put(b"tx_k2", b"val2", ttl=10)
    db.rollback()

    print(f"  get(b'tx_k1') after rollback = {db.get(b'tx_k1')!r}  (None = rolled back)")

    # Commit path.
    db.begin(write=True)
    db.put(b"tx_k1", b"val1", ttl=10)
    db.commit()

    remaining = db.ttl(b"tx_k1")
    print(f"  ttl(b'tx_k1') after commit = {remaining:.1f} s")


# ---------------------------------------------------------------------------
# 6. Real-world: fixed-window rate limiter
# ---------------------------------------------------------------------------
class RateLimiter:
    """
    Fixed-window rate limiter backed by SNKV TTL.

    Each user gets a counter that expires at the end of the window.
    If a key is absent (expired or never set), the window resets.
    """

    def __init__(self, db: KVStore, limit: int, window_s: float) -> None:
        self._db     = db
        self._limit  = limit
        self._window = window_s
        try:
            self._cf = db.open_column_family("rl")
        except NotFoundError:
            self._cf = db.create_column_family("rl")

    def is_allowed(self, user_id: str) -> bool:
        key = user_id.encode()
        raw = self._cf.get(key)

        if raw is None:
            # First request in this window.
            self._cf.put(key, b"1", ttl=self._window)
            return True

        count = int(raw)
        if count >= self._limit:
            return False

        # Increment within current window — preserve remaining TTL.
        remaining_s = self._cf.ttl(key)
        ttl = remaining_s if remaining_s is not None else self._window
        self._cf.put(key, str(count + 1).encode(), ttl=ttl)
        return True

    def close(self) -> None:
        self._cf.close()


def section_rate_limiter(db: KVStore) -> None:
    print("\n--- 6. Real-world: rate limiter (5 req / 2 s window) ---")

    rl = RateLimiter(db, limit=5, window_s=2)

    for i in range(7):
        allowed = rl.is_allowed("alice")
        print(f"  request {i+1}: {'ALLOWED' if allowed else 'BLOCKED'}")

    print("  (waiting for window to reset...)")
    time.sleep(2.1)

    allowed = rl.is_allowed("alice")
    print(f"  request after reset: {'ALLOWED' if allowed else 'BLOCKED'}")

    rl.close()


# ---------------------------------------------------------------------------
def main() -> None:
    with KVStore(DB_FILE) as db:
        print("=== SNKV TTL Example ===")

        section_basic_ttl(db)
        section_lazy_expiry(db)
        section_purge(db)
        section_named_cf_ttl(db)
        section_ttl_in_transaction(db)
        section_rate_limiter(db)


if __name__ == "__main__":
    main()

    for ext in ("", "-wal", "-shm"):
        try:
            os.remove(DB_FILE + ext)
        except FileNotFoundError:
            pass

    print("\n[OK] ttl.py example complete.")
