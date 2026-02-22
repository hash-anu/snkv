# SPDX-License-Identifier: Apache-2.0
"""
Session KVStore Example
Demonstrates: Real-world web session management with create/get/cleanup

This example simulates how SNKV could back a web session store.
Sessions are stored as JSON blobs keyed by session ID.
A "sessions:" column family keeps sessions separate from other data.

Run:
    python examples/session_store.py
"""

import json
import os
import time
import uuid
from snkv import KVStore, NotFoundError

DB_FILE = "session_store_example.db"

# Prefix for session keys: allows efficient prefix scans for cleanup
SESSION_PREFIX = b"sess:"
# Time-to-live in seconds (for demo, 5 seconds)
SESSION_TTL = 5


class SessionStore:
    """Minimal web session store backed by SNKV."""

    def __init__(self, db_path: str) -> None:
        self._db = KVStore(db_path, wal_size_limit=100)
        # Use a dedicated column family to isolate sessions
        try:
            self._cf = self._db.open_column_family("sessions")
        except Exception:
            self._cf = self._db.create_column_family("sessions")

    def create(self, user_id: str, **data) -> str:
        """Create a new session. Returns the session ID."""
        session_id = str(uuid.uuid4())
        payload = json.dumps({
            "user_id":   user_id,
            "created_at": time.time(),
            "data":      data,
        })
        key = SESSION_PREFIX + session_id.encode()
        self._cf.put(key, payload.encode())
        return session_id

    def get(self, session_id: str) -> dict | None:
        """Return session data, or None if expired/missing."""
        key = SESSION_PREFIX + session_id.encode()
        raw = self._cf.get(key)
        if raw is None:
            return None
        payload = json.loads(raw)
        # Expire old sessions
        if time.time() - payload["created_at"] > SESSION_TTL:
            self._cf.delete(key)
            return None
        return payload

    def delete(self, session_id: str) -> None:
        """Explicitly invalidate a session (logout)."""
        key = SESSION_PREFIX + session_id.encode()
        try:
            self._cf.delete(key)
        except NotFoundError:
            pass

    def cleanup_expired(self) -> int:
        """Remove all expired sessions. Returns count of sessions removed."""
        now = time.time()
        to_delete = []
        for key, value in self._cf.prefix_iterator(SESSION_PREFIX):
            try:
                payload = json.loads(value)
                if now - payload["created_at"] > SESSION_TTL:
                    to_delete.append(key)
            except (json.JSONDecodeError, KeyError):
                to_delete.append(key)  # corrupt entry - clean up

        for key in to_delete:
            try:
                self._cf.delete(key)
            except NotFoundError:
                pass
        return len(to_delete)

    def active_count(self) -> int:
        """Return number of currently stored sessions (includes expired)."""
        return sum(1 for _ in self._cf.prefix_iterator(SESSION_PREFIX))

    def close(self) -> None:
        self._cf.close()
        self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def demo():
    with SessionStore(DB_FILE) as store:
        # --- Create sessions ---
        print("--- Creating Sessions ---")
        sid1 = store.create("alice", role="admin",  ip="10.0.0.1")
        sid2 = store.create("bob",   role="viewer", ip="10.0.0.2")
        sid3 = store.create("carol", role="editor", ip="10.0.0.3")
        print(f"  Created session for alice:  {sid1[:8]}...")
        print(f"  Created session for bob:    {sid2[:8]}...")
        print(f"  Created session for carol:  {sid3[:8]}...")
        print(f"  Active sessions: {store.active_count()}")

        # --- Retrieve sessions ---
        print("\n--- Retrieving Sessions ---")
        s = store.get(sid1)
        print(f"  alice session: user_id={s['user_id']}, role={s['data']['role']}")

        s = store.get(sid2)
        print(f"  bob session:   user_id={s['user_id']}, role={s['data']['role']}")

        # Non-existent session
        missing = store.get("00000000-0000-0000-0000-000000000000")
        print(f"  missing session: {missing}")

        # --- Logout (explicit delete) ---
        print("\n--- Logout (delete session) ---")
        store.delete(sid3)
        s = store.get(sid3)
        print(f"  carol session after logout: {s}")
        print(f"  Active sessions: {store.active_count()}")

        # --- Expiry cleanup ---
        print(f"\n--- Waiting {SESSION_TTL}s for sessions to expire ---")
        time.sleep(SESSION_TTL + 0.1)

        s = store.get(sid1)
        print(f"  alice session after TTL: {s}  (expired)")

        removed = store.cleanup_expired()
        print(f"  Cleanup removed {removed} expired session(s)")
        print(f"  Active sessions after cleanup: {store.active_count()}")

        # --- Stats ---
        print("\n--- Stats ---")
        st = store._db.stats()
        print(f"  puts={st['puts']}, gets={st['gets']}, deletes={st['deletes']}")


if __name__ == "__main__":
    demo()

    # Cleanup
    for ext in ("", "-wal", "-shm"):
        try:
            os.remove(DB_FILE + ext)
        except FileNotFoundError:
            pass
    print("\n[OK] session_store.py complete")
