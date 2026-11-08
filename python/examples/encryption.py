# SPDX-License-Identifier: Apache-2.0
"""
Encryption Example
Demonstrates: creating encrypted stores, password management, and the full
lifecycle: encrypted → plaintext → re-encrypted.

Run:
    python examples/encryption.py

Sections:
    1. Create an encrypted store and basic put/get
    2. Re-open with correct and wrong passwords
    3. Plaintext not present in raw file
    4. reencrypt — rotate to a new password
    5. remove_encryption — convert to plaintext
    6. open_encrypted on a plain store — re-encrypts transparently
    7. Full lifecycle: encrypted → plain → re-encrypted
    8. Real-world: encrypted session store
"""

import os

from snkv import KVStore, AuthError

DB_FILE = "encryption_example.db"


def _cleanup(path: str) -> None:
    for ext in ("", "-wal", "-shm"):
        try:
            os.remove(path + ext)
        except FileNotFoundError:
            pass


# ---------------------------------------------------------------------------
# 1. Create an encrypted store — basic put / get
# ---------------------------------------------------------------------------
def section_basic(db_path: str) -> None:
    print("\n--- 1. Create encrypted store ---")

    with KVStore.open_encrypted(db_path, b"secret") as db:
        print(f"  is_encrypted() = {db.is_encrypted()}")

        db.put(b"user:1", b"alice")
        db.put(b"user:2", b"bob")

        print(f"  get(b'user:1') = {db.get(b'user:1')}")
        print(f"  get(b'user:2') = {db.get(b'user:2')}")
        print(f"  get(b'missing') = {db.get(b'missing')!r}")


# ---------------------------------------------------------------------------
# 2. Re-open with correct and wrong passwords
# ---------------------------------------------------------------------------
def section_reopen(db_path: str) -> None:
    print("\n--- 2. Re-open with correct / wrong password ---")

    # Correct password works.
    with KVStore.open_encrypted(db_path, b"secret") as db:
        val = db.get(b"user:1")
        print(f"  correct password → get(b'user:1') = {val}")

    # Wrong password raises AuthError.
    try:
        KVStore.open_encrypted(db_path, b"wrong")
        print("  ERROR: expected AuthError")
    except AuthError:
        print("  wrong password → AuthError raised  (correct)")


# ---------------------------------------------------------------------------
# 3. Plaintext not visible in the raw file
# ---------------------------------------------------------------------------
def section_ciphertext(db_path: str) -> None:
    print("\n--- 3. Plaintext not in raw file ---")

    secret_value = b"supersecret_payload"
    with KVStore.open_encrypted(db_path, b"secret") as db:
        db.put(b"payload", secret_value)

    with open(db_path, "rb") as f:
        raw = f.read()

    found = secret_value in raw
    print(f"  secret bytes found in file: {found}  (expected: False)")


# ---------------------------------------------------------------------------
# 4. reencrypt — rotate to a new password
# ---------------------------------------------------------------------------
def section_reencrypt(db_path: str) -> None:
    print("\n--- 4. reencrypt — password rotation ---")

    with KVStore.open_encrypted(db_path, b"secret") as db:
        db.put(b"important", b"data")
        db.reencrypt(b"newsecret")
        print("  reencrypt(b'newsecret') done")

    # Old password must fail.
    try:
        KVStore.open_encrypted(db_path, b"secret")
        print("  ERROR: expected AuthError for old password")
    except AuthError:
        print("  old password → AuthError  (correct)")

    # New password works; data intact.
    with KVStore.open_encrypted(db_path, b"newsecret") as db:
        val = db.get(b"important")
        print(f"  new password → get(b'important') = {val}")


# ---------------------------------------------------------------------------
# 5. remove_encryption — convert to plaintext
# ---------------------------------------------------------------------------
def section_remove_encryption(db_path: str) -> None:
    print("\n--- 5. remove_encryption ---")

    with KVStore.open_encrypted(db_path, b"newsecret") as db:
        db.put(b"plaintext_key", b"plaintext_value")
        db.remove_encryption()
        print("  remove_encryption() done")

    # Now opens as a plain store — no password needed.
    with KVStore(db_path) as db:
        print(f"  is_encrypted() = {db.is_encrypted()}")
        val = db.get(b"plaintext_key")
        print(f"  get(b'plaintext_key') = {val}")


# ---------------------------------------------------------------------------
# 6. open_encrypted on a plain store — encrypts transparently
# ---------------------------------------------------------------------------
def section_reencrypt_plain(db_path: str) -> None:
    print("\n--- 6. open_encrypted on plain store → encrypts ---")

    # At this point the store is plaintext (from section 5).
    with KVStore.open_encrypted(db_path, b"renewed") as db:
        print(f"  is_encrypted() after open_encrypted on plain = {db.is_encrypted()}")
        val = db.get(b"plaintext_key")
        print(f"  get(b'plaintext_key') = {val}  (data preserved)")

    # Old plain-open still sees ciphertext blobs but cannot decrypt.
    with KVStore(db_path) as db:
        raw = db.get(b"plaintext_key")
        readable = raw == b"plaintext_value"
        print(f"  plain open readable without password: {readable}  (expected: False)")

    # Correct password reopens cleanly.
    with KVStore.open_encrypted(db_path, b"renewed") as db:
        val = db.get(b"plaintext_key")
        print(f"  correct password → get(b'plaintext_key') = {val}")


# ---------------------------------------------------------------------------
# 7. Full lifecycle in one sequence
# ---------------------------------------------------------------------------
def section_full_lifecycle(db_path: str) -> None:
    print("\n--- 7. Full lifecycle: encrypted → plain → re-encrypted ---")

    pw1 = b"phase1"
    pw2 = b"phase3"

    # Phase 1: encrypted.
    with KVStore.open_encrypted(db_path, pw1) as db:
        db.put(b"persist", b"survives_all_phases")
        db.put(b"p1:key", b"p1:val")
        print(f"  phase1 encrypted — is_encrypted() = {db.is_encrypted()}")

    # Transition 1→2: remove encryption.
    with KVStore.open_encrypted(db_path, pw1) as db:
        db.remove_encryption()
        print("  transition 1→2: remove_encryption()")

    # Phase 2: plaintext.
    with KVStore(db_path) as db:
        db.put(b"p2:key", b"p2:val")
        print(f"  phase2 plaintext — is_encrypted() = {db.is_encrypted()}")
        print(f"  p1:key survives: {db.get(b'p1:key')}")
        print(f"  persist survives: {db.get(b'persist')}")

    # Transition 2→3: re-encrypt plain store.
    with KVStore.open_encrypted(db_path, pw2) as db:
        print(f"  transition 2→3: open_encrypted re-encrypts — is_encrypted() = {db.is_encrypted()}")
        db.put(b"p3:key", b"p3:val")
        print(f"  p1:key: {db.get(b'p1:key')}")
        print(f"  p2:key: {db.get(b'p2:key')}")
        print(f"  persist: {db.get(b'persist')}")

    # Phase 1 password no longer works.
    try:
        KVStore.open_encrypted(db_path, pw1)
        print("  ERROR: expected AuthError for phase1 password")
    except AuthError:
        print(f"  phase1 password rejected  (correct)")

    # Phase 3 password opens with all data.
    with KVStore.open_encrypted(db_path, pw2) as db:
        print(f"  phase3 password accepted — all keys: "
              f"p1={db.get(b'p1:key')!r} p2={db.get(b'p2:key')!r} "
              f"p3={db.get(b'p3:key')!r} persist={db.get(b'persist')!r}")


# ---------------------------------------------------------------------------
# 8. Real-world: encrypted session store
# ---------------------------------------------------------------------------
class SessionStore:
    """
    Simple encrypted session store.

    Each session is stored as key=session_id, value=user_data.
    The store is opened once with a master key and used for all sessions.
    """

    def __init__(self, path: str, master_key: bytes) -> None:
        self._db = KVStore.open_encrypted(path, master_key)

    def create(self, session_id: str, user_data: bytes, ttl: float = 3600) -> None:
        self._db.put(session_id.encode(), user_data, ttl=ttl)

    def get(self, session_id: str) -> bytes | None:
        return self._db.get(session_id.encode())

    def revoke(self, session_id: str) -> None:
        try:
            self._db.delete(session_id.encode())
        except KeyError:
            pass

    def purge_expired(self) -> int:
        return self._db.purge_expired()

    def close(self) -> None:
        self._db.close()


def section_session_store(db_path: str) -> None:
    print("\n--- 8. Real-world: encrypted session store ---")

    store = SessionStore(db_path, b"master_key_32bytes_padded_here!!")

    store.create("sess:alice", b'{"user":"alice","role":"admin"}', ttl=3600)
    store.create("sess:bob",   b'{"user":"bob","role":"viewer"}',  ttl=1800)

    print(f"  alice session: {store.get('sess:alice')}")
    print(f"  bob session:   {store.get('sess:bob')}")

    store.revoke("sess:bob")
    print(f"  after revoke — bob session: {store.get('sess:bob')!r}")

    n = store.purge_expired()
    print(f"  purge_expired() removed {n} expired session(s)")

    store.close()


# ---------------------------------------------------------------------------
def main() -> None:
    print("=== SNKV Encryption Example ===")

    _cleanup(DB_FILE)

    section_basic(DB_FILE)
    section_reopen(DB_FILE)
    section_ciphertext(DB_FILE)
    section_reencrypt(DB_FILE)
    section_remove_encryption(DB_FILE)
    section_reencrypt_plain(DB_FILE)
    _cleanup(DB_FILE)

    section_full_lifecycle(DB_FILE)
    _cleanup(DB_FILE)

    section_session_store(DB_FILE)
    _cleanup(DB_FILE)


if __name__ == "__main__":
    main()
    print("\n[OK] encryption.py example complete.")
