# Proposal: Password-Based Encryption for SNKV

**Status:** Draft
**Author:** SNKV Contributors
**Target version:** 0.7.0

---

## 1. Motivation

SNKV stores data in a plain SQLite B-tree file. Anyone who has the `.db` file can read every key and value with a hex editor or any SQLite tool. This is unacceptable for:

- Mobile / desktop apps storing user credentials, tokens, or personal data
- Server-side stores containing PII or secrets that must be encrypted at rest
- Compliance requirements (GDPR, HIPAA, SOC 2) that mandate encryption at rest
- Shared hosting where the filesystem is accessible to other tenants

**Goal:** Add transparent, authenticated value encryption so that without the correct password, all stored values are indistinguishable from random bytes — while preserving every existing kvstore API.

---

## 2. Threat Model

| Protected against | Not protected against |
|---|---|
| Attacker with read access to the `.db` file (values) | Attacker with the correct password |
| Offline brute-force on a weak password (Argon2 slows this) | Attacker with access to the running process (memory) |
| Ciphertext tampering (AEAD tag detects modification) | Side-channel attacks on CPU/memory |
| Value length inference (nonce+tag add fixed 40 bytes) | Key names (stored in plaintext — see §3 decision #4) |

**Key names are stored in plaintext.** This is a deliberate trade-off to preserve all iterator, prefix, seek, and range functionality. Values — the actual sensitive data — are fully encrypted.

This matches the model used by RocksDB encryption-at-rest and most production embedded encrypted stores.

---

## 3. Design Decisions

| # | Decision | Choice | Rationale |
|---|---|---|---|
| 1 | Crypto library | Monocypher (vendored) | Single C file, no dependencies, CC0 license, independently audited |
| 2 | KDF | Argon2id | Memory-hard; OWASP and RFC 9106 recommended |
| 3 | Cipher | XChaCha20-Poly1305 (AEAD) | 192-bit nonce eliminates nonce-reuse risk; fast in software; no padding |
| 4 | Key name encryption | **No** — keys stored in plaintext | Preserves all iterator/prefix/seek/range/CF functionality unchanged |
| 5 | Nonce | 24 random bytes per record, prepended to ciphertext | 2^192 nonce space makes collision negligible across all records |
| 6 | Password verification | Encrypt known plaintext `"SNKV_AUTH_V1"` in `__snkv_auth__` CF | Fails fast on wrong password before any data access |
| 7 | In-memory key | Derived once at open; held in `KVStore.aEncKey[32]` | Never written to disk; zeroed on close |
| 8 | All kvstore APIs supported | Yes — no feature restrictions on encrypted stores | Value encrypt/decrypt is transparent at the B-tree put/get layer |

---

## 4. Dependencies

### 4.1 Monocypher (vendored, pure C)

Monocypher provides:
- `crypto_aead_lock` / `crypto_aead_unlock` — XChaCha20-Poly1305 encrypt/decrypt
- `crypto_argon2` — Argon2id key derivation
- `crypto_wipe` — secure memory zeroing

Vendor into `src/monocypher/`:

```
src/monocypher/
  monocypher.h    (~500 lines)
  monocypher.c    (~2500 lines)
```

Source: https://monocypher.org
License: **CC0** (public domain) — no attribution required.
Version: 4.0.2 (latest stable).

### 4.2 No other dependencies

No OpenSSL, libsodium, or mbedTLS. Monocypher is self-contained pure C.

---

## 5. Cryptographic Construction

### 5.1 Key derivation (once at open)

```
salt[16]     = random bytes, stored in __snkv_auth__["salt"]
Argon2id params (stored in __snkv_auth__["params"]):
    m_cost   = 65536   (64 MB memory)
    t_cost   = 3       (3 passes)
    lanes    = 1

enc_key[32]  = Argon2id(password, salt, m_cost, t_cost, lanes, output=32)
```

`enc_key` is held in `KVStore.aEncKey[32]` in memory only. Zeroed via `crypto_wipe` in `kvstore_close`.

### 5.2 Value encryption (per record)

Every value is stored in this layout:

```
┌─────────────────┬────────────────────┬──────────────────┐
│  nonce (24 B)   │  ciphertext (N B)  │  Poly1305 tag (16 B) │
└─────────────────┴────────────────────┴──────────────────┘
```

**Encrypt:**
```c
uint8_t nonce[24];
randombytes(nonce, 24);   // OS CSPRNG: getrandom() on Linux, BCryptGenRandom() on Windows

crypto_aead_lock(
    ciphertext,           // output
    tag,                  // 16-byte output
    enc_key,              // 32-byte key
    nonce,                // 24-byte nonce
    NULL, 0,              // no additional data
    plaintext, n          // input
);

stored = nonce || ciphertext || tag   // total: n + 40 bytes
```

**Decrypt:**
```c
uint8_t *nonce      = stored;
uint8_t *ciphertext = stored + 24;
uint8_t *tag        = stored + 24 + n;

int rc = crypto_aead_unlock(
    plaintext,            // output
    tag,                  // 16-byte tag to verify
    enc_key,              // 32-byte key
    nonce,                // 24-byte nonce
    NULL, 0,              // no additional data
    ciphertext, n         // input
);
// rc != 0 → KVSTORE_CORRUPT (tamper detected)
```

### 5.3 Password verification

**On create:**
```
stored = encrypt("SNKV_AUTH_V1")    → nonce || ciphertext || tag
write __snkv_auth__["verify"] = stored
```

**On open:**
```
decrypt(__snkv_auth__["verify"]) → plaintext
if plaintext != "SNKV_AUTH_V1"  → return KVSTORE_AUTH_FAILED
```

### 5.4 Random bytes (cross-platform)

```c
/* kvstore_enc.c */
static void kvstoreEncRandBytes(uint8_t *buf, int n) {
#ifdef _WIN32
    BCryptGenRandom(NULL, buf, n, BCRYPT_USE_SYSTEM_PREFERRED_RNG);
#else
    getrandom(buf, n, 0);   /* Linux 3.17+ */
    /* fallback: read /dev/urandom if getrandom unavailable */
#endif
}
```

---

## 6. C API

### 6.1 New return codes (added to `kvstore.h`)

```c
#define KVSTORE_AUTH_FAILED   5   /* wrong password */
#define KVSTORE_CORRUPT       6   /* AEAD tag mismatch — data tampered or wrong key */
```

### 6.2 Open encrypted store

```c
/*
** Open or create a password-encrypted key-value store.
**
**   zFilename  — path to .db file (NULL for in-memory)
**   pPassword  — password bytes (any encoding; not NUL-terminated)
**   nPassword  — length in bytes
**   ppKV       — output handle
**   pConfig    — same as kvstore_open_v2 (NULL = defaults)
**
** On first open (new file): generates salt, derives enc_key, writes
**   __snkv_auth__ metadata and verification tag.
** On subsequent open: re-derives enc_key, verifies tag.
**   Returns KVSTORE_AUTH_FAILED if password is wrong.
**
** All existing kvstore_* functions work unchanged on the returned handle.
** Encryption and decryption are transparent.
** The encryption key is zeroed from memory when kvstore_close() is called.
*/
int kvstore_open_encrypted(const char *zFilename,
                           const void *pPassword, int nPassword,
                           KVStore **ppKV,
                           const KVStoreConfig *pConfig);
```

### 6.3 Utility functions

```c
/*
** Returns 1 if pKV was opened with kvstore_open_encrypted, 0 otherwise.
*/
int kvstore_is_encrypted(KVStore *pKV);

/*
** Change the password of an open encrypted store.
** Derives a new enc_key, re-encrypts all values in a single transaction,
** writes new salt + verification tag. Store remains open after call.
** Returns KVSTORE_ERROR if store is not encrypted.
*/
int kvstore_reencrypt(KVStore *pKV,
                      const void *pNewPassword, int nNewPassword);

/*
** Decrypt all values in-place and remove __snkv_auth__ CF.
** After this call the store is a plain (unencrypted) KVStore.
** pKV remains open and usable as a plain store.
*/
int kvstore_remove_encryption(KVStore *pKV);
```

---

## 7. Internal Implementation

### 7.1 `KVStore` struct additions (2 fields)

```c
/* src/kvstore.c — added to struct KVStore */
uint8_t  aEncKey[32];   /* 256-bit derived key; all-zeros if not encrypted */
int      bEncrypted;    /* 1 if opened with kvstore_open_encrypted */
```

### 7.2 Integration points in `kvstore.c` (2 locations only)

All encryption is isolated to the two lowest-level B-tree helpers:

```
kvstoreRawBtreePut(pKV, table, key, nKey, value, nValue)
    → if pKV->bEncrypted:
          enc_value = kvstoreEncEncrypt(pKV, value, nValue, &enc_len)
          write (key, enc_value) to B-tree
          snkv_free(enc_value)
      else: write (key, value) as-is   ← unchanged path

kvstoreRawBtreeGet(pKV, table, key, nKey, &outValue, &outLen)
    → fetch raw bytes from B-tree
    → if pKV->bEncrypted:
          plain = kvstoreEncDecrypt(pKV, raw, rawLen, &plainLen, &rc)
          if rc == KVSTORE_CORRUPT: return KVSTORE_CORRUPT
          return plain
      else: return raw bytes as-is   ← unchanged path
```

Every API that builds on these two helpers — `kvstore_put`, `kvstore_get`, `kvstore_delete`, `kvstore_cf_put`, `kvstore_cf_get`, `kvstore_iterator_value`, TTL functions, stats, checkpoint, vacuum — automatically gets encryption with **zero changes** to those call sites.

### 7.3 Static helpers in `kvstore_enc.c`

```c
/* Allocates and returns nonce||ciphertext||tag. Caller must snkv_free(). */
static uint8_t *kvstoreEncEncrypt(KVStore *pKV,
                                   const void *pPlain, int nPlain,
                                   int *pnOut);

/* Allocates and returns plaintext. Returns NULL + KVSTORE_CORRUPT on tag failure. */
static uint8_t *kvstoreEncDecrypt(KVStore *pKV,
                                   const void *pEnc, int nEnc,
                                   int *pnOut, int *pRc);
```

### 7.4 `kvstore_open_encrypted` flow

```
1. kvstore_open_v2(zFilename, &pKV, pConfig)   — opens plain file first
2. Look up __snkv_auth__["salt"]:
   EXISTS (open existing encrypted store):
     a. Read salt[16] and params (m_cost, t_cost)
     b. enc_key = Argon2id(password, salt, m_cost, t_cost)
     c. Decrypt __snkv_auth__["verify"]
     d. If plaintext != "SNKV_AUTH_V1":
            crypto_wipe(enc_key, 32)
            kvstore_close(pKV)
            return KVSTORE_AUTH_FAILED
   ABSENT (new store):
     a. randombytes(salt, 16)
     b. enc_key = Argon2id(password, salt, 65536, 3)
     c. Write salt + params to __snkv_auth__
     d. Write encrypt("SNKV_AUTH_V1") to __snkv_auth__["verify"]
3. pKV->bEncrypted = 1
4. memcpy(pKV->aEncKey, enc_key, 32)
5. crypto_wipe(enc_key, 32)   — clear stack copy
6. return KVSTORE_OK
```

### 7.5 `kvstore_close` addition

```c
if( pKV->bEncrypted ){
    crypto_wipe(pKV->aEncKey, 32);
    pKV->bEncrypted = 0;
}
```

---

## 8. Storage Layout

| CF name | Key | Value | Purpose |
|---|---|---|---|
| `__snkv_auth__` | `"salt"` | 16 random bytes (plaintext) | Argon2id salt |
| `__snkv_auth__` | `"params"` | 8 bytes: m_cost(4) + t_cost(4), big-endian | Argon2id parameters |
| `__snkv_auth__` | `"verify"` | `nonce(24) \| enc("SNKV_AUTH_V1") \| tag(16)` | Password verification |
| default CF | user key (plaintext) | `nonce(24) \| ciphertext(N) \| tag(16)` | Encrypted user values |
| any user CF | user key (plaintext) | same layout | Encrypted CF values |
| `__snkv_ttl__*` | user key (plaintext) | `nonce(24) \| enc(expiry_ms) \| tag(16)` | Encrypted TTL timestamps |

Keys are always plaintext. Every value stored through `kvstoreRawBtreePut` is encrypted — this includes TTL timestamps and internal metadata stored as values.

---

## 9. Supported API Matrix

Every existing kvstore API works on an encrypted store with no changes:

| API | Works on encrypted store |
|---|---|
| `kvstore_put` / `kvstore_get` / `kvstore_delete` | Yes |
| `kvstore_exists` / `kvstore_count` / `kvstore_clear` | Yes |
| `kvstore_iterator_*` (forward, reverse) | Yes |
| `kvstore_prefix_iterator_*` | Yes — keys are plaintext |
| `kvstore_iterator_seek` | Yes — keys are plaintext |
| Column family: `kvstore_cf_*` | Yes |
| TTL: `kvstore_put_ttl` / `kvstore_get_ttl` / `kvstore_purge_expired` | Yes |
| `kvstore_put_if_absent` | Yes |
| `kvstore_stats` / `kvstore_stats_reset` | Yes |
| `kvstore_checkpoint` / `kvstore_vacuum` | Yes — WAL / B-tree pages contain ciphertext |
| `kvstore_begin` / `kvstore_commit` / `kvstore_rollback` | Yes |

---

## 11. Argon2id Parameters

| Parameter | Default | Meaning |
|---|---|---|
| `m_cost` | 65536 | 64 MB memory |
| `t_cost` | 3 | 3 passes |
| `lanes` | 1 | Monocypher is single-threaded |
| Output | 32 bytes | 256-bit key |

Key derivation takes ~0.3–1 s on a modern CPU — intentional, slows brute force.

Custom params can be passed via `KVStoreConfig` for mobile/embedded use cases:

```c
cfg.argon2_m_cost = 8192;  /* 8 MB — faster, weaker */
cfg.argon2_t_cost = 1;
```

Minimum enforced: `m_cost >= 8192`, `t_cost >= 1`.

---

## 12. Overhead

| Type | Cost |
|---|---|
| Per-record storage overhead | +40 bytes (24 nonce + 16 tag) |
| Per-record encrypt/decrypt CPU | ~50 ns for 1 KB value (XChaCha20 is fast) |
| Open / key derivation | ~0.3–1 s (Argon2id, once per open) |
| Memory | 32 bytes (`aEncKey`) + ~64 MB during open (Argon2 work area, freed after) |

---

## 13. Thread Safety

Same as `KVStore`. `aEncKey` is read-only after open — concurrent readers (each with their own handle) are safe. No mutex needed for the key bytes.

---

## 14. Build Changes

### 14.1 Makefile

```makefile
monocypher.o: src/monocypher/monocypher.c src/monocypher/monocypher.h
	$(CC) $(CFLAGS) -I src/monocypher -c $< -o $@

kvstore_enc.o: src/kvstore_enc.c include/kvstore.h src/monocypher/monocypher.h
	$(CC) $(CFLAGS) -I include -I src/monocypher -c $< -o $@

libsnkv.a: kvstore.o kvstore_enc.o monocypher.o
	$(AR) rcs $@ $^
```

Pure C — no C++ compiler needed.

---

## 15. File Layout

```
include/
  kvstore.h              (updated — 2 new error codes, 4 new declarations, 2 KVStoreConfig fields)

src/
  kvstore.c              (updated — aEncKey/bEncrypted in struct, 2 integration points, close zeroing)
  kvstore_enc.c          (new — ~280 lines: open_encrypted, reencrypt, remove_encryption,
                                            is_encrypted, encrypt/decrypt helpers, randombytes)
  monocypher/
    monocypher.h         (new — vendored, CC0)
    monocypher.c         (new — vendored, CC0)

tests/
  test_enc.c             (new — 20 C tests)

```

Total new code: ~380 lines C (excluding vendored Monocypher ~3000 lines).

---

## 16. Test Plan

### 16.1 C tests (`tests/test_enc.c`) — 20 tests

| # | Test |
|---|------|
| 1 | Open encrypted + `put` + `get` → correct value |
| 2 | Close + reopen with correct password → value readable |
| 3 | Reopen with wrong password → `KVSTORE_AUTH_FAILED` |
| 4 | Raw B-tree bytes differ from plaintext (not stored in clear) |
| 5 | Two puts of same value → different ciphertext (random nonce) |
| 6 | Flip one ciphertext byte → `KVSTORE_CORRUPT` on get |
| 7 | `kvstore_is_encrypted` → 1 on encrypted, 0 on plain |
| 8 | `kvstore_reencrypt` → new password works, old fails |
| 9 | `kvstore_remove_encryption` → file readable as plain store |
| 10 | Forward iterator → correct (plaintext) keys and decrypted values |
| 11 | Prefix iterator → works (keys plaintext) |
| 12 | `kvstore_iterator_seek` → works |
| 13 | Reverse iterator → works |
| 14 | Column family put/get on encrypted store |
| 15 | TTL: `kvstore_put_ttl` + `kvstore_get_ttl` → correct value + remaining |
| 16 | TTL: expired key returns `KVSTORE_NOTFOUND` |
| 17 | `kvstore_purge_expired` on encrypted store |
| 18 | `kvstore_put_if_absent` on encrypted store |
| 19 | `kvstore_begin` / `kvstore_commit` with encrypted puts (atomic) |
| 20 | `aEncKey` all-zeros after `kvstore_close` |

## 17. Example

See [examples/encryption.c](../../examples/encryption.c) for a runnable encrypted session store demo (create, reopen, prefix iteration, wrong-password handling).

---

## 18. Implementation Phases

| Phase | Scope | Version |
|---|---|---|
| 1 | Vendor Monocypher; `kvstore_enc.c`: `open_encrypted`, `is_encrypted`, `close` zeroing; encrypt/decrypt at `kvstoreRawBtreePut`/`kvstoreRawBtreeGet` | 0.7.0 |
| 2 | `kvstore_reencrypt`, `kvstore_remove_encryption` | 0.7.0 |
| 3 | Custom Argon2 params via `KVStoreConfig` | 0.8.0 |
