/* SPDX-License-Identifier: Apache-2.0 */
/*
** Encryption Example
**
** Demonstrates:
**   - kvstore_open_encrypted: create / open an encrypted store
**   - kvstore_is_encrypted: inspect encryption status
**   - Transparent put/get/delete/iterate on an encrypted store
**   - Wrong-password rejection (KVSTORE_AUTH_FAILED)
**   - kvstore_reencrypt: change the password in-place
**   - kvstore_remove_encryption: decrypt the store permanently
**   - Multiple column families with encryption
**   - TTL + encryption together
*/

#define SNKV_IMPLEMENTATION
#include "snkv.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#define DB_PATH  "examples/enc_demo.db"
#define PASSWORD "hunter2"
#define NEW_PASS "correct-horse-battery-staple"

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

static void print_separator(const char *title) {
    printf("\n--- %s ---\n", title);
}

static void print_get(KVStore *pKV, const char *key) {
    void *val = NULL; int nVal = 0;
    int rc = kvstore_get(pKV, key, (int)strlen(key), &val, &nVal);
    if (rc == KVSTORE_OK) {
        printf("  get(\"%s\") = \"%.*s\"\n", key, nVal, (char *)val);
        snkv_free(val);
    } else if (rc == KVSTORE_NOTFOUND) {
        printf("  get(\"%s\") = <not found>\n", key);
    } else {
        printf("  get(\"%s\") = ERROR %d\n", key, rc);
    }
}

/* ------------------------------------------------------------------ */
/* Section 1: Create an encrypted store and do basic operations.      */
/* ------------------------------------------------------------------ */
static void section_create(void) {
    print_separator("1. Create encrypted store");

    KVStore *pKV = NULL;
    int rc = kvstore_open_encrypted(DB_PATH, PASSWORD, (int)strlen(PASSWORD),
                                    &pKV, NULL);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "  kvstore_open_encrypted failed: %d\n", rc);
        return;
    }
    printf("  opened encrypted store (is_encrypted=%d)\n",
           kvstore_is_encrypted(pKV));

    /* All standard put/get/delete APIs work transparently. */
    kvstore_put(pKV, "username", 8, "alice",        5);
    kvstore_put(pKV, "email",    5, "alice@example.com", 17);
    kvstore_put(pKV, "role",     4, "admin",        5);

    print_get(pKV, "username");
    print_get(pKV, "email");
    print_get(pKV, "role");

    kvstore_close(pKV);
    printf("  store closed\n");
}

/* ------------------------------------------------------------------ */
/* Section 2: Re-open the store with the correct password.            */
/* ------------------------------------------------------------------ */
static void section_reopen_correct(void) {
    print_separator("2. Re-open with correct password");

    KVStore *pKV = NULL;
    int rc = kvstore_open_encrypted(DB_PATH, PASSWORD, (int)strlen(PASSWORD),
                                    &pKV, NULL);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "  re-open failed: %d\n", rc);
        return;
    }
    printf("  re-opened successfully (is_encrypted=%d)\n",
           kvstore_is_encrypted(pKV));

    print_get(pKV, "username");
    print_get(pKV, "email");

    kvstore_close(pKV);
}

/* ------------------------------------------------------------------ */
/* Section 3: Wrong password is rejected.                             */
/* ------------------------------------------------------------------ */
static void section_wrong_password(void) {
    print_separator("3. Wrong password rejected");

    KVStore *pKV = NULL;
    int rc = kvstore_open_encrypted(DB_PATH, "wrongpassword", 13, &pKV, NULL);
    if (rc == KVSTORE_AUTH_FAILED) {
        printf("  kvstore_open_encrypted(wrong pw) => KVSTORE_AUTH_FAILED (expected)\n");
    } else {
        printf("  unexpected rc=%d\n", rc);
    }
    /* pKV is guaranteed NULL on failure — no resource leak. */
}

/* ------------------------------------------------------------------ */
/* Section 4: Iterators work transparently on encrypted stores.       */
/* ------------------------------------------------------------------ */
static void section_iteration(void) {
    print_separator("4. Iteration over encrypted store");

    KVStore *pKV = NULL;
    kvstore_open_encrypted(DB_PATH, PASSWORD, (int)strlen(PASSWORD),
                           &pKV, NULL);

    /* Add a few more keys first. */
    kvstore_put(pKV, "city",    4, "Wonderland", 10);
    kvstore_put(pKV, "country", 7, "Neverland",   9);

    KVIterator *it = NULL;
    kvstore_iterator_create(pKV, &it);
    kvstore_iterator_first(it);

    printf("  all keys in store:\n");
    while (!kvstore_iterator_eof(it)) {
        void *k = NULL, *v = NULL;
        int nk = 0, nv = 0;
        kvstore_iterator_key(it, &k, &nk);
        kvstore_iterator_value(it, &v, &nv);
        printf("    \"%.*s\" => \"%.*s\"\n", nk, (char *)k, nv, (char *)v);
        kvstore_iterator_next(it);
    }
    kvstore_iterator_close(it);
    kvstore_close(pKV);
}

/* ------------------------------------------------------------------ */
/* Section 5: Encryption + column families.                           */
/* ------------------------------------------------------------------ */
static void section_column_families(void) {
    print_separator("5. Column families on encrypted store");

    KVStore *pKV = NULL;
    kvstore_open_encrypted(DB_PATH, PASSWORD, (int)strlen(PASSWORD),
                           &pKV, NULL);

    KVColumnFamily *pSessions = NULL;
    kvstore_cf_create(pKV, "sessions", &pSessions);

    kvstore_cf_put(pSessions, "sess:abc", 8, "{\"user\":\"alice\"}", 16);
    kvstore_cf_put(pSessions, "sess:def", 8, "{\"user\":\"bob\"}",   14);

    void *val = NULL; int nVal = 0;
    kvstore_cf_get(pSessions, "sess:abc", 8, &val, &nVal);
    printf("  cf_get(sessions, \"sess:abc\") = \"%.*s\"\n", nVal, (char *)val);
    snkv_free(val);

    kvstore_cf_close(pSessions);
    kvstore_close(pKV);
}

/* ------------------------------------------------------------------ */
/* Section 6: TTL and encryption work together.                       */
/* ------------------------------------------------------------------ */
static void section_ttl(void) {
    print_separator("6. TTL + encryption");

    KVStore *pKV = NULL;
    kvstore_open_encrypted(DB_PATH, PASSWORD, (int)strlen(PASSWORD),
                           &pKV, NULL);

    /* Store a session token that expires in 10 seconds. */
    int64_t expire_ms = kvstore_now_ms() + 10000;
    int rc = kvstore_put_ttl(pKV, "session_token", 13,
                             "tok-1a2b3c4d", 12, expire_ms);

    void *val = NULL; int nVal = 0; int64_t rem = 0;
    rc = kvstore_get_ttl(pKV, "session_token", 13, &val, &nVal, &rem);
    if (rc == KVSTORE_OK) {
        printf("  session_token = \"%.*s\", expires in %lld ms\n",
               nVal, (char *)val, (long long)rem);
        snkv_free(val);
    }

    kvstore_close(pKV);
}

/* ------------------------------------------------------------------ */
/* Section 7: Change the password (reencrypt).                        */
/* ------------------------------------------------------------------ */
static void section_reencrypt(void) {
    print_separator("7. Change password (reencrypt)");

    KVStore *pKV = NULL;
    kvstore_open_encrypted(DB_PATH, PASSWORD, (int)strlen(PASSWORD),
                           &pKV, NULL);

    int rc = kvstore_reencrypt(pKV, NEW_PASS, (int)strlen(NEW_PASS));
    printf("  kvstore_reencrypt: %s\n", rc == KVSTORE_OK ? "OK" : "FAIL");
    kvstore_close(pKV);

    /* Old password no longer works. */
    KVStore *pKV2 = NULL;
    rc = kvstore_open_encrypted(DB_PATH, PASSWORD, (int)strlen(PASSWORD),
                                &pKV2, NULL);
    printf("  open with old password: %s\n",
           rc == KVSTORE_AUTH_FAILED ? "AUTH_FAILED (expected)" : "unexpected");

    /* New password works; data is intact. */
    kvstore_open_encrypted(DB_PATH, NEW_PASS, (int)strlen(NEW_PASS),
                           &pKV2, NULL);
    printf("  open with new password: %s\n",
           kvstore_is_encrypted(pKV2) ? "OK (still encrypted)" : "FAIL");
    print_get(pKV2, "username");
    kvstore_close(pKV2);
}

/* ------------------------------------------------------------------ */
/* Section 8: Remove encryption — store becomes plaintext.            */
/* ------------------------------------------------------------------ */
static void section_remove_encryption(void) {
    print_separator("8. Remove encryption");

    KVStore *pKV = NULL;
    kvstore_open_encrypted(DB_PATH, NEW_PASS, (int)strlen(NEW_PASS),
                           &pKV, NULL);

    int rc = kvstore_remove_encryption(pKV);
    printf("  kvstore_remove_encryption: %s\n", rc == KVSTORE_OK ? "OK" : "FAIL");
    kvstore_close(pKV);

    /* Now opens as a plain store with no password. */
    KVStore *pPlain = NULL;
    kvstore_open(DB_PATH, &pPlain, KVSTORE_JOURNAL_WAL);
    printf("  plain open after remove_encryption: is_encrypted=%d\n",
           kvstore_is_encrypted(pPlain));
    print_get(pPlain, "username");
    print_get(pPlain, "email");
    kvstore_close(pPlain);
}

/* ------------------------------------------------------------------ */

int main(void) {
    printf("=== SNKV Encryption Demo ===\n");

    /* Start fresh. */
    remove(DB_PATH);
    remove(DB_PATH "-wal");
    remove(DB_PATH "-shm");

    section_create();
    section_reopen_correct();
    section_wrong_password();
    section_iteration();
    section_column_families();
    section_ttl();
    section_reencrypt();
    section_remove_encryption();

    /* Clean up demo files. */
    remove(DB_PATH);
    remove(DB_PATH "-wal");
    remove(DB_PATH "-shm");

    printf("\nDone.\n");
    return 0;
}
