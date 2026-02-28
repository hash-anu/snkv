/* SPDX-License-Identifier: Apache-2.0 */
/*
** TTL (Time-To-Live) Example
**
** Demonstrates:
**   - kvstore_put_ttl / kvstore_get_ttl on the default CF
**   - kvstore_ttl_remaining to inspect remaining lifetime
**   - kvstore_purge_expired for bulk cleanup
**   - kvstore_cf_put_ttl / kvstore_cf_get_ttl on a named CF
**   - Overwriting a key's TTL
**   - Lazy expiry on get
*/

#define SNKV_IMPLEMENTATION
#include "snkv.h"
#include <stdio.h>
#include <string.h>
#include <time.h>

/* Portable millisecond sleep. */
static void sleep_ms(int ms) {
    struct timespec ts = { ms / 1000, (ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

/* Print remaining TTL for a key in the default CF. */
static void print_ttl(KVStore *pKV, const char *key) {
    int64_t remaining = 0;
    int rc = kvstore_ttl_remaining(pKV, key, (int)strlen(key), &remaining);
    if (rc == KVSTORE_NOTFOUND) {
        printf("  ttl(%s) = <not found>\n", key);
    } else if (rc == KVSTORE_OK && remaining == KVSTORE_NO_TTL) {
        printf("  ttl(%s) = no expiry\n", key);
    } else if (rc == KVSTORE_OK) {
        printf("  ttl(%s) = %lld ms remaining\n", key, (long long)remaining);
    }
}

/* ------------------------------------------------------------------ */
/* Section 1: Basic put / get with TTL on the default column family.  */
/* ------------------------------------------------------------------ */
static void section_basic_ttl(KVStore *pKV) {
    printf("\n--- 1. Basic TTL on default CF ---\n");

    /* Write a key that lives for 5 seconds. */
    int64_t expire_ms = kvstore_now_ms() + 5000;
    int rc = kvstore_put_ttl(pKV, "token", 5, "abc-xyz-789", 11, expire_ms);
    printf("  put_ttl(\"token\", 5 s): %s\n", rc == KVSTORE_OK ? "OK" : "FAIL");

    /* Read it back — should succeed and return remaining > 0. */
    void *val = NULL; int nVal = 0; int64_t rem = 0;
    rc = kvstore_get_ttl(pKV, "token", 5, &val, &nVal, &rem);
    if (rc == KVSTORE_OK) {
        printf("  get_ttl(\"token\"): value=\"%.*s\", remaining=%lld ms\n",
               nVal, (char *)val, (long long)rem);
        snkv_free(val);
    }

    /* Inspect TTL separately. */
    print_ttl(pKV, "token");

    /* Regular kvstore_put also works — TTL entry is removed automatically. */
    kvstore_put(pKV, "token", 5, "overwritten", 11);
    print_ttl(pKV, "token");   /* should print "no expiry" */
}

/* ------------------------------------------------------------------ */
/* Section 2: Lazy expiry — expired keys are deleted on access.       */
/* ------------------------------------------------------------------ */
static void section_lazy_expiry(KVStore *pKV) {
    printf("\n--- 2. Lazy expiry ---\n");

    /* Expire in 50 ms. */
    int64_t expire_ms = kvstore_now_ms() + 50;
    kvstore_put_ttl(pKV, "flash", 5, "here today", 10, expire_ms);
    printf("  inserted \"flash\" (expires in 50 ms)\n");

    sleep_ms(100);   /* wait past expiry */

    void *val = NULL; int nVal = 0; int64_t rem = -1;
    int rc = kvstore_get_ttl(pKV, "flash", 5, &val, &nVal, &rem);
    if (rc == KVSTORE_NOTFOUND) {
        printf("  get_ttl(\"flash\") after expiry: NOTFOUND (lazy-deleted), remaining=%lld\n",
               (long long)rem);
    }

    /* Confirm raw get also returns NOTFOUND. */
    rc = kvstore_get(pKV, "flash", 5, &val, &nVal);
    printf("  raw get(\"flash\"): %s\n",
           rc == KVSTORE_NOTFOUND ? "NOTFOUND (confirmed)" : "unexpected result");
}

/* ------------------------------------------------------------------ */
/* Section 3: Bulk expiry cleanup with purge_expired.                 */
/* ------------------------------------------------------------------ */
static void section_purge(KVStore *pKV) {
    printf("\n--- 3. purge_expired ---\n");

    int64_t past   = kvstore_now_ms() - 1000;   /* already expired */
    int64_t future = kvstore_now_ms() + 60000;  /* 1 min from now */

    kvstore_put_ttl(pKV, "old1", 4, "v", 1, past);
    kvstore_put_ttl(pKV, "old2", 4, "v", 1, past);
    kvstore_put_ttl(pKV, "old3", 4, "v", 1, past);
    kvstore_put_ttl(pKV, "keep", 4, "v", 1, future);

    int n = -1;
    int rc = kvstore_purge_expired(pKV, &n);
    printf("  purge_expired: rc=%s, deleted=%d\n",
           rc == KVSTORE_OK ? "OK" : "FAIL", n);

    /* "keep" should still be here. */
    void *val = NULL; int nVal = 0;
    rc = kvstore_get(pKV, "keep", 4, &val, &nVal);
    printf("  get(\"keep\"): %s\n", rc == KVSTORE_OK ? "OK (survived)" : "NOTFOUND");
    if (val) snkv_free(val);
}

/* ------------------------------------------------------------------ */
/* Section 4: TTL on a named column family.                           */
/* ------------------------------------------------------------------ */
static void section_named_cf_ttl(KVStore *pKV) {
    printf("\n--- 4. CF-level TTL (named CF) ---\n");

    KVColumnFamily *pCF = NULL;
    int rc = kvstore_cf_create(pKV, "rate_limits", &pCF);
    if (rc != KVSTORE_OK) { printf("  cf_create failed\n"); return; }

    /* Rate-limit token expires in 1 second. */
    int64_t expire_ms = kvstore_now_ms() + 1000;
    rc = kvstore_cf_put_ttl(pCF, "user:42", 7, "5", 1, expire_ms);
    printf("  cf_put_ttl(\"rate_limits\", \"user:42\", 1 s): %s\n",
           rc == KVSTORE_OK ? "OK" : "FAIL");

    void *val = NULL; int nVal = 0; int64_t rem = 0;
    rc = kvstore_cf_get_ttl(pCF, "user:42", 7, &val, &nVal, &rem);
    if (rc == KVSTORE_OK) {
        printf("  cf_get_ttl: value=\"%.*s\", remaining=%lld ms\n",
               nVal, (char *)val, (long long)rem);
        snkv_free(val);
    }

    /* Extend the TTL by overwriting with a longer expiry. */
    expire_ms = kvstore_now_ms() + 30000;
    kvstore_cf_put_ttl(pCF, "user:42", 7, "5", 1, expire_ms);
    rc = kvstore_cf_ttl_remaining(pCF, "user:42", 7, &rem);
    printf("  after extending TTL: remaining=%lld ms\n", (long long)rem);

    /* Remove TTL entirely (expire_ms=0 → permanent). */
    kvstore_cf_put_ttl(pCF, "user:42", 7, "5", 1, 0);
    rc = kvstore_cf_ttl_remaining(pCF, "user:42", 7, &rem);
    printf("  after removing TTL:  remaining=%s\n",
           rem == KVSTORE_NO_TTL ? "KVSTORE_NO_TTL (permanent)" : "unexpected");

    /* Purge on this CF only — nothing should be deleted. */
    int n = -1;
    rc = kvstore_cf_purge_expired(pCF, &n);
    printf("  cf_purge_expired(\"rate_limits\"): deleted=%d\n", n);

    kvstore_cf_close(pCF);
}

/* ------------------------------------------------------------------ */
/* Section 5: TTL inside an explicit transaction.                     */
/* ------------------------------------------------------------------ */
static void section_ttl_in_transaction(KVStore *pKV) {
    printf("\n--- 5. TTL inside explicit transaction ---\n");

    int64_t expire_ms = kvstore_now_ms() + 10000;

    int rc = kvstore_begin(pKV, 1);
    printf("  begin: %s\n", rc == KVSTORE_OK ? "OK" : "FAIL");

    kvstore_put_ttl(pKV, "tx_k1", 5, "val1", 4, expire_ms);
    kvstore_put_ttl(pKV, "tx_k2", 5, "val2", 4, expire_ms);

    /* Rollback — both data and TTL entries should vanish. */
    rc = kvstore_rollback(pKV);
    printf("  rollback: %s\n", rc == KVSTORE_OK ? "OK" : "FAIL");

    void *val = NULL; int nVal = 0;
    rc = kvstore_get(pKV, "tx_k1", 5, &val, &nVal);
    printf("  get(\"tx_k1\") after rollback: %s\n",
           rc == KVSTORE_NOTFOUND ? "NOTFOUND (correct)" : "unexpected");

    /* Now commit path. */
    rc = kvstore_begin(pKV, 1);
    kvstore_put_ttl(pKV, "tx_k1", 5, "val1", 4, expire_ms);
    rc = kvstore_commit(pKV);
    printf("  commit: %s\n", rc == KVSTORE_OK ? "OK" : "FAIL");

    int64_t rem = 0;
    rc = kvstore_ttl_remaining(pKV, "tx_k1", 5, &rem);
    printf("  ttl_remaining(\"tx_k1\") after commit: %lld ms\n", (long long)rem);
}

/* ------------------------------------------------------------------ */
int main(void) {
    const char *path = "ttl_example.db";
    remove(path);

    KVStore *pKV = NULL;
    int rc = kvstore_open(path, &pKV, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "kvstore_open failed: %d\n", rc);
        return 1;
    }

    printf("=== SNKV TTL Example ===");

    section_basic_ttl(pKV);
    section_lazy_expiry(pKV);
    section_purge(pKV);
    section_named_cf_ttl(pKV);
    section_ttl_in_transaction(pKV);

    kvstore_close(pKV);
    remove(path);

    printf("\n[OK] ttl.c example complete.\n");
    return 0;
}
