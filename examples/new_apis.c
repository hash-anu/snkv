/* SPDX-License-Identifier: Apache-2.0 */
/*
** New APIs Example
**
** Demonstrates:
**   - kvstore_iterator_seek() — forward and reverse positional seek
**   - kvstore_put_if_absent() / kvstore_cf_put_if_absent() — atomic conditional insert
**   - kvstore_clear() / kvstore_cf_clear() — bulk truncation
**   - kvstore_count() / kvstore_cf_count() — entry count
**   - kvstore_stats_reset() + extended kvstore_stats() fields
*/

#define SNKV_IMPLEMENTATION
#include "snkv.h"
#include <stdio.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Section 1: Iterator seek — forward                                  */
/* ------------------------------------------------------------------ */
static void section_seek_forward(KVStore *pKV) {
    printf("\n--- 1. Iterator seek (forward) ---\n");

    kvstore_put(pKV, "apple",  5, "fruit",  5);
    kvstore_put(pKV, "banana", 6, "fruit",  5);
    kvstore_put(pKV, "cherry", 6, "fruit",  5);
    kvstore_put(pKV, "date",   4, "fruit",  5);

    KVIterator *pIter = NULL;
    kvstore_iterator_create(pKV, &pIter);

    /* Seek to exact key. */
    kvstore_iterator_seek(pIter, "banana", 6);
    if (!kvstore_iterator_eof(pIter)) {
        void *pKey; int nKey;
        kvstore_iterator_key(pIter, &pKey, &nKey);
        printf("  seek(\"banana\")  → \"%.*s\"\n", nKey, (char *)pKey);
    }

    /* Seek between keys — lands on next key >= target. */
    kvstore_iterator_seek(pIter, "bo", 2);
    if (!kvstore_iterator_eof(pIter)) {
        void *pKey; int nKey;
        kvstore_iterator_key(pIter, &pKey, &nKey);
        printf("  seek(\"bo\")      → \"%.*s\"  (next >= target)\n", nKey, (char *)pKey);
    }

    /* Seek past last key → eof. */
    kvstore_iterator_seek(pIter, "zzz", 3);
    printf("  seek(\"zzz\")     → eof=%d\n", kvstore_iterator_eof(pIter));

    /* Seek then walk forward. */
    kvstore_iterator_seek(pIter, "cherry", 6);
    printf("  walk from \"cherry\":");
    while (!kvstore_iterator_eof(pIter)) {
        void *pKey; int nKey;
        kvstore_iterator_key(pIter, &pKey, &nKey);
        printf(" %.*s", nKey, (char *)pKey);
        kvstore_iterator_next(pIter);
    }
    printf("\n");

    kvstore_iterator_close(pIter);
    kvstore_clear(pKV);
}

/* ------------------------------------------------------------------ */
/* Section 2: Iterator seek — reverse                                  */
/* ------------------------------------------------------------------ */
static void section_seek_reverse(KVStore *pKV) {
    printf("\n--- 2. Iterator seek (reverse) ---\n");

    kvstore_put(pKV, "aaa", 3, "v", 1);
    kvstore_put(pKV, "bbb", 3, "v", 1);
    kvstore_put(pKV, "ccc", 3, "v", 1);

    KVIterator *pIter = NULL;
    kvstore_reverse_iterator_create(pKV, &pIter);

    /* Seek on a reverse iterator → nearest key <= target. */
    kvstore_iterator_last(pIter);
    kvstore_iterator_seek(pIter, "bbb", 3);
    if (!kvstore_iterator_eof(pIter)) {
        void *pKey; int nKey;
        kvstore_iterator_key(pIter, &pKey, &nKey);
        printf("  rev seek(\"bbb\") → \"%.*s\"\n", nKey, (char *)pKey);
    }

    /* Seek between keys — lands on nearest key <= target. */
    kvstore_iterator_seek(pIter, "bbc", 3);
    if (!kvstore_iterator_eof(pIter)) {
        void *pKey; int nKey;
        kvstore_iterator_key(pIter, &pKey, &nKey);
        printf("  rev seek(\"bbc\") → \"%.*s\"  (nearest <=)\n", nKey, (char *)pKey);
    }

    /* Seek before all keys → eof. */
    kvstore_iterator_seek(pIter, "000", 3);
    printf("  rev seek(\"000\") → eof=%d\n", kvstore_iterator_eof(pIter));

    kvstore_iterator_close(pIter);
    kvstore_clear(pKV);
}

/* ------------------------------------------------------------------ */
/* Section 3: Iterator seek — prefix iterator                          */
/* ------------------------------------------------------------------ */
static void section_seek_prefix(KVStore *pKV) {
    printf("\n--- 3. Seek within a prefix iterator ---\n");

    kvstore_put(pKV, "user:alice", 10, "1", 1);
    kvstore_put(pKV, "user:bob",   8,  "2", 1);
    kvstore_put(pKV, "user:carol", 10, "3", 1);
    kvstore_put(pKV, "team:alpha", 10, "x", 1);  /* different prefix */

    KVIterator *pIter = NULL;
    kvstore_prefix_iterator_create(pKV, "user:", 5, &pIter);

    /* Seek to a key inside the prefix. */
    kvstore_iterator_seek(pIter, "user:bob", 8);
    printf("  seek(\"user:bob\") within prefix \"user:\":\n");
    while (!kvstore_iterator_eof(pIter)) {
        void *pKey; int nKey;
        kvstore_iterator_key(pIter, &pKey, &nKey);
        printf("    %.*s\n", nKey, (char *)pKey);
        kvstore_iterator_next(pIter);
    }
    printf("  (prefix boundary respected — \"team:alpha\" not visited)\n");

    kvstore_iterator_close(pIter);
    kvstore_clear(pKV);
}

/* ------------------------------------------------------------------ */
/* Section 4: put_if_absent — default CF                               */
/* ------------------------------------------------------------------ */
static void section_put_if_absent(KVStore *pKV) {
    printf("\n--- 4. put_if_absent (default CF) ---\n");

    int inserted = 0;

    /* Key absent → inserted. */
    kvstore_put_if_absent(pKV, "lock", 4, "owner:alice", 11, 0, &inserted);
    printf("  put_if_absent(\"lock\", absent) → inserted=%d\n", inserted);

    /* Key present → not inserted. */
    kvstore_put_if_absent(pKV, "lock", 4, "owner:bob", 9, 0, &inserted);
    printf("  put_if_absent(\"lock\", present) → inserted=%d\n", inserted);

    /* Verify the original value is untouched. */
    void *pVal = NULL; int nVal = 0;
    kvstore_get(pKV, "lock", 4, &pVal, &nVal);
    printf("  current value: \"%.*s\"\n", nVal, (char *)pVal);
    snkv_free(pVal);

    /* put_if_absent with TTL — insert a lock that auto-releases in 5 s. */
    kvstore_put_if_absent(pKV, "session:42", 10, "token-xyz", 9,
                          kvstore_now_ms() + 5000, &inserted);
    int64_t rem = 0;
    kvstore_ttl_remaining(pKV, "session:42", 10, &rem);
    printf("  put_if_absent(\"session:42\", ttl=5s) → inserted=%d, remaining=%lld ms\n",
           inserted, (long long)rem);

    kvstore_clear(pKV);
}

/* ------------------------------------------------------------------ */
/* Section 5: put_if_absent — named CF                                 */
/* ------------------------------------------------------------------ */
static void section_put_if_absent_cf(KVStore *pKV) {
    printf("\n--- 5. put_if_absent (named CF) ---\n");

    KVColumnFamily *pCF = NULL;
    kvstore_cf_create(pKV, "dedup", &pCF);

    int inserted = 0;

    kvstore_cf_put_if_absent(pCF, "msg:001", 7, "hello", 5, 0, &inserted);
    printf("  cf put_if_absent(\"msg:001\", first)  → inserted=%d\n", inserted);

    kvstore_cf_put_if_absent(pCF, "msg:001", 7, "world", 5, 0, &inserted);
    printf("  cf put_if_absent(\"msg:001\", second) → inserted=%d\n", inserted);

    void *pVal = NULL; int nVal = 0;
    kvstore_cf_get(pCF, "msg:001", 7, &pVal, &nVal);
    printf("  value: \"%.*s\"  (first write wins)\n", nVal, (char *)pVal);
    snkv_free(pVal);

    /* put_if_absent inside an explicit transaction. */
    kvstore_begin(pKV, 1);
    kvstore_cf_put_if_absent(pCF, "msg:002", 7, "a", 1, 0, &inserted);
    kvstore_cf_put_if_absent(pCF, "msg:002", 7, "b", 1, 0, &inserted);  /* same tx */
    kvstore_commit(pKV);

    kvstore_cf_get(pCF, "msg:002", 7, &pVal, &nVal);
    printf("  tx: msg:002 = \"%.*s\"  (first write in tx wins)\n", nVal, (char *)pVal);
    snkv_free(pVal);

    kvstore_cf_close(pCF);
}

/* ------------------------------------------------------------------ */
/* Section 6: clear                                                    */
/* ------------------------------------------------------------------ */
static void section_clear(KVStore *pKV) {
    printf("\n--- 6. clear ---\n");

    /* Populate default CF. */
    for (int i = 0; i < 10; i++) {
        char key[16]; int n = snprintf(key, sizeof key, "k%d", i);
        kvstore_put(pKV, key, n, "v", 1);
    }

    int64_t before = 0, after = 0;
    kvstore_count(pKV, &before);
    printf("  count before clear: %lld\n", (long long)before);

    kvstore_clear(pKV);
    kvstore_count(pKV, &after);
    printf("  count after  clear: %lld\n", (long long)after);

    /* New inserts work normally after clear. */
    kvstore_put(pKV, "fresh", 5, "start", 5);
    kvstore_count(pKV, &after);
    printf("  count after re-insert: %lld\n", (long long)after);

    /* CF-level clear is isolated. */
    KVColumnFamily *pCF = NULL;
    kvstore_cf_create(pKV, "scratch", &pCF);
    kvstore_cf_put(pCF, "tmp1", 4, "x", 1);
    kvstore_cf_put(pCF, "tmp2", 4, "x", 1);

    int64_t cfBefore = 0, cfAfter = 0;
    kvstore_cf_count(pCF, &cfBefore);
    kvstore_cf_clear(pCF);
    kvstore_cf_count(pCF, &cfAfter);
    printf("  CF \"scratch\": count %lld → %lld after cf_clear\n",
           (long long)cfBefore, (long long)cfAfter);

    /* Default CF unaffected. */
    int64_t defaultCount = 0;
    kvstore_count(pKV, &defaultCount);
    printf("  default CF count unchanged: %lld\n", (long long)defaultCount);

    kvstore_cf_close(pCF);
    kvstore_clear(pKV);
}

/* ------------------------------------------------------------------ */
/* Section 7: count                                                    */
/* ------------------------------------------------------------------ */
static void section_count(KVStore *pKV) {
    printf("\n--- 7. count ---\n");

    int64_t n = 0;
    kvstore_count(pKV, &n);
    printf("  empty store: count=%lld\n", (long long)n);

    for (int i = 0; i < 5; i++) {
        char key[8]; int kn = snprintf(key, sizeof key, "k%d", i);
        kvstore_put(pKV, key, kn, "v", 1);
    }
    kvstore_count(pKV, &n);
    printf("  after 5 puts: count=%lld\n", (long long)n);

    kvstore_delete(pKV, "k2", 2);
    kvstore_count(pKV, &n);
    printf("  after delete k2: count=%lld\n", (long long)n);

    /* count() includes expired-but-not-yet-purged keys. */
    int64_t past_ms = kvstore_now_ms() - 1000;
    kvstore_put_ttl(pKV, "exp", 3, "v", 1, past_ms);
    kvstore_count(pKV, &n);
    printf("  with 1 expired (unpurged): count=%lld\n", (long long)n);

    kvstore_purge_expired(pKV, NULL);
    kvstore_count(pKV, &n);
    printf("  after purge_expired: count=%lld\n", (long long)n);

    /* CF count is independent of default CF. */
    KVColumnFamily *pCF = NULL;
    kvstore_cf_create(pKV, "myCF", &pCF);
    kvstore_cf_put(pCF, "a", 1, "v", 1);
    kvstore_cf_put(pCF, "b", 1, "v", 1);

    int64_t cfN = 0;
    kvstore_cf_count(pCF, &cfN);
    int64_t defN = 0;
    kvstore_count(pKV, &defN);
    printf("  CF count=%lld, default CF count=%lld (isolated)\n",
           (long long)cfN, (long long)defN);

    kvstore_cf_close(pCF);
    kvstore_clear(pKV);
}

/* ------------------------------------------------------------------ */
/* Section 8: extended stats + stats_reset                             */
/* ------------------------------------------------------------------ */
static void section_stats(KVStore *pKV) {
    printf("\n--- 8. Extended stats + stats_reset ---\n");

    kvstore_stats_reset(pKV);

    /* Write a key. */
    kvstore_put(pKV, "hello", 5, "0123456789", 10);   /* 15 bytes */

    /* Read it back. */
    void *pVal = NULL; int nVal = 0;
    kvstore_get(pKV, "hello", 5, &pVal, &nVal);
    snkv_free(pVal);

    /* Add an expired TTL key and read it (triggers lazy expiry). */
    int64_t past_ms = kvstore_now_ms() - 1000;
    kvstore_put_ttl(pKV, "gone", 4, "x", 1, past_ms);
    { int64_t rem = 0;
      kvstore_get_ttl(pKV, "gone", 4, &pVal, &nVal, &rem); } /* lazy-expires on read */

    /* Purge nothing remaining, but record any purges. */
    kvstore_purge_expired(pKV, NULL);

    KVStoreStats st;
    kvstore_stats(pKV, &st);

    printf("  puts          = %llu\n",  (unsigned long long)st.nPuts);
    printf("  gets          = %llu\n",  (unsigned long long)st.nGets);
    printf("  bytes_written = %llu\n",  (unsigned long long)st.nBytesWritten);
    printf("  bytes_read    = %llu\n",  (unsigned long long)st.nBytesRead);
    printf("  wal_commits   = %llu\n",  (unsigned long long)st.nWalCommits);
    printf("  checkpoints   = %llu\n",  (unsigned long long)st.nCheckpoints);
    printf("  ttl_expired   = %llu\n",  (unsigned long long)st.nTtlExpired);
    printf("  ttl_purged    = %llu\n",  (unsigned long long)st.nTtlPurged);
    printf("  db_pages      = %llu\n",  (unsigned long long)st.nDbPages);

    /* Reset and verify cumulative counters clear. */
    kvstore_stats_reset(pKV);
    kvstore_stats(pKV, &st);
    printf("  after reset: puts=%llu gets=%llu bytes_written=%llu\n",
           (unsigned long long)st.nPuts,
           (unsigned long long)st.nGets,
           (unsigned long long)st.nBytesWritten);
    printf("  db_pages after reset = %llu  (always live)\n",
           (unsigned long long)st.nDbPages);

    kvstore_clear(pKV);
}

/* ------------------------------------------------------------------ */
int main(void) {
    const char *path = "new_apis_example.db";
    remove(path);

    KVStore *pKV = NULL;
    int rc = kvstore_open(path, &pKV, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "kvstore_open failed: %d\n", rc);
        return 1;
    }

    printf("=== SNKV New APIs Example ===");

    section_seek_forward(pKV);
    section_seek_reverse(pKV);
    section_seek_prefix(pKV);
    section_put_if_absent(pKV);
    section_put_if_absent_cf(pKV);
    section_clear(pKV);
    section_count(pKV);
    section_stats(pKV);

    kvstore_close(pKV);
    remove(path);
    remove("new_apis_example.db-wal");
    remove("new_apis_example.db-shm");

    printf("\n[OK] new_apis.c example complete.\n");
    return 0;
}
