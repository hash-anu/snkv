/* SPDX-License-Identifier: Apache-2.0 */
/*
** Reverse Iterator Examples
**
** Demonstrates:
**   - kvstore_reverse_iterator_create  — full descending scan
**   - kvstore_reverse_prefix_iterator_create — prefix scan in reverse
**   - kvstore_cf_reverse_iterator_create / kvstore_cf_reverse_prefix_iterator_create
**   - Practical use-case: leaderboard (top-N scores)
*/

#define SNKV_IMPLEMENTATION
#include "snkv.h"
#include <stdio.h>
#include <string.h>

/* --------------------------------------------------------------------- */
/* 1. Full reverse scan                                                   */
/* --------------------------------------------------------------------- */
static void example_reverse_scan(void) {
    KVStore   *pKV;
    KVIterator *pIter;

    printf("=== Reverse Scan ===\n");

    kvstore_open("rev_basic.db", &pKV, KVSTORE_JOURNAL_WAL);

    kvstore_put(pKV, "apple",  5, "0.50", 4);
    kvstore_put(pKV, "banana", 6, "0.30", 4);
    kvstore_put(pKV, "cherry", 6, "1.20", 4);
    kvstore_put(pKV, "date",   4, "2.00", 4);
    kvstore_put(pKV, "elderberry", 10, "3.50", 4);

    kvstore_reverse_iterator_create(pKV, &pIter);

    printf("%-12s %s\n", "Fruit", "Price");
    printf("--------------------\n");

    for (kvstore_iterator_last(pIter);
         !kvstore_iterator_eof(pIter);
         kvstore_iterator_prev(pIter)) {

        void *pKey, *pValue;
        int   nKey, nValue;

        kvstore_iterator_key(pIter, &pKey, &nKey);
        kvstore_iterator_value(pIter, &pValue, &nValue);

        printf("%-12.*s %.*s\n", nKey, (char *)pKey, nValue, (char *)pValue);
    }

    kvstore_iterator_close(pIter);
    kvstore_close(pKV);
    remove("rev_basic.db");
    printf("\n");
}

/* --------------------------------------------------------------------- */
/* 2. Reverse prefix scan                                                 */
/* --------------------------------------------------------------------- */
static void example_reverse_prefix_scan(void) {
    KVStore   *pKV;
    KVIterator *pIter;

    printf("=== Reverse Prefix Scan ===\n");

    kvstore_open("rev_prefix.db", &pKV, KVSTORE_JOURNAL_WAL);

    /* Mix of user: and admin: keys */
    kvstore_put(pKV, "admin:root",    10, "active",   6);
    kvstore_put(pKV, "user:alice",    10, "online",   6);
    kvstore_put(pKV, "user:bob",       8, "offline",  7);
    kvstore_put(pKV, "user:charlie",  12, "online",   6);
    kvstore_put(pKV, "user:diana",    10, "away",     4);
    kvstore_put(pKV, "user:eve",       8, "online",   6);

    /* Reverse-scan only "user:" keys: eve → diana → charlie → bob → alice */
    kvstore_reverse_prefix_iterator_create(pKV, "user:", 5, &pIter);

    printf("%-16s %s\n", "User", "Status");
    printf("-------------------------------\n");

    /* Reverse prefix iterator is already positioned — do NOT call last() */
    while (!kvstore_iterator_eof(pIter)) {
        void *pKey, *pValue;
        int   nKey, nValue;

        kvstore_iterator_key(pIter, &pKey, &nKey);
        kvstore_iterator_value(pIter, &pValue, &nValue);

        printf("%-16.*s %.*s\n", nKey, (char *)pKey, nValue, (char *)pValue);

        kvstore_iterator_prev(pIter);
    }

    kvstore_iterator_close(pIter);
    kvstore_close(pKV);
    remove("rev_prefix.db");
    printf("\n");
}

/* --------------------------------------------------------------------- */
/* 3. Column-family reverse scan                                          */
/* --------------------------------------------------------------------- */
static void example_cf_reverse_scan(void) {
    KVStore        *pKV;
    KVColumnFamily *pCF;
    KVIterator      *pIter;

    printf("=== Column Family Reverse Scan ===\n");

    kvstore_open("rev_cf.db", &pKV, KVSTORE_JOURNAL_WAL);
    kvstore_cf_create(pKV, "products", &pCF);

    kvstore_cf_put(pCF, "gizmo",  5, "9.99",  4);
    kvstore_cf_put(pCF, "gadget", 6, "24.99", 5);
    kvstore_cf_put(pCF, "widget", 6, "4.99",  4);
    kvstore_cf_put(pCF, "doohickey", 9, "14.99", 5);

    kvstore_cf_reverse_iterator_create(pCF, &pIter);

    printf("%-12s %s\n", "Product", "Price");
    printf("--------------------\n");

    for (kvstore_iterator_last(pIter);
         !kvstore_iterator_eof(pIter);
         kvstore_iterator_prev(pIter)) {

        void *pKey, *pValue;
        int   nKey, nValue;

        kvstore_iterator_key(pIter, &pKey, &nKey);
        kvstore_iterator_value(pIter, &pValue, &nValue);

        printf("%-12.*s %.*s\n", nKey, (char *)pKey, nValue, (char *)pValue);
    }

    kvstore_iterator_close(pIter);
    kvstore_cf_close(pCF);
    kvstore_close(pKV);
    remove("rev_cf.db");
    printf("\n");
}

/* --------------------------------------------------------------------- */
/* 4. Leaderboard: top-N scores via reverse prefix scan                  */
/*                                                                        */
/* Keys are stored as "score:<8-digit-score-padded>:<player>"            */
/* so lexicographic descending order = score descending.                 */
/* --------------------------------------------------------------------- */
static void example_leaderboard(void) {
    KVStore   *pKV;
    KVIterator *pIter;
    char        key[64];
    int         i;

    printf("=== Leaderboard: Top-5 Scores ===\n");

    kvstore_open("leaderboard.db", &pKV, KVSTORE_JOURNAL_WAL);

    /* Store scores as zero-padded keys so sort order = score order */
    const char *players[] = { "alice", "bob", "charlie", "diana", "eve",
                               "frank", "grace", "hank", "iris", "jake" };
    int scores[]          = { 9200, 7800, 12500, 3400, 11000,
                              8600, 15000, 4200, 9800, 6300 };

    for (i = 0; i < 10; i++) {
        snprintf(key, sizeof(key), "score:%08d:%s", scores[i], players[i]);
        kvstore_put(pKV, key, (int)strlen(key), players[i], (int)strlen(players[i]));
    }

    /* Reverse-prefix over "score:" → highest scores first */
    kvstore_reverse_prefix_iterator_create(pKV, "score:", 6, &pIter);

    printf("%-5s %-10s %s\n", "Rank", "Player", "Score");
    printf("----------------------------\n");

    int rank = 1;
    while (!kvstore_iterator_eof(pIter) && rank <= 5) {
        void *pKey, *pValue;
        int   nKey, nValue;
        int   score;

        kvstore_iterator_key(pIter, &pKey, &nKey);
        kvstore_iterator_value(pIter, &pValue, &nValue);

        /* Extract score from key: "score:XXXXXXXX:name" */
        sscanf((char *)pKey + 6, "%d", &score);

        printf("%-5d %-10.*s %d\n", rank, nValue, (char *)pValue, score);

        rank++;
        kvstore_iterator_prev(pIter);
    }

    kvstore_iterator_close(pIter);
    kvstore_close(pKV);
    remove("leaderboard.db");
    printf("\n");
}

int main(void) {
    example_reverse_scan();
    example_reverse_prefix_scan();
    example_cf_reverse_scan();
    example_leaderboard();
    printf("All reverse iterator examples passed.\n");
    return 0;
}
