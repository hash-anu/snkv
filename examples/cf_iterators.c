/* SPDX-License-Identifier: Apache-2.0 */
/*
** Iterating a Named Column Family
** Demonstrates: kvstore_cf_iterator_create (full scan of one CF),
**               kvstore_cf_prefix_iterator_create (keys with a prefix),
**               kvstore_cf_reverse_prefix_iterator_create (prefix, newest
**               first), and that each CF is a separate key space
**
** iterators.c and iterator_reverse.c scan the default CF; these are the
** same calls for any other CF.  Keys come back sorted byte by byte, so a
** prefix like "order:2026-11-" selects one month of orders.
*/

#define SNKV_IMPLEMENTATION
#include "snkv.h"
#include <stdio.h>
#include <string.h>

#define DB_FILE "cf_iterators.db"

static void cleanup(void) {
    remove(DB_FILE);
    remove(DB_FILE "-wal");
    remove(DB_FILE "-shm");
}

static void print_row(KVIterator *it) {
    void *pKey = NULL, *pVal = NULL;
    int nKey = 0, nVal = 0;
    kvstore_iterator_key(it, &pKey, &nKey);       /* owned by the iterator: */
    kvstore_iterator_value(it, &pVal, &nVal);     /* do not free            */
    printf("  %.*s = %.*s\n", nKey, (char *)pKey, nVal, (char *)pVal);
}

int main(void) {
    KVStore *pKV = NULL;
    KVColumnFamily *pOrders = NULL, *pUsers = NULL;
    KVIterator *it = NULL;

    cleanup();
    kvstore_open(DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
    kvstore_cf_create(pKV, "orders", &pOrders);
    kvstore_cf_create(pKV, "users", &pUsers);

    /* Keys with a sortable date: lexicographic order = date order. */
    kvstore_cf_put(pOrders, "order:2026-10-30:001", 20, "laptop", 6);
    kvstore_cf_put(pOrders, "order:2026-11-01:002", 20, "mouse", 5);
    kvstore_cf_put(pOrders, "order:2026-11-02:003", 20, "keyboard", 8);
    kvstore_cf_put(pOrders, "order:2026-11-03:004", 20, "monitor", 7);
    kvstore_cf_put(pOrders, "refund:2026-11-02:001", 21, "mouse", 5);

    /* Same key in another CF: CFs are separate key spaces. */
    kvstore_cf_put(pUsers, "order:2026-11-01:002", 20, "(not an order)", 14);

    /* 1. Full scan of one CF: only its own keys, in key order. */
    printf("=== kvstore_cf_iterator_create: every key in \"orders\" ===\n");
    if (kvstore_cf_iterator_create(pOrders, &it) == KVSTORE_OK) {
        for (kvstore_iterator_first(it); !kvstore_iterator_eof(it); kvstore_iterator_next(it))
            print_row(it);
        kvstore_iterator_close(it);
    }
    printf("(the \"users\" CF's key with the same name is not in this scan)\n\n");

    /* 2. Prefix scan: the iterator is already on the first match; no
    ** iterator_first needed.  It stops at the first key without the prefix. */
    printf("=== kvstore_cf_prefix_iterator_create: November orders ===\n");
    if (kvstore_cf_prefix_iterator_create(pOrders, "order:2026-11-", 14, &it) == KVSTORE_OK) {
        for (; !kvstore_iterator_eof(it); kvstore_iterator_next(it))
            print_row(it);
        kvstore_iterator_close(it);
    }
    printf("\n");

    /* 3. Reverse prefix scan: newest first.  Positioned on the last match;
    ** walk backwards with kvstore_iterator_prev. */
    printf("=== kvstore_cf_reverse_prefix_iterator_create: November, newest first ===\n");
    if (kvstore_cf_reverse_prefix_iterator_create(pOrders, "order:2026-11-", 14, &it) == KVSTORE_OK) {
        for (; !kvstore_iterator_eof(it); kvstore_iterator_prev(it))
            print_row(it);
        kvstore_iterator_close(it);
    }

    /* Typical use of the reverse form: "latest order" = first row. */
    printf("\nLatest order: ");
    if (kvstore_cf_reverse_prefix_iterator_create(pOrders, "order:", 6, &it) == KVSTORE_OK) {
        if (!kvstore_iterator_eof(it)) print_row(it);
        kvstore_iterator_close(it);
    }

    kvstore_cf_close(pUsers);
    kvstore_cf_close(pOrders);
    kvstore_close(pKV);
    cleanup();
    printf("\n[OK] cf_iterators.c example complete.\n");
    return 0;
}
