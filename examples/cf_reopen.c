/* SPDX-License-Identifier: Apache-2.0 */
/*
** Column Families Across Restarts
** Demonstrates: kvstore_cf_open (reopen an existing CF after the store is
**               closed), KVSTORE_NOTFOUND for a missing CF,
**               kvstore_cf_get_default, kvstore_cf_exists, kvstore_cf_delete
**
** column_families.c creates CFs; a real application also has to find them
** again after a restart.  CF names and their data are stored in the file,
** so reopening the store and calling kvstore_cf_open by name is enough.
*/

#define SNKV_IMPLEMENTATION
#include "snkv.h"
#include <stdio.h>
#include <string.h>

#define DB_FILE "cf_reopen.db"

static void cleanup(void) {
    remove(DB_FILE);
    remove(DB_FILE "-wal");
    remove(DB_FILE "-shm");
}

/* First run of the "application": create CFs and write some data. */
static void first_run(void) {
    KVStore *pKV = NULL;
    KVColumnFamily *pUsers = NULL, *pSessions = NULL;

    printf("=== First run: create column families ===\n");
    kvstore_open(DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
    kvstore_cf_create(pKV, "users", &pUsers);
    kvstore_cf_create(pKV, "sessions", &pSessions);

    kvstore_cf_put(pUsers, "user:1", 6, "alice", 5);
    kvstore_cf_put(pUsers, "user:2", 6, "bob", 3);
    kvstore_cf_put(pSessions, "sess:abc", 8, "user:1", 6);

    /* The default CF is what kvstore_put / kvstore_get use. */
    kvstore_put(pKV, "app:version", 11, "1.4", 3);
    printf("Wrote 2 users, 1 session and app:version, then closed the store.\n\n");

    kvstore_cf_close(pSessions);
    kvstore_cf_close(pUsers);
    kvstore_close(pKV);
}

/* Second run: the process restarted; find the CFs again by name. */
static void second_run(void) {
    KVStore *pKV = NULL;
    KVColumnFamily *pUsers = NULL, *pMissing = NULL, *pDefault = NULL;
    void *pValue = NULL;
    int nValue = 0, exists = 0, rc;

    printf("=== Second run: reopen and find the CFs again ===\n");
    kvstore_open(DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);

    /* kvstore_cf_open: an existing CF, by name. */
    rc = kvstore_cf_open(pKV, "users", &pUsers);
    printf("kvstore_cf_open(\"users\")   -> %s\n", rc == KVSTORE_OK ? "OK" : "error");
    if (rc != KVSTORE_OK) { kvstore_close(pKV); return; }

    kvstore_cf_get(pUsers, "user:1", 6, &pValue, &nValue);
    printf("users/user:1 survived the restart: %.*s\n", nValue, (char *)pValue);
    snkv_free(pValue);

    /* A CF that was never created: KVSTORE_NOTFOUND, not an error.
    ** Typical pattern: open it, and create it only if it is missing. */
    rc = kvstore_cf_open(pKV, "audit_log", &pMissing);
    printf("kvstore_cf_open(\"audit_log\") -> %s\n",
           rc == KVSTORE_NOTFOUND ? "KVSTORE_NOTFOUND (create it on demand)" : "unexpected");
    if (rc == KVSTORE_NOTFOUND) {
        kvstore_cf_create(pKV, "audit_log", &pMissing);
        printf("  created \"audit_log\"\n");
    }

    /* kvstore_cf_exists: is a key present, without reading its value. */
    kvstore_cf_exists(pUsers, "user:2", 6, &exists);
    printf("\nkvstore_cf_exists(users, \"user:2\") -> %d\n", exists);

    /* kvstore_cf_delete: remove one key; deleting it again is NOTFOUND. */
    rc = kvstore_cf_delete(pUsers, "user:2", 6);
    printf("kvstore_cf_delete(users, \"user:2\") -> %s\n", rc == KVSTORE_OK ? "OK" : "error");
    rc = kvstore_cf_delete(pUsers, "user:2", 6);
    printf("delete again                       -> %s\n",
           rc == KVSTORE_NOTFOUND ? "KVSTORE_NOTFOUND" : "unexpected");
    kvstore_cf_exists(pUsers, "user:2", 6, &exists);
    printf("kvstore_cf_exists(users, \"user:2\") -> %d\n", exists);

    /* kvstore_cf_get_default: a handle to the default CF, so code written
    ** against KVColumnFamily* works for it too.  It takes a reference:
    ** close it like any other CF handle. */
    kvstore_cf_get_default(pKV, &pDefault);
    kvstore_cf_get(pDefault, "app:version", 11, &pValue, &nValue);
    printf("\nkvstore_cf_get_default + cf_get(\"app:version\") -> %.*s\n", nValue, (char *)pValue);
    printf("(the same key kvstore_put wrote: the default CF and kvstore_put/get are one namespace)\n");
    snkv_free(pValue);

    kvstore_cf_close(pDefault);
    kvstore_cf_close(pMissing);
    kvstore_cf_close(pUsers);
    kvstore_close(pKV);
}

int main(void) {
    cleanup();
    first_run();
    second_run();
    cleanup();
    printf("\n[OK] cf_reopen.c example complete.\n");
    return 0;
}
