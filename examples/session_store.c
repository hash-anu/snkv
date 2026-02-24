/* SPDX-License-Identifier: Apache-2.0 */
/*
** Session Store Example
** Demonstrates: Real-world web session management with create/get/cleanup
*/

#define SNKV_IMPLEMENTATION
#include "snkv.h"
#include <stdio.h>
#include <string.h>
#include <time.h>

typedef struct {
    char user_id[64];
    time_t created_at;
    time_t last_access;
    int visit_count;
} Session;

static int session_create(KeyValueStore *pKV, const char *session_id,
                          const char *user_id) {
    Session sess;
    memset(&sess, 0, sizeof(sess));

    strncpy(sess.user_id, user_id, sizeof(sess.user_id) - 1);
    sess.created_at = time(NULL);
    sess.last_access = sess.created_at;
    sess.visit_count = 1;

    return keyvaluestore_put(pKV, session_id, strlen(session_id),
                       &sess, sizeof(Session));
}

static int session_get(KeyValueStore *pKV, const char *session_id, Session *pSess) {
    void *pValue;
    int nValue;
    int rc;

    rc = keyvaluestore_get(pKV, session_id, strlen(session_id), &pValue, &nValue);

    if (rc == KEYVALUESTORE_OK) {
        memcpy(pSess, pValue, sizeof(Session));
        snkv_free(pValue);

        /* Update last access time */
        pSess->last_access = time(NULL);
        pSess->visit_count++;

        keyvaluestore_put(pKV, session_id, strlen(session_id),
                    pSess, sizeof(Session));
    }

    return rc;
}

static void session_delete(KeyValueStore *pKV, const char *session_id) {
    keyvaluestore_delete(pKV, session_id, strlen(session_id));
}

static int session_cleanup_expired(KeyValueStore *pKV, int max_age_seconds) {
    KeyValueIterator *pIter;
    time_t now = time(NULL);
    int deleted = 0;

    keyvaluestore_iterator_create(pKV, &pIter);

    for (keyvaluestore_iterator_first(pIter);
         !keyvaluestore_iterator_eof(pIter);
         keyvaluestore_iterator_next(pIter)) {

        void *pKey, *pValue;
        int nKey, nValue;

        keyvaluestore_iterator_key(pIter, &pKey, &nKey);
        keyvaluestore_iterator_value(pIter, &pValue, &nValue);

        if ((int)nValue >= (int)sizeof(Session)) {
            Session *pSess = (Session*)pValue;
            if (now - pSess->last_access > max_age_seconds) {
                char *key_copy = snkv_malloc(nKey + 1);
                memcpy(key_copy, pKey, nKey);
                key_copy[nKey] = '\0';

                keyvaluestore_delete(pKV, key_copy, nKey);
                snkv_free(key_copy);
                deleted++;
            }
        }
    }

    keyvaluestore_iterator_close(pIter);
    return deleted;
}

int main(void) {
    KeyValueStore *pKV;
    Session sess;

    printf("=== Session Store ===\n\n");

    keyvaluestore_open("sessions.db", &pKV, KEYVALUESTORE_JOURNAL_WAL);

    /* Create sessions */
    printf("Creating sessions...\n");
    session_create(pKV, "sess_abc123", "user_alice");
    session_create(pKV, "sess_def456", "user_bob");
    session_create(pKV, "sess_ghi789", "user_charlie");

    /* Access a session */
    printf("\nAccessing session...\n");
    if (session_get(pKV, "sess_abc123", &sess) == KEYVALUESTORE_OK) {
        printf("Session for user: %s\n", sess.user_id);
        printf("Visit count: %d\n", sess.visit_count);
    }

    /* Delete a specific session */
    printf("\nDeleting session sess_ghi789...\n");
    session_delete(pKV, "sess_ghi789");

    /* Verify deletion */
    if (session_get(pKV, "sess_ghi789", &sess) == KEYVALUESTORE_NOTFOUND) {
        printf("Session sess_ghi789: deleted successfully\n");
    }

    /* Clean up old sessions (older than 1 hour) */
    printf("\nCleaning up expired sessions...\n");
    int deleted = session_cleanup_expired(pKV, 3600);
    printf("Deleted %d expired sessions\n", deleted);

    keyvaluestore_close(pKV);
    remove("sessions.db");

    printf("\nSession store example passed.\n");
    return 0;
}
