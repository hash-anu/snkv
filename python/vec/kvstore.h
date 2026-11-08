/* SPDX-License-Identifier: Apache-2.0 */
/*
** kvstore.h — shim for building the vector store against snkv.h.
**
** kvstore_vec.h includes "kvstore.h".  In the repository that header pulls
** in SQLite's internal headers; here the same declarations come from the
** single-header amalgamation instead.  setup.py copies kvstore_vec.c,
** kvstore_vec.h and the usearch sources next to this file at build time.
*/
#ifndef SNKV_PY_KVSTORE_SHIM_H
#define SNKV_PY_KVSTORE_SHIM_H

#include "snkv.h"

/*
** kvstore_vec.c uses SQLite's mutex API.  Its declarations sit inside
** snkv.h's SNKV_IMPLEMENTATION block, which only snkv_module.c compiles,
** so declare them for the other translation units.
*/
#ifndef SNKV_IMPLEMENTATION
typedef struct sqlite3_mutex sqlite3_mutex;

#define SQLITE_MUTEX_FAST      0
#define SQLITE_MUTEX_RECURSIVE 1

sqlite3_mutex *sqlite3_mutex_alloc(int);
void           sqlite3_mutex_free(sqlite3_mutex*);
void           sqlite3_mutex_enter(sqlite3_mutex*);
void           sqlite3_mutex_leave(sqlite3_mutex*);
#endif

#endif /* SNKV_PY_KVSTORE_SHIM_H */
