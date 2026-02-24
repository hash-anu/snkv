/*
** snkv_module.c - CPython C extension for SNKV key-value store
**
** Exposes the full SNKV C API to Python as the low-level '_snkv' module.
** The high-level 'snkv' package (snkv/__init__.py) builds on top of this.
**
** Build (from repo root):
**   pip install -e python/
** or:
**   cd python && python setup.py build_ext --inplace
**
** Keys and values are raw bytes (Python bytes / buffer protocol).
** String encoding is handled by the higher-level snkv.Store wrapper.
*/

#define SNKV_IMPLEMENTATION
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "snkv.h"

/* =====================================================================
** Forward declarations
** ===================================================================== */
static PyTypeObject KeyValueStoreType;
static PyTypeObject ColumnFamilyType;
static PyTypeObject IteratorType;

/* =====================================================================
** Module-level exception types
** ===================================================================== */
static PyObject *SnkvError;
static PyObject *SnkvNotFoundError;
static PyObject *SnkvBusyError;
static PyObject *SnkvLockedError;
static PyObject *SnkvReadOnlyError;
static PyObject *SnkvCorruptError;

/* =====================================================================
** Internal helpers
** ===================================================================== */

/* Map a KEYVALUESTORE_* return code + optional db handle to a Python exception. */
static PyObject *
snkv_raise_from(KeyValueStore *db, int rc)
{
    PyObject *exc;
    const char *msg = NULL;

    if (db) {
        msg = keyvaluestore_errmsg(db);
    }
    if (!msg || !msg[0]) {
        msg = "snkv error";
    }

    switch (rc) {
        case KEYVALUESTORE_NOTFOUND: exc = SnkvNotFoundError; break;
        case KEYVALUESTORE_BUSY:     exc = SnkvBusyError;     break;
        case KEYVALUESTORE_LOCKED:   exc = SnkvLockedError;   break;
        case KEYVALUESTORE_READONLY: exc = SnkvReadOnlyError; break;
        case KEYVALUESTORE_CORRUPT:  exc = SnkvCorruptError;  break;
        default:               exc = SnkvError;         break;
    }
    PyErr_SetString(exc, msg);
    return NULL;
}

/* Closed-object guard macros */
#define KV_CHECK_OPEN(self) \
    do { if (!(self)->db) { \
        PyErr_SetString(SnkvError, "KeyValueStore is closed"); \
        return NULL; } } while (0)

#define CF_CHECK_OPEN(self) \
    do { if (!(self)->cf) { \
        PyErr_SetString(SnkvError, "ColumnFamily is closed"); \
        return NULL; } } while (0)

#define IT_CHECK_OPEN(self) \
    do { if (!(self)->iter) { \
        PyErr_SetString(SnkvError, "Iterator is closed"); \
        return NULL; } } while (0)


/* =====================================================================
** IteratorObject
** ===================================================================== */

typedef struct {
    PyObject_HEAD
    KeyValueIterator    *iter;
    PyObject      *store_ref;   /* KeyValueStoreObject* kept alive via Py_INCREF */
    KeyValueStore       *db;          /* convenience pointer for error messages    */
    int            needs_first; /* 1 = normal iter (call first() on __next__) */
    int            started;     /* 0 = before first read, 1 = in progress    */
} IteratorObject;

static void
Iterator_dealloc(IteratorObject *self)
{
    if (self->iter) {
        keyvaluestore_iterator_close(self->iter);
        self->iter = NULL;
    }
    Py_XDECREF(self->store_ref);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *
Iterator_first(IteratorObject *self, PyObject *Py_UNUSED(ignored))
{
    int rc;
    IT_CHECK_OPEN(self);
    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_iterator_first(self->iter);
    Py_END_ALLOW_THREADS
    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    self->started = 1;
    self->needs_first = 0;
    Py_RETURN_NONE;
}

static PyObject *
Iterator_next_method(IteratorObject *self, PyObject *Py_UNUSED(ignored))
{
    int rc;
    IT_CHECK_OPEN(self);
    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_iterator_next(self->iter);
    Py_END_ALLOW_THREADS
    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    Py_RETURN_NONE;
}

static PyObject *
Iterator_eof(IteratorObject *self, PyObject *Py_UNUSED(ignored))
{
    if (!self->iter) Py_RETURN_TRUE;
    return PyBool_FromLong(keyvaluestore_iterator_eof(self->iter));
}

static PyObject *
Iterator_key(IteratorObject *self, PyObject *Py_UNUSED(ignored))
{
    void *pKey = NULL;
    int   nKey = 0, rc;
    IT_CHECK_OPEN(self);
    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_iterator_key(self->iter, &pKey, &nKey);
    Py_END_ALLOW_THREADS
    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    return PyBytes_FromStringAndSize((const char *)pKey, nKey);
}

static PyObject *
Iterator_value(IteratorObject *self, PyObject *Py_UNUSED(ignored))
{
    void *pValue = NULL;
    int   nValue = 0, rc;
    IT_CHECK_OPEN(self);
    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_iterator_value(self->iter, &pValue, &nValue);
    Py_END_ALLOW_THREADS
    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    return PyBytes_FromStringAndSize((const char *)pValue, nValue);
}

static PyObject *
Iterator_item(IteratorObject *self, PyObject *Py_UNUSED(ignored))
{
    void *pKey = NULL, *pValue = NULL;
    int   nKey = 0,     nValue = 0, rc;
    PyObject *k, *v, *pair;

    IT_CHECK_OPEN(self);
    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_iterator_key(self->iter, &pKey, &nKey);
    if (rc == KEYVALUESTORE_OK)
        rc = keyvaluestore_iterator_value(self->iter, &pValue, &nValue);
    Py_END_ALLOW_THREADS
    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);

    k = PyBytes_FromStringAndSize((const char *)pKey, nKey);
    if (!k) return NULL;
    v = PyBytes_FromStringAndSize((const char *)pValue, nValue);
    if (!v) { Py_DECREF(k); return NULL; }
    pair = PyTuple_Pack(2, k, v);
    Py_DECREF(k);
    Py_DECREF(v);
    return pair;
}

static PyObject *
Iterator_close(IteratorObject *self, PyObject *Py_UNUSED(ignored))
{
    if (self->iter) {
        keyvaluestore_iterator_close(self->iter);
        self->iter = NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
Iterator_enter(PyObject *self, PyObject *Py_UNUSED(ignored))
{
    Py_INCREF(self);
    return self;
}

static PyObject *
Iterator_exit(IteratorObject *self, PyObject *args)
{
    (void)args;
    if (self->iter) {
        keyvaluestore_iterator_close(self->iter);
        self->iter = NULL;
    }
    Py_RETURN_FALSE;
}

/* Python iterator protocol */
static PyObject *
Iterator_iter(PyObject *self)
{
    Py_INCREF(self);
    return self;
}

/*
** __next__:
**   - First call on a normal iterator: call first(), check eof, read item.
**   - First call on a prefix iterator: already positioned, check eof, read item.
**   - Subsequent calls: advance (next()), check eof, read item.
**   Returns (key_bytes, value_bytes) tuple, or NULL (StopIteration) at end.
*/
static PyObject *
Iterator_iternext(IteratorObject *self)
{
    int rc, eof;

    if (!self->iter) return NULL;  /* closed -> StopIteration */

    if (!self->started) {
        /* First call */
        self->started = 1;
        if (self->needs_first) {
            Py_BEGIN_ALLOW_THREADS
            rc = keyvaluestore_iterator_first(self->iter);
            Py_END_ALLOW_THREADS
            if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
        }
        /* Prefix iterators are already positioned; fall through to read. */
    } else {
        /* Advance to next position */
        Py_BEGIN_ALLOW_THREADS
        rc = keyvaluestore_iterator_next(self->iter);
        Py_END_ALLOW_THREADS
        if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    }

    /* Check if we're past the end */
    eof = keyvaluestore_iterator_eof(self->iter);
    if (eof) return NULL;  /* StopIteration */

    /* Read and return (key, value) */
    return Iterator_item(self, NULL);
}

static PyMethodDef Iterator_methods[] = {
    {"first",  (PyCFunction)Iterator_first,        METH_NOARGS, "Move to first key."},
    {"next",   (PyCFunction)Iterator_next_method,  METH_NOARGS, "Advance to next key."},
    {"eof",    (PyCFunction)Iterator_eof,           METH_NOARGS, "True if past last key."},
    {"key",    (PyCFunction)Iterator_key,           METH_NOARGS, "Return current key bytes."},
    {"value",  (PyCFunction)Iterator_value,         METH_NOARGS, "Return current value bytes."},
    {"item",   (PyCFunction)Iterator_item,          METH_NOARGS, "Return (key, value) tuple."},
    {"close",  (PyCFunction)Iterator_close,         METH_NOARGS, "Close the iterator."},
    {"__enter__", (PyCFunction)Iterator_enter,      METH_NOARGS, NULL},
    {"__exit__",  (PyCFunction)Iterator_exit,       METH_VARARGS, NULL},
    {NULL, NULL, 0, NULL}
};

static PyTypeObject IteratorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "_snkv.Iterator",
    .tp_basicsize = sizeof(IteratorObject),
    .tp_dealloc   = (destructor)Iterator_dealloc,
    .tp_iter      = Iterator_iter,
    .tp_iternext  = (iternextfunc)Iterator_iternext,
    .tp_flags     = Py_TPFLAGS_DEFAULT,
    .tp_doc       = "Ordered key-value iterator.",
    .tp_methods   = Iterator_methods,
};


/* =====================================================================
** ColumnFamilyObject
** ===================================================================== */

typedef struct {
    PyObject_HEAD
    KeyValueColumnFamily *cf;
    PyObject       *store_ref;  /* KeyValueStoreObject* */
    KeyValueStore        *db;         /* convenience for error messages */
} ColumnFamilyObject;

/* Forward declaration for make_iterator */
static PyObject *make_iterator(KeyValueIterator *iter, PyObject *store_ref,
                                KeyValueStore *db, int needs_first);

static void
ColumnFamily_dealloc(ColumnFamilyObject *self)
{
    if (self->cf) {
        keyvaluestore_cf_close(self->cf);
        self->cf = NULL;
    }
    Py_XDECREF(self->store_ref);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *
ColumnFamily_put(ColumnFamilyObject *self, PyObject *args)
{
    Py_buffer key_buf, val_buf;
    int rc;
    PyObject *result;

    CF_CHECK_OPEN(self);
    if (!PyArg_ParseTuple(args, "y*y*", &key_buf, &val_buf)) return NULL;

    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_cf_put(self->cf,
                        key_buf.buf, (int)key_buf.len,
                        val_buf.buf, (int)val_buf.len);
    Py_END_ALLOW_THREADS

    PyBuffer_Release(&key_buf);
    PyBuffer_Release(&val_buf);

    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    result = Py_None;
    Py_INCREF(result);
    return result;
}

static PyObject *
ColumnFamily_get(ColumnFamilyObject *self, PyObject *args)
{
    Py_buffer key_buf;
    void     *value = NULL;
    int       nValue = 0, rc;
    PyObject *result;

    CF_CHECK_OPEN(self);
    if (!PyArg_ParseTuple(args, "y*", &key_buf)) return NULL;

    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_cf_get(self->cf, key_buf.buf, (int)key_buf.len,
                        &value, &nValue);
    Py_END_ALLOW_THREADS

    PyBuffer_Release(&key_buf);

    if (rc == KEYVALUESTORE_NOTFOUND) {
        PyErr_SetNone(SnkvNotFoundError);
        return NULL;
    }
    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);

    result = PyBytes_FromStringAndSize((const char *)value, nValue);
    snkv_free(value);
    return result;
}

static PyObject *
ColumnFamily_delete(ColumnFamilyObject *self, PyObject *args)
{
    Py_buffer key_buf;
    int rc;
    PyObject *result;

    CF_CHECK_OPEN(self);
    if (!PyArg_ParseTuple(args, "y*", &key_buf)) return NULL;

    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_cf_delete(self->cf, key_buf.buf, (int)key_buf.len);
    Py_END_ALLOW_THREADS

    PyBuffer_Release(&key_buf);

    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    result = Py_None;
    Py_INCREF(result);
    return result;
}

static PyObject *
ColumnFamily_exists(ColumnFamilyObject *self, PyObject *args)
{
    Py_buffer key_buf;
    int exists = 0, rc;

    CF_CHECK_OPEN(self);
    if (!PyArg_ParseTuple(args, "y*", &key_buf)) return NULL;

    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_cf_exists(self->cf, key_buf.buf, (int)key_buf.len, &exists);
    Py_END_ALLOW_THREADS

    PyBuffer_Release(&key_buf);

    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    return PyBool_FromLong(exists);
}

static PyObject *
ColumnFamily_iterator(ColumnFamilyObject *self, PyObject *Py_UNUSED(ignored))
{
    KeyValueIterator *iter = NULL;
    int rc;

    CF_CHECK_OPEN(self);
    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_cf_iterator_create(self->cf, &iter);
    Py_END_ALLOW_THREADS
    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    return make_iterator(iter, self->store_ref, self->db, /*needs_first=*/1);
}

static PyObject *
ColumnFamily_prefix_iterator(ColumnFamilyObject *self, PyObject *args)
{
    Py_buffer prefix_buf;
    KeyValueIterator *iter = NULL;
    int rc;

    CF_CHECK_OPEN(self);
    if (!PyArg_ParseTuple(args, "y*", &prefix_buf)) return NULL;

    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_cf_prefix_iterator_create(self->cf,
                                            prefix_buf.buf,
                                            (int)prefix_buf.len,
                                            &iter);
    Py_END_ALLOW_THREADS

    PyBuffer_Release(&prefix_buf);

    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    return make_iterator(iter, self->store_ref, self->db, /*needs_first=*/0);
}

static PyObject *
ColumnFamily_close(ColumnFamilyObject *self, PyObject *Py_UNUSED(ignored))
{
    if (self->cf) {
        keyvaluestore_cf_close(self->cf);
        self->cf = NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
ColumnFamily_enter(PyObject *self, PyObject *Py_UNUSED(ignored))
{
    Py_INCREF(self);
    return self;
}

static PyObject *
ColumnFamily_exit(ColumnFamilyObject *self, PyObject *args)
{
    (void)args;
    if (self->cf) {
        keyvaluestore_cf_close(self->cf);
        self->cf = NULL;
    }
    Py_RETURN_FALSE;
}

static PyMethodDef ColumnFamily_methods[] = {
    {"put",              (PyCFunction)ColumnFamily_put,              METH_VARARGS, "put(key, value) -> None"},
    {"get",              (PyCFunction)ColumnFamily_get,              METH_VARARGS, "get(key) -> bytes"},
    {"delete",           (PyCFunction)ColumnFamily_delete,           METH_VARARGS, "delete(key) -> None"},
    {"exists",           (PyCFunction)ColumnFamily_exists,           METH_VARARGS, "exists(key) -> bool"},
    {"iterator",         (PyCFunction)ColumnFamily_iterator,         METH_NOARGS,  "iterator() -> Iterator"},
    {"prefix_iterator",  (PyCFunction)ColumnFamily_prefix_iterator,  METH_VARARGS, "prefix_iterator(prefix) -> Iterator"},
    {"close",            (PyCFunction)ColumnFamily_close,            METH_NOARGS,  "close() -> None"},
    {"__enter__",        (PyCFunction)ColumnFamily_enter,            METH_NOARGS,  NULL},
    {"__exit__",         (PyCFunction)ColumnFamily_exit,             METH_VARARGS, NULL},
    {NULL, NULL, 0, NULL}
};

static PyTypeObject ColumnFamilyType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "_snkv.ColumnFamily",
    .tp_basicsize = sizeof(ColumnFamilyObject),
    .tp_dealloc   = (destructor)ColumnFamily_dealloc,
    .tp_flags     = Py_TPFLAGS_DEFAULT,
    .tp_doc       = "A logical namespace within a SNKV store.",
    .tp_methods   = ColumnFamily_methods,
};


/* =====================================================================
** Factory helpers (defined after type structs are complete)
** ===================================================================== */

static PyObject *
make_iterator(KeyValueIterator *iter, PyObject *store_ref, KeyValueStore *db, int needs_first)
{
    IteratorObject *obj;
    obj = (IteratorObject *)IteratorType.tp_alloc(&IteratorType, 0);
    if (!obj) {
        keyvaluestore_iterator_close(iter);
        return NULL;
    }
    obj->iter        = iter;
    obj->store_ref   = store_ref;
    obj->db          = db;
    obj->needs_first = needs_first;
    obj->started     = 0;
    Py_XINCREF(store_ref);
    return (PyObject *)obj;
}

static PyObject *
make_column_family(KeyValueColumnFamily *cf, PyObject *store_ref, KeyValueStore *db)
{
    ColumnFamilyObject *obj;
    obj = (ColumnFamilyObject *)ColumnFamilyType.tp_alloc(&ColumnFamilyType, 0);
    if (!obj) {
        keyvaluestore_cf_close(cf);
        return NULL;
    }
    obj->cf        = cf;
    obj->store_ref = store_ref;
    obj->db        = db;
    Py_XINCREF(store_ref);
    return (PyObject *)obj;
}


/* =====================================================================
** KeyValueStoreObject
** ===================================================================== */

typedef struct {
    PyObject_HEAD
    KeyValueStore *db;
} KeyValueStoreObject;

static void
KeyValueStore_dealloc(KeyValueStoreObject *self)
{
    if (self->db) {
        KeyValueStore *db = self->db;
        self->db = NULL;
        Py_BEGIN_ALLOW_THREADS
        keyvaluestore_close(db);
        Py_END_ALLOW_THREADS
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *
KeyValueStore_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    KeyValueStoreObject *self = (KeyValueStoreObject *)type->tp_alloc(type, 0);
    if (self) self->db = NULL;
    return (PyObject *)self;
}

/* KeyValueStore(filename=None, journal_mode=JOURNAL_WAL) */
static int
KeyValueStore_init(KeyValueStoreObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"filename", "journal_mode", NULL};
    const char *filename   = NULL;
    int         journal_mode = KEYVALUESTORE_JOURNAL_WAL;
    KeyValueStore    *db = NULL;
    int         rc;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|zi", kwlist,
                                     &filename, &journal_mode))
        return -1;

    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_open(filename, &db, journal_mode);
    Py_END_ALLOW_THREADS

    if (rc != KEYVALUESTORE_OK) {
        snkv_raise_from(db, rc);
        if (db) { KeyValueStore *tmp = db; Py_BEGIN_ALLOW_THREADS keyvaluestore_close(tmp); Py_END_ALLOW_THREADS }
        return -1;
    }
    self->db = db;
    return 0;
}

/* KeyValueStore.open_v2(filename=None, **config) -- classmethod */
static PyObject *
KeyValueStore_open_v2(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {
        "filename", "journal_mode", "sync_level", "cache_size",
        "page_size", "read_only", "busy_timeout", "wal_size_limit", NULL
    };
    const char   *filename = NULL;
    KeyValueStoreConfig cfg       = {0};
    KeyValueStore      *db        = NULL;
    KeyValueStoreObject *self;
    int            rc;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|ziiiiiii", kwlist,
                                     &filename,
                                     &cfg.journalMode,
                                     &cfg.syncLevel,
                                     &cfg.cacheSize,
                                     &cfg.pageSize,
                                     &cfg.readOnly,
                                     &cfg.busyTimeout,
                                     &cfg.walSizeLimit))
        return NULL;

    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_open_v2(filename, &db, &cfg);
    Py_END_ALLOW_THREADS

    if (rc != KEYVALUESTORE_OK) {
        snkv_raise_from(db, rc);
        if (db) { KeyValueStore *tmp = db; Py_BEGIN_ALLOW_THREADS keyvaluestore_close(tmp); Py_END_ALLOW_THREADS }
        return NULL;
    }

    self = (KeyValueStoreObject *)type->tp_alloc(type, 0);
    if (!self) {
        KeyValueStore *tmp = db;
        Py_BEGIN_ALLOW_THREADS keyvaluestore_close(tmp); Py_END_ALLOW_THREADS
        return NULL;
    }
    self->db = db;
    return (PyObject *)self;
}

/* KeyValueStore.close() */
static PyObject *
KeyValueStore_close(KeyValueStoreObject *self, PyObject *Py_UNUSED(ignored))
{
    if (self->db) {
        KeyValueStore *db = self->db;
        int rc;
        self->db = NULL;
        Py_BEGIN_ALLOW_THREADS
        rc = keyvaluestore_close(db);
        Py_END_ALLOW_THREADS
        if (rc != KEYVALUESTORE_OK) return snkv_raise_from(NULL, rc);
    }
    Py_RETURN_NONE;
}

/* KeyValueStore.put(key, value) */
static PyObject *
KeyValueStore_put(KeyValueStoreObject *self, PyObject *args)
{
    Py_buffer key_buf, val_buf;
    int rc;
    PyObject *result;

    KV_CHECK_OPEN(self);
    if (!PyArg_ParseTuple(args, "y*y*", &key_buf, &val_buf)) return NULL;

    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_put(self->db,
                     key_buf.buf, (int)key_buf.len,
                     val_buf.buf, (int)val_buf.len);
    Py_END_ALLOW_THREADS

    PyBuffer_Release(&key_buf);
    PyBuffer_Release(&val_buf);

    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    result = Py_None;
    Py_INCREF(result);
    return result;
}

/* KeyValueStore.get(key) -> bytes */
static PyObject *
KeyValueStore_get(KeyValueStoreObject *self, PyObject *args)
{
    Py_buffer key_buf;
    void     *value  = NULL;
    int       nValue = 0, rc;
    PyObject *result;

    KV_CHECK_OPEN(self);
    if (!PyArg_ParseTuple(args, "y*", &key_buf)) return NULL;

    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_get(self->db, key_buf.buf, (int)key_buf.len,
                     &value, &nValue);
    Py_END_ALLOW_THREADS

    PyBuffer_Release(&key_buf);

    if (rc == KEYVALUESTORE_NOTFOUND) {
        PyErr_SetNone(SnkvNotFoundError);
        return NULL;
    }
    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);

    result = PyBytes_FromStringAndSize((const char *)value, nValue);
    snkv_free(value);
    return result;
}

/* KeyValueStore.delete(key) */
static PyObject *
KeyValueStore_delete(KeyValueStoreObject *self, PyObject *args)
{
    Py_buffer key_buf;
    int rc;
    PyObject *result;

    KV_CHECK_OPEN(self);
    if (!PyArg_ParseTuple(args, "y*", &key_buf)) return NULL;

    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_delete(self->db, key_buf.buf, (int)key_buf.len);
    Py_END_ALLOW_THREADS

    PyBuffer_Release(&key_buf);

    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    result = Py_None;
    Py_INCREF(result);
    return result;
}

/* KeyValueStore.exists(key) -> bool */
static PyObject *
KeyValueStore_exists(KeyValueStoreObject *self, PyObject *args)
{
    Py_buffer key_buf;
    int exists = 0, rc;

    KV_CHECK_OPEN(self);
    if (!PyArg_ParseTuple(args, "y*", &key_buf)) return NULL;

    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_exists(self->db, key_buf.buf, (int)key_buf.len, &exists);
    Py_END_ALLOW_THREADS

    PyBuffer_Release(&key_buf);

    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    return PyBool_FromLong(exists);
}

/* KeyValueStore.begin(write=False) */
static PyObject *
KeyValueStore_begin(KeyValueStoreObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"write", NULL};
    int wrflag = 0, rc;

    KV_CHECK_OPEN(self);
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|p", kwlist, &wrflag))
        return NULL;

    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_begin(self->db, wrflag);
    Py_END_ALLOW_THREADS

    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    Py_RETURN_NONE;
}

/* KeyValueStore.commit() */
static PyObject *
KeyValueStore_commit(KeyValueStoreObject *self, PyObject *Py_UNUSED(ignored))
{
    int rc;
    KV_CHECK_OPEN(self);
    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_commit(self->db);
    Py_END_ALLOW_THREADS
    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    Py_RETURN_NONE;
}

/* KeyValueStore.rollback() */
static PyObject *
KeyValueStore_rollback(KeyValueStoreObject *self, PyObject *Py_UNUSED(ignored))
{
    int rc;
    KV_CHECK_OPEN(self);
    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_rollback(self->db);
    Py_END_ALLOW_THREADS
    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    Py_RETURN_NONE;
}

/* KeyValueStore.errmsg() -> str */
static PyObject *
KeyValueStore_errmsg(KeyValueStoreObject *self, PyObject *Py_UNUSED(ignored))
{
    const char *msg;
    KV_CHECK_OPEN(self);
    msg = keyvaluestore_errmsg(self->db);
    return PyUnicode_FromString(msg ? msg : "");
}

/* KeyValueStore.stats() -> dict */
static PyObject *
KeyValueStore_stats(KeyValueStoreObject *self, PyObject *Py_UNUSED(ignored))
{
    KeyValueStoreStats stats;
    int rc;
    KV_CHECK_OPEN(self);
    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_stats(self->db, &stats);
    Py_END_ALLOW_THREADS
    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    return Py_BuildValue("{sKsKsKsKsK}",
        "puts",       (unsigned long long)stats.nPuts,
        "gets",       (unsigned long long)stats.nGets,
        "deletes",    (unsigned long long)stats.nDeletes,
        "iterations", (unsigned long long)stats.nIterations,
        "errors",     (unsigned long long)stats.nErrors);
}

/* KeyValueStore.sync() */
static PyObject *
KeyValueStore_sync(KeyValueStoreObject *self, PyObject *Py_UNUSED(ignored))
{
    int rc;
    KV_CHECK_OPEN(self);
    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_sync(self->db);
    Py_END_ALLOW_THREADS
    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    Py_RETURN_NONE;
}

/* KeyValueStore.vacuum(n_pages=0) */
static PyObject *
KeyValueStore_vacuum(KeyValueStoreObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"n_pages", NULL};
    int n_pages = 0, rc;

    KV_CHECK_OPEN(self);
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i", kwlist, &n_pages))
        return NULL;

    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_incremental_vacuum(self->db, n_pages);
    Py_END_ALLOW_THREADS

    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    Py_RETURN_NONE;
}

/* KeyValueStore.integrity_check() -> None or raises CorruptError */
static PyObject *
KeyValueStore_integrity_check(KeyValueStoreObject *self, PyObject *Py_UNUSED(ignored))
{
    char     *errmsg = NULL;
    int       rc;
    PyObject *msg_obj;

    KV_CHECK_OPEN(self);
    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_integrity_check(self->db, &errmsg);
    Py_END_ALLOW_THREADS

    if (rc == KEYVALUESTORE_OK) {
        snkv_free(errmsg);
        Py_RETURN_NONE;
    }
    msg_obj = PyUnicode_FromString(errmsg ? errmsg : "integrity check failed");
    snkv_free(errmsg);
    PyErr_SetObject(SnkvCorruptError, msg_obj);
    Py_XDECREF(msg_obj);
    return NULL;
}

/* KeyValueStore.checkpoint(mode=CHECKPOINT_PASSIVE) -> (nLog, nCkpt) */
static PyObject *
KeyValueStore_checkpoint(KeyValueStoreObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"mode", NULL};
    int mode = KEYVALUESTORE_CHECKPOINT_PASSIVE;
    int nLog = 0, nCkpt = 0, rc;

    KV_CHECK_OPEN(self);
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i", kwlist, &mode))
        return NULL;

    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_checkpoint(self->db, mode, &nLog, &nCkpt);
    Py_END_ALLOW_THREADS

    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    return Py_BuildValue("(ii)", nLog, nCkpt);
}

/* KeyValueStore.cf_create(name) -> ColumnFamily */
static PyObject *
KeyValueStore_cf_create(KeyValueStoreObject *self, PyObject *args)
{
    const char     *name = NULL;
    KeyValueColumnFamily *cf   = NULL;
    int             rc;

    KV_CHECK_OPEN(self);
    if (!PyArg_ParseTuple(args, "s", &name)) return NULL;

    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_cf_create(self->db, name, &cf);
    Py_END_ALLOW_THREADS

    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    return make_column_family(cf, (PyObject *)self, self->db);
}

/* KeyValueStore.cf_open(name) -> ColumnFamily */
static PyObject *
KeyValueStore_cf_open(KeyValueStoreObject *self, PyObject *args)
{
    const char     *name = NULL;
    KeyValueColumnFamily *cf   = NULL;
    int             rc;

    KV_CHECK_OPEN(self);
    if (!PyArg_ParseTuple(args, "s", &name)) return NULL;

    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_cf_open(self->db, name, &cf);
    Py_END_ALLOW_THREADS

    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    return make_column_family(cf, (PyObject *)self, self->db);
}

/* KeyValueStore.cf_get_default() -> ColumnFamily */
static PyObject *
KeyValueStore_cf_get_default(KeyValueStoreObject *self, PyObject *Py_UNUSED(ignored))
{
    KeyValueColumnFamily *cf = NULL;
    int             rc;

    KV_CHECK_OPEN(self);
    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_cf_get_default(self->db, &cf);
    Py_END_ALLOW_THREADS

    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    return make_column_family(cf, (PyObject *)self, self->db);
}

/* KeyValueStore.cf_list() -> list[str] */
static PyObject *
KeyValueStore_cf_list(KeyValueStoreObject *self, PyObject *Py_UNUSED(ignored))
{
    char    **names  = NULL;
    int       count  = 0, i, rc;
    PyObject *result = NULL;

    KV_CHECK_OPEN(self);
    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_cf_list(self->db, &names, &count);
    Py_END_ALLOW_THREADS

    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);

    result = PyList_New(count);
    if (!result) goto cleanup;

    for (i = 0; i < count; i++) {
        PyObject *s = PyUnicode_FromString(names[i]);
        if (!s) { Py_DECREF(result); result = NULL; goto cleanup; }
        PyList_SET_ITEM(result, i, s);
    }

cleanup:
    if (names) {
        for (i = 0; i < count; i++) sqliteFree(names[i]);
        sqliteFree(names);
    }
    return result;
}

/* KeyValueStore.cf_drop(name) */
static PyObject *
KeyValueStore_cf_drop(KeyValueStoreObject *self, PyObject *args)
{
    const char *name = NULL;
    int         rc;

    KV_CHECK_OPEN(self);
    if (!PyArg_ParseTuple(args, "s", &name)) return NULL;

    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_cf_drop(self->db, name);
    Py_END_ALLOW_THREADS

    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    Py_RETURN_NONE;
}

/* KeyValueStore.iterator() -> Iterator */
static PyObject *
KeyValueStore_iterator(KeyValueStoreObject *self, PyObject *Py_UNUSED(ignored))
{
    KeyValueIterator *iter = NULL;
    int         rc;

    KV_CHECK_OPEN(self);
    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_iterator_create(self->db, &iter);
    Py_END_ALLOW_THREADS

    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    return make_iterator(iter, (PyObject *)self, self->db, /*needs_first=*/1);
}

/* KeyValueStore.prefix_iterator(prefix) -> Iterator */
static PyObject *
KeyValueStore_prefix_iterator(KeyValueStoreObject *self, PyObject *args)
{
    Py_buffer   prefix_buf;
    KeyValueIterator *iter = NULL;
    int         rc;

    KV_CHECK_OPEN(self);
    if (!PyArg_ParseTuple(args, "y*", &prefix_buf)) return NULL;

    Py_BEGIN_ALLOW_THREADS
    rc = keyvaluestore_prefix_iterator_create(self->db,
                                         prefix_buf.buf,
                                         (int)prefix_buf.len,
                                         &iter);
    Py_END_ALLOW_THREADS

    PyBuffer_Release(&prefix_buf);

    if (rc != KEYVALUESTORE_OK) return snkv_raise_from(self->db, rc);
    return make_iterator(iter, (PyObject *)self, self->db, /*needs_first=*/0);
}

/* KeyValueStore.__enter__ */
static PyObject *
KeyValueStore_enter(PyObject *self, PyObject *Py_UNUSED(ignored))
{
    Py_INCREF(self);
    return self;
}

/* KeyValueStore.__exit__ */
static PyObject *
KeyValueStore_exit(KeyValueStoreObject *self, PyObject *args)
{
    (void)args;
    if (self->db) {
        KeyValueStore *db = self->db;
        self->db = NULL;
        Py_BEGIN_ALLOW_THREADS
        keyvaluestore_close(db);
        Py_END_ALLOW_THREADS
    }
    Py_RETURN_FALSE;
}

static PyMethodDef KeyValueStore_methods[] = {
    /* Class method */
    {"open_v2",          (PyCFunction)KeyValueStore_open_v2,          METH_CLASS|METH_VARARGS|METH_KEYWORDS,
     "open_v2(filename=None, *, journal_mode, sync_level, cache_size, page_size, read_only, busy_timeout, wal_size_limit) -> KeyValueStore"},

    /* Core KV */
    {"put",              (PyCFunction)KeyValueStore_put,               METH_VARARGS,  "put(key, value) -> None"},
    {"get",              (PyCFunction)KeyValueStore_get,               METH_VARARGS,  "get(key) -> bytes"},
    {"delete",           (PyCFunction)KeyValueStore_delete,            METH_VARARGS,  "delete(key) -> None"},
    {"exists",           (PyCFunction)KeyValueStore_exists,            METH_VARARGS,  "exists(key) -> bool"},

    /* Transactions */
    {"begin",            (PyCFunction)KeyValueStore_begin,             METH_VARARGS|METH_KEYWORDS, "begin(write=False) -> None"},
    {"commit",           (PyCFunction)KeyValueStore_commit,            METH_NOARGS,   "commit() -> None"},
    {"rollback",         (PyCFunction)KeyValueStore_rollback,          METH_NOARGS,   "rollback() -> None"},

    /* Column families */
    {"cf_create",        (PyCFunction)KeyValueStore_cf_create,         METH_VARARGS,  "cf_create(name) -> ColumnFamily"},
    {"cf_open",          (PyCFunction)KeyValueStore_cf_open,           METH_VARARGS,  "cf_open(name) -> ColumnFamily"},
    {"cf_get_default",   (PyCFunction)KeyValueStore_cf_get_default,    METH_NOARGS,   "cf_get_default() -> ColumnFamily"},
    {"cf_list",          (PyCFunction)KeyValueStore_cf_list,           METH_NOARGS,   "cf_list() -> list[str]"},
    {"cf_drop",          (PyCFunction)KeyValueStore_cf_drop,           METH_VARARGS,  "cf_drop(name) -> None"},

    /* Iterators */
    {"iterator",         (PyCFunction)KeyValueStore_iterator,          METH_NOARGS,   "iterator() -> Iterator"},
    {"prefix_iterator",  (PyCFunction)KeyValueStore_prefix_iterator,   METH_VARARGS,  "prefix_iterator(prefix) -> Iterator"},

    /* Maintenance */
    {"errmsg",           (PyCFunction)KeyValueStore_errmsg,            METH_NOARGS,   "errmsg() -> str"},
    {"stats",            (PyCFunction)KeyValueStore_stats,             METH_NOARGS,   "stats() -> dict"},
    {"sync",             (PyCFunction)KeyValueStore_sync,              METH_NOARGS,   "sync() -> None"},
    {"vacuum",           (PyCFunction)KeyValueStore_vacuum,            METH_VARARGS|METH_KEYWORDS, "vacuum(n_pages=0) -> None"},
    {"integrity_check",  (PyCFunction)KeyValueStore_integrity_check,   METH_NOARGS,   "integrity_check() -> None"},
    {"checkpoint",       (PyCFunction)KeyValueStore_checkpoint,        METH_VARARGS|METH_KEYWORDS, "checkpoint(mode=CHECKPOINT_PASSIVE) -> (nLog, nCkpt)"},

    /* Lifecycle */
    {"close",            (PyCFunction)KeyValueStore_close,             METH_NOARGS,   "close() -> None"},
    {"__enter__",        (PyCFunction)KeyValueStore_enter,             METH_NOARGS,   NULL},
    {"__exit__",         (PyCFunction)KeyValueStore_exit,              METH_VARARGS,  NULL},
    {NULL, NULL, 0, NULL}
};

static PyTypeObject KeyValueStoreType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "_snkv.KeyValueStore",
    .tp_basicsize = sizeof(KeyValueStoreObject),
    .tp_dealloc   = (destructor)KeyValueStore_dealloc,
    .tp_flags     = Py_TPFLAGS_DEFAULT,
    .tp_doc       = "SNKV key-value store handle.",
    .tp_methods   = KeyValueStore_methods,
    .tp_new       = KeyValueStore_new,
    .tp_init      = (initproc)KeyValueStore_init,
};


/* =====================================================================
** Module definition
** ===================================================================== */

static struct PyModuleDef snkv_module = {
    PyModuleDef_HEAD_INIT,
    "_snkv",
    "Low-level CPython bindings for SNKV embedded key-value store.\n"
    "Use the 'snkv' package instead of importing this directly.",
    -1,
    NULL
};

PyMODINIT_FUNC
PyInit__snkv(void)
{
    PyObject *m;

    /* Finalise types */
    if (PyType_Ready(&IteratorType)    < 0) return NULL;
    if (PyType_Ready(&ColumnFamilyType) < 0) return NULL;
    if (PyType_Ready(&KeyValueStoreType)     < 0) return NULL;

    m = PyModule_Create(&snkv_module);
    if (!m) return NULL;

    /* ---- Exception hierarchy ----
    **
    **   Exception
    **     snkv.Error
    **       snkv.BusyError
    **       snkv.LockedError
    **       snkv.ReadOnlyError
    **       snkv.CorruptError
    **   KeyError
    **     snkv.NotFoundError  (also a subclass of snkv.Error)
    */
    SnkvError = PyErr_NewExceptionWithDoc(
        "_snkv.Error",
        "Base class for all SNKV errors.", NULL, NULL);
    if (!SnkvError) goto error;

    /* NotFoundError inherits from both KeyError and Error */
    {
        PyObject *bases = PyTuple_Pack(2, PyExc_KeyError, SnkvError);
        if (!bases) goto error;
        SnkvNotFoundError = PyErr_NewExceptionWithDoc(
            "_snkv.NotFoundError",
            "Key or column family not found.", bases, NULL);
        Py_DECREF(bases);
        if (!SnkvNotFoundError) goto error;
    }

    SnkvBusyError = PyErr_NewExceptionWithDoc(
        "_snkv.BusyError",
        "Database is locked by another connection (SQLITE_BUSY).", SnkvError, NULL);
    if (!SnkvBusyError) goto error;

    SnkvLockedError = PyErr_NewExceptionWithDoc(
        "_snkv.LockedError",
        "Database is locked within the same connection (SQLITE_LOCKED).", SnkvError, NULL);
    if (!SnkvLockedError) goto error;

    SnkvReadOnlyError = PyErr_NewExceptionWithDoc(
        "_snkv.ReadOnlyError",
        "Attempt to write a read-only database.", SnkvError, NULL);
    if (!SnkvReadOnlyError) goto error;

    SnkvCorruptError = PyErr_NewExceptionWithDoc(
        "_snkv.CorruptError",
        "Database file is corrupt.", SnkvError, NULL);
    if (!SnkvCorruptError) goto error;

    /* Add exceptions to module */
    Py_INCREF(SnkvError);
    if (PyModule_AddObject(m, "Error",         SnkvError)         < 0) goto error;
    Py_INCREF(SnkvNotFoundError);
    if (PyModule_AddObject(m, "NotFoundError", SnkvNotFoundError) < 0) goto error;
    Py_INCREF(SnkvBusyError);
    if (PyModule_AddObject(m, "BusyError",     SnkvBusyError)     < 0) goto error;
    Py_INCREF(SnkvLockedError);
    if (PyModule_AddObject(m, "LockedError",   SnkvLockedError)   < 0) goto error;
    Py_INCREF(SnkvReadOnlyError);
    if (PyModule_AddObject(m, "ReadOnlyError", SnkvReadOnlyError) < 0) goto error;
    Py_INCREF(SnkvCorruptError);
    if (PyModule_AddObject(m, "CorruptError",  SnkvCorruptError)  < 0) goto error;

    /* Add types */
    Py_INCREF(&KeyValueStoreType);
    if (PyModule_AddObject(m, "KeyValueStore",       (PyObject *)&KeyValueStoreType)      < 0) goto error;
    Py_INCREF(&ColumnFamilyType);
    if (PyModule_AddObject(m, "ColumnFamily",  (PyObject *)&ColumnFamilyType) < 0) goto error;
    Py_INCREF(&IteratorType);
    if (PyModule_AddObject(m, "Iterator",      (PyObject *)&IteratorType)     < 0) goto error;

    /* Journal mode constants */
    if (PyModule_AddIntConstant(m, "JOURNAL_DELETE",      KEYVALUESTORE_JOURNAL_DELETE)     < 0) goto error;
    if (PyModule_AddIntConstant(m, "JOURNAL_WAL",         KEYVALUESTORE_JOURNAL_WAL)        < 0) goto error;

    /* Sync level constants */
    if (PyModule_AddIntConstant(m, "SYNC_OFF",            KEYVALUESTORE_SYNC_OFF)           < 0) goto error;
    if (PyModule_AddIntConstant(m, "SYNC_NORMAL",         KEYVALUESTORE_SYNC_NORMAL)        < 0) goto error;
    if (PyModule_AddIntConstant(m, "SYNC_FULL",           KEYVALUESTORE_SYNC_FULL)          < 0) goto error;

    /* Checkpoint mode constants */
    if (PyModule_AddIntConstant(m, "CHECKPOINT_PASSIVE",  KEYVALUESTORE_CHECKPOINT_PASSIVE) < 0) goto error;
    if (PyModule_AddIntConstant(m, "CHECKPOINT_FULL",     KEYVALUESTORE_CHECKPOINT_FULL)    < 0) goto error;
    if (PyModule_AddIntConstant(m, "CHECKPOINT_RESTART",  KEYVALUESTORE_CHECKPOINT_RESTART) < 0) goto error;
    if (PyModule_AddIntConstant(m, "CHECKPOINT_TRUNCATE", KEYVALUESTORE_CHECKPOINT_TRUNCATE)< 0) goto error;

    /* Error code constants (mirror KEYVALUESTORE_* values) */
    if (PyModule_AddIntConstant(m, "OK",       KEYVALUESTORE_OK)       < 0) goto error;
    if (PyModule_AddIntConstant(m, "ERROR",    KEYVALUESTORE_ERROR)    < 0) goto error;
    if (PyModule_AddIntConstant(m, "BUSY",     KEYVALUESTORE_BUSY)     < 0) goto error;
    if (PyModule_AddIntConstant(m, "LOCKED",   KEYVALUESTORE_LOCKED)   < 0) goto error;
    if (PyModule_AddIntConstant(m, "NOMEM",    KEYVALUESTORE_NOMEM)    < 0) goto error;
    if (PyModule_AddIntConstant(m, "READONLY", KEYVALUESTORE_READONLY) < 0) goto error;
    if (PyModule_AddIntConstant(m, "CORRUPT",  KEYVALUESTORE_CORRUPT)  < 0) goto error;
    if (PyModule_AddIntConstant(m, "NOTFOUND", KEYVALUESTORE_NOTFOUND) < 0) goto error;
    if (PyModule_AddIntConstant(m, "PROTOCOL", KEYVALUESTORE_PROTOCOL) < 0) goto error;

    /* Limits */
    if (PyModule_AddIntConstant(m, "MAX_COLUMN_FAMILIES",
                                KEYVALUESTORE_MAX_COLUMN_FAMILIES)      < 0) goto error;

    return m;

error:
    Py_DECREF(m);
    return NULL;
}
