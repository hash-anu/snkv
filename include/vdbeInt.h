/*
** Minimal vdbeInt.h stub for SNKV
**
** Provides the internal Vdbe struct definition needed by status.c
** (which iterates db->pVdbe to count prepared statement memory).
*/
#ifndef SQLITE_VDBEINT_H
#define SQLITE_VDBEINT_H

#include "sqliteInt.h"

/*
** Minimal Vdbe struct – only the fields actually accessed by the
** btree/pager/status layer.  status.c walks pVNext; everything else
** is opaque.
*/
struct Vdbe {
  sqlite3 *db;
  Vdbe *pVNext;
  Vdbe *pVPrev;
};

/*
** Minimal Mem / sqlite3_value struct.
**
** In real SQLite, Mem and sqlite3_value are the same type:
**   sqlite3.h:  typedef struct sqlite3_value sqlite3_value;
**   vdbe.h:     typedef struct sqlite3_value Mem;
**
** So the struct tag is "sqlite3_value" and both names refer to it.
** Only the fields actually referenced by the btree/pager/status layer
** are declared here.
*/
struct sqlite3_value {
  union MemValue {
    double r;
    i64 i;
    int nZero;
    const char *zPType;
    FuncDef *pDef;
  } u;
  u16 flags;
  u8  enc;
  u8  eSubtype;
  int n;
  char *z;
  char *zMalloc;
  int szMalloc;
  u32 uTemp;
  sqlite3 *db;
  void (*xDel)(void*);
};

/*
** Mem flags – only the values needed by vdbe_stubs.c
*/
#define MEM_Null      0x0001
#define MEM_Str       0x0002
#define MEM_Int       0x0004
#define MEM_Real      0x0008
#define MEM_Blob      0x0010

#endif /* SQLITE_VDBEINT_H */
