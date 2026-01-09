/*
 * src/hash.c
 *
 * Small generic hash-table implementation used by the project.
 * Supports string and binary keys; keys may be copied into the
 * table when requested (see `copyKey`). Primary operations:
 * initialize, clear, insert, find, and remove.
 * 
 *           Hash
           │
    ┌──────┴─────────┐
    │                │
 Buckets            Global List
 ht[0] ──► A ──► C   first ─► C ⇄ A ⇄ B ⇄ D
 ht[1] ──► B
 ht[2] ──► D

 * 
 */

#include "sqliteInt.h"
#include <assert.h>
/*
 * Initialize the Hash structure pointed to by pNew.
 * keyClass selects the key type (string or binary). If copyKey is
 * non-zero, keys will be copied into the table when inserted.
 */
void sqlite3HashInit(Hash *pNew, int keyClass, int copyKey){
  assert( pNew!=0 );
  assert( keyClass>=SQLITE_HASH_STRING && keyClass<=SQLITE_HASH_BINARY );
  pNew->keyClass = keyClass;
#if 0
  if( keyClass==SQLITE_HASH_POINTER || keyClass==SQLITE_HASH_INT ) copyKey = 0;
#endif
  pNew->copyKey = copyKey;
  pNew->first = 0;
  pNew->count = 0;
  pNew->htsize = 0;
  pNew->ht = 0;
}
/*
 * Clear the hash table, free all elements and any allocated buckets.
 * After this call the hash table is in an empty state.
 */
void sqlite3HashClear(Hash *pH){
  HashElem *elem;

  assert( pH!=0 );
  elem = pH->first;
  pH->first = 0;
  if( pH->ht ) sqliteFree(pH->ht);
  pH->ht = 0;
  pH->htsize = 0;
  while( elem ){
    HashElem *next_elem = elem->next;
    if( pH->copyKey && elem->pKey ){
      sqliteFree(elem->pKey);
    }
    sqliteFree(elem);
    elem = next_elem;
  }
  pH->count = 0;
}


/* Compute a case-insensitive hash for a string key. If nKey<=0
 * the length is taken via strlen(). The returned value is non-negative.
 */
static int strHash(const void *pKey, int nKey){
  const char *z = (const char *)pKey;
  int h = 0;
  if( nKey<=0 ) nKey = strlen(z);
  while( nKey > 0  ){
    h = (h<<3) ^ h ^ sqlite3UpperToLower[(unsigned char)*z++];
    nKey--;
  }
  return h & 0x7fffffff;
}

/* Compare two string keys case-insensitively; return 0 if equal. The
 * function assumes both lengths are equal (n1==n2) and otherwise returns
 * non-zero.
 */
static int strCompare(const void *pKey1, int n1, const void *pKey2, int n2){
  if( n1!=n2 ) return 1;
  return sqlite3StrNICmp((const char*)pKey1,(const char*)pKey2,n1);

}
/* Compute a simple rolling hash for a binary key of length nKey.
 * Returns a non-negative integer.
 */
static int binHash(const void *pKey, int nKey){
  int h = 0;
  const char *z = (const char *)pKey;
  while( nKey-- > 0 ){
    h = (h<<3) ^ h ^ *(z++);
  }
  return h & 0x7fffffff;
}
/* Compare two binary keys of equal length using memcmp. Return 0 if equal. */
static int binCompare(const void *pKey1, int n1, const void *pKey2, int n2){
  if( n1!=n2 ) return 1;
  return memcmp(pKey1,pKey2,n1);
}

/* Return a pointer to the hash function for the given key class. */
static int (*hashFunction(int keyClass))(const void*,int){
  if( keyClass==SQLITE_HASH_STRING ){
    return &strHash;
  }else{
    assert( keyClass==SQLITE_HASH_BINARY );
    return &binHash;
  }
}

/* Return a pointer to the comparison function for the given key class. */
static int (*compareFunction(int keyClass))(const void*,int,const void*,int){
  if( keyClass==SQLITE_HASH_STRING ){
    return &strCompare;
  }else{
    assert( keyClass==SQLITE_HASH_BINARY );
    return &binCompare;
  }
}

/* Insert pNew into the bucket entry pEntry and update the
 * doubly-linked list (pH->first) bookkeeping.
 */
static void insertElement(
  Hash *pH,
  struct _ht *pEntry,
  HashElem *pNew
){
  HashElem *pHead;
  pHead = pEntry->chain;
  if( pHead ){
    pNew->next = pHead;
    pNew->prev = pHead->prev;
    if( pHead->prev ){ pHead->prev->next = pNew; }
    else             { pH->first = pNew; }
    pHead->prev = pNew;
  }else{
    pNew->next = pH->first;
    if( pH->first ){ pH->first->prev = pNew; }
    pNew->prev = 0;
    pH->first = pNew;
  }
  pEntry->count++;
  pEntry->chain = pNew;
}


/* Resize the hash table to new_size buckets (must be a power of two).
 * Existing elements are reinserted into the new table. If allocation
 * for the new bucket array fails, the function returns without change.
 */
static void rehash(Hash *pH, int new_size){
  struct _ht *new_ht;
  HashElem *elem, *next_elem;
  int (*xHash)(const void*,int);

  assert( (new_size & (new_size-1))==0 );
  new_ht = (struct _ht *)sqliteMalloc( new_size*sizeof(struct _ht) );
  if( new_ht==0 ) return;
  if( pH->ht ) sqliteFree(pH->ht);
  pH->ht = new_ht;
  pH->htsize = new_size;
  xHash = hashFunction(pH->keyClass);
  for(elem=pH->first, pH->first=0; elem; elem = next_elem){
    int h = (*xHash)(elem->pKey, elem->nKey) & (new_size-1);
    next_elem = elem->next;
    insertElement(pH, &new_ht[h], elem);
  }
}

/* Locate an element that matches pKey/nKey in bucket h. Returns the
 * matching HashElem or NULL if not found.
 */
static HashElem *findElementGivenHash(
  const Hash *pH,
  const void *pKey,
  int nKey,
  int h
){
  HashElem *elem;
  int count;
  int (*xCompare)(const void*,int,const void*,int);

  if( pH->ht ){
    struct _ht *pEntry = &pH->ht[h];
    elem = pEntry->chain;
    count = pEntry->count;
    xCompare = compareFunction(pH->keyClass);
    while( count-- && elem ){
      if( (*xCompare)(elem->pKey,elem->nKey,pKey,nKey)==0 ){
        return elem;
      }
      elem = elem->next;
    }
  }
  return 0;
}

/* Remove the given element from the table (using the supplied bucket
 * index h) and free its memory. If copyKey was set, the stored key is
 * freed as well.
 */
static void removeElementGivenHash(
  Hash *pH,
  HashElem* elem,
  int h
){
  struct _ht *pEntry;
  if( elem->prev ){
    elem->prev->next = elem->next; 
  }else{
    pH->first = elem->next;
  }
  if( elem->next ){
    elem->next->prev = elem->prev;
  }
  pEntry = &pH->ht[h];
  if( pEntry->chain==elem ){
    pEntry->chain = elem->next;
  }
  pEntry->count--;
  if( pEntry->count<=0 ){
    pEntry->chain = 0;
  }
  if( pH->copyKey && elem->pKey ){
    sqliteFree(elem->pKey);
  }
  sqliteFree( elem );
  pH->count--;
  if( pH->count<=0 ){
    assert( pH->first==0 );
    assert( pH->count==0 );
    sqlite3HashClear(pH);
  }
}

/* Lookup the element associated with pKey/nKey and return its data
 * pointer. Returns NULL if not found or if the table is empty.
 */
void *sqlite3HashFind(const Hash *pH, const void *pKey, int nKey){
  int h;
  HashElem *elem;
  int (*xHash)(const void*,int);

  if( pH==0 || pH->ht==0 ) return 0;
  xHash = hashFunction(pH->keyClass);
  assert( xHash!=0 );
  h = (*xHash)(pKey,nKey);
  assert( (pH->htsize & (pH->htsize-1))==0 );
  elem = findElementGivenHash(pH,pKey,nKey, h & (pH->htsize-1));
  return elem ? elem->data : 0;
}

/* Insert or replace an element with key pKey/nKey and associated
 * data pointer. If data==NULL the matching element is removed.
 * Returns the old data pointer when replacing/removing, or NULL on
 * successful insertion. If memory allocation fails the provided
 * "data" pointer is returned (table left unchanged).
 */
void *sqlite3HashInsert(Hash *pH, const void *pKey, int nKey, void *data){
  int hraw;
  int h;
  HashElem *elem;
  HashElem *new_elem;
  int (*xHash)(const void*,int);

  assert( pH!=0 );
  xHash = hashFunction(pH->keyClass);
  assert( xHash!=0 );
  hraw = (*xHash)(pKey, nKey);
  assert( (pH->htsize & (pH->htsize-1))==0 );
  h = hraw & (pH->htsize-1);
  elem = findElementGivenHash(pH,pKey,nKey,h);
  if( elem ){
    void *old_data = elem->data;
    if( data==0 ){
      removeElementGivenHash(pH,elem,h);
    }else{
      elem->data = data;
    }
    return old_data;
  }
  if( data==0 ) return 0;
  new_elem = (HashElem*)sqliteMalloc( sizeof(HashElem) );
  if( new_elem==0 ) return data;
  if( pH->copyKey && pKey!=0 ){
    new_elem->pKey = sqliteMallocRaw( nKey );
    if( new_elem->pKey==0 ){
      sqliteFree(new_elem);
      return data;
    }
    memcpy((void*)new_elem->pKey, pKey, nKey);
  }else{
    new_elem->pKey = (void*)pKey;
  }
  new_elem->nKey = nKey;
  pH->count++;
  if( pH->htsize==0 ){
    rehash(pH,8);
    if( pH->htsize==0 ){
      pH->count = 0;
      sqliteFree(new_elem);
      return data;
    }
  }
  if( pH->count > pH->htsize ){
    rehash(pH,pH->htsize*2);
  }
  assert( pH->htsize>0 );
  assert( (pH->htsize & (pH->htsize-1))==0 );
  h = hraw & (pH->htsize-1);
  insertElement(pH, &pH->ht[h], new_elem);
  new_elem->data = data;
  return 0;
}