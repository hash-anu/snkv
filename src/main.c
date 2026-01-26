/*
** Simple test program for KVStore
** Uses kvstore.h APIs only
*/

#include <stdio.h>
#include <string.h>
#include "kvstore.h"

static void die(int rc, const char *msg){
  if( rc!=KVSTORE_OK ){
    printf("ERROR (%d): %s\n", rc, msg);
    exit(1);
  }
}

int main(void){
  KVStore *kv;
  KVIterator *it;
  int rc;
  void *val;
  int vlen;
  int exists;

  printf("Opening KVStore...\n");
  rc = kvstore_open("test.kv", &kv, 0);
  die(rc, "kvstore_open failed");

  /* ---------------- TRANSACTION BEGIN ---------------- */
  rc = kvstore_begin(kv, 1);
  die(rc, "begin write transaction failed");

  printf("Putting values...\n");
  rc = kvstore_put(kv, "apple", 5, "red", 3);
  die(rc, "put apple failed");

  rc = kvstore_put(kv, "banana", 6, "yellow", 6);
  die(rc, "put banana failed");

  rc = kvstore_put(kv, "grape", 5, "purple", 6);
  die(rc, "put grape failed");

  rc = kvstore_commit(kv);
  die(rc, "commit failed");

  /* ---------------- GET ---------------- */
  printf("\nReading values...\n");
  rc = kvstore_get(kv, "banana", 6, &val, &vlen);
  die(rc, "get banana failed");

  printf("banana = %.*s\n", vlen, (char*)val);
  sqliteFree(val);

  /* ---------------- EXISTS ---------------- */
  rc = kvstore_exists(kv, "apple", 5, &exists);
  die(rc, "exists check failed");
  printf("apple exists? %s\n", exists ? "yes" : "no");

  rc = kvstore_exists(kv, "orange", 6, &exists);
  die(rc, "exists check failed");
  printf("orange exists? %s\n", exists ? "yes" : "no");

  /* ---------------- ITERATOR ---------------- */
  printf("\nIterating over KVStore:\n");

  rc = kvstore_iterator_create(kv, &it);
  die(rc, "iterator create failed");

  rc = kvstore_iterator_first(it);
  die(rc, "iterator first failed");

  while( !kvstore_iterator_eof(it) ){
    void *k, *v;
    int klen, vlen;

    kvstore_iterator_key(it, &k, &klen);
    kvstore_iterator_value(it, &v, &vlen);

    printf("  %.*s => %.*s\n",
           klen, (char*)k,
           vlen, (char*)v);

    kvstore_iterator_next(it);
  }

  kvstore_iterator_close(it);

  /* ---------------- DELETE ---------------- */
  printf("\nDeleting key 'apple'...\n");
  rc = kvstore_begin(kv, 1);
  die(rc, "begin write transaction failed");

  rc = kvstore_delete(kv, "apple", 5);
  die(rc, "delete apple failed");

  rc = kvstore_commit(kv);
  die(rc, "commit failed");

  rc = kvstore_exists(kv, "apple", 5, &exists);
  die(rc, "exists failed");
  printf("apple exists after delete? %s\n", exists ? "yes" : "no");

  /* ---------------- CLEANUP ---------------- */
  printf("\nClosing KVStore...\n");
  kvstore_close(kv);

  printf("KVStore test completed successfully.\n");
  return 0;
}
