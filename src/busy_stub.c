int sqlite3InvokeBusyHandler(void *p, int count){
  (void)p;
  (void)count;
  return 0; /* never retry */
}

