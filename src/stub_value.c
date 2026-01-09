/* stub_value.c - dummy stubs to satisfy linker */

typedef struct sqlite3_value sqlite3_value;

/* Always return NULL, since we won't use it */
sqlite3_value *sqlite3ValueNew(void){
    return 0;
}

/* Do nothing */
void sqlite3ValueSetStr(
    sqlite3_value *v,
    const char *z,
    int n,
    void (*xDel)(void*)
){
    (void)v; (void)z; (void)n; (void)xDel;
}

