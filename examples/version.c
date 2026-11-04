/* SPDX-License-Identifier: Apache-2.0 */
/*
** Library Version
** Demonstrates: KVSTORE_VERSION / KVSTORE_VERSION_NUMBER (the header you
**               compiled against) and kvstore_libversion /
**               kvstore_libversion_number (the library actually linked)
**
** With the single-header build (snkv.h) both are always the same.  With a
** shared library they can differ, e.g. an app built against 0.9.0 running
** with an older libsnkv: check at startup.
*/

#define SNKV_IMPLEMENTATION
#include "snkv.h"
#include <stdio.h>

/* Compile-time check: refuse to build against a header that is too old. */
#if KVSTORE_VERSION_NUMBER < 9000
#  error "this example needs SNKV 0.9.0 or newer"
#endif

int main(void) {
    printf("=== SNKV version ===\n");
    printf("header  (compile time): %s  number %d\n", KVSTORE_VERSION, KVSTORE_VERSION_NUMBER);
    printf("library (run time):     %s  number %d\n", kvstore_libversion(), kvstore_libversion_number());

    /* NUMBER = MAJOR*1000000 + MINOR*1000 + PATCH: compare as integers. */
    int lib = kvstore_libversion_number();
    printf("major %d, minor %d, patch %d\n", lib / 1000000, (lib / 1000) % 1000, lib % 1000);

    if (lib < KVSTORE_VERSION_NUMBER) {
        printf("warning: linked library is older than the header this app was built with\n");
        return 1;
    }
    printf("\n[OK] version.c example complete.\n");
    return 0;
}
