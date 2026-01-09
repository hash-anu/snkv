#include <stdio.h>
#include <assert.h>
#include <string.h>
#include "helloworld.h"

void test_HelloWorld_Create() {
    HelloWorld* hw = HelloWorld_Create();
    assert(hw != NULL);
    assert(hw->message != NULL);
    assert(strcmp(hw->message, "Hello, World!") == 0);
    HelloWorld_Destroy(hw);
}

void test_HelloWorld_PrintMessage() {
    HelloWorld* hw = HelloWorld_Create();
    HelloWorld_PrintMessage(hw); // This should print "Hello, World!"
    HelloWorld_Destroy(hw);
}

void test_HelloWorld_Destroy() {
    HelloWorld* hw = HelloWorld_Create();
    HelloWorld_Destroy(hw); // Ensure no memory leaks occur
}

int main() {
    test_HelloWorld_Create();
    test_HelloWorld_PrintMessage();
    test_HelloWorld_Destroy();
    printf("All tests passed!\n");
    return 0;
}
