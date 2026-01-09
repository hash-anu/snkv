#ifndef HELLOWORLD_H
#define HELLOWORLD_H

typedef struct HelloWorld {
    char *message;
} HelloWorld;

HelloWorld* HelloWorld_Create(void);
const char* HelloWorld_GetMessage(const HelloWorld* hw);
void HelloWorld_PrintMessage(const HelloWorld* hw);
void HelloWorld_Destroy(HelloWorld* hw);

#endif // HELLOWORLD_H