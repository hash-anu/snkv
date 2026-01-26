# ...existing code...
CC = gcc
CFLAGS = -g -Wall -Iinclude
SRC = src/main.c src/kvstore.c src/os.c src/os_unix.c src/os_win.c src/util.c src/printf.c src/random.c src/hash.c src/pager.c   src/btree.c   src/busy_stub.c src/stub_value.c src/main.c
OBJ = $(SRC:.c=.o)
TARGET = snkv

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

tests/test_helloworld: tests/test_helloworld.c src/helloworld.c
	$(CC) $(CFLAGS) -o $@ $^

test: tests/test_helloworld

clean:
	rm -f $(OBJ) $(TARGET) tests/test_helloworld
