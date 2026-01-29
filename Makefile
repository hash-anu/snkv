# ...existing code...
CC = gcc
CFLAGS = -g -Wall -Iinclude -DKVSTORE_ENABLE_STATS=0 \
    -DKVSTORE_ENABLE_VALIDATION=0 \
    -DKVSTORE_ENABLE_CORRUPTION_CHECK=0
SRC = src/main.c src/kvstore.c src/os.c src/os_unix.c src/os_win.c src/util.c src/printf.c src/random.c src/hash.c src/pager.c   src/btree.c   src/busy_stub.c src/stub_value.c src/main.c
OBJ = $(SRC:.c=.o)
TARGET = snkv

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

tests/test_prod: tests/test_prod.c src/kvstore.c src/os.c src/os_unix.c src/os_win.c src/util.c src/printf.c src/random.c src/hash.c src/pager.c   src/btree.c   src/busy_stub.c src/stub_value.c
	$(CC) $(CFLAGS) -o $@ $^

test_prod: tests/test_prod

tests/test_columnfamily: tests/test_columnfamily.c src/kvstore.c src/os.c src/os_unix.c src/os_win.c src/util.c src/printf.c src/random.c src/hash.c src/pager.c   src/btree.c   src/busy_stub.c src/stub_value.c
	$(CC) $(CFLAGS) -o $@ $^

test_cf: tests/test_columnfamily

tests/test_benchmark: tests/test_benchmark.c src/kvstore.c src/os.c src/os_unix.c src/os_win.c src/util.c src/printf.c src/random.c src/hash.c src/pager.c   src/btree.c   src/busy_stub.c src/stub_value.c
	$(CC) $(CFLAGS) -o $@ $^

test_benchmark: tests/test_benchmark

clean:
	rm -f $(OBJ) $(TARGET) tests/test_columnfamily tests/test_prod tests/test_benchmark
