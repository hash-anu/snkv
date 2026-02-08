CC ?= gcc
CFLAGS = -g -Wall -Iinclude

# ---- Platform detection ----
UNAME_S := $(shell uname -s 2>/dev/null || echo Windows)

ifeq ($(UNAME_S),Linux)
  LDFLAGS = 
endif
ifeq ($(UNAME_S),Darwin)
  LDFLAGS = -lpthread -lm
endif
ifeq ($(UNAME_S),Windows)
  # Native Windows / MSYS2 / MinGW
  LDFLAGS = -lws2_32
  TARGET_EXT = .exe
endif
# Fallback for MSYS/Cygwin reporting MINGW/MSYS
ifneq (,$(findstring MINGW,$(UNAME_S)))
  LDFLAGS = -lws2_32
  TARGET_EXT = .exe
endif
ifneq (,$(findstring MSYS,$(UNAME_S)))
  LDFLAGS = -lws2_32
  TARGET_EXT = .exe
endif

TARGET_EXT ?=
LDFLAGS ?=

# ---- Source files ----
# Core SQLite btree -> pager -> os layer.
# Platform-specific files (os_unix.c, os_win.c, mutex_unix.c, mutex_w32.c)
# are guarded by #if SQLITE_OS_UNIX / SQLITE_OS_WIN internally,
# so they compile to empty on the wrong platform.
SQLITE_CORE = src/btree.c src/btmutex.c \
              src/pager.c src/pcache.c src/pcache1.c \
              src/wal.c src/memjournal.c src/bitvec.c \
              src/os.c src/os_unix.c src/os_win.c src/os_kv.c \
              src/mutex.c src/mutex_noop.c src/mutex_unix.c src/mutex_w32.c \
              src/malloc.c src/status.c src/global.c \
              src/hash.c src/util.c src/printf.c src/random.c \
              src/threads.c \
              src/fault.c src/mem1.c src/rowset.c \
              src/sqlite_stubs.c

# Library objects (everything except main.c)
LIB_SRC = src/kvstore.c src/kvstore_mutex.c $(SQLITE_CORE)
LIB_OBJ = $(LIB_SRC:.c=.o)

SRC = src/main.c $(LIB_SRC)
OBJ = $(SRC:.c=.o)
TARGET = snkv$(TARGET_EXT)

# ---- Test files ----
TEST_SRC = tests/test_prod.c tests/test_columnfamily.c tests/test_benchmark.c \
           tests/test_acid.c tests/test_mutex_journal.c tests/test_json.c \
           tests/test_wal.c tests/test_stress.c
TEST_BIN = $(TEST_SRC:.c=$(TARGET_EXT))

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

# ---- Test targets ----
tests: $(TEST_BIN)

tests/%$(TARGET_EXT): tests/%.c $(LIB_OBJ)
	$(CC) $(CFLAGS) -o $@ $< $(LIB_OBJ) $(LDFLAGS)

test: tests
	@for t in $(TEST_BIN); do \
	  echo "=== Running $$t ==="; \
	  ./$$t || exit 1; \
	  echo; \
	done

clean:
	rm -f $(OBJ) $(TARGET) $(TEST_BIN) tests/*.o
	rm -f *.db tests/*.db

.PHONY: all clean tests test
