CC  ?= gcc
CXX ?= g++

# ---- Build mode ----
BUILD ?= debug

BASE_CFLAGS = -Wall -Iinclude

ifeq ($(BUILD),debug)
  CFLAGS = $(BASE_CFLAGS) -g -O0 -DDEBUG
endif

ifeq ($(BUILD),release)
  CFLAGS = $(BASE_CFLAGS) -O2 -DNDEBUG
endif

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
  LDFLAGS =
  TARGET_EXT = .exe
endif
# Fallback for MSYS/Cygwin reporting MINGW/MSYS
ifneq (,$(findstring MINGW,$(UNAME_S)))
  LDFLAGS =
  TARGET_EXT = .exe
endif
ifneq (,$(findstring MSYS,$(UNAME_S)))
  LDFLAGS =
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
              src/fault.c src/mem1.c

# Library objects
LIB_SRC = src/kvstore.c src/monocypher/monocypher.c $(SQLITE_CORE)
LIB_OBJ = $(LIB_SRC:.c=.o)

# Static library
LIB = libsnkv.a

# ---- Test files ----
TEST_SRC = tests/test_prod.c tests/test_columnfamily.c tests/test_benchmark.c \
           tests/test_acid.c tests/test_mutex_journal.c tests/test_json.c \
           tests/test_wal.c tests/test_stress.c tests/test_prefix.c \
           tests/test_concurrent.c \
           tests/test_crash_recovery.c \
           tests/test_autovacuum.c \
           tests/test_config.c \
           tests/test_checkpoint.c \
           tests/test_ttl.c \
           tests/test_iterator_reverse.c \
           tests/test_new_apis.c \
           tests/test_enc.c
TEST_BIN = $(TEST_SRC:.c=$(TARGET_EXT))

# ---- Example files ----
EXAMPLE_SRC = $(wildcard examples/*.c)
EXAMPLE_BIN = $(EXAMPLE_SRC:.c=$(TARGET_EXT))

all: $(LIB)

# ---- Single-header amalgamation ----
snkv.h: include/*.h src/*.c scripts/gen_snkv_header.sh
	sh scripts/gen_snkv_header.sh > $@

$(LIB): $(LIB_OBJ)
	ar rcs $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

# ---- Example targets (header-only via snkv.h) ----
examples: snkv.h $(EXAMPLE_BIN)

examples/%$(TARGET_EXT): examples/%.c snkv.h
	cp snkv.h examples/snkv.h
	$(CC) -g -Wall -Iexamples -o $@ $< $(LDFLAGS)

run-examples: examples
	@for e in $(EXAMPLE_BIN); do \
	  echo "=== Running $$e ==="; \
	  ./$$e || exit 1; \
	  echo; \
	done

# ---- Vector example (requires g++ runtime — built separately from snkv.h examples) ----
VEC_EXAMPLE_BIN = examples/vector$(TARGET_EXT)

vector-examples: $(VLIB) $(VEC_EXAMPLE_BIN)

examples/vector.o: examples/vector.c
	$(CC) $(VEC_CFLAGS) -c -o $@ $<

$(VEC_EXAMPLE_BIN): examples/vector.o $(LIB_OBJ) $(VEC_OBJ)
	$(CXX) -o $@ $< $(LIB_OBJ) $(VEC_OBJ) $(LDFLAGS) -lm

run-vector-examples: vector-examples
	@echo "=== Running $(VEC_EXAMPLE_BIN) ==="; \
	./$(VEC_EXAMPLE_BIN) || exit 1

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
	rm -f *.db *.db-wal *.db-shm

# ============================================================
# ---- Vector support (requires g++ / C++17) ----
# Build with:   make vector
# Test with:    make test-vector
# ============================================================

# Vector-specific flags
VEC_CFLAGS   = $(CFLAGS) -Isrc/usearch -Isrc

ifeq ($(BUILD),debug)
  VEC_CXXFLAGS = -Wall -std=c++17 -g -O0 -DDEBUG \
                 -Iinclude -Isrc -Isrc/usearch -Isrc/usearch/include \
                 -DUSEARCH_USE_FP16LIB=0
endif
ifeq ($(BUILD),release)
  VEC_CXXFLAGS = -Wall -std=c++17 -O2 -DNDEBUG \
                 -Iinclude -Isrc -Isrc/usearch -Isrc/usearch/include \
                 -DUSEARCH_USE_FP16LIB=0
endif

# Vector object files (built separately from core LIB_OBJ)
VEC_OBJ = src/kvstore_vec.o src/usearch/usearch_impl.o

# Combined static library: core + vector
VLIB = libsnkv_vec.a

# Vector test binary
VEC_TEST_BIN = tests/test_vec$(TARGET_EXT)

# Build the combined vector library
vector: $(VLIB)

$(VLIB): $(LIB_OBJ) $(VEC_OBJ)
	ar rcs $@ $^

# kvstore_vec.c is plain C — compiled with gcc, extra include path for usearch.h
src/kvstore_vec.o: src/kvstore_vec.c
	$(CC) $(VEC_CFLAGS) -c -o $@ $<

# usearch HNSW wrapper — compiled with g++
src/usearch/usearch_impl.o: src/usearch/usearch_impl.cpp
	$(CXX) $(VEC_CXXFLAGS) -c -o $@ $<

# Build and run vector tests
test-vector: $(VEC_TEST_BIN)
	@echo "=== Running $(VEC_TEST_BIN) ==="; \
	./$(VEC_TEST_BIN) || exit 1
	rm -f tests/*.db tests/*.db-wal tests/*.db-shm

# Link vector test: C file compiled with gcc, linked with g++ for C++ runtime
tests/test_vec.o: tests/test_vec.c
	$(CC) $(VEC_CFLAGS) -c -o $@ $<

$(VEC_TEST_BIN): tests/test_vec.o $(LIB_OBJ) $(VEC_OBJ)
	$(CXX) -o $@ $< $(LIB_OBJ) $(VEC_OBJ) $(LDFLAGS) -lm

# ============================================================

clean:
	rm -f $(LIB_OBJ) $(LIB) $(TEST_BIN) $(EXAMPLE_BIN) tests/*.o
	rm -f $(VEC_OBJ) $(VLIB) $(VEC_TEST_BIN) $(VEC_EXAMPLE_BIN) examples/vector.o
	rm -f snkv.h
	rm -f *.db *.db-wal *.db-shm
	rm -f tests/*.db tests/*.db-wal tests/*.db-shm
	rm -f examples/*.db examples/*.db-wal examples/*.db-shm examples/snkv.h
	rm -f tests/test_crash_10gb$(TARGET_EXT)
	rm -f tests/crash_10gb.db tests/crash_10gb.db-wal tests/crash_10gb.db-shm

# ---- 10 GB kill-9 crash safety test (run manually, not part of 'make test') ----
# Requires ~11 GB free disk and takes 15-60 minutes depending on storage speed.
# POSIX only (Linux / macOS) for 'run' mode; 'write' and 'verify' are cross-platform.
tests/test_crash_10gb$(TARGET_EXT): tests/test_crash_10gb.c $(LIB_OBJ)
	$(CC) $(CFLAGS) -o $@ $< $(LIB_OBJ) $(LDFLAGS)

test-crash-10gb: tests/test_crash_10gb$(TARGET_EXT)
	./tests/test_crash_10gb$(TARGET_EXT) run tests/crash_10gb.db

.PHONY: all clean tests test examples run-examples snkv.h test-crash-10gb \
        vector test-vector vector-examples run-vector-examples
