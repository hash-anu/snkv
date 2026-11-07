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
  LDFLAGS = -lpthread
endif
ifeq ($(UNAME_S),Darwin)
  LDFLAGS = -lpthread
endif
ifeq ($(UNAME_S),Windows)
  # Native Windows (MSVC / Git-Bash without uname) — no winpthreads
  LDFLAGS =
  TARGET_EXT = .exe
endif
# MSYS2 / MinGW: uname reports MINGW64_NT-... or MSYS_NT-...
# These toolchains ship winpthreads, so -lpthread is available.
ifneq (,$(findstring MINGW,$(UNAME_S)))
  LDFLAGS = -lpthread
  TARGET_EXT = .exe
endif
ifneq (,$(findstring MSYS,$(UNAME_S)))
  LDFLAGS = -lpthread
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
           tests/test_enc.c \
           tests/test_regressions.c \
           tests/test_fault_inject.c \
           tests/test_new_gaps.c \
           tests/test_shared_handle.c \
           tests/test_threads.c \
	   tests/test_review_fixes.c \
           tests/test_large_values.c \
           tests/test_oom.c
TEST_BIN := $(TEST_SRC:.c=$(TARGET_EXT))

# test_multiprocess uses fork() — POSIX only (Linux / macOS)
ifneq (,$(filter Linux Darwin,$(UNAME_S)))
  TEST_SRC += tests/test_multiprocess.c
  TEST_BIN += tests/test_multiprocess$(TARGET_EXT)
endif

# ---- Example files ----
EXAMPLE_SRC = $(filter-out examples/vector.c,$(wildcard examples/*.c))
EXAMPLE_BIN = $(EXAMPLE_SRC:.c=$(TARGET_EXT))

all: $(LIB)

# ---- Single-header amalgamation ----
snkv.h: include/*.h src/*.c scripts/gen_snkv_header.sh
	@$(if $(V),,echo "  GEN     $@";)
	$(if $(V),,@)sh scripts/gen_snkv_header.sh > $@

$(LIB): $(LIB_OBJ)
	ar rcs $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

# ---- Example targets (header-only via snkv.h) ----
examples: snkv.h $(EXAMPLE_BIN)

# Copy the header once per run, not once per example: on a Windows drive
# (WSL /mnt/c) an editor or virus scanner may briefly hold the file open,
# and each extra overwrite is another chance to fail with "Permission denied".
examples/snkv.h: snkv.h
	@cp snkv.h $@

# Short build lines; `make V=1 examples` shows the full compiler command.
examples/%$(TARGET_EXT): examples/%.c examples/snkv.h
	@$(if $(V),,echo "  CC      $@";)
	$(if $(V),,@)$(CC) -g -Wall -Iexamples -o $@ $< $(LDFLAGS)

run-examples: examples
	@for e in $(EXAMPLE_BIN); do \
	  echo "=== Running $$e ==="; \
	  ./$$e || exit 1; \
	  echo; \
	done

# ---- Vector example (requires g++ runtime — built separately from snkv.h examples) ----
VEC_OBJ         = src/kvstore_vec.o src/usearch/usearch_impl.o
VLIB            = libsnkv_vec.a
VEC_EXAMPLE_BIN = examples/vector$(TARGET_EXT)

vector-examples: $(VLIB) $(VEC_EXAMPLE_BIN)
	@echo "=== Running $(VEC_EXAMPLE_BIN) ==="; \
	./$(VEC_EXAMPLE_BIN) || exit 1

examples/vector.o: examples/vector.c
	$(CC) $(VEC_CFLAGS) -c -o $@ $<

$(VEC_EXAMPLE_BIN): examples/vector.o $(LIB_OBJ) $(VEC_OBJ)
	$(CXX) -o $@ $< $(LIB_OBJ) $(VEC_OBJ) $(LDFLAGS)

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
	$(CXX) -o $@ $< $(LIB_OBJ) $(VEC_OBJ) $(LDFLAGS)

# ============================================================

# ============================================================
# ---- Sanitizer & coverage targets (Linux / macOS) ----------
# ============================================================
ASAN_CFLAGS  = $(BASE_CFLAGS) -g -O1 -fsanitize=address,undefined -fno-omit-frame-pointer
UBSAN_CFLAGS = $(BASE_CFLAGS) -g -O1 -fsanitize=undefined -fno-omit-frame-pointer
TSAN_CFLAGS  = $(BASE_CFLAGS) -g -O1 -fsanitize=thread -fno-omit-frame-pointer
COV_CFLAGS   = $(BASE_CFLAGS) -g -O0 --coverage
LCOV_OUT     = coverage-report

test-asan:
	$(MAKE) clean
	$(MAKE) CFLAGS="$(ASAN_CFLAGS)" LDFLAGS="$(LDFLAGS) -fsanitize=address,undefined" test

test-ubsan:
	$(MAKE) clean
	$(MAKE) CFLAGS="$(UBSAN_CFLAGS)" LDFLAGS="$(LDFLAGS) -fsanitize=undefined" test

test-tsan:
	$(MAKE) clean
	TSAN_OPTIONS="suppressions=$(CURDIR)/tests/tsan.supp $(TSAN_OPTIONS)" \
	  $(MAKE) CFLAGS="$(TSAN_CFLAGS)" LDFLAGS="$(LDFLAGS) -fsanitize=thread" test

# Simulated power loss at every write/sync (tests/test_powerloss.c).
# PL_STRIDE=n tests every n-th crash point; PL_SEEDS=n random tear seeds.
# Built with its own kvstore.c at the minimum Argon2 cost: the encrypted runs
# open the store thousands of times, and crash safety does not depend on it.
PL_LIB_OBJ = $(filter-out src/kvstore.o,$(LIB_OBJ))
tests/test_powerloss$(TARGET_EXT): tests/test_powerloss.c src/kvstore.c $(PL_LIB_OBJ)
	$(CC) $(CFLAGS) -DSNKV_ENC_M_COST=8 -DSNKV_ENC_T_COST=1 -o $@ \
	  tests/test_powerloss.c src/kvstore.c $(PL_LIB_OBJ) $(LDFLAGS)

test-powerloss: tests/test_powerloss$(TARGET_EXT)
	./tests/test_powerloss$(TARGET_EXT)
	rm -f pl.db pl.db-wal pl.db-shm pl.db-journal

# ---- Fuzzing (Linux / macOS, needs clang with libFuzzer) ----
# make fuzz                 run each fuzzer for FUZZ_TIME seconds (default 60)
# make fuzz FUZZ_TIME=3600  longer run
# Crashes are saved in fuzz/artifacts/; the corpus grows in fuzz/corpus/.
# Reproduce one: ./fuzz/fuzz_api fuzz/artifacts/crash-<hash>
FUZZ_CC     ?= clang
FUZZ_TIME   ?= 60
FUZZ_DB_DIR ?= fuzz/build
# The *_MAX limits cap the Argon2 cost a damaged file may ask for (default
# 1 GiB, 64 passes): without them one input can spend the whole memory or
# time limit on key derivation, which is expected behaviour, not a bug.
FUZZ_CFLAGS  = -g -O1 -Iinclude -DSNKV_ENC_M_COST=8 -DSNKV_ENC_T_COST=1 \
               -DSNKV_ENC_M_COST_MAX=1024 -DSNKV_ENC_T_COST_MAX=4 \
               -fsanitize=address,undefined -fno-sanitize-recover=undefined \
               -fno-omit-frame-pointer
FUZZ_OBJ     = $(patsubst %.c,fuzz/build/%.o,$(LIB_SRC))
FUZZ_RUN     = -max_total_time=$(FUZZ_TIME) -timeout=20 -rss_limit_mb=2048 \
               -artifact_prefix=fuzz/artifacts/

fuzz/build/%.o: %.c
	@mkdir -p $(dir $@)
	$(FUZZ_CC) $(FUZZ_CFLAGS) -fsanitize=fuzzer-no-link -c -o $@ $<

fuzz/fuzz_api fuzz/fuzz_open: fuzz/%: fuzz/%.c $(FUZZ_OBJ)
	$(FUZZ_CC) $(FUZZ_CFLAGS) -fsanitize=fuzzer -o $@ $< $(FUZZ_OBJ) -lpthread

fuzz/gen_corpus: fuzz/gen_corpus.c $(FUZZ_OBJ)
	$(FUZZ_CC) $(FUZZ_CFLAGS) -fsanitize=fuzzer-no-link -o $@ $< $(FUZZ_OBJ) -lpthread

fuzz-corpus: fuzz/gen_corpus
	@mkdir -p fuzz/corpus/api fuzz/corpus/open fuzz/artifacts $(FUZZ_DB_DIR)
	./fuzz/gen_corpus fuzz/corpus/open

fuzz-api: fuzz/fuzz_api fuzz-corpus
	FUZZ_DB_DIR=$(FUZZ_DB_DIR) ./fuzz/fuzz_api $(FUZZ_RUN) fuzz/corpus/api

fuzz-open: fuzz/fuzz_open fuzz-corpus
	FUZZ_DB_DIR=$(FUZZ_DB_DIR) ./fuzz/fuzz_open $(FUZZ_RUN) fuzz/corpus/open

# Both fuzzers; "make -k fuzz" runs the second even if the first finds a crash.
fuzz: fuzz-api fuzz-open

# Replay every input that once found a bug (fuzz/regress/), once each.  Add
# a new crash input here when fixing the bug it found.
# "make -k fuzz-regress" replays both sets even if the first fails.
FUZZ_REGRESS = -runs=0 -timeout=20 -artifact_prefix=fuzz/artifacts/

fuzz-regress-api: fuzz/fuzz_api
	@mkdir -p $(FUZZ_DB_DIR) fuzz/artifacts
	FUZZ_DB_DIR=$(FUZZ_DB_DIR) ./fuzz/fuzz_api $(FUZZ_REGRESS) fuzz/regress/api

fuzz-regress-open: fuzz/fuzz_open
	@mkdir -p $(FUZZ_DB_DIR) fuzz/artifacts
	FUZZ_DB_DIR=$(FUZZ_DB_DIR) ./fuzz/fuzz_open $(FUZZ_REGRESS) fuzz/regress/open

fuzz-regress: fuzz-regress-api fuzz-regress-open

test-coverage:
	$(MAKE) clean
	$(MAKE) CFLAGS="$(COV_CFLAGS)" test
	$(MAKE) CFLAGS="$(COV_CFLAGS)" LDFLAGS="$(LDFLAGS) --coverage" test-vector
	lcov --capture --directory . --output-file $(LCOV_OUT).info --ignore-errors mismatch,negative
	genhtml $(LCOV_OUT).info --output-directory $(LCOV_OUT)/html
	@echo "HTML report: $(LCOV_OUT)/html/index.html"

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
	rm -f tests/test_powerloss$(TARGET_EXT)
	rm -rf fuzz/build
	rm -f fuzz/fuzz_api fuzz/fuzz_open fuzz/gen_corpus

# ---- 10 GB kill-9 crash safety test (run manually, not part of 'make test') ----
# Requires ~11 GB free disk and takes 15-60 minutes depending on storage speed.
# POSIX only (Linux / macOS) for 'run' mode; 'write' and 'verify' are cross-platform.
tests/test_crash_10gb$(TARGET_EXT): tests/test_crash_10gb.c $(LIB_OBJ)
	$(CC) $(CFLAGS) -o $@ $< $(LIB_OBJ) $(LDFLAGS)

test-crash-10gb: tests/test_crash_10gb$(TARGET_EXT)
	./tests/test_crash_10gb$(TARGET_EXT) run tests/crash_10gb.db

.PHONY: all clean tests test examples run-examples snkv.h test-crash-10gb \
        vector test-vector vector-examples run-vector-examples \
        test-asan test-ubsan test-tsan test-coverage test-powerloss \
        fuzz fuzz-api fuzz-open fuzz-corpus \
        fuzz-regress fuzz-regress-api fuzz-regress-open
