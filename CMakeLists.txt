cmake_minimum_required(VERSION 3.15)
project(snkv C)

# --------------------------------------------------
# Compiler settings
# --------------------------------------------------
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

if (MSVC)
    add_compile_options(/W3)
else()
    add_compile_options(-g -Wall)
endif()

include_directories(include)

# --------------------------------------------------
# Platform detection
# --------------------------------------------------
set(SNKV_LIBS "")

if (WIN32)
    list(APPEND SNKV_LIBS ws2_32)
elseif (APPLE)
    list(APPEND SNKV_LIBS pthread m)
endif()

# --------------------------------------------------
# SQLite core sources
# --------------------------------------------------
set(SQLITE_CORE
    src/btree.c
    src/btmutex.c
    src/pager.c
    src/pcache.c
    src/pcache1.c
    src/wal.c
    src/memjournal.c
    src/bitvec.c
    src/os.c
    src/os_unix.c
    src/os_win.c
    src/os_kv.c
    src/mutex.c
    src/mutex_noop.c
    src/mutex_unix.c
    src/mutex_w32.c
    src/malloc.c
    src/status.c
    src/global.c
    src/hash.c
    src/util.c
    src/printf.c
    src/random.c
    src/threads.c
    src/fault.c
    src/mem1.c
    src/rowset.c
)

# --------------------------------------------------
# SNKV library
# --------------------------------------------------
set(LIB_SRC
    src/kvstore.c
    src/kvstore_mutex.c
    ${SQLITE_CORE}
)

add_library(snkv STATIC ${LIB_SRC})
target_link_libraries(snkv PRIVATE ${SNKV_LIBS})

# --------------------------------------------------
# Examples
# --------------------------------------------------
file(GLOB EXAMPLE_SRC examples/*.c)

foreach(example ${EXAMPLE_SRC})
    get_filename_component(example_name ${example} NAME_WE)
    add_executable(${example_name} ${example})
    target_link_libraries(${example_name} PRIVATE snkv ${SNKV_LIBS})
endforeach()

# --------------------------------------------------
# Tests
# --------------------------------------------------
enable_testing()

set(TEST_SRC
    tests/test_prod.c
    tests/test_columnfamily.c
    tests/test_benchmark.c
    tests/test_acid.c
    tests/test_mutex_journal.c
    tests/test_json.c
    tests/test_wal.c
    tests/test_stress.c
    tests/test_prefix.c
)

foreach(test ${TEST_SRC})
    get_filename_component(test_name ${test} NAME_WE)
    add_executable(${test_name} ${test})
    target_link_libraries(${test_name} PRIVATE snkv ${SNKV_LIBS})
    add_test(NAME ${test_name} COMMAND ${test_name})
endforeach()
