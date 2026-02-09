/*
** platform_compat.h
** Cross-platform compatibility layer for Mac/Linux/Windows
** Include this at the top of test files to ensure portability
*/

#ifndef PLATFORM_COMPAT_H
#define PLATFORM_COMPAT_H

#include <time.h>

/* ===================================================================
** Windows Platform Detection and Compatibility
** =================================================================== */
#ifdef _WIN32
  #define PLATFORM_WINDOWS 1
  #include <windows.h>
  #include <process.h>
  /* Windows doesn't have unistd.h - provide alternatives */
  #define unlink _unlink
  #define snprintf _snprintf
  
  /* Sleep/usleep compatibility */
  #define sleep(x) Sleep((x)*1000)
  #define usleep(x) Sleep((x)/1000)
  
  /* stat compatibility */
  #include <sys/stat.h>
  #define stat _stat
  
  /* ===================================================================
  ** Windows Threading (pthread compatibility layer)
  ** =================================================================== */
  typedef HANDLE pthread_t;
  typedef struct { int dummy; } pthread_attr_t;
  
  static inline int pthread_create(pthread_t *thread, void *attr, 
                                    void *(*start_routine)(void*), void *arg) {
    (void)attr; /* unused */
    *thread = (HANDLE)_beginthread((void(*)(void*))start_routine, 0, arg);
    return (*thread == (HANDLE)-1L) ? -1 : 0;
  }
  
  static inline int pthread_join(pthread_t thread, void **retval) {
    (void)retval; /* unused */
    WaitForSingleObject(thread, INFINITE);
    CloseHandle(thread);
    return 0;
  }
  
  /* ===================================================================
  ** Windows Timing (gettimeofday compatibility)
  ** =================================================================== */
  
  static inline int gettimeofday(struct timeval *tv, void *tz) {
    (void)tz; /* unused */
    FILETIME ft;
    ULARGE_INTEGER ui;
    GetSystemTimeAsFileTime(&ft);
    ui.LowPart = ft.dwLowDateTime;
    ui.HighPart = ft.dwHighDateTime;
    /* Convert to microseconds since Unix epoch */
    unsigned long long t = ui.QuadPart / 10 - 11644473600000000ULL;
    tv->tv_sec = (long)(t / 1000000ULL);
    tv->tv_usec = (long)(t % 1000000ULL);
    return 0;
  }
  
  /* ===================================================================
  ** Windows clock_gettime compatibility
  ** =================================================================== */
  #ifndef CLOCK_MONOTONIC
  #define CLOCK_MONOTONIC 1
  #endif
  
  static inline int clock_gettime(int clk_id, struct timespec *ts) {
    (void)clk_id; /* unused */
    LARGE_INTEGER freq, count;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&count);
    ts->tv_sec = (long)(count.QuadPart / freq.QuadPart);
    ts->tv_nsec = (long)((count.QuadPart % freq.QuadPart) * 1000000000LL / freq.QuadPart);
    return 0;
  }

/* ===================================================================
** POSIX Platforms (Mac/Linux)
** =================================================================== */
#else
  #define PLATFORM_WINDOWS 0
  #include <pthread.h>
  #include <unistd.h>
  #include <sys/time.h>
  #include <sys/stat.h>
#endif

/* ===================================================================
** Console Colors - Enable VT100 on Windows 10+
** =================================================================== */
static inline void enable_ansi_colors(void) {
#ifdef _WIN32
  HANDLE hOut = GetStdHandle(STD_OUTPUT_HANDLE);
  DWORD dwMode = 0;
  GetConsoleMode(hOut, &dwMode);
  SetConsoleMode(hOut, dwMode | 0x0004); /* ENABLE_VIRTUAL_TERMINAL_PROCESSING */
#endif
}

#endif /* PLATFORM_COMPAT_H */
