#if defined(_MSC_VER) && !defined(__clang__)
#define LUMINA_CXX_VER _MSVC_LANG
#else
#define LUMINA_CXX_VER __cplusplus
#endif

#ifdef __clang__
#define LUMINA_LIFETIME_BOUND [[clang::lifetimebound]]
#else
#define LUMINA_LIFETIME_BOUND
#endif

#if defined(__arm__) || defined(__aarch64__)
// ARMv7 Architecture Reference Manual (for YIELD)
// ARM Compiler toolchain Compiler Reference (for __yield() instrinsic)
#if defined(__CC_ARM)
#define LUMINA_YIELD() __yield()
#else
#define LUMINA_YIELD() __asm__ __volatile__("yield")
#endif // __CC_ARM
#elif defined(__SSE2__)
#define LUMINA_YIELD() _mm_pause()
#else
#define LUMINA_YIELD() ((void)0)
#endif // __arm__ || __aarch64__
