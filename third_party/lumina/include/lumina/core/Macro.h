/*
 * Copyright 2025-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
