// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Adapted from Apache Arrow
// https://github.com/apache/arrow/blob/main/cpp/src/arrow/util/macros.h

#pragma once

#include <cstdint>

#if defined(__GNUC__) && __GNUC__ >= 4
#define PAIMON_LIKELY(x) (__builtin_expect((x), 1))
#define PAIMON_UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define PAIMON_LIKELY(x) (x)
#define PAIMON_UNLIKELY(x) (x)
#endif

#define MEMORY_BARRIER() __asm__ __volatile__("" ::: "memory")

#define PAIMON_EXPAND(x) x
#define PAIMON_STRINGIFY(x) #x
#define PAIMON_CONCAT(x, y) x##y

// From Google gutil
#ifndef PAIMON_DISALLOW_COPY_AND_ASSIGN
#define PAIMON_DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&) = delete;           \
    void operator=(const TypeName&) = delete
#endif

#ifndef PAIMON_DEFAULT_MOVE_AND_ASSIGN
#define PAIMON_DEFAULT_MOVE_AND_ASSIGN(TypeName) \
    TypeName(TypeName&&) = default;              \
    TypeName& operator=(TypeName&&) = default
#endif

#define PAIMON_UNUSED(x) (void)(x)
#define PAIMON_ARG_UNUSED(x)
//
// GCC can be told that a certain branch is not likely to be taken (for
// instance, a CHECK failure), and use that information in static analysis.
// Giving it this information can help it optimize for the common case in
// the absence of better information (ie. -fprofile-arcs).
//
#if defined(__GNUC__)
#define PAIMON_PREDICT_FALSE(x) (__builtin_expect(!!(x), 0))
#define PAIMON_PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#define PAIMON_NORETURN __attribute__((noreturn))
#define PAIMON_NOINLINE __attribute__((noinline))
#define PAIMON_PREFETCH(addr) __builtin_prefetch(addr)
#elif defined(_MSC_VER)
#define PAIMON_NORETURN __declspec(noreturn)
#define PAIMON_NOINLINE __declspec(noinline)
#define PAIMON_PREDICT_FALSE(x) (x)
#define PAIMON_PREDICT_TRUE(x) (x)
#define PAIMON_PREFETCH(addr)
#else
#define PAIMON_NORETURN
#define PAIMON_PREDICT_FALSE(x) (x)
#define PAIMON_PREDICT_TRUE(x) (x)
#define PAIMON_PREFETCH(addr)
#endif

#if (defined(__GNUC__) || defined(__APPLE__))
#define PAIMON_MUST_USE_RESULT __attribute__((warn_unused_result))
#elif defined(_MSC_VER)
#define PAIMON_MUST_USE_RESULT
#else
#define PAIMON_MUST_USE_RESULT
#endif

#if defined(__clang__)
// Only clang supports warn_unused_result as a type annotation.
#define PAIMON_MUST_USE_TYPE PAIMON_MUST_USE_RESULT
#else
#define PAIMON_MUST_USE_TYPE
#endif

// ----------------------------------------------------------------------

// clang-format off
// [[deprecated]] is only available in C++14, use this for the time being
// This macro takes an optional deprecation message
#ifdef __COVERITY__
#  define PAIMON_DEPRECATED(...)
#  define PAIMON_DEPRECATED_USING(...)
#elif __cplusplus > 201103L
#  define PAIMON_DEPRECATED(...) [[deprecated(__VA_ARGS__)]]
#  define PAIMON_DEPRECATED_USING(...) PAIMON_DEPRECATED(__VA_ARGS__)
#else
# ifdef __GNUC__
#  define PAIMON_DEPRECATED(...) __attribute__((deprecated(__VA_ARGS__)))
#  define PAIMON_DEPRECATED_USING(...) PAIMON_DEPRECATED(__VA_ARGS__)
# elif defined(_MSC_VER)
#  define PAIMON_DEPRECATED(...) __declspec(deprecated(__VA_ARGS__))
#  define PAIMON_DEPRECATED_USING(...)
# else
#  define PAIMON_DEPRECATED(...)
#  define PAIMON_DEPRECATED_USING(...)
# endif
#endif
// clang-format on

// ----------------------------------------------------------------------

// macros to disable padding
// these macros are portable across different compilers and platforms
// [https://github.com/google/flatbuffers/blob/master/include/flatbuffers/flatbuffers.h#L1355]
#if !defined(MANUALLY_ALIGNED_STRUCT)
#if defined(_MSC_VER)
#define MANUALLY_ALIGNED_STRUCT(alignment) \
    __pragma(pack(1));                     \
    struct __declspec(align(alignment))
#define STRUCT_END(name, size) \
    __pragma(pack());          \
    static_assert(sizeof(name) == size, "compiler breaks packing rules")
#elif defined(__GNUC__) || defined(__clang__)
#define MANUALLY_ALIGNED_STRUCT(alignment) \
    _Pragma("pack(1)") struct __attribute__((aligned(alignment)))
#define STRUCT_END(name, size) \
    _Pragma("pack()") static_assert(sizeof(name) == size, "compiler breaks packing rules")
#else
#error Unknown compiler, please define structure alignment macros
#endif
#endif  // !defined(MANUALLY_ALIGNED_STRUCT)

// ----------------------------------------------------------------------
// Convenience macro disabling a particular UBSan check in a function

#if defined(__clang__)
#define PAIMON_DISABLE_UBSAN(feature) __attribute__((no_sanitize(feature)))
#else
#define PAIMON_DISABLE_UBSAN(feature)
#endif

// ----------------------------------------------------------------------
// Machine information

#if INTPTR_MAX == INT64_MAX
#define PAIMON_BITNESS 64
#elif INTPTR_MAX == INT32_MAX
#define PAIMON_BITNESS 32
#else
#error Unexpected INTPTR_MAX
#endif

// ----------------------------------------------------------------------
// From googletest
// (also in parquet-cpp)

// When you need to test the private or protected members of a class,
// use the FRIEND_TEST macro to declare your tests as friends of the
// class.  For example:
//
// class MyClass {
//  private:
//   void MyMethod();
//   FRIEND_TEST(MyClassTest, MyMethod);
// };
//
// class MyClassTest : public testing::Test {
//   // ...
// };
//
// TEST_F(MyClassTest, MyMethod) {
//   // Can call MyClass::MyMethod() here.
// }

#define FRIEND_TEST(test_case_name, test_name) friend class test_case_name##_##test_name##_Test
