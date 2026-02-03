/*
 * Copyright 2024-present Alibaba Inc.
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

#include "paimon/memory/bytes.h"

#include <memory>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"

namespace paimon::test {
TEST(BytesTest, TestSimple) {
    auto pool = paimon::GetMemoryPool();
    Bytes bytes("abcde", pool.get());
    Bytes moved_bytes("efgh", pool.get());
    ASSERT_EQ(9, pool->CurrentUsage());

    moved_bytes = std::move(bytes);
    ASSERT_EQ(5, pool->CurrentUsage());
    ASSERT_EQ("abcde", std::string(moved_bytes.data(), moved_bytes.size()));
}

TEST(BytesTest, TestCopyOf) {
    auto pool = paimon::GetMemoryPool();
    // pool allocate 5 bytes + sizeof(Bytes)
    auto bytes = Bytes::AllocateBytes("abcde", pool.get());
    ASSERT_EQ("abcde", std::string(bytes->data(), bytes->size()));
    // pool allocate 100 bytes + sizeof(Bytes)
    auto cp_bytes = Bytes::CopyOf(*bytes, 100, pool.get());
    ASSERT_EQ("abcde", std::string(cp_bytes->data(), 5));
    ASSERT_EQ(5 + 100 + sizeof(Bytes) * 2, pool->CurrentUsage());
}

TEST(BytesTest, TestAllocateBytesAndMove) {
    auto pool = paimon::GetMemoryPool();
    // pool allocate 5 + sizeof(Bytes)
    PAIMON_UNIQUE_PTR<Bytes> bytes = Bytes::AllocateBytes("abcde", pool.get());
    ASSERT_EQ("abcde", std::string(bytes->data(), bytes->size()));
    ASSERT_EQ(5 + sizeof(Bytes), pool->CurrentUsage());

    // pool allocate 4 + sizeof(Bytes)
    PAIMON_UNIQUE_PTR<Bytes> moved_bytes = Bytes::AllocateBytes("efgh", pool.get());
    ASSERT_EQ("efgh", std::string(moved_bytes->data(), moved_bytes->size()));
    ASSERT_EQ(9 + 2 * sizeof(Bytes), pool->CurrentUsage());

    // pool deallocate sizeof(Bytes) + 4
    moved_bytes = std::move(bytes);
    ASSERT_FALSE(bytes);
    ASSERT_EQ("abcde", std::string(moved_bytes->data(), moved_bytes->size()));
    ASSERT_EQ(5 + sizeof(Bytes), pool->CurrentUsage());

    // pool allocate sizeof(Bytes) and deallocate sizeof(Bytes)
    std::shared_ptr<Bytes> shared_bytes = std::move(moved_bytes);
    ASSERT_FALSE(moved_bytes);
    ASSERT_EQ("abcde", std::string(shared_bytes->data(), shared_bytes->size()));
    ASSERT_EQ(5 + sizeof(Bytes), pool->CurrentUsage());
}

TEST(BytesTest, TestCompare) {
    auto pool = paimon::GetMemoryPool();
    PAIMON_UNIQUE_PTR<Bytes> bytes1 = Bytes::AllocateBytes("abcde", pool.get());
    PAIMON_UNIQUE_PTR<Bytes> bytes2 = Bytes::AllocateBytes("abcdf", pool.get());
    ASSERT_EQ(*bytes1, *bytes1);
    ASSERT_EQ(*bytes2, *bytes2);
    ASSERT_EQ(*bytes1, *bytes1);
    ASSERT_EQ(*bytes2, *bytes2);
    ASSERT_FALSE(*bytes1 == *bytes2);
    ASSERT_LT(*bytes1, *bytes2);
    ASSERT_FALSE(*bytes1 < *bytes1);
}

// Test to verify that move assignment correctly handles memory and prevents double-free.
// Before the fix, the old implementation used memcpy + destructor which caused:
// 1. The target's original memory was freed in destructor
// 2. After memcpy, both source and target pointed to same memory
// 3. When source was "reset" via placement new, it became empty
// 4. But if move assignment was called again on the same target, the memcpy'd
//    pointer would be freed again (double-free) or memory accounting would be wrong.
TEST(BytesTest, TestMoveAssignmentNoDoubleFree) {
    auto pool = paimon::GetMemoryPool();

    // Create three Bytes objects on stack
    Bytes a("aaaa", pool.get());          // 4 bytes
    Bytes b("bb", pool.get());            // 2 bytes
    Bytes c("cccccc", pool.get());        // 6 bytes
    ASSERT_EQ(12, pool->CurrentUsage());  // 4 + 2 + 6 = 12

    // First move: b = std::move(a)
    // Should free b's original memory (2 bytes), transfer a's memory to b
    b = std::move(a);
    ASSERT_EQ(10, pool->CurrentUsage());  // 4 + 6 = 10 (b's 2 bytes freed)
    ASSERT_EQ("aaaa", std::string(b.data(), b.size()));
    // Moved-from object is expected to be empty by Bytes' contract.
    ASSERT_EQ(nullptr, a.data());  // NOLINT(bugprone-use-after-move, clang-analyzer-cplusplus.Move)
    ASSERT_EQ(0, a.size());        // NOLINT(bugprone-use-after-move, clang-analyzer-cplusplus.Move)

    // Second move: b = std::move(c)
    // Should free b's current memory (4 bytes from a), transfer c's memory to b
    // This is where the old implementation would cause issues:
    // - Old code would call destructor on b, freeing the 4 bytes
    // - Then memcpy c into b, making b point to c's 6-byte buffer
    // - Memory accounting would be wrong because Free was called on wrong data
    b = std::move(c);
    ASSERT_EQ(6, pool->CurrentUsage());  // Only c's 6 bytes remain (now owned by b)
    ASSERT_EQ("cccccc", std::string(b.data(), b.size()));
    // Moved-from object is expected to be empty by Bytes' contract.
    ASSERT_EQ(nullptr, c.data());  // NOLINT(bugprone-use-after-move, clang-analyzer-cplusplus.Move)
    ASSERT_EQ(0, c.size());        // NOLINT(bugprone-use-after-move, clang-analyzer-cplusplus.Move)

    // Self-assignment should be safe. Use an alias to avoid -Wself-move.
    Bytes* self = &b;
    b = std::move(*self);
    ASSERT_EQ(6, pool->CurrentUsage());
    ASSERT_EQ("cccccc", std::string(b.data(), b.size()));
}

// Test move assignment with heap-allocated Bytes to verify no double-free
// when combining unique_ptr semantics with move assignment
TEST(BytesTest, TestMoveAssignmentHeapAllocated) {
    auto pool = paimon::GetMemoryPool();

    auto bytes1 = Bytes::AllocateBytes("hello", pool.get());   // 5 bytes + sizeof(Bytes)
    auto bytes2 = Bytes::AllocateBytes("world!", pool.get());  // 6 bytes + sizeof(Bytes)
    size_t expected = 5 + 6 + 2 * sizeof(Bytes);
    ASSERT_EQ(expected, pool->CurrentUsage());

    // Move the content of bytes1 into bytes2's Bytes object
    // This should free "world!" (6 bytes) and transfer "hello" ownership
    *bytes2 = std::move(*bytes1);
    expected = 5 + 2 * sizeof(Bytes);  // "world!" freed, "hello" transferred
    ASSERT_EQ(expected, pool->CurrentUsage());
    ASSERT_EQ("hello", std::string(bytes2->data(), bytes2->size()));
    ASSERT_EQ(nullptr, bytes1->data());

    // Reset bytes2, which should free "hello"
    bytes2.reset();
    expected = sizeof(Bytes);  // Only bytes1's empty Bytes struct remains
    ASSERT_EQ(expected, pool->CurrentUsage());
}
}  // namespace paimon::test
