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
}  // namespace paimon::test
