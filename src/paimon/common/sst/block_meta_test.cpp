/*
 * Copyright 2026-present Alibaba Inc.
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

#include <cstdint>
#include <string>

#include "gtest/gtest.h"
#include "paimon/common/sst/block_handle.h"
#include "paimon/common/sst/block_trailer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class BlockMetaTest : public ::testing::Test {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(BlockMetaTest, TestBlockHandleSimple) {
    int64_t offset = 1024;
    int32_t size = 256;
    BlockHandle handle(offset, size);

    // Test Offset and Size
    ASSERT_EQ(handle.Offset(), offset);
    ASSERT_EQ(handle.Size(), size);

    // Test GetFullBlockSize
    ASSERT_EQ(handle.GetFullBlockSize(), size + BlockHandle::MAX_ENCODED_LENGTH);

    // Test ToString
    ASSERT_EQ(handle.ToString(), "BlockHandle{offset=1024, size=256}");

    // Test WriteBlockHandle and ReadBlockHandle (round-trip serialization)
    ASSERT_OK_AND_ASSIGN(MemorySlice slice, handle.WriteBlockHandle(pool_.get()));
    MemorySliceInput input(slice);
    ASSERT_OK_AND_ASSIGN(BlockHandle restored, BlockHandle::ReadBlockHandle(&input));
    ASSERT_EQ(restored.Offset(), offset);
    ASSERT_EQ(restored.Size(), size);
    ASSERT_EQ(restored.GetFullBlockSize(), handle.GetFullBlockSize());
    ASSERT_EQ(restored.ToString(), handle.ToString());

    // Test with zero values
    BlockHandle zero_handle(0, 0);
    ASSERT_EQ(zero_handle.Offset(), 0);
    ASSERT_EQ(zero_handle.Size(), 0);
    ASSERT_EQ(zero_handle.GetFullBlockSize(), BlockHandle::MAX_ENCODED_LENGTH);
    ASSERT_EQ(zero_handle.ToString(), "BlockHandle{offset=0, size=0}");

    // Test with large values
    int64_t large_offset = 9999999999ll;
    int32_t large_size = 2147483647;
    BlockHandle large_handle(large_offset, large_size);
    ASSERT_EQ(large_handle.Offset(), large_offset);
    ASSERT_EQ(large_handle.Size(), large_size);
    ASSERT_EQ(large_handle.ToString(), "BlockHandle{offset=9999999999, size=2147483647}");

    // Round-trip for large values
    ASSERT_OK_AND_ASSIGN(MemorySlice large_slice, large_handle.WriteBlockHandle(pool_.get()));
    MemorySliceInput large_input(large_slice);
    ASSERT_OK_AND_ASSIGN(BlockHandle large_restored, BlockHandle::ReadBlockHandle(&large_input));
    ASSERT_EQ(large_restored.Offset(), large_offset);
    ASSERT_EQ(large_restored.Size(), large_size);
}

TEST_F(BlockMetaTest, TestBlockTrailerSimple) {
    int8_t compression_type = 2;
    int32_t crc32c = 0x12345678;
    BlockTrailer trailer(compression_type, crc32c);

    // Test CompressionType and Crc32c
    ASSERT_EQ(trailer.CompressionType(), compression_type);
    ASSERT_EQ(trailer.Crc32c(), crc32c);

    // Test ToString
    std::string str = trailer.ToString();
    ASSERT_NE(str.find("compression_type=2"), std::string::npos);
    ASSERT_NE(str.find("0x12345678"), std::string::npos);

    // Test ENCODED_LENGTH constant
    ASSERT_EQ(BlockTrailer::ENCODED_LENGTH, 5);

    // Test WriteBlockTrailer and ReadBlockTrailer (round-trip serialization)
    MemorySlice slice = trailer.WriteBlockTrailer(pool_.get());
    ASSERT_EQ(slice.Length(), BlockTrailer::ENCODED_LENGTH);
    MemorySliceInput input(slice);
    auto restored = BlockTrailer::ReadBlockTrailer(&input);
    ASSERT_NE(restored, nullptr);
    ASSERT_EQ(restored->CompressionType(), compression_type);
    ASSERT_EQ(restored->Crc32c(), crc32c);
    ASSERT_EQ(restored->ToString(), trailer.ToString());

    // Test with zero values
    BlockTrailer zero_trailer(0, 0);
    ASSERT_EQ(zero_trailer.CompressionType(), 0);
    ASSERT_EQ(zero_trailer.Crc32c(), 0);

    MemorySlice zero_slice = zero_trailer.WriteBlockTrailer(pool_.get());
    MemorySliceInput zero_input(zero_slice);
    auto zero_restored = BlockTrailer::ReadBlockTrailer(&zero_input);
    ASSERT_EQ(zero_restored->CompressionType(), 0);
    ASSERT_EQ(zero_restored->Crc32c(), 0);

    // Test with negative crc32c (valid signed int32)
    int32_t negative_crc = -1;
    BlockTrailer neg_trailer(/*compression_type=*/1, negative_crc);
    ASSERT_EQ(neg_trailer.Crc32c(), negative_crc);
    ASSERT_EQ(neg_trailer.CompressionType(), 1);

    MemorySlice neg_slice = neg_trailer.WriteBlockTrailer(pool_.get());
    MemorySliceInput neg_input(neg_slice);
    auto neg_restored = BlockTrailer::ReadBlockTrailer(&neg_input);
    ASSERT_EQ(neg_restored->CompressionType(), 1);
    ASSERT_EQ(neg_restored->Crc32c(), negative_crc);
}

}  // namespace paimon::test
