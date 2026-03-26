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

#include <gtest/gtest.h>

#include "paimon/common/global_index/btree/btree_file_footer.h"
#include "paimon/common/sst/bloom_filter_handle.h"
#include "paimon/common/sst/block_handle.h"
#include "paimon/memory/memory_pool.h"

namespace paimon::test {

class BTreeFileFooterTest : public ::testing::Test {
protected:
    void SetUp() override { pool_ = GetDefaultPool(); }

    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(BTreeFileFooterTest, ReadWriteRoundTrip) {
    // Create a footer with all handles
    auto bloom_filter_handle = std::make_shared<BloomFilterHandle>(100, 50, 1000);
    auto index_block_handle = std::make_shared<BlockHandle>(200, 80);
    auto null_bitmap_handle = std::make_shared<BlockHandle>(300, 40);

    auto footer = std::make_shared<BTreeFileFooter>(bloom_filter_handle, index_block_handle,
                                                     null_bitmap_handle);

    // Write
    auto serialized = BTreeFileFooter::Write(footer, pool_.get());
    ASSERT_NE(serialized, nullptr);
    EXPECT_EQ(serialized->Length(), BTreeFileFooter::ENCODED_LENGTH);

    // Read
    auto input = serialized->ToInput();
    auto deserialized = BTreeFileFooter::Read(input);
    ASSERT_OK(deserialized.status());
    auto deserialized_footer = deserialized.value();

    // Verify bloom filter handle
    auto bf_handle = deserialized_footer->GetBloomFilterHandle();
    ASSERT_NE(bf_handle, nullptr);
    EXPECT_EQ(bf_handle->Offset(), 100);
    EXPECT_EQ(bf_handle->Size(), 50);
    EXPECT_EQ(bf_handle->ExpectedEntries(), 1000);

    // Verify index block handle
    auto ib_handle = deserialized_footer->GetIndexBlockHandle();
    ASSERT_NE(ib_handle, nullptr);
    EXPECT_EQ(ib_handle->Offset(), 200);
    EXPECT_EQ(ib_handle->Size(), 80);

    // Verify null bitmap handle
    auto nb_handle = deserialized_footer->GetNullBitmapHandle();
    ASSERT_NE(nb_handle, nullptr);
    EXPECT_EQ(nb_handle->Offset(), 300);
    EXPECT_EQ(nb_handle->Size(), 40);
}

TEST_F(BTreeFileFooterTest, ReadWriteWithNullBloomFilter) {
    // Create a footer without bloom filter
    auto index_block_handle = std::make_shared<BlockHandle>(200, 80);
    auto null_bitmap_handle = std::make_shared<BlockHandle>(300, 40);

    auto footer = std::make_shared<BTreeFileFooter>(nullptr, index_block_handle, null_bitmap_handle);

    // Write
    auto serialized = BTreeFileFooter::Write(footer, pool_.get());
    ASSERT_NE(serialized, nullptr);
    EXPECT_EQ(serialized->Length(), BTreeFileFooter::ENCODED_LENGTH);

    // Read
    auto input = serialized->ToInput();
    auto deserialized = BTreeFileFooter::Read(input);
    ASSERT_OK(deserialized.status());
    auto deserialized_footer = deserialized.value();

    // Verify bloom filter handle is null
    EXPECT_EQ(deserialized_footer->GetBloomFilterHandle(), nullptr);

    // Verify index block handle
    auto ib_handle = deserialized_footer->GetIndexBlockHandle();
    ASSERT_NE(ib_handle, nullptr);
    EXPECT_EQ(ib_handle->Offset(), 200);
    EXPECT_EQ(ib_handle->Size(), 80);

    // Verify null bitmap handle
    auto nb_handle = deserialized_footer->GetNullBitmapHandle();
    ASSERT_NE(nb_handle, nullptr);
    EXPECT_EQ(nb_handle->Offset(), 300);
    EXPECT_EQ(nb_handle->Size(), 40);
}

TEST_F(BTreeFileFooterTest, ReadWriteWithNullNullBitmap) {
    // Create a footer without null bitmap
    auto bloom_filter_handle = std::make_shared<BloomFilterHandle>(100, 50, 1000);
    auto index_block_handle = std::make_shared<BlockHandle>(200, 80);

    auto footer = std::make_shared<BTreeFileFooter>(bloom_filter_handle, index_block_handle, nullptr);

    // Write
    auto serialized = BTreeFileFooter::Write(footer, pool_.get());
    ASSERT_NE(serialized, nullptr);
    EXPECT_EQ(serialized->Length(), BTreeFileFooter::ENCODED_LENGTH);

    // Read
    auto input = serialized->ToInput();
    auto deserialized = BTreeFileFooter::Read(input);
    ASSERT_OK(deserialized.status());
    auto deserialized_footer = deserialized.value();

    // Verify bloom filter handle
    auto bf_handle = deserialized_footer->GetBloomFilterHandle();
    ASSERT_NE(bf_handle, nullptr);
    EXPECT_EQ(bf_handle->Offset(), 100);
    EXPECT_EQ(bf_handle->Size(), 50);
    EXPECT_EQ(bf_handle->ExpectedEntries(), 1000);

    // Verify index block handle
    auto ib_handle = deserialized_footer->GetIndexBlockHandle();
    ASSERT_NE(ib_handle, nullptr);
    EXPECT_EQ(ib_handle->Offset(), 200);
    EXPECT_EQ(ib_handle->Size(), 80);

    // Verify null bitmap handle is null
    EXPECT_EQ(deserialized_footer->GetNullBitmapHandle(), nullptr);
}

TEST_F(BTreeFileFooterTest, ReadWriteWithAllNullHandles) {
    // Create a footer with only index block handle (required)
    auto index_block_handle = std::make_shared<BlockHandle>(200, 80);

    auto footer = std::make_shared<BTreeFileFooter>(nullptr, index_block_handle, nullptr);

    // Write
    auto serialized = BTreeFileFooter::Write(footer, pool_.get());
    ASSERT_NE(serialized, nullptr);
    EXPECT_EQ(serialized->Length(), BTreeFileFooter::ENCODED_LENGTH);

    // Read
    auto input = serialized->ToInput();
    auto deserialized = BTreeFileFooter::Read(input);
    ASSERT_OK(deserialized.status());
    auto deserialized_footer = deserialized.value();

    // Verify bloom filter handle is null
    EXPECT_EQ(deserialized_footer->GetBloomFilterHandle(), nullptr);

    // Verify index block handle
    auto ib_handle = deserialized_footer->GetIndexBlockHandle();
    ASSERT_NE(ib_handle, nullptr);
    EXPECT_EQ(ib_handle->Offset(), 200);
    EXPECT_EQ(ib_handle->Size(), 80);

    // Verify null bitmap handle is null
    EXPECT_EQ(deserialized_footer->GetNullBitmapHandle(), nullptr);
}

TEST_F(BTreeFileFooterTest, MagicNumberVerification) {
    // Create a valid footer
    auto index_block_handle = std::make_shared<BlockHandle>(200, 80);
    auto footer = std::make_shared<BTreeFileFooter>(nullptr, index_block_handle, nullptr);

    // Write
    auto serialized = BTreeFileFooter::Write(footer, pool_.get());
    ASSERT_NE(serialized, nullptr);

    // Read
    auto input = serialized->ToInput();
    auto deserialized = BTreeFileFooter::Read(input);
    ASSERT_OK(deserialized.status());
}

TEST_F(BTreeFileFooterTest, InvalidMagicNumber) {
    // Create a buffer with invalid magic number
    auto output = std::make_shared<MemorySliceOutput>(BTreeFileFooter::ENCODED_LENGTH, pool_.get());

    // Write bloom filter handle (all zeros for null)
    output->WriteValue(static_cast<int64_t>(0));
    output->WriteValue(static_cast<int32_t>(0));
    output->WriteValue(static_cast<int64_t>(0));

    // Write index block handle
    output->WriteValue(static_cast<int64_t>(200));
    output->WriteValue(static_cast<int32_t>(80));

    // Write null bitmap handle (all zeros for null)
    output->WriteValue(static_cast<int64_t>(0));
    output->WriteValue(static_cast<int32_t>(0));

    // Write invalid magic number
    output->WriteValue(static_cast<int32_t>(12345));  // Invalid magic number

    auto serialized = output->ToSlice();
    auto input = serialized->ToInput();

    // Read should fail
    auto deserialized = BTreeFileFooter::Read(input);
    EXPECT_FALSE(deserialized.ok());
    EXPECT_TRUE(deserialized.status().IsIOError());
}

TEST_F(BTreeFileFooterTest, EncodedLength) {
    // Verify ENCODED_LENGTH = 48
    // bloom_filter: 8(offset) + 4(size) + 8(expected_entries) = 20 bytes
    // index_block: 8(offset) + 4(size) = 12 bytes
    // null_bitmap: 8(offset) + 4(size) = 12 bytes
    // magic_number: 4 bytes
    // Total = 20 + 12 + 12 + 4 = 48 bytes
    EXPECT_EQ(BTreeFileFooter::ENCODED_LENGTH, 48);

    // Create a footer and verify the serialized length
    auto bloom_filter_handle = std::make_shared<BloomFilterHandle>(100, 50, 1000);
    auto index_block_handle = std::make_shared<BlockHandle>(200, 80);
    auto null_bitmap_handle = std::make_shared<BlockHandle>(300, 40);
    auto footer = std::make_shared<BTreeFileFooter>(bloom_filter_handle, index_block_handle,
                                                     null_bitmap_handle);

    auto serialized = BTreeFileFooter::Write(footer, pool_.get());
    ASSERT_NE(serialized, nullptr);
    EXPECT_EQ(serialized->Length(), 48);
}

}  // namespace paimon::test