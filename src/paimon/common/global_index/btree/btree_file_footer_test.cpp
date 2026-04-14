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

#include "paimon/common/global_index/btree/btree_file_footer.h"

#include <gtest/gtest.h>

#include "paimon/common/sst/block_handle.h"
#include "paimon/common/sst/bloom_filter_handle.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class BTreeFileFooterTest : public ::testing::Test {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(BTreeFileFooterTest, ReadWriteRoundTrip) {
    auto bloom_filter_handle = std::make_shared<BloomFilterHandle>(100, 50, 1000);
    auto index_block_handle = std::make_shared<BlockHandle>(200, 80);
    auto null_bitmap_handle = std::make_shared<BlockHandle>(300, 40);

    auto footer = std::make_shared<BTreeFileFooter>(bloom_filter_handle, index_block_handle,
                                                    null_bitmap_handle);

    auto serialized = BTreeFileFooter::Write(footer, pool_.get());
    EXPECT_EQ(serialized.Length(), BTreeFileFooter::ENCODED_LENGTH);

    auto input = serialized.ToInput();
    ASSERT_OK_AND_ASSIGN(auto deserialized_footer, BTreeFileFooter::Read(&input));

    auto bf_handle = deserialized_footer->GetBloomFilterHandle();
    ASSERT_NE(bf_handle, nullptr);
    EXPECT_EQ(bf_handle->Offset(), 100);
    EXPECT_EQ(bf_handle->Size(), 50);
    EXPECT_EQ(bf_handle->ExpectedEntries(), 1000);

    auto ib_handle = deserialized_footer->GetIndexBlockHandle();
    ASSERT_NE(ib_handle, nullptr);
    EXPECT_EQ(ib_handle->Offset(), 200);
    EXPECT_EQ(ib_handle->Size(), 80);

    auto nb_handle = deserialized_footer->GetNullBitmapHandle();
    ASSERT_NE(nb_handle, nullptr);
    EXPECT_EQ(nb_handle->Offset(), 300);
    EXPECT_EQ(nb_handle->Size(), 40);
}

TEST_F(BTreeFileFooterTest, ReadWriteWithNullBloomFilter) {
    auto index_block_handle = std::make_shared<BlockHandle>(200, 80);
    auto null_bitmap_handle = std::make_shared<BlockHandle>(300, 40);

    auto footer =
        std::make_shared<BTreeFileFooter>(nullptr, index_block_handle, null_bitmap_handle);

    auto serialized = BTreeFileFooter::Write(footer, pool_.get());
    EXPECT_EQ(serialized.Length(), BTreeFileFooter::ENCODED_LENGTH);

    auto input = serialized.ToInput();
    ASSERT_OK_AND_ASSIGN(auto deserialized_footer, BTreeFileFooter::Read(&input));

    EXPECT_EQ(deserialized_footer->GetBloomFilterHandle(), nullptr);

    auto ib_handle = deserialized_footer->GetIndexBlockHandle();
    ASSERT_NE(ib_handle, nullptr);
    EXPECT_EQ(ib_handle->Offset(), 200);
    EXPECT_EQ(ib_handle->Size(), 80);

    auto nb_handle = deserialized_footer->GetNullBitmapHandle();
    ASSERT_NE(nb_handle, nullptr);
    EXPECT_EQ(nb_handle->Offset(), 300);
    EXPECT_EQ(nb_handle->Size(), 40);
}

TEST_F(BTreeFileFooterTest, ReadWriteWithNullNullBitmap) {
    auto bloom_filter_handle = std::make_shared<BloomFilterHandle>(100, 50, 1000);
    auto index_block_handle = std::make_shared<BlockHandle>(200, 80);

    auto footer =
        std::make_shared<BTreeFileFooter>(bloom_filter_handle, index_block_handle, nullptr);

    auto serialized = BTreeFileFooter::Write(footer, pool_.get());
    EXPECT_EQ(serialized.Length(), BTreeFileFooter::ENCODED_LENGTH);

    auto input = serialized.ToInput();
    ASSERT_OK_AND_ASSIGN(auto deserialized_footer, BTreeFileFooter::Read(&input));

    auto bf_handle = deserialized_footer->GetBloomFilterHandle();
    ASSERT_NE(bf_handle, nullptr);
    EXPECT_EQ(bf_handle->Offset(), 100);
    EXPECT_EQ(bf_handle->Size(), 50);
    EXPECT_EQ(bf_handle->ExpectedEntries(), 1000);

    auto ib_handle = deserialized_footer->GetIndexBlockHandle();
    ASSERT_NE(ib_handle, nullptr);
    EXPECT_EQ(ib_handle->Offset(), 200);
    EXPECT_EQ(ib_handle->Size(), 80);

    EXPECT_EQ(deserialized_footer->GetNullBitmapHandle(), nullptr);
}

TEST_F(BTreeFileFooterTest, ReadWriteWithAllNullHandles) {
    auto index_block_handle = std::make_shared<BlockHandle>(200, 80);

    auto footer = std::make_shared<BTreeFileFooter>(nullptr, index_block_handle, nullptr);

    auto serialized = BTreeFileFooter::Write(footer, pool_.get());
    EXPECT_EQ(serialized.Length(), BTreeFileFooter::ENCODED_LENGTH);

    auto input = serialized.ToInput();
    ASSERT_OK_AND_ASSIGN(auto deserialized_footer, BTreeFileFooter::Read(&input));

    EXPECT_EQ(deserialized_footer->GetBloomFilterHandle(), nullptr);

    auto ib_handle = deserialized_footer->GetIndexBlockHandle();
    ASSERT_NE(ib_handle, nullptr);
    EXPECT_EQ(ib_handle->Offset(), 200);
    EXPECT_EQ(ib_handle->Size(), 80);

    EXPECT_EQ(deserialized_footer->GetNullBitmapHandle(), nullptr);
}

TEST_F(BTreeFileFooterTest, InvalidMagicNumber) {
    MemorySliceOutput output(BTreeFileFooter::ENCODED_LENGTH, pool_.get());

    output.WriteValue(static_cast<int64_t>(0));
    output.WriteValue(static_cast<int32_t>(0));
    output.WriteValue(static_cast<int64_t>(0));

    output.WriteValue(static_cast<int64_t>(200));
    output.WriteValue(static_cast<int32_t>(80));

    output.WriteValue(static_cast<int64_t>(0));
    output.WriteValue(static_cast<int32_t>(0));

    output.WriteValue(static_cast<int32_t>(1));      // version
    output.WriteValue(static_cast<int32_t>(12345));  // Invalid magic number

    auto serialized = output.ToSlice();
    auto input = serialized.ToInput();

    auto deserialized = BTreeFileFooter::Read(&input);
    ASSERT_NOK_WITH_MSG(deserialized, "not a btree index file");
}

}  // namespace paimon::test
