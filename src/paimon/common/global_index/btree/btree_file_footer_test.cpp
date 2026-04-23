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
    BloomFilterHandle bloom_filter_handle(100, 50, 1000);
    BlockHandle index_block_handle(200, 80);
    BlockHandle null_bitmap_handle(300, 40);

    auto footer = std::make_shared<BTreeFileFooter>(bloom_filter_handle, index_block_handle,
                                                    null_bitmap_handle);

    auto serialized = BTreeFileFooter::Write(footer, pool_.get());
    ASSERT_EQ(serialized.Length(), BTreeFileFooter::kEncodingLength);

    auto input = serialized.ToInput();
    ASSERT_OK_AND_ASSIGN(auto deserialized_footer, BTreeFileFooter::Read(&input));

    const auto& bf_handle = deserialized_footer->GetBloomFilterHandle();
    ASSERT_TRUE(bf_handle.has_value());
    ASSERT_EQ(bf_handle->Offset(), 100);
    ASSERT_EQ(bf_handle->Size(), 50);
    ASSERT_EQ(bf_handle->ExpectedEntries(), 1000);

    const auto& ib_handle = deserialized_footer->GetIndexBlockHandle();
    ASSERT_EQ(ib_handle.Offset(), 200);
    ASSERT_EQ(ib_handle.Size(), 80);

    const auto& nb_handle = deserialized_footer->GetNullBitmapHandle();
    ASSERT_TRUE(nb_handle.has_value());
    ASSERT_EQ(nb_handle->Offset(), 300);
    ASSERT_EQ(nb_handle->Size(), 40);
}

TEST_F(BTreeFileFooterTest, ReadWriteWithNullBloomFilter) {
    BlockHandle index_block_handle(200, 80);
    BlockHandle null_bitmap_handle(300, 40);

    auto footer =
        std::make_shared<BTreeFileFooter>(std::nullopt, index_block_handle, null_bitmap_handle);

    auto serialized = BTreeFileFooter::Write(footer, pool_.get());
    ASSERT_EQ(serialized.Length(), BTreeFileFooter::kEncodingLength);

    auto input = serialized.ToInput();
    ASSERT_OK_AND_ASSIGN(auto deserialized_footer, BTreeFileFooter::Read(&input));

    ASSERT_FALSE(deserialized_footer->GetBloomFilterHandle().has_value());

    const auto& ib_handle = deserialized_footer->GetIndexBlockHandle();
    ASSERT_EQ(ib_handle.Offset(), 200);
    ASSERT_EQ(ib_handle.Size(), 80);

    const auto& nb_handle = deserialized_footer->GetNullBitmapHandle();
    ASSERT_TRUE(nb_handle.has_value());
    ASSERT_EQ(nb_handle->Offset(), 300);
    ASSERT_EQ(nb_handle->Size(), 40);
}

TEST_F(BTreeFileFooterTest, ReadWriteWithNullNullBitmap) {
    BloomFilterHandle bloom_filter_handle(100, 50, 1000);
    BlockHandle index_block_handle(200, 80);

    auto footer =
        std::make_shared<BTreeFileFooter>(bloom_filter_handle, index_block_handle, std::nullopt);

    auto serialized = BTreeFileFooter::Write(footer, pool_.get());
    ASSERT_EQ(serialized.Length(), BTreeFileFooter::kEncodingLength);

    auto input = serialized.ToInput();
    ASSERT_OK_AND_ASSIGN(auto deserialized_footer, BTreeFileFooter::Read(&input));

    const auto& bf_handle = deserialized_footer->GetBloomFilterHandle();
    ASSERT_TRUE(bf_handle.has_value());
    ASSERT_EQ(bf_handle->Offset(), 100);
    ASSERT_EQ(bf_handle->Size(), 50);
    ASSERT_EQ(bf_handle->ExpectedEntries(), 1000);

    const auto& ib_handle = deserialized_footer->GetIndexBlockHandle();
    ASSERT_EQ(ib_handle.Offset(), 200);
    ASSERT_EQ(ib_handle.Size(), 80);

    ASSERT_FALSE(deserialized_footer->GetNullBitmapHandle().has_value());
}

TEST_F(BTreeFileFooterTest, ReadWriteWithAllNullHandles) {
    BlockHandle index_block_handle(200, 80);

    auto footer = std::make_shared<BTreeFileFooter>(std::nullopt, index_block_handle, std::nullopt);

    auto serialized = BTreeFileFooter::Write(footer, pool_.get());
    ASSERT_EQ(serialized.Length(), BTreeFileFooter::kEncodingLength);

    auto input = serialized.ToInput();
    ASSERT_OK_AND_ASSIGN(auto deserialized_footer, BTreeFileFooter::Read(&input));

    ASSERT_FALSE(deserialized_footer->GetBloomFilterHandle().has_value());

    const auto& ib_handle = deserialized_footer->GetIndexBlockHandle();
    ASSERT_EQ(ib_handle.Offset(), 200);
    ASSERT_EQ(ib_handle.Size(), 80);

    ASSERT_FALSE(deserialized_footer->GetNullBitmapHandle().has_value());
}

TEST_F(BTreeFileFooterTest, InvalidMagicNumber) {
    MemorySliceOutput output(BTreeFileFooter::kEncodingLength, pool_.get());

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
    ASSERT_NOK_WITH_MSG(deserialized, "File is not a btree index file (expected magic number");
}

}  // namespace paimon::test
