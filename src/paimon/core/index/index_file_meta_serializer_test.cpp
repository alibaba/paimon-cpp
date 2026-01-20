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

#include "paimon/core/index/index_file_meta_serializer.h"

#include <cstdlib>

#include "gtest/gtest.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class IndexFileMetaSerializerTest : public testing::Test {
 public:
    void SetUp() override {
        memory_pool_ = GetDefaultPool();
    }

 private:
    std::shared_ptr<IndexFileMeta> GetRandomDeletionVectorIndexFile() {
        LinkedHashMap<std::string, DeletionVectorMeta> deletion_vectors_ranges;
        deletion_vectors_ranges.insert_or_assign(
            "my_file_name1",
            DeletionVectorMeta("my_file_name1", std::rand(), std::rand(), std::nullopt));
        deletion_vectors_ranges.insert_or_assign(
            "my_file_name2",
            DeletionVectorMeta("my_file_name2", std::rand(), std::rand(), std::nullopt));
        return std::make_shared<IndexFileMeta>(
            "DELETION_VECTORS", "deletion_vectors_index_file_name" + std::to_string(std::rand()),
            std::rand(), rand(), deletion_vectors_ranges, /*external_path=*/std::nullopt);
    }

    const int32_t TRIES = 100;

    std::shared_ptr<MemoryPool> memory_pool_;
};

TEST_F(IndexFileMetaSerializerTest, TestEqual) {
    auto index_meta1 = GetRandomDeletionVectorIndexFile();
    index_meta1->file_size_ = 10;
    auto index_meta2 = GetRandomDeletionVectorIndexFile();
    index_meta2->file_size_ = 20;
    ASSERT_EQ(*index_meta1, *index_meta1);
    ASSERT_TRUE(index_meta1->TEST_Equal(*index_meta1));
    ASSERT_FALSE(*index_meta1 == *index_meta2);
    ASSERT_FALSE(index_meta1->TEST_Equal(*index_meta2));
}

TEST_F(IndexFileMetaSerializerTest, TestToFromRow) {
    IndexFileMetaSerializer serializer(memory_pool_);
    for (int32_t i = 0; i < TRIES; i++) {
        auto expected = GetRandomDeletionVectorIndexFile();
        ASSERT_OK_AND_ASSIGN(BinaryRow row, serializer.ToRow(expected));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> actual, serializer.FromRow(row));
        ASSERT_EQ(expected->ToString(), actual->ToString());
        ASSERT_EQ(*expected, *actual);
        ASSERT_TRUE(expected->TEST_Equal(*actual));
    }
}

TEST_F(IndexFileMetaSerializerTest, TestToFromRowWithNullDeletionVectorMetas) {
    IndexFileMetaSerializer serializer(memory_pool_);
    auto expected = std::make_shared<IndexFileMeta>(
        "DELETION_VECTORS", "deletion_vectors_index_file_0", /*file_size=*/10,
        /*row_count=*/5,
        /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt);
    ASSERT_OK_AND_ASSIGN(BinaryRow row, serializer.ToRow(expected));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> actual, serializer.FromRow(row));
    ASSERT_EQ(expected->ToString(), actual->ToString());
    ASSERT_EQ(*expected, *actual);
}

TEST_F(IndexFileMetaSerializerTest, TestToFromRowWithGlobalIndex) {
    auto bytes = std::make_shared<Bytes>("apple", memory_pool_.get());
    IndexFileMetaSerializer serializer(memory_pool_);
    GlobalIndexMeta global_index_meta(
        /*row_range_start=*/10, /*row_range_end=*/50,
        /*index_field_id=*/5, /*extra_field_ids=*/std::optional<std::vector<int32_t>>({0, 1}),
        bytes);
    {
        auto expected =
            std::make_shared<IndexFileMeta>("bitmap", "bitmap_index_file_0", /*file_size=*/10,
                                            /*row_count=*/41, /*dv_ranges=*/std::nullopt,
                                            /*external_path=*/std::nullopt, global_index_meta);
        ASSERT_OK_AND_ASSIGN(BinaryRow row, serializer.ToRow(expected));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> actual, serializer.FromRow(row));
        ASSERT_EQ(expected->ToString(), actual->ToString());
        ASSERT_EQ(*expected, *actual);
    }
    {
        // test external path
        auto expected = std::make_shared<IndexFileMeta>(
            "bitmap", "bitmap_index_file_0", /*file_size=*/10,
            /*row_count=*/41, /*dv_ranges=*/std::nullopt,
            /*external_path=*/"FILE:/tmp/external/bitmap_index_file_0", global_index_meta);
        ASSERT_OK_AND_ASSIGN(BinaryRow row, serializer.ToRow(expected));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> actual, serializer.FromRow(row));
        ASSERT_EQ(expected->ToString(), actual->ToString());
        ASSERT_EQ(*expected, *actual);
    }
}

TEST_F(IndexFileMetaSerializerTest, TestSerialize) {
    IndexFileMetaSerializer serializer(memory_pool_);
    auto expected = GetRandomDeletionVectorIndexFile();
    for (int32_t i = 0; i < TRIES; i++) {
        MemorySegmentOutputStream out(1024, memory_pool_);
        ASSERT_OK(serializer.Serialize(expected, &out));
        PAIMON_UNIQUE_PTR<Bytes> bytes = MemorySegmentUtils::CopyToBytes(
            out.Segments(), 0, out.CurrentSize(), memory_pool_.get());
        auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
        DataInputStream in(input_stream);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> actual, serializer.Deserialize(&in));
        ASSERT_EQ(expected->ToString(), actual->ToString());
        ASSERT_EQ(*expected, *actual);
    }
}

}  // namespace paimon::test
