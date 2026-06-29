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

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/common/utils/serialization_utils.h"
#include "paimon/core/io/chain_data_file_path_factory.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_meta_serializer.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/source/chain_data_split_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/table/source/data_split.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {
constexpr const char* kFileName = "data-b446f78a-2cfb-4b3b-add8-31295d24a277-0.parquet";
constexpr const char* kBucketPath = "data/parquet/append_09.db/append_09/f1=20/bucket-0";
constexpr const char* kBranch = "delta";
constexpr const char* kFallbackBucketPath = "data/parquet/append_09.db/append_09/f1=10/bucket-1";
constexpr const char* kSingleSplitBucketPath =
    "data/parquet/append_table_with_append_pt_branch.db/append_table_with_append_pt_branch/"
    "pt=2/bucket-0";
constexpr const char* kSingleSplitFileName0 = "data-39204ff8-55b2-497b-8e87-a1c736799eab-0.parquet";
constexpr const char* kSingleSplitFileName1 = "data-625b3277-84d3-4320-80b9-89a5075bf5fd-0.parquet";

std::shared_ptr<DataFileMeta> CreateDataFileMeta(
    const std::string& file_name, std::optional<std::string> external_path = std::nullopt) {
    return std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/1024, /*row_count=*/7, BinaryRow::EmptyRow(),
        BinaryRow::EmptyRow(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
        /*min_sequence_number=*/0, /*max_sequence_number=*/6, /*schema_id=*/0,
        DataFileMeta::DUMMY_LEVEL, std::vector<std::optional<std::string>>(), Timestamp(0, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, external_path,
        /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
}

void WriteStringMap(MemorySegmentOutputStream* out,
                    const std::unordered_map<std::string, std::string>& values) {
    out->WriteValue<int32_t>(values.size());
    for (const auto& [key, value] : values) {
        out->WriteString(key);
        out->WriteString(value);
    }
}

std::string ToString(const MemorySegmentOutputStream& out,
                     const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_UNIQUE_PTR<Bytes> bytes =
        MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
    return std::string(bytes->data(), bytes->size());
}

std::string SerializeVersion7DataSplit(const std::shared_ptr<DataSplitImpl>& split,
                                       const std::shared_ptr<MemoryPool>& pool) {
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    out.WriteValue<int64_t>(DataSplitImpl::MAGIC);
    out.WriteValue<int32_t>(7);
    out.WriteValue<int64_t>(split->SnapshotId());
    EXPECT_OK(SerializationUtils::SerializeBinaryRow(split->Partition(), &out));
    out.WriteValue<int32_t>(split->Bucket());
    out.WriteString(split->BucketPath());

    out.WriteValue<bool>(split->TotalBuckets().has_value());
    if (split->TotalBuckets().has_value()) {
        out.WriteValue<int32_t>(split->TotalBuckets().value());
    }

    DataFileMetaSerializer serializer(pool);
    EXPECT_OK(serializer.SerializeList(split->BeforeFiles(), &out));
    DeletionFile::SerializeList(split->BeforeDeletionFiles(), &out);
    EXPECT_OK(serializer.SerializeList(split->DataFiles(), &out));
    DeletionFile::SerializeList(split->DeletionFiles(), &out);
    out.WriteValue<bool>(split->IsStreaming());
    out.WriteValue<bool>(split->RawConvertible());
    return ToString(out, pool);
}

std::shared_ptr<DataSplitImpl> CreateBaseSplit(const std::shared_ptr<MemoryPool>& pool) {
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/118,
                                   ChainDataSplitImpl::VIRTUAL_BUCKET_PATH,
                                   {CreateDataFileMeta(kFileName)});
    return builder.WithSnapshot(42)
        .WithTotalBuckets(256)
        .IsStreaming(false)
        .RawConvertible(true)
        .Build()
        .value();
}

std::string AppendChainDataSplitTail(
    const std::string& base_bytes, const std::shared_ptr<MemoryPool>& pool,
    const std::unordered_map<std::string, std::string>& bucket_paths,
    const std::unordered_map<std::string, std::string>& branches) {
    MemorySegmentOutputStream tail(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    tail.WriteValue<bool>(true);
    EXPECT_OK(SerializationUtils::SerializeBinaryRow(BinaryRow::EmptyRow(), &tail));
    WriteStringMap(&tail, bucket_paths);
    WriteStringMap(&tail, branches);
    return base_bytes + ToString(tail, pool);
}

std::string AppendMalformedChainDataSplitTail(const std::string& base_bytes,
                                              const std::shared_ptr<MemoryPool>& pool) {
    MemorySegmentOutputStream tail(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    tail.WriteValue<bool>(true);
    return base_bytes + ToString(tail, pool);
}

}  // namespace

TEST(ChainDataSplitTest, DeserializeChainDataSplitTail) {
    auto pool = GetDefaultPool();
    auto base_split = CreateBaseSplit(pool);
    ASSERT_OK_AND_ASSIGN(std::string base_bytes, Split::Serialize(base_split, pool));

    std::string chain_bytes = AppendChainDataSplitTail(base_bytes, pool, {{kFileName, kBucketPath}},
                                                       {{kFileName, kBranch}});

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> split,
                         Split::Deserialize(chain_bytes.data(), chain_bytes.size(), pool));
    auto chain_split = std::dynamic_pointer_cast<ChainDataSplitImpl>(split);
    ASSERT_TRUE(chain_split);
    EXPECT_TRUE(chain_split->AllSnapshotSplit());
    EXPECT_EQ(chain_split->BucketPath(), ChainDataSplitImpl::VIRTUAL_BUCKET_PATH);
    EXPECT_EQ(chain_split->FileBucketPathMapping().at(kFileName), kBucketPath);
    EXPECT_EQ(chain_split->FileBranchMapping().at(kFileName), kBranch);
}

TEST(ChainDataSplitTest, DeserializeVersion7SingleSplitWithOriginalBucketPath) {
    auto pool = GetDefaultPool();
    DataSplitImpl::Builder builder(
        BinaryRow::EmptyRow(), /*bucket=*/3, kSingleSplitBucketPath,
        {CreateDataFileMeta(kSingleSplitFileName0), CreateDataFileMeta(kSingleSplitFileName1)});
    ASSERT_OK_AND_ASSIGN(auto base_split, builder.WithSnapshot(42)
                                              .WithTotalBuckets(256)
                                              .IsStreaming(false)
                                              .RawConvertible(true)
                                              .Build());
    std::string base_bytes = SerializeVersion7DataSplit(base_split, pool);
    std::string split_bytes = AppendChainDataSplitTail(
        base_bytes, pool,
        {{kSingleSplitFileName0, kSingleSplitBucketPath},
         {kSingleSplitFileName1, kSingleSplitBucketPath}},
        {{kSingleSplitFileName0, "snapshot"}, {kSingleSplitFileName1, "snapshot"}});

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> split,
                         Split::Deserialize(split_bytes.data(), split_bytes.size(), pool));
    auto chain_split = std::dynamic_pointer_cast<ChainDataSplitImpl>(split);
    ASSERT_TRUE(chain_split);
    EXPECT_TRUE(chain_split->AllSnapshotSplit());
    EXPECT_EQ(chain_split->BucketPath(), kSingleSplitBucketPath);
    EXPECT_EQ(chain_split->FileBucketPathMapping().at(kSingleSplitFileName0),
              kSingleSplitBucketPath);
    EXPECT_EQ(chain_split->FileBucketPathMapping().at(kSingleSplitFileName1),
              kSingleSplitBucketPath);
    EXPECT_EQ(chain_split->FileBranchMapping().at(kSingleSplitFileName0), "snapshot");
    EXPECT_EQ(chain_split->FileBranchMapping().at(kSingleSplitFileName1), "snapshot");
}

TEST(ChainDataSplitTest, ChainDataFilePathFactoryUsesPerFileBucketPath) {
    auto fallback = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(fallback->Init(kFallbackBucketPath, "parquet", "data-",
                             /*external_path_provider=*/nullptr));
    ChainDataFilePathFactory factory(fallback, {{kFileName, kBucketPath}});

    auto file_meta = CreateDataFileMeta(kFileName);
    EXPECT_EQ(factory.ToPath(file_meta), std::string(kBucketPath) + "/" + kFileName);
}

TEST(ChainDataSplitTest, ChainDataFilePathFactoryPreservesExternalPath) {
    auto fallback = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(fallback->Init(kFallbackBucketPath, "parquet", "data-",
                             /*external_path_provider=*/nullptr));
    ChainDataFilePathFactory factory(fallback, {{kFileName, kBucketPath}});

    const std::string external_path = "hdfs://external/path/" + std::string(kFileName);
    auto file_meta = CreateDataFileMeta(kFileName, external_path);
    EXPECT_EQ(factory.ToPath(file_meta), external_path);
}

TEST(ChainDataSplitTest, MalformedChainDataSplitTailReturnsContextualError) {
    auto pool = GetDefaultPool();
    auto base_split = CreateBaseSplit(pool);
    ASSERT_OK_AND_ASSIGN(std::string base_bytes, Split::Serialize(base_split, pool));

    std::string chain_bytes = AppendMalformedChainDataSplitTail(base_bytes, pool);

    ASSERT_NOK_WITH_MSG(Split::Deserialize(chain_bytes.data(), chain_bytes.size(), pool),
                        "ChainDataSplit");
}

}  // namespace paimon::test
