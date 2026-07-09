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
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/io/chain_split_file_path_factory.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_meta_serializer.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/source/chain_split_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/fallback_data_split.h"
#include "paimon/data/timestamp.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/table/source/data_split.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {
constexpr const char* kFileName = "data-b446f78a-2cfb-4b3b-add8-31295d24a277-0.parquet";
constexpr const char* kBucketPath = "data/parquet/append_09.db/append_09/f1=20/bucket-0";
constexpr const char* kBranch = "delta";
constexpr const char* kSingleSplitBucketPath =
    "data/parquet/append_table_with_append_pt_branch.db/append_table_with_append_pt_branch/"
    "pt=2/bucket-0";
constexpr int64_t kSplitSerializerMagic = 0x53504C49545F5631L;  // "SPLIT_V1"
constexpr int32_t kSplitSerializerVersion = 1;
constexpr int32_t kSplitSerializerChainSplit = 4;

Result<std::string> ReadCompatibilityBytes(const std::string& file_name) {
    auto file_system = std::make_unique<LocalFileSystem>();
    PAIMON_ASSIGN_OR_RAISE(auto input_stream,
                           file_system->Open(GetDataDir() + "/compatibility/" + file_name));
    std::string bytes(input_stream->Length().value_or(0), '\0');
    PAIMON_RETURN_NOT_OK(input_stream->Read(bytes.data(), bytes.size()));
    PAIMON_RETURN_NOT_OK(input_stream->Close());
    return bytes;
}

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
    out->WriteValue<bool>(true);
    out->WriteValue<int32_t>(values.size());
    for (const auto& [key, value] : values) {
        out->WriteValue<int32_t>(static_cast<int32_t>(key.size()));
        out->Write(key.data(), static_cast<uint32_t>(key.size()));
        out->WriteValue<int32_t>(static_cast<int32_t>(value.size()));
        out->Write(value.data(), static_cast<uint32_t>(value.size()));
    }
}

std::string ToString(const MemorySegmentOutputStream& out,
                     const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_UNIQUE_PTR<Bytes> bytes =
        MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
    return std::string(bytes->data(), bytes->size());
}

std::string SerializeChainSplit(const BinaryRow& logical_partition,
                                const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
                                const std::unordered_map<std::string, std::string>& bucket_paths,
                                const std::unordered_map<std::string, std::string>& branches,
                                const std::shared_ptr<MemoryPool>& pool) {
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    out.WriteValue<int64_t>(kSplitSerializerMagic);
    out.WriteValue<int32_t>(kSplitSerializerVersion);
    out.WriteValue<int32_t>(kSplitSerializerChainSplit);
    EXPECT_OK(SerializationUtils::SerializeBinaryRow(logical_partition, &out));
    DataFileMetaSerializer serializer(pool);
    EXPECT_OK(serializer.SerializeList(data_files, &out));
    WriteStringMap(&out, bucket_paths);
    WriteStringMap(&out, branches);
    return ToString(out, pool);
}

}  // namespace

TEST(ChainSplitTest, DeserializeChainSplit) {
    auto pool = GetDefaultPool();

    std::string chain_bytes =
        SerializeChainSplit(BinaryRow::EmptyRow(), {CreateDataFileMeta(kFileName)},
                            {{kFileName, kBucketPath}}, {{kFileName, kBranch}}, pool);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> split,
                         Split::Deserialize(chain_bytes.data(), chain_bytes.size(), pool));
    auto chain_split = std::dynamic_pointer_cast<ChainSplitImpl>(split);
    ASSERT_TRUE(chain_split);
    EXPECT_FALSE(chain_split->AllSnapshotSplit());
    EXPECT_EQ(chain_split->BucketPath(), ChainSplitImpl::VIRTUAL_BUCKET_PATH);
    ASSERT_EQ(chain_split->DataFiles().size(), 1U);
    EXPECT_EQ(chain_split->DataFiles()[0]->file_name, kFileName);
    EXPECT_EQ(chain_split->FileBucketPathMapping().at(kFileName), kBucketPath);
    EXPECT_EQ(chain_split->FileBranchMapping().at(kFileName), kBranch);
}

TEST(ChainSplitTest, DeserializeJavaGoldenChainSplit) {
    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::string chain_bytes, ReadCompatibilityBytes("split-v1-chain"));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> split,
                         Split::Deserialize(chain_bytes.data(), chain_bytes.size(), pool));
    auto chain_split = std::dynamic_pointer_cast<ChainSplitImpl>(split);
    ASSERT_TRUE(chain_split);
    EXPECT_EQ(chain_split->FileBucketPathMapping().at("file-a"), "dt=20260706/bucket-3");
    EXPECT_EQ(chain_split->FileBucketPathMapping().at("file-b"), "dt=20260706/bucket-3");
    EXPECT_EQ(chain_split->FileBucketPathMapping().at("chain-file"), "dt=20260707/bucket-5");
    EXPECT_EQ(chain_split->FileBranchMapping().at("file-a"), "snapshot");
    EXPECT_EQ(chain_split->FileBranchMapping().at("file-b"), "snapshot");
    EXPECT_EQ(chain_split->FileBranchMapping().at("chain-file"), "delta");
}

TEST(ChainSplitTest, DeserializeJavaGoldenIndexedSplit) {
    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::string indexed_bytes, ReadCompatibilityBytes("split-v1-indexed"));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> split,
                         Split::Deserialize(indexed_bytes.data(), indexed_bytes.size(), pool));
    auto indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(split);
    ASSERT_TRUE(indexed_split);
    ASSERT_EQ(indexed_split->RowRanges().size(), 2U);
    EXPECT_EQ(indexed_split->RowRanges()[0], Range(1, 4));
    EXPECT_EQ(indexed_split->RowRanges()[1], Range(11, 13));
    ASSERT_EQ(indexed_split->Scores().size(), 3U);
    EXPECT_FLOAT_EQ(indexed_split->Scores()[0], 0.5f);
    EXPECT_FLOAT_EQ(indexed_split->Scores()[1], 0.25f);
    EXPECT_FLOAT_EQ(indexed_split->Scores()[2], 0.125f);
}

TEST(ChainSplitTest, DeserializeJavaGoldenFallbackDataSplit) {
    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::string fallback_bytes,
                         ReadCompatibilityBytes("split-v1-fallback-data"));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> split,
                         Split::Deserialize(fallback_bytes.data(), fallback_bytes.size(), pool));
    auto fallback_split = std::dynamic_pointer_cast<FallbackDataSplit>(split);
    ASSERT_TRUE(fallback_split);
    EXPECT_TRUE(fallback_split->IsFallback());
    ASSERT_TRUE(std::dynamic_pointer_cast<DataSplitImpl>(fallback_split->GetSplit()));

    ASSERT_OK_AND_ASSIGN(std::string serialized, Split::Serialize(fallback_split, pool));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> round_trip,
                         Split::Deserialize(serialized.data(), serialized.size(), pool));
    auto round_trip_fallback = std::dynamic_pointer_cast<FallbackDataSplit>(round_trip);
    ASSERT_TRUE(round_trip_fallback);
    EXPECT_TRUE(round_trip_fallback->IsFallback());
}

TEST(ChainSplitTest, DeserializeChainSplitWithMultipleFiles) {
    auto pool = GetDefaultPool();
    const std::string file_name0 = "data-39204ff8-55b2-497b-8e87-a1c736799eab-0.parquet";
    const std::string file_name1 = "data-625b3277-84d3-4320-80b9-89a5075bf5fd-0.parquet";

    std::string split_bytes = SerializeChainSplit(
        BinaryRow::EmptyRow(), {CreateDataFileMeta(file_name0), CreateDataFileMeta(file_name1)},
        {{file_name0, kSingleSplitBucketPath}, {file_name1, kSingleSplitBucketPath}},
        {{file_name0, "snapshot"}, {file_name1, "snapshot"}}, pool);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> split,
                         Split::Deserialize(split_bytes.data(), split_bytes.size(), pool));
    auto chain_split = std::dynamic_pointer_cast<ChainSplitImpl>(split);
    ASSERT_TRUE(chain_split);
    EXPECT_FALSE(chain_split->AllSnapshotSplit());
    EXPECT_EQ(chain_split->BucketPath(), ChainSplitImpl::VIRTUAL_BUCKET_PATH);
    ASSERT_EQ(chain_split->DataFiles().size(), 2U);
    EXPECT_EQ(chain_split->FileBucketPathMapping().at(file_name0), kSingleSplitBucketPath);
    EXPECT_EQ(chain_split->FileBucketPathMapping().at(file_name1), kSingleSplitBucketPath);
    EXPECT_EQ(chain_split->FileBranchMapping().at(file_name0), "snapshot");
    EXPECT_EQ(chain_split->FileBranchMapping().at(file_name1), "snapshot");
}

TEST(ChainSplitTest, ChainSplitFilePathFactoryUsesPerFileBucketPath) {
    ChainSplitFilePathFactory factory(
        std::unordered_map<std::string, std::string>{{kFileName, kBucketPath}});

    auto file_meta = CreateDataFileMeta(kFileName);
    EXPECT_EQ(factory.ToPath(file_meta), std::string(kBucketPath) + "/" + kFileName);
}

TEST(ChainSplitTest, ChainSplitFilePathFactoryUsesPerFileBucketPathForAlignedFile) {
    ChainSplitFilePathFactory factory(
        std::unordered_map<std::string, std::string>{{kFileName, kBucketPath}});

    auto file_meta = CreateDataFileMeta(kFileName);
    EXPECT_EQ(factory.ToAlignedPath("index-0.index", file_meta),
              std::string(kBucketPath) + "/index-0.index");
}

TEST(ChainSplitTest, ChainSplitFilePathFactoryPreservesExternalPath) {
    ChainSplitFilePathFactory factory(
        std::unordered_map<std::string, std::string>{{kFileName, kBucketPath}});

    const std::string external_path = "hdfs://external/path/" + std::string(kFileName);
    auto file_meta = CreateDataFileMeta(kFileName, external_path);
    EXPECT_EQ(factory.ToPath(file_meta), external_path);
}

TEST(ChainSplitTest, ChainSplitFilePathFactoryPreservesExternalAlignedPath) {
    ChainSplitFilePathFactory factory(
        std::unordered_map<std::string, std::string>{{kFileName, kBucketPath}});

    const std::string external_path = "hdfs://external/path/" + std::string(kFileName);
    auto file_meta = CreateDataFileMeta(kFileName, external_path);
    EXPECT_EQ(factory.ToAlignedPath("index-0.index", file_meta),
              std::string("hdfs://external/path/index-0.index"));
}

TEST(ChainSplitTest, ChainSplitFilePathFactoryRejectsMissingBucketPathMapping) {
    ASSERT_NOK_WITH_MSG(ChainSplitFilePathFactory::Create({CreateDataFileMeta(kFileName)}, {}),
                        "bucket path is missing for ChainSplit file");
}

TEST(ChainSplitTest, SerializeChainSplitPreservesMappings) {
    auto pool = GetDefaultPool();

    std::string chain_bytes =
        SerializeChainSplit(BinaryRow::EmptyRow(), {CreateDataFileMeta(kFileName)},
                            {{kFileName, kBucketPath}}, {{kFileName, kBranch}}, pool);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> split,
                         Split::Deserialize(chain_bytes.data(), chain_bytes.size(), pool));
    ASSERT_OK_AND_ASSIGN(std::string serialized, Split::Serialize(split, pool));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> round_trip,
                         Split::Deserialize(serialized.data(), serialized.size(), pool));

    auto chain_split = std::dynamic_pointer_cast<ChainSplitImpl>(round_trip);
    ASSERT_TRUE(chain_split);
    EXPECT_EQ(chain_split->FileBucketPathMapping().at(kFileName), kBucketPath);
    EXPECT_EQ(chain_split->FileBranchMapping().at(kFileName), kBranch);
}

TEST(ChainSplitTest, MalformedChainSplitReturnsError) {
    auto pool = GetDefaultPool();
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    out.WriteValue<int64_t>(kSplitSerializerMagic);
    out.WriteValue<int32_t>(kSplitSerializerVersion);
    out.WriteValue<int32_t>(kSplitSerializerChainSplit);
    EXPECT_OK(SerializationUtils::SerializeBinaryRow(BinaryRow::EmptyRow(), &out));
    std::string chain_bytes = ToString(out, pool);

    ASSERT_NOK(Split::Deserialize(chain_bytes.data(), chain_bytes.size(), pool));
}

}  // namespace paimon::test
