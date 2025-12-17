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

#include "paimon/core/table/source/fallback_data_split.h"

#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <variant>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/data/timestamp.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(FallbackDataSplitTest, TestDeserialize) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/parquet/append_table_with_append_pt_branch.db/"
                            "append_table_with_append_pt_branch/data-splits/"
                            "data_split-0";
    auto file_system = std::make_unique<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);
    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto fallback_data_split = std::dynamic_pointer_cast<FallbackDataSplit>(result);
    ASSERT_TRUE(fallback_data_split);
    // not fallback branch
    ASSERT_FALSE(fallback_data_split->IsFallback());

    auto result_data_split =
        std::dynamic_pointer_cast<DataSplitImpl>(fallback_data_split->GetSplit());
    ASSERT_TRUE(result_data_split);

    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-eefbe5f2-f015-487c-b2ab-141b2b2cffd7-0.parquet", /*file_size=*/619, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/
        SimpleStats::EmptyStats(),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({1, 110}, {1, 110}, {0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1755880762233ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-43880011-d066-4255-ad65-891d79cde23b-0.parquet", /*file_size=*/891, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/
        SimpleStats::EmptyStats(),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({1, 120, 1200}, {1, 120, 1200}, {0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/2,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1755884315482ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRowGenerator::GenerateRow({1}, pool.get()),
        /*bucket=*/0, /*bucket_path=*/
        "data/parquet/append_table_with_append_pt_branch.db/append_table_with_append_pt_branch/"
        "pt=1/bucket-0",
        {file_meta1, file_meta2});
    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(builder.WithSnapshot(2)
                                                                            .WithTotalBuckets(-1)
                                                                            .IsStreaming(false)
                                                                            .RawConvertible(true)
                                                                            .Build()
                                                                            .value());
    ASSERT_EQ(*result_data_split, *expected_data_split)
        << result_data_split->ToString() << std::endl
        << expected_data_split->ToString();
}

TEST(FallbackDataSplitTest, TestDeserialize2) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/parquet/append_table_with_append_pt_branch.db/"
                            "append_table_with_append_pt_branch/data-splits/"
                            "data_split-1";
    auto file_system = std::make_unique<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);
    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto fallback_data_split = std::dynamic_pointer_cast<FallbackDataSplit>(result);
    ASSERT_TRUE(fallback_data_split);
    // is fallback branch
    ASSERT_TRUE(fallback_data_split->IsFallback());

    auto result_data_split =
        std::dynamic_pointer_cast<DataSplitImpl>(fallback_data_split->GetSplit());
    ASSERT_TRUE(result_data_split);

    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-39204ff8-55b2-497b-8e87-a1c736799eab-0.parquet", /*file_size=*/619, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/
        SimpleStats::EmptyStats(),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({2, 210}, {2, 210}, {0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1755880762585ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-625b3277-84d3-4320-80b9-89a5075bf5fd-0.parquet", /*file_size=*/891, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/
        SimpleStats::EmptyStats(),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({2, 220, 2200}, {2, 220, 2200}, {0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/1,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1755884315889ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    std::vector<DataSplit::SimpleDataFileMeta> file_list;
    file_list.emplace_back(
        "data/parquet/append_table_with_append_pt_branch.db/append_table_with_append_pt_branch/"
        "pt=2/bucket-0/data-39204ff8-55b2-497b-8e87-a1c736799eab-0.parquet",
        619, 1, /*min_sequence_number=*/0,
        /*max_sequence_number=*/0, /*schema_id=*/0, /*level=*/0, Timestamp(1755880762585ll, 0),
        /*delete_row_count=*/0);
    file_list.emplace_back(
        "data/parquet/append_table_with_append_pt_branch.db/append_table_with_append_pt_branch/"
        "pt=2/bucket-0/data-625b3277-84d3-4320-80b9-89a5075bf5fd-0.parquet",
        891, 1, /*min_sequence_number=*/0,
        /*max_sequence_number=*/0, /*schema_id=*/1, /*level=*/0, Timestamp(1755884315889ll, 0),
        /*delete_row_count=*/0);
    ASSERT_EQ(fallback_data_split->GetFileList(), file_list);

    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRowGenerator::GenerateRow({2}, pool.get()),
        /*bucket=*/0, /*bucket_path=*/
        "data/parquet/append_table_with_append_pt_branch.db/append_table_with_append_pt_branch/"
        "pt=2/bucket-0",
        {file_meta1, file_meta2});
    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(builder.WithSnapshot(2)
                                                                            .WithTotalBuckets(-1)
                                                                            .IsStreaming(false)
                                                                            .RawConvertible(true)
                                                                            .Build()
                                                                            .value());
    ASSERT_EQ(*result_data_split, *expected_data_split)
        << result_data_split->ToString() << std::endl
        << expected_data_split->ToString();
}
}  // namespace paimon::test
