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

#include "paimon/table/source/data_split.h"

#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/data_define.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/data/timestamp.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(DataSplitTest, TestDeserializeVersion8WithWriteColsAndExternalPath) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/pk_dv_index_in_data_with_external.db/"
                            "pk_dv_index_in_data_with_external/data_splits/"
                            "data_split-02";
    auto file_system = std::make_unique<LocalFileSystem>();

    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);

    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto result_data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-72b62a5f-d698-4db5-b51a-04c0dc027702-0.orc", /*file_size=*/961, /*row_count=*/5,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alex", 0}, pool.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({"Tony", 0}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({"Alex", 0}, {"Tony", 0},
                                          {
                                              0,
                                              0,
                                          },
                                          pool.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alex", 10, 0, 12.1}, {"Tony", 10, 0, 17.1},
                                          {0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/5, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1757354415711ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/
        "FILE:/tmp/external/f1=10/bucket-1/data-72b62a5f-d698-4db5-b51a-04c0dc027702-0.orc",
        /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
        /*bucket=*/1, /*bucket_path=*/
        "data/orc/pk_dv_index_in_data_with_external.db/pk_dv_index_in_data_with_external/f1=10/"
        "bucket-1",
        {file_meta});

    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(
        builder.WithSnapshot(4)
            .WithTotalBuckets(2)
            .IsStreaming(false)
            .RawConvertible(true)
            .WithDataDeletionFiles({DeletionFile(
                "FILE:/tmp/external/f1=10/bucket-1/index-419e7c6b-9cad-49e8-9cd2-6187471df954-1",
                /*offset=*/1,
                /*length=*/22, /*cardinality=*/1)})
            .Build()
            .value());
    ASSERT_EQ(*result_data_split, *expected_data_split) << result_data_split->ToString();
    ASSERT_OK_AND_ASSIGN(std::string serialize_bytes, Split::Serialize(result_data_split, pool));
    ASSERT_EQ(serialize_bytes, std::string(split_bytes.data(), split_bytes.size()));
}

TEST(DataSplitTest, TestDeserializeVersion8WithWriteCols) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/pk_dv_index_not_in_data_no_external.db/"
                            "pk_dv_index_not_in_data_no_external/data_splits/"
                            "data_split-02";
    auto file_system = std::make_unique<LocalFileSystem>();

    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);

    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto result_data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-aa87291d-2a90-4846-b106-1bb4c76d74db-0.orc", /*file_size=*/961, /*row_count=*/5,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alex", 0}, pool.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({"Tony", 0}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({"Alex", 0}, {"Tony", 0},
                                          {
                                              0,
                                              0,
                                          },
                                          pool.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alex", 10, 0, 12.1}, {"Tony", 10, 0, 17.1},
                                          {0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/5, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1757349273246ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
        /*bucket=*/1, /*bucket_path=*/
        "data/orc/pk_dv_index_not_in_data_no_external.db/pk_dv_index_not_in_data_no_external/f1=10/"
        "bucket-1",
        {file_meta});

    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(
        builder.WithSnapshot(4)
            .WithTotalBuckets(2)
            .IsStreaming(false)
            .RawConvertible(true)
            .WithDataDeletionFiles({DeletionFile("data/orc/pk_dv_index_not_in_data_no_external.db/"
                                                 "pk_dv_index_not_in_data_no_external/index/"
                                                 "index-aa60193d-d7cd-434f-bc1a-c1adb210e1f7-1",
                                                 /*offset=*/1,
                                                 /*length=*/22, /*cardinality=*/1)})
            .Build()
            .value());
    ASSERT_EQ(*result_data_split, *expected_data_split) << result_data_split->ToString();
    ASSERT_OK_AND_ASSIGN(std::string serialize_bytes, Split::Serialize(result_data_split, pool));
    ASSERT_EQ(serialize_bytes, std::string(split_bytes.data(), split_bytes.size()));
}

TEST(DataSplitTest, TestDeserializeVersion7WithFirstRowId) {
    // test data split with version 7
    std::string file_name =
        paimon::test::GetDataDir() +
        "/orc/append_table_with_first_row_id.db/append_table_with_first_row_id/data_splits/"
        "data_split-02";
    auto file_system = std::make_unique<LocalFileSystem>();

    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);

    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto result_data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-92480a4b-ec0b-4585-883a-a99679870c4d-0.orc", /*file_size=*/653, /*row_count=*/5,
        /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/
        SimpleStats::EmptyStats(),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alex", 110, 0, 11.1}, {"Tony", 110, 1, 16.1},
                                          {0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/2, /*max_sequence_number=*/2, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1754073518741ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/5, /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRow::EmptyRow(),
        /*bucket=*/0, /*bucket_path=*/
        "data/append_table_with_support_first_row_id.db/append_table_with_support_first_row_id/"
        "bucket-0",
        {file_meta});

    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(builder.WithSnapshot(2)
                                                                            .WithTotalBuckets(-1)
                                                                            .IsStreaming(false)
                                                                            .RawConvertible(true)
                                                                            .Build()
                                                                            .value());
    ASSERT_EQ(*result_data_split, *expected_data_split);
    ASSERT_OK(Split::Serialize(result_data_split, pool));
}

TEST(DataSplitTest, TestDeserializeVersion7WithNullFirstRowId) {
    // test data split with version 7
    std::string file_name =
        paimon::test::GetDataDir() +
        "/orc/append_table_with_first_row_id.db/append_table_with_first_row_id/data_splits/"
        "data_split-01";
    auto file_system = std::make_unique<LocalFileSystem>();

    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);
    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto result_data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-16bd83f7-282a-479a-9968-0868436516b0-0.orc", /*file_size=*/567, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/
        SimpleStats::EmptyStats(),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 10, 1, 11.1}, {"Alice", 10, 1, 11.1},
                                          {0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1754068646844ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
        /*bucket=*/0, /*bucket_path=*/
        "data/append_table_with_first_row_id.db/append_table_with_first_row_id/f1=10/bucket-0",
        {file_meta});

    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(builder.WithSnapshot(1)
                                                                            .WithTotalBuckets(2)
                                                                            .IsStreaming(false)
                                                                            .RawConvertible(true)
                                                                            .Build()
                                                                            .value());
    ASSERT_EQ(*result_data_split, *expected_data_split);
    ASSERT_OK(Split::Serialize(result_data_split, pool));
}

TEST(DataSplitTest, TestDeserializeVersion6PkWithTotalBuckets) {
    // test data split with version 6
    std::string file_name =
        paimon::test::GetDataDir() +
        "/orc/pk_table_with_total_buckets.db/pk_table_with_total_buckets/data_splits/"
        "data_split-01";
    auto file_system = std::make_unique<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);
    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto result_data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-d7725088-6bd4-4e70-9ce6-714ae93b47cc-0.orc", /*file_size=*/863, /*row_count=*/1,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alice", 1}, pool.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({"Alice", 1}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 1}, {"Alice", 1}, {0, 0}, pool.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 10, 1, 11.1}, {"Alice", 10, 1, 11.1},
                                          {0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1743525392885ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
        /*bucket=*/0, /*bucket_path=*/
        "data/pk_table_with_total_buckets.db/pk_table_with_total_buckets/f1=10/bucket-0",
        {file_meta});

    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(builder.WithSnapshot(1)
                                                                            .WithTotalBuckets(2)
                                                                            .IsStreaming(false)
                                                                            .RawConvertible(true)
                                                                            .Build()
                                                                            .value());
    ASSERT_EQ(*result_data_split, *expected_data_split);
    ASSERT_OK(Split::Serialize(result_data_split, pool));
}

TEST(DataSplitTest, TestDeserializeVersion5PkWithExternalPath) {
    // test data split with version 5
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_10_external_path.db/append_10_external_path/data_splits/"
                            "data_split-02";
    auto file_system = std::make_unique<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);
    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto result_data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0.orc", /*file_size=*/645, /*row_count=*/5,
        BinaryRow::EmptyRow(), BinaryRow::EmptyRow(), SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({"Alice", 10, 0, 11.1}, {"Tony", 20, 1, 14.1},
                                          {0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1737111915429ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/"file:/tmp/bucket-0/data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0.orc",
        /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRow::EmptyRow(),
        /*bucket=*/0, /*bucket_path=*/
        "data/append_10_external_path2.db/append_10_external_path2/bucket-0", {file_meta});

    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(
        builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build().value());

    ASSERT_EQ(*result_data_split, *expected_data_split);

    std::vector<DataSplit::SimpleDataFileMeta> file_list = {DataSplit::SimpleDataFileMeta(
        {"file:/tmp/bucket-0/data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0.orc",
         /*file_size=*/645, /*row_count=*/5, /*min_sequence_number=*/0,
         /*max_sequence_number=*/4, /*schema_id=*/0, /*level=*/0,
         /*creation_time=*/Timestamp(1737111915429ll, 0), /*delete_row_count=*/0})};
    ASSERT_EQ(file_list, result_data_split->GetFileList());

    ASSERT_OK(Split::Serialize(result_data_split, pool));
}

TEST(DataSplitTest, TestDeserializeVersion5PkWithEmptyExternalPath) {
    // test data split with version 5
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_10_external_path.db/append_10_external_path/data_splits/"
                            "data_split-01";
    auto file_system = std::make_unique<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);
    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto result_data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-64d93fc3-eaf2-4253-9cff-a9faa701e207-0.orc", /*file_size=*/645, /*row_count=*/5,
        BinaryRow::EmptyRow(), BinaryRow::EmptyRow(), SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({"Alice", 10, 0, 11.1}, {"Tony", 20, 1, 14.1},
                                          {0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1737052260143ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRow::EmptyRow(),
        /*bucket=*/0, /*bucket_path=*/
        "data/append_10_external_path.db/append_10_external_path/bucket-0", {file_meta});

    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(
        builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build().value());

    ASSERT_EQ(*result_data_split, *expected_data_split);
    ASSERT_EQ(5, expected_data_split->PartialMergedRowCount());

    ASSERT_OK(Split::Serialize(result_data_split, pool));
}

TEST(DataSplitTest, TestDeserializeVersion4PkWithSnapshot4WithDvCardinality) {
    // test data split with version 4
    std::string file_name =
        paimon::test::GetDataDir() +
        "/orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality/data_splits/"
        "data_split-01";
    auto file_system = std::make_unique<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);
    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto result_data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-2ffe7ae9-2cf7-41e9-944b-2065585cde31-0.orc", /*file_size=*/1318, /*row_count=*/7,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alex", 0}, pool.get()),
        /*max_key=*/
        BinaryRowGenerator::GenerateRow(
            {"Whether I shall turn out to be the hero of my own life.", 0}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats(
            {"Alex", 0}, {"Whether I shall turn out to be the hero of my own life.", 0}, {0, 0},
            pool.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alex", 10, 0}, {"Whether I shall!", 10, 0}, {0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/6, /*schema_id=*/0,
        /*level=*/5, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1734707235578ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::optional<std::vector<std::string>>({"f0", "f1", "f2"}),
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
        /*bucket=*/1, /*bucket_path=*/
        "data/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality/f1=10/bucket-1",
        {file_meta});

    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(
        builder.WithSnapshot(4)
            .WithDataDeletionFiles(
                {DeletionFile("data/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality/"
                              "index/index-86356766-3238-46e6-990b-656cd7409eaa-1",
                              /*offset=*/1, /*length=*/24, /*cardinality=*/2)})
            .IsStreaming(false)
            .RawConvertible(true)
            .Build()
            .value());

    ASSERT_EQ(*result_data_split, *expected_data_split);
    ASSERT_EQ(5, expected_data_split->PartialMergedRowCount());
}

TEST(DataSplitTest, TestDeserializeVersion3AppendWithSnapshot1) {
    // test data split with version 3
    std::string file_name =
        paimon::test::GetDataDir() + "/orc/append_10.db/append_10/data_splits/data_split-01";
    auto file_system = std::make_unique<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);
    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto result_data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);

    auto file_meta = std::make_shared<DataFileMeta>(
        "data-de4e972f-5cc8-49b1-844e-374191534c68-0.orc", /*file_size=*/575, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({"Bob", 10, 0, 12.1}, {"Tony", 10, 0, 14.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1731404403198ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(/*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
                                   /*bucket=*/1, /*bucket_path=*/
                                   "data/append_10.db/append_10/f1=10/bucket-1", {file_meta});

    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(
        builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build().value());
    ASSERT_EQ(*result_data_split, *expected_data_split);
}

TEST(DataSplitTest, TestDeserializeVersion3AppendWithSnapshot1WithStatsDenseStore) {
    // test data split with version 3
    std::string file_name =
        paimon::test::GetDataDir() +
        "/orc/append_10_stats_dense_store.db/append_10_stats_dense_store/data_splits/"
        "data_split-01";
    auto file_system = std::make_unique<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);
    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto result_data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);

    auto file_meta = std::make_shared<DataFileMeta>(
        "data-c2613568-0412-4cd9-a0c4-1eae8e4ca89b-0.orc", /*file_size=*/575, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({"Bob", 10, 0}, {"Tony", 10, 0}, {0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1731412938891ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::optional<std::vector<std::string>>({"f0", "f1", "f2"}),
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
        /*bucket=*/1, /*bucket_path=*/
        "data/append_10_stats_dense_store.db/append_10_stats_dense_store/f1=10/bucket-1",
        {file_meta});

    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(
        builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build().value());
    ASSERT_EQ(*result_data_split, *expected_data_split);
}

TEST(DataSplitTest, TestDeserializeAppendWithSnapshot1) {
    // test multiple rows in one data file
    std::string file_name =
        paimon::test::GetDataDir() + "/orc/append_09.db/append_09/data_splits/data_split-01";
    auto file_system = std::make_unique<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);
    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto result_data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);

    auto file_meta = std::make_shared<DataFileMeta>(
        "data-4e30d6c0-f109-4300-a010-4ba03047dd9d-0.orc", /*file_size=*/575, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({"Bob", 10, 0, 12.1}, {"Tony", 10, 0, 14.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1721643142456ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
        /*bucket=*/1, /*bucket_path=*/
        "data/append_09.db/append_09/f1=10/bucket-1", {file_meta});

    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(
        builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build().value());
    ASSERT_EQ(*result_data_split, *expected_data_split);
}

TEST(DataSplitTest, TestDeserializeAppendWithSnapshot3) {
    // test with null value & multiple DataFileMeta
    std::string file_name =
        paimon::test::GetDataDir() + "/orc/append_09.db/append_09/data_splits/data_split-02";
    auto file_system = std::make_unique<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);
    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto result_data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);

    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc", /*file_size=*/541, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({"Lucy", 20, 1, 14.1}, {"Lucy", 20, 1, 14.1},
                                          {0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1721643142472ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);

    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc", /*file_size=*/506, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({"Paul", 20, 1, NullType()}, {"Paul", 20, 1, NullType()},
                                          {0, 0, 0, 1}, pool.get()),
        /*min_sequence_number=*/1, /*max_sequence_number=*/1, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1721643267404ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRowGenerator::GenerateRow({20}, pool.get()),
        /*bucket=*/0, /*bucket_path=*/
        "data/append_09.db/append_09/f1=20/bucket-0", {file_meta1, file_meta2});

    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(
        builder.WithSnapshot(3).IsStreaming(false).RawConvertible(true).Build().value());
    ASSERT_EQ(*result_data_split, *expected_data_split);

    std::vector<DataSplit::SimpleDataFileMeta> file_list = {
        DataSplit::SimpleDataFileMeta(
            {"data/append_09.db/append_09/f1=20/bucket-0/"
             "data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc",
             /*file_size=*/541, /*row_count=*/1, /*min_sequence_number=*/0,
             /*max_sequence_number=*/0, /*schema_id=*/0, /*level=*/0,
             /*creation_time=*/Timestamp(1721643142472ll, 0), /*delete_row_count=*/0}),
        DataSplit::SimpleDataFileMeta({"data/append_09.db/append_09/f1=20/bucket-0/"
                                       "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc",
                                       /*file_size=*/506, /*row_count=*/1,
                                       /*min_sequence_number=*/1,
                                       /*max_sequence_number=*/1, /*schema_id=*/0, /*level=*/0,
                                       /*creation_time=*/Timestamp(1721643267404ll, 0),
                                       /*delete_row_count=*/0})};

    ASSERT_EQ(file_list, result_data_split->GetFileList());
}

TEST(DataSplitTest, TestDeserializeAppendWithSnapshot5) {
    // test with full compaction & multiple DataFileMetas
    std::string file_name =
        paimon::test::GetDataDir() + "/orc/append_09.db/append_09/data_splits/data_split-03";
    auto file_system = std::make_unique<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);
    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto result_data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);

    auto file_meta = std::make_shared<DataFileMeta>(
        "data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc", /*file_size=*/640, /*row_count=*/8,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({"Alex", 10, 0, 12.1}, {"Tony", 10, 0, 17.1},
                                          {0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1721643834472ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Compact(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
        /*bucket=*/1, /*bucket_path=*/
        "data/append_09.db/append_09/f1=10/bucket-1", {file_meta});

    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(
        builder.WithSnapshot(5).IsStreaming(false).RawConvertible(true).Build().value());
    ASSERT_EQ(*result_data_split, *expected_data_split);
}

TEST(DataSplitTest, TestDeserializePkWithSnapshot2) {
    // test only one file in datasplit
    std::string file_name =
        paimon::test::GetDataDir() + "/orc/pk_09.db/pk_09/data_splits/data_split-01";
    auto file_system = std::make_unique<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);
    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto result_data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);

    auto file_meta = std::make_shared<DataFileMeta>(
        "data-980e82b4-2345-4976-bc1d-ea989fcdbffa-0.orc", /*file_size=*/1397, /*row_count=*/2,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alice", 1}, pool.get()), /*max_key=*/
        BinaryRowGenerator::GenerateRow(
            {"Two roads diverged in a wood, and I took the one less traveled by, And "
             "that has made all the difference.",
             1},
            pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 1}, {"Two roads diverh", 1}, {0, 0},
                                          pool.get()), /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 10, 1, 11.0}, {"Two roads diverh", 10, 1, 11.1},
                                          {0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/1, /*schema_id=*/0,
        /*level=*/5, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1725562946338ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
        /*bucket=*/0, /*bucket_path=*/
        "data/pk_09.db/pk_09/f1=10/bucket-0", {file_meta});

    auto expected_data_split =
        std::dynamic_pointer_cast<DataSplitImpl>(builder.WithSnapshot(2)
                                                     .WithDataDeletionFiles({std::nullopt})
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());
    ASSERT_EQ(*result_data_split, *expected_data_split);
}

TEST(DataSplitTest, TestDeserializePkWithSnapshot6OfSingleFile) {
    // test data split with deletion vector info
    std::string file_name =
        paimon::test::GetDataDir() + "/orc/pk_09.db/pk_09/data_splits/data_split-02";
    auto file_system = std::make_unique<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);
    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto result_data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);

    auto file_meta = std::make_shared<DataFileMeta>(
        "data-6871b960-edd9-40fc-9859-aaca9ea205cf-0.orc", /*file_size=*/887, /*row_count=*/5,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alex", 0}, pool.get()), /*max_key=*/
        BinaryRowGenerator::GenerateRow({"Tony", 0}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({"Alex", 0}, {"Tony", 0}, {0, 0},
                                          pool.get()), /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alex", 10, 0, 12.1}, {"Tony", 10, 0, 17.1},
                                          {0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/5, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1725562946453ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(/*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
                                   /*bucket=*/1, /*bucket_path=*/
                                   "data/pk_09.db/pk_09/f1=10/bucket-1", {file_meta});

    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(
        builder.WithSnapshot(6)
            .WithDataDeletionFiles({DeletionFile(
                "data/pk_09.db/pk_09/index/index-7badd250-6c0b-49e9-8e40-2449ae9a2539-1",
                /*offset=*/1, /*length=*/22, /*cardinality=*/std::nullopt)})
            .IsStreaming(false)
            .RawConvertible(true)
            .Build()
            .value());
    ASSERT_EQ(*result_data_split, *expected_data_split);
    ASSERT_EQ(0, expected_data_split->PartialMergedRowCount());
}

TEST(DataSplitTest, TestDeserializePkWithSnapshot6OfMultiFiles) {
    // test multiple files in data splits (a deletion file contains multiple bitmaps)
    std::string file_name =
        paimon::test::GetDataDir() + "/orc/pk_09.db/pk_09/data_splits/data_split-03";
    auto file_system = std::make_unique<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);
    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto result_data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);

    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-980e82b4-2345-4976-bc1d-ea989fcdbffa-0.orc", /*file_size=*/1397, /*row_count=*/2,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alice", 1}, pool.get()), /*max_key=*/
        BinaryRowGenerator::GenerateRow(
            {"Two roads diverged in a wood, and I took the one less traveled by, And "
             "that has made all the difference.",
             1},
            pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 1}, {"Two roads diverh", 1}, {0, 0},
                                          pool.get()), /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 10, 1, 11.0}, {"Two roads diverh", 10, 1, 11.1},
                                          {0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/1, /*schema_id=*/0,
        /*level=*/5, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1725562946338ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);

    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-1c7a85f1-55bd-424f-b503-34a33be0fb96-0.orc", /*file_size=*/1148, /*row_count=*/2,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Lily", 1}, pool.get()), /*max_key=*/
        BinaryRowGenerator::GenerateRow(
            {"Whether I shall turn out to be the hero of my own life.", 1}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({"Lily", 1}, {"Whether I shall!", 1}, {0, 0},
                                          pool.get()), /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Lily", 10, 1, 19.1}, {"Whether I shall!", 10, 1, 20.1},
                                          {0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/2, /*max_sequence_number=*/3, /*schema_id=*/0,
        /*level=*/4, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1725563027164ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);

    auto file_meta3 = std::make_shared<DataFileMeta>(
        "data-8cdb8b8d-5830-4b3b-aa94-8a30c449277a-0.orc", /*file_size=*/810, /*row_count=*/1,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alice", 1}, pool.get()), /*max_key=*/
        BinaryRowGenerator::GenerateRow({"Alice", 1}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 1}, {"Alice", 1}, {0, 0},
                                          pool.get()), /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 10, 1, 19.1}, {"Alice", 10, 1, 19.1},
                                          {0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/4, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/3, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1725563103281ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Compact(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
        /*bucket=*/0, /*bucket_path=*/
        "data/pk_09.db/pk_09/f1=10/bucket-0", {file_meta1, file_meta2, file_meta3});

    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(
        builder.WithSnapshot(6)
            .WithDataDeletionFiles(
                {DeletionFile(
                     "data/pk_09.db/pk_09/index/index-7badd250-6c0b-49e9-8e40-2449ae9a2539-0",
                     /*offset=*/31, /*length=*/22, /*cardinality=*/std::nullopt),
                 DeletionFile(
                     "data/pk_09.db/pk_09/index/index-7badd250-6c0b-49e9-8e40-2449ae9a2539-0",
                     /*offset=*/1, /*length=*/22, /*cardinality=*/std::nullopt),
                 std::nullopt})
            .IsStreaming(false)
            .RawConvertible(true)
            .Build()
            .value());
    ASSERT_EQ(*result_data_split, *expected_data_split);
}

TEST(DataSplitTest, TestDeserializePkWithSnapshot8) {
    // test the data split of merged data file
    std::string file_name =
        paimon::test::GetDataDir() + "/orc/pk_09.db/pk_09/data_splits/data_split-04";
    auto file_system = std::make_unique<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);
    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto result_data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);

    auto file_meta = std::make_shared<DataFileMeta>(
        "data-79413425-25fd-426f-a8a3-618d57f0e9a9-0.orc", /*file_size=*/1295, /*row_count=*/3,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alice", 1}, pool.get()), /*max_key=*/
        BinaryRowGenerator::GenerateRow(
            {"Whether I shall turn out to be the hero of my own life.", 1}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 1}, {"Whether I shall!", 1}, {0, 0},
                                          pool.get()), /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 10, 1, 11.0}, {"Whether I shall!", 10, 1, 21.1},
                                          {0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/5, /*schema_id=*/0,
        /*level=*/5, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1725629198650ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Compact(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(/*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
                                   /*bucket=*/0, /*bucket_path=*/
                                   "data/pk_09.db/pk_09/f1=10/bucket-0", {file_meta});

    auto expected_data_split =
        std::dynamic_pointer_cast<DataSplitImpl>(builder.WithSnapshot(8)
                                                     .WithDataDeletionFiles({std::nullopt})
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());
    ASSERT_EQ(*result_data_split, *expected_data_split);
}

TEST(DataSplitTest, TestDeserializePk10WithSnapshot6) {
    // test serialize and deserialize datasplit with deletion file and dense_stats
    std::string file_name =
        paimon::test::GetDataDir() +
        "/orc/pk_table_with_alter_table.db/pk_table_with_alter_table/data_splits/"
        "data_split-01";
    auto file_system = std::make_unique<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    ASSERT_TRUE(input_stream);
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);
    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> result,
                         Split::Deserialize((char*)split_bytes.data(), split_bytes.size(), pool));
    auto result_data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);

    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-3842c1d6-6b34-4b2c-a648-9e95b4fb941b-0.orc", /*file_size=*/1165, /*row_count=*/7,
        /*min_key=*/BinaryRowGenerator::GenerateRow({22}, pool.get()), /*max_key=*/
        BinaryRowGenerator::GenerateRow({82}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({22}, {82}, {0}, pool.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({1, 1, 22, 23, 24, 25, 26, "Alex"},
                                          {1, 1, 82, 83, 84, 85, 86, "Whether I shall!"},
                                          {0, 0, 0, 0, 0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/6, /*schema_id=*/0,
        /*level=*/5, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1730544332215ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-d6d370f3-242b-45c9-8739-44bf31b2b449-0.orc", /*file_size=*/924, /*row_count=*/1,
        /*min_key=*/BinaryRowGenerator::GenerateRow({52}, pool.get()), /*max_key=*/
        BinaryRowGenerator::GenerateRow({52}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({52}, {52}, {0}, pool.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({1, "Alex", 52, 514, 518, 516, 1, 519},
                                          {1, "Alex", 52, 514, 518, 516, 1, 519},
                                          {0, 0, 0, 0, 0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/7, /*max_sequence_number=*/7, /*schema_id=*/1,
        /*level=*/4, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1730547649721ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRowGenerator::GenerateRow({1, 1}, pool.get()), /*bucket=*/0,
        /*bucket_path=*/
        "data/pk_table_with_alter_table.db/pk_table_with_alter_table/key0=1/key1=1/bucket-0",
        {file_meta1, file_meta2});

    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(
        builder.WithSnapshot(6)
            .WithDataDeletionFiles(
                {DeletionFile("data/pk_table_with_alter_table.db/pk_table_with_alter_table/index/"
                              "index-51804749-ed6c-4e7b-b3e9-337cfe38499c-1",
                              /*offset=*/1, /*length=*/26, /*cardinality=*/std::nullopt),
                 std::nullopt})
            .IsStreaming(false)
            .RawConvertible(true)
            .Build()
            .value());
    ASSERT_EQ(*result_data_split, *expected_data_split);
}

TEST(DataSplitTest, TestPartialMergedRowCount) {
    auto pool = GetDefaultPool();
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-0.orc", /*file_size=*/100, /*row_count=*/2,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alice", 1}, pool.get()), /*max_key=*/
        BinaryRowGenerator::GenerateRow({"David", 1}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 1}, {"David", 1}, {0, 0},
                                          pool.get()), /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 10, 1, 11.0}, {"David", 10, 1, 11.1},
                                          {0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/1, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1725562946338ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-1.orc", /*file_size=*/100, /*row_count=*/2,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Bob", 1}, pool.get()), /*max_key=*/
        BinaryRowGenerator::GenerateRow({"David", 1}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({"Bob", 1}, {"David", 1}, {0, 0},
                                          pool.get()), /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Bob", 10, 1, 11.0}, {"David", 10, 1, 11.1},
                                          {0, 0, 0, 0}, pool.get()),
        /*min_sequence_number=*/2, /*max_sequence_number=*/3, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1725562947338ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
        /*bucket=*/0, /*bucket_path=*/
        "fake_table/f1=10/bucket-0", {file_meta, file_meta2});

    auto expected_data_split =
        std::dynamic_pointer_cast<DataSplitImpl>(builder.WithSnapshot(1)
                                                     .WithDataDeletionFiles({std::nullopt})
                                                     .IsStreaming(false)
                                                     .RawConvertible(false)
                                                     .Build()
                                                     .value());
    ASSERT_EQ(0, expected_data_split->PartialMergedRowCount());
}

}  // namespace paimon::test
