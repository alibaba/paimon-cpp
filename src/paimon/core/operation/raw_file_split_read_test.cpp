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

#include "paimon/core/operation/raw_file_split_read.h"

#include <optional>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/data/timestamp.h"
#include "paimon/executor.h"
#include "paimon/format/file_format.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/read_context.h"
#include "paimon/status.h"
#include "paimon/table/source/data_split.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class RawFileSplitReadTest : public ::testing::Test {
    std::vector<std::shared_ptr<DataSplit>> PrepareDataSplits() const {
        auto meta1 = std::make_shared<DataFileMeta>(
            "data-01b6a930-6564-409b-b8f4-ed1307790d72-0.orc", /*file_size=*/575, /*row_count=*/3,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            BinaryRowGenerator::GenerateStats({"Bob", 10, 0, 12.1}, {"Tony", 10, 0, 14.1},
                                              {0, 0, 0, 0}, pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1728497439433ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        DataSplitImpl::Builder builder1(BinaryRowGenerator::GenerateRow({10, 0}, pool_.get()),
                                        /*bucket=*/0, /*bucket_path=*/
                                        paimon::test::GetDataDir() +
                                            "/orc/multi_partition_append_table.db/"
                                            "multi_partition_append_table/f1=10/f2=0/bucket-0",
                                        {meta1});
        EXPECT_OK_AND_ASSIGN(
            auto data_split1,
            builder1.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());

        auto meta2 = std::make_shared<DataFileMeta>(
            "data-b79de94d-abe4-47d6-8e6c-911816487252-0.orc", /*file_size=*/541, /*row_count=*/1,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            BinaryRowGenerator::GenerateStats({"Lucy", 20, 1, 14.1}, {"Lucy", 20, 1, 14.1},
                                              {0, 0, 0, 0}, pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1728497439453ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        DataSplitImpl::Builder builder2(BinaryRowGenerator::GenerateRow({20, 1}, pool_.get()),
                                        /*bucket=*/0, /*bucket_path=*/
                                        paimon::test::GetDataDir() +
                                            "/orc/multi_partition_append_table.db/"
                                            "multi_partition_append_table/f1=20/f2=1/bucket-0",
                                        {meta2});
        EXPECT_OK_AND_ASSIGN(
            auto data_split2,
            builder2.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());

        auto meta3 = std::make_shared<DataFileMeta>(
            "data-955cbedd-ffcc-4234-8b98-4c3f08f78309-0.orc", /*file_size=*/543, /*row_count=*/1,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            BinaryRowGenerator::GenerateStats({"Alice", 10, 1, 11.1}, {"Alice", 10, 1, 11.1},
                                              {0, 0, 0, 0}, pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1728497439469ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        DataSplitImpl::Builder builder3(BinaryRowGenerator::GenerateRow({10, 1}, pool_.get()),
                                        /*bucket=*/0, /*bucket_path=*/
                                        paimon::test::GetDataDir() +
                                            "/orc/multi_partition_append_table.db/"
                                            "multi_partition_append_table/f1=10/f2=1/bucket-0",
                                        {meta3});
        EXPECT_OK_AND_ASSIGN(
            auto data_split3,
            builder3.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());
        std::vector<std::shared_ptr<DataSplit>> data_splits = {data_split1, data_split2,
                                                               data_split3};
        return data_splits;
    }

    void CheckReadResult(const std::shared_ptr<arrow::Schema>& read_schema,
                         const std::shared_ptr<arrow::ChunkedArray>& expected_array) const {
        std::string path = paimon::test::GetDataDir() +
                           "/orc/multi_partition_append_table.db/"
                           "multi_partition_append_table";
        ReadContextBuilder context_builder(path);
        context_builder.SetReadSchema(read_schema->field_names());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, context_builder.Finish());
        SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
        ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));

        ASSERT_OK_AND_ASSIGN(auto internal_context,
                             InternalReadContext::Create(std::move(read_context), table_schema,
                                                         table_schema->Options()));
        auto data_splits = PrepareDataSplits();
        const auto& core_options = internal_context->GetCoreOptions();
        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
        ASSERT_OK_AND_ASSIGN(std::vector<std::string> external_paths,
                             core_options.CreateExternalPaths());
        ASSERT_OK_AND_ASSIGN(std::optional<std::string> global_index_external_path,
                             core_options.CreateGlobalIndexExternalPath());

        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FileStorePathFactory> path_factory,
            FileStorePathFactory::Create(
                internal_context->GetPath(), arrow_schema, table_schema->PartitionKeys(),
                core_options.GetPartitionDefaultName(),
                core_options.GetWriteFileFormat()->Identifier(), core_options.DataFilePrefix(),
                core_options.LegacyPartitionNameEnabled(), external_paths,
                global_index_external_path, core_options.IndexFileInDataFileDir(), pool_));
        auto split_read =
            std::make_unique<RawFileSplitRead>(path_factory, std::move(internal_context), pool_,
                                               CreateDefaultExecutor(/*thread_count=*/2));

        std::vector<std::unique_ptr<BatchReader>> batch_readers;
        batch_readers.reserve(data_splits.size());
        for (const auto& split : data_splits) {
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader,
                                 split_read->CreateReader(split));
            batch_readers.emplace_back(std::move(reader));
        }
        auto batch_reader = std::make_unique<ConcatBatchReader>(std::move(batch_readers), pool_);
        ASSERT_OK_AND_ASSIGN(auto result_array,
                             ReadResultCollector::CollectResult(batch_reader.get()));
        ASSERT_TRUE(result_array->Equals(expected_array));
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

// test simple, recall all columns with sequence in data
TEST_F(RawFileSplitReadTest, TestCreateReader) {
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
      [0, "Bob", 10, 0, 12.1],
      [0, "Emily", 10, 0, 13.1],
      [0, "Tony", 10, 0, 14.1],
      [0, "Lucy", 20, 1, 14.1],
      [0, "Alice", 10, 1, 11.1]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckReadResult(read_schema, expected_array);
}

// recall all columns with reverse sequence in data
TEST_F(RawFileSplitReadTest, TestCreateReaderWithReserveSequence) {
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
      [0, 12.1, 0, 10, "Bob"],
      [0, 13.1, 0, 10, "Emily"],
      [0, 14.1, 0, 10, "Tony"],
      [0, 14.1, 1, 20, "Lucy"],
      [0, 11.1, 1, 10, "Alice"]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckReadResult(read_schema, expected_array);
}

// recall all partition columns with sequence in data
TEST_F(RawFileSplitReadTest, TestCreateReaderWithPartitionKeys) {
    std::vector<DataField> read_fields = {DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
      [0, 10, 0],
      [0, 10, 0],
      [0, 10, 0],
      [0, 20, 1],
      [0, 10, 1]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckReadResult(read_schema, expected_array);
}

// recall all partition columns with reverse sequence in data
TEST_F(RawFileSplitReadTest, TestCreateReaderWithPartitionKeysWithReverseSequence) {
    std::vector<DataField> read_fields = {DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(1, arrow::field("f1", arrow::int32()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
      [0, 0, 10],
      [0, 0, 10],
      [0, 0, 10],
      [0, 1, 20],
      [0, 1, 10]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckReadResult(read_schema, expected_array);
}

// recall part partition keys with sequence in data
TEST_F(RawFileSplitReadTest, TestCreateReaderWithPartPartition) {
    std::vector<DataField> read_fields = {DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
      [0, 0, 12.1],
      [0, 0, 13.1],
      [0, 0, 14.1],
      [0, 1, 14.1],
      [0, 1, 11.1]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckReadResult(read_schema, expected_array);
}

// recall part partition keys with reverse sequence in data
TEST_F(RawFileSplitReadTest, TestCreateReaderWithPartPartitionWithReserveSequence) {
    std::vector<DataField> read_fields = {DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
      [0, 10, "Bob"],
      [0, 10, "Emily"],
      [0, 10, "Tony"],
      [0, 20, "Lucy"],
      [0, 10, "Alice"]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckReadResult(read_schema, expected_array);
}

// recall non partition columns with sequence in data
TEST_F(RawFileSplitReadTest, TestCreateReaderWithNonPartition) {
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
      [0, "Bob", 12.1],
      [0, "Emily", 13.1],
      [0, "Tony", 14.1],
      [0, "Lucy", 14.1],
      [0, "Alice", 11.1]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckReadResult(read_schema, expected_array);
}

// recall non partition columns with reverse sequence in data
TEST_F(RawFileSplitReadTest, TestCreateReaderWithNonPartitionWithReserveSequence) {
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
      [0, 12.1, "Bob"],
      [0, 13.1, "Emily"],
      [0, 14.1, "Tony"],
      [0, 14.1, "Lucy"],
      [0, 11.1, "Alice"]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckReadResult(read_schema, expected_array);
}

TEST_F(RawFileSplitReadTest, TestEmptyPlan) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/multi_partition_append_table.db/"
                       "multi_partition_append_table";
    ReadContextBuilder context_builder(path);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));

    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    const auto& core_options = internal_context->GetCoreOptions();
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> external_paths,
                         core_options.CreateExternalPaths());
    ASSERT_OK_AND_ASSIGN(std::optional<std::string> global_index_external_path,
                         core_options.CreateGlobalIndexExternalPath());

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            internal_context->GetPath(), arrow_schema, table_schema->PartitionKeys(),
            core_options.GetPartitionDefaultName(), core_options.GetWriteFileFormat()->Identifier(),
            core_options.DataFilePrefix(), core_options.LegacyPartitionNameEnabled(),
            external_paths, global_index_external_path, core_options.IndexFileInDataFileDir(),
            pool_));

    auto split_read =
        std::make_unique<RawFileSplitRead>(path_factory, std::move(internal_context), pool_,
                                           CreateDefaultExecutor(/*thread_count=*/2));
    DataSplitImpl::Builder builder(BinaryRowGenerator::GenerateRow({10, 0}, pool_.get()),
                                   /*bucket=*/0, /*bucket_path=*/
                                   paimon::test::GetDataDir() +
                                       "/orc/multi_partition_append_table.db/"
                                       "multi_partition_append_table/f1=10/f2=0/bucket-0",
                                   {});
    ASSERT_OK_AND_ASSIGN(auto data_split,
                         builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());

    std::vector<std::unique_ptr<BatchReader>> batch_readers;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader, split_read->CreateReader(data_split));
    batch_readers.push_back(std::move(reader));
    auto batch_reader = std::make_unique<ConcatBatchReader>(std::move(batch_readers), pool_);
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));
    ASSERT_EQ(result_array, nullptr);
}

TEST_F(RawFileSplitReadTest, TestMatch) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/pk_table_with_total_buckets.db/pk_table_with_total_buckets";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema({"f0", "f1", "f2", "f3"});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InternalReadContext> internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    auto split_read =
        std::make_unique<RawFileSplitRead>(/*path_factory=*/nullptr, std::move(internal_context),
                                           pool_, CreateDefaultExecutor(/*thread_count=*/2));
    auto create_data_split = [this](bool is_streaming,
                                    bool raw_convertible) -> std::shared_ptr<DataSplit> {
        auto meta = std::make_shared<DataFileMeta>(
            "data-d7725088-6bd4-4e70-9ce6-714ae93b47cc-0.orc", /*file_size=*/863, /*row_count=*/1,
            /*min_key=*/BinaryRowGenerator::GenerateRow({"Alice", 1}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({"Alice", 1}, pool_.get()),
            /*key_stats=*/
            BinaryRowGenerator::GenerateStats({"Alice", 1}, {"Alice", 1}, {0, 0}, pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats({"Alice", 10, 1, 11.1}, {"Alice", 10, 1, 11.1},
                                              {0, 0, 0, 0}, pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1743525392885ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        DataSplitImpl::Builder builder(BinaryRowGenerator::GenerateRow({10, 0}, pool_.get()),
                                       /*bucket=*/0, /*bucket_path=*/
                                       paimon::test::GetDataDir() +
                                           "/orc/multi_partition_append_table.db/"
                                           "multi_partition_append_table/f1=10/f2=0/bucket-0",
                                       {meta});
        return builder.WithSnapshot(1)
            .IsStreaming(is_streaming)
            .RawConvertible(raw_convertible)
            .Build()
            .value();
    };
    {
        auto data_split = create_data_split(/*is_streaming=*/false, /*raw_convertible=*/true);
        auto split_impl = std::dynamic_pointer_cast<DataSplitImpl>(data_split);
        ASSERT_TRUE(split_impl);
        ASSERT_OK_AND_ASSIGN(bool match_result,
                             split_read->Match(data_split, /*force_keep_delete=*/true));
        ASSERT_FALSE(match_result);
    }
    {
        auto data_split = create_data_split(/*is_streaming=*/false, /*raw_convertible=*/true);
        auto split_impl = std::dynamic_pointer_cast<DataSplitImpl>(data_split);
        ASSERT_TRUE(split_impl);
        ASSERT_OK_AND_ASSIGN(bool match_result,
                             split_read->Match(data_split, /*force_keep_delete=*/false));
        ASSERT_TRUE(match_result);
    }
    {
        auto data_split = create_data_split(/*is_streaming=*/true, /*raw_convertible=*/true);
        auto split_impl = std::dynamic_pointer_cast<DataSplitImpl>(data_split);
        ASSERT_TRUE(split_impl);
        ASSERT_OK_AND_ASSIGN(bool match_result,
                             split_read->Match(data_split, /*force_keep_delete=*/false));
        ASSERT_FALSE(match_result);
    }
    {
        auto data_split = create_data_split(/*is_streaming=*/false, /*raw_convertible=*/false);
        auto split_impl = std::dynamic_pointer_cast<DataSplitImpl>(data_split);
        ASSERT_TRUE(split_impl);
        ASSERT_OK_AND_ASSIGN(bool match_result,
                             split_read->Match(data_split, /*force_keep_delete=*/false));
        ASSERT_FALSE(match_result);
    }
    {
        ASSERT_NOK(split_read->Match(nullptr, /*force_keep_delete=*/false));
    }
}

}  // namespace paimon::test
