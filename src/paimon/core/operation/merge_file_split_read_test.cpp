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

#include "paimon/core/operation/merge_file_split_read.h"

#include <cstddef>
#include <map>
#include <optional>
#include <ostream>
#include <tuple>
#include <utility>
#include <variant>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/fields_comparator.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/table/source/data_split.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon {
class FileSystem;
}  // namespace paimon

namespace paimon::test {
// Parameter: min_heap/loser_tree; enable/disable IO prefetch; enable/disable multi thread row to
// batch
class MergeFileSplitReadTest : public ::testing::Test,
                               public ::testing::WithParamInterface<std::tuple<bool, bool, bool>> {
    void SetUp() override {}
    void TearDown() override {}

    void CheckResult(const std::shared_ptr<arrow::ChunkedArray>& result,
                     const std::shared_ptr<arrow::ChunkedArray>& expected,
                     const std::shared_ptr<arrow::Schema>& schema) const {
        if (!std::get<2>(GetParam())) {
            ASSERT_TRUE(result->ApproxEquals(*expected)) << result->ToString();
            return;
        }
        ASSERT_OK_AND_ASSIGN(auto sorted_result, ReadResultCollector::SortArray(result, schema));
        ASSERT_OK_AND_ASSIGN(auto sorted_expected,
                             ReadResultCollector::SortArray(expected, schema));
        ASSERT_TRUE(sorted_result->ApproxEquals(*sorted_expected))
            << sorted_result->ToString() << std::endl
            << sorted_expected->ToString();
    }

    std::shared_ptr<InternalReadContext> CreateInternalReadContext(
        const std::shared_ptr<ReadContext>& read_context, int32_t schema_id = 0) {
        SchemaManager schema_manager(fs_, read_context->GetPath());
        EXPECT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(schema_id));
        EXPECT_OK_AND_ASSIGN(auto context, InternalReadContext::Create(read_context, table_schema,
                                                                       read_context->GetOptions()));
        return context;
    }

    void AddOptions(ReadContextBuilder* context_builder) const {
        auto [use_min_heap, enable_io_prefetch, enable_multi_thread_row_to_batch] = GetParam();
        if (use_min_heap) {
            context_builder->AddOption(Options::SORT_ENGINE, "min-heap");
        } else {
            context_builder->AddOption(Options::SORT_ENGINE, "loser-tree");
        }
        if (enable_io_prefetch) {
            context_builder->AddOption("test.enable-adaptive-prefetch-strategy", "false");
            context_builder->EnablePrefetch(true);
            context_builder->SetPrefetchBatchCount(/*batch_count=*/3);
        } else {
            context_builder->EnablePrefetch(false);
        }
        if (enable_multi_thread_row_to_batch) {
            context_builder->EnableMultiThreadRowToBatch(true);
            context_builder->SetRowToBatchThreadNumber(4);
        } else {
            context_builder->EnableMultiThreadRowToBatch(false);
        }
    }

    // for table pk_table_with_mor
    std::vector<std::shared_ptr<DataSplit>> PrepareDataSplit() const {
        auto meta1_1 = std::make_shared<DataFileMeta>(
            "data-c80ccf0f-6387-4cbc-8889-ade8cef54c43-0.parquet", /*file_size=*/3346,
            /*row_count=*/4,
            /*min_key=*/BinaryRowGenerator::GenerateRow({0, 0}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({1, 1}, pool_.get()),
            /*key_stats=*/BinaryRowGenerator::GenerateStats({0, 0}, {1, 1}, {0, 0}, pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats(
                {0, 0, 0, 0, "apple", "!", static_cast<double>(10), false},
                {1, 1, 0, 0, "driver", "you", 13.3, true}, {0, 0, 0, 0, 0, 0, 0, 0}, pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/3, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1735149279565ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        auto meta1_2 = std::make_shared<DataFileMeta>(
            "data-c80ccf0f-6387-4cbc-8889-ade8cef54c43-1.parquet", /*file_size=*/3370,
            /*row_count=*/4,
            /*min_key=*/BinaryRowGenerator::GenerateRow({0, 0}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({1, 2}, pool_.get()),
            /*key_stats=*/BinaryRowGenerator::GenerateStats({0, 0}, {1, 2}, {0, 0}, pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats(
                {0, 0, 0, 0, "abandon", "!", static_cast<double>(110), false},
                {1, 2, 0, 0, "driver", "see", 112.2, true}, {0, 0, 0, 0, 0, 0, 0, 0}, pool_.get()),
            /*min_sequence_number=*/4, /*max_sequence_number=*/7, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1735149279917ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        auto meta1_3 = std::make_shared<DataFileMeta>(
            "data-c80ccf0f-6387-4cbc-8889-ade8cef54c43-2.parquet", /*file_size=*/3252,
            /*row_count=*/1,
            /*min_key=*/BinaryRowGenerator::GenerateRow({100, 200}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({100, 200}, pool_.get()),
            /*key_stats=*/
            BinaryRowGenerator::GenerateStats({100, 200}, {100, 200}, {0, 0}, pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats({100, 200, 0, 0, "max", "number", 140.4, false},
                                              {100, 200, 0, 0, "max", "number", 140.4, false},
                                              {0, 0, 0, 0, 0, 0, 0, 0}, pool_.get()),
            /*min_sequence_number=*/8, /*max_sequence_number=*/8, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1735230606999ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        DataSplitImpl::Builder builder1(BinaryRowGenerator::GenerateRow({0, 0}, pool_.get()),
                                        /*bucket=*/0, /*bucket_path=*/
                                        paimon::test::GetDataDir() +
                                            "/parquet/pk_table_with_mor.db/"
                                            "pk_table_with_mor/p0=0/p1=0/bucket-0",
                                        {meta1_1, meta1_2, meta1_3});
        EXPECT_OK_AND_ASSIGN(
            auto data_split1,
            builder1.WithSnapshot(3).IsStreaming(false).RawConvertible(false).Build());
        auto meta2_1 = std::make_shared<DataFileMeta>(
            "data-24f8588c-d950-4e44-9d99-a023ea65a136-0.parquet", /*file_size=*/3245,
            /*row_count=*/1,
            /*min_key=*/BinaryRowGenerator::GenerateRow({0, 1}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({0, 1}, pool_.get()),
            /*key_stats=*/BinaryRowGenerator::GenerateStats({0, 1}, {0, 1}, {0, 0}, pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats(
                {0, 1, 0, 1, "mouse", "you", static_cast<double>(30), false},
                {0, 1, 0, 1, "mouse", "you", static_cast<double>(30), false},
                {0, 0, 0, 0, 0, 0, 0, 0}, pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1735149279951ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        auto meta2_2 = std::make_shared<DataFileMeta>(
            "data-24f8588c-d950-4e44-9d99-a023ea65a136-1.parquet", /*file_size=*/3229,
            /*row_count=*/1,
            /*min_key=*/BinaryRowGenerator::GenerateRow({0, 1}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({0, 1}, pool_.get()),
            /*key_stats=*/BinaryRowGenerator::GenerateStats({0, 1}, {0, 1}, {0, 0}, pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats(
                {0, 1, 0, 1, "zoo", "you", static_cast<double>(130), false},
                {0, 1, 0, 1, "zoo", "you", static_cast<double>(130), false},
                {0, 0, 0, 0, 0, 0, 0, 0}, pool_.get()),
            /*min_sequence_number=*/1, /*max_sequence_number=*/1, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1735149279612ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        DataSplitImpl::Builder builder2(BinaryRowGenerator::GenerateRow({0, 1}, pool_.get()),
                                        /*bucket=*/0, /*bucket_path=*/
                                        paimon::test::GetDataDir() +
                                            "/parquet/pk_table_with_mor.db/"
                                            "pk_table_with_mor/p0=0/p1=1/bucket-0",
                                        {meta2_1, meta2_2});
        EXPECT_OK_AND_ASSIGN(
            auto data_split2,
            builder2.WithSnapshot(3).IsStreaming(false).RawConvertible(false).Build());

        auto meta3_1 = std::make_shared<DataFileMeta>(
            "data-184f2304-49fd-4916-ba07-037757e904eb-0.parquet", /*file_size=*/3259,
            /*row_count=*/1,
            /*min_key=*/BinaryRowGenerator::GenerateRow({0, 0}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({0, 0}, pool_.get()),
            /*key_stats=*/BinaryRowGenerator::GenerateStats({0, 0}, {0, 0}, {0, 0}, pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats(
                {0, 0, 1, 0, "elephant", "hi", static_cast<double>(120), false},
                {0, 0, 1, 0, "elephant", "hi", static_cast<double>(120), false},
                {0, 0, 0, 0, 0, 0, 0, 0}, pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1735149271981ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        auto meta3_2 = std::make_shared<DataFileMeta>(
            "data-184f2304-49fd-4916-ba07-037757e904eb-1.parquet", /*file_size=*/3259,
            /*row_count=*/1,
            /*min_key=*/BinaryRowGenerator::GenerateRow({0, 0}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({0, 0}, pool_.get()),
            /*key_stats=*/BinaryRowGenerator::GenerateStats({0, 0}, {0, 0}, {0, 0}, pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats(
                {0, 0, 1, 0, "elephant", "hi", static_cast<double>(20), true},
                {0, 0, 1, 0, "elephant", "hi", static_cast<double>(20), true},
                {0, 0, 0, 0, 0, 0, 0, 0}, pool_.get()),
            /*min_sequence_number=*/1, /*max_sequence_number=*/1, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1735149279651ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        DataSplitImpl::Builder builder3(BinaryRowGenerator::GenerateRow({1, 0}, pool_.get()),
                                        /*bucket=*/0, /*bucket_path=*/
                                        paimon::test::GetDataDir() +
                                            "/parquet/pk_table_with_mor.db/"
                                            "pk_table_with_mor/p0=1/p1=0/bucket-0",
                                        {meta3_1, meta3_2});
        EXPECT_OK_AND_ASSIGN(
            auto data_split3,
            builder3.WithSnapshot(3).IsStreaming(false).RawConvertible(false).Build());
        return {data_split1, data_split2, data_split3};
    }

    // for table pk_table_partial_update
    std::vector<std::shared_ptr<DataSplit>> PrepareDataSplit2() const {
        auto meta1_1 = std::make_shared<DataFileMeta>(
            "data-d03e13e5-5e2e-463a-b53a-8d44e4dc9141-0.parquet",
            /*file_size=*/2554, /*row_count=*/
            3,
            /*min_key=*/BinaryRowGenerator::GenerateRow({0, 1}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({1, 1}, pool_.get()),
            /*key_stats=*/BinaryRowGenerator::GenerateStats({0, 0}, {1, 1}, {0, 0}, pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats({0, 0, 2.0, false, "apple"},
                                              {1, 1, 2.0, true, "banana"}, {0, 0, 2, 0, 1},
                                              pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1736793059256ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        auto meta1_2 = std::make_shared<DataFileMeta>(
            "data-d03e13e5-5e2e-463a-b53a-8d44e4dc9141-1.parquet",
            /*file_size=*/2623, /*row_count=*/
            5,
            /*min_key=*/BinaryRowGenerator::GenerateRow({0, 1}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({2, 2}, pool_.get()),
            /*key_stats=*/BinaryRowGenerator::GenerateStats({0, 0}, {2, 2}, {0, 0}, pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats({0, 0, 100.0, false, "new_apple"},
                                              {2, 2, 144.4, true, "orange"}, {0, 0, 0, 0, 3},
                                              pool_.get()),
            /*min_sequence_number=*/3, /*max_sequence_number=*/7, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1736793059526ll, 0),
            /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        DataSplitImpl::Builder builder1(
            /*partition=*/BinaryRow::EmptyRow(),
            /*bucket=*/0, /*bucket_path=*/
            paimon::test::GetDataDir() +
                "/parquet/pk_table_partial_update.db/pk_table_partial_update/bucket-0",
            {meta1_1, meta1_2});
        EXPECT_OK_AND_ASSIGN(
            auto data_split1,
            builder1.WithSnapshot(2).IsStreaming(false).RawConvertible(false).Build());

        return {data_split1};
    }

    Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::shared_ptr<InternalReadContext>& internal_context,
        const std::vector<std::shared_ptr<DataSplit>>& data_splits) {
        const auto& core_options = internal_context->GetCoreOptions();
        const auto& table_schema = internal_context->GetTableSchema();
        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
        EXPECT_OK_AND_ASSIGN(std::vector<std::string> external_paths,
                             core_options.CreateExternalPaths());
        EXPECT_OK_AND_ASSIGN(std::optional<std::string> global_index_external_path,
                             core_options.CreateGlobalIndexExternalPath());

        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FileStorePathFactory> path_factory,
            FileStorePathFactory::Create(
                internal_context->GetPath(), arrow_schema, table_schema->PartitionKeys(),
                core_options.GetPartitionDefaultName(),
                core_options.GetWriteFileFormat()->Identifier(), core_options.DataFilePrefix(),
                core_options.LegacyPartitionNameEnabled(), external_paths,
                global_index_external_path, core_options.IndexFileInDataFileDir(), pool_));
        PAIMON_ASSIGN_OR_RAISE(auto split_read,
                               MergeFileSplitRead::Create(path_factory, std::move(internal_context),
                                                          pool_, executor_));
        std::vector<std::unique_ptr<BatchReader>> batch_readers;
        batch_readers.reserve(data_splits.size());
        for (const auto& split : data_splits) {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                                   split_read->CreateReader(split));
            batch_readers.emplace_back(std::move(reader));
        }
        return std::make_unique<ConcatBatchReader>(std::move(batch_readers), pool_);
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<FileSystem> fs_ = std::make_shared<LocalFileSystem>();
    std::shared_ptr<Executor> executor_ = CreateDefaultExecutor(/*thread_count=*/4);
};

// test GenerateKeyValueReadSchema with user define fields
TEST_F(MergeFileSplitReadTest, TestGenerateKeyValueReadSchema) {
    std::string table_path =
        paimon::test::GetDataDir() + "/parquet/pk_table_with_mor.db/pk_table_with_mor";
    auto schema_manager = std::make_unique<SchemaManager>(fs_, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager->ReadSchema(/*schema_id=*/0));
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::SEQUENCE_FIELD, "s0,s1"},
                                               {Options::MERGE_ENGINE, "deduplicate"},
                                               {Options::SORT_ENGINE, "min-heap"},
                                               {Options::IGNORE_DELETE, "true"}}));
    std::vector<DataField> raw_read_fields = {
        DataField(1, arrow::field("k1", arrow::int32(), /*nullable=*/false)),
        DataField(3, arrow::field("p1", arrow::int32(), /*nullable=*/false)),
        DataField(5, arrow::field("s1", arrow::utf8())),
        DataField(6, arrow::field("v0", arrow::float64())),
        DataField(7, arrow::field("v1", arrow::boolean()))};
    auto raw_read_schema = DataField::ConvertDataFieldsToArrowSchema(raw_read_fields);
    ASSERT_TRUE(raw_read_schema);

    std::shared_ptr<arrow::Schema> value_schema;
    std::shared_ptr<arrow::Schema> read_schema;
    std::shared_ptr<FieldsComparator> key_comparator;
    std::shared_ptr<FieldsComparator> interval_partition_comparator;
    std::shared_ptr<FieldsComparator> sequence_fields_comparator;
    ASSERT_OK(MergeFileSplitRead::GenerateKeyValueReadSchema(
        *table_schema, options, raw_read_schema, &value_schema, &read_schema, &key_comparator,
        &interval_partition_comparator, &sequence_fields_comparator));

    // check result
    std::vector<DataField> expected_value_fields = {
        DataField(1, arrow::field("k1", arrow::int32(), /*nullable=*/false)),
        DataField(3, arrow::field("p1", arrow::int32(), /*nullable=*/false)),
        DataField(5, arrow::field("s1", arrow::utf8())),
        DataField(6, arrow::field("v0", arrow::float64())),
        DataField(7, arrow::field("v1", arrow::boolean())),
        DataField(4, arrow::field("s0", arrow::utf8()))};
    auto expected_value_schema = DataField::ConvertDataFieldsToArrowSchema(expected_value_fields);

    ASSERT_OK_AND_ASSIGN(auto result_fields,
                         DataField::ConvertArrowSchemaToDataFields(value_schema));
    ASSERT_OK_AND_ASSIGN(auto expected_fields,
                         DataField::ConvertArrowSchemaToDataFields(expected_value_schema));
    for (size_t i = 0; i < expected_fields.size(); i++) {
        EXPECT_EQ(result_fields[i], expected_fields[i]);
        EXPECT_OK_AND_ASSIGN(std::string result_str, result_fields[i].ToJsonString());
        EXPECT_OK_AND_ASSIGN(std::string expected_str, expected_fields[i].ToJsonString());
    }
    ASSERT_TRUE(value_schema->Equals(*expected_value_schema));

    std::vector<DataField> expected_read_data_fields = {
        SpecialFields::SequenceNumber(),
        SpecialFields::ValueKind(),
        DataField(0, arrow::field("k0", arrow::int32(), /*nullable=*/false)),
        DataField(1, arrow::field("k1", arrow::int32(), /*nullable=*/false)),
        DataField(3, arrow::field("p1", arrow::int32(), /*nullable=*/false)),
        DataField(5, arrow::field("s1", arrow::utf8())),
        DataField(6, arrow::field("v0", arrow::float64())),
        DataField(7, arrow::field("v1", arrow::boolean())),
        DataField(4, arrow::field("s0", arrow::utf8()))};
    auto expected_read_schema =
        DataField::ConvertDataFieldsToArrowSchema(expected_read_data_fields);
    ASSERT_TRUE(read_schema->Equals(*expected_read_schema));

    std::vector<int32_t> expected_sort_key_fields = {0, 1};
    ASSERT_EQ(key_comparator->sort_fields_, expected_sort_key_fields);
    ASSERT_EQ(key_comparator->is_ascending_order_, true);
    ASSERT_EQ(interval_partition_comparator->sort_fields_, expected_sort_key_fields);
    ASSERT_EQ(interval_partition_comparator->is_ascending_order_, true);

    std::vector<int32_t> expected_sort_seq_fields = {5, 2};
    ASSERT_EQ(sequence_fields_comparator->sort_fields_, expected_sort_seq_fields);
    ASSERT_EQ(sequence_fields_comparator->is_ascending_order_, true);
}

// test GenerateKeyValueReadSchema without user define fields
TEST_F(MergeFileSplitReadTest, TestGenerateKeyValueReadSchema1) {
    std::string table_path =
        paimon::test::GetDataDir() + "/parquet/pk_table_with_mor.db/pk_table_with_mor";
    auto schema_manager = std::make_unique<SchemaManager>(fs_, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager->ReadSchema(/*schema_id=*/0));
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::MERGE_ENGINE, "deduplicate"},
                                               {Options::SORT_ENGINE, "min-heap"},
                                               {Options::IGNORE_DELETE, "true"}}));
    std::vector<DataField> raw_read_fields = {
        DataField(1, arrow::field("k1", arrow::int32(), /*nullable=*/false)),
        DataField(3, arrow::field("p1", arrow::int32(), /*nullable=*/false)),
        DataField(5, arrow::field("s1", arrow::utf8())),
        DataField(6, arrow::field("v0", arrow::float64())),
        DataField(7, arrow::field("v1", arrow::boolean()))};
    auto raw_read_schema = DataField::ConvertDataFieldsToArrowSchema(raw_read_fields);
    ASSERT_TRUE(raw_read_schema);

    std::shared_ptr<arrow::Schema> value_schema;
    std::shared_ptr<arrow::Schema> read_schema;
    std::shared_ptr<FieldsComparator> key_comparator;
    std::shared_ptr<FieldsComparator> interval_partition_comparator;
    std::shared_ptr<FieldsComparator> sequence_fields_comparator;
    ASSERT_OK(MergeFileSplitRead::GenerateKeyValueReadSchema(
        *table_schema, options, raw_read_schema, &value_schema, &read_schema, &key_comparator,
        &interval_partition_comparator, &sequence_fields_comparator));

    // check result
    std::vector<DataField> expected_value_fields = {
        DataField(1, arrow::field("k1", arrow::int32(), /*nullable=*/false)),
        DataField(3, arrow::field("p1", arrow::int32(), /*nullable=*/false)),
        DataField(5, arrow::field("s1", arrow::utf8())),
        DataField(6, arrow::field("v0", arrow::float64())),
        DataField(7, arrow::field("v1", arrow::boolean()))};
    auto expected_value_schema = DataField::ConvertDataFieldsToArrowSchema(expected_value_fields);
    ASSERT_TRUE(value_schema->Equals(*expected_value_schema));

    std::vector<DataField> expected_read_data_fields = {
        SpecialFields::SequenceNumber(),
        SpecialFields::ValueKind(),
        DataField(0, arrow::field("k0", arrow::int32(), /*nullable=*/false)),
        DataField(1, arrow::field("k1", arrow::int32(), /*nullable=*/false)),
        DataField(3, arrow::field("p1", arrow::int32(), /*nullable=*/false)),
        DataField(5, arrow::field("s1", arrow::utf8())),
        DataField(6, arrow::field("v0", arrow::float64())),
        DataField(7, arrow::field("v1", arrow::boolean()))};
    auto expected_read_schema =
        DataField::ConvertDataFieldsToArrowSchema(expected_read_data_fields);
    ASSERT_TRUE(read_schema->Equals(*expected_read_schema));

    std::vector<int32_t> expected_sort_key_fields = {0, 1};
    ASSERT_EQ(key_comparator->sort_fields_, expected_sort_key_fields);
    ASSERT_EQ(key_comparator->is_ascending_order_, true);
    ASSERT_EQ(interval_partition_comparator->sort_fields_, expected_sort_key_fields);
    ASSERT_EQ(interval_partition_comparator->is_ascending_order_, true);

    ASSERT_FALSE(sequence_fields_comparator);
}

// test no predicate
TEST_F(MergeFileSplitReadTest, TestGenerateKeyPredicates) {
    std::string table_path =
        paimon::test::GetDataDir() + "/parquet/pk_table_with_mor.db/pk_table_with_mor";
    auto schema_manager = std::make_unique<SchemaManager>(fs_, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager->ReadSchema(/*schema_id=*/0));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> predicate_result,
        MergeFileSplitRead::GenerateKeyPredicates(/*predicate=*/nullptr, *table_schema));
    ASSERT_FALSE(predicate_result);
}

// test exist primary predicate
TEST_F(MergeFileSplitReadTest, TestGenerateKeyPredicates1) {
    std::string table_path =
        paimon::test::GetDataDir() + "/parquet/pk_table_with_mor.db/pk_table_with_mor";
    auto schema_manager = std::make_unique<SchemaManager>(fs_, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager->ReadSchema(/*schema_id=*/0));

    ASSERT_OK_AND_ASSIGN(
        auto predicate_result,
        PredicateBuilder::And({PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"k0",
                                                       FieldType::INT, Literal(3)),
                               PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"p0",
                                                       FieldType::INT, Literal(5))}));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> result,
                         MergeFileSplitRead::GenerateKeyPredicates(
                             /*predicate=*/predicate_result, *table_schema));
    ASSERT_OK_AND_ASSIGN(auto expected_key_predicate,
                         PredicateBuilder::And({PredicateBuilder::Equal(
                             /*field_index=*/0, /*field_name=*/"k0", FieldType::INT, Literal(3))}));
    ASSERT_EQ(*result, *expected_key_predicate);
}

// test non-primary predicate
TEST_F(MergeFileSplitReadTest, TestGenerateKeyPredicates2) {
    std::string table_path =
        paimon::test::GetDataDir() + "/parquet/pk_table_with_mor.db/pk_table_with_mor";
    auto schema_manager = std::make_unique<SchemaManager>(fs_, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager->ReadSchema(/*schema_id=*/0));

    ASSERT_OK_AND_ASSIGN(
        auto predicate,
        PredicateBuilder::And({PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"p1",
                                                       FieldType::INT, Literal(3)),
                               PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"p0",
                                                       FieldType::INT, Literal(5))}));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> result,
                         MergeFileSplitRead::GenerateKeyPredicates(
                             /*predicate=*/predicate, *table_schema));
    ASSERT_FALSE(result);
}

TEST_P(MergeFileSplitReadTest, TestSimple) {
    std::string path =
        paimon::test::GetDataDir() + "/parquet/pk_table_with_mor.db/pk_table_with_mor";
    ReadContextBuilder context_builder(path);

    std::vector<DataField> raw_read_fields = {DataField(1, arrow::field("k1", arrow::int32())),
                                              DataField(3, arrow::field("p1", arrow::int32())),
                                              DataField(5, arrow::field("s1", arrow::utf8())),
                                              DataField(6, arrow::field("v0", arrow::float64())),
                                              DataField(7, arrow::field("v1", arrow::boolean()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(raw_read_fields);
    ASSERT_TRUE(read_schema);

    context_builder.SetReadSchema({"k1", "p1", "s1", "v0", "v1"});
    context_builder.SetOptions({{Options::SEQUENCE_FIELD, "s0,s1"},
                                {Options::MERGE_ENGINE, "deduplicate"},
                                {Options::IGNORE_DELETE, "true"}});
    AddOptions(&context_builder);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, context_builder.Finish());

    auto internal_context = CreateInternalReadContext(read_context);

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<MergeFileSplitRead> split_read,
        MergeFileSplitRead::Create(/*path_factory=*/nullptr, internal_context, pool_, executor_));
    auto data_splits = PrepareDataSplit();

    // test split read match
    {
        ASSERT_OK_AND_ASSIGN(bool matched,
                             split_read->Match(data_splits[0], /*force_keep_delete=*/false));
        ASSERT_TRUE(matched);
    }
    {
        auto fake_data_split = PrepareDataSplit()[0];
        auto split_impl = dynamic_cast<DataSplitImpl*>(fake_data_split.get());
        split_impl->before_files_ = split_impl->data_files_;
        ASSERT_OK_AND_ASSIGN(bool matched,
                             split_read->Match(fake_data_split, /*force_keep_delete=*/false));
        ASSERT_FALSE(matched);
    }

    ASSERT_OK_AND_ASSIGN(auto batch_reader, CreateReader(internal_context, data_splits));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result_array,
                         ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
                        [0, 0, 0, "see",   110.0, false],
                        [0, 1, 0, "you",   11.1, false],
                        [0, 0, 0, "later", 12.2, true],
                        [0, 1, 0, "!",     13.3, false],
                        [0, 2, 0, "!",     13.3, false],
                        [0, 200, 0, "number",140.4, false],
                        [0, 1, 1, "you",   130.0, false],
                        [0, 0, 0, "hi",    120.0, false]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(result_array, expected_array, read_schema);
}

TEST_P(MergeFileSplitReadTest, TestLookUp) {
    std::string path =
        paimon::test::GetDataDir() + "/parquet/pk_table_with_mor.db/pk_table_with_mor";
    ReadContextBuilder context_builder(path);

    std::vector<DataField> raw_read_fields = {DataField(1, arrow::field("k1", arrow::int32())),
                                              DataField(3, arrow::field("p1", arrow::int32())),
                                              DataField(5, arrow::field("s1", arrow::utf8())),
                                              DataField(6, arrow::field("v0", arrow::float64())),
                                              DataField(7, arrow::field("v1", arrow::boolean()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(raw_read_fields);
    ASSERT_TRUE(read_schema);

    context_builder.SetReadSchema({"k1", "p1", "s1", "v0", "v1"});
    context_builder.SetOptions({{Options::SEQUENCE_FIELD, "s0,s1"},
                                {Options::MERGE_ENGINE, "deduplicate"},
                                {Options::IGNORE_DELETE, "true"},
                                {Options::FORCE_LOOKUP, "true"}});
    AddOptions(&context_builder);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, context_builder.Finish());

    auto internal_context = CreateInternalReadContext(read_context);

    ASSERT_OK_AND_ASSIGN(auto batch_reader, CreateReader(internal_context, PrepareDataSplit()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result_array,
                         ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
                        [0, 0, 0, "see",   110.0, false],
                        [0, 1, 0, "you",   11.1, false],
                        [0, 0, 0, "later", 12.2, true],
                        [0, 1, 0, "!",     13.3, false],
                        [0, 2, 0, "!",     13.3, false],
                        [0, 200, 0, "number",140.4, false],
                        [0, 1, 1, "you",   130.0, false],
                        [0, 0, 0, "hi",    120.0, false]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(result_array, expected_array, read_schema);
}

TEST_P(MergeFileSplitReadTest, TestReadWithLimits) {
    std::string path =
        paimon::test::GetDataDir() + "/parquet/pk_table_with_mor.db/pk_table_with_mor";
    ReadContextBuilder context_builder(path);
    context_builder.SetOptions({{Options::SEQUENCE_FIELD, "s0,s1"},
                                {Options::MERGE_ENGINE, "deduplicate"},
                                {Options::IGNORE_DELETE, "true"},
                                {Options::READ_BATCH_SIZE, "1"}});
    AddOptions(&context_builder);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, context_builder.Finish());

    auto internal_context = CreateInternalReadContext(read_context);

    ASSERT_OK_AND_ASSIGN(auto batch_reader, CreateReader(internal_context, PrepareDataSplit()));

    // simulate read limits, only read 4 batches
    for (int32_t i = 0; i < 4; i++) {
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, batch_reader->NextBatch());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array,
                             ReadResultCollector::GetArray(std::move(batch)));
        ASSERT_TRUE(array);
        ASSERT_EQ(array->length(), 1);
    }
    batch_reader->Close();
}

TEST_P(MergeFileSplitReadTest, TestDeduplicateMergeEngineWithDeleteMsg) {
    std::string path =
        paimon::test::GetDataDir() + "/parquet/pk_table_partial_update.db/pk_table_partial_update";
    ReadContextBuilder context_builder(path);

    std::vector<DataField> raw_read_fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                              DataField(1, arrow::field("k1", arrow::int32())),
                                              DataField(2, arrow::field("v0", arrow::float64())),
                                              DataField(3, arrow::field("v1", arrow::boolean())),
                                              DataField(4, arrow::field("v2", arrow::utf8()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(raw_read_fields);
    ASSERT_TRUE(read_schema);

    context_builder.SetReadSchema({"k0", "k1", "v0", "v1", "v2"});
    context_builder.SetOptions({{Options::MERGE_ENGINE, "deduplicate"}});
    AddOptions(&context_builder);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, context_builder.Finish());

    auto internal_context = CreateInternalReadContext(read_context);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, CreateReader(internal_context, PrepareDataSplit2()));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result_array,
                         ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
                        [0, 0, 1, 100.0, true, "new_apple"],
                        [0, 1, 1, 133.3, false, null],
                        [0, 2, 1, 144.4, true,  "orange"]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(result_array, expected_array, read_schema);
}

TEST_P(MergeFileSplitReadTest, TestReadWithPredicate) {
    std::string path =
        paimon::test::GetDataDir() + "/parquet/pk_table_with_mor.db/pk_table_with_mor";
    ReadContextBuilder context_builder(path);

    std::vector<DataField> raw_read_fields = {DataField(1, arrow::field("k1", arrow::int32())),
                                              DataField(3, arrow::field("p1", arrow::int32())),
                                              DataField(5, arrow::field("s1", arrow::utf8())),
                                              DataField(4, arrow::field("s0", arrow::utf8())),
                                              DataField(6, arrow::field("v0", arrow::float64())),
                                              DataField(7, arrow::field("v1", arrow::boolean()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(raw_read_fields);
    ASSERT_TRUE(read_schema);

    context_builder.SetReadSchema({"k1", "p1", "s1", "s0", "v0", "v1"});
    context_builder.SetOptions({{Options::SEQUENCE_FIELD, "s0,s1"},
                                {Options::MERGE_ENGINE, "deduplicate"},
                                {Options::IGNORE_DELETE, "true"}});
    AddOptions(&context_builder);

    // less_than will be ignore as it is partition predicate
    auto less_than = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"p1",
                                                   FieldType::INT, Literal(-1));
    // greater_or_equal is key predicate, will always be pushed down
    auto greater_or_equal = PredicateBuilder::GreaterOrEqual(/*field_index=*/0, /*field_name=*/"k1",
                                                             FieldType::INT, Literal(1));
    // greater_than is value predicate, will be pushed down while the number of sorted run in
    // section equals 1
    auto greater_than = PredicateBuilder::GreaterThan(/*field_index=*/4, /*field_name=*/"v0",
                                                      FieldType::DOUBLE, Literal(150.0));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate_result,
                         PredicateBuilder::And({less_than, greater_or_equal, greater_than}));
    context_builder.SetPredicate(predicate_result);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, context_builder.Finish());

    auto internal_context = CreateInternalReadContext(read_context);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, CreateReader(internal_context, PrepareDataSplit()));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result_array,
                         ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
                        [0, 0, 0, "see",   "apple",    110.0, false],
                        [0, 1, 0, "you",   "banana",   11.1, false],
                        [0, 0, 0, "later", "car",      12.2, true],
                        [0, 1, 0, "!",     "driver",   13.3, false],
                        [0, 2, 0, "!",     "driver",   13.3, false],
                        [0, 1, 1, "you",   "zoo",      130.0, false]

    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(result_array, expected_array, read_schema);
}

TEST_P(MergeFileSplitReadTest, TestReadWithAlterTable) {
    std::string path =
        paimon::test::GetDataDir() + "/parquet/pk_table_with_mor.db/pk_table_with_mor";
    ReadContextBuilder context_builder(path);

    std::vector<DataField> raw_read_fields = {DataField(0, arrow::field("k1", arrow::int32())),
                                              DataField(1, arrow::field("k0", arrow::int32())),
                                              DataField(2, arrow::field("p0", arrow::int32())),
                                              DataField(3, arrow::field("p1", arrow::int32())),
                                              DataField(4, arrow::field("s1", arrow::utf8())),
                                              DataField(5, arrow::field("s0", arrow::binary())),
                                              DataField(6, arrow::field("v0", arrow::int32())),
                                              DataField(7, arrow::field("v1", arrow::utf8())),
                                              DataField(8, arrow::field("v2", arrow::int32()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(raw_read_fields);
    ASSERT_TRUE(read_schema);

    context_builder.SetReadSchema({"k1", "k0", "p0", "p1", "s1", "s0", "v0", "v1", "v2"});
    context_builder.SetOptions({{Options::SEQUENCE_FIELD, "s0,s1"},
                                {Options::MERGE_ENGINE, "deduplicate"},
                                {Options::IGNORE_DELETE, "true"}});
    AddOptions(&context_builder);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, context_builder.Finish());

    auto internal_context = CreateInternalReadContext(read_context, /*schema_id=*/7);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, CreateReader(internal_context, PrepareDataSplit()));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result_array,
                         ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
           [0, 0, 0, 0, 0, "apple", "see", 110, "false", null],
           [0, 0, 1, 0, 0, "banana", "you", 11, "false", null],
           [0, 1, 0, 0, 0, "car", "later", 12, "true", null],
           [0, 1, 1, 0, 0, "driver", "!", 13, "false", null],
           [0, 1, 2, 0, 0, "driver", "!", 13, "false", null],
           [0, 100, 200, 0, 0, "max", "number", 140, "false", null],
           [0, 0, 1, 0, 1, "zoo", "you", 130, "false", null],
           [0, 0, 0, 1, 0, "elephant", "hi", 120, "false", null]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(result_array, expected_array, read_schema);
}

TEST_P(MergeFileSplitReadTest, TestReadWithAlterTableWithReverseSequence) {
    std::string path =
        paimon::test::GetDataDir() + "/parquet/pk_table_with_mor.db/pk_table_with_mor";
    ReadContextBuilder context_builder(path);

    std::vector<DataField> raw_read_fields = {DataField(8, arrow::field("v2", arrow::int32())),
                                              DataField(3, arrow::field("p1", arrow::int32())),
                                              DataField(1, arrow::field("k0", arrow::int32())),
                                              DataField(2, arrow::field("p0", arrow::int32())),
                                              DataField(5, arrow::field("s0", arrow::binary())),
                                              DataField(6, arrow::field("v0", arrow::int32()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(raw_read_fields);
    ASSERT_TRUE(read_schema);

    context_builder.SetReadSchema({"v2", "p1", "k0", "p0", "s0", "v0"});
    context_builder.SetOptions({{Options::SEQUENCE_FIELD, "s0,s1"},
                                {Options::MERGE_ENGINE, "deduplicate"},
                                {Options::IGNORE_DELETE, "true"}});
    AddOptions(&context_builder);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, context_builder.Finish());

    auto internal_context = CreateInternalReadContext(read_context, /*schema_id=*/7);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, CreateReader(internal_context, PrepareDataSplit()));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result_array,
                         ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
           [0, null, 0, 0, 0, "see", 110],
           [0, null, 0, 1, 0, "you", 11],
           [0, null, 0, 0, 0, "later", 12],
           [0, null, 0, 1, 0, "!", 13],
           [0, null, 0, 2, 0, "!", 13],
           [0, null, 0, 200, 0, "number", 140],
           [0, null, 1, 1, 0, "you", 130],
           [0, null, 0, 0, 1, "hi", 120]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(result_array, expected_array, read_schema);
}

TEST_P(MergeFileSplitReadTest, TestAggregateMergeEngine) {
    std::string path =
        paimon::test::GetDataDir() + "/parquet/pk_table_with_mor.db/pk_table_with_mor";
    ReadContextBuilder context_builder(path);

    std::vector<DataField> raw_read_fields = {DataField(1, arrow::field("k1", arrow::int32())),
                                              DataField(3, arrow::field("p1", arrow::int32())),
                                              DataField(5, arrow::field("s1", arrow::utf8())),
                                              DataField(6, arrow::field("v0", arrow::float64())),
                                              DataField(7, arrow::field("v1", arrow::boolean()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(raw_read_fields);
    ASSERT_TRUE(read_schema);

    context_builder.SetReadSchema({"k1", "p1", "s1", "v0", "v1"});
    context_builder.SetOptions({{Options::SEQUENCE_FIELD, "s0,s1"},
                                {Options::MERGE_ENGINE, "aggregation"},
                                {"fields.v1.aggregate-function", "bool_and"},
                                {"fields.v0.aggregate-function", "sum"},
                                {Options::IGNORE_DELETE, "true"}});
    AddOptions(&context_builder);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, context_builder.Finish());
    auto internal_context = CreateInternalReadContext(read_context);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, CreateReader(internal_context, PrepareDataSplit()));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result_array,
                         ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
                        [0, 0, 0, "see",   120.0, false],
                        [0, 1, 0, "you",   122.2, false],
                        [0, 0, 0, "later", 124.4, true],
                        [0, 1, 0, "!",     13.3, false],
                        [0, 2, 0, "!",     13.3, false],
                        [0, 200, 0, "number",140.4, false],
                        [0, 1, 1, "you",   160.0, false],
                        [0, 0, 0, "hi",    140.0, false]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(result_array, expected_array, read_schema);
}

TEST_P(MergeFileSplitReadTest, TestPartialUpdateMergeEngine) {
    std::string path =
        paimon::test::GetDataDir() + "/parquet/pk_table_with_mor.db/pk_table_with_mor";
    ReadContextBuilder context_builder(path);

    std::vector<DataField> raw_read_fields = {DataField(1, arrow::field("k1", arrow::int32())),
                                              DataField(3, arrow::field("p1", arrow::int32())),
                                              DataField(5, arrow::field("s1", arrow::utf8())),
                                              DataField(6, arrow::field("v0", arrow::float64()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(raw_read_fields);
    ASSERT_TRUE(read_schema);

    context_builder.SetReadSchema({"k1", "p1", "s1", "v0"});
    context_builder.SetOptions({{Options::SEQUENCE_FIELD, "s0,s1"},
                                {Options::MERGE_ENGINE, "partial-update"},
                                {"fields.v1.sequence-group", "v0"},
                                {"fields.v0.aggregate-function", "first_value"},
                                {Options::IGNORE_DELETE, "true"}});
    AddOptions(&context_builder);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, context_builder.Finish());
    auto internal_context = CreateInternalReadContext(read_context);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, CreateReader(internal_context, PrepareDataSplit()));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result_array,
                         ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
                        [0, 0, 0, "see",     110.0],
                        [0, 1, 0, "you", 11.1],
                        [0, 0, 0, "later",   112.2],
                        [0, 1, 0, "!",       13.3],
                        [0, 2, 0, "!",       13.3],
                        [0, 200, 0, "number",140.4],
                        [0, 1, 1, "you",     30.0],
                        [0, 0, 0, "hi",      120.0]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(result_array, expected_array, read_schema);
}

TEST_P(MergeFileSplitReadTest, TestPartialUpdateMergeEngineWithIgnoreDelete) {
    std::string path =
        paimon::test::GetDataDir() + "/parquet/pk_table_partial_update.db/pk_table_partial_update";
    ReadContextBuilder context_builder(path);

    std::vector<DataField> raw_read_fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                              DataField(1, arrow::field("k1", arrow::int32())),
                                              DataField(2, arrow::field("v0", arrow::float64())),
                                              DataField(3, arrow::field("v1", arrow::boolean())),
                                              DataField(4, arrow::field("v2", arrow::utf8()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(raw_read_fields);
    ASSERT_TRUE(read_schema);

    context_builder.SetReadSchema({"k0", "k1", "v0", "v1", "v2"});
    context_builder.SetOptions(
        {{Options::MERGE_ENGINE, "partial-update"}, {Options::IGNORE_DELETE, "true"}});
    AddOptions(&context_builder);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, context_builder.Finish());
    auto internal_context = CreateInternalReadContext(read_context);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, CreateReader(internal_context, PrepareDataSplit2()));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result_array,
                         ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(
        arrow::struct_(fields_with_row_kind),
        {R"([ [0, 0, 1, 100.0, true, "new_apple"], [0, 1, 0, 2.0, false, null], [0, 1, 1, 133.3,
        false, "banana"], [0, 2, 1, 144.4, true, "orange"]

    ])"},
        &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(result_array, expected_array, read_schema);
}

TEST_P(MergeFileSplitReadTest, TestPartialUpdateMergeEngineWithRemoveRecordOnDelete) {
    std::string path =
        paimon::test::GetDataDir() + "/parquet/pk_table_partial_update.db/pk_table_partial_update";
    ReadContextBuilder context_builder(path);

    std::vector<DataField> raw_read_fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                              DataField(1, arrow::field("k1", arrow::int32())),
                                              DataField(2, arrow::field("v0", arrow::float64())),
                                              DataField(3, arrow::field("v1", arrow::boolean())),
                                              DataField(4, arrow::field("v2", arrow::utf8()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(raw_read_fields);
    ASSERT_TRUE(read_schema);

    context_builder.SetReadSchema({"k0", "k1", "v0", "v1", "v2"});
    context_builder.SetOptions({{Options::MERGE_ENGINE, "partial-update"},
                                {Options::PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE, "true"}});
    AddOptions(&context_builder);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, context_builder.Finish());
    auto internal_context = CreateInternalReadContext(read_context);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, CreateReader(internal_context, PrepareDataSplit2()));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result_array,
                         ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(
        arrow::struct_(fields_with_row_kind),
        {R"([ [0, 0, 1, 100.0, true, "new_apple"], [0, 1, 1, 133.3, false, "banana"], [0, 2, 1,
        144.4, true, "orange"]

    ])"},
        &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(result_array, expected_array, read_schema);
}

TEST_P(MergeFileSplitReadTest, TestEmptyPlan) {
    std::string path =
        paimon::test::GetDataDir() + "/parquet/pk_table_partial_update.db/pk_table_partial_update";
    ReadContextBuilder context_builder(path);

    std::vector<DataField> raw_read_fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                              DataField(1, arrow::field("k1", arrow::int32())),
                                              DataField(2, arrow::field("v0", arrow::float64())),
                                              DataField(3, arrow::field("v1", arrow::boolean())),
                                              DataField(4, arrow::field("v2", arrow::utf8()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(raw_read_fields);
    ASSERT_TRUE(read_schema);

    context_builder.SetReadSchema({"k0", "k1", "v0", "v1", "v2"});
    context_builder.SetOptions({{Options::MERGE_ENGINE, "partial-update"},
                                {Options::PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE, "true"}});
    AddOptions(&context_builder);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, context_builder.Finish());
    auto internal_context = CreateInternalReadContext(read_context);
    std::vector<std::shared_ptr<DataSplit>> empty_data_split;
    ASSERT_OK_AND_ASSIGN(auto batch_reader, CreateReader(internal_context, empty_data_split));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> read_result,
                         ReadResultCollector::CollectResult(batch_reader.get()));
    // empty result with null pointer batch
    ASSERT_FALSE(read_result);
}

TEST_P(MergeFileSplitReadTest, TestIOException) {
    std::string path =
        paimon::test::GetDataDir() + "/parquet/pk_table_with_mor.db/pk_table_with_mor";
    std::vector<DataField> raw_read_fields = {DataField(1, arrow::field("k1", arrow::int32())),
                                              DataField(3, arrow::field("p1", arrow::int32())),
                                              DataField(5, arrow::field("s1", arrow::utf8())),
                                              DataField(6, arrow::field("v0", arrow::float64())),
                                              DataField(7, arrow::field("v1", arrow::boolean()))};

    ReadContextBuilder context_builder(path);
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(raw_read_fields);
    ASSERT_TRUE(read_schema);

    context_builder.SetReadSchema({"k1", "p1", "s1", "v0", "v1"});
    context_builder.SetOptions({{Options::SEQUENCE_FIELD, "s0,s1"},
                                {Options::MERGE_ENGINE, "deduplicate"},
                                {Options::IGNORE_DELETE, "true"}});
    AddOptions(&context_builder);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, context_builder.Finish());

    auto internal_context = CreateInternalReadContext(read_context);

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
                        [0, 0, 0, "see",   110.0, false],
                        [0, 1, 0, "you",   11.1, false],
                        [0, 0, 0, "later", 12.2, true],
                        [0, 1, 0, "!",     13.3, false],
                        [0, 2, 0, "!",     13.3, false],
                        [0, 200, 0, "number",140.4, false],
                        [0, 1, 1, "you",   130.0, false],
                        [0, 0, 0, "hi",    120.0, false]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());

    bool run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 300; i++) {
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        auto batch_reader = CreateReader(internal_context, PrepareDataSplit());
        CHECK_HOOK_STATUS(batch_reader.status(), i);
        auto read_result = ReadResultCollector::CollectResult(batch_reader.value().get());
        CHECK_HOOK_STATUS(read_result.status(), i);
        auto result_array = read_result.value();
        CheckResult(result_array, expected_array, read_schema);
        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

TEST_P(MergeFileSplitReadTest, Test09VersionWithoutInlineFieldId) {
    std::string path = paimon::test::GetDataDir() + "/orc/pk_09.db/pk_09";
    ReadContextBuilder context_builder(path);
    std::vector<DataField> raw_read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                              DataField(2, arrow::field("f2", arrow::int32())),
                                              DataField(0, arrow::field("f0", arrow::utf8())),
                                              DataField(1, arrow::field("f1", arrow::int32()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(raw_read_fields);
    ASSERT_TRUE(read_schema);
    context_builder.SetReadSchema({"f3", "f2", "f0", "f1"});
    context_builder.SetOptions({{Options::FILE_FORMAT, "orc"},
                                {Options::MERGE_ENGINE, "deduplicate"},
                                {"orc.read.enable-metrics", "true"}});
    AddOptions(&context_builder);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, context_builder.Finish());
    auto meta1 = std::make_shared<DataFileMeta>(
        "data-00e3ed53-16ba-4537-9264-b7dc03fefc65-0.orc", /*file_size=*/803, /*row_count=*/1,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Tony", 0}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({"Tony", 0}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({"Tony", 0}, {"Tony", 0}, {0, 0}, pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Tony", 10, 0, 14.1}, {"Tony", 10, 0, 14.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/5, /*max_sequence_number=*/5, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(0ll, 0),
        /*delete_row_count=*/1, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    auto meta2 = std::make_shared<DataFileMeta>(
        "data-6871b960-edd9-40fc-9859-aaca9ea205cf-0.orc", /*file_size=*/887, /*row_count=*/5,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alex", 0}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({"Tony", 0}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({"Alex", 0}, {"Tony", 0}, {0, 0}, pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alex", 10, 0, 12.1}, {"Tony", 10, 0, 17.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(0ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "/orc/pk_09.db/pk_09/f1=10/bucket-1/", {meta1, meta2});
    ASSERT_OK_AND_ASSIGN(auto data_split,
                         builder.WithSnapshot(5).IsStreaming(false).RawConvertible(false).Build());
    auto internal_context = CreateInternalReadContext(read_context);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, CreateReader(internal_context, {data_split}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result_array,
                         ReadResultCollector::CollectResult(batch_reader.get()));
    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
                        [0, 16.1, 0, "Alex", 10],
                        [0, 12.1, 0, "Bob", 10],
                        [0, 17.1, 0, "David", 10],
                        [0, 13.1, 0, "Emily", 10]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(result_array, expected_array, read_schema);

    batch_reader->Close();
    auto read_metrics = batch_reader->GetReaderMetrics();
    ASSERT_OK_AND_ASSIGN(uint64_t io_count, read_metrics->GetCounter("orc.read.io.count"));
    ASSERT_GT(io_count, 0);
    ASSERT_OK_AND_ASSIGN(uint64_t latency,
                         read_metrics->GetCounter("orc.read.inclusive.latency.us"));
    ASSERT_GT(latency, 0);
}

INSTANTIATE_TEST_SUITE_P(UseMinHeapAndEnablePrefetchAndEnableMultiThreadProject,
                         MergeFileSplitReadTest,
                         ::testing::Combine(::testing::Bool(), ::testing::Bool(),
                                            ::testing::Bool()));

}  // namespace paimon::test
