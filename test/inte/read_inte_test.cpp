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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/reader/complete_row_kind_batch_reader.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/core/table/source/fallback_data_split.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/table/source/data_split.h"
#include "paimon/table/source/table_read.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

struct TestParam {
    bool enable_prefetch;
    std::string enable_adaptive_prefetch_strategy;
    std::string file_format;
    PrefetchCacheMode cache_mode;
};

// read_inte_test.cpp test mainly for raw file split read (pk+dv & append only)
// see merge_file_split_read_test.cpp for pk merger on read tests
class ReadInteTest : public testing::Test, public ::testing::WithParamInterface<TestParam> {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }
    void TearDown() override {}

    using DataSplitsSimple =
        std::vector<std::tuple<std::string, BinaryRow, std::vector<std::string>>>;
    using DataSplitsDv = std::vector<std::tuple<std::string, BinaryRow, std::vector<std::string>,
                                                std::vector<std::optional<DeletionFile>>>>;
    using DataSplitsSchema = std::vector<
        std::tuple<std::string, BinaryRow, std::vector<std::string>, std::vector<int64_t>>>;

    using DataSplitsSchemaDv =
        std::vector<std::tuple<std::string, BinaryRow, std::vector<std::string>,
                               std::vector<int64_t>, std::vector<std::optional<DeletionFile>>>>;

    std::vector<std::shared_ptr<Split>> CreateDataSplits(const DataSplitsSimple& input_data_splits,
                                                         int64_t snapshot_id) const {
        DataSplitsSchemaDv results;
        results.reserve(input_data_splits.size());

        for (const auto& input_data_split : input_data_splits) {
            auto result =
                std::make_tuple(std::get<0>(input_data_split), std::get<1>(input_data_split),
                                std::get<2>(input_data_split), std::vector<int64_t>(),
                                std::vector<std::optional<DeletionFile>>());
            results.push_back(result);
        }
        return CreateDataSplits(results, snapshot_id);
    }

    std::vector<std::shared_ptr<Split>> CreateDataSplits(const DataSplitsDv& input_data_splits,
                                                         int64_t snapshot_id) const {
        DataSplitsSchemaDv results;
        results.reserve(input_data_splits.size());
        for (const auto& input_data_split : input_data_splits) {
            auto result =
                std::make_tuple(std::get<0>(input_data_split), std::get<1>(input_data_split),
                                std::get<2>(input_data_split), std::vector<int64_t>(),
                                std::get<3>(input_data_split));
            results.push_back(result);
        }
        return CreateDataSplits(results, snapshot_id);
    }

    std::vector<std::shared_ptr<Split>> CreateDataSplits(const DataSplitsSchema& input_data_splits,
                                                         int64_t snapshot_id) const {
        DataSplitsSchemaDv results;
        results.reserve(input_data_splits.size());
        for (const auto& input_data_split : input_data_splits) {
            auto result =
                std::make_tuple(std::get<0>(input_data_split), std::get<1>(input_data_split),
                                std::get<2>(input_data_split), std::get<3>(input_data_split),
                                std::vector<std::optional<DeletionFile>>());
            results.push_back(result);
        }
        return CreateDataSplits(results, snapshot_id);
    }

    std::vector<std::shared_ptr<Split>> CreateDataSplits(
        const DataSplitsSchemaDv& input_data_splits, int64_t snapshot_id) const {
        std::vector<std::shared_ptr<Split>> data_splits;
        for (const auto& input_data_split : input_data_splits) {
            std::vector<std::shared_ptr<DataFileMeta>> data_file_metas;
            const auto& bucket_path = std::get<0>(input_data_split);
            const auto& partition = std::get<1>(input_data_split);
            const auto& data_files = std::get<2>(input_data_split);
            const auto& schema_ids = std::get<3>(input_data_split);
            const auto& deletion_files = std::get<4>(input_data_split);
            EXPECT_TRUE(deletion_files.empty() || deletion_files.size() == data_files.size());
            EXPECT_TRUE(schema_ids.empty() || schema_ids.size() == data_files.size());
            for (uint32_t j = 0; j < data_files.size(); j++) {
                const auto& data_file = data_files[j];
                data_file_metas.push_back(std::make_shared<DataFileMeta>(
                    data_file, /*file_size=*/1, /*row_count=*/1, /*min_key=*/BinaryRow::EmptyRow(),
                    /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
                    /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
                    /*max_sequence_number=*/0, /*schema_id=*/schema_ids.empty() ? 0 : schema_ids[j],
                    /*level=*/0,
                    /*extra_files=*/std::vector<std::optional<std::string>>(),
                    /*creation_time=*/Timestamp(1721643142472ll, 0), /*delete_row_count=*/0,
                    /*embedded_index=*/nullptr, FileSource::Append(),
                    /*value_stats_cols=*/std::nullopt,
                    /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
                    /*write_cols=*/std::nullopt));
            }
            auto bucket_str = bucket_path.substr(bucket_path.find("bucket-") + 7);
            int32_t bucket = std::stoi(bucket_str);
            DataSplitImpl::Builder builder(partition, bucket,
                                           /*bucket_path=*/bucket_path, std::move(data_file_metas));
            EXPECT_OK_AND_ASSIGN(auto data_split, builder.WithSnapshot(snapshot_id)
                                                      .WithDataDeletionFiles(deletion_files)
                                                      .IsStreaming(false)
                                                      .RawConvertible(true)
                                                      .Build());
            data_splits.push_back(data_split);
        }
        return data_splits;
    }

    std::shared_ptr<Split> GetDataSplitFromFile(const std::string& split_file_name) {
        auto file_system = std::make_shared<LocalFileSystem>();
        EXPECT_OK_AND_ASSIGN(auto input_stream, file_system->Open(split_file_name));
        std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);
        EXPECT_OK_AND_ASSIGN([[maybe_unused]] int32_t read_len,
                             input_stream->Read(split_bytes.data(), split_bytes.size()));
        EXPECT_OK(input_stream->Close());

        EXPECT_OK_AND_ASSIGN(auto split,
                             Split::Deserialize(reinterpret_cast<char*>(split_bytes.data()),
                                                split_bytes.size(), pool_));
        return std::dynamic_pointer_cast<Split>(split);
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

std::vector<TestParam> PrepareTestParam() {
    std::vector<TestParam> values = {
        TestParam{false, "false", "parquet", PrefetchCacheMode::ALWAYS},
        TestParam{true, "true", "parquet", PrefetchCacheMode::ALWAYS},
        TestParam{true, "false", "parquet", PrefetchCacheMode::ALWAYS},
        TestParam{true, "false", "parquet", PrefetchCacheMode::NEVER},
        TestParam{true, "false", "parquet", PrefetchCacheMode::EXCLUDE_BITMAP_OR_PREDICATE}};

#ifdef PAIMON_ENABLE_ORC
    values.push_back(TestParam{false, "false", "orc", PrefetchCacheMode::ALWAYS});
    values.push_back(TestParam{true, "true", "orc", PrefetchCacheMode::ALWAYS});
    values.push_back(TestParam{true, "false", "orc", PrefetchCacheMode::ALWAYS});
    values.push_back(TestParam{true, "false", "orc", PrefetchCacheMode::NEVER});
    values.push_back(
        TestParam{true, "false", "orc", PrefetchCacheMode::EXCLUDE_BITMAP_OR_PREDICATE});
#endif
    return values;
}

INSTANTIATE_TEST_SUITE_P(EnablePaimonPrefetch, ReadInteTest,
                         ::testing::ValuesIn(PrepareTestParam()));

TEST_P(ReadInteTest, TestAppendSimple) {
    auto param = GetParam();
    std::string path =
        paimon::test::GetDataDir() + "/" + param.file_format + "/append_09.db/append_09";

    auto check_result = [&](const std::optional<std::string>& specific_table_schema) {
        std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                              DataField(1, arrow::field("f1", arrow::int32())),
                                              DataField(2, arrow::field("f2", arrow::int32())),
                                              DataField(3, arrow::field("f3", arrow::float64()))};

        ReadContextBuilder context_builder(path);
        context_builder.AddOption(Options::FILE_FORMAT, param.file_format);
        context_builder.EnablePrefetch(param.enable_prefetch)
            .AddOption("test.enable-adaptive-prefetch-strategy", "false")
            .AddOption("orc.read.enable-metrics", "true");
        context_builder.SetPrefetchCacheMode(param.cache_mode);

        if (specific_table_schema) {
            context_builder.SetTableSchema(specific_table_schema.value());
        }
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

        std::vector<std::string> file_list;
        if (param.file_format == "orc") {
            file_list = {"data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc",
                         "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc"};
        } else if (param.file_format == "parquet") {
            file_list = {"data-b446f78a-2cfb-4b3b-add8-31295d24a277-0.parquet",
                         "data-fd72a479-53ae-42f7-aec0-e982ee555928-0.parquet"};
        }
        DataSplitsSimple input_data_splits = {
            {paimon::test::GetDataDir() + "/" + param.file_format +
                 "/append_09.db/append_09/f1=20/"
                 "bucket-0",
             BinaryRowGenerator::GenerateRow({20}, pool_.get()), file_list}};

        auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/3);
        ASSERT_EQ(data_splits.size(), 1);
        ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
        ASSERT_OK_AND_ASSIGN(auto result_array,
                             ReadResultCollector::CollectResult(batch_reader.get()));

        auto fields_with_row_kind = read_fields;
        fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
        std::shared_ptr<arrow::DataType> arrow_data_type =
            DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
      [0, "Lucy", 20, 1, 14.1],
      [0, "Paul", 20, 1, null]
    ])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        ASSERT_TRUE(result_array->Equals(expected_array));

        // test metrics
        auto read_metrics = batch_reader->GetReaderMetrics();

        auto split_concat_batch_reader = dynamic_cast<ConcatBatchReader*>(batch_reader.get());
        ASSERT_TRUE(split_concat_batch_reader);
        ASSERT_EQ(1, split_concat_batch_reader->readers_.size());
        auto complete_batch_reader =
            dynamic_cast<CompleteRowKindBatchReader*>(split_concat_batch_reader->readers_[0].get());
        ASSERT_TRUE(complete_batch_reader);
        auto file_concat_batch_reader =
            dynamic_cast<ConcatBatchReader*>(complete_batch_reader->reader_.get());
        ASSERT_TRUE(file_concat_batch_reader);
        ASSERT_EQ(2, file_concat_batch_reader->readers_.size());

        if (param.file_format == "orc") {
            ASSERT_OK_AND_ASSIGN(
                uint64_t reader0_latency,
                file_concat_batch_reader->readers_[0]->GetReaderMetrics()->GetCounter(
                    "orc.read.inclusive.latency.us"));
            ASSERT_OK_AND_ASSIGN(
                uint64_t reader1_latency,
                file_concat_batch_reader->readers_[1]->GetReaderMetrics()->GetCounter(
                    "orc.read.inclusive.latency.us"));
            uint64_t expected_read_latency = reader0_latency + reader1_latency;

            ASSERT_OK_AND_ASSIGN(
                uint64_t reader0_io_count,
                file_concat_batch_reader->readers_[0]->GetReaderMetrics()->GetCounter(
                    "orc.read.io.count"));
            ASSERT_OK_AND_ASSIGN(
                uint64_t reader1_io_count,
                file_concat_batch_reader->readers_[1]->GetReaderMetrics()->GetCounter(
                    "orc.read.io.count"));
            uint64_t expected_read_io_count = reader0_io_count + reader1_io_count;

            ASSERT_OK_AND_ASSIGN(uint64_t result_read_latency,
                                 read_metrics->GetCounter("orc.read.inclusive.latency.us"));
            ASSERT_EQ(result_read_latency, expected_read_latency);
            ASSERT_OK_AND_ASSIGN(uint64_t result_read_io_count,
                                 read_metrics->GetCounter("orc.read.io.count"));
            ASSERT_EQ(result_read_io_count, expected_read_io_count);
        }
    };

    // check without specific table schema
    check_result(std::nullopt);

    {
        // check with specific table schema
        auto fs = std::make_shared<LocalFileSystem>();
        std::string schema_str;
        ASSERT_OK(fs->ReadFile(path + "/schema/schema-0", &schema_str));
        check_result(std::optional<std::string>(schema_str));
    }
}

TEST_P(ReadInteTest, TestReadWithLimits) {
    auto param = GetParam();
    std::string path =
        paimon::test::GetDataDir() + "/" + param.file_format + "/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption(Options::READ_BATCH_SIZE, "1");
    context_builder.EnablePrefetch(param.enable_prefetch)
        .SetPrefetchCacheMode(param.cache_mode)
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy)
        .AddOption("orc.read.enable-metrics", "true")
        .SetPrefetchBatchCount(10);

    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list;
    if (param.file_format == "orc") {
        file_list = {"data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc",
                     "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc"};
    } else if (param.file_format == "parquet") {
        file_list = {"data-b446f78a-2cfb-4b3b-add8-31295d24a277-0.parquet",
                     "data-fd72a479-53ae-42f7-aec0-e982ee555928-0.parquet"};
    }

    DataSplitsSimple input_data_splits = {{paimon::test::GetDataDir() + "/" + param.file_format +
                                               "/append_09.db/append_09/f1=20/"
                                               "bucket-0",
                                           BinaryRowGenerator::GenerateRow({20}, pool_.get()),
                                           file_list}};

    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/3);
    ASSERT_EQ(data_splits.size(), 1);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));

    // simulate read limits, only read 2 batches
    for (int32_t i = 0; i < 2; i++) {
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, batch_reader->NextBatch());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array,
                             ReadResultCollector::GetArray(std::move(batch)));
        ASSERT_TRUE(array);
        ASSERT_EQ(array->length(), 1);
    }
    batch_reader->Close();
    // test metrics

    if (param.file_format == "orc") {
        auto read_metrics = batch_reader->GetReaderMetrics();
        ASSERT_TRUE(read_metrics);
        ASSERT_OK_AND_ASSIGN(uint64_t io_count, read_metrics->GetCounter("orc.read.io.count"));
        ASSERT_GT(io_count, 0);
        ASSERT_OK_AND_ASSIGN(uint64_t latency,
                             read_metrics->GetCounter("orc.read.inclusive.latency.us"));
        ASSERT_GT(latency, 0);
    }
}

TEST_P(ReadInteTest, TestReadOnlyPartitionField) {
    auto param = GetParam();
    std::string path = paimon::test::GetDataDir() + "/" + param.file_format +
                       "/append_table_with_rt_branch.db/append_table_with_rt_branch";
    std::vector<DataField> read_fields = {
        DataField(0, arrow::field("dt", arrow::utf8())),
    };

    ReadContextBuilder context_builder(path);
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format);
    context_builder.SetReadSchema({"dt"});
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.EnablePrefetch(param.enable_prefetch)
        .AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::shared_ptr<Split>> data_splits;
    data_splits.reserve(3);
    for (size_t i = 0; i < 3; ++i) {
        std::string file_name = path + "/data-splits/data_split-" + std::to_string(i);
        auto split = GetDataSplitFromFile(file_name);
        data_splits.push_back(split);
    }

    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [0, "20240725"],
        [0, "20240725"],
        [0, "20240726"],
        [0, "20240726"]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(expected_array)) << result_array->ToString();
}

TEST_P(ReadInteTest, TestAppendReadWithMultipleBuckets) {
    std::vector<DataField> read_fields = {
        DataField(3, arrow::field("f3", arrow::float64())),
        DataField(0, arrow::field("f0", arrow::utf8())),
        DataField(1, arrow::field("f1", arrow::int32())),
    };

    auto param = GetParam();
    std::string path =
        paimon::test::GetDataDir() + "/" + param.file_format + "/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema({"f3", "f0", "f1"});
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("read.batch-size", "2")
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy)
        .EnablePrefetch(param.enable_prefetch);

    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list_0;
    std::vector<std::string> file_list_1;
    std::vector<std::string> file_list_2;
    if (param.file_format == "orc") {
        file_list_0 = {"data-d41fd7d1-b3e4-4905-aad9-b20a780e90a2-0.orc"};
        file_list_1 = {"data-4e30d6c0-f109-4300-a010-4ba03047dd9d-0.orc",
                       "data-10b9eea8-241d-4e4b-8ab8-2a82d72d79a2-0.orc",
                       "data-e2bb59ee-ae25-4e5b-9bcc-257250bc5fdd-0.orc",
                       "data-2d5ea1ea-77c1-47ff-bb87-19a509962a37-0.orc"};
        file_list_2 = {"data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc",
                       "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc"};
    } else if (param.file_format == "parquet") {
        file_list_0 = {"data-46e27d5b-4850-4d1e-abb6-b3aabbbc08cb-0.parquet"};
        file_list_1 = {"data-864a052b-a938-4e04-b32c-6c72699a0c92-0.parquet",
                       "data-c0401350-64a3-4a54-a143-dd125ad9a8e5-0.parquet",
                       "data-7a912f84-04b7-4bbb-8dc6-53f4a292ea25-0.parquet",
                       "data-bb891df7-ea12-4b7e-9017-41aabe08c8ec-0.parquet"};
        file_list_2 = {"data-b446f78a-2cfb-4b3b-add8-31295d24a277-0.parquet",
                       "data-fd72a479-53ae-42f7-aec0-e982ee555928-0.parquet"};
    }

    DataSplitsSimple input_data_splits = {
        {paimon::test::GetDataDir() + "/" + param.file_format +
             "/append_09.db/append_09/f1=10/bucket-0",
         BinaryRowGenerator::GenerateRow({10}, pool_.get()), file_list_0},
        {paimon::test::GetDataDir() + "/" + param.file_format +
             "/append_09.db/append_09/f1=10/bucket-1",
         BinaryRowGenerator::GenerateRow({10}, pool_.get()), file_list_1},
        {paimon::test::GetDataDir() + "/" + param.file_format +
             "/append_09.db/append_09/f1=20/bucket-0",
         BinaryRowGenerator::GenerateRow({20}, pool_.get()), file_list_2}};
    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/4);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [0, 11.1, "Alice", 10], [0, 12.1, "Bob", 10],  [0, 13.1, "Emily", 10], [0, 14.1, "Tony",
        10], [0, 15.1, "Emily", 10], [0, 12.1, "Bob", 10],  [0, 16.1, "Alex", 10],  [0, 17.1,
        "David", 10], [0, 17.1, "Lily", 10],  [0, 14.1, "Lucy", 20], [0, null, "Paul", 20]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(*expected_array));
}

TEST_P(ReadInteTest, TestAppendReadWithPredicate) {
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32()))};

    ASSERT_OK_AND_ASSIGN(
        auto predicate,
        PredicateBuilder::Or(
            {PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f3",
                                           FieldType::DOUBLE, Literal(static_cast<double>(15.0))),
             PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f1", FieldType::INT,
                                     Literal(20))}));

    auto param = GetParam();
    std::string path =
        paimon::test::GetDataDir() + "/" + param.file_format + "/append_09.db/append_09";

    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema({"f3", "f0", "f1"});
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .SetPredicate(predicate)
        .EnablePredicateFilter(true)
        .EnablePrefetch(param.enable_prefetch)
        .AddOption("read.batch-size", "2")
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy)
        .AddOption("orc.read.enable-metrics", "true");
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list_0;
    std::vector<std::string> file_list_1;
    std::vector<std::string> file_list_2;
    if (param.file_format == "orc") {
        file_list_0 = {"data-d41fd7d1-b3e4-4905-aad9-b20a780e90a2-0.orc"};
        file_list_1 = {"data-4e30d6c0-f109-4300-a010-4ba03047dd9d-0.orc",
                       "data-10b9eea8-241d-4e4b-8ab8-2a82d72d79a2-0.orc",
                       "data-e2bb59ee-ae25-4e5b-9bcc-257250bc5fdd-0.orc",
                       "data-2d5ea1ea-77c1-47ff-bb87-19a509962a37-0.orc"};
        file_list_2 = {"data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc",
                       "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc"};
    } else if (param.file_format == "parquet") {
        file_list_0 = {"data-46e27d5b-4850-4d1e-abb6-b3aabbbc08cb-0.parquet"};
        file_list_1 = {"data-864a052b-a938-4e04-b32c-6c72699a0c92-0.parquet",
                       "data-c0401350-64a3-4a54-a143-dd125ad9a8e5-0.parquet",
                       "data-7a912f84-04b7-4bbb-8dc6-53f4a292ea25-0.parquet",
                       "data-bb891df7-ea12-4b7e-9017-41aabe08c8ec-0.parquet"};
        file_list_2 = {"data-b446f78a-2cfb-4b3b-add8-31295d24a277-0.parquet",
                       "data-fd72a479-53ae-42f7-aec0-e982ee555928-0.parquet"};
    }

    DataSplitsSimple input_data_splits = {
        {paimon::test::GetDataDir() + "/" + param.file_format +
             "/append_09.db/append_09/f1=10/bucket-0",
         BinaryRowGenerator::GenerateRow({10}, pool_.get()), file_list_0},
        {paimon::test::GetDataDir() + "/" + param.file_format +
             "/append_09.db/append_09/f1=10/bucket-1",
         BinaryRowGenerator::GenerateRow({10}, pool_.get()), file_list_1},
        {paimon::test::GetDataDir() + "/" + param.file_format +
             "/append_09.db/append_09/f1=20/bucket-0",
         BinaryRowGenerator::GenerateRow({20}, pool_.get()), file_list_2}};

    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/4);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [0, 15.1, "Emily", 10], [0, 16.1, "Alex", 10], [0, 17.1, "David", 10],
        [0, 17.1, "Lily", 10],  [0, 14.1, "Lucy", 20], [0, null, "Paul", 20]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(*expected_array));
    batch_reader->Close();
    if (param.file_format == "orc") {
        // test metrics
        auto read_metrics = batch_reader->GetReaderMetrics();
        ASSERT_TRUE(read_metrics);
        ASSERT_OK_AND_ASSIGN(uint64_t io_count, read_metrics->GetCounter("orc.read.io.count"));
        ASSERT_GT(io_count, 0);
        ASSERT_OK_AND_ASSIGN(uint64_t latency,
                             read_metrics->GetCounter("orc.read.inclusive.latency.us"));
        ASSERT_GT(latency, 0);
    }
}

TEST_P(ReadInteTest, TestAppendReadWithComplexTypePredicate) {
    std::vector<DataField> read_fields = {
        DataField(5, arrow::field("f6", arrow::binary())),
        DataField(1, arrow::field("f2", arrow::int32())),
        DataField(3, arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO))),
        DataField(2, arrow::field("f3", arrow::date32())),
        DataField(4, arrow::field("f5", arrow::decimal128(23, 5)))};

    auto predicate =
        PredicateBuilder::And(
            {PredicateBuilder::Or(
                 {PredicateBuilder::GreaterThan(/*field_index=*/4, /*field_name=*/"f5",
                                                FieldType::DECIMAL, Literal(Decimal(5, 2, 0))),
                  PredicateBuilder::LessThan(/*field_index=*/2, /*field_name=*/"f4",
                                             FieldType::TIMESTAMP,
                                             Literal(Timestamp(-2240521239999l, 1002))),
                  PredicateBuilder::IsNull(/*field_index=*/3, /*field_name=*/"f3",
                                           FieldType::DATE)})
                 .value_or(nullptr),
             PredicateBuilder::IsNotNull(/*field_index=*/0, /*field_name=*/"f6",
                                         FieldType::BINARY)})
            .value_or(nullptr);
    ASSERT_TRUE(predicate);
    auto param = GetParam();

    std::string path = paimon::test::GetDataDir() + "/" + param.file_format +
                       "/append_complex_data.db/append_complex_data";
    ReadContextBuilder context_builder(path);
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.SetReadSchema({"f6", "f2", "f4", "f3", "f5"});
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("read.batch-size", "2");
    context_builder.SetPredicate(predicate);
    context_builder.EnablePredicateFilter(true);
    context_builder.EnablePrefetch(param.enable_prefetch)
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list_0;
    std::vector<std::string> file_list_1;
    std::vector<std::string> file_list_2;
    if (param.file_format == "orc") {
        file_list_0 = {"data-14a30421-7650-486c-9876-66a1fa4356ff-0.orc"};
        file_list_1 = {"data-d39c4ccc-6245-460c-bd70-632bd2b26234-0.orc"};
        file_list_2 = {"data-b20718c4-b2e1-4928-b563-11539edc9572-0.orc"};
    } else if (param.file_format == "parquet") {
        file_list_0 = {"data-f8754699-0c43-4e53-be00-7e8af1754913-0.parquet"};
        file_list_1 = {"data-ac0894ca-fc13-49c8-bb22-4556c8ee416c-0.parquet"};
        file_list_2 = {"data-e0a6a424-e3b0-47ba-b259-d033fc01e87c-0.parquet"};
    }

    DataSplitsSimple input_data_splits = {
        {paimon::test::GetDataDir() + "/" + param.file_format +
             "/append_complex_data.db/append_complex_data/f1=10/bucket-0",
         BinaryRowGenerator::GenerateRow({10}, pool_.get()), file_list_0},
        {paimon::test::GetDataDir() + "/" + param.file_format +
             "/append_complex_data.db/append_complex_data/f1=20/bucket-0",
         BinaryRowGenerator::GenerateRow({20}, pool_.get()), file_list_1},
        {paimon::test::GetDataDir() + "/" + param.file_format +
             "/append_complex_data.db/append_complex_data/f1=20/bucket-1",
         BinaryRowGenerator::GenerateRow({20}, pool_.get()), file_list_2}};
    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/4);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [0, "add", 1, "2033-05-18 03:33:20.0",         1234,  "123456789987654321.45678"],
        [0, "cat", 1, "2033-05-18 03:33:20.000001001", 19909, "12.30000"],
        [0, "fat", 1, "1899-01-01 00:59:20.001001001", null,  "0.00000"],
        [0, "bad", 1, "1899-01-01 00:59:20.001001001", -1234, "-123456789987654321.45678"]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array);
    ASSERT_TRUE(result_array->Equals(*expected_array)) << result_array->ToString();
}

TEST_P(ReadInteTest, TestAppendReadWithPredicateOnlyPushdown) {
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32()))};
    ASSERT_OK_AND_ASSIGN(
        auto predicate,
        PredicateBuilder::Or(
            {PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f3",
                                           FieldType::DOUBLE, Literal(static_cast<double>(15.0))),
             PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f3", FieldType::DOUBLE)}));

    auto param = GetParam();
    std::string path =
        paimon::test::GetDataDir() + "/" + param.file_format + "/append_09.db/append_09";

    ReadContextBuilder context_builder(path);
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.SetReadSchema({"f3", "f0", "f1"});
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("read.batch-size", "2")
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy)
        .SetPredicate(predicate)
        .EnablePrefetch(param.enable_prefetch);

    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list_0;
    std::vector<std::string> file_list_1;
    std::vector<std::string> file_list_2;
    if (param.file_format == "orc") {
        file_list_0 = {"data-d41fd7d1-b3e4-4905-aad9-b20a780e90a2-0.orc"};
        file_list_1 = {"data-4e30d6c0-f109-4300-a010-4ba03047dd9d-0.orc",
                       "data-10b9eea8-241d-4e4b-8ab8-2a82d72d79a2-0.orc",
                       "data-e2bb59ee-ae25-4e5b-9bcc-257250bc5fdd-0.orc",
                       "data-2d5ea1ea-77c1-47ff-bb87-19a509962a37-0.orc"};
        file_list_2 = {"data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc",
                       "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc"};
    } else if (param.file_format == "parquet") {
        file_list_0 = {"data-46e27d5b-4850-4d1e-abb6-b3aabbbc08cb-0.parquet"};
        file_list_1 = {"data-864a052b-a938-4e04-b32c-6c72699a0c92-0.parquet",
                       "data-c0401350-64a3-4a54-a143-dd125ad9a8e5-0.parquet",
                       "data-7a912f84-04b7-4bbb-8dc6-53f4a292ea25-0.parquet",
                       "data-bb891df7-ea12-4b7e-9017-41aabe08c8ec-0.parquet"};
        file_list_2 = {"data-b446f78a-2cfb-4b3b-add8-31295d24a277-0.parquet",
                       "data-fd72a479-53ae-42f7-aec0-e982ee555928-0.parquet"};
    }

    DataSplitsSimple input_data_splits = {
        {paimon::test::GetDataDir() + "/" + param.file_format +
             "/append_09.db/append_09/f1=10/bucket-0",
         BinaryRowGenerator::GenerateRow({10}, pool_.get()), file_list_0},
        {paimon::test::GetDataDir() + "/" + param.file_format +
             "/append_09.db/append_09/f1=10/bucket-1",
         BinaryRowGenerator::GenerateRow({10}, pool_.get()), file_list_1},
        {paimon::test::GetDataDir() + "/" + param.file_format +
             "/append_09.db/append_09/f1=20/bucket-0",
         BinaryRowGenerator::GenerateRow({20}, pool_.get()), file_list_2}};

    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/4);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [0, 15.1, "Emily", 10], [0, 12.1, "Bob", 10],  [0, 16.1, "Alex", 10],
        [0, 17.1, "David", 10], [0, 17.1, "Lily", 10],  [0, null, "Paul", 20]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(*expected_array)) << result_array->ToString();
}

TEST_P(ReadInteTest, TestAppendReadWithPredicateAllFiltered) {
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32()))};

    auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f3",
                                                   FieldType::DOUBLE, Literal(25.0));

    auto param = GetParam();
    std::string path =
        paimon::test::GetDataDir() + "/" + param.file_format + "/append_09.db/append_09";

    ReadContextBuilder context_builder(path);
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.SetReadSchema({"f3", "f0", "f1"});
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("read.batch-size", "2")
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy)
        .SetPredicate(predicate)
        .EnablePredicateFilter(true)
        .EnablePrefetch(param.enable_prefetch);

    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list_0;
    std::vector<std::string> file_list_1;
    if (param.file_format == "orc") {
        file_list_0 = {"data-d41fd7d1-b3e4-4905-aad9-b20a780e90a2-0.orc"};
        file_list_1 = {"data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc",
                       "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc"};
    } else if (param.file_format == "parquet") {
        file_list_0 = {"data-46e27d5b-4850-4d1e-abb6-b3aabbbc08cb-0.parquet"};
        file_list_1 = {"data-b446f78a-2cfb-4b3b-add8-31295d24a277-0.parquet",
                       "data-fd72a479-53ae-42f7-aec0-e982ee555928-0.parquet"};
    }

    DataSplitsSimple input_data_splits = {
        {paimon::test::GetDataDir() + "/" + param.file_format +
             "/append_09.db/append_09/f1=10/bucket-0",
         BinaryRowGenerator::GenerateRow({10}, pool_.get()), file_list_0},
        {paimon::test::GetDataDir() + "/" + param.file_format +
             "/append_09.db/append_09/f1=20/bucket-0",
         BinaryRowGenerator::GenerateRow({20}, pool_.get()), file_list_1}};

    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/4);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));
    ASSERT_FALSE(result_array);
}

TEST_P(ReadInteTest, TestAppendReadIOException) {
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32()))};
    auto param = GetParam();

    std::vector<std::string> file_list_0;
    std::vector<std::string> file_list_1;
    if (param.file_format == "orc") {
        file_list_0 = {"data-d41fd7d1-b3e4-4905-aad9-b20a780e90a2-0.orc"};
        file_list_1 = {"data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc",
                       "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc"};
    } else if (param.file_format == "parquet") {
        file_list_0 = {"data-46e27d5b-4850-4d1e-abb6-b3aabbbc08cb-0.parquet"};
        file_list_1 = {"data-b446f78a-2cfb-4b3b-add8-31295d24a277-0.parquet",
                       "data-fd72a479-53ae-42f7-aec0-e982ee555928-0.parquet"};
    }

    DataSplitsSimple input_data_splits = {
        {paimon::test::GetDataDir() + "/" + param.file_format +
             "/append_09.db/append_09/f1=10/bucket-0",
         BinaryRowGenerator::GenerateRow({10}, pool_.get()), file_list_0},
        {paimon::test::GetDataDir() + "/" + param.file_format +
             "/append_09.db/append_09/f1=20/bucket-0",
         BinaryRowGenerator::GenerateRow({20}, pool_.get()), file_list_1}};

    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/4);

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [0, 11.1, "Alice", 10], [0, 14.1, "Lucy", 20], [0, null, "Paul", 20]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());

    bool run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 200; i++) {
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        ReadContextBuilder context_builder(paimon::test::GetDataDir() + "/" + param.file_format +
                                           "/append_09.db/append_09/");
        context_builder.SetReadSchema({"f3", "f0", "f1"});
        context_builder.SetPrefetchCacheMode(param.cache_mode);
        context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
            .AddOption("read.batch-size", "2")
            .EnablePrefetch(param.enable_prefetch)
            .AddOption("test.enable-adaptive-prefetch-strategy",
                       param.enable_adaptive_prefetch_strategy);

        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        Result<std::unique_ptr<TableRead>> table_read = TableRead::Create(std::move(read_context));
        CHECK_HOOK_STATUS(table_read.status(), i);
        Result<std::unique_ptr<BatchReader>> batch_reader =
            table_read.value()->CreateReader(data_splits);
        CHECK_HOOK_STATUS(batch_reader.status(), i);
        auto result = ReadResultCollector::CollectResult(batch_reader.value().get());
        CHECK_HOOK_STATUS(result.status(), i);
        auto result_array = result.value();
        ASSERT_TRUE(result_array);
        ASSERT_TRUE(result_array->Equals(*expected_array));
        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

TEST_P(ReadInteTest, TestPkTableWithDeletionVectorSimple) {
    // test with only one file in deletion vector file
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};

    auto param = GetParam();
    std::string path = paimon::test::GetDataDir() + "/" + param.file_format +
                       "/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality/";
    std::string data_file, dv_file;
    if (param.file_format == "orc") {
        data_file = "data-2ffe7ae9-2cf7-41e9-944b-2065585cde31-0.orc";
        dv_file = "index/index-86356766-3238-46e6-990b-656cd7409eaa-1";
    } else if (param.file_format == "parquet") {
        data_file = "data-ed5d7184-cf42-4f4a-bf83-1e3080d9012d-0.parquet";
        dv_file = "index/index-62372b7b-d1cf-4dde-a162-c9d9d35006b4-1";
    }
    ReadContextBuilder context_builder(path);
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("read.batch-size", "2");
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.EnablePrefetch(param.enable_prefetch)
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy);

    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    DataSplitsDv input_data_splits = {
        {path + "f1=10/bucket-1",
         BinaryRowGenerator::GenerateRow({10}, pool_.get()),
         {data_file},
         {DeletionFile(path + dv_file,
                       /*offset=*/1, /*length=*/24, /*cardinality=*/2)}}};
    auto data_splits = CreateDataSplits(input_data_splits,
                                        /*snapshot_id=*/4);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
       [0, "Alex", 10, 0, 16.1], [0, "Bob", 10, 0, 12.1],
       [0, "David", 10, 0, 17.1], [0, "Emily", 10, 0, 13.1],
       [0, "Whether I shall turn out to be the hero of my own life.", 10, 0, 19.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ReadInteTest, TestPkTableWithDeletionVector) {
    // test with only one file in deletion vector file
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};

    auto param = GetParam();

    std::string path = paimon::test::GetDataDir() + "/" + param.file_format + "/pk_09.db/pk_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("read.batch-size", "2")
        .EnablePrefetch(param.enable_prefetch)
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy);

    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list;
    std::string deletion_file;
    if (param.file_format == "orc") {
        file_list = {"data-980e82b4-2345-4976-bc1d-ea989fcdbffa-0.orc",
                     "data-1c7a85f1-55bd-424f-b503-34a33be0fb96-0.orc",
                     "data-8cdb8b8d-5830-4b3b-aa94-8a30c449277a-0.orc"};
        deletion_file = "index-7badd250-6c0b-49e9-8e40-2449ae9a2539-0";
    } else if (param.file_format == "parquet") {
        file_list = {"data-0f3a001e-ce2f-4f01-bb47-08d9901cb76c-0.parquet",
                     "data-0f3a001e-ce2f-4f01-bb47-08d9901cb76c-1.parquet",
                     "data-2b672a79-68bb-447c-8f8d-b4764e29b391-0.parquet"};
        deletion_file = "index-ff7801ad-df90-49b2-9619-586eab9c68e3-0";
    }
    DataSplitsDv input_data_splits = {
        {path + "/f1=10/bucket-0",
         BinaryRowGenerator::GenerateRow({10}, pool_.get()),
         file_list,
         {DeletionFile(path + "/index/" + deletion_file,
                       /*offset=*/31, /*length=*/22, /*cardinality=*/std::nullopt),
          DeletionFile(path + "/index/" + deletion_file,
                       /*offset=*/1, /*length=*/22, /*cardinality=*/std::nullopt),
          std::nullopt}}};
    auto data_splits = CreateDataSplits(input_data_splits,
                                        /*snapshot_id=*/6);

    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
       [0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
       [0, "Whether I shall turn out to be the hero of my own life.", 10, 1, 19.1], [0, "Alice", 10, 1, 19.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result));
}

TEST_P(ReadInteTest, TestPkTableWithSnapshot6) {
    // test multiple buckets in snapshot 6, with predicate push down and paimon filter
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};

    auto param = GetParam();

    auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f3",
                                                   FieldType::DOUBLE, Literal(15.0));
    std::string path = paimon::test::GetDataDir() + "/" + param.file_format + "/pk_09.db/pk_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("read.batch-size", "2");
    context_builder.SetPredicate(predicate);
    context_builder.EnablePredicateFilter(true);
    context_builder.EnablePrefetch(param.enable_prefetch)
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy);

    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());

    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list_0;
    std::vector<std::string> file_list_1;
    std::vector<std::string> file_list_2;
    std::string deletion_file_0;
    std::string deletion_file_1;
    if (param.file_format == "orc") {
        file_list_0 = {"data-980e82b4-2345-4976-bc1d-ea989fcdbffa-0.orc",
                       "data-1c7a85f1-55bd-424f-b503-34a33be0fb96-0.orc",
                       "data-8cdb8b8d-5830-4b3b-aa94-8a30c449277a-0.orc"};
        file_list_1 = {"data-6871b960-edd9-40fc-9859-aaca9ea205cf-0.orc"};
        file_list_2 = {"data-f27b8807-74b7-4aea-a7b4-5fcb385bd104-0.orc"};
        deletion_file_0 = "index-7badd250-6c0b-49e9-8e40-2449ae9a2539-0";
        deletion_file_1 = "index-7badd250-6c0b-49e9-8e40-2449ae9a2539-1";
    } else if (param.file_format == "parquet") {
        file_list_0 = {"data-0f3a001e-ce2f-4f01-bb47-08d9901cb76c-0.parquet",
                       "data-0f3a001e-ce2f-4f01-bb47-08d9901cb76c-1.parquet",
                       "data-2b672a79-68bb-447c-8f8d-b4764e29b391-0.parquet"};
        file_list_1 = {"data-6d416200-c9db-49ff-bc27-79837bf1aaab-0.parquet"};
        file_list_2 = {"data-cf4e088c-c6e7-4812-bb97-196f6aff271c-0.parquet"};
        deletion_file_0 = "index-ff7801ad-df90-49b2-9619-586eab9c68e3-0";
        deletion_file_1 = "index-ff7801ad-df90-49b2-9619-586eab9c68e3-1";
    }
    DataSplitsDv input_data_splits = {
        {path + "/f1=10/bucket-0",
         BinaryRowGenerator::GenerateRow({10}, pool_.get()),
         file_list_0,
         {DeletionFile(path + "/index/" + deletion_file_0,
                       /*offset=*/31, /*length=*/22, /*cardinality=*/std::nullopt),
          DeletionFile(path + "/index/" + deletion_file_0,
                       /*offset=*/1, /*length=*/22, /*cardinality=*/std::nullopt),
          std::nullopt}},
        {path + "/f1=10/bucket-1",
         BinaryRowGenerator::GenerateRow({10}, pool_.get()),
         file_list_1,
         {DeletionFile(path + "/index/" + deletion_file_1,
                       /*offset=*/1, /*length=*/22, /*cardinality=*/std::nullopt)}},
        {path + "/f1=20/bucket-0",
         BinaryRowGenerator::GenerateRow({20}, pool_.get()),
         file_list_2,
         {std::nullopt}},
    };
    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/6);

    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
       [0, "Whether I shall turn out to be the hero of my own life.", 10, 1, 19.1],
       [0, "Alice", 10, 1, 19.1], [0, "Alex", 10, 0, 16.1],
       [0, "David", 10, 0, 17.1], [0, "Paul", 20, 1, 18.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ReadInteTest, TestPkTableWithSnapshot8) {
    // test multiple buckets in snapshot 8, also recall partial fields with reversed order
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(1, arrow::field("f1", arrow::int32()))};

    auto param = GetParam();

    std::string path = paimon::test::GetDataDir() + "/" + param.file_format + "/pk_09.db/pk_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.SetReadSchema({"f0", "f3", "f1"});
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("read.batch-size", "2");
    context_builder.EnablePrefetch(param.enable_prefetch)
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());

    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list_0;
    std::vector<std::string> file_list_1;
    std::vector<std::string> file_list_2;
    std::string deletion_file;
    if (param.file_format == "orc") {
        file_list_0 = {"data-6871b960-edd9-40fc-9859-aaca9ea205cf-0.orc"};
        file_list_1 = {"data-79413425-25fd-426f-a8a3-618d57f0e9a9-0.orc"};
        file_list_2 = {"data-f27b8807-74b7-4aea-a7b4-5fcb385bd104-0.orc"};
        deletion_file = "index-7badd250-6c0b-49e9-8e40-2449ae9a2539-1";
    } else if (param.file_format == "parquet") {
        file_list_0 = {"data-6d416200-c9db-49ff-bc27-79837bf1aaab-0.parquet"};
        file_list_1 = {"data-2b672a79-68bb-447c-8f8d-b4764e29b391-1.parquet"};
        file_list_2 = {"data-cf4e088c-c6e7-4812-bb97-196f6aff271c-0.parquet"};
        deletion_file = "index-ff7801ad-df90-49b2-9619-586eab9c68e3-1";
    }

    DataSplitsDv input_data_splits = {
        {path + "/f1=10/bucket-1",
         BinaryRowGenerator::GenerateRow({10}, pool_.get()),
         file_list_0,
         {DeletionFile(path + "/index/" + deletion_file,
                       /*offset=*/1, /*length=*/22, /*cardinality=*/std::nullopt)}},
        {path + "/f1=10/bucket-0",
         BinaryRowGenerator::GenerateRow({10}, pool_.get()),
         file_list_1,
         {std::nullopt}},
        {path + "/f1=20/bucket-0",
         BinaryRowGenerator::GenerateRow({20}, pool_.get()),
         file_list_2,
         {std::nullopt}},
    };
    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/6);

    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
       [0, "Alex", 16.1, 10], [0, "Bob", 12.1, 10], [0, "David", 17.1, 10], [0, "Emily", 13.1, 10], [0, "Alice", 21.1, 10],
       [0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 11.0, 10],
       [0, "Whether I shall turn out to be the hero of my own life.", 19.1, 10], [0, "Lucy", 14.1, 20], [0, "Paul", 18.1, 20]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ReadInteTest, TestAppendReadWithSchemaEvolution) {
    auto param = GetParam();
    std::string path = paimon::test::GetDataDir() + "/" + param.file_format +
                       "/append_table_with_alter_table.db/append_table_with_alter_table/";

    auto check_result = [&](const std::optional<std::string>& specific_table_schema) {
        std::vector<DataField> read_fields = {DataField(0, arrow::field("key0", arrow::int32())),
                                              DataField(1, arrow::field("key1", arrow::int32())),
                                              DataField(6, arrow::field("k", arrow::int32())),
                                              DataField(3, arrow::field("c", arrow::int32())),
                                              DataField(7, arrow::field("d", arrow::int32())),
                                              DataField(5, arrow::field("a", arrow::int32())),
                                              DataField(8, arrow::field("e", arrow::int32()))};

        ReadContextBuilder context_builder(path);
        context_builder.SetPrefetchCacheMode(param.cache_mode);
        context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
            .AddOption("read.batch-size", "2");
        context_builder.EnablePrefetch(param.enable_prefetch)
            .AddOption("test.enable-adaptive-prefetch-strategy",
                       param.enable_adaptive_prefetch_strategy);
        if (specific_table_schema) {
            context_builder.SetTableSchema(specific_table_schema.value());
        }

        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());

        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

        std::vector<std::string> file_list_0;
        std::vector<std::string> file_list_1;
        if (param.file_format == "orc") {
            file_list_0 = {"data-2190cec3-ce87-4175-8d19-9268becf4440-0.orc",
                           "data-b34cd128-03e3-4e70-ba9c-5dec2183849c-0.orc"};
            file_list_1 = {"data-13824b84-8572-4a20-b712-c0475d1828b4-0.orc",
                           "data-492ed5ab-4740-4e93-8a0a-79a6893b1770-0.orc"};
        } else if (param.file_format == "parquet") {
            file_list_0 = {"data-512651de-64b5-4a10-8068-65403aaccdb8-0.parquet",
                           "data-1aaec161-5365-426f-b33d-3cd99a3908f2-0.parquet"};
            file_list_1 = {"data-11b12094-192f-4ad8-92a8-ae8cba5e25ef-0.parquet",
                           "data-9dfb749f-0509-4db2-ae7b-1e4448b32165-0.parquet"};
        }

        DataSplitsSchema input_data_splits = {
            {path + "key0=0/key1=1/bucket-0", BinaryRowGenerator::GenerateRow({0, 1}, pool_.get()),
             file_list_0,
             /*schema ids*/ {0, 1}},
            {path + "key0=1/key1=1/bucket-0", BinaryRowGenerator::GenerateRow({1, 1}, pool_.get()),
             file_list_1,
             /*schema ids*/ {1, 0}}};
        auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/2);
        ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
        ASSERT_OK_AND_ASSIGN(auto result_array,
                             ReadResultCollector::CollectResult(batch_reader.get()));

        auto fields_with_row_kind = read_fields;
        fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
        std::shared_ptr<arrow::DataType> arrow_data_type =
            DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [0, 0, 1, 16, 13, null, 15, null],
        [0, 0, 1, 26, 23, null, 25, null],
        [0, 0, 1, 36, 33, null, 35, null],
        [0, 0, 1, 66, 63, 517, 65, 618],
        [0, 0, 1, 76, 73, 527, 75, 628],
        [0, 0, 1, 86, 83, 537, 85, 638],
        [0, 1, 1, 96, 93, 547, 95, 648],
        [0, 1, 1, 106, 103, 557, 105, 658],
        [0, 1, 1, 46, 43, null, 45, null],
        [0, 1, 1, 56, 53, null, 55, null]
    ])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        ASSERT_TRUE(result_array->Equals(*expected_array));
    };

    // check without specific table schema
    check_result(std::nullopt);

    {
        // check with specific table schema
        auto fs = std::make_shared<LocalFileSystem>();
        std::string schema_str;
        ASSERT_OK(fs->ReadFile(path + "/schema/schema-1", &schema_str));
        check_result(std::optional<std::string>(schema_str));
    }
}

TEST_P(ReadInteTest, TestAppendReadWithSchemaEvolutionWithPredicateFilter) {
    std::vector<DataField> read_fields = {DataField(5, arrow::field("a", arrow::int32())),
                                          DataField(6, arrow::field("k", arrow::int32())),
                                          DataField(1, arrow::field("key1", arrow::int32())),
                                          DataField(7, arrow::field("d", arrow::int32())),
                                          DataField(0, arrow::field("key0", arrow::int32())),
                                          DataField(3, arrow::field("c", arrow::int32()))};

    auto not_null =
        PredicateBuilder::IsNotNull(/*field_index=*/3, /*field_name=*/"d", FieldType::INT);
    auto equal = PredicateBuilder::Equal(/*field_index=*/4, /*field_name=*/"key0", FieldType::INT,
                                         Literal(0));
    auto less_than = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"k",
                                                FieldType::INT, Literal(90));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({not_null, equal, less_than}));
    ASSERT_TRUE(predicate);

    auto param = GetParam();
    std::string path = paimon::test::GetDataDir() + "/" + param.file_format +
                       "/append_table_with_alter_table.db/append_table_with_alter_table/";
    ReadContextBuilder context_builder(path);
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.SetReadSchema({"a", "k", "key1", "d", "key0", "c"});
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("read.batch-size", "2");
    context_builder.SetPredicate(predicate);
    context_builder.EnablePredicateFilter(true);
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.EnablePrefetch(param.enable_prefetch)
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());

    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list_0;
    std::vector<std::string> file_list_1;
    if (param.file_format == "orc") {
        file_list_0 = {"data-2190cec3-ce87-4175-8d19-9268becf4440-0.orc",
                       "data-b34cd128-03e3-4e70-ba9c-5dec2183849c-0.orc"};
        file_list_1 = {"data-13824b84-8572-4a20-b712-c0475d1828b4-0.orc",
                       "data-492ed5ab-4740-4e93-8a0a-79a6893b1770-0.orc"};
    } else if (param.file_format == "parquet") {
        file_list_0 = {"data-512651de-64b5-4a10-8068-65403aaccdb8-0.parquet",
                       "data-1aaec161-5365-426f-b33d-3cd99a3908f2-0.parquet"};
        file_list_1 = {"data-11b12094-192f-4ad8-92a8-ae8cba5e25ef-0.parquet",
                       "data-9dfb749f-0509-4db2-ae7b-1e4448b32165-0.parquet"};
    }

    DataSplitsSchema input_data_splits = {
        {path + "key0=0/key1=1/bucket-0", BinaryRowGenerator::GenerateRow({0, 1}, pool_.get()),
         file_list_0,
         /*schema ids*/ {0, 1}},
        {path + "key0=1/key1=1/bucket-0", BinaryRowGenerator::GenerateRow({1, 1}, pool_.get()),
         file_list_1,
         /*schema ids*/ {1, 0}}};

    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/2);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [0, 65, 66, 1, 517, 0,  63],
        [0, 75, 76, 1, 527, 0, 73],
        [0, 85, 86, 1, 537, 0, 83]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok()) << array_status.ToString();
    ASSERT_TRUE(result_array->Equals(*expected_array)) << result_array->ToString();
}

TEST_P(ReadInteTest, TestAppendReadWithSchemaEvolutionWithPredicateOnlyPushDown) {
    std::vector<DataField> read_fields = {DataField(5, arrow::field("a", arrow::int32())),
                                          DataField(6, arrow::field("k", arrow::int32())),
                                          DataField(1, arrow::field("key1", arrow::int32())),
                                          DataField(7, arrow::field("d", arrow::int32())),
                                          DataField(0, arrow::field("key0", arrow::int32())),
                                          DataField(3, arrow::field("c", arrow::int32()))};

    // only less_than can be push down: equal is partition filter and not_null is non-exist
    // field in old data
    auto not_null =
        PredicateBuilder::IsNotNull(/*field_index=*/3, /*field_name=*/"d", FieldType::INT);
    auto equal = PredicateBuilder::Equal(/*field_index=*/4, /*field_name=*/"key0", FieldType::INT,
                                         Literal(0));
    auto less_than = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"k",
                                                FieldType::INT, Literal(90));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({not_null, equal, less_than}));

    auto param = GetParam();
    std::string path = paimon::test::GetDataDir() + "/" + param.file_format +
                       "/append_table_with_alter_table.db/"
                       "append_table_with_alter_table/";
    ReadContextBuilder context_builder(path);
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.SetReadSchema({"a", "k", "key1", "d", "key0", "c"});
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("read.batch-size", "2");
    context_builder.SetPredicate(predicate);
    context_builder.EnablePrefetch(param.enable_prefetch)
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());

    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list_0;
    std::vector<std::string> file_list_1;
    if (param.file_format == "orc") {
        file_list_0 = {"data-2190cec3-ce87-4175-8d19-9268becf4440-0.orc",
                       "data-b34cd128-03e3-4e70-ba9c-5dec2183849c-0.orc"};
        file_list_1 = {"data-13824b84-8572-4a20-b712-c0475d1828b4-0.orc",
                       "data-492ed5ab-4740-4e93-8a0a-79a6893b1770-0.orc"};
    } else if (param.file_format == "parquet") {
        file_list_0 = {"data-512651de-64b5-4a10-8068-65403aaccdb8-0.parquet",
                       "data-1aaec161-5365-426f-b33d-3cd99a3908f2-0.parquet"};
        file_list_1 = {"data-11b12094-192f-4ad8-92a8-ae8cba5e25ef-0.parquet",
                       "data-9dfb749f-0509-4db2-ae7b-1e4448b32165-0.parquet"};
    }

    DataSplitsSchema input_data_splits = {
        {path + "key0=0/key1=1/bucket-0", BinaryRowGenerator::GenerateRow({0, 1}, pool_.get()),
         file_list_0,
         /*schema ids*/ {0, 1}},
        {path + "key0=1/key1=1/bucket-0", BinaryRowGenerator::GenerateRow({1, 1}, pool_.get()),
         file_list_1,
         /*schema ids*/ {1, 0}}};

    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/2);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [0, 15, 16, 1, null, 0,  13],
        [0, 25, 26, 1, null, 0, 23],
        [0, 35, 36, 1, null, 0, 33],
        [0, 65, 66, 1, 517, 0,  63],
        [0, 75, 76, 1, 527, 0, 73],
        [0, 85, 86, 1, 537, 0, 83],
        [0, 45, 46, 1, null, 1, 43],
        [0, 55, 56, 1, null, 1, 53]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok()) << array_status.ToString();
    ASSERT_TRUE(result_array->Equals(*expected_array)) << result_array->ToString();
}

TEST_P(ReadInteTest, TestPkReadSnapshot5WithSchemaEvolution) {
    std::vector<DataField> read_fields = {DataField(1, arrow::field("key1", arrow::int32())),
                                          DataField(7, arrow::field("k", arrow::utf8())),
                                          DataField(2, arrow::field("key_2", arrow::int32())),
                                          DataField(4, arrow::field("c", arrow::int32())),
                                          DataField(8, arrow::field("d", arrow::int32())),
                                          DataField(6, arrow::field("a", arrow::int32())),
                                          DataField(0, arrow::field("key0", arrow::int32())),
                                          DataField(9, arrow::field("e", arrow::int32()))};
    auto param = GetParam();

    std::string path = paimon::test::GetDataDir() + "/" + param.file_format +
                       "/pk_table_with_alter_table.db/pk_table_with_alter_table/";
    ReadContextBuilder context_builder(path);
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.SetReadSchema({"key1", "k", "key_2", "c", "d", "a", "key0", "e"});
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("read.batch-size", "2");
    context_builder.EnablePrefetch(param.enable_prefetch)
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());

    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list_0;
    std::vector<std::string> file_list_1;
    std::string deletion_file_0;
    std::string deletion_file_1;
    if (param.file_format == "orc") {
        file_list_0 = {"data-6bb8ac04-cf0d-4f9c-9d97-ce613c22d6b3-0.orc",
                       "data-89bbaa51-a16a-4d63-bea9-a473cc8eab16-0.orc"};
        file_list_1 = {"data-3842c1d6-6b34-4b2c-a648-9e95b4fb941b-0.orc"};
        deletion_file_0 = "index-710224a0-f7db-4fe9-ad32-45f406435201-0";
        deletion_file_1 = "index-710224a0-f7db-4fe9-ad32-45f406435201-1";
    } else {
        file_list_0 = {"data-4d96625f-825f-4703-a8c6-d38353070969-0.parquet",
                       "data-b835e55a-9ace-40bf-acf5-0640c4cb2a60-0.parquet"};
        file_list_1 = {"data-8969384c-d715-4113-b663-2248c9a8c8d9-0.parquet"};
        deletion_file_0 = "index-84326ea4-5d74-40a2-8a08-9d43d29f657a-0";
        deletion_file_1 = "index-84326ea4-5d74-40a2-8a08-9d43d29f657a-1";
    }

    DataSplitsSchemaDv input_data_splits = {
        {path + "key0=0/key1=1/bucket-0",
         BinaryRowGenerator::GenerateRow({0, 1}, pool_.get()),
         file_list_0,
         /*schema ids*/ {0, 0},
         /*deletion file*/
         {DeletionFile(path + "/index/" + deletion_file_0,
                       /*offset=*/1, /*length=*/22, /*cardinality=*/std::nullopt),
          std::nullopt}},
        {path + "key0=1/key1=1/bucket-0",
         BinaryRowGenerator::GenerateRow({1, 1}, pool_.get()),
         file_list_1,
         /*schema ids*/ {0},
         /*deletion file*/
         {DeletionFile(path + "/index/" + deletion_file_1,
                       /*offset=*/1, /*length=*/24, /*cardinality=*/std::nullopt)}}};

    // as delta files in snapshot 5 is not compacted, we can only recall the data of snapshot 4
    // with new schema
    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/5);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
      [0, 1, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 2, 4, null, 6, 0, null],
      [0, 1, "Alice", 12, 94, null, 96, 0, null],
      [0, 1, "Bob", 22, 24, null, 26, 1, null],
      [0, 1, "Emily", 32, 34, null, 36, 1, null],
      [0, 1, "Alex", 52, 54, null, 56, 1, null],
      [0, 1, "David", 62, 64, null, 66, 1, null],
      [0, 1, "Whether I shall turn out to be the hero of my own life.", 72, 74, null, 76, 1, null]
])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(*expected_array));
}

TEST_P(ReadInteTest, TestPkReadSnapshot6WithSchemaEvolution) {
    std::vector<DataField> read_fields = {DataField(1, arrow::field("key1", arrow::int32())),
                                          DataField(7, arrow::field("k", arrow::utf8())),
                                          DataField(2, arrow::field("key_2", arrow::int32())),
                                          DataField(4, arrow::field("c", arrow::int32())),
                                          DataField(8, arrow::field("d", arrow::int32())),
                                          DataField(6, arrow::field("a", arrow::int32())),
                                          DataField(0, arrow::field("key0", arrow::int32())),
                                          DataField(9, arrow::field("e", arrow::int32()))};
    auto param = GetParam();
    std::string path = paimon::test::GetDataDir() + "/" + param.file_format +
                       "/pk_table_with_alter_table.db/pk_table_with_alter_table/";
    ReadContextBuilder context_builder(path);
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.SetReadSchema({"key1", "k", "key_2", "c", "d", "a", "key0", "e"});
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("read.batch-size", "2");
    context_builder.EnablePrefetch(param.enable_prefetch)
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());

    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list_0;
    std::vector<std::string> file_list_1;
    std::string deletion_file;
    if (param.file_format == "orc") {
        file_list_0 = {"data-3842c1d6-6b34-4b2c-a648-9e95b4fb941b-0.orc",
                       "data-d6d370f3-242b-45c9-8739-44bf31b2b449-0.orc"};
        file_list_1 = {"data-7b538b91-5dbb-4e16-a639-1b5c0696db8c-0.orc"};
        deletion_file = "index-51804749-ed6c-4e7b-b3e9-337cfe38499c-1";
    } else if (param.file_format == "parquet") {
        file_list_0 = {"data-8969384c-d715-4113-b663-2248c9a8c8d9-0.parquet",
                       "data-f2f38e80-7d28-4d51-90b3-c28951e5cdc0-0.parquet"};
        file_list_1 = {"data-d7a33230-223e-4d65-8e39-bc7ed26bdd32-0.parquet"};
        deletion_file = "index-c93829f3-1a72-4d88-8401-70663ce46426-1";
    }

    DataSplitsSchemaDv input_data_splits = {
        {path + "key0=1/key1=1/bucket-0",
         BinaryRowGenerator::GenerateRow({1, 1}, pool_.get()),
         file_list_0,
         /*schema ids*/ {0, 1},
         /*deletion file*/
         {DeletionFile(path + "index/" + deletion_file,
                       /*offset=*/1, /*length=*/26, /*cardinality=*/std::nullopt),
          std::nullopt}},
        {path + "key0=0/key1=1/bucket-0", BinaryRowGenerator::GenerateRow({0, 1}, pool_.get()),
         file_list_1,
         /*schema ids*/ {1},
         /*deletion file*/ {std::nullopt}}};
    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/6);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
      [0, 1, "Bob", 22, 24, null, 26, 1, null],
      [0, 1, "Emily", 32, 34, null, 36, 1, null],
      [0, 1, "David", 62, 64, null, 66, 1, null],
      [0, 1, "Whether I shall turn out to be the hero of my own life.", 72, 74, null, 76, 1, null],
      [0, 1, "Alex", 52, 514, 518, 516, 1, 519],
      [0, 1, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 2, 4, null, 6, 0, null],
      [0, 1, "Alice", 12, 94, null, 96, 0, null],
      [0, 1, "Paul", 502, 504, 508, 506, 0, 509]
])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(*expected_array));
}

TEST_P(ReadInteTest, TestPkReadSnapshot6WithSchemaEvolutionWithPredicateOnlyPushDown) {
    std::vector<DataField> read_fields = {DataField(1, arrow::field("key1", arrow::int32())),
                                          DataField(7, arrow::field("k", arrow::utf8())),
                                          DataField(2, arrow::field("key_2", arrow::int32())),
                                          DataField(4, arrow::field("c", arrow::int32())),
                                          DataField(8, arrow::field("d", arrow::int32())),
                                          DataField(6, arrow::field("a", arrow::int32())),
                                          DataField(0, arrow::field("key0", arrow::int32())),
                                          DataField(9, arrow::field("e", arrow::int32()))};
    auto param = GetParam();
    std::string path = paimon::test::GetDataDir() + "/" + param.file_format +
                       "/pk_table_with_alter_table.db/pk_table_with_alter_table/";
    // equal is partition filter which will not be applied, less_than is applied at new data,
    // but not old data, therefore 9e95b4fb941b-0.orc will not be filtered
    auto equal = PredicateBuilder::Equal(/*field_index=*/6, /*field_name=*/"key0", FieldType::INT,
                                         Literal(1));
    auto less_than = PredicateBuilder::LessThan(/*field_index=*/7, /*field_name=*/"e",
                                                FieldType::INT, Literal(510));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({equal, less_than}));

    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema({{"key1", "k", "key_2", "c", "d", "a", "key0", "e"}});
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("read.batch-size", "2");
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.SetPredicate(predicate);
    context_builder.EnablePrefetch(param.enable_prefetch)
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list_0;
    std::vector<std::string> file_list_1;
    std::string deletion_file;
    if (param.file_format == "orc") {
        file_list_0 = {"data-3842c1d6-6b34-4b2c-a648-9e95b4fb941b-0.orc",
                       "data-d6d370f3-242b-45c9-8739-44bf31b2b449-0.orc"};
        file_list_1 = {"data-7b538b91-5dbb-4e16-a639-1b5c0696db8c-0.orc"};
        deletion_file = "index-51804749-ed6c-4e7b-b3e9-337cfe38499c-1";
    } else if (param.file_format == "parquet") {
        file_list_0 = {"data-8969384c-d715-4113-b663-2248c9a8c8d9-0.parquet",
                       "data-f2f38e80-7d28-4d51-90b3-c28951e5cdc0-0.parquet"};
        file_list_1 = {"data-d7a33230-223e-4d65-8e39-bc7ed26bdd32-0.parquet"};
        deletion_file = "index-c93829f3-1a72-4d88-8401-70663ce46426-1";
    }

    DataSplitsSchemaDv input_data_splits = {
        {path + "key0=1/key1=1/bucket-0",
         BinaryRowGenerator::GenerateRow({1, 1}, pool_.get()),
         file_list_0,
         /*schema ids*/ {0, 1},
         /*deletion file*/
         {DeletionFile(path + "index/" + deletion_file,
                       /*offset=*/1, /*length=*/26, /*cardinality=*/std::nullopt),
          std::nullopt}},
        {path + "key0=0/key1=1/bucket-0", BinaryRowGenerator::GenerateRow({0, 1}, pool_.get()),
         file_list_1,
         /*schema ids*/ {1},
         /*deletion file*/ {std::nullopt}}};
    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/6);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
      [0, 1, "Bob", 22, 24, null, 26, 1, null],
      [0, 1, "Emily", 32, 34, null, 36, 1, null],
      [0, 1, "David", 62, 64, null, 66, 1, null],
      [0, 1, "Whether I shall turn out to be the hero of my own life.", 72, 74, null, 76, 1, null],
      [0, 1, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 2, 4, null, 6, 0, null],
      [0, 1, "Alice", 12, 94, null, 96, 0, null],
      [0, 1, "Paul", 502, 504, 508, 506, 0, 509]
])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(*expected_array));
}

TEST_P(ReadInteTest, TestPkReadSnapshot6WithSchemaEvolutionWithPredicateFilter) {
    std::vector<DataField> read_fields = {DataField(1, arrow::field("key1", arrow::int32())),
                                          DataField(7, arrow::field("k", arrow::utf8())),
                                          DataField(2, arrow::field("key_2", arrow::int32())),
                                          DataField(4, arrow::field("c", arrow::int32())),
                                          DataField(8, arrow::field("d", arrow::int32())),
                                          DataField(6, arrow::field("a", arrow::int32())),
                                          DataField(0, arrow::field("key0", arrow::int32())),
                                          DataField(9, arrow::field("e", arrow::int32()))};
    auto param = GetParam();
    std::string path = paimon::test::GetDataDir() + "/" + param.file_format +
                       "/pk_table_with_alter_table.db/pk_table_with_alter_table/";
    auto equal = PredicateBuilder::Equal(/*field_index=*/6, /*field_name=*/"key0", FieldType::INT,
                                         Literal(0));
    auto less_than = PredicateBuilder::LessThan(/*field_index=*/7, /*field_name=*/"e",
                                                FieldType::INT, Literal(510));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({equal, less_than}));

    ReadContextBuilder context_builder(path);
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.SetReadSchema({"key1", "k", "key_2", "c", "d", "a", "key0", "e"});
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("read.batch-size", "2");
    context_builder.SetPredicate(predicate);
    context_builder.EnablePredicateFilter(true);
    context_builder.EnablePrefetch(param.enable_prefetch)
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());

    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list_0;
    std::vector<std::string> file_list_1;
    std::string deletion_file;
    if (param.file_format == "orc") {
        file_list_0 = {"data-3842c1d6-6b34-4b2c-a648-9e95b4fb941b-0.orc",
                       "data-d6d370f3-242b-45c9-8739-44bf31b2b449-0.orc"};
        file_list_1 = {"data-7b538b91-5dbb-4e16-a639-1b5c0696db8c-0.orc"};
        deletion_file = "index-51804749-ed6c-4e7b-b3e9-337cfe38499c-1";
    } else if (param.file_format == "parquet") {
        file_list_0 = {"data-8969384c-d715-4113-b663-2248c9a8c8d9-0.parquet",
                       "data-f2f38e80-7d28-4d51-90b3-c28951e5cdc0-0.parquet"};
        file_list_1 = {"data-d7a33230-223e-4d65-8e39-bc7ed26bdd32-0.parquet"};
        deletion_file = "index-c93829f3-1a72-4d88-8401-70663ce46426-1";
    }

    DataSplitsSchemaDv input_data_splits = {
        {path + "key0=1/key1=1/bucket-0",
         BinaryRowGenerator::GenerateRow({1, 1}, pool_.get()),
         file_list_0,
         /*schema ids*/ {0, 1},
         /*deletion file*/
         {DeletionFile(path + "index/" + deletion_file,
                       /*offset=*/1, /*length=*/26, /*cardinality=*/std::nullopt),
          std::nullopt}},
        {path + "key0=0/key1=1/bucket-0", BinaryRowGenerator::GenerateRow({0, 1}, pool_.get()),
         file_list_1,
         /*schema ids*/ {1},
         /*deletion file*/ {std::nullopt}}};

    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/6);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
      [0, 1, "Paul", 502, 504, 508, 506, 0, 509]
])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(*expected_array)) << result_array->ToString();
}

TEST_P(ReadInteTest, TestAppendReadWithSchemaEvolutionWithBuildInFieldId) {
    std::vector<DataField> read_fields = {DataField(0, arrow::field("key0", arrow::int32())),
                                          DataField(1, arrow::field("key1", arrow::int32())),
                                          DataField(6, arrow::field("k", arrow::int32())),
                                          DataField(3, arrow::field("c", arrow::int32())),
                                          DataField(7, arrow::field("d", arrow::int32())),
                                          DataField(5, arrow::field("a", arrow::int32())),
                                          DataField(8, arrow::field("e", arrow::int32()))};
    auto param = GetParam();
    std::string path;
    std::vector<std::string> file_list_0;
    std::vector<std::string> file_list_1;
    if (param.file_format == "orc") {
        path = paimon::test::GetDataDir() + "/" + param.file_format +
               "/append_table_with_alter_table_build_in_fieldid.db/"
               "append_table_with_alter_table_build_in_fieldid/";

        file_list_0 = {"data-35e7027e-b12a-4ebf-ae15-4c0fe8d6a895-0.orc",
                       "data-ac270e04-7158-4c5e-b432-babe240911bf-0.orc"};
        file_list_1 = {"data-e3d1a6f0-c5ef-4ba4-bf52-dca99e8c919f-0.orc",
                       "data-eb59bfff-0979-4f01-9724-00d4a64be98e-0.orc"};
    } else if (param.file_format == "parquet") {
        path = paimon::test::GetDataDir() + "/" + param.file_format +
               "/append_table_with_alter_table.db/"
               "append_table_with_alter_table/";
        file_list_0 = {"data-512651de-64b5-4a10-8068-65403aaccdb8-0.parquet",
                       "data-1aaec161-5365-426f-b33d-3cd99a3908f2-0.parquet"};
        file_list_1 = {"data-9dfb749f-0509-4db2-ae7b-1e4448b32165-0.parquet",
                       "data-11b12094-192f-4ad8-92a8-ae8cba5e25ef-0.parquet"};
    }

    ReadContextBuilder context_builder(path);
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.SetReadSchema({"key0", "key1", "k", "c", "d", "a", "e"});
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("read.batch-size", "2");
    context_builder.EnablePrefetch(param.enable_prefetch)
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());

    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    DataSplitsSchema input_data_splits = {
        {path + "key0=0/key1=1/bucket-0", BinaryRowGenerator::GenerateRow({0, 1}, pool_.get()),
         file_list_0,
         /*schema ids*/ {0, 1}},
        {path + "key0=1/key1=1/bucket-0", BinaryRowGenerator::GenerateRow({1, 1}, pool_.get()),
         file_list_1,
         /*schema ids*/ {0, 1}}};
    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/2);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [0, 0, 1, 16, 13, null, 15, null],
        [0, 0, 1, 26, 23, null, 25, null],
        [0, 0, 1, 36, 33, null, 35, null],
        [0, 0, 1, 66, 63, 517, 65, 618],
        [0, 0, 1, 76, 73, 527, 75, 628],
        [0, 0, 1, 86, 83, 537, 85, 638],
        [0, 1, 1, 46, 43, null, 45, null],
        [0, 1, 1, 56, 53, null, 55, null],
        [0, 1, 1, 96, 93, 547, 95, 648],
        [0, 1, 1, 106, 103, 557, 105, 658]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(*expected_array));
}

TEST_P(ReadInteTest, TestAppendReadNestedType) {
    auto param = GetParam();
    auto map_type = arrow::map(arrow::int8(), arrow::int16());
    auto list_type = arrow::list(DataField::ConvertDataFieldToArrowField(
        DataField(536871936, arrow::field("item", arrow::float32()))));
    std::vector<DataField> struct_fields = {DataField(3, arrow::field("f0", arrow::boolean())),
                                            DataField(4, arrow::field("f1", arrow::int64()))};
    auto struct_type = DataField::ConvertDataFieldsToArrowStructType(struct_fields);
    std::vector<DataField> read_fields = {
        DataField(0, arrow::field("f1", map_type)),
        DataField(1, arrow::field("f2", list_type)),
        DataField(2, arrow::field("f3", struct_type)),
        DataField(5, arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO))),
        DataField(6, arrow::field("f5", arrow::date32())),
        DataField(7, arrow::field("f6", arrow::decimal128(2, 2)))};
    std::string path = paimon::test::GetDataDir() + "/" + param.file_format +
                       "/append_complex_build_in_fieldid.db/append_complex_build_in_fieldid/";
    ReadContextBuilder context_builder(path);
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("read.batch-size", "2");
    context_builder.EnablePrefetch(param.enable_prefetch)
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());

    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list;
    if (param.file_format == "orc") {
        file_list = {"data-6dac9052-36d8-4950-8f74-b2bbc082e489-0.orc"};
    } else if (param.file_format == "parquet") {
        file_list = {"data-3570e113-9ede-4c86-bf5d-040085424886-0.parquet"};
    }

    DataSplitsSchema input_data_splits = {{path + "bucket-0", BinaryRow::EmptyRow(), file_list,
                                           /*schema ids*/ {0}}};
    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/1);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [0, [[0, 0]], [0.1, 0.2], [true, 2], "1970-01-01 00:02:03.123123", 2456, "0.22"],
        [0, [[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], "1970-01-01 00:02:03.123123", 245, "0.12"],
        [0, [[1, 64], [2, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.0", 24, null]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(*expected_array)) << result_array->ToString();
}

TEST_P(ReadInteTest, TestAppendReadWithSchemaEvolutionWithCast) {
    std::vector<DataField> read_fields = {
        DataField(6, arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO))),
        DataField(0, arrow::field("key0", arrow::int32())),
        DataField(1, arrow::field("key1", arrow::int32())),
        DataField(2, arrow::field("f3", arrow::int32())),
        DataField(3, arrow::field("f1", arrow::utf8())),
        DataField(4, arrow::field("f2", arrow::decimal128(6, 3))),
        DataField(5, arrow::field("f0", arrow::boolean())),
        DataField(8, arrow::field("f6", arrow::int32()))};

    auto param = GetParam();
    std::string path = paimon::test::GetDataDir() + "/" + param.file_format +
                       "/append_table_alter_table_with_cast.db/"
                       "append_table_alter_table_with_cast/";
    ReadContextBuilder context_builder(path);
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.SetReadSchema({"f4", "key0", "key1", "f3", "f1", "f2", "f0", "f6"});
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("read.batch-size", "2");
    context_builder.EnablePrefetch(param.enable_prefetch)
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());

    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list_0;
    std::vector<std::string> file_list_1;
    if (param.file_format == "orc") {
        file_list_0 = {"data-2bcdce58-3846-4ec5-a3b4-e526d89c343b-0.orc",
                       "data-f3c60c32-3b3a-4ee7-9208-dc046b5213f5-0.orc"};
        file_list_1 = {"data-d453951e-d871-416e-9931-05283d1e772d-0.orc",
                       "data-81a1c016-765b-48c9-b209-0d8e95bf8a00-0.orc"};
    } else if (param.file_format == "parquet") {
        file_list_0 = {"data-124e046e-ab87-4454-a5e6-0f97eb3a9713-0.parquet",
                       "data-c6ed61af-aa32-4be5-bbb9-d26fcad0fda6-0.parquet"};
        file_list_1 = {"data-0a160cd8-29db-4f17-8550-633e4379db55-0.parquet",
                       "data-aa07c6cd-2405-4e22-8d0e-8ac2854c1552-0.parquet"};
    }
    DataSplitsSchema input_data_splits = {
        {path + "key0=0/key1=1/bucket-0", BinaryRowGenerator::GenerateRow({0, 1}, pool_.get()),
         file_list_0,
         /*schema ids*/ {0, 1}},
        {path + "key0=1/key1=1/bucket-0", BinaryRowGenerator::GenerateRow({1, 1}, pool_.get()),
         file_list_1,
         /*schema ids*/ {0, 1}}};
    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/2);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [0, "1970-01-05 00:00:00",            0, 1, 100, "2024-11-26 06:38:56.001000001", "0.020",    true,  null],
        [0, "1969-11-18 00:00:00",            0, 1, 110, "2024-11-26 06:38:56.011000011", "11.120",   true,  null],
        [0, "1971-03-21 00:00:00",            0, 1, 120, "2024-11-26 06:38:56.021000021", "22.220",   false, null],
        [0, "2024-11-26 06:38:56.054000054",  0, 1, 150, "2024-11-26 15:28:31",           "55.002",   true,  56],
        [0, "2024-11-26 06:38:56.064000064",  0, 1, 160, "2024-11-26 15:28:41",           "666.012",  false, 66],
        [0, "2024-11-26 06:38:56.074000074",  0, 1, 170, "2024-11-26 15:28:51",           "-77.022",  true,  76],
        [0, "1957-11-01 00:00:00",            1, 1, 130, "2024-11-26 06:38:56.031000031", "333.320",  false, null],
        [0, "2091-09-07 00:00:00",            1, 1, 140, "2024-11-26 06:38:56.041000041", "444.420",  true,  null],
        [0, "2024-11-26 06:38:56.084000084",  1, 1, 180, "2024-11-26 15:29:01",           "8.032",    true,  -86],
        [0, "2024-11-26 06:38:56.094000094",  1, 1, 190, "I'm strange",                   "-999.420", false, 96]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(*expected_array));
}

TEST_P(ReadInteTest, TestAppendReadWithSchemaEvolutionWithCastWithPredicatePushDown) {
    std::vector<DataField> read_fields = {
        DataField(6, arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO))),
        DataField(0, arrow::field("key0", arrow::int32())),
        DataField(1, arrow::field("key1", arrow::int32())),
        DataField(2, arrow::field("f3", arrow::int32())),
        DataField(3, arrow::field("f1", arrow::utf8())),
        DataField(4, arrow::field("f2", arrow::decimal128(6, 3))),
        DataField(5, arrow::field("f0", arrow::boolean())),
        DataField(8, arrow::field("f6", arrow::int32()))};
    // greater_than will not push down, as with casting, only support integer predicate push
    // down
    std::string str_literal = "2024-11-26 06:38:56.022000000";
    auto greater_than = PredicateBuilder::GreaterThan(
        /*field_index=*/4, /*field_name=*/"f1", FieldType::STRING,
        Literal(FieldType::STRING, str_literal.data(), str_literal.size()));
    auto less_than = PredicateBuilder::LessThan(/*field_index=*/3, /*field_name=*/"f3",
                                                FieldType::INT, Literal(175));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({greater_than, less_than}));
    auto param = GetParam();
    std::string path = paimon::test::GetDataDir() + "/" + param.file_format +
                       "/append_table_alter_table_with_cast.db/"
                       "append_table_alter_table_with_cast/";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema({"f4", "key0", "key1", "f3", "f1", "f2", "f0", "f6"});
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format)
        .AddOption("read.batch-size", "2");
    context_builder.SetPredicate(predicate);
    context_builder.EnablePrefetch(param.enable_prefetch)
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());

    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list_0;
    std::vector<std::string> file_list_1;
    if (param.file_format == "orc") {
        file_list_0 = {"data-2bcdce58-3846-4ec5-a3b4-e526d89c343b-0.orc",
                       "data-f3c60c32-3b3a-4ee7-9208-dc046b5213f5-0.orc"};
        file_list_1 = {"data-d453951e-d871-416e-9931-05283d1e772d-0.orc",
                       "data-81a1c016-765b-48c9-b209-0d8e95bf8a00-0.orc"};
    } else if (param.file_format == "parquet") {
        file_list_0 = {"data-124e046e-ab87-4454-a5e6-0f97eb3a9713-0.parquet",
                       "data-c6ed61af-aa32-4be5-bbb9-d26fcad0fda6-0.parquet"};
        file_list_1 = {"data-0a160cd8-29db-4f17-8550-633e4379db55-0.parquet",
                       "data-aa07c6cd-2405-4e22-8d0e-8ac2854c1552-0.parquet"};
    }

    DataSplitsSchema input_data_splits = {
        {path + "key0=0/key1=1/bucket-0", BinaryRowGenerator::GenerateRow({0, 1}, pool_.get()),
         file_list_0,
         /*schema ids*/ {0, 1}},
        {path + "key0=1/key1=1/bucket-0", BinaryRowGenerator::GenerateRow({1, 1}, pool_.get()),
         file_list_1,
         /*schema ids*/ {0, 1}}};
    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/2);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [0, "1970-01-05 00:00:00",            0, 1, 100, "2024-11-26 06:38:56.001000001", "0.020",    true,  null],
        [0, "1969-11-18 00:00:00",            0, 1, 110, "2024-11-26 06:38:56.011000011", "11.120",   true,  null],
        [0, "1971-03-21 00:00:00",            0, 1, 120, "2024-11-26 06:38:56.021000021", "22.220",   false, null],
        [0, "2024-11-26 06:38:56.054000054",  0, 1, 150, "2024-11-26 15:28:31",           "55.002",   true,  56],
        [0, "2024-11-26 06:38:56.064000064",  0, 1, 160, "2024-11-26 15:28:41",           "666.012",  false, 66],
        [0, "2024-11-26 06:38:56.074000074",  0, 1, 170, "2024-11-26 15:28:51",           "-77.022",  true,  76],
        [0, "1957-11-01 00:00:00",            1, 1, 130, "2024-11-26 06:38:56.031000031", "333.320",  false, null],
        [0, "2091-09-07 00:00:00",            1, 1, 140, "2024-11-26 06:38:56.041000041", "444.420",  true,  null]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(*expected_array)) << result_array->ToString();
}

TEST_P(ReadInteTest, TestReadWithPKFallBackBranch) {
    auto param = GetParam();
    std::string path = paimon::test::GetDataDir() + "/" + param.file_format +
                       "/append_table_with_rt_branch.db/append_table_with_rt_branch";
    std::vector<std::shared_ptr<Split>> data_splits;
    data_splits.reserve(3);
    for (size_t i = 0; i < 3; ++i) {
        std::string file_name = path + "/data-splits/data_split-" + std::to_string(i);
        auto split = GetDataSplitFromFile(file_name);
        ASSERT_TRUE(std::dynamic_pointer_cast<FallbackDataSplit>(split));
        data_splits.push_back(split);
    }

    auto check_result = [&](const std::optional<std::string>& specific_table_schema) {
        std::vector<DataField> read_fields = {
            DataField(0, arrow::field("dt", arrow::utf8())),
            DataField(1, arrow::field("name", arrow::utf8())),
            DataField(2, arrow::field("amount", arrow::int32())),
        };

        ReadContextBuilder context_builder(path);
        context_builder.SetPrefetchCacheMode(param.cache_mode);
        context_builder.EnablePrefetch(param.enable_prefetch)
            .AddOption(Options::FILE_FORMAT, param.file_format)
            .AddOption("test.enable-adaptive-prefetch-strategy",
                       param.enable_adaptive_prefetch_strategy);
        if (specific_table_schema) {
            context_builder.SetTableSchema(specific_table_schema.value());
        }
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
        ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
        ASSERT_OK_AND_ASSIGN(auto result_array,
                             ReadResultCollector::CollectResult(batch_reader.get()));

        auto fields_with_row_kind = read_fields;
        fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
        std::shared_ptr<arrow::DataType> arrow_data_type =
            DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [0, "20240725", "apple", 5],
        [0, "20240725", "banana", 7],
        [0, "20240726", "cherry", 3],
        [0, "20240726", "pear", 6]
    ])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        ASSERT_TRUE(result_array->Equals(*expected_array));
    };

    // check without specific table schema
    check_result(std::nullopt);
    {
        // check with specific table schema
        auto fs = std::make_shared<LocalFileSystem>();
        std::string schema_str;
        ASSERT_OK(fs->ReadFile(path + "/schema/schema-1", &schema_str));
        check_result(std::optional<std::string>(schema_str));
    }
}

TEST_P(ReadInteTest, TestReadWithAppendFallBackBranch) {
    auto param = GetParam();
    std::string path = paimon::test::GetDataDir() + "/" + param.file_format +
                       "/append_table_with_append_pt_branch.db/append_table_with_append_pt_branch";
    std::vector<std::shared_ptr<Split>> data_splits;
    data_splits.reserve(2);
    for (size_t i = 0; i < 2; ++i) {
        std::string file_name = path + "/data-splits/data_split-" + std::to_string(i);
        auto split = GetDataSplitFromFile(file_name);
        ASSERT_TRUE(std::dynamic_pointer_cast<FallbackDataSplit>(split));
        data_splits.push_back(split);
    }

    ReadContextBuilder context_builder(path);
    context_builder.EnablePrefetch(param.enable_prefetch);
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));

    std::vector<DataField> read_fields = {
        DataField(0, arrow::field("pt", arrow::int32())),
        DataField(1, arrow::field("value", arrow::int32())),
        DataField(2, arrow::field("value2", arrow::int32())),
    };

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [0, 1, 110, null],
        [0, 1, 120, 1200],
        [0, 2, 210, null],
        [0, 2, 220, 2200]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(*expected_array));
}

TEST_P(ReadInteTest, TestFallBackBranchStreamRead) {
    auto param = GetParam();
    std::string path = paimon::test::GetDataDir() + "/" + param.file_format +
                       "/append_table_with_rt_branch.db/append_table_with_rt_branch";

    std::string split_file_name = path + "/data-splits/data_split-stream";
    auto data_split = GetDataSplitFromFile(split_file_name);
    ASSERT_FALSE(std::dynamic_pointer_cast<FallbackDataSplit>(data_split));

    std::vector<DataField> read_fields = {DataField(0, arrow::field("dt", arrow::utf8())),
                                          DataField(1, arrow::field("name", arrow::utf8())),
                                          DataField(2, arrow::field("amount", arrow::int32()))};
    ReadContextBuilder context_builder(path);
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.EnablePrefetch(param.enable_prefetch)
        .AddOption("test.enable-adaptive-prefetch-strategy",
                   param.enable_adaptive_prefetch_strategy);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_split));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
      [0, "20240725", "apple", 5],
      [0, "20240725", "banana", 7]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(expected_array));
}

TEST_P(ReadInteTest, TestReadWithPKRtBranch) {
    auto param = GetParam();
    std::string path = paimon::test::GetDataDir() + "/" + param.file_format +
                       "/append_table_with_rt_branch.db/append_table_with_rt_branch";
    std::vector<std::shared_ptr<Split>> data_splits;
    data_splits.reserve(4);
    for (size_t i = 0; i < 4; ++i) {
        std::string file_name = path + "/data-splits/data_split-rt-" + std::to_string(i);
        auto split = GetDataSplitFromFile(file_name);
        ASSERT_FALSE(std::dynamic_pointer_cast<FallbackDataSplit>(split));
        data_splits.push_back(split);
    }

    auto check_result = [&](const std::optional<std::string>& specific_table_schema) {
        std::vector<DataField> read_fields = {
            DataField(0, arrow::field("dt", arrow::utf8())),
            DataField(1, arrow::field("name", arrow::utf8())),
            DataField(2, arrow::field("amount", arrow::int32())),
        };

        ReadContextBuilder context_builder(path);
        context_builder.SetPrefetchCacheMode(param.cache_mode);
        context_builder.EnablePrefetch(param.enable_prefetch)
            .AddOption("test.enable-adaptive-prefetch-strategy",
                       param.enable_adaptive_prefetch_strategy)
            .WithBranch("rt");
        if (specific_table_schema) {
            context_builder.SetTableSchema(specific_table_schema.value());
        }
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
        ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
        ASSERT_OK_AND_ASSIGN(auto result_array,
                             ReadResultCollector::CollectResult(batch_reader.get()));

        auto fields_with_row_kind = read_fields;
        fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
        std::shared_ptr<arrow::DataType> arrow_data_type =
            DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [0, "20240726", "cherry", 3],
        [0, "20240726", "pear", 6],
        [0, "20240725", "apple", 4],
        [0, "20240725", "peach", 10]
    ])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        ASSERT_TRUE(result_array->Equals(*expected_array));
    };

    // check without specific table schema
    check_result(std::nullopt);
    // when read with rt branch, specific table schema takes no effective
    check_result("no use schema");
}

TEST_P(ReadInteTest, TestReadWithAppendPtBranch) {
    auto param = GetParam();
    std::string path = paimon::test::GetDataDir() + "/" + param.file_format +
                       "/append_table_with_append_pt_branch.db/append_table_with_append_pt_branch";
    std::vector<std::shared_ptr<Split>> data_splits;
    for (size_t i = 0; i < 1; ++i) {
        std::string file_name = path + "/data-splits/data_split-pt-" + std::to_string(i);
        auto split = GetDataSplitFromFile(file_name);
        ASSERT_FALSE(std::dynamic_pointer_cast<FallbackDataSplit>(split));
        data_splits.push_back(split);
    }

    auto check_result = [&](const std::optional<std::string>& specific_table_schema) {
        std::vector<DataField> read_fields = {
            DataField(0, arrow::field("pt", arrow::int32())),
            DataField(1, arrow::field("value", arrow::int32())),
            DataField(2, arrow::field("value2", arrow::int32())),
        };

        ReadContextBuilder context_builder(path);
        context_builder.SetPrefetchCacheMode(param.cache_mode);
        context_builder.EnablePrefetch(param.enable_prefetch)
            .AddOption("test.enable-adaptive-prefetch-strategy",
                       param.enable_adaptive_prefetch_strategy)
            .WithBranch("test");
        if (specific_table_schema) {
            context_builder.SetTableSchema(specific_table_schema.value());
        }
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
        ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
        ASSERT_OK_AND_ASSIGN(auto result_array,
                             ReadResultCollector::CollectResult(batch_reader.get()));

        auto fields_with_row_kind = read_fields;
        fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
        std::shared_ptr<arrow::DataType> arrow_data_type =
            DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [0, 2, 210, null],
        [0, 2, 220, 2200]
    ])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        ASSERT_TRUE(result_array->Equals(*expected_array));
    };

    // check without specific table schema
    check_result(std::nullopt);
    // when read with rt branch, specific table schema takes no effective
    check_result("no use schema");
}

TEST_P(ReadInteTest, TestSpecificFs) {
    class CountableInputStream : public InputStream {
     public:
        CountableInputStream(const std::shared_ptr<InputStream>& input, size_t* io_count)
            : input_(input), io_count_(io_count) {}
        ~CountableInputStream() override = default;

        Status Seek(int64_t offset, SeekOrigin origin) override {
            return input_->Seek(offset, origin);
        }
        Result<int64_t> GetPos() const override {
            return input_->GetPos();
        }
        Result<int32_t> Read(char* buffer, uint32_t size) override {
            (*io_count_)++;
            return input_->Read(buffer, size);
        }
        Result<int32_t> Read(char* buffer, uint32_t size, uint64_t offset) override {
            (*io_count_)++;
            return input_->Read(buffer, size, offset);
        }
        void ReadAsync(char* buffer, uint32_t size, uint64_t offset,
                       std::function<void(Status)>&& callback) override {
            (*io_count_)++;
            return input_->ReadAsync(buffer, size, offset, std::move(callback));
        }

        Status Close() override {
            return input_->Close();
        }
        Result<std::string> GetUri() const override {
            return input_->GetUri();
        }
        Result<uint64_t> Length() const override {
            return input_->Length();
        }

        std::shared_ptr<InputStream> input_;
        size_t* io_count_;
    };

    class CountableFileSystem : public FileSystem {
     public:
        CountableFileSystem(const std::shared_ptr<FileSystem>& fs, size_t* io_count)
            : fs_(fs), io_count_(io_count) {}
        ~CountableFileSystem() override = default;

        Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input, fs_->Open(path));
            return std::make_unique<CountableInputStream>(input, io_count_);
        }
        Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                     bool overwrite) const override {
            return Status::Invalid("Not Implemented for CountableFileSystem");
        }
        Status Mkdirs(const std::string& path) const override {
            return fs_->Mkdirs(path);
        }
        Status Rename(const std::string& src, const std::string& dst) const override {
            return fs_->Rename(src, dst);
        }
        Status Delete(const std::string& path, bool recursive = true) const override {
            return fs_->Delete(path, recursive);
        }
        Result<std::unique_ptr<FileStatus>> GetFileStatus(const std::string& path) const override {
            return fs_->GetFileStatus(path);
        }
        Status ListDir(const std::string& directory,
                       std::vector<std::unique_ptr<BasicFileStatus>>* status_list) const override {
            return fs_->ListDir(directory, status_list);
        }
        Status ListFileStatus(
            const std::string& path,
            std::vector<std::unique_ptr<FileStatus>>* status_list) const override {
            return fs_->ListFileStatus(path, status_list);
        }
        Result<bool> Exists(const std::string& path) const override {
            return fs_->Exists(path);
        }

        std::shared_ptr<FileSystem> fs_;
        size_t* io_count_;
    };

    size_t io_count = 0;
    auto param = GetParam();
    std::string path =
        paimon::test::GetDataDir() + "/" + param.file_format + "/append_09.db/append_09";
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};

    auto countable_fs =
        std::make_shared<CountableFileSystem>(std::make_shared<LocalFileSystem>(), &io_count);
    ReadContextBuilder context_builder(path);
    context_builder.SetPrefetchCacheMode(param.cache_mode);
    context_builder.AddOption(Options::FILE_FORMAT, param.file_format);
    context_builder.EnablePrefetch(param.enable_prefetch)
        .AddOption("test.enable-adaptive-prefetch-strategy", "false")
        .AddOption("orc.read.enable-metrics", "true")
        .WithFileSystem(countable_fs);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::string> file_list;
    if (param.file_format == "orc") {
        file_list = {"data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc",
                     "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc"};
    } else if (param.file_format == "parquet") {
        file_list = {"data-b446f78a-2cfb-4b3b-add8-31295d24a277-0.parquet",
                     "data-fd72a479-53ae-42f7-aec0-e982ee555928-0.parquet"};
    }
    DataSplitsSimple input_data_splits = {{paimon::test::GetDataDir() + "/" + param.file_format +
                                               "/append_09.db/append_09/f1=20/"
                                               "bucket-0",
                                           BinaryRowGenerator::GenerateRow({20}, pool_.get()),
                                           file_list}};

    auto data_splits = CreateDataSplits(input_data_splits, /*snapshot_id=*/3);
    ASSERT_EQ(data_splits.size(), 1);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));

    auto fields_with_row_kind = read_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
      [0, "Lucy", 20, 1, 14.1],
      [0, "Paul", 20, 1, null]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(expected_array));
    ASSERT_GT(io_count, 0);
}

}  // namespace paimon::test
