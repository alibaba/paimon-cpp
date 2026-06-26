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

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/table/source/startup_mode.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/counting_cache_test_utils.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon {
class DataSplit;
class RecordBatch;
}  // namespace paimon

namespace paimon::test {
// This is a sdk end-to-end test demo that supports write, commit, scan, and read operations.
class WriteAndReadInteTest
    : public ::testing::Test,
      public ::testing::WithParamInterface<std::pair<std::string, std::string>> {
    void SetUp() override {
        auto [file_format, file_system] = GetParam();
        dir_ = UniqueTestDirectory::Create(file_system);
        test_dir_ = dir_->Str();
    }
    void TearDown() override {
        dir_.reset();
    }

    std::map<std::string, std::string> AddOptionsForJindo(
        const std::map<std::string, std::string>& options) const {
        std::map<std::string, std::string> jindo_options = GetJindoTestOptions();
        auto new_options = options;
        new_options.insert(jindo_options.begin(), jindo_options.end());
        return new_options;
    }

    Status WriteNextSchema(const std::vector<DataField>& fields, int32_t highest_field_id,
                           const std::map<std::string, std::string>& options) const {
        auto file_system = dir_->GetFileSystem();
        std::string table_path = PathUtil::JoinPath(test_dir_, "foo.db/bar");
        SchemaManager schema_manager(file_system, table_path);
        PAIMON_ASSIGN_OR_RAISE(auto latest_schema_opt, schema_manager.Latest());
        if (!latest_schema_opt) {
            return Status::Invalid("table schema does not exist");
        }
        auto next_schema = std::make_shared<TableSchema>(*latest_schema_opt.value());
        next_schema->id_ = latest_schema_opt.value()->Id() + 1;
        next_schema->fields_ = fields;
        next_schema->highest_field_id_ = highest_field_id;
        next_schema->options_ = options;
        PAIMON_ASSIGN_OR_RAISE(std::string schema_content, next_schema->ToJsonString());
        std::string schema_path = PathUtil::JoinPath(schema_manager.SchemaDirectory(),
                                                     "schema-" + std::to_string(next_schema->Id()));
        return file_system->AtomicStore(schema_path, schema_content);
    }

    Result<bool> ReadAndCheckProjectedResult(const std::map<std::string, std::string>& options,
                                             const std::vector<std::string>& read_fields,
                                             const std::shared_ptr<arrow::DataType>& expected_type,
                                             const std::string& expected_data) const {
        std::string table_path = PathUtil::JoinPath(test_dir_, "foo.db/bar");
        PAIMON_ASSIGN_OR_RAISE(auto plan, InnerScan(options));

        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.SetOptions(options).SetReadFieldNames(read_fields);
        PAIMON_ASSIGN_OR_RAISE(auto read_context, read_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_read, TableRead::Create(std::move(read_context)));
        PAIMON_ASSIGN_OR_RAISE(auto batch_reader, table_read->CreateReader(plan->Splits()));
        PAIMON_ASSIGN_OR_RAISE(auto actual, ReadResultCollector::CollectResult(batch_reader.get()));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            auto expected, arrow::ipc::internal::json::ArrayFromJSON(expected_type, expected_data));
        return std::make_shared<arrow::ChunkedArray>(expected)->Equals(actual);
    }

    /// Read with a custom Arrow read schema (supports nested column pruning and
    /// paimon.map.selected-keys metadata for shared-shredding partial key recall).
    Result<bool> ReadAndCheckWithReadSchema(const std::map<std::string, std::string>& options,
                                            const std::shared_ptr<arrow::Schema>& read_schema,
                                            const std::shared_ptr<arrow::DataType>& expected_type,
                                            const std::string& expected_data) const {
        std::string table_path = PathUtil::JoinPath(test_dir_, "foo.db/bar");
        PAIMON_ASSIGN_OR_RAISE(auto plan, InnerScan(options));

        auto c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*read_schema, c_schema.get()));
        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
        PAIMON_ASSIGN_OR_RAISE(auto read_context, read_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_read, TableRead::Create(std::move(read_context)));
        PAIMON_ASSIGN_OR_RAISE(auto batch_reader, table_read->CreateReader(plan->Splits()));
        PAIMON_ASSIGN_OR_RAISE(auto actual, ReadResultCollector::CollectResult(batch_reader.get()));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            auto expected, arrow::ipc::internal::json::ArrayFromJSON(expected_type, expected_data));
        return std::make_shared<arrow::ChunkedArray>(expected)->Equals(actual);
    }

    Result<std::shared_ptr<Plan>> InnerScan(
        const std::map<std::string, std::string>& options) const {
        std::string table_path = PathUtil::JoinPath(test_dir_, "foo.db/bar");
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.SetOptions(options).AddOption(Options::SCAN_MODE,
                                                           StartupMode::LatestFull().ToString());
        PAIMON_ASSIGN_OR_RAISE(auto scan_context, scan_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_scan, TableScan::Create(std::move(scan_context)));
        return table_scan->CreatePlan();
    }

 private:
    std::string test_dir_;
    std::unique_ptr<UniqueTestDirectory> dir_;
};

TEST_P(WriteAndReadInteTest, TestAppendSimple) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::int32())};
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    // manifest and file format are upper case
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, StringUtils::ToUpperCase(file_system)},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));
    int64_t commit_identifier = 0;
    std::string data = R"([
            ["banana", 2],
            ["dog", 1],
            ["lucy", 14],
            ["mouse", 100]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
            [0, "banana", 2],
            [0, "dog", 1],
            [0, "lucy", 14],
            [0, "mouse", 100]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestPKSimple) {
    arrow::FieldVector fields = {
        arrow::field("pk", arrow::utf8()),
        arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::float64()),
    };
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},         {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},        {Options::BUCKET, "1"},
        {Options::FILE_SYSTEM, file_system},        {"orc.read.enable-lazy-decoding", "true"},
        {"orc.dictionary-key-size-threshold", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{},
                                                         /*primary_keys=*/{"pk"}, options,
                                                         /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;
    std::string data_1 = R"([
            ["lucy", 14, 5.2],
            ["dog", 1, 4.1],
            ["banana", 2, 3.0],
            ["mouse", 100, 10.3]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_1,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch_1), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    std::string data_2 = R"([
            ["apple", 20, 23.0],
            ["mouse", 200, 20.3],
            ["dog", 21, 24.1]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_2,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_2,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(commit_msgs,
                         helper->WriteAndCommit(std::move(batch_2), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string data = R"([
            [0, "apple", 20, 23.0],
            [0, "banana", 2, 3.0],
            [0, "dog", 21, 24.1],
            [0, "lucy", 14, 5.2],
            [0, "mouse", 200, 20.3]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(data_type, data_splits, data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestNestedType) {
    // map use list(struct(key, value)) as lance does not support map
    arrow::FieldVector fields = {
        arrow::field("f1", arrow::list(arrow::struct_({arrow::field("key", arrow::int8()),
                                                       arrow::field("value", arrow::int16())}))),
        arrow::field("f2", arrow::list(arrow::float32())),
        arrow::field("f3", arrow::struct_({arrow::field("f0", arrow::boolean()),
                                           arrow::field("f1", arrow::int64())})),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f5", arrow::date32()),
        arrow::field("f6", arrow::decimal128(2, 2))};
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},  {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));
    int64_t commit_identifier = 0;
    std::string data = R"([
        [[[0, 0]], [0.1, 0.2], [true, 2], "1970-01-01 00:02:03.123123", 2456, "0.22"],
        [[[0, 1]], [0.1, 0.3], [true, 1], "1970-01-01 00:02:03.999999", 24, "0.28"],
        [[[10, 10]], [1.1, 1.2], [false, 12], "1970-01-01 00:02:03.123123", 2456, "0.22"],
        [[[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], "1970-01-01 00:02:03.123123", 245, "0.12"],
        [[[1, 64], [2, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.0", 24, "0.78"],
        [[[11, 64], [12, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.123123", 24, "0.78"]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));

    std::string expected_data = R"([
        [0, [[0, 0]], [0.1, 0.2], [true, 2], "1970-01-01 00:02:03.123123", 2456, "0.22"],
        [0, [[0, 1]], [0.1, 0.3], [true, 1], "1970-01-01 00:02:03.999999", 24, "0.28"],
        [0, [[10, 10]], [1.1, 1.2], [false, 12], "1970-01-01 00:02:03.123123", 2456, "0.22"],
        [0, [[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], "1970-01-01 00:02:03.123123", 245, "0.12"],
        [0, [[1, 64], [2, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.0", 24, "0.78"],
        [0, [[11, 64], [12, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.123123", 24, "0.78"]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestAppendExternalPath) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    // create external path dir
    auto external_dir1 = UniqueTestDirectory::Create(file_system);
    ASSERT_TRUE(external_dir1);
    std::string external_test_dir1 = external_dir1->Str();
    auto external_dir2 = UniqueTestDirectory::Create(file_system);
    ASSERT_TRUE(external_dir2);
    std::string external_test_dir2 = external_dir2->Str();

    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::FILE_SYSTEM, file_system},
        {Options::DATA_FILE_PREFIX, "test-data-"},
        {Options::DATA_FILE_EXTERNAL_PATHS_STRATEGY, "round-robin"}};
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
        options[Options::DATA_FILE_EXTERNAL_PATHS] = external_test_dir1 + "," + external_test_dir2;
    } else {
        options[Options::DATA_FILE_EXTERNAL_PATHS] =
            "FILE://" + external_test_dir1 + ",FILE://" + external_test_dir2;
    }
    // create table
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{"f1"},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/true));

    int64_t commit_identifier = 0;
    // write snapshot1
    std::string data1 = R"([
        ["Alice", 10, 0, 11.1],
        ["Bob", 10, 1, 12.1],
        ["Cathy", 10, 0, 13.1],
        ["Emily", 10, 0, 14.1]
    ])";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch1,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), data1,
                                    /*partition_map=*/{{"f1", "10"}}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1,
                         helper->WriteAndCommit(std::move(batch1), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    // write snapshot2
    std::string data2 = R"([
        ["Alex", 10, 1, 21.1],
        ["Lucy", 10, 0, 22.1],
        ["Tom", 10, 1, 23.1],
        ["John", 10, 1, 24.1]
    ])";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch2,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), data2,
                                    /*partition_map=*/{{"f1", "10"}}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2,
                         helper->WriteAndCommit(std::move(batch2), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    // check external paths have data file & prefix with "test-data-"
    {
        auto get_file_list_in_external_path = [&](const UniqueTestDirectory* external_dir) {
            auto fs = external_dir->GetFileSystem();
            auto bucket_dir = external_dir->Str() + "/f1=10/bucket-0/";
            std::vector<std::unique_ptr<BasicFileStatus>> all_file_status;
            ASSERT_OK(fs->ListDir(bucket_dir, &all_file_status));
            ASSERT_FALSE(all_file_status.empty());
            for (const auto& file_status : all_file_status) {
                ASSERT_TRUE(PathUtil::GetName(file_status->GetPath()).find("test-data-") !=
                            std::string::npos);
            }
        };
        get_file_list_in_external_path(external_dir1.get());
        get_file_list_in_external_path(external_dir2.get());
    }

    // read
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
        [0, "Alice", 10, 0, 11.1],
        [0, "Bob", 10, 1, 12.1],
        [0, "Cathy", 10, 0, 13.1],
        [0, "Emily", 10, 0, 14.1],
        [0, "Alex", 10, 1, 21.1],
        [0, "Lucy", 10, 0, 22.1],
        [0, "Tom", 10, 1, 23.1],
        [0, "John", 10, 1, 24.1]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestAppendExternalPathAndNoneExternalPathStrategy) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    // create external path dir
    auto external_dir = UniqueTestDirectory::Create(file_system);
    ASSERT_TRUE(external_dir);
    std::string external_test_dir = external_dir->Str();

    // external path will not take effective as DATA_FILE_EXTERNAL_PATHS_STRATEGY default is None
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, file_format},
                                                  {Options::TARGET_FILE_SIZE, "1024"},
                                                  {Options::FILE_SYSTEM, file_system},
                                                  {Options::DATA_FILE_PREFIX, "test-data-"}};
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
        options[Options::DATA_FILE_EXTERNAL_PATHS] = external_test_dir;
    } else {
        options[Options::DATA_FILE_EXTERNAL_PATHS] = "FILE://" + external_test_dir;
    }
    // create table
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{"f1"},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/true));

    int64_t commit_identifier = 0;
    // write snapshot1
    std::string data1 = R"([
        ["Alice", 10, 0, 11.1],
        ["Bob", 10, 1, 12.1],
        ["Cathy", 10, 0, 13.1],
        ["Emily", 10, 0, 14.1]
    ])";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch1,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), data1,
                                    /*partition_map=*/{{"f1", "10"}}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1,
                         helper->WriteAndCommit(std::move(batch1), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    // write snapshot2
    std::string data2 = R"([
        ["Alex", 10, 1, 21.1],
        ["Lucy", 10, 0, 22.1],
        ["Tom", 10, 1, 23.1],
        ["John", 10, 1, 24.1]
    ])";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch2,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), data2,
                                    /*partition_map=*/{{"f1", "10"}}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2,
                         helper->WriteAndCommit(std::move(batch2), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    // check external path does not have any data file
    {
        auto fs = external_dir->GetFileSystem();
        std::vector<std::unique_ptr<BasicFileStatus>> file_status_list;
        ASSERT_OK(fs->ListDir(external_test_dir, &file_status_list));
        ASSERT_TRUE(file_status_list.empty());
    }
    // read
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
        [0, "Alice", 10, 0, 11.1],
        [0, "Bob", 10, 1, 12.1],
        [0, "Cathy", 10, 0, 13.1],
        [0, "Emily", 10, 0, 14.1],
        [0, "Alex", 10, 1, 21.1],
        [0, "Lucy", 10, 0, 22.1],
        [0, "Tom", 10, 1, 23.1],
        [0, "John", 10, 1, 24.1]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestAppendTimestampType) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
    };
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},  {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system}, {"orc.timestamp-ltz.legacy.type", "false"}};
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));
    int64_t commit_identifier = 0;
    std::string data = R"([
["1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001", "1970-01-01 00:00:02", "1970-01-01 00:00:00.002", "1970-01-01 00:00:00.000002", "1970-01-01 00:00:00.000000002"],
["1970-01-01 00:00:03", "1970-01-01 00:00:00.003", null, "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000004"],
["1970-01-01 00:00:05", "1970-01-01 00:00:00.005", null, null, "1970-01-01 00:00:06", null, "1970-01-01 00:00:00.000006", null]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
[0, "1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001", "1970-01-01 00:00:02", "1970-01-01 00:00:00.002", "1970-01-01 00:00:00.000002", "1970-01-01 00:00:00.000000002"],
[0, "1970-01-01 00:00:03", "1970-01-01 00:00:00.003", null, "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000004"],
[0, "1970-01-01 00:00:05", "1970-01-01 00:00:00.005", null, null, "1970-01-01 00:00:06", null, "1970-01-01 00:00:00.000006", null]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestPkTimestampType) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
        arrow::field("value", arrow::int32()),
    };
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},  {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "1"},
        {Options::FILE_SYSTEM, file_system}, {"orc.timestamp-ltz.legacy.type", "false"}};
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{},
                                        /*primary_keys=*/
                                        {"ts_sec", "ts_milli", "ts_micro", "ts_nano", "ts_tz_sec",
                                         "ts_tz_milli", "ts_tz_micro", "ts_tz_nano"},
                                        options, /*is_streaming_mode=*/false));
    int64_t commit_identifier = 0;
    std::string data = R"([
["1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1969-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001", "1970-01-01 00:00:02", "1970-01-01 00:00:00.002", "1970-01-01 00:00:00.000002", "1970-01-01 00:00:00.000000002", 2],
["1970-01-01 00:00:01", "1969-01-01 00:00:00.003", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000004", 0],
["1970-01-01 00:00:01", "1969-01-01 00:00:00.003", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000005", 1]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
[0, "1970-01-01 00:00:01", "1969-01-01 00:00:00.003", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000004", 0],
[0, "1970-01-01 00:00:01", "1969-01-01 00:00:00.003", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000005", 1],
[0, "1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1969-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001", "1970-01-01 00:00:02", "1970-01-01 00:00:00.002", "1970-01-01 00:00:00.000002", "1970-01-01 00:00:00.000000002", 2]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestPKWithSequenceFieldInPKField) {
    arrow::FieldVector fields = {
        arrow::field("p1", arrow::utf8()),
        arrow::field("p2", arrow::int32()),
        arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::float64()),
    };
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},       {Options::BUCKET, "1"},
        {Options::FILE_SYSTEM, file_system},       {Options::SEQUENCE_FIELD, "p2"},
        {"orc.read.enable-lazy-decoding", "true"}, {"orc.dictionary-key-size-threshold", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{},
                                                         /*primary_keys=*/{"p1", "p2"}, options,
                                                         /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;
    std::string data_1 = R"([
            ["banana", 2, 12, 13.0],
            ["lucy", 0, 14, 5.2],
            ["dog", 1, 1, 4.1],
            ["banana", 2, 2, 3.0],
            ["mouse", 3, 100, 10.3]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_1,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch_1), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    std::string data_2 = R"([
            ["apple", 0, 20, 23.0],
            ["banana", 1, 200, 20.3],
            ["dog", 1, 21, 24.1],
            ["mouse", 3, 200, 20.3]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_2,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_2,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(commit_msgs,
                         helper->WriteAndCommit(std::move(batch_2), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string data = R"([
            [0, "apple", 0, 20, 23.0],
            [0, "banana", 1, 200, 20.3],
            [0, "banana", 2, 2, 3.0],
            [0, "dog", 1, 21, 24.1],
            [0, "lucy", 0, 14, 5.2],
            [0, "mouse", 3, 200, 20.3]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(data_type, data_splits, data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestPKWithSequenceFieldPartialInPKField) {
    arrow::FieldVector fields = {
        arrow::field("p1", arrow::utf8()),
        arrow::field("p2", arrow::int32()),
        arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::float64()),
    };
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},       {Options::BUCKET, "1"},
        {Options::FILE_SYSTEM, file_system},       {Options::SEQUENCE_FIELD, "p2,f1"},
        {"orc.read.enable-lazy-decoding", "true"}, {"orc.dictionary-key-size-threshold", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{},
                                                         /*primary_keys=*/{"p1", "p2"}, options,
                                                         /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;
    std::string data_1 = R"([
            ["banana", 2, 12, 13.0],
            ["lucy", 0, 14, 5.2],
            ["dog", 1, 1, 4.1],
            ["banana", 2, 2, 3.0],
            ["mouse", 3, 100, 10.3]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_1,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch_1), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    std::string data_2 = R"([
            ["apple", 0, 20, 23.0],
            ["banana", 1, 200, 20.3],
            ["dog", 1, 21, 24.1],
            ["mouse", 3, 10, 20.3]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_2,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_2,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(commit_msgs,
                         helper->WriteAndCommit(std::move(batch_2), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string data = R"([
            [0, "apple", 0, 20, 23.0],
            [0, "banana", 1, 200, 20.3],
            [0, "banana", 2, 12, 13.0],
            [0, "dog", 1, 21, 24.1],
            [0, "lucy", 0, 14, 5.2],
            [0, "mouse", 3, 100, 10.3]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(data_type, data_splits, data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestWriteSamePartitionTwiceWithAllBasicTypesForAppend) {
    arrow::FieldVector fields = {
        arrow::field("f_bool", arrow::boolean()), arrow::field("f_int8", arrow::int8()),
        arrow::field("f_int16", arrow::int16()),  arrow::field("f_int32", arrow::int32()),
        arrow::field("f_int64", arrow::int64()),  arrow::field("f_string", arrow::utf8()),
        arrow::field("f_date", arrow::date32()),  arrow::field("f_value", arrow::int32())};
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "1"},
        {Options::BUCKET_KEY, "f_value"},
        {Options::FILE_SYSTEM, file_system},
        {Options::PARTITION_GENERATE_LEGACY_NAME, "false"},
        {Options::PARTITION_DEFAULT_NAME, "null"}};
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema,
                                        /*partition_keys=*/
                                        {"f_bool", "f_int8", "f_int16", "f_int32", "f_int64",
                                         "f_string", "f_date"},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;

    {
        std::map<std::string, std::string> partition_map = {
            {"f_bool", "true"},      {"f_int8", "1"},       {"f_int16", "100"},
            {"f_int32", "10000"},    {"f_int64", "100000"}, {"f_string", "hello"},
            {"f_date", "1970-01-02"}};

        // First write to the same partition
        std::string data1 = R"([
            [true, 1, 100, 10000, 100000, "hello", 1, 10],
            [true, 1, 100, 10000, 100000, "hello", 1, 20]
    ])";
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> batch1,
            TestHelper::MakeRecordBatch(arrow::struct_(fields), data1, partition_map,
                                        /*bucket=*/0, {}));
        ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                             helper->WriteAndCommit(std::move(batch1), commit_identifier++,
                                                    /*expected_commit_messages=*/std::nullopt));

        // Second write to the same partition
        std::string data2 = R"([
            [true, 1, 100, 10000, 100000, "hello", 1, 30],
            [true, 1, 100, 10000, 100000, "hello", 1, 40]
    ])";
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> batch2,
            TestHelper::MakeRecordBatch(arrow::struct_(fields), data2, partition_map,
                                        /*bucket=*/0, {}));
        ASSERT_OK_AND_ASSIGN(commit_msgs,
                             helper->WriteAndCommit(std::move(batch2), commit_identifier++,
                                                    /*expected_commit_messages=*/std::nullopt));
    }
    {
        // write all null for partition fields
        std::map<std::string, std::string> partition_map = {
            {"f_bool", "null"},  {"f_int8", "null"},   {"f_int16", "null"}, {"f_int32", "null"},
            {"f_int64", "null"}, {"f_string", "null"}, {"f_date", "null"}};

        // First write to the same partition
        std::string data1 = R"([
            [null, null, null, null, null, null, null, 50]
    ])";
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> batch1,
            TestHelper::MakeRecordBatch(arrow::struct_(fields), data1, partition_map,
                                        /*bucket=*/0, {}));
        ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                             helper->WriteAndCommit(std::move(batch1), commit_identifier++,
                                                    /*expected_commit_messages=*/std::nullopt));

        // Second write to the same partition
        std::string data2 = R"([
            [null, null, null, null, null, null, null, 60]
    ])";
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> batch2,
            TestHelper::MakeRecordBatch(arrow::struct_(fields), data2, partition_map,
                                        /*bucket=*/0, {}));
        ASSERT_OK_AND_ASSIGN(commit_msgs,
                             helper->WriteAndCommit(std::move(batch2), commit_identifier++,
                                                    /*expected_commit_messages=*/std::nullopt));
    }
    // Read and verify
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
            [0, true, 1, 100, 10000, 100000, "hello", 1, 10],
            [0, true, 1, 100, 10000, 100000, "hello", 1, 20],
            [0, true, 1, 100, 10000, 100000, "hello", 1, 30],
            [0, true, 1, 100, 10000, 100000, "hello", 1, 40],
            [0, null, null, null, null, null, null, null, 50],
            [0, null, null, null, null, null, null, null, 60]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestWriteSamePartitionTwiceWithAllBasicTypesForPk) {
    arrow::FieldVector fields = {
        arrow::field("f_bool", arrow::boolean()), arrow::field("f_int8", arrow::int8()),
        arrow::field("f_int16", arrow::int16()),  arrow::field("f_int32", arrow::int32()),
        arrow::field("f_int64", arrow::int64()),  arrow::field("f_string", arrow::utf8()),
        arrow::field("f_date", arrow::date32()),  arrow::field("f_value", arrow::int32()),
        arrow::field("pk", arrow::utf8())};
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, file_format},
                                                  {Options::TARGET_FILE_SIZE, "1024"},
                                                  {Options::BUCKET, "1"},
                                                  {Options::FILE_SYSTEM, file_system},
                                                  {Options::PARTITION_GENERATE_LEGACY_NAME, "true"},
                                                  {Options::PARTITION_DEFAULT_NAME, "null"}};
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper,
        TestHelper::Create(
            test_dir_, schema,
            /*partition_keys=*/
            {"f_bool", "f_int8", "f_int16", "f_int32", "f_int64", "f_string", "f_date"},
            /*primary_keys=*/
            {"pk", "f_bool", "f_int8", "f_int16", "f_int32", "f_int64", "f_string", "f_date"},
            options, /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;

    {
        std::map<std::string, std::string> partition_map = {
            {"f_bool", "true"},      {"f_int8", "1"},       {"f_int16", "100"},
            {"f_int32", "10000"},    {"f_int64", "100000"}, {"f_string", "hello"},
            {"f_date", "1970-01-02"}};

        // First write to the same partition
        std::string data1 = R"([
            [true, 1, 100, 10000, 100000,"hello", 1, 10, "pk1"],
            [true, 1, 100, 10000, 100000,"hello", 1, 20, "pk2"]
    ])";
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> batch1,
            TestHelper::MakeRecordBatch(arrow::struct_(fields), data1, partition_map,
                                        /*bucket=*/0, {}));
        ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                             helper->WriteAndCommit(std::move(batch1), commit_identifier++,
                                                    /*expected_commit_messages=*/std::nullopt));

        // Second write to the same partition
        std::string data2 = R"([
            [true, 1, 100, 10000, 100000,"hello", 1, 30, "pk1"],
            [true, 1, 100, 10000, 100000,"hello", 1, 40, "pk3"]
    ])";
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> batch2,
            TestHelper::MakeRecordBatch(arrow::struct_(fields), data2, partition_map,
                                        /*bucket=*/0, {}));
        ASSERT_OK_AND_ASSIGN(commit_msgs,
                             helper->WriteAndCommit(std::move(batch2), commit_identifier++,
                                                    /*expected_commit_messages=*/std::nullopt));
    }
    // Read and verify
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
            [0, true, 1, 100, 10000, 100000, "hello", 1, 30, "pk1"],
            [0, true, 1, 100, 10000, 100000, "hello", 1, 20, "pk2"],
            [0, true, 1, 100, 10000, 100000, "hello", 1, 40, "pk3"]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

std::vector<std::pair<std::string, std::string>> GetTestValuesForWriteAndReadInteTest() {
    std::vector<std::pair<std::string, std::string>> values = {{"parquet", "local"}};
    // values.emplace_back("parquet", "jindo");
#ifdef PAIMON_ENABLE_ORC
    values.emplace_back("orc", "local");
#endif
#ifdef PAIMON_ENABLE_LANCE
    values.emplace_back("lance", "local");
#endif
#ifdef PAIMON_ENABLE_AVRO
    values.emplace_back("avro", "local");
#endif
    return values;
}

/// End-to-end test for parquet page-level filtering with a PK table.
/// Writes data with page index enabled and small page size so multiple pages are created,
/// then reads with a PK equality predicate and verifies only matching rows are returned.
TEST_P(WriteAndReadInteTest, TestPKWithParquetPageIndexFilter) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" || file_system != "local") {
        return;
    }

    auto test_dir = UniqueTestDirectory::Create("local");
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::utf8()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, "parquet"},
        {Options::TARGET_FILE_SIZE, "1048576"},
        {Options::BUCKET, "1"},
        {Options::FILE_SYSTEM, "local"},
        // Force exactly one row per parquet page. Parquet's writer checks the page
        // byte threshold only after every `write_batch_size` values, so the default
        // batch=1024 packs all rows into a single page regardless of page.size.
        // write.batch-size=1 + page.size=1 + no dictionary together guarantee that
        // every value triggers a page flush, giving ColumnIndexFilter pages whose
        // min == max == that row's value. With predicate f0="Alice", exactly one
        // page survives page pruning, so the reader emits exactly one row -- and
        // that result is attributable purely to page filtering (no row-level
        // filter is enabled below).
        {Options::WRITE_BATCH_SIZE, "1"},
        {"parquet.page.size", "1"},
        {"parquet.enable-dictionary", "false"},
        {"parquet.write.enable-page-index", "true"},
    };
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir->Str(), schema, /*partition_keys=*/{"f1"},
                                            /*primary_keys=*/{"f0", "f1"}, options,
                                            /*is_streaming_mode=*/true));
    std::string table_path = test_dir->Str() + "/foo.db/bar";
    int64_t commit_identifier = 0;

    // Write data: 12 rows across 2 partitions
    std::string data_p1 = R"([
        ["Alice", "p1", 10, 1.1],
        ["Bob", "p1", 20, 2.2],
        ["Cathy", "p1", 30, 3.3],
        ["David", "p1", 40, 4.4],
        ["Emily", "p1", 50, 5.5],
        ["Frank", "p1", 60, 6.6]
    ])";
    std::string data_p2 = R"([
        ["Grace", "p2", 70, 7.7],
        ["Helen", "p2", 80, 8.8],
        ["Ivan", "p2", 90, 9.9],
        ["Jack", "p2", 100, 10.1],
        ["Kate", "p2", 110, 11.2],
        ["Lucy", "p2", 120, 12.3]
    ])";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_p1,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), data_p1,
                                    /*partition_map=*/{{"f1", "p1"}}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_p2,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), data_p2,
                                    /*partition_map=*/{{"f1", "p2"}}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs_1,
                         helper->WriteAndCommit(std::move(batch_p1), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs_2,
                         helper->WriteAndCommit(std::move(batch_p2), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    // Scan with PK predicate: f0 = "Alice"
    std::string literal_str = "Alice";
    auto predicate = PredicateBuilder::Equal(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
        Literal(FieldType::STRING, literal_str.data(), literal_str.size()));

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_MODE, StartupMode::LatestFull().ToString())
        .SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 2);
    ASSERT_FALSE(result_plan->Splits().empty());

    // Read with predicate but WITHOUT EnablePredicateFilter -- so any narrowing
    // of the result is attributable to split/file/RG/page pruning, not to a
    // post-read row-level filter. This is what makes the exact assertion below
    // meaningful as a check that page-index filtering is wired and working.
    ReadContextBuilder read_context_builder(table_path);
    read_context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // Expected: p2 file is pruned by file-level min/max key stats (f0 range
    // [Grace, Lucy] doesn't overlap "Alice"). Inside p1's file, write.batch-size=1
    // + page.size=1 produces one row per page, so page-index filter keeps only
    // the page whose min == max == "Alice" -- one row.
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto expected_data_type = arrow::struct_(fields_with_row_kind);
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(expected_data_type, R"([
[0, "Alice", "p1", 10, 1.1]
])")
            .ValueOrDie());
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

/// End-to-end test for parquet page-level filtering on an append-only table.
/// Append-only tables read parquet files directly without PK merge, so the result
/// reflects exactly what survives row-group and page-index pruning.
TEST_P(WriteAndReadInteTest, TestAppendWithParquetPageIndexFilter) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" || file_system != "local") {
        return;
    }

    auto test_dir = UniqueTestDirectory::Create("local");
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::utf8()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, "parquet"},
        {Options::TARGET_FILE_SIZE, "1048576"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, "local"},
        // Force exactly one row per parquet page (see the PK variant for why these
        // three options together are required). With one row per page,
        // ColumnIndexFilter keeps only the page whose min == max == "Alice", and
        // without row-level filter the reader output is precisely that one row.
        {Options::WRITE_BATCH_SIZE, "1"},
        {"parquet.page.size", "1"},
        {"parquet.enable-dictionary", "false"},
        {"parquet.write.enable-page-index", "true"},
    };
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir->Str(), schema, /*partition_keys=*/{"f1"},
                                            /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/true));
    std::string table_path = test_dir->Str() + "/foo.db/bar";
    int64_t commit_identifier = 0;

    // Write data: 12 rows across 2 partitions.
    std::string data_p1 = R"([
        ["Alice", "p1", 10, 1.1],
        ["Bob", "p1", 20, 2.2],
        ["Cathy", "p1", 30, 3.3],
        ["David", "p1", 40, 4.4],
        ["Emily", "p1", 50, 5.5],
        ["Frank", "p1", 60, 6.6]
    ])";
    std::string data_p2 = R"([
        ["Grace", "p2", 70, 7.7],
        ["Helen", "p2", 80, 8.8],
        ["Ivan", "p2", 90, 9.9],
        ["Jack", "p2", 100, 10.1],
        ["Kate", "p2", 110, 11.2],
        ["Lucy", "p2", 120, 12.3]
    ])";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_p1,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), data_p1,
                                    /*partition_map=*/{{"f1", "p1"}}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_p2,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), data_p2,
                                    /*partition_map=*/{{"f1", "p2"}}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs_1,
                         helper->WriteAndCommit(std::move(batch_p1), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs_2,
                         helper->WriteAndCommit(std::move(batch_p2), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    // Predicate: f0 = "Alice"
    std::string literal_str = "Alice";
    auto predicate = PredicateBuilder::Equal(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
        Literal(FieldType::STRING, literal_str.data(), literal_str.size()));

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_MODE, StartupMode::LatestFull().ToString())
        .SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 2);
    ASSERT_FALSE(result_plan->Splits().empty());

    // Read with predicate but WITHOUT EnablePredicateFilter, so the narrowing
    // observed below is attributable to page-index filtering rather than a
    // post-read row-level filter.
    ReadContextBuilder read_context_builder(table_path);
    read_context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // Partition p2's row groups don't overlap "Alice" (min/max f0 in [Grace, Lucy]),
    // so the whole file is skipped. Within p1, page-index pruning narrows down to the
    // page containing "Alice". With no PK merge, the result is exactly that one row.
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto expected_data_type = arrow::struct_(fields_with_row_kind);
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(expected_data_type, R"([
[0, "Alice", "p1", 10, 1.1]
])")
            .ValueOrDie());
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(WriteAndReadInteTest, TestAppendWithParquetMetadataCache) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" || file_system != "local") {
        return;
    }

    auto test_dir = UniqueTestDirectory::Create("local");
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::int32())};
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},      {Options::FILE_FORMAT, "parquet"},
        {Options::TARGET_FILE_SIZE, "1048576"}, {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, "local"},
    };
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir->Str(), schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/true));
    std::string table_path = test_dir->Str() + "/foo.db/bar";

    std::string data = R"([
        ["banana", 2],
        ["dog", 1],
        ["lucy", 14],
        ["mouse", 100]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                                /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto expected_data_type = arrow::struct_(fields_with_row_kind);
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(expected_data_type, R"([
            [0, "banana", 2],
            [0, "dog", 1],
            [0, "lucy", 14],
            [0, "mouse", 100]
        ])")
            .ValueOrDie());

    auto cache =
        std::make_shared<CountingRoutingCache>(CacheKind::DATA_FILE_FOOTER, 128 * 1024 * 1024);
    auto read_once = [&]() -> Result<bool> {
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.AddOption(Options::SCAN_MODE, StartupMode::LatestFull().ToString());
        PAIMON_ASSIGN_OR_RAISE(auto scan_context, scan_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_scan, TableScan::Create(std::move(scan_context)));
        PAIMON_ASSIGN_OR_RAISE(auto result_plan, table_scan->CreatePlan());
        if (result_plan->SnapshotId() != std::optional<int64_t>(1)) {
            return Status::Invalid("unexpected snapshot id");
        }
        if (result_plan->Splits().empty()) {
            return Status::Invalid("no splits found");
        }

        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.WithCache(cache);
        PAIMON_ASSIGN_OR_RAISE(auto read_context, read_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_read, TableRead::Create(std::move(read_context)));
        PAIMON_ASSIGN_OR_RAISE(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
        PAIMON_ASSIGN_OR_RAISE(auto read_result,
                               ReadResultCollector::CollectResult(batch_reader.get()));
        if (!read_result) {
            return Status::Invalid("read result is null");
        }
        return expected->Equals(read_result);
    };

    ASSERT_OK_AND_ASSIGN(bool first_success, read_once());
    ASSERT_TRUE(first_success);
    ASSERT_EQ(1, cache->GetCount());
    ASSERT_EQ(1, cache->SupplierCallCount());
    ASSERT_EQ(1, cache->Size());
    ASSERT_EQ(CacheKind::DATA_FILE_FOOTER, cache->LastKind());

    ASSERT_OK_AND_ASSIGN(bool second_success, read_once());
    ASSERT_TRUE(second_success);
    ASSERT_EQ(2, cache->GetCount());
    ASSERT_EQ(1, cache->SupplierCallCount());
    ASSERT_EQ(1, cache->Size());
    ASSERT_EQ(CacheKind::DATA_FILE_FOOTER, cache->LastKind());
}

TEST_P(WriteAndReadInteTest, TestAppendSharedShreddingMap) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    };
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "2"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        [1, [["a", 10], ["b", 20]]],
        [2, [["c", 30], ["a", 40], ["b", 50]]],
        [3, null],
        [4, [["d", 60], ["a", null], ["c", 70]]]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                                /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
        [0, 1, [["a", 10], ["b", 20]]],
        [0, 2, [["a", 40], ["b", 50], ["c", 30]]],
        [0, 3, null],
        [0, 4, [["a", null], ["c", 70], ["d", 60]]]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestSharedShreddingWithSchemaEvolution) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields_v0 = {
        arrow::field("f0", map_type),
        arrow::field("f1", map_type),
        arrow::field("k1", arrow::utf8()),
    };
    std::map<std::string, std::string> options_v0 = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.f0.map.storage-layout", "shared-shredding"},
        {"fields.f0.map.shared-shredding.max-columns", "1"},
        {"fields.f1.map.storage-layout", "shared-shredding"},
        {"fields.f1.map.shared-shredding.max-columns", "1"},
    };
    if (file_system == "jindo") {
        options_v0 = AddOptionsForJindo(options_v0);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields_v0),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options_v0,
                                            /*is_streaming_mode=*/false));

    ASSERT_OK_AND_ASSIGN(auto batch_v0,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields_v0),
                                                     R"([
                [[["a", 10], ["z", 11]], [["b", 20]], "old-1"],
                [[["a", 12]], [["b", 21], ["y", 22]], "old-2"]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v0), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector fields_v1 = {
        arrow::field("f0", map_type),
        arrow::field("f1", map_type),
        arrow::field("k2", arrow::utf8()),
        arrow::field("f2", map_type),
    };
    std::map<std::string, std::string> options_v1 = options_v0;
    options_v1["fields.f2.map.storage-layout"] = "shared-shredding";
    options_v1["fields.f2.map.shared-shredding.max-columns"] = "1";
    std::vector<DataField> data_fields_v1 = {
        DataField(0, fields_v1[0]),
        DataField(1, fields_v1[1]),
        DataField(2, fields_v1[2]),
        DataField(3, fields_v1[3]),
    };
    ASSERT_OK(WriteNextSchema(data_fields_v1, /*highest_field_id=*/3, options_v1));

    helper.reset();
    ASSERT_OK_AND_ASSIGN(helper,
                         TestHelper::Create(PathUtil::JoinPath(test_dir_, "foo.db/bar"), options_v1,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields_v1),
                                                     R"([
                [[["a", 30], ["z", 31]], [["b", 40]], "new-1", [["c", 50], ["x", 51]]],
                [[["a", 32]], [["b", 41]], "new-2", [["c", 52]]]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v1), /*commit_identifier=*/1,
                                     /*expected_commit_messages=*/std::nullopt));

    auto expected_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("f0", map_type),
        arrow::field("f2", map_type),
        arrow::field("k2", arrow::utf8()),
    });
    ASSERT_OK_AND_ASSIGN(bool success,
                         ReadAndCheckProjectedResult(options_v1, {"f0", "f2", "k2"}, expected_type,
                                                     R"([
                [0, [["a", 10], ["z", 11]], null, "old-1"],
                [0, [["a", 12]], null, "old-2"],
                [0, [["a", 30], ["z", 31]], [["c", 50], ["x", 51]], "new-1"],
                [0, [["a", 32]], [["c", 52]], "new-2"]
            ])"));
    ASSERT_TRUE(success);
}

// Verify storage-layout evolution: default->shared-shredding.
TEST_P(WriteAndReadInteTest, TestMapStorageLayoutDefaultToSharedShredding) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    };
    std::map<std::string, std::string> options_v0 = {
        {Options::MANIFEST_FORMAT, "avro"},  {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system}, {"fields.tags.map.storage-layout", "default"},
    };
    if (file_system == "jindo") {
        options_v0 = AddOptionsForJindo(options_v0);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options_v0,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v0,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([[1, [["a", 10], ["z", 11]]], [2, null]])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v0), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    std::map<std::string, std::string> options_v1 = options_v0;
    options_v1["fields.tags.map.storage-layout"] = "shared-shredding";
    options_v1["fields.tags.map.shared-shredding.max-columns"] = "1";
    ASSERT_OK(WriteNextSchema({DataField(0, fields[0]), DataField(1, fields[1])},
                              /*highest_field_id=*/1, options_v1));
    helper.reset();
    ASSERT_OK_AND_ASSIGN(helper,
                         TestHelper::Create(PathUtil::JoinPath(test_dir_, "foo.db/bar"), options_v1,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v1, TestHelper::MakeRecordBatch(
                                            arrow::struct_(fields),
                                            R"([[3, [["a", 30], ["z", 31]]], [4, [["a", 40]]]])",
                                            /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v1), /*commit_identifier=*/1,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector expected_fields = fields;
    expected_fields.insert(expected_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(arrow::struct_(expected_fields), splits,
                                                    R"([
                [0, 1, [["a", 10], ["z", 11]]],
                [0, 2, null],
                [0, 3, [["a", 30], ["z", 31]]],
                [0, 4, [["a", 40]]]
            ])"));
    ASSERT_TRUE(success);
}

// Verify storage-layout evolution: shared-shredding->default.
TEST_P(WriteAndReadInteTest, TestMapStorageLayoutSharedShreddingToDefault) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    };
    std::map<std::string, std::string> options_v0 = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "1"},
    };
    if (file_system == "jindo") {
        options_v0 = AddOptionsForJindo(options_v0);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options_v0,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v0, TestHelper::MakeRecordBatch(
                                            arrow::struct_(fields),
                                            R"([[1, [["a", 10], ["z", 11]]], [2, [["a", 20]]]])",
                                            /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v0), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    std::map<std::string, std::string> options_v1 = options_v0;
    options_v1["fields.tags.map.storage-layout"] = "default";
    options_v1.erase("fields.tags.map.shared-shredding.max-columns");
    ASSERT_OK(WriteNextSchema({DataField(0, fields[0]), DataField(1, fields[1])},
                              /*highest_field_id=*/1, options_v1));
    helper.reset();
    ASSERT_OK_AND_ASSIGN(helper,
                         TestHelper::Create(PathUtil::JoinPath(test_dir_, "foo.db/bar"), options_v1,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([[3, [["a", 30], ["z", 31]]], [4, null]])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v1), /*commit_identifier=*/1,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector expected_fields = fields;
    expected_fields.insert(expected_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(arrow::struct_(expected_fields), splits,
                                                    R"([
                [0, 1, [["a", 10], ["z", 11]]],
                [0, 2, [["a", 20]]],
                [0, 3, [["a", 30], ["z", 31]]],
                [0, 4, null]
            ])"));
    ASSERT_TRUE(success);
}

// Nested map values through both selected physical columns and overflow.
TEST_P(WriteAndReadInteTest, TestSharedShreddingWithStructValue) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto value_type = arrow::struct_({
        arrow::field("name", arrow::utf8()),
        arrow::field("score", arrow::int32()),
    });
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), value_type)),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, [["a", ["alice", 10]], ["z", ["zoe", 11]]]],
                [2, [["a", ["amy", null]]]],
                [3, null]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector expected_fields = fields;
    expected_fields.insert(expected_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(arrow::struct_(expected_fields), splits,
                                                    R"([
                [0, 1, [["a", ["alice", 10]], ["z", ["zoe", 11]]]],
                [0, 2, [["a", ["amy", null]]]],
                [0, 3, null]
            ])"));
    ASSERT_TRUE(success);
}

// Keep ORC lazy dictionary decoding enabled across a default -> shared-shredding schema change.
// The test inspects every user-visible batch directly, because ReadResultCollector would otherwise
// decode dictionary arrays and hide a type mismatch between old and new files.
TEST_P(WriteAndReadInteTest, TestOrcDictionaryLazyDecodingWithSharedShredding) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "orc") {
        return;
    }

    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::utf8())),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},       {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},       {"fields.tags.map.storage-layout", "default"},
        {"orc.read.enable-lazy-decoding", "true"}, {"orc.dictionary-key-size-threshold", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v0,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, [["a", "red"], ["z", "blue"]]],
                [2, [["a", "red"], ["z", "green"]]]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v0), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    std::map<std::string, std::string> options_v1 = options;
    options_v1["fields.tags.map.storage-layout"] = "shared-shredding";
    options_v1["fields.tags.map.shared-shredding.max-columns"] = "1";
    ASSERT_OK(WriteNextSchema({DataField(0, fields[0]), DataField(1, fields[1])},
                              /*highest_field_id=*/1, options_v1));

    helper.reset();
    std::string table_path = PathUtil::JoinPath(test_dir_, "foo.db/bar");
    ASSERT_OK_AND_ASSIGN(helper, TestHelper::Create(table_path, options_v1,
                                                    /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [3, [["a", "red"], ["z", "yellow"]]],
                [4, [["a", "red"]]]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v1), /*commit_identifier=*/1,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector expected_fields = fields;
    expected_fields.insert(expected_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(arrow::struct_(expected_fields), splits,
                                                    R"([
                [0, 1, [["a", "red"], ["z", "blue"]]],
                [0, 2, [["a", "red"], ["z", "green"]]],
                [0, 3, [["a", "red"], ["z", "yellow"]]],
                [0, 4, [["a", "red"]]]
            ])"));
    ASSERT_TRUE(success);
}

// Verify shared-shredding in the PK read path.
TEST_P(WriteAndReadInteTest, TestPkSharedShreddingMap) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    arrow::FieldVector fields = {
        arrow::field("pk", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{"pk"}, options,
                                            /*is_streaming_mode=*/true));

    ASSERT_OK_AND_ASSIGN(auto batch_0, TestHelper::MakeRecordBatch(
                                           arrow::struct_(fields),
                                           R"([[1, [["a", 10], ["z", 11]]], [2, [["b", 20]]]])",
                                           /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_0), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto batch_1, TestHelper::MakeRecordBatch(
                                           arrow::struct_(fields),
                                           R"([[1, [["a", 100], ["z", 101]]], [3, [["c", 30]]]])",
                                           /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_1), /*commit_identifier=*/1,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector expected_fields = fields;
    expected_fields.insert(expected_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(arrow::struct_(expected_fields), splits,
                                                    R"([
                [0, 1, [["a", 100], ["z", 101]]],
                [0, 2, [["b", 20]]],
                [0, 3, [["c", 30]]]
            ])"));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestSharedShreddingPartialKeyRecallWithOverflow) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "2"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));

    // max-columns=2, so keys are allocated in insertion order:
    // Row 1: {a:1, b:2, c:3, d:4} -> physical cols: a, b; overflow: c, d
    // Row 2: {a:10, b:20}          -> physical cols: a, b; no overflow
    // Row 3: null
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, [["a", 1], ["b", 2], ["c", 3], ["d", 4]]],
                [2, [["a", 10], ["b", 20]]],
                [3, null]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    // Sub-case 1: read only key "a" (in physical column)
    {
        auto selected_keys_meta =
            arrow::KeyValueMetadata::Make({"paimon.map.selected-keys"}, {"a"});
        auto read_schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type)->WithMetadata(selected_keys_meta),
        });
        auto expected_type = arrow::struct_({
            arrow::field("_VALUE_KIND", arrow::int8()),
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type),
        });
        ASSERT_OK_AND_ASSIGN(bool success,
                             ReadAndCheckWithReadSchema(options, read_schema, expected_type,
                                                        R"([
                    [0, 1, [["a", 1]]],
                    [0, 2, [["a", 10]]],
                    [0, 3, null]
                ])"));
        ASSERT_TRUE(success);
    }

    // Sub-case 2: read only key "c" (in overflow)
    {
        auto selected_keys_meta =
            arrow::KeyValueMetadata::Make({"paimon.map.selected-keys"}, {"c"});
        auto read_schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type)->WithMetadata(selected_keys_meta),
        });
        auto expected_type = arrow::struct_({
            arrow::field("_VALUE_KIND", arrow::int8()),
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type),
        });
        ASSERT_OK_AND_ASSIGN(bool success,
                             ReadAndCheckWithReadSchema(options, read_schema, expected_type,
                                                        R"([
                    [0, 1, [["c", 3]]],
                    [0, 2, []],
                    [0, 3, null]
                ])"));
        ASSERT_TRUE(success);
    }

    // Sub-case 3: read keys "c" and "a" (cross physical + overflow), order follows user request
    {
        auto selected_keys_meta =
            arrow::KeyValueMetadata::Make({"paimon.map.selected-keys"}, {"c,a"});
        auto read_schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type)->WithMetadata(selected_keys_meta),
        });
        auto expected_type = arrow::struct_({
            arrow::field("_VALUE_KIND", arrow::int8()),
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type),
        });
        ASSERT_OK_AND_ASSIGN(bool success,
                             ReadAndCheckWithReadSchema(options, read_schema, expected_type,
                                                        R"([
                    [0, 1, [["c", 3], ["a", 1]]],
                    [0, 2, [["a", 10]]],
                    [0, 3, null]
                ])"));
        ASSERT_TRUE(success);
    }
}

TEST_P(WriteAndReadInteTest, TestSharedShreddingPartialKeyRecallWithNullOrMissingKey) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "3"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));

    // Row 1: tags=null
    // Row 2: tags={b:2} (no key "a")
    // Row 3: tags={a:30, b:40}
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, null],
                [2, [["b", 2]]],
                [3, [["a", 30], ["b", 40]]]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    // Sub-case 1: row with null tags, request key "a" -> tags should be null
    // Sub-case 2: row without key "a", request key "a" -> key "a" maps to null value
    // Sub-case 3: row with key "a", request key "a" -> normal value
    {
        auto selected_keys_meta =
            arrow::KeyValueMetadata::Make({"paimon.map.selected-keys"}, {"a"});
        auto read_schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type)->WithMetadata(selected_keys_meta),
        });
        auto expected_type = arrow::struct_({
            arrow::field("_VALUE_KIND", arrow::int8()),
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type),
        });
        ASSERT_OK_AND_ASSIGN(bool success,
                             ReadAndCheckWithReadSchema(options, read_schema, expected_type,
                                                        R"([
                    [0, 1, null],
                    [0, 2, []],
                    [0, 3, [["a", 30]]]
                ])"));
        ASSERT_TRUE(success);
    }

    // Sub-case 4: request a key that was never written ("nonexistent") -> all rows have null value
    {
        auto selected_keys_meta =
            arrow::KeyValueMetadata::Make({"paimon.map.selected-keys"}, {"nonexistent"});
        auto read_schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type)->WithMetadata(selected_keys_meta),
        });
        auto expected_type = arrow::struct_({
            arrow::field("_VALUE_KIND", arrow::int8()),
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type),
        });
        ASSERT_OK_AND_ASSIGN(bool success,
                             ReadAndCheckWithReadSchema(options, read_schema, expected_type,
                                                        R"([
                    [0, 1, null],
                    [0, 2, []],
                    [0, 3, []]
                ])"));
        ASSERT_TRUE(success);
    }
}

TEST_P(WriteAndReadInteTest, TestSharedShreddingPartialKeyRecallMultipleColumns) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
        arrow::field("metrics", map_type),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "3"},
        {"fields.metrics.map.storage-layout", "shared-shredding"},
        {"fields.metrics.map.shared-shredding.max-columns", "3"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));

    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, [["a", 1], ["b", 2]], [["x", 100], ["y", 200]]],
                [2, [["a", 10], ["c", 30]], [["x", 1000], ["z", 3000]]]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    // Sub-case 1: partial key recall on both columns independently
    {
        auto tags_meta = arrow::KeyValueMetadata::Make({"paimon.map.selected-keys"}, {"a"});
        auto metrics_meta = arrow::KeyValueMetadata::Make({"paimon.map.selected-keys"}, {"x"});
        auto read_schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type)->WithMetadata(tags_meta),
            arrow::field("metrics", map_type)->WithMetadata(metrics_meta),
        });
        auto expected_type = arrow::struct_({
            arrow::field("_VALUE_KIND", arrow::int8()),
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type),
            arrow::field("metrics", map_type),
        });
        ASSERT_OK_AND_ASSIGN(bool success,
                             ReadAndCheckWithReadSchema(options, read_schema, expected_type,
                                                        R"([
                    [0, 1, [["a", 1]], [["x", 100]]],
                    [0, 2, [["a", 10]], [["x", 1000]]]
                ])"));
        ASSERT_TRUE(success);
    }

    // Sub-case 2: partial key recall on tags + full recall on metrics
    {
        auto tags_meta = arrow::KeyValueMetadata::Make({"paimon.map.selected-keys"}, {"a,b"});
        auto read_schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type)->WithMetadata(tags_meta),
            arrow::field("metrics", map_type),
        });
        auto expected_type = arrow::struct_({
            arrow::field("_VALUE_KIND", arrow::int8()),
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type),
            arrow::field("metrics", map_type),
        });
        ASSERT_OK_AND_ASSIGN(bool success,
                             ReadAndCheckWithReadSchema(options, read_schema, expected_type,
                                                        R"([
                    [0, 1, [["a", 1], ["b", 2]], [["x", 100], ["y", 200]]],
                    [0, 2, [["a", 10]], [["x", 1000], ["z", 3000]]]
                ])"));
        ASSERT_TRUE(success);
    }
}

TEST_P(WriteAndReadInteTest, TestMapStorageLayoutDefaultToSharedShreddingPartialKeyRecall) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    };
    std::map<std::string, std::string> options_v0 = {
        {Options::MANIFEST_FORMAT, "avro"},  {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system}, {"fields.tags.map.storage-layout", "default"},
    };
    if (file_system == "jindo") {
        options_v0 = AddOptionsForJindo(options_v0);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options_v0,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v0,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, [["a", 10], ["b", 20]]],
                [2, null]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v0), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    std::map<std::string, std::string> options_v1 = options_v0;
    options_v1["fields.tags.map.storage-layout"] = "shared-shredding";
    options_v1["fields.tags.map.shared-shredding.max-columns"] = "1";
    ASSERT_OK(WriteNextSchema({DataField(0, fields[0]), DataField(1, fields[1])},
                              /*highest_field_id=*/1, options_v1));
    helper.reset();
    ASSERT_OK_AND_ASSIGN(helper,
                         TestHelper::Create(PathUtil::JoinPath(test_dir_, "foo.db/bar"), options_v1,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [3, [["a", 30], ["z", 31]]],
                [4, [["z", 41]]]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v1), /*commit_identifier=*/1,
                                     /*expected_commit_messages=*/std::nullopt));

    auto selected_keys_meta = arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"a"});
    auto read_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type)->WithMetadata(selected_keys_meta),
    });
    auto expected_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    });
    ASSERT_OK_AND_ASSIGN(bool success,
                         ReadAndCheckWithReadSchema(options_v1, read_schema, expected_type,
                                                    R"([
                [0, 1, [["a", 10]]],
                [0, 2, null],
                [0, 3, [["a", 30]]],
                [0, 4, []]
            ])"));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestMapStorageLayoutSharedShreddingToDefaultPartialKeyRecall) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    };
    std::map<std::string, std::string> options_v0 = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "1"},
    };
    if (file_system == "jindo") {
        options_v0 = AddOptionsForJindo(options_v0);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options_v0,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v0,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, [["a", 10], ["z", 11]]],
                [2, [["z", 21]]]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v0), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    std::map<std::string, std::string> options_v1 = options_v0;
    options_v1["fields.tags.map.storage-layout"] = "default";
    options_v1.erase("fields.tags.map.shared-shredding.max-columns");
    ASSERT_OK(WriteNextSchema({DataField(0, fields[0]), DataField(1, fields[1])},
                              /*highest_field_id=*/1, options_v1));
    helper.reset();
    ASSERT_OK_AND_ASSIGN(helper,
                         TestHelper::Create(PathUtil::JoinPath(test_dir_, "foo.db/bar"), options_v1,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [3, [["a", 30], ["z", 31]]],
                [4, null]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v1), /*commit_identifier=*/1,
                                     /*expected_commit_messages=*/std::nullopt));

    auto selected_keys_meta = arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"a"});
    auto read_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type)->WithMetadata(selected_keys_meta),
    });
    auto expected_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    });
    ASSERT_OK_AND_ASSIGN(bool success,
                         ReadAndCheckWithReadSchema(options_v1, read_schema, expected_type,
                                                    R"([
                [0, 1, [["a", 10]]],
                [0, 2, []],
                [0, 3, [["a", 30]]],
                [0, 4, null]
            ])"));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestSharedShreddingDuplicateSelectedKeys) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch, TestHelper::MakeRecordBatch(
                                         arrow::struct_(fields), R"([[1, [["a", 10], ["b", 20]]]])",
                                         /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    auto selected_keys_meta =
        arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"a,a"});
    auto read_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type)->WithMetadata(selected_keys_meta),
    });
    auto expected_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    });
    ASSERT_NOK_WITH_MSG(ReadAndCheckWithReadSchema(options, read_schema, expected_type, "[]"),
                        "Duplicate selected key 'a'");
}

TEST_P(WriteAndReadInteTest, TestSharedShreddingAllNullMapColumn) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, null],
                [2, null],
                [3, null]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector expected_fields = fields;
    expected_fields.insert(expected_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(arrow::struct_(expected_fields), splits,
                                                    R"([
                [0, 1, null],
                [0, 2, null],
                [0, 3, null]
            ])"));
    ASSERT_TRUE(success);
}

INSTANTIATE_TEST_SUITE_P(FileFormatAndFileSystem, WriteAndReadInteTest,
                         ::testing::ValuesIn(GetTestValuesForWriteAndReadInteTest()));

}  // namespace paimon::test
