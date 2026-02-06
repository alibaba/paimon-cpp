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

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/table/source/startup_mode.h"
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

std::vector<std::pair<std::string, std::string>> GetTestValuesForWriteAndReadInteTest() {
    std::vector<std::pair<std::string, std::string>> values = {{"parquet", "local"}};
#ifdef PAIMON_ENABLE_ORC
    values.emplace_back("orc", "local");
    // values.emplace_back("parquet", "jindo");
#endif
#ifdef PAIMON_ENABLE_LANCE
    values.emplace_back("lance", "local");
#endif
#ifdef PAIMON_ENABLE_AVRO
    values.emplace_back("avro", "local");
#endif
    return values;
}

INSTANTIATE_TEST_SUITE_P(FileFormatAndFileSystem, WriteAndReadInteTest,
                         ::testing::ValuesIn(GetTestValuesForWriteAndReadInteTest()));

}  // namespace paimon::test
