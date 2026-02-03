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
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"

namespace paimon {
class DataSplit;
}  // namespace paimon

namespace paimon::test {
class ScanAndReadInteTest : public testing::Test,
                            public ::testing::WithParamInterface<std::pair<std::string, bool>> {
 public:
    void CheckStreamScanResult(
        const std::unique_ptr<TableScan>& table_scan, const std::unique_ptr<TableRead>& table_read,
        const std::vector<std::optional<int64_t>>& expected_snapshot_ids,
        const std::vector<std::shared_ptr<arrow::ChunkedArray>>& expected_array) const {
        size_t scan_id = 0;
        while (true) {
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> result_plan, table_scan->CreatePlan());
            if (scan_id == expected_snapshot_ids.size()) {
                // no snapshot
                ASSERT_EQ(std::nullopt, result_plan->SnapshotId());
                ASSERT_TRUE(result_plan->Splits().empty());
                break;
            }
            ASSERT_EQ(result_plan->SnapshotId(), expected_snapshot_ids[scan_id]);
            auto splits = result_plan->Splits();
            ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(splits));
            ASSERT_OK_AND_ASSIGN(auto read_result,
                                 ReadResultCollector::CollectResult(batch_reader.get()));
            if (expected_array[scan_id]) {
                ASSERT_TRUE(read_result);
                ASSERT_TRUE(expected_array[scan_id]->type()->Equals(read_result->type()));
                ASSERT_TRUE(expected_array[scan_id]->Equals(read_result))
                    << read_result->ToString() << std::endl
                    << "expected" << expected_array[scan_id]->ToString();
            } else {
                ASSERT_FALSE(read_result);
            }
            scan_id++;
        }
    }

    void PrintPlan(const std::shared_ptr<Plan>& plan) const {
        std::string snapshot_str =
            plan->SnapshotId() ? std::to_string(plan->SnapshotId().value()) : "null";
        std::cout << "snapshot id=" << snapshot_str << std::endl;
        const auto& splits = plan->Splits();
        for (const auto& split : splits) {
            auto split_impl = std::dynamic_pointer_cast<DataSplitImpl>(split);
            std::cout << split_impl->ToString() << std::endl;
        }
    }

    void CheckPostponeFile(const std::string& root_path,
                           const std::vector<std::string>& subdirs) const {
        std::vector<std::unique_ptr<BasicFileStatus>> status_list;
        auto file_system = std::make_shared<LocalFileSystem>();
        for (const auto& dir : subdirs) {
            ASSERT_OK(file_system->ListDir(PathUtil::JoinPath(root_path, dir), &status_list));
        }
        ASSERT_FALSE(status_list.empty());
        for (const auto& file_status : status_list) {
            std::string path = file_status->GetPath();
            ASSERT_TRUE(path.find("-u-") != std::string::npos);
            ASSERT_TRUE(path.find("-s-") != std::string::npos);
            ASSERT_TRUE(path.find("-w-") != std::string::npos);
            // writer id
            ASSERT_TRUE(path.find("12345") != std::string::npos);
        }
    }

    void AddReadOptionsForPrefetch(ReadContextBuilder* read_context_builder) {
        auto [file_format, enable_prefetch] = GetParam();
        read_context_builder->AddOption("test.enable-adaptive-prefetch-strategy", "false");
        if (enable_prefetch) {
            read_context_builder->EnablePrefetch(true).SetPrefetchBatchCount(3);
        }
    }

    void AdjustSplitWithExternalPath(const std::string& src_path, const std::string& target_path,
                                     bool adjust_index,
                                     std::vector<std::shared_ptr<Split>>* splits_ptr) {
        // adjust external path from src_path to target_path
        auto& splits = *splits_ptr;
        for (auto& split : splits) {
            auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
            ASSERT_TRUE(data_split);
            auto& data_files = data_split->data_files_;
            for (auto& file : data_files) {
                auto& external_path = file->external_path;
                if (external_path) {
                    external_path =
                        StringUtils::Replace(external_path.value(), src_path, target_path);
                }
            }
            if (adjust_index) {
                auto& deletion_files = data_split->data_deletion_files_;
                for (auto& file : deletion_files) {
                    if (file) {
                        file.value().path =
                            StringUtils::Replace(file.value().path, src_path, target_path);
                    }
                }
            }
        }
    }

 private:
    std::shared_ptr<arrow::StructType> arrow_data_type_ =
        std::dynamic_pointer_cast<arrow::StructType>(DataField::ConvertDataFieldsToArrowStructType(
            {SpecialFields::ValueKind(), DataField(0, arrow::field("f0", arrow::utf8())),
             DataField(1, arrow::field("f1", arrow::int32())),
             DataField(2, arrow::field("f2", arrow::int32())),
             DataField(3, arrow::field("f3", arrow::float64()))}));
};

TEST_P(ScanAndReadInteTest, TestWithAppendSnapshotIOException) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = GetDataDir() + "/" + file_format + "/append_09.db/append_09";

    bool run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 500; i++) {
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        // scan
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "1");
        Result<std::unique_ptr<ScanContext>> scan_context = scan_context_builder.Finish();
        CHECK_HOOK_STATUS(scan_context.status(), i);
        Result<std::unique_ptr<TableScan>> table_scan =
            TableScan::Create(std::move(scan_context).value());
        CHECK_HOOK_STATUS(table_scan.status(), i);
        Result<std::shared_ptr<Plan>> result_plan = table_scan.value()->CreatePlan();
        CHECK_HOOK_STATUS(result_plan.status(), i);
        ASSERT_EQ(result_plan.value()->SnapshotId().value(), 1);

        auto splits = result_plan.value()->Splits();
        ASSERT_EQ(3, splits.size());
        // read
        ReadContextBuilder read_context_builder(table_path);
        AddReadOptionsForPrefetch(&read_context_builder);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                             read_context_builder.Finish());

        Result<std::unique_ptr<TableRead>> table_read = TableRead::Create(std::move(read_context));
        CHECK_HOOK_STATUS(table_read.status(), i);
        Result<std::unique_ptr<BatchReader>> batch_reader =
            table_read.value()->CreateReader(splits);
        CHECK_HOOK_STATUS(batch_reader.status(), i);
        auto read_result = ReadResultCollector::CollectResult(batch_reader.value().get());
        CHECK_HOOK_STATUS(read_result.status(), i);

        // check result
        auto expected = std::make_shared<arrow::ChunkedArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 0, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Lucy", 20, 1, 14.1]
   ])")
                .ValueOrDie());
        ASSERT_TRUE(expected);
        ASSERT_TRUE(expected->Equals(read_result.value())) << read_result.value()->ToString();
        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

TEST_P(ScanAndReadInteTest, TestWithPkSnapshotIOException) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = GetDataDir() + "/" + file_format +
                             "/pk_table_scan_and_read_dv.db/pk_table_scan_and_read_dv/";

    bool run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 800; i++) {
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        // scan
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "6");
        Result<std::unique_ptr<ScanContext>> scan_context = scan_context_builder.Finish();
        CHECK_HOOK_STATUS(scan_context.status(), i);
        Result<std::unique_ptr<TableScan>> table_scan =
            TableScan::Create(std::move(scan_context).value());
        CHECK_HOOK_STATUS(table_scan.status(), i);
        Result<std::shared_ptr<Plan>> result_plan = table_scan.value()->CreatePlan();
        CHECK_HOOK_STATUS(result_plan.status(), i);
        ASSERT_EQ(result_plan.value()->SnapshotId().value(), 6);
        auto splits = result_plan.value()->Splits();
        ASSERT_EQ(3, splits.size());
        // read
        ReadContextBuilder read_context_builder(table_path);
        AddReadOptionsForPrefetch(&read_context_builder);

        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                             read_context_builder.Finish());
        Result<std::unique_ptr<TableRead>> table_read = TableRead::Create(std::move(read_context));
        CHECK_HOOK_STATUS(table_read.status(), i);
        Result<std::unique_ptr<BatchReader>> batch_reader =
            table_read.value()->CreateReader(splits);
        CHECK_HOOK_STATUS(batch_reader.status(), i);
        auto read_result = ReadResultCollector::CollectResult(batch_reader.value().get());
        CHECK_HOOK_STATUS(read_result.status(), i);

        // check result
        auto expected = std::make_shared<arrow::ChunkedArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Alice", 10, 1, 19.1],
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, 18.1]
   ])")
                .ValueOrDie());
        ASSERT_TRUE(expected);
        ASSERT_TRUE(expected->Equals(read_result.value())) << read_result.value()->ToString();
        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

TEST_P(ScanAndReadInteTest, TestWithAppendSnapshot1) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = GetDataDir() + "/" + file_format + "/append_09.db/append_09";

    // scan
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "1");
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 1);

    auto splits = result_plan->Splits();
    ASSERT_EQ(3, splits.size());
    // read
    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 0, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Lucy", 20, 1, 14.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestWithAppendSnapshot3) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = GetDataDir() + "/" + file_format + "/append_09.db/append_09";

    // scan
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "3");
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 3);

    auto splits = result_plan->Splits();
    ASSERT_EQ(3, splits.size());

    // read
    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 0, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Emily", 10, 0, 15.1],
[0, "Bob", 10, 0, 12.1],
[0, "Alex", 10, 0, 16.1],
[0, "David", 10, 0, 17.1],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, null]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestWithAppendSnapshot5) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = GetDataDir() + "/" + file_format + "/append_09.db/append_09";

    // scan
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "5");
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 5);

    auto splits = result_plan->Splits();
    ASSERT_EQ(3, splits.size());

    // read
    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 0, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Emily", 10, 0, 15.1],
[0, "Bob", 10, 0, 12.1],
[0, "Alex", 10, 0, 16.1],
[0, "David", 10, 0, 17.1],
[0, "Lily", 10, 0, 17.1],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, null]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestWithAppendSnapshotWithStreamWithDefaultMode) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = GetDataDir() + "/" + file_format + "/append_09.db/append_09";

    // scan
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "1").WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::optional<int64_t>> expected_snapshot_ids = {std::nullopt, 1, 2, 3, 4};
    auto expected_snapshot1 = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
            [0, "Alice", 10, 1, 11.1],
            [0, "Bob", 10, 0, 12.1],
            [0, "Emily", 10, 0, 13.1],
            [0, "Tony", 10, 0, 14.1],
            [0, "Lucy", 20, 1, 14.1]
    ])")
            .ValueOrDie());
    auto expected_snapshot2 = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
            [0, "Emily", 10, 0, 15.1],
            [0, "Bob", 10, 0, 12.1],
            [0, "Alex", 10, 0, 16.1],
            [0, "Paul", 20, 1, null]
        ])")
            .ValueOrDie());
    auto expected_snapshot3 = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
             [0, "David", 10, 0, 17.1]
        ])")
            .ValueOrDie());
    auto expected_snapshot4 = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
             [0, "Lily", 10, 0, 17.1]
        ])")
            .ValueOrDie());

    std::vector<std::shared_ptr<arrow::ChunkedArray>> expected_arrays = {
        nullptr, expected_snapshot1, expected_snapshot2, expected_snapshot3, expected_snapshot4};
    CheckStreamScanResult(table_scan, table_read, expected_snapshot_ids, expected_arrays);
}

TEST_P(ScanAndReadInteTest, TestJavaPaimon1WithAppendSnapshot1) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = GetDataDir() + "/" + file_format + "/append_10.db/append_10";

    // scan
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "1");
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 1);

    auto splits = result_plan->Splits();
    ASSERT_EQ(3, splits.size());

    // read
    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
        [0, "Alice", 10, 1, 11.1],
        [0, "Bob", 10, 0, 12.1],
        [0, "Emily", 10, 0, 13.1],
        [0, "Tony", 10, 0, 14.1],
        [0, "Lucy", 20, 1, 14.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestJavaPaimon1WithAppendSnapshotOfNestedType) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = GetDataDir() + "/" + file_format +
                             "/append_complex_build_in_fieldid.db/"
                             "append_complex_build_in_fieldid/";
    // scan
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "1");
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 1);

    auto splits = result_plan->Splits();
    ASSERT_EQ(1, splits.size());

    // read
    auto map_type = arrow::map(arrow::int8(), arrow::int16());
    auto list_type = arrow::list(DataField::ConvertDataFieldToArrowField(
        DataField(536871936, arrow::field("item", arrow::float32()))));
    std::vector<DataField> struct_fields = {DataField(3, arrow::field("f0", arrow::boolean())),
                                            DataField(4, arrow::field("f1", arrow::int64()))};
    auto struct_type = DataField::ConvertDataFieldsToArrowStructType(struct_fields);
    std::vector<DataField> read_fields = {
        SpecialFields::ValueKind(),
        DataField(0, arrow::field("f1", map_type)),
        DataField(1, arrow::field("f2", list_type)),
        DataField(2, arrow::field("f3", struct_type)),
        DataField(5, arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO))),
        DataField(6, arrow::field("f5", arrow::date32())),
        DataField(7, arrow::field("f6", arrow::decimal128(2, 2)))};
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, [[0, 0]], [0.1, 0.2], [true, 2], "1970-01-01 00:02:03.123123", 2456, "0.22"],
[0, [[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], "1970-01-01 00:02:03.123123", 245, "0.12"],
[0, [[1, 64], [2, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.0", 24, null]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(expected_array->Equals(read_result)) << read_result->ToString();
}

// test pk with dv
TEST_P(ScanAndReadInteTest, TestWithPKWithDvBatchScanSnapshot6) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = GetDataDir() + "/" + file_format +
                             "/pk_table_scan_and_read_dv.db/pk_table_scan_and_read_dv/";

    // normal batch scan case for pk+dv, all data in level 0 is filtered out
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "6");
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 6);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Alice", 10, 1, 19.1],
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, 18.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestWithPKWithDvBatchScanSnapshot1) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = GetDataDir() + "/" + file_format +
                             "/pk_table_scan_and_read_dv.db/pk_table_scan_and_read_dv/";
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "1");
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 1);
    // snapshot 1 is an append snapshot without compact, data is all in level 0, therefore batch
    // scan return empty plan
    ASSERT_TRUE(result_plan->Splits().empty());
}

TEST_P(ScanAndReadInteTest, TestWithPKWithDvBatchScanSnapshot6WithPartitionAndBucketFilter) {
    auto [file_format, enable_prefetch] = GetParam();
    // all data in level 0 & not in partition 10, bucket 1 is filtered out
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_dv.db/pk_table_scan_and_read_dv/";

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "6");
    scan_context_builder.SetBucketFilter(1).SetPartitionFilter({{{"f1", "10"}}});
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 6);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestWithPKWithDvBatchScanSnapshot6WithPredicate) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_dv.db/pk_table_scan_and_read_dv/";
    // predicate: f0 != "Alice" (key predicate) and f3 > 18 (value predicate) and all data in level
    // 0
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "6");

    std::string literal_str = "Alice";
    auto not_equal = PredicateBuilder::NotEqual(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
        Literal(FieldType::STRING, literal_str.data(), literal_str.size()));
    auto greater_than = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f3",
                                                      FieldType::DOUBLE, Literal(18.0));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({not_equal, greater_than}));
    scan_context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    read_context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 6);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, 18.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestWithPKWithDvBatchScanSnapshot4WithPredicate) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_dv.db/pk_table_scan_and_read_dv/";

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "4");
    scan_context_builder.SetBucketFilter(0).SetPartitionFilter({{{"f1", "10"}}});
    // predicate f3 > 20.0 will be applied, as in kv mode value filter is enabled
    auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f3",
                                                   FieldType::DOUBLE, Literal(20.0));
    scan_context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 4);
    ASSERT_TRUE(result_plan->Splits().empty());
}

TEST_P(ScanAndReadInteTest, TestWithPKWithDvBatchScanSnapshot6WithLimit) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_dv.db/pk_table_scan_and_read_dv/";

    // normal batch scan case for pk+dv, all data in level 0 is filtered out, set row limits to 6,
    // data in partition 20 is truncated
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.SetLimit(6);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 6);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Alice", 10, 1, 19.1],
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestWithPKWithDvStreamFromSnapshot4) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_dv.db/pk_table_scan_and_read_dv/";

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "4")
        .AddOption(Options::SCAN_MODE, "from-snapshot-full")
        .WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    // from an compact snapshot
    std::vector<std::optional<int64_t>> expected_snapshot_ids = {4, 5, 6};
    auto expected_snapshot4_batch = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Alice", 10, 1, 19.1],
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, 18.1]
    ])")
            .ValueOrDie());

    auto expected_snapshot5_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[2, "Alex", 10, 0, 21.2],
[0, "Marco", 10, 0, 21.1],
[0, "Skye", 10, 0, 21.0]
    ])")
            .ValueOrDie());

    auto expected_snapshot6_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[3, "Alex", 10, 0, 31.2],
[0, "Marco2", 10, 0, 31.1],
[0, "Skye2", 10, 0, 31.0]
        ])")
            .ValueOrDie());
    std::vector<std::shared_ptr<arrow::ChunkedArray>> expected_arrays = {
        expected_snapshot4_batch, expected_snapshot5_stream, expected_snapshot6_stream};
    CheckStreamScanResult(table_scan, table_read, expected_snapshot_ids, expected_arrays);
}

TEST_P(ScanAndReadInteTest, TestWithPKWithDvStreamFromSnapshot5) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_dv.db/pk_table_scan_and_read_dv/";

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "5")
        .AddOption(Options::SCAN_MODE, "from-snapshot-full")
        .WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    // from an append snapshot, first plan is snapshot5 with merge, second plan is snapshot6
    // (isStreaming=true)
    std::vector<std::optional<int64_t>> expected_snapshot_ids = {5, 6};
    auto expected_snapshot5_batch = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Alice", 10, 1, 19.1],
[0, "Alex", 10, 0, 21.2],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Marco", 10, 0, 21.1],
[0, "Skye", 10, 0, 21.0],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, 18.1]
    ])")
            .ValueOrDie());
    auto expected_snapshot6_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[3, "Alex", 10, 0, 31.2],
[0, "Marco2", 10, 0, 31.1],
[0, "Skye2", 10, 0, 31.0]
        ])")
            .ValueOrDie());
    std::vector<std::shared_ptr<arrow::ChunkedArray>> expected_arrays = {expected_snapshot5_batch,
                                                                         expected_snapshot6_stream};
    CheckStreamScanResult(table_scan, table_read, expected_snapshot_ids, expected_arrays);
}

TEST_P(ScanAndReadInteTest, TestWithPKWithDvStreamFromSnapshot6) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_dv.db/pk_table_scan_and_read_dv/";

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_MODE, "latest-full").WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    // from an append snapshot, first plan is snapshot6 with merge, level 0 data in base manifest
    // list is contained
    std::vector<std::optional<int64_t>> expected_snapshot_ids = {6};
    auto expected_snapshot6_batch = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Alice", 10, 1, 19.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Marco", 10, 0, 21.1],
[0, "Marco2", 10, 0, 31.1],
[0, "Skye", 10, 0, 21.0],
[0, "Skye2", 10, 0, 31.0],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, 18.1]
    ])")
            .ValueOrDie());
    std::vector<std::shared_ptr<arrow::ChunkedArray>> expected_arrays = {expected_snapshot6_batch};
    CheckStreamScanResult(table_scan, table_read, expected_snapshot_ids, expected_arrays);
}

TEST_P(ScanAndReadInteTest, TestWithPKWithDvStreamFromSnapshot1) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_dv.db/pk_table_scan_and_read_dv/";

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "1")
        .AddOption(Options::SCAN_MODE, "from-snapshot-full")
        .WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    // from the first snapshot
    std::vector<std::optional<int64_t>> expected_snapshot_ids = {1, 3, 5, 6};
    auto expected_snapshot1_batch = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alice", 10, 1, 11.1],
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, 18.1]
    ])")
            .ValueOrDie());

    auto expected_snapshot3_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[2, "Alice", 10, 1, 19.1],
[3, "Tony", 10, 0, 14.1]
    ])")
            .ValueOrDie());
    auto expected_snapshot5_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[2, "Alex", 10, 0, 21.2],
[0, "Marco", 10, 0, 21.1],
[0, "Skye", 10, 0, 21.0]
        ])")
            .ValueOrDie());
    auto expected_snapshot6_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[3, "Alex", 10, 0, 31.2],
[0, "Marco2", 10, 0, 31.1],
[0, "Skye2", 10, 0, 31.0]
        ])")
            .ValueOrDie());
    std::vector<std::shared_ptr<arrow::ChunkedArray>> expected_arrays = {
        expected_snapshot1_batch, expected_snapshot3_stream, expected_snapshot5_stream,
        expected_snapshot6_stream};
    CheckStreamScanResult(table_scan, table_read, expected_snapshot_ids, expected_arrays);
}

TEST_P(ScanAndReadInteTest, TestWithPKWithDvStreamFromSnapshot2) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_dv.db/pk_table_scan_and_read_dv/";

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "2")
        .AddOption(Options::SCAN_MODE, "from-snapshot")
        .WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    // use from-snapshot mode, first plan is empty, second plan is snapshot 3 with delta files
    // (snapshot 2 is compact snapshot, so skipped)
    std::vector<std::optional<int64_t>> expected_snapshot_ids = {std::nullopt, 3, 5, 6};
    auto expected_snapshot3_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[2, "Alice", 10, 1, 19.1],
[3, "Tony", 10, 0, 14.1]
    ])")
            .ValueOrDie());
    auto expected_snapshot5_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[2, "Alex", 10, 0, 21.2],
[0, "Marco", 10, 0, 21.1],
[0, "Skye", 10, 0, 21.0]
        ])")
            .ValueOrDie());
    auto expected_snapshot6_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[3, "Alex", 10, 0, 31.2],
[0, "Marco2", 10, 0, 31.1],
[0, "Skye2", 10, 0, 31.0]
        ])")
            .ValueOrDie());
    std::vector<std::shared_ptr<arrow::ChunkedArray>> expected_arrays = {
        nullptr, expected_snapshot3_stream, expected_snapshot5_stream, expected_snapshot6_stream};
    CheckStreamScanResult(table_scan, table_read, expected_snapshot_ids, expected_arrays);
}

TEST_P(ScanAndReadInteTest, TestWithPKWithNestedType) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path =
        paimon::test::GetDataDir() + file_format + "/pk_table_nested_type.db/pk_table_nested_type/";

    ScanContextBuilder scan_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(
        auto read_context,
        read_context_builder.SetReadSchema({"shopId", "dt", "hr", "col0", "col1", "col2"})
            .Finish());

    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 2);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    auto struct_inner_type =
        arrow::struct_({arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
                        arrow::field("f2", arrow::list(arrow::int32()))});

    auto map_type = arrow::map(arrow::int32(), arrow::int32());

    auto data_type =
        arrow::struct_({arrow::field("_VALUE_KIND", arrow::int8()),
                        arrow::field("shopId", arrow::int32()), arrow::field("dt", arrow::utf8()),
                        arrow::field("hr", arrow::int32()), arrow::field("col0", arrow::int32()),
                        arrow::field("col1", struct_inner_type), arrow::field("col2", map_type)});

    auto array1 = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
        [0, 1005, "2025-04-14", 10, 50, ["str-5", 500, [5, null, 6]], [[5, 5], [105, 105]]],
        [0, 1006, "2025-04-14", 10, 60, ["str-6", 600, [6, null, 7]], [[6, 6], [106, 106]]]
    ])")
                      .ValueOrDie();

    auto array2 = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
        [0, 1001, "2025-04-14", 10, 10, ["str-1", 100, [1, null, 2]], [[1, 1], [101, 101]]],
        [0, 1004, "2025-04-14", 10, 40, ["str-4", 400, [4, null, 5]], [[4, 4], [104, 104]]],
        [0, 1007, "2025-04-14", 10, 70, ["str-7", 700, [7, null, 8]], [[7, 7], [107, 107]]]
    ])")
                      .ValueOrDie();
    auto expected_array = arrow::ChunkedArray::Make({array1, array2}).ValueOrDie();
    ASSERT_TRUE(expected_array->Equals(read_result)) << read_result->ToString();
}

// test pk with mor
TEST_P(ScanAndReadInteTest, TestWithPKWithMorBatchScanLatestSnapshot) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_mor.db/pk_table_scan_and_read_mor/";

    // normal batch scan case for pk+mor, use latest snapshot if not specified
    ScanContextBuilder scan_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 5);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Marco", 10, 0, 21.1],
[0, "Marco2", 10, 0, 31.1],
[0, "Skye", 10, 0, 21.0],
[0, "Skye2", 10, 0, 31.0],
[0, "Alice", 10, 1, 19.1],
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, 18.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestWithPKWithMorBatchScanSnapshot2) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_mor.db/pk_table_scan_and_read_mor/";

    // normal batch scan case for pk+mor, read snapshot 2 (append snapshot), all data is in level 0
    // with merge read
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "2");
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 2);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alice", 10, 1, 19.1],
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, 18.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestWithPKWithMorBatchScanSnapshot5WithPartitionAndBucketFilter) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_mor.db/pk_table_scan_and_read_mor/";

    // all data not in partition 10, bucket 1 is filtered out
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "5");
    scan_context_builder.SetBucketFilter(1).SetPartitionFilter({{{"f1", "10"}}});
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 5);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Marco", 10, 0, 21.1],
[0, "Marco2", 10, 0, 31.1],
[0, "Skye", 10, 0, 21.0],
[0, "Skye2", 10, 0, 31.0]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestWithPKWithMorBatchScanSnapshot5WithPredicate) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_mor.db/pk_table_scan_and_read_mor/";

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "5");

    // predicate: f0 != "Alice" and f0 < "Lucy" and f3 <= 30.0
    // as f3 <= 30.0 is a value filter, it does not take effect
    // f0 < "Lucy" will skip the data file which delete "Tony" in scan.
    // Therefore, the false positives returned by the pk table for predicate pushdown may be
    // incorrect. (e.g., Tony is deleted but returned as a insert record)
    std::string literal_str = "Alice";
    auto not_equal = PredicateBuilder::NotEqual(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
        Literal(FieldType::STRING, literal_str.data(), literal_str.size()));
    std::string literal_str2 = "Lucy";
    auto less_than = PredicateBuilder::LessThan(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
        Literal(FieldType::STRING, literal_str2.data(), literal_str2.size()));
    auto less_or_equal = PredicateBuilder::LessOrEqual(/*field_index=*/3, /*field_name=*/"f3",
                                                       FieldType::DOUBLE, Literal(30.0));
    ASSERT_OK_AND_ASSIGN(auto predicate,
                         PredicateBuilder::And({not_equal, less_than, less_or_equal}));
    scan_context_builder.SetPredicate(predicate);

    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    read_context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 5);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Marco", 10, 0, 21.1],
[0, "Marco2", 10, 0, 31.1],
[0, "Skye", 10, 0, 21.0],
[0, "Skye2", 10, 0, 31.0],
[0, "Tony", 10, 0, 14.1],
[0, "Alice", 10, 1, 19.1],
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestWithPKWithMorBatchScanSnapshot3WithPredicate) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_mor.db/pk_table_scan_and_read_mor/";

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "3");
    scan_context_builder.SetBucketFilter(1).SetPartitionFilter({{{"f1", "10"}}});
    // predicate f3 > 20.0 will be applied as all data files in bucket 1 in compact snapshot 3 are
    // all filtered
    auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f3",
                                                   FieldType::DOUBLE, Literal(20.0));
    scan_context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 3);
    ASSERT_TRUE(result_plan->Splits().empty());
}

TEST_P(ScanAndReadInteTest, TestWithPKWithDvWithInvalidAggregateBatchScanSnapshot3) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_dv.db/pk_table_scan_and_read_dv/";

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "3")
        .AddOption(Options::MERGE_ENGINE, "aggregation")
        .AddOption("fields.f3.aggregate-function", "rbm32");
    scan_context_builder.SetBucketFilter(1).SetPartitionFilter({{{"f1", "10"}}});
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    read_context_builder.AddOption(Options::MERGE_ENGINE, "aggregation")
        .AddOption("fields.f3.aggregate-function", "rbm32");
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 3);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestWithPKWithMorWithInvalidAggregateBatchScanSnapshot3) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_mor.db/pk_table_scan_and_read_mor/";

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "3")
        .AddOption(Options::MERGE_ENGINE, "aggregation")
        .AddOption("fields.f3.aggregate-function", "rbm32");
    scan_context_builder.SetBucketFilter(1).SetPartitionFilter({{{"f1", "10"}}});
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    read_context_builder.AddOption(Options::MERGE_ENGINE, "aggregation")
        .AddOption("fields.f3.aggregate-function", "rbm32");
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 3);
    ASSERT_NOK_WITH_MSG(
        table_read->CreateReader(result_plan->Splits()),
        "Use unsupported aggregation rbm32 or spell aggregate function incorrectly");
}

TEST_P(ScanAndReadInteTest, TestWithPKWithAggregateBatchScanSnapshot3WithPredicate) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_mor.db/pk_table_scan_and_read_mor/";

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "3")
        .AddOption(Options::MERGE_ENGINE, "aggregation")
        .AddOption("fields.f3.aggregate-function", "sum");
    scan_context_builder.SetBucketFilter(1).SetPartitionFilter({{{"f1", "10"}}});
    // predicate f3 > 20.0 will not be applied, as merge engine is aggregation
    auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f3",
                                                   FieldType::DOUBLE, Literal(20.0));
    scan_context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    read_context_builder.AddOption(Options::MERGE_ENGINE, "aggregation")
        .AddOption("fields.f3.aggregate-function", "sum");
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 3);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 0]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestWithPKWithPartialUpdateBatchScanSnapshot3WithPredicate) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_mor.db/pk_table_scan_and_read_mor/";

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "3")
        .AddOption(Options::MERGE_ENGINE, "partial-update")
        .AddOption(Options::IGNORE_DELETE, "true");
    scan_context_builder.SetBucketFilter(1).SetPartitionFilter({{{"f1", "10"}}});
    // predicate f3 > 20.0 will not be applied, as merge engine is partial-update
    auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f3",
                                                   FieldType::DOUBLE, Literal(20.0));
    scan_context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    read_context_builder.AddOption(Options::MERGE_ENGINE, "partial-update")
        .AddOption(Options::IGNORE_DELETE, "true");
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 3);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));
    ASSERT_TRUE(read_result);
    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestWithPKWithMorBatchScanSnapshot5WithLimit) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_mor.db/pk_table_scan_and_read_mor/";

    // in pk mor mode, limit does not take effect, as we do not know the number of records after
    // merging
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.SetLimit(6);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 5);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Marco", 10, 0, 21.1],
[0, "Marco2", 10, 0, 31.1],
[0, "Skye", 10, 0, 21.0],
[0, "Skye2", 10, 0, 31.0],
[0, "Alice", 10, 1, 19.1],
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Lucy", 20, 1, 14.1], [0, "Paul", 20, 1, 18.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestWithPKWithMorStreamFromSnapshot4) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_mor.db/pk_table_scan_and_read_mor/";

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "4")
        .AddOption(Options::SCAN_MODE, "from-snapshot-full")
        .WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    // first plan is snapshot4 (isStreaming=false), second plan is snapshot5
    std::vector<std::optional<int64_t>> expected_snapshot_ids = {4, 5};
    auto expected_snapshot4_batch = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alex", 10, 0, 21.2],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Marco", 10, 0, 21.1],
[0, "Skye", 10, 0, 21.0],
[0, "Alice", 10, 1, 19.1],
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, 18.1]
    ])")
            .ValueOrDie());
    auto expected_snapshot5_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[3, "Alex", 10, 0, 31.2],
[0, "Marco2", 10, 0, 31.1],
[0, "Skye2", 10, 0, 31.0]
        ])")
            .ValueOrDie());
    std::vector<std::shared_ptr<arrow::ChunkedArray>> expected_arrays = {expected_snapshot4_batch,
                                                                         expected_snapshot5_stream};
    CheckStreamScanResult(table_scan, table_read, expected_snapshot_ids, expected_arrays);
}

TEST_P(ScanAndReadInteTest, TestWithPKWithMorStreamFromSnapshot1) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_mor.db/pk_table_scan_and_read_mor/";

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "1")
        .AddOption(Options::SCAN_MODE, "from-snapshot-full")
        .WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::optional<int64_t>> expected_snapshot_ids = {1, 2, 4, 5};
    auto expected_snapshot1_batch = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alice", 10, 1, 11.1],
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, 18.1]
    ])")
            .ValueOrDie());
    auto expected_snapshot2_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[2, "Alice", 10, 1, 19.1],
[3, "Tony", 10, 0, 14.1]
    ])")
            .ValueOrDie());

    auto expected_snapshot4_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[2, "Alex", 10, 0, 21.2],
[0, "Marco", 10, 0, 21.1],
[0, "Skye", 10, 0, 21.0]
        ])")
            .ValueOrDie());

    auto expected_snapshot5_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[3, "Alex", 10, 0, 31.2],
[0, "Marco2", 10, 0, 31.1],
[0, "Skye2", 10, 0, 31.0]
        ])")
            .ValueOrDie());
    std::vector<std::shared_ptr<arrow::ChunkedArray>> expected_arrays = {
        expected_snapshot1_batch, expected_snapshot2_stream, expected_snapshot4_stream,
        expected_snapshot5_stream};
    CheckStreamScanResult(table_scan, table_read, expected_snapshot_ids, expected_arrays);
}

TEST_P(ScanAndReadInteTest, TestWithPKWithMorStreamFromSnapshot2) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_mor.db/pk_table_scan_and_read_mor/";

    // use from-snapshot mode, first plan is empty, snapshot 2 in second plan only includes delta
    // files
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "2")
        .AddOption(Options::SCAN_MODE, "from-snapshot")
        .WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    std::vector<std::optional<int64_t>> expected_snapshot_ids = {std::nullopt, 2, 4, 5};
    auto expected_snapshot2_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[2, "Alice", 10, 1, 19.1],
[3, "Tony", 10, 0, 14.1]
    ])")
            .ValueOrDie());

    auto expected_snapshot4_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[2, "Alex", 10, 0, 21.2],
[0, "Marco", 10, 0, 21.1],
[0, "Skye", 10, 0, 21.0]
        ])")
            .ValueOrDie());

    auto expected_snapshot5_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[3, "Alex", 10, 0, 31.2],
[0, "Marco2", 10, 0, 31.1],
[0, "Skye2", 10, 0, 31.0]
        ])")
            .ValueOrDie());
    std::vector<std::shared_ptr<arrow::ChunkedArray>> expected_arrays = {
        nullptr, expected_snapshot2_stream, expected_snapshot4_stream, expected_snapshot5_stream};
    CheckStreamScanResult(table_scan, table_read, expected_snapshot_ids, expected_arrays);
}

TEST_P(ScanAndReadInteTest, TestWithPKWithMorStreamFromSnapshot5WithPredicate) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_table_scan_and_read_mor.db/pk_table_scan_and_read_mor/";

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "5")
        .AddOption(Options::SCAN_MODE, "from-snapshot")
        .WithStreamingMode(true);
    auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f3",
                                                   FieldType::DOUBLE, Literal(50.0));
    scan_context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    read_context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    // first plan is snapshot 5 with empty plan, second plan is snapshot 5 with delta files
    // (predicates take no effects in delta mode)
    std::vector<std::optional<int64_t>> expected_snapshot_ids = {std::nullopt, 5};
    // for streaming data split, return array with row_kind
    auto expected_snapshot5_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[3, "Alex", 10, 0, 31.2],
[0, "Marco2", 10, 0, 31.1],
[0, "Skye2", 10, 0, 31.0]
        ])")
            .ValueOrDie());
    std::vector<std::shared_ptr<arrow::ChunkedArray>> expected_arrays = {nullptr,
                                                                         expected_snapshot5_stream};
    CheckStreamScanResult(table_scan, table_read, expected_snapshot_ids, expected_arrays);
}

// test first row merge engine
TEST_P(ScanAndReadInteTest, TestWithPKWithFirstRowBatchScanSnapshot5) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path =
        paimon::test::GetDataDir() + file_format +
        "/pk_table_scan_and_read_first_row.db/pk_table_scan_and_read_first_row/";

    // normal batch scan case for pk+first row, all data in level 0 is filtered out
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "5");
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 5);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alice", 10, 1, 11.1],
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, 18.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestWithPKWithFirstRowStreamFromSnapshot3) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path =
        paimon::test::GetDataDir() + file_format +
        "/pk_table_scan_and_read_first_row.db/pk_table_scan_and_read_first_row/";

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "3")
        .AddOption(Options::SCAN_MODE, "from-snapshot-full")
        .WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    // from a compact snapshot
    std::vector<std::optional<int64_t>> expected_snapshot_ids = {3, 4, 5};
    auto expected_snapshot3_batch = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alice", 10, 1, 11.1],
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, 18.1]
    ])")
            .ValueOrDie());

    auto expected_snapshot4_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[2, "Alex", 10, 0, 21.2],
[0, "Marco", 10, 0, 21.1],
[0, "Skye", 10, 0, 21.0]
        ])")
            .ValueOrDie());
    // delete record is ignored
    auto expected_snapshot5_stream = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Marco2", 10, 0, 31.1],
[0, "Skye2", 10, 0, 31.0]
        ])")
            .ValueOrDie());
    std::vector<std::shared_ptr<arrow::ChunkedArray>> expected_arrays = {
        expected_snapshot3_batch, expected_snapshot4_stream, expected_snapshot5_stream};
    CheckStreamScanResult(table_scan, table_read, expected_snapshot_ids, expected_arrays);
}

TEST_P(ScanAndReadInteTest, TestWithPKWithFirstRowStreamFromSnapshot5) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path =
        paimon::test::GetDataDir() + file_format +
        "/pk_table_scan_and_read_first_row.db/pk_table_scan_and_read_first_row/";

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_MODE, "latest-full").WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    // from an append snapshot, first plan is snapshot5 with merge, all level 0 data is merged
    std::vector<std::optional<int64_t>> expected_snapshot_ids = {5};
    auto expected_snapshot5_batch = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alice", 10, 1, 11.1],
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Marco", 10, 0, 21.1],
[0, "Marco2", 10, 0, 31.1],
[0, "Skye", 10, 0, 21.0],
[0, "Skye2", 10, 0, 31.0],
[0, "Tony", 10, 0, 14.1],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, 18.1]
    ])")
            .ValueOrDie());

    std::vector<std::shared_ptr<arrow::ChunkedArray>> expected_arrays = {expected_snapshot5_batch};
    CheckStreamScanResult(table_scan, table_read, expected_snapshot_ids, expected_arrays);
}

TEST_P(ScanAndReadInteTest, TestWithPKWith09VersionDvBatchScanLatestSnapshot) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format + "/pk_09.db/pk_09/";

    // normal batch scan case for pk+dv (09 version)
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.SetLimit(2);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 8);
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Alice", 10, 1, 21.1],
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Whether I shall turn out to be the hero of my own life.", 10, 1, 19.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestWithEmptyPartitionValue) {
    auto [file_format, enable_prefetch] = GetParam();

    auto check_result =
        [&](const std::string& table_path,
            const std::vector<std::map<std::string, std::string>>& partition_filters,
            const std::shared_ptr<arrow::ChunkedArray>& expected) {
            ScanContextBuilder scan_context_builder(table_path);
            scan_context_builder.SetPartitionFilter(partition_filters);
            ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
            ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

            ReadContextBuilder read_context_builder(table_path);
            AddReadOptionsForPrefetch(&read_context_builder);
            ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
            ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

            ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
            ASSERT_EQ(result_plan->SnapshotId().value(), 1);
            ASSERT_OK_AND_ASSIGN(auto batch_reader,
                                 table_read->CreateReader(result_plan->Splits()));
            ASSERT_OK_AND_ASSIGN(auto read_result,
                                 ReadResultCollector::CollectResult(batch_reader.get()));
            // check result
            ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
        };

    auto expected_without_partition_filter = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "2025", 500, -1, 15.1],
[0, "2024", 100, -2, 11.1],
[0, "2024", 300, 1, 13.1],
[0, null, 200, -3, 12.1],
[0, null, 50, 1, 14.1],
[0, null, 100, 2, 16.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected_without_partition_filter);

    {
        std::string table_path = paimon::test::GetDataDir() + file_format +
                                 "/append_with_empty_partition.db/append_with_empty_partition/";

        check_result(table_path, {}, expected_without_partition_filter);
        check_result(table_path, {{{"f0", "2025"}}},
                     expected_without_partition_filter->Slice(0, 1));
        check_result(table_path, {{{"f0", "__DEFAULT_PARTITION__"}}},
                     expected_without_partition_filter->Slice(3, 3));
        check_result(table_path, {{{"f0", "__DEFAULT_PARTITION__"}}, {{"f0", "2024"}}},
                     expected_without_partition_filter->Slice(1, 5));
    }
    {
        std::string table_path = paimon::test::GetDataDir() + file_format +
                                 "/append_with_empty_partition_with_specific_name.db/"
                                 "append_with_empty_partition_with_specific_name/";

        // check with specific partition name
        check_result(table_path, {}, expected_without_partition_filter);
        check_result(table_path, {{{"f0", "2025"}}},
                     expected_without_partition_filter->Slice(0, 1));
        check_result(table_path, {{{"f0", "__HIVE_DEFAULT_PARTITION__"}}},
                     expected_without_partition_filter->Slice(3, 3));
        check_result(table_path, {{{"f0", "__HIVE_DEFAULT_PARTITION__"}}, {{"f0", "2024"}}},
                     expected_without_partition_filter->Slice(1, 5));
    }
}

TEST_P(ScanAndReadInteTest, TestWithMultipleEmptyPartitionValue) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/append_with_empty_partition_with_empty_value.db/"
                             "append_with_empty_partition_with_empty_value/";

    auto check_result =
        [&](const std::vector<std::map<std::string, std::string>>& partition_filters,
            const std::shared_ptr<arrow::ChunkedArray>& expected) {
            ScanContextBuilder scan_context_builder(table_path);
            scan_context_builder.SetPartitionFilter(partition_filters);
            ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
            ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

            ReadContextBuilder read_context_builder(table_path);
            AddReadOptionsForPrefetch(&read_context_builder);
            ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
            ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

            ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
            ASSERT_EQ(result_plan->SnapshotId().value(), 1);
            ASSERT_OK_AND_ASSIGN(auto batch_reader,
                                 table_read->CreateReader(result_plan->Splits()));
            ASSERT_OK_AND_ASSIGN(auto read_result,
                                 ReadResultCollector::CollectResult(batch_reader.get()));
            // check result
            ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
        };

    auto expected_without_partition_filter = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "", 200, -3, 12.1],
[0, "", 50, 1, 14.1],
[0, "2025", 500, -1, 15.1],
[0, " ", 100, 0, 15.1],
[0, "2024", 100, -2, 11.1],
[0, "2024", 300, 1, 13.1],
[0, null, 100, 2, 16.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected_without_partition_filter);

    check_result({}, expected_without_partition_filter);
    check_result({{{"f0", "2025"}}}, expected_without_partition_filter->Slice(2, 1));
    check_result({{{"f0", "__DEFAULT_PARTITION__"}}},
                 expected_without_partition_filter->Slice(6, 1));
    check_result({{{"f0", ""}}}, expected_without_partition_filter->Slice(0, 2));
    check_result({{{"f0", " "}}}, expected_without_partition_filter->Slice(3, 1));
    check_result({{{"f0", ""}}, {{"f0", "2025"}}}, expected_without_partition_filter->Slice(0, 3));
    check_result({{{"f0", "2024"}}, {{"f0", "__DEFAULT_PARTITION__"}}},
                 expected_without_partition_filter->Slice(4, 3));
}

TEST_P(ScanAndReadInteTest, TestMemoryUse) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format + "/append_09.db/append_09/";

    // scan
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "1");
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 1);

    auto splits = result_plan->Splits();
    ASSERT_EQ(3, splits.size());
    // read
    std::shared_ptr<MemoryPool> read_pool = GetMemoryPool();
    {
        ReadContextBuilder read_context_builder(table_path);
        AddReadOptionsForPrefetch(&read_context_builder);
        read_context_builder.WithMemoryPool(read_pool);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                             read_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
        ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(splits));
        ASSERT_OK_AND_ASSIGN(auto read_result,
                             ReadResultCollector::CollectResult(batch_reader.get()));

        // check result
        auto expected = std::make_shared<arrow::ChunkedArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 0, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Lucy", 20, 1, 14.1]
   ])")
                .ValueOrDie());
        ASSERT_TRUE(expected);
        ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
    }
    // check all memory is released
    ASSERT_GT(read_pool->MaxMemoryUsage(), 0);
    ASSERT_EQ(read_pool->CurrentUsage(), 0);
}

TEST_P(ScanAndReadInteTest, TestPkScanWithPostponeBucket) {
    auto [file_format, enable_prefetch] = GetParam();

    auto test_dir = UniqueTestDirectory::Create("local");
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::int32()),
                                 arrow::field("f2", arrow::float64())};
    auto field_with_row_kind = fields;
    field_with_row_kind.insert(field_with_row_kind.begin(),
                               arrow::field("_VALUE_KIND", arrow::int8()));

    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, file_format},
                                                  {Options::TARGET_FILE_SIZE, "1024"},
                                                  {Options::BUCKET, "-2"},
                                                  {Options::FILE_SYSTEM, "local"}};
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir->Str(), schema, /*partition_keys=*/{"f1"},
                                            /*primary_keys=*/{"f0", "f1"}, options,
                                            /*is_streaming_mode=*/true));
    std::string table_path = test_dir->Str() + "/foo.db/bar";
    int64_t commit_identifier = 0;
    // write batch1
    std::string data1 = R"([
            ["banana", 1, 3.5],
            ["dog", 1, 2000.5],
            ["lucy", 1, 10000.5],
            ["mouse", 1, 10.5]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch1,
                         TestHelper::MakeRecordBatch(
                             arrow::struct_(fields), data1,
                             /*partition_map=*/std::map<std::string, std::string>({{"f1", "1"}}),
                             /*bucket=*/-2, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1,
                         helper->WriteAndCommit(std::move(batch1), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    // write batch2
    std::string data2 = R"([
        ["Paul", 2, 12.1],
        ["Cathy", 2, 13.1],
        ["Emily", 2, 14.1],
        ["Cathy", 2, 13.1]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch2,
                         TestHelper::MakeRecordBatch(
                             arrow::struct_(fields), data2,
                             /*partition_map=*/std::map<std::string, std::string>({{"f1", "2"}}),
                             /*bucket=*/-2,
                             {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT,
                              RecordBatch::RowKind::INSERT, RecordBatch::RowKind::DELETE}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2,
                         helper->WriteAndCommit(std::move(batch2), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    std::vector<std::string> subdirs = {"f1=1/bucket-postpone", "f1=2/bucket-postpone"};
    CheckPostponeFile(table_path, subdirs);

    {
        // batch scan
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.WithStreamingMode(false);
        ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
        ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
        ASSERT_EQ(result_plan->SnapshotId().value(), 2);
        ASSERT_TRUE(result_plan->Splits().empty());
    }
    {
        // stream scan: from snapshot 1
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "1").WithStreamingMode(true);
        ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

        ReadContextBuilder read_context_builder(table_path);
        AddReadOptionsForPrefetch(&read_context_builder);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                             read_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

        std::vector<std::optional<int64_t>> expected_snapshot_ids = {std::nullopt, 1, 2};
        auto expected_snapshot1 = std::make_shared<arrow::ChunkedArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(field_with_row_kind), R"([
            [0, "banana", 1, 3.5],
            [0, "dog", 1, 2000.5],
            [0, "lucy", 1, 10000.5],
            [0, "mouse", 1, 10.5]
    ])")
                .ValueOrDie());
        auto expected_snapshot2 = std::make_shared<arrow::ChunkedArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(field_with_row_kind), R"([
            [0, "Paul", 2, 12.1],
            [0, "Cathy", 2, 13.1],
            [0, "Emily", 2, 14.1],
            [3, "Cathy", 2, 13.1]
        ])")
                .ValueOrDie());
        std::vector<std::shared_ptr<arrow::ChunkedArray>> expected_arrays = {
            nullptr, expected_snapshot1, expected_snapshot2};
        CheckStreamScanResult(table_scan, table_read, expected_snapshot_ids, expected_arrays);
    }
    {
        // stream scan: from snapshot 2
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "2").WithStreamingMode(true);
        ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

        ReadContextBuilder read_context_builder(table_path);
        AddReadOptionsForPrefetch(&read_context_builder);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                             read_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

        std::vector<std::optional<int64_t>> expected_snapshot_ids = {std::nullopt, 2};
        auto expected_snapshot2 = std::make_shared<arrow::ChunkedArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(field_with_row_kind), R"([
            [0, "Paul", 2, 12.1],
            [0, "Cathy", 2, 13.1],
            [0, "Emily", 2, 14.1],
            [3, "Cathy", 2, 13.1]
        ])")
                .ValueOrDie());
        std::vector<std::shared_ptr<arrow::ChunkedArray>> expected_arrays = {nullptr,
                                                                             expected_snapshot2};
        CheckStreamScanResult(table_scan, table_read, expected_snapshot_ids, expected_arrays);
    }
    {
        // stream scan: from snapshot full
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "1")
            .AddOption(Options::SCAN_MODE, "from-snapshot-full")
            .WithStreamingMode(true);
        ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

        ReadContextBuilder read_context_builder(table_path);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                             read_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

        std::vector<std::optional<int64_t>> expected_snapshot_ids = {1, 2};
        auto expected_snapshot1 = std::make_shared<arrow::ChunkedArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(field_with_row_kind), R"([
            [0, "banana", 1, 3.5],
            [0, "dog", 1, 2000.5],
            [0, "lucy", 1, 10000.5],
            [0, "mouse", 1, 10.5]
    ])")
                .ValueOrDie());
        auto expected_snapshot2 = std::make_shared<arrow::ChunkedArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(field_with_row_kind), R"([
            [0, "Paul", 2, 12.1],
            [0, "Cathy", 2, 13.1],
            [0, "Emily", 2, 14.1],
            [3, "Cathy", 2, 13.1]
        ])")
                .ValueOrDie());
        std::vector<std::shared_ptr<arrow::ChunkedArray>> expected_arrays = {expected_snapshot1,
                                                                             expected_snapshot2};
        CheckStreamScanResult(table_scan, table_read, expected_snapshot_ids, expected_arrays);
    }
}

TEST_P(ScanAndReadInteTest, TestScanWithPredicateAndReadWithUnorderedFieldForParquet) {
    auto [file_format, enable_prefetch] = GetParam();
    if (file_format != "parquet") {
        return;
    }
    std::string table_path =
        paimon::test::GetDataDir() + "parquet/parquet_append_table.db/parquet_append_table";
    ScanContextBuilder scan_context_builder(table_path);

    auto predicate = PredicateBuilder::LessThan(
        /*field_index=*/3, /*field_name=*/"f4", FieldType::INT, Literal(300006));
    scan_context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 2);
    ASSERT_EQ(result_plan->Splits().size(), 1);

    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    read_context_builder.SetReadSchema({"f10", "f8", "f4", "f13"});
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(
                {arrow::field("_VALUE_KIND", arrow::int8()),
                 arrow::field("f10",
                              arrow::map(arrow::list(arrow::float32()),
                                         arrow::struct_({arrow::field("f0", arrow::boolean()),
                                                         arrow::field("f1", arrow::int64())}))),
                 arrow::field("f8", arrow::utf8()), arrow::field("f4", arrow::int32()),
                 arrow::field("f13", arrow::decimal128(2, 2))}),
            R"([
[0, [[[5.11, 5.21], [true, 61]]], "s31", 300001, "0.91"],
[0, [[[5.12, 5.22], [false, 62]]], "s32", 300002, "0.92"],
[0, null, "s33", 300003, "0.93"],
[0, [[[5.141, 5.241], [false, 641]], [[5.14, 5.24], [false, 64]]], "s34", 300004, "0.94"],
[0, [[[5.15, 5.25], [true, 65]]], "s35", 300005, "0.95"],
[0, [[[5.16, 5.26], null]], "s36", 300006, "0.96"]
])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

#ifdef PAIMON_ENABLE_LANCE
TEST_F(ScanAndReadInteTest, TestScanWithPredicateAndReadWithUnorderedFieldForLance) {
    auto test_dir = UniqueTestDirectory::Create("local");
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::int32()),
                                 arrow::field("f2", arrow::float64())};
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, "lance"},
                                                  {Options::TARGET_FILE_SIZE, "1024"},
                                                  {Options::BUCKET, "-1"},
                                                  {Options::FILE_SYSTEM, "local"}};
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir->Str(), schema, /*partition_keys=*/{},
                                            /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));
    std::string table_path = test_dir->Str() + "/foo.db/bar";
    int64_t commit_identifier = 0;
    std::string data = R"([
            ["banana", 2, 3.5],
            ["dog", 1, 2000.5],
            ["lucy", 14, 10000.5],
            ["mouse", 100, 10.5]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    ScanContextBuilder scan_context_builder(table_path);
    // predicate does not take effective as lance file does not have stats
    auto predicate = PredicateBuilder::GreaterThan(
        /*field_index=*/2, /*field_name=*/"f2", FieldType::DOUBLE, Literal(50000.2));
    scan_context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 1);

    ReadContextBuilder read_context_builder(table_path);
    read_context_builder.SetReadSchema({"f2", "f0"});
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({arrow::field("_VALUE_KIND", arrow::int8()), fields[2], fields[0]}),
            R"([[0, 3.5, "banana"],
                [0, 2000.5, "dog"],
                [0, 10000.5, "lucy"],
                [0, 10.5, "mouse"]])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}
#endif

TEST_P(ScanAndReadInteTest, TestAppendTableWithMultipleFileFormat) {
    auto [file_format, enable_prefetch] = GetParam();
    if (file_format != "parquet") {
        return;
    }
    std::string table_path =
        paimon::test::GetDataDir() +
        "/append_table_with_multiple_file_format.db/append_table_with_multiple_file_format";
    // scan
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "2");
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 2);

    auto splits = result_plan->Splits();
    ASSERT_EQ(1, splits.size());
    // read
    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 0, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Lucy", 20, 1, 14.1],
[0, "Skye", 20, 1, 19.1],
[0, "Bob", 10, 0, 20.1]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestPkDvTableIndexInDataAndNoExternalPath) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/pk_dv_index_in_data_no_external.db/pk_dv_index_in_data_no_external";
    // scan
    ScanContextBuilder scan_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 4);

    auto splits = result_plan->Splits();
    ASSERT_EQ(3, splits.size());
    // read
    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Alice", 10, 1, 19.1],
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, 18.1]
])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestPkDvTableIndexNotInDataAndNoExternalPath) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path =
        paimon::test::GetDataDir() + file_format +
        "/pk_dv_index_not_in_data_no_external.db/pk_dv_index_not_in_data_no_external";
    // scan
    ScanContextBuilder scan_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 4);

    auto splits = result_plan->Splits();
    ASSERT_EQ(3, splits.size());
    // read
    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Alice", 10, 1, 19.1],
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, 18.1]
])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestPkDvTableIndexNotInDataAndWithExternalPath) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path =
        paimon::test::GetDataDir() + file_format +
        "/pk_dv_index_not_in_data_with_external.db/pk_dv_index_not_in_data_with_external";
    std::string external_path = paimon::test::GetDataDir() + file_format +
                                "/pk_dv_index_not_in_data_with_external.db/external";
    // scan
    ScanContextBuilder scan_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 4);

    auto splits = result_plan->Splits();
    ASSERT_EQ(3, splits.size());
    AdjustSplitWithExternalPath("FILE:/tmp/external", external_path, /*adjust_index=*/false,
                                &splits);
    // read
    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Alice", 10, 1, 19.1],
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, 18.1]
])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestScanAndReadWithDisableIndex) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path =
        paimon::test::GetDataDir() + file_format + "/append_with_bitmap.db/append_with_bitmap";
    auto predicate =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                Literal(FieldType::STRING, "Alice0", 6));

    // scan
    ScanContextBuilder scan_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(
        auto scan_context,
        scan_context_builder.AddOption("file-index.read.enabled", "false").Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 1);

    auto splits = result_plan->Splits();
    ASSERT_EQ(1, splits.size());
    // read
    ReadContextBuilder read_context_builder(table_path);
    read_context_builder.AddOption("file-index.read.enabled", "false");
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result, when file-index.read.enabled = false, index will be ignored
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 1, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Lucy", 20, 1, 15.1],
[0, "Bob", 10, 1, 16.1],
[0, "Tony", 20, 0, 17.1],
[0, "Alice", 20, null, 18.1]
])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestPkDvTableIndexInDataAndWithExternalPath) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path =
        paimon::test::GetDataDir() + file_format +
        "/pk_dv_index_in_data_with_external.db/pk_dv_index_in_data_with_external";
    std::string external_path =
        paimon::test::GetDataDir() + file_format + "/pk_dv_index_in_data_with_external.db/external";
    // scan
    ScanContextBuilder scan_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 4);

    auto splits = result_plan->Splits();
    ASSERT_EQ(3, splits.size());
    AdjustSplitWithExternalPath("FILE:/tmp/external", external_path, /*adjust_index=*/true,
                                &splits);
    // read
    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type_, R"([
[0, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 10, 1, 11.0],
[0, "Alice", 10, 1, 19.1],
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 13.1],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, 18.1]
])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestTimestampType) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/append_with_multiple_ts_precision_and_timezone.db"
                             "/append_with_multiple_ts_precision_and_timezone/";
    ScanContextBuilder scan_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 1);

    auto splits = result_plan->Splits();
    ASSERT_EQ(1, splits.size());

    // read
    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));
    // check result
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
    };
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
[0, "1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001", "1970-01-01 00:00:02", "1970-01-01 00:00:00.002", "1970-01-01 00:00:00.000002", "1970-01-01 00:00:00.000000002"],
[0, "1970-01-01 00:00:03", "1970-01-01 00:00:00.003", null, "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000004"],
[0, "1970-01-01 00:00:05", "1970-01-01 00:00:00.005", null, null, "1970-01-01 00:00:06", null, "1970-01-01 00:00:00.000006", null]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(ScanAndReadInteTest, TestCastTimestampType) {
    TimezoneGuard tz_guard("Asia/Shanghai");
    auto [file_format, enable_prefetch] = GetParam();
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/append_with_cast_timestamp.db"
                             "/append_with_cast_timestamp/";
    // scan
    ScanContextBuilder scan_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 1);

    auto splits = result_plan->Splits();
    ASSERT_EQ(1, splits.size());

    // read
    ReadContextBuilder read_context_builder(table_path);
    AddReadOptionsForPrefetch(&read_context_builder);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // check result
    arrow::FieldVector fields = {
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("ts_sec", arrow::int32()),
        arrow::field("ts_milli", arrow::date32()),
        arrow::field("ts_micro", arrow::utf8()),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO, "Asia/Shanghai")),
        arrow::field("ts_tz_sec", arrow::int32()),
        arrow::field("ts_tz_milli", arrow::date32()),
        arrow::field("ts_tz_micro", arrow::utf8()),
        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("int_ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("int_ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, "Asia/Shanghai")),
    };
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
[0, -28799, 0, "1970-01-01 00:00:00.000001", "1969-12-31 16:00:00.000000001", 2, 0, "1970-01-01 08:00:00.000002", "1970-01-01 08:00:00.000000002", "1970-01-01 08:00:00", "1970-01-01 00:00:00.000000"],
[0, -28797, 0, null, "1969-12-31 16:00:00.000000003", 4, 0, "1970-01-01 08:00:00.000004", "1970-01-01 08:00:00.000000004", "1970-01-01 08:00:01", "1970-01-01 00:00:01.000000"],
[0, -28795, 0, null, null, 6, null, "1970-01-01 08:00:00.000006", null, "1970-01-01 07:59:59", "1969-12-31 23:59:59.000000"]
   ])")
            .ValueOrDie());
    ASSERT_TRUE(expected);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

#ifdef PAIMON_ENABLE_AVRO
// TODO(zjw): remove DISABLED_ when avro write is ready
TEST_F(ScanAndReadInteTest, DISABLED_TestAvroWithAppendTable) {
    auto read_data = [](int64_t snapshot_id, const std::string& result_json) {
        std::string table_path = GetDataDir() + "/avro/append_multiple.db/append_multiple";
        // scan
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, std::to_string(snapshot_id));
        ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
        ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
        ASSERT_EQ(result_plan->SnapshotId().value(), snapshot_id);
        auto splits = result_plan->Splits();
        ASSERT_EQ(3, splits.size());

        // read
        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.AddOption("test.enable-adaptive-prefetch-strategy", "false");
        read_context_builder.EnablePrefetch(true).SetPrefetchBatchCount(3);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                             read_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
        ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(splits));
        ASSERT_OK_AND_ASSIGN(auto read_result,
                             ReadResultCollector::CollectResult(batch_reader.get()));

        // check result
        auto timezone = DateTimeUtils::GetLocalTimezoneName();
        arrow::FieldVector fields = {
            arrow::field("_VALUE_KIND", arrow::int8()),
            arrow::field("f0", arrow::int8()),
            arrow::field("f1", arrow::int16()),
            arrow::field("f2", arrow::int32()),
            arrow::field("f3", arrow::int64()),
            arrow::field("f4", arrow::float32()),
            arrow::field("f5", arrow::float64()),
            arrow::field("f6", arrow::utf8()),
            arrow::field("f7", arrow::binary()),
            arrow::field("f8", arrow::date32()),
            arrow::field("f9", arrow::decimal128(5, 2)),
            arrow::field("f10", arrow::timestamp(arrow::TimeUnit::SECOND)),
            arrow::field("f11", arrow::timestamp(arrow::TimeUnit::MILLI)),
            arrow::field("f12", arrow::timestamp(arrow::TimeUnit::MICRO)),
            arrow::field("f13", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
            arrow::field("f14", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
            arrow::field("f15", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
            arrow::field("f16", arrow::struct_(
                                    {arrow::field("f0", arrow::map(arrow::utf8(), arrow::int32())),
                                     arrow::field("f1", arrow::list(arrow::int32()))})),
        };
        auto expected = std::make_shared<arrow::ChunkedArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), result_json)
                .ValueOrDie());
        ASSERT_TRUE(expected);
        ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
    };

    read_data(1, R"([
[0, 2, 10, 1, 100, 2.0, 2.0, "two", "bbb", 123, "123.45", "1970-01-02 00:00:00", "1970-01-02 00:00:00.000", "1970-01-02 00:00:00.000000", "1970-01-02 00:00:00", "1970-01-02 00:00:00.000", "1970-01-02 00:00:00.000000",[[["key",123]],[1,2,3]]],
[0, 1, 10, 0, 100, 1.0, 1.0, "one", "aaa", 123, "123.45", "1970-01-01 00:00:00", "1970-01-01 00:00:00.000", "1970-01-01 00:00:00.000000", "1970-01-01 00:00:00", "1970-01-01 00:00:00.000", "1970-01-01 00:00:00.000000",[[["key",123]],[1,2,3]]],
[0, 3, 11, 0, 100, null, 3.0, "three", "ccc", 123, "123.45", "1970-01-03 00:00:00", "1970-01-03 00:00:00.000", "1970-01-03 00:00:00.000000", "1970-01-03 00:00:00", "1970-01-03 00:00:00.000", "1970-01-03 00:00:00.000000",[[["key",123]],[1,2,3]]],
[0, 4, 11, 0, 100, 4.0, null, "four", "ddd", 123, "123.45", "1970-01-04 00:00:00", "1970-01-04 00:00:00.000", "1970-01-04 00:00:00.000000", "1970-01-04 00:00:00", "1970-01-04 00:00:00.000", "1970-01-04 00:00:00.000000",[[["key",123]],[1,2,3]]]
])");

    read_data(2, R"([
[0, 6, 10, 1, 100, 6.0, 4.0, "six", "fff", 123, "123.45", "1970-01-02 00:00:00", "1970-01-06 00:00:00.000", "1970-01-06 00:00:00.000000", "1970-01-06 00:00:00", "1970-01-06 00:00:00.000", "1970-01-06 00:00:00.000000",[[["key",123]],[1,2,3]]],
[0, 5, 10, 0, 100, 5.0, 2.0, null, "eee", 123, "123.45", "1970-01-01 00:00:00", "1970-01-05 00:00:00.000", "1970-01-05 00:00:00.000000", "1970-01-05 00:00:00", "1970-01-05 00:00:00.000", "1970-01-05 00:00:00.000000",[[["key",123]],[1,2,3]]],
[0, 7, 11, 0, 100, 7.0, 6.0, "seven", "ggg", 123, "123.45", "1970-01-03 00:00:00", "1970-01-07 00:00:00.000", "1970-01-07 00:00:00.000000", "1970-01-07 00:00:00", "1970-01-07 00:00:00.000", "1970-01-07 00:00:00.000000",[[["key",123]],[1,2,3]]]
])");
}

TEST_F(ScanAndReadInteTest, DISABLED_TestAvroWithPkTable) {
    auto read_data = [](int64_t snapshot_id, const std::string& result_json) {
        std::string table_path =
            GetDataDir() + "/avro/pk_with_multiple_type.db/pk_with_multiple_type";
        // scan
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, std::to_string(snapshot_id));
        ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
        ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
        ASSERT_EQ(result_plan->SnapshotId().value(), snapshot_id);

        auto splits = result_plan->Splits();
        ASSERT_EQ(1, splits.size());

        // read
        ReadContextBuilder read_context_builder(table_path);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                             read_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
        ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(splits));
        ASSERT_OK_AND_ASSIGN(auto read_result,
                             ReadResultCollector::CollectResult(batch_reader.get()));

        // check result
        arrow::FieldVector fields = {
            arrow::field("_VALUE_KIND", arrow::int8()),
            arrow::field("f0", arrow::boolean()),
            arrow::field("f1", arrow::int8()),
            arrow::field("f2", arrow::int16()),
            arrow::field("f3", arrow::int32()),
            arrow::field("f4", arrow::int64()),
            arrow::field("f5", arrow::float32()),
            arrow::field("f6", arrow::float64()),
            arrow::field("f7", arrow::utf8()),
            arrow::field("f8", arrow::binary()),
            arrow::field("f9", arrow::date32()),
            arrow::field("f10", arrow::decimal128(5, 2)),
            arrow::field("f11", arrow::struct_(
                                    {arrow::field("f0", arrow::map(arrow::utf8(), arrow::int32())),
                                     arrow::field("f1", arrow::list(arrow::int32()))})),
        };
        auto expected = std::make_shared<arrow::ChunkedArray>(
            arrow::ipc::internal::json::ArrayFromJSON(struct_(fields), result_json).ValueOrDie());
        ASSERT_TRUE(expected);
        ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
    };

    read_data(1, R"([
[0, false, 10, 1, 1, 1000, 1.5, 2.5, "Alice", "abcdef", 100, "123.45", [[["key",123]],[1,2,3]]],
[0, false, 10, 1, 1, 1000, 1.5, 2.5, "Bob", "abcdef", 100, "123.45", [[["key",123]],[1,2,3]]],
[0, true, 10, 1, 1, 1000, 1.5, 2.5, "Emily", "abcdef", 100, "123.45", [[["key",123]],[1,2,3]]],
[0, true, 10, 1, 1, 1000, 1.5, 2.5, "Tony", "abcdef", 100, "123.45", [[["key",123]],[1,2,3]]]
])");

    read_data(2, R"([
[0, false, 10, 1, 1, 1000, 1.5, 2.5, "Alice", "abcdef", 100, "123.45", [[["key",123]],[1,2,3]]],
[0, false, 10, 1, 1, 1000, 1.5, 2.5, "Bob", "abcdef", 100, "123.45", [[["key",123]],[1,2,3]]],
[0, true, 10, 1, 1, 1000, 1.5, 2.5, "Lucy", "abcdef", 100, "123.45", [[["key",123]],[1,2,3]]],
[0, true, 10, 1, 1, 1000, 1.5, 2.5, "Tony", "abcdef", 100, "123.45", [[["key",123]],[1,2,3]]]
])");
}
#endif

std::vector<std::pair<std::string, bool>> GetTestValuesForScanAndReadInteTest() {
    std::vector<std::pair<std::string, bool>> values = {{"parquet", false}, {"parquet", true}};
#ifdef PAIMON_ENABLE_ORC
    values.emplace_back("orc", false);
    values.emplace_back("orc", true);
#endif
    return values;
}

INSTANTIATE_TEST_SUITE_P(FileFormatAndEnablePaimonPrefetch, ScanAndReadInteTest,
                         ::testing::ValuesIn(std::vector<std::pair<std::string, bool>>(
                             GetTestValuesForScanAndReadInteTest())));

}  // namespace paimon::test
