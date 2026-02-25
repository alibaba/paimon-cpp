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

#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/startup_mode.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class ScanInteTest : public testing::Test {
 public:
    std::vector<std::shared_ptr<DataSplitImpl>> CollectDataSplits(
        const std::shared_ptr<Plan>& plan) const {
        std::vector<std::shared_ptr<DataSplitImpl>> result_data_splits;
        for (const auto& result : plan->Splits()) {
            auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);
            EXPECT_TRUE(data_split);
            result_data_splits.push_back(data_split);
        }
        return result_data_splits;
    }

    void CheckResult(std::vector<std::shared_ptr<DataSplitImpl>> expected,
                     std::vector<std::shared_ptr<DataSplitImpl>> result) const {
        ASSERT_EQ(result.size(), expected.size());
        for (size_t i = 0; i < result.size(); i++) {
            ASSERT_EQ(*result[i], *expected[i]) << result[i]->ToString() << std::endl
                                                << expected[i]->ToString();
        }
    }

    void CheckStreamScanResult(TableScan* table_scan,
                               const std::vector<std::optional<int64_t>> expected_snapshot_ids,
                               const std::vector<std::vector<std::shared_ptr<DataSplitImpl>>>&
                                   expected_data_splits) const {
        size_t scan_id = 0;
        while (true) {
            ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
            if (scan_id == expected_snapshot_ids.size()) {
                // no snapshot
                ASSERT_EQ(std::nullopt, result_plan->SnapshotId());
                ASSERT_TRUE(result_plan->Splits().empty());
                return;
            }
            // check snapshot ids
            ASSERT_EQ(result_plan->SnapshotId(), expected_snapshot_ids[scan_id]);

            std::vector<std::shared_ptr<DataSplitImpl>> result_data_splits;
            for (const auto& result : result_plan->Splits()) {
                auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);
                ASSERT_TRUE(data_split);
                result_data_splits.push_back(data_split);
            }
            // check data splits
            CheckResult(expected_data_splits[scan_id], result_data_splits);
            scan_id++;
        }
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();

    std::shared_ptr<arrow::DataType> arrow_data_type_ =
        arrow::struct_({arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
                        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())});

    std::shared_ptr<DataFileMeta> meta_snapshot1_partition10_bucket0_ =
        std::make_shared<DataFileMeta>(
            "data-d41fd7d1-b3e4-4905-aad9-b20a780e90a2-0.orc", /*file_size=*/543, /*row_count=*/1,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            BinaryRowGenerator::GenerateStats({std::string("Alice"), 10, 1, 11.1},
                                              {std::string("Alice"), 10, 1, 11.1}, {0, 0, 0, 0},
                                              pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1721643142435ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);

    std::shared_ptr<DataFileMeta> meta_snapshot1_partition10_bucket1_ =
        std::make_shared<DataFileMeta>(
            "data-4e30d6c0-f109-4300-a010-4ba03047dd9d-0.orc", /*file_size=*/575, /*row_count=*/3,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            BinaryRowGenerator::GenerateStats({std::string("Bob"), 10, 0, 12.1},
                                              {std::string("Tony"), 10, 0, 14.1}, {0, 0, 0, 0},
                                              pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1721643142456ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);

    std::shared_ptr<DataFileMeta> meta_snapshot1_partition20_bucket0_ =
        std::make_shared<DataFileMeta>(
            "data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc", /*file_size=*/541, /*row_count=*/1,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            BinaryRowGenerator::GenerateStats({std::string("Lucy"), 20, 1, 14.1},
                                              {std::string("Lucy"), 20, 1, 14.1}, {0, 0, 0, 0},
                                              pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1721643142472ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);

    std::shared_ptr<DataFileMeta> meta_snapshot2_partition10_bucket1_ =
        std::make_shared<DataFileMeta>(
            "data-10b9eea8-241d-4e4b-8ab8-2a82d72d79a2-0.orc", /*file_size=*/589, /*row_count=*/3,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            BinaryRowGenerator::GenerateStats({std::string("Alex"), 10, 0, 12.1},
                                              {std::string("Emily"), 10, 0, 16.1}, {0, 0, 0, 0},
                                              pool_.get()),
            /*min_sequence_number=*/3, /*max_sequence_number=*/5, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1721643267385ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);

    std::shared_ptr<DataFileMeta> meta_snapshot2_partition20_bucket0_ =
        std::make_shared<DataFileMeta>(
            "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc", /*file_size=*/506, /*row_count=*/1,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            BinaryRowGenerator::GenerateStats({std::string("Paul"), 20, 1, NullType()},
                                              {std::string("Paul"), 20, 1, NullType()},
                                              {0, 0, 0, 1}, pool_.get()),
            /*min_sequence_number=*/1, /*max_sequence_number=*/1, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1721643267404ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);

    std::shared_ptr<DataFileMeta> meta_snapshot3_partition10_bucket1_ =
        std::make_shared<DataFileMeta>(
            "data-e2bb59ee-ae25-4e5b-9bcc-257250bc5fdd-0.orc", /*file_size=*/541, /*row_count=*/1,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            BinaryRowGenerator::GenerateStats({std::string("David"), 10, 0, 17.1},
                                              {std::string("David"), 10, 0, 17.1}, {0, 0, 0, 0},
                                              pool_.get()),
            /*min_sequence_number=*/6, /*max_sequence_number=*/6, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1721643314161ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);

    std::shared_ptr<DataFileMeta> meta_snapshot4_partition10_bucket1_ =
        std::make_shared<DataFileMeta>(
            "data-2d5ea1ea-77c1-47ff-bb87-19a509962a37-0.orc", /*file_size=*/538, /*row_count=*/1,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            BinaryRowGenerator::GenerateStats({std::string("Lily"), 10, 0, 17.1},
                                              {std::string("Lily"), 10, 0, 17.1}, {0, 0, 0, 0},
                                              pool_.get()),
            /*min_sequence_number=*/7, /*max_sequence_number=*/7, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1721643834400ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);

    std::shared_ptr<DataFileMeta> meta_snapshot5_partition10_bucket1_ =
        std::make_shared<DataFileMeta>(
            "data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc", /*file_size=*/640, /*row_count=*/8,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            BinaryRowGenerator::GenerateStats({std::string("Alex"), 10, 0, 12.1},
                                              {std::string("Tony"), 10, 0, 17.1}, {0, 0, 0, 0},
                                              pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/7, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1721643834472ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Compact(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
};

TEST_F(ScanInteTest, TestScanAppendWithSnapshot1) {
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check snapshot id
    ASSERT_EQ(result_plan->SnapshotId().value(), 1);

    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    DataSplitImpl::Builder builder1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-0",
        {meta_snapshot1_partition10_bucket0_});
    auto expected_data_split1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1.WithTotalBuckets(2)
                                                     .WithSnapshot(1)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder2(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot1_partition10_bucket1_});
    auto expected_data_split2 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder2.WithTotalBuckets(2)
                                                     .WithSnapshot(1)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder3(
        BinaryRowGenerator::GenerateRow({20}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=20/bucket-0",
        {meta_snapshot1_partition20_bucket0_});
    auto expected_data_split3 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder3.WithTotalBuckets(2)
                                                     .WithSnapshot(1)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {
        expected_data_split1, expected_data_split2, expected_data_split3};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithSnapshot3) {
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "3");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check snapshot id
    ASSERT_EQ(result_plan->SnapshotId().value(), 3);

    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    DataSplitImpl::Builder builder1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-0",
        {meta_snapshot1_partition10_bucket0_});
    auto expected_data_split1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1.WithTotalBuckets(2)
                                                     .WithSnapshot(3)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder2(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()), /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot1_partition10_bucket1_, meta_snapshot2_partition10_bucket1_,
         meta_snapshot3_partition10_bucket1_});
    auto expected_data_split2 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder2.WithTotalBuckets(2)
                                                     .WithSnapshot(3)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder3(
        BinaryRowGenerator::GenerateRow({20}, pool_.get()), /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=20/bucket-0",
        {meta_snapshot1_partition20_bucket0_, meta_snapshot2_partition20_bucket0_});
    auto expected_data_split3 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder3.WithTotalBuckets(2)
                                                     .WithSnapshot(3)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {
        expected_data_split1, expected_data_split2, expected_data_split3};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanInvalidSnapshot) {
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "100");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_NOK_WITH_MSG(
        table_scan->CreatePlan(),
        "The specified scan snapshotId 100 is out of available snapshotId range [1, 5].");
}

TEST_F(ScanInteTest, TestBatchScanMultipleTimes) {
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "1");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // batch scan multiple
    ASSERT_NOK_WITH_MSG(table_scan->CreatePlan(), "end of scan");
}

TEST_F(ScanInteTest, TestScanAppendWithSnapshot3WithSplitTargetSize) {
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "3")
        .AddOption(Options::SOURCE_SPLIT_OPEN_FILE_COST, "1024")
        .AddOption(Options::SOURCE_SPLIT_TARGET_SIZE, "2048");
    // open cost = 1024, and split target size is 2048, indicates at most 2 files in a split
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check snapshot id
    ASSERT_EQ(result_plan->SnapshotId().value(), 3);

    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    DataSplitImpl::Builder builder1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-0",
        {meta_snapshot1_partition10_bucket0_});
    auto expected_data_split1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1.WithTotalBuckets(2)
                                                     .WithSnapshot(3)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder2(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()), /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot1_partition10_bucket1_, meta_snapshot2_partition10_bucket1_});
    auto expected_data_split2 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder2.WithTotalBuckets(2)
                                                     .WithSnapshot(3)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder3(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot3_partition10_bucket1_});
    auto expected_data_split3 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder3.WithTotalBuckets(2)
                                                     .WithSnapshot(3)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder4(
        BinaryRowGenerator::GenerateRow({20}, pool_.get()), /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=20/bucket-0",
        {meta_snapshot1_partition20_bucket0_, meta_snapshot2_partition20_bucket0_});
    auto expected_data_split4 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder4.WithTotalBuckets(2)
                                                     .WithSnapshot(3)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {
        expected_data_split1, expected_data_split2, expected_data_split3, expected_data_split4};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithSnapshot3WithRowCountLimit) {
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "3").SetLimit(3);
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check snapshot id
    ASSERT_EQ(result_plan->SnapshotId().value(), 3);

    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    // since row limit is set to 3, we only return partition10, bucket0 and partition10, bucket1 in
    // plan (without partition20, bucket0)
    DataSplitImpl::Builder builder1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-0",
        {meta_snapshot1_partition10_bucket0_});
    auto expected_data_split1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1.WithTotalBuckets(2)
                                                     .WithSnapshot(3)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder3(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot1_partition10_bucket1_, meta_snapshot2_partition10_bucket1_,
         meta_snapshot3_partition10_bucket1_});
    auto expected_data_split3 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder3.WithTotalBuckets(2)
                                                     .WithSnapshot(3)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {expected_data_split1,
                                                                        expected_data_split3};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithSnapshot3WithBucketFilter) {
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";
    ScanContextBuilder context_builder(table_path);
    context_builder.SetBucketFilter(0).AddOption(Options::SCAN_SNAPSHOT_ID, "3");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check snapshot id
    ASSERT_EQ(result_plan->SnapshotId().value(), 3);

    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    DataSplitImpl::Builder builder1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-0",
        {meta_snapshot1_partition10_bucket0_});
    auto expected_data_split1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1.WithTotalBuckets(2)
                                                     .WithSnapshot(3)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder3(
        BinaryRowGenerator::GenerateRow({20}, pool_.get()), /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=20/bucket-0",
        {meta_snapshot1_partition20_bucket0_, meta_snapshot2_partition20_bucket0_});
    auto expected_data_split3 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder3.WithTotalBuckets(2)
                                                     .WithSnapshot(3)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {expected_data_split1,
                                                                        expected_data_split3};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithStreamWithDefaultMode) {
    // from snapshot is specified
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "1").WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    DataSplitImpl::Builder builder1_1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-0",
        {meta_snapshot1_partition10_bucket0_});
    auto expected_data_split1_1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1_1.WithTotalBuckets(2)
                                                     .WithSnapshot(1)
                                                     .IsStreaming(true)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder1_2(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot1_partition10_bucket1_});
    auto expected_data_split1_2 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1_2.WithTotalBuckets(2)
                                                     .WithSnapshot(1)
                                                     .IsStreaming(true)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder1_3(
        BinaryRowGenerator::GenerateRow({20}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=20/bucket-0",
        {meta_snapshot1_partition20_bucket0_});
    auto expected_data_split1_3 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1_3.WithTotalBuckets(2)
                                                     .WithSnapshot(1)
                                                     .IsStreaming(true)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    // second scan
    DataSplitImpl::Builder builder2_1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot2_partition10_bucket1_});
    auto expected_data_split2_1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder2_1.WithTotalBuckets(2)
                                                     .WithSnapshot(2)
                                                     .IsStreaming(true)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder2_2(
        BinaryRowGenerator::GenerateRow({20}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=20/bucket-0",
        {meta_snapshot2_partition20_bucket0_});
    auto expected_data_split2_2 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder2_2.WithTotalBuckets(2)
                                                     .WithSnapshot(2)
                                                     .IsStreaming(true)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    // third scan
    DataSplitImpl::Builder builder3_1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot3_partition10_bucket1_});
    auto expected_data_split3_1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder3_1.WithTotalBuckets(2)
                                                     .WithSnapshot(3)
                                                     .IsStreaming(true)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    // fourth scan
    DataSplitImpl::Builder builder4_1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot4_partition10_bucket1_});
    auto expected_data_split4_1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder4_1.WithTotalBuckets(2)
                                                     .WithSnapshot(4)
                                                     .IsStreaming(true)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    std::vector<std::vector<std::shared_ptr<DataSplitImpl>>> expected_data_splits = {
        {},
        {expected_data_split1_1, expected_data_split1_2, expected_data_split1_3},
        {expected_data_split2_1, expected_data_split2_2},
        {expected_data_split3_1},
        {expected_data_split4_1}};

    std::vector<std::optional<int64_t>> expected_snapshot_ids = {std::nullopt, 1, 2, 3, 4};
    CheckStreamScanResult(table_scan.get(), expected_snapshot_ids, expected_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithStreamOfLatestFullMode) {
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::SCAN_MODE, StartupMode::LatestFull().ToString())
        .WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    DataSplitImpl::Builder builder1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-0",
        {meta_snapshot1_partition10_bucket0_});
    auto expected_data_split1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1.WithTotalBuckets(2)
                                                     .WithSnapshot(5)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder2(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot5_partition10_bucket1_});
    auto expected_data_split2 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder2.WithTotalBuckets(2)
                                                     .WithSnapshot(5)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder3(
        BinaryRowGenerator::GenerateRow({20}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=20/bucket-0",
        {meta_snapshot1_partition20_bucket0_, meta_snapshot2_partition20_bucket0_});
    auto expected_data_split3 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder3.WithTotalBuckets(2)
                                                     .WithSnapshot(5)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    std::vector<std::vector<std::shared_ptr<DataSplitImpl>>> expected_data_splits = {
        {expected_data_split1, expected_data_split2, expected_data_split3}};

    std::vector<std::optional<int64_t>> expected_snapshot_ids = {5};
    CheckStreamScanResult(table_scan.get(), expected_snapshot_ids, expected_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithBatchScanOfLatestMode) {
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::SCAN_MODE, StartupMode::Latest().ToString());
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check snapshot id
    ASSERT_EQ(result_plan->SnapshotId().value(), 5);
    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    DataSplitImpl::Builder builder1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-0",
        {meta_snapshot1_partition10_bucket0_});
    auto expected_data_split1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1.WithTotalBuckets(2)
                                                     .WithSnapshot(5)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder2(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot5_partition10_bucket1_});
    auto expected_data_split2 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder2.WithTotalBuckets(2)
                                                     .WithSnapshot(5)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder3(
        BinaryRowGenerator::GenerateRow({20}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=20/bucket-0",
        {meta_snapshot1_partition20_bucket0_, meta_snapshot2_partition20_bucket0_});
    auto expected_data_split3 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder3.WithTotalBuckets(2)
                                                     .WithSnapshot(5)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {
        expected_data_split1, expected_data_split2, expected_data_split3};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithStreamOfLatestMode) {
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";

    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::SCAN_MODE, StartupMode::Latest().ToString())
        .WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    std::vector<std::optional<int64_t>> expected_snapshot_ids = {};
    CheckStreamScanResult(table_scan.get(), expected_snapshot_ids, {});
}

TEST_F(ScanInteTest, TestScanAppendWithStreamOfFromSnapshotMode) {
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";

    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::SCAN_MODE, StartupMode::FromSnapshot().ToString())
        .AddOption(Options::SCAN_SNAPSHOT_ID, "2")
        .WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    DataSplitImpl::Builder builder2_1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot2_partition10_bucket1_});
    auto expected_data_split2_1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder2_1.WithTotalBuckets(2)
                                                     .WithSnapshot(2)
                                                     .IsStreaming(true)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder2_2(
        BinaryRowGenerator::GenerateRow({20}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=20/bucket-0",
        {meta_snapshot2_partition20_bucket0_});
    auto expected_data_split2_2 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder2_2.WithTotalBuckets(2)
                                                     .WithSnapshot(2)
                                                     .IsStreaming(true)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder3_1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot3_partition10_bucket1_});
    auto expected_data_split3_1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder3_1.WithTotalBuckets(2)
                                                     .WithSnapshot(3)
                                                     .IsStreaming(true)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder4_1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot4_partition10_bucket1_});
    auto expected_data_split4_1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder4_1.WithTotalBuckets(2)
                                                     .WithSnapshot(4)
                                                     .IsStreaming(true)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    std::vector<std::vector<std::shared_ptr<DataSplitImpl>>> expected_data_splits = {
        {},
        {expected_data_split2_1, expected_data_split2_2},
        {expected_data_split3_1},
        {expected_data_split4_1}};

    std::vector<std::optional<int64_t>> expected_snapshot_ids = {std::nullopt, 2, 3, 4};
    CheckStreamScanResult(table_scan.get(), expected_snapshot_ids, expected_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithStreamOfFromSnapshotFullMode) {
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";

    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::SCAN_MODE, StartupMode::FromSnapshotFull().ToString())
        .AddOption(Options::SCAN_SNAPSHOT_ID, "2")
        .WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_TRUE(scan_context);
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    DataSplitImpl::Builder builder1_1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-0",
        {meta_snapshot1_partition10_bucket0_});
    auto expected_data_split1_1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1_1.WithTotalBuckets(2)
                                                     .WithSnapshot(2)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder1_2(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot1_partition10_bucket1_, meta_snapshot2_partition10_bucket1_});
    auto expected_data_split1_2 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1_2.WithTotalBuckets(2)
                                                     .WithSnapshot(2)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder1_3(
        BinaryRowGenerator::GenerateRow({20}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=20/bucket-0",
        {meta_snapshot1_partition20_bucket0_, meta_snapshot2_partition20_bucket0_});
    auto expected_data_split1_3 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1_3.WithTotalBuckets(2)
                                                     .WithSnapshot(2)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder3_1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot3_partition10_bucket1_});
    auto expected_data_split3_1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder3_1.WithTotalBuckets(2)
                                                     .WithSnapshot(3)
                                                     .IsStreaming(true)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder4_1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot4_partition10_bucket1_});
    auto expected_data_split4_1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder4_1.WithTotalBuckets(2)
                                                     .WithSnapshot(4)
                                                     .IsStreaming(true)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    std::vector<std::vector<std::shared_ptr<DataSplitImpl>>> expected_data_splits = {
        {expected_data_split1_1, expected_data_split1_2, expected_data_split1_3},
        {expected_data_split3_1},
        {expected_data_split4_1}};

    std::vector<std::optional<int64_t>> expected_snapshot_ids = {2, 3, 4};
    CheckStreamScanResult(table_scan.get(), expected_snapshot_ids, expected_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithInvalidOptions) {
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";
    {
        ScanContextBuilder context_builder(table_path);
        context_builder.AddOption(Options::SCAN_MODE, StartupMode::FromSnapshot().ToString())
            .WithStreamingMode(true);
        ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
        ASSERT_NOK_WITH_MSG(
            table_scan->CreatePlan(),
            "scan.snapshot-id or scan.tag-name must be set when startup mode is FROM_SNAPSHOT");
    }
    {
        ScanContextBuilder context_builder(table_path);
        context_builder.AddOption(Options::BUCKET, "-2").WithStreamingMode(true);
        ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)),
                            "do not support bucket=-2 in scan process");
    }
}

TEST_F(ScanInteTest, TestScanAppendWithSnapshot1WithEqualPredicate) {
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";

    std::string val("Bob");
    auto predicate =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                Literal(FieldType::STRING, val.data(), val.size()));

    ScanContextBuilder context_builder(table_path);
    context_builder.SetPredicate(predicate).AddOption(Options::SCAN_SNAPSHOT_ID, "1");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check snapshot id
    ASSERT_EQ(result_plan->SnapshotId().value(), 1);

    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    DataSplitImpl::Builder builder(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot1_partition10_bucket1_});
    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(builder.WithTotalBuckets(2)
                                                                            .WithSnapshot(1)
                                                                            .IsStreaming(false)
                                                                            .RawConvertible(true)
                                                                            .Build()
                                                                            .value());

    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {expected_data_split};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithStreamWithAndPredicate) {
    // from snapshot is specified
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";

    auto greater_than_predicate = PredicateBuilder::GreaterThan(
        /*field_index=*/3, /*field_name=*/"f3", FieldType::DOUBLE, Literal(13.1));
    std::string val("Paul");
    auto less_than_predicate = PredicateBuilder::LessThan(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
        Literal(FieldType::STRING, val.data(), val.size()));
    val = "David";
    auto not_in_predicate =
        PredicateBuilder::NotIn(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                {Literal(FieldType::STRING, val.data(), val.size())});
    auto not_equal = PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                FieldType::INT, {Literal(20)});
    ASSERT_OK_AND_ASSIGN(auto predicate,
                         PredicateBuilder::And({greater_than_predicate, less_than_predicate,
                                                not_in_predicate, not_equal}));

    ScanContextBuilder context_builder(table_path);
    context_builder.SetPredicate(predicate)
        .AddOption(Options::SCAN_SNAPSHOT_ID, "1")
        .WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    DataSplitImpl::Builder builder1_2(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot1_partition10_bucket1_});
    auto expected_data_split1_2 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1_2.WithTotalBuckets(2)
                                                     .WithSnapshot(1)
                                                     .IsStreaming(true)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    // second scan
    DataSplitImpl::Builder builder2_1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot2_partition10_bucket1_});
    auto expected_data_split2_1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder2_1.WithTotalBuckets(2)
                                                     .WithSnapshot(2)
                                                     .IsStreaming(true)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    // files in third scan is all filtered
    // fourth scan
    DataSplitImpl::Builder builder4_1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot4_partition10_bucket1_});
    auto expected_data_split4_1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder4_1.WithTotalBuckets(2)
                                                     .WithSnapshot(4)
                                                     .IsStreaming(true)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    std::vector<std::vector<std::shared_ptr<DataSplitImpl>>> expected_data_splits = {
        {}, {expected_data_split1_2}, {expected_data_split2_1}, {}, {expected_data_split4_1}};

    std::vector<std::optional<int64_t>> expected_snapshot_ids = {std::nullopt, 1, 2, 3, 4};
    CheckStreamScanResult(table_scan.get(), expected_snapshot_ids, expected_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithSnapshot1WithPartitionFilter) {
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";

    std::map<std::string, std::string> partition_keys;
    partition_keys["f1"] = "10";
    ScanContextBuilder context_builder(table_path);
    context_builder.SetPartitionFilter({partition_keys}).AddOption(Options::SCAN_SNAPSHOT_ID, "1");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check snapshot id
    ASSERT_EQ(result_plan->SnapshotId().value(), 1);

    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    DataSplitImpl::Builder builder1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-0",
        {meta_snapshot1_partition10_bucket0_});
    auto expected_data_split1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1.WithTotalBuckets(2)
                                                     .WithSnapshot(1)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder2(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot1_partition10_bucket1_});
    auto expected_data_split2 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder2.WithTotalBuckets(2)
                                                     .WithSnapshot(1)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {expected_data_split1,
                                                                        expected_data_split2};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithSnapshot1WithInvalidPartitionFilter) {
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";

    std::map<std::string, std::string> partition_keys;
    partition_keys["invalid_partition_key"] = "10";
    ScanContextBuilder context_builder(table_path);
    context_builder.SetPartitionFilter({partition_keys}).AddOption(Options::SCAN_SNAPSHOT_ID, "1");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)),
                        "field invalid_partition_key does not exist in partition keys");
}

TEST_F(ScanInteTest, TestScanAppendWithSnapshot1WithPartitionFilterAndPredicateFilter) {
    std::string table_path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";

    // set predicate filter, f1 = 20
    auto predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::INT,
                                             Literal(20));
    // set partition filter, f1 = 10
    std::map<std::string, std::string> partition_keys;
    partition_keys["f1"] = "10";

    ScanContextBuilder context_builder(table_path);
    context_builder.SetPredicate(predicate)
        .SetPartitionFilter({partition_keys})
        .AddOption(Options::SCAN_SNAPSHOT_ID, "1");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check snapshot id
    ASSERT_EQ(result_plan->SnapshotId().value(), 1);

    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    DataSplitImpl::Builder builder1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-0",
        {meta_snapshot1_partition10_bucket0_});
    auto expected_data_split1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1.WithTotalBuckets(2)
                                                     .WithSnapshot(1)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder2(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() + "orc/append_09.db/append_09/f1=10/bucket-1",
        {meta_snapshot1_partition10_bucket1_});
    auto expected_data_split2 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder2.WithTotalBuckets(2)
                                                     .WithSnapshot(1)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {expected_data_split1,
                                                                        expected_data_split2};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithSnapshot1WithMultiPartitionKeys) {
    std::string table_path = paimon::test::GetDataDir() +
                             "orc/multi_partition_append_table.db/multi_partition_append_table";

    // set partition filter, f1 = 10, f2 = 0
    std::map<std::string, std::string> partition_keys;
    partition_keys["f1"] = "10";
    partition_keys["f2"] = "0";

    ScanContextBuilder context_builder(table_path);
    context_builder.SetPartitionFilter({partition_keys}).AddOption(Options::SCAN_SNAPSHOT_ID, "1");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check snapshot id
    ASSERT_EQ(result_plan->SnapshotId().value(), 1);

    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    auto meta = std::make_shared<DataFileMeta>(
        "data-01b6a930-6564-409b-b8f4-ed1307790d72-0.orc", /*file_size=*/575, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Bob"), 10, 0, 12.1},
                                          {std::string("Tony"), 10, 0, 14.1}, {0, 0, 0, 0},
                                          pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1728497439433ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder1(
        BinaryRowGenerator::GenerateRow({10, 0}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() +
            "orc/multi_partition_append_table.db/multi_partition_append_table/f1=10/f2=0/bucket-0",
        {meta});
    auto expected_data_split1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1.WithTotalBuckets(-1)
                                                     .WithSnapshot(1)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {expected_data_split1};
    CheckResult(expected_data_splits, result_data_splits);
}

// test complex type ts & decimal
TEST_F(ScanInteTest, TestScanAppendComplexDataWithSnapshot4WithPredicateFilter) {
    std::string table_path =
        paimon::test::GetDataDir() + "orc/append_complex_data.db/append_complex_data";
    // set predicate filter
    // less than 2024
    auto predicate1 =
        PredicateBuilder::LessThan(/*field_index=*/3, /*field_name=*/"f4", FieldType::TIMESTAMP,
                                   Literal(paimon::Timestamp(1735344000, 0)));
    auto predicate2 = PredicateBuilder::GreaterThan(
        /*field_index=*/4, /*field_name=*/"f5", FieldType::DECIMAL,
        Literal(paimon::Decimal(5, 2, 0)));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({predicate1, predicate2}));

    ScanContextBuilder context_builder(table_path);
    context_builder.SetPredicate(predicate).AddOption(Options::SCAN_SNAPSHOT_ID, "4");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_TRUE(result_plan);
    // check snapshot id
    ASSERT_EQ(result_plan->SnapshotId().value(), 4);

    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    auto meta = std::make_shared<DataFileMeta>(
        "data-14a30421-7650-486c-9876-66a1fa4356ff-0.orc", /*file_size=*/1004, /*row_count=*/6,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats(
            {10, 1, 0, TimestampType(Timestamp(-2240521239999ll, 1001), 9),
             Decimal(23, 5, DecimalUtils::StrToInt128("-12345000").value()), NullType()},
            {10, 1, 20006, TimestampType(Timestamp(2000000000000ll, 1001), 9),
             Decimal(23, 5, DecimalUtils::StrToInt128("12345678998765432145678").value()),
             NullType()},
            {0, 0, 1, 0, 1, 1}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/5, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1767506722625ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Compact(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() +
            "orc/append_complex_data.db/append_complex_data/f1=10/bucket-0",
        {meta});
    auto expected_data_split1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1.WithTotalBuckets(2)
                                                     .WithSnapshot(4)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {expected_data_split1};
    CheckResult(expected_data_splits, result_data_splits);
}

// test complex type date & binary
TEST_F(ScanInteTest, TestScanAppendComplexDataWithSnapshot4WithPredicateFilter2) {
    std::string table_path =
        paimon::test::GetDataDir() + "orc/append_complex_data.db/append_complex_data";
    // set predicate filter
    auto predicate1 = PredicateBuilder::GreaterThan(/*field_index=*/2, /*field_name=*/"f3",
                                                    FieldType::DATE, Literal(FieldType::DATE, 0));
    // BINARY does not have stats in manifest, min/max in value stats is null
    // if row_count != null_count and min/max is null, file will not be filtered
    auto predicate2 = PredicateBuilder::GreaterThan(
        /*field_index=*/5, /*field_name=*/"f6", FieldType::BINARY,
        Literal(FieldType::BINARY, "zoo", 3));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({predicate1, predicate2}));

    ScanContextBuilder context_builder(table_path);
    context_builder.SetPredicate(predicate).AddOption(Options::SCAN_SNAPSHOT_ID, "4");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check snapshot id
    ASSERT_EQ(result_plan->SnapshotId().value(), 4);

    // check data splits
    std::vector<std::shared_ptr<DataSplitImpl>> result_data_splits;
    for (const auto& result : result_plan->Splits()) {
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(result);
        ASSERT_TRUE(data_split);
        result_data_splits.push_back(data_split);
    }

    auto meta = std::make_shared<DataFileMeta>(
        "data-14a30421-7650-486c-9876-66a1fa4356ff-0.orc", /*file_size=*/1004, /*row_count=*/6,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats(
            {10, 1, 0, TimestampType(Timestamp(-2240521239999ll, 1001), 9),
             Decimal(23, 5, DecimalUtils::StrToInt128("-12345000").value()), NullType()},
            {10, 1, 20006, TimestampType(Timestamp(2000000000000ll, 1001), 9),
             Decimal(23, 5, DecimalUtils::StrToInt128("12345678998765432145678").value()),
             NullType()},
            {0, 0, 1, 0, 1, 1}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/5, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1767506722625ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Compact(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() +
            "orc/append_complex_data.db/append_complex_data/f1=10/bucket-0",
        {meta});
    auto expected_data_split1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1.WithTotalBuckets(2)
                                                     .WithSnapshot(4)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {expected_data_split1};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithSnapshot1WithEnableStatsDenseStore) {
    std::string table_path = paimon::test::GetDataDir() +
                             "orc/append_10_stats_dense_store.db/append_10_stats_dense_store";
    ScanContextBuilder context_builder(table_path);
    auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f3",
                                                   FieldType::DOUBLE, Literal(13.0));
    context_builder.SetPredicate(predicate).AddOption(Options::SCAN_SNAPSHOT_ID, "1");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-cdb38c8a-31c1-4824-a024-9abd3fbb466f-0.orc", /*file_size=*/543, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Alice"), 10, 1},
                                          {std::string("Alice"), 10, 1}, {0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1731412938869ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::optional<std::vector<std::string>>({"f0", "f1", "f2"}),
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-c2613568-0412-4cd9-a0c4-1eae8e4ca89b-0.orc", /*file_size=*/575, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Bob"), 10, 0}, {std::string("Tony"), 10, 0},
                                          {0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1731412938891ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::optional<std::vector<std::string>>({"f0", "f1", "f2"}),
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    auto file_meta3 = std::make_shared<DataFileMeta>(
        "data-a6d1261a-f798-4fbd-a251-6d6c7d8060dd-0.orc", /*file_size=*/541, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Lucy"), 20, 1},
                                          {std::string("Lucy"), 20, 1}, {0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1731412938908ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::optional<std::vector<std::string>>({"f0", "f1", "f2"}),
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder1(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() +
            "orc/append_10_stats_dense_store.db/append_10_stats_dense_store/f1=10/bucket-0",
        {file_meta1});
    auto expected_data_split1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1.WithTotalBuckets(2)
                                                     .WithSnapshot(1)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());

    DataSplitImpl::Builder builder2(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() +
            "orc/append_10_stats_dense_store.db/append_10_stats_dense_store/f1=10/bucket-1",
        {file_meta2});
    auto expected_data_split2 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder2.WithTotalBuckets(2)
                                                     .WithSnapshot(1)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());
    DataSplitImpl::Builder builder3(
        BinaryRowGenerator::GenerateRow({20}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() +
            "orc/append_10_stats_dense_store.db/append_10_stats_dense_store/f1=20/bucket-0",
        {file_meta3});
    auto expected_data_split3 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder3.WithTotalBuckets(2)
                                                     .WithSnapshot(1)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());
    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {
        expected_data_split1, expected_data_split2, expected_data_split3};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithSnapshot1WithEnableStatsDenseStore2) {
    std::string table_path = paimon::test::GetDataDir() +
                             "orc/append_10_stats_dense_store.db/append_10_stats_dense_store";
    ScanContextBuilder context_builder(table_path);
    auto greater_than = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f3",
                                                      FieldType::DOUBLE, Literal(13.0));
    auto equal = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                         Literal(FieldType::STRING, "Emily", 5));
    auto predicate = PredicateBuilder::And({greater_than, equal}).value();

    context_builder.SetPredicate(predicate).AddOption(Options::SCAN_SNAPSHOT_ID, "1");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-c2613568-0412-4cd9-a0c4-1eae8e4ca89b-0.orc", /*file_size=*/575, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Bob"), 10, 0}, {std::string("Tony"), 10, 0},
                                          {0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1731412938891ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::optional<std::vector<std::string>>({"f0", "f1", "f2"}),
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder(
        BinaryRowGenerator::GenerateRow({10}, pool_.get()),
        /*bucket=*/1, /*bucket_path=*/
        paimon::test::GetDataDir() +
            "orc/append_10_stats_dense_store.db/append_10_stats_dense_store/f1=10/bucket-1",
        {file_meta});
    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(builder.WithTotalBuckets(2)
                                                                            .WithSnapshot(1)
                                                                            .IsStreaming(false)
                                                                            .RawConvertible(true)
                                                                            .Build()
                                                                            .value());
    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {expected_data_split};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanPKWithSnapshot1WithBucketStats) {
    std::string table_path = paimon::test::GetDataDir() +
                             "orc/pk_table_with_total_buckets.db/pk_table_with_total_buckets";
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "1").SetBucketFilter(2);
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 1);
    ASSERT_TRUE(result_plan->Splits().empty());
}

TEST_F(ScanInteTest, TestScanPKWithInvalidOptions) {
    std::string table_path = paimon::test::GetDataDir() +
                             "orc/pk_table_with_total_buckets.db/pk_table_with_total_buckets";
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::BUCKET, "-1").WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)),
                        "do not support pk table bucket=-1 in scan process");
}

TEST_F(ScanInteTest, TestReadWithNoSnapshot) {
    std::string table_path = paimon::test::GetDataDir() +
                             "orc/append_table_with_nested_type.db/append_table_with_nested_type";
    ScanContextBuilder context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_FALSE(result_plan->SnapshotId());
    ASSERT_TRUE(result_plan->Splits().empty());
}

TEST_F(ScanInteTest, TestScanAppendWithAlterTableWithCast) {
    std::string table_path =
        paimon::test::GetDataDir() +
        "orc/append_table_alter_table_with_cast.db/append_table_alter_table_with_cast";
    ScanContextBuilder context_builder(table_path);

    auto child1 =
        PredicateBuilder::Or(
            {PredicateBuilder::IsNotNull(/*field_index=*/0, /*field_name=*/"f4",
                                         FieldType::TIMESTAMP),
             PredicateBuilder::IsNotNull(/*field_index=*/1, /*field_name=*/"key0", FieldType::INT),
             PredicateBuilder::IsNotNull(/*field_index=*/2, /*field_name=*/"key1", FieldType::INT)})
            .value();

    auto sub_child1 =
        PredicateBuilder::IsNotNull(/*field_index=*/3, /*field_name=*/"f3", FieldType::INT);
    auto sub_child2 =
        PredicateBuilder::IsNotNull(/*field_index=*/4, /*field_name=*/"f1", FieldType::STRING);
    auto sub_child3 =
        PredicateBuilder::IsNotNull(/*field_index=*/5, /*field_name=*/"f2", FieldType::DECIMAL);
    auto child2 = PredicateBuilder::And({sub_child1, sub_child2, sub_child3}).value();

    auto child3 = PredicateBuilder::GreaterThan(/*field_index=*/7, /*field_name=*/"f6",
                                                FieldType::INT, Literal(80));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({child1, child2, child3}));

    context_builder.SetPredicate(predicate).AddOption(Options::SCAN_SNAPSHOT_ID, "2");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-81a1c016-765b-48c9-b209-0d8e95bf8a00-0.orc", /*file_size=*/1070, /*row_count=*/2,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats(
            {TimestampType(Timestamp(1732603136084l, 84), 9), 1, 1, 180,
             std::string("2024-11-26 15:29"), Decimal(6, 3, -999420), false, -86},
            {TimestampType(Timestamp(1732603136094l, 94), 9), 1, 1, 190, std::string("I'm strange"),
             Decimal(6, 3, 8032), true, 96},
            {0, 0, 0, 0, 0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/1, /*schema_id=*/1,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1732635461460ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder(BinaryRowGenerator::GenerateRow({1, 1}, pool_.get()),
                                   /*bucket=*/0, /*bucket_path=*/
                                   paimon::test::GetDataDir() +
                                       "orc/append_table_alter_table_with_cast.db/"
                                       "append_table_alter_table_with_cast/key0=1/key1=1/bucket-0",
                                   {file_meta});
    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(builder.WithTotalBuckets(-1)
                                                                            .WithSnapshot(2)
                                                                            .IsStreaming(false)
                                                                            .RawConvertible(true)
                                                                            .Build()
                                                                            .value());
    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {expected_data_split};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithAlterTableWithNoCast) {
    std::string table_path = paimon::test::GetDataDir() +
                             "orc/append_table_with_alter_table.db/append_table_with_alter_table";
    ScanContextBuilder context_builder(table_path);

    auto child1 = PredicateBuilder::GreaterThan(/*field_index=*/2, /*field_name=*/"k",
                                                FieldType::INT, Literal(36));
    auto child2 = PredicateBuilder::LessThan(/*field_index=*/2, /*field_name=*/"k", FieldType::INT,
                                             Literal(96));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({child1, child2}));

    context_builder.SetPredicate(predicate).AddOption(Options::SCAN_SNAPSHOT_ID, "2");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-492ed5ab-4740-4e93-8a0a-79a6893b1770-0.orc", /*file_size=*/603, /*row_count=*/2,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({1, 1, 42, 43, 44, 45, 46}, {1, 1, 52, 53, 54, 55, 56},
                                          {0, 0, 0, 0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/1, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1730458825047ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder1(BinaryRowGenerator::GenerateRow({1, 1}, pool_.get()),
                                    /*bucket=*/0, /*bucket_path=*/
                                    paimon::test::GetDataDir() +
                                        "orc/append_table_with_alter_table.db/"
                                        "append_table_with_alter_table/key0=1/key1=1/bucket-0",
                                    {file_meta1});
    auto expected_data_split1 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder1.WithTotalBuckets(-1)
                                                     .WithSnapshot(2)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());
    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-b34cd128-03e3-4e70-ba9c-5dec2183849c-0.orc", /*file_size=*/680, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({0, 1, 66, 63, 517, 65, 618},
                                          {0, 1, 86, 83, 537, 85, 638}, {0, 0, 0, 0, 0, 0, 0},
                                          pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/1,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1730459969493ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder2(BinaryRowGenerator::GenerateRow({0, 1}, pool_.get()),
                                    /*bucket=*/0, /*bucket_path=*/
                                    paimon::test::GetDataDir() +
                                        "orc/append_table_with_alter_table.db/"
                                        "append_table_with_alter_table/key0=0/key1=1/bucket-0",
                                    {file_meta2});
    auto expected_data_split2 =
        std::dynamic_pointer_cast<DataSplitImpl>(builder2.WithTotalBuckets(-1)
                                                     .WithSnapshot(2)
                                                     .IsStreaming(false)
                                                     .RawConvertible(true)
                                                     .Build()
                                                     .value());
    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {expected_data_split1,
                                                                        expected_data_split2};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithAlterTableWithDenseField) {
    std::string table_path = paimon::test::GetDataDir() +
                             "orc/append_table_with_alter_table_with_dense_field.db/"
                             "append_table_with_alter_table_with_dense_field";
    ScanContextBuilder context_builder(table_path);

    auto child1 = PredicateBuilder::GreaterThan(/*field_index=*/2, /*field_name=*/"f1",
                                                FieldType::INT, Literal(22));
    auto child3 = PredicateBuilder::LessThan(/*field_index=*/3, /*field_name=*/"f3",
                                             FieldType::DOUBLE, Literal(0.0));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({child1, child3}));

    context_builder.SetPredicate(predicate).AddOption(Options::SCAN_SNAPSHOT_ID, "2");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-e925c7db-58e3-45e3-b21e-b1a7774a5caf-0.orc", /*file_size=*/682, /*row_count=*/2,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({1, std::string("Cathy"), 13, 23},
                                          {1, std::string("David"), 14, 24}, {0, 0, 0, 0},
                                          pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/1, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1751647880163ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::optional<std::vector<std::string>>({"key0", "f0", "f1", "f2"}),
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(
        BinaryRowGenerator::GenerateRow({1}, pool_.get()),
        /*bucket=*/0, /*bucket_path=*/
        paimon::test::GetDataDir() +
            "orc/append_table_with_alter_table_with_dense_field.db/"
            "append_table_with_alter_table_with_dense_field/key0=1/bucket-0",
        {file_meta});
    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(builder.WithTotalBuckets(-1)
                                                                            .WithSnapshot(2)
                                                                            .IsStreaming(false)
                                                                            .RawConvertible(true)
                                                                            .Build()
                                                                            .value());
    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {expected_data_split};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithBitmapEmbeddedIndex) {
    std::string table_path =
        paimon::test::GetDataDir() + "orc/append_with_bitmap.db/append_with_bitmap/";
    ScanContextBuilder context_builder(table_path);

    auto child1 = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                          Literal(FieldType::STRING, "Tony", 4));
    auto child2 = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::INT,
                                          Literal(10));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({child1, child2}));

    context_builder.SetPredicate(predicate).AddOption(Options::SCAN_SNAPSHOT_ID, "1");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    std::vector<uint8_t> embedded_bytes = {
        0,  5,   78,  78,  208, 26,  53,  174, 0,   0,   0,   1,   0,   0,   0,   96,  0,   0,
        0,  3,   0,   2,   102, 48,  0,   0,   0,   1,   0,   6,   98,  105, 116, 109, 97,  112,
        0,  0,   0,   96,  0,   0,   0,   176, 0,   2,   102, 49,  0,   0,   0,   1,   0,   6,
        98, 105, 116, 109, 97,  112, 0,   0,   1,   16,  0,   0,   0,   102, 0,   2,   102, 50,
        0,  0,   0,   1,   0,   6,   98,  105, 116, 109, 97,  112, 0,   0,   1,   118, 0,   0,
        0,  108, 0,   0,   0,   0,   2,   0,   0,   0,   8,   0,   0,   0,   5,   0,   0,   0,
        0,  1,   0,   0,   0,   5,   65,  108, 105, 99,  101, 0,   0,   0,   0,   0,   0,   0,
        85, 0,   0,   0,   5,   0,   0,   0,   5,   65,  108, 105, 99,  101, 0,   0,   0,   0,
        0,  0,   0,   20,  0,   0,   0,   3,   66,  111, 98,  0,   0,   0,   20,  0,   0,   0,
        20, 0,   0,   0,   5,   69,  109, 105, 108, 121, 255, 255, 255, 253, 255, 255, 255, 255,
        0,  0,   0,   4,   76,  117, 99,  121, 255, 255, 255, 251, 255, 255, 255, 255, 0,   0,
        0,  4,   84,  111, 110, 121, 0,   0,   0,   40,  0,   0,   0,   20,  58,  48,  0,   0,
        1,  0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   0,   0,   7,   0,   58,  48,
        0,  0,   1,   0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   1,   0,   5,   0,
        58, 48,  0,   0,   1,   0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   3,   0,
        6,  0,   2,   0,   0,   0,   8,   0,   0,   0,   2,   0,   0,   0,   0,   1,   0,   0,
        0,  10,  0,   0,   0,   0,   0,   0,   0,   28,  0,   0,   0,   2,   0,   0,   0,   10,
        0,  0,   0,   22,  0,   0,   0,   26,  0,   0,   0,   20,  0,   0,   0,   0,   0,   0,
        0,  22,  58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   2,   0,   16,  0,   0,   0,
        4,  0,   6,   0,   7,   0,   58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   4,   0,
        16, 0,   0,   0,   0,   0,   1,   0,   2,   0,   3,   0,   5,   0,   2,   0,   0,   0,
        8,  0,   0,   0,   2,   1,   255, 255, 255, 248, 0,   0,   0,   18,  0,   0,   0,   1,
        0,  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   28,  0,   0,   0,   2,   0,   0,
        0,  0,   0,   0,   0,   0,   0,   0,   0,   22,  0,   0,   0,   1,   0,   0,   0,   22,
        0,  0,   0,   24,  58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   2,   0,   16,  0,
        0,  0,   2,   0,   3,   0,   6,   0,   58,  48,  0,   0,   1,   0,   0,   0,   0,   0,
        3,  0,   16,  0,   0,   0,   0,   0,   1,   0,   4,   0,   5,   0};
    auto embedded_index = std::make_shared<Bytes>(embedded_bytes.size(), pool_.get());
    memcpy(embedded_index->data(), reinterpret_cast<const void*>(embedded_bytes.data()),
           embedded_bytes.size());

    auto file_meta = std::make_shared<DataFileMeta>(
        "data-62feb610-c83f-4217-9b50-bbad9cd08eb4-0.orc", /*file_size=*/689,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Alice"), 10, 0, 11.1},
                                          {std::string("Tony"), 20, 1, 18.1}, {0, 0, 1, 0},
                                          pool_.get()),
        /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1745000702835ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/embedded_index, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/table_path + "bucket-0", {file_meta});
    ASSERT_OK_AND_ASSIGN(auto expected_data_split, builder.WithTotalBuckets(-1)
                                                       .WithSnapshot(1)
                                                       .IsStreaming(false)
                                                       .RawConvertible(true)
                                                       .Build());
    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {expected_data_split};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithBitmapEmbeddedIndexWithEmptyResult) {
    std::string table_path =
        paimon::test::GetDataDir() + "orc/append_with_bitmap.db/append_with_bitmap/";
    ScanContextBuilder context_builder(table_path);

    auto child1 = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                          Literal(FieldType::STRING, "Lucy", 4));
    auto child2 = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::INT,
                                          Literal(10));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({child1, child2}));

    context_builder.SetPredicate(predicate).AddOption(Options::SCAN_SNAPSHOT_ID, "1");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_TRUE(result_plan->Splits().empty());
}

TEST_F(ScanInteTest, TestScanAppendWithBitmapNoEmbeddedIndex) {
    std::string table_path =
        paimon::test::GetDataDir() +
        "orc/append_with_bitmap_no_embedding.db/append_with_bitmap_no_embedding/";
    ScanContextBuilder context_builder(table_path);

    auto child1 = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                          Literal(FieldType::STRING, "Lucy", 4));
    auto child2 = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::INT,
                                          Literal(10));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({child1, child2}));

    context_builder.SetPredicate(predicate).AddOption(Options::SCAN_SNAPSHOT_ID, "1");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-414509f5-e40c-4245-b992-bbf486778ac9-0.orc", /*file_size=*/689,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Alice"), 10, 0, 11.1},
                                          {std::string("Tony"), 20, 1, 18.1}, {0, 0, 1, 0},
                                          pool_.get()),
        /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/
        std::vector<std::optional<std::string>>(
            {"data-414509f5-e40c-4245-b992-bbf486778ac9-0.orc.index"}),
        /*creation_time=*/Timestamp(1745235371029ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/table_path + "bucket-0", {file_meta});
    ASSERT_OK_AND_ASSIGN(auto expected_data_split, builder.WithTotalBuckets(-1)
                                                       .WithSnapshot(1)
                                                       .IsStreaming(false)
                                                       .RawConvertible(true)
                                                       .Build());
    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {expected_data_split};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithBitmapAndAlterTable) {
    std::string table_path =
        paimon::test::GetDataDir() +
        "orc/append_with_bitmap_alter_table.db/append_with_bitmap_alter_table/";
    ScanContextBuilder context_builder(table_path);
    // file0 will be removed as f5 not exists
    auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f5",
                                                   FieldType::INT, Literal(100));

    context_builder.SetPredicate(predicate).AddOption(Options::SCAN_SNAPSHOT_ID, "2");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());

    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    std::vector<uint8_t> embedded_bytes = {
        0,   5,   78,  78,  208, 26,  53,  174, 0,   0,   0,   1,   0,   0,   0,   96,  0,   0,
        0,   3,   0,   2,   102, 49,  0,   0,   0,   1,   0,   6,   98,  105, 116, 109, 97,  112,
        0,   0,   0,   96,  0,   0,   0,   102, 0,   2,   102, 52,  0,   0,   0,   1,   0,   6,
        98,  105, 116, 109, 97,  112, 0,   0,   0,   198, 0,   0,   0,   101, 0,   2,   102, 53,
        0,   0,   0,   1,   0,   6,   98,  105, 116, 109, 97,  112, 0,   0,   1,   43,  0,   0,
        0,   82,  0,   0,   0,   0,   2,   0,   0,   0,   4,   0,   0,   0,   3,   0,   0,   0,
        0,   1,   0,   0,   0,   0,   0,   0,   0,   10,  0,   0,   0,   0,   0,   0,   0,   52,
        0,   0,   0,   3,   0,   0,   0,   0,   0,   0,   0,   10,  255, 255, 255, 253, 255, 255,
        255, 255, 0,   0,   0,   0,   0,   0,   0,   20,  255, 255, 255, 254, 255, 255, 255, 255,
        0,   0,   0,   0,   0,   0,   0,   30,  0,   0,   0,   0,   0,   0,   0,   20,  58,  48,
        0,   0,   1,   0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   0,   0,   3,   0,
        2,   0,   0,   0,   4,   0,   0,   0,   4,   0,   0,   0,   0,   1,   0,   0,   0,   5,
        65,  108, 105, 99,  101, 0,   0,   0,   0,   0,   0,   0,   70,  0,   0,   0,   4,   0,
        0,   0,   5,   65,  108, 105, 99,  101, 255, 255, 255, 255, 255, 255, 255, 255, 0,   0,
        0,   3,   66,  111, 98,  255, 255, 255, 253, 255, 255, 255, 255, 0,   0,   0,   5,   68,
        97,  118, 105, 100, 255, 255, 255, 252, 255, 255, 255, 255, 0,   0,   0,   5,   69,  109,
        105, 108, 121, 255, 255, 255, 254, 255, 255, 255, 255, 2,   0,   0,   0,   4,   0,   0,
        0,   2,   1,   255, 255, 255, 252, 0,   0,   0,   18,  0,   0,   0,   1,   0,   0,   0,
        100, 0,   0,   0,   0,   0,   0,   0,   28,  0,   0,   0,   2,   0,   0,   0,   100, 0,
        0,   0,   0,   0,   0,   0,   20,  0,   0,   0,   101, 255, 255, 255, 254, 255, 255, 255,
        255, 58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   0,
        0,   2,   0};
    auto embedded_index = std::make_shared<Bytes>(embedded_bytes.size(), pool_.get());
    memcpy(embedded_index->data(), reinterpret_cast<const void*>(embedded_bytes.data()),
           embedded_bytes.size());
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-a29b7235-760d-4838-881c-39cbef585dd2-0.orc", /*file_size=*/666,
        /*row_count=*/4, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({10, std::string("Alice"), 21.1, 100},
                                          {30, std::string("Emily"), 24.1, 101}, {0, 0, 0, 1},
                                          pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/3, /*schema_id=*/1, /*level=*/0,
        /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1745253323731ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/embedded_index, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/table_path + "bucket-0", {file_meta});
    ASSERT_OK_AND_ASSIGN(auto expected_data_split, builder.WithTotalBuckets(-1)
                                                       .WithSnapshot(2)
                                                       .IsStreaming(false)
                                                       .RawConvertible(true)
                                                       .Build());
    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {expected_data_split};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithBitmapAndAlterTable3) {
    std::string table_path =
        paimon::test::GetDataDir() +
        "orc/append_with_bitmap_alter_table.db/append_with_bitmap_alter_table/";
    ScanContextBuilder context_builder(table_path);
    auto child1 = PredicateBuilder::IsNull(/*field_index=*/3, /*field_name=*/"f5", FieldType::INT);
    auto child2 = PredicateBuilder::LessThan(/*field_index=*/2, /*field_name=*/"f3",
                                             FieldType::DOUBLE, Literal(20.0));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({child1, child2}));

    context_builder.SetPredicate(predicate).AddOption(Options::SCAN_SNAPSHOT_ID, "2");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());

    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    std::vector<uint8_t> embedded_bytes = {
        0,  5,   78,  78,  208, 26,  53,  174, 0,   0,   0,   1,   0,   0,   0,   96,  0,   0,
        0,  3,   0,   2,   102, 48,  0,   0,   0,   1,   0,   6,   98,  105, 116, 109, 97,  112,
        0,  0,   0,   96,  0,   0,   0,   176, 0,   2,   102, 49,  0,   0,   0,   1,   0,   6,
        98, 105, 116, 109, 97,  112, 0,   0,   1,   16,  0,   0,   0,   102, 0,   2,   102, 50,
        0,  0,   0,   1,   0,   6,   98,  105, 116, 109, 97,  112, 0,   0,   1,   118, 0,   0,
        0,  108, 0,   0,   0,   0,   2,   0,   0,   0,   8,   0,   0,   0,   5,   0,   0,   0,
        0,  1,   0,   0,   0,   5,   65,  108, 105, 99,  101, 0,   0,   0,   0,   0,   0,   0,
        85, 0,   0,   0,   5,   0,   0,   0,   5,   65,  108, 105, 99,  101, 0,   0,   0,   0,
        0,  0,   0,   20,  0,   0,   0,   3,   66,  111, 98,  0,   0,   0,   20,  0,   0,   0,
        20, 0,   0,   0,   5,   69,  109, 105, 108, 121, 255, 255, 255, 253, 255, 255, 255, 255,
        0,  0,   0,   4,   76,  117, 99,  121, 255, 255, 255, 251, 255, 255, 255, 255, 0,   0,
        0,  4,   84,  111, 110, 121, 0,   0,   0,   40,  0,   0,   0,   20,  58,  48,  0,   0,
        1,  0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   0,   0,   7,   0,   58,  48,
        0,  0,   1,   0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   1,   0,   5,   0,
        58, 48,  0,   0,   1,   0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   3,   0,
        6,  0,   2,   0,   0,   0,   8,   0,   0,   0,   2,   0,   0,   0,   0,   1,   0,   0,
        0,  10,  0,   0,   0,   0,   0,   0,   0,   28,  0,   0,   0,   2,   0,   0,   0,   10,
        0,  0,   0,   22,  0,   0,   0,   26,  0,   0,   0,   20,  0,   0,   0,   0,   0,   0,
        0,  22,  58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   2,   0,   16,  0,   0,   0,
        4,  0,   6,   0,   7,   0,   58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   4,   0,
        16, 0,   0,   0,   0,   0,   1,   0,   2,   0,   3,   0,   5,   0,   2,   0,   0,   0,
        8,  0,   0,   0,   2,   1,   255, 255, 255, 248, 0,   0,   0,   18,  0,   0,   0,   1,
        0,  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   28,  0,   0,   0,   2,   0,   0,
        0,  0,   0,   0,   0,   0,   0,   0,   0,   22,  0,   0,   0,   1,   0,   0,   0,   22,
        0,  0,   0,   24,  58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   2,   0,   16,  0,
        0,  0,   2,   0,   3,   0,   6,   0,   58,  48,  0,   0,   1,   0,   0,   0,   0,   0,
        3,  0,   16,  0,   0,   0,   0,   0,   1,   0,   4,   0,   5,   0};
    auto embedded_index = std::make_shared<Bytes>(embedded_bytes.size(), pool_.get());
    memcpy(embedded_index->data(), reinterpret_cast<const void*>(embedded_bytes.data()),
           embedded_bytes.size());
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-68014988-5451-478f-a18a-a1668214cf3d-0.orc", /*file_size=*/689,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Alice"), 10, 0, 11.1},
                                          {std::string("Tony"), 20, 1, 18.1}, {0, 0, 1, 0},
                                          pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/7, /*schema_id=*/0, /*level=*/0,
        /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1745251357742ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/embedded_index, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/table_path + "bucket-0", {file_meta});
    ASSERT_OK_AND_ASSIGN(auto expected_data_split, builder.WithTotalBuckets(-1)
                                                       .WithSnapshot(2)
                                                       .IsStreaming(false)
                                                       .RawConvertible(true)
                                                       .Build());
    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {expected_data_split};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithBitmapAndAlterTable2) {
    std::string table_path =
        paimon::test::GetDataDir() +
        "orc/append_with_bitmap_alter_table.db/append_with_bitmap_alter_table/";
    ScanContextBuilder context_builder(table_path);

    // in stats filter: predicate is trimmed as type for f1 is not consist: int->bigint
    // in index filter: predicate is removed as type for f1 is converted bigint->int, which is not
    // safe
    auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f1",
                                                   FieldType::BIGINT, Literal(100l));

    context_builder.SetPredicate(predicate).AddOption(Options::SCAN_SNAPSHOT_ID, "2");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());

    // check data splits
    auto result_data_splits = CollectDataSplits(result_plan);
    std::vector<uint8_t> embedded_bytes = {
        0,  5,   78,  78,  208, 26,  53,  174, 0,   0,   0,   1,   0,   0,   0,   96,  0,   0,
        0,  3,   0,   2,   102, 48,  0,   0,   0,   1,   0,   6,   98,  105, 116, 109, 97,  112,
        0,  0,   0,   96,  0,   0,   0,   176, 0,   2,   102, 49,  0,   0,   0,   1,   0,   6,
        98, 105, 116, 109, 97,  112, 0,   0,   1,   16,  0,   0,   0,   102, 0,   2,   102, 50,
        0,  0,   0,   1,   0,   6,   98,  105, 116, 109, 97,  112, 0,   0,   1,   118, 0,   0,
        0,  108, 0,   0,   0,   0,   2,   0,   0,   0,   8,   0,   0,   0,   5,   0,   0,   0,
        0,  1,   0,   0,   0,   5,   65,  108, 105, 99,  101, 0,   0,   0,   0,   0,   0,   0,
        85, 0,   0,   0,   5,   0,   0,   0,   5,   65,  108, 105, 99,  101, 0,   0,   0,   0,
        0,  0,   0,   20,  0,   0,   0,   3,   66,  111, 98,  0,   0,   0,   20,  0,   0,   0,
        20, 0,   0,   0,   5,   69,  109, 105, 108, 121, 255, 255, 255, 253, 255, 255, 255, 255,
        0,  0,   0,   4,   76,  117, 99,  121, 255, 255, 255, 251, 255, 255, 255, 255, 0,   0,
        0,  4,   84,  111, 110, 121, 0,   0,   0,   40,  0,   0,   0,   20,  58,  48,  0,   0,
        1,  0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   0,   0,   7,   0,   58,  48,
        0,  0,   1,   0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   1,   0,   5,   0,
        58, 48,  0,   0,   1,   0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   3,   0,
        6,  0,   2,   0,   0,   0,   8,   0,   0,   0,   2,   0,   0,   0,   0,   1,   0,   0,
        0,  10,  0,   0,   0,   0,   0,   0,   0,   28,  0,   0,   0,   2,   0,   0,   0,   10,
        0,  0,   0,   22,  0,   0,   0,   26,  0,   0,   0,   20,  0,   0,   0,   0,   0,   0,
        0,  22,  58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   2,   0,   16,  0,   0,   0,
        4,  0,   6,   0,   7,   0,   58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   4,   0,
        16, 0,   0,   0,   0,   0,   1,   0,   2,   0,   3,   0,   5,   0,   2,   0,   0,   0,
        8,  0,   0,   0,   2,   1,   255, 255, 255, 248, 0,   0,   0,   18,  0,   0,   0,   1,
        0,  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   28,  0,   0,   0,   2,   0,   0,
        0,  0,   0,   0,   0,   0,   0,   0,   0,   22,  0,   0,   0,   1,   0,   0,   0,   22,
        0,  0,   0,   24,  58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   2,   0,   16,  0,
        0,  0,   2,   0,   3,   0,   6,   0,   58,  48,  0,   0,   1,   0,   0,   0,   0,   0,
        3,  0,   16,  0,   0,   0,   0,   0,   1,   0,   4,   0,   5,   0};
    auto embedded_index = std::make_shared<Bytes>(embedded_bytes.size(), pool_.get());
    memcpy(embedded_index->data(), reinterpret_cast<const void*>(embedded_bytes.data()),
           embedded_bytes.size());
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-68014988-5451-478f-a18a-a1668214cf3d-0.orc", /*file_size=*/689,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Alice"), 10, 0, 11.1},
                                          {std::string("Tony"), 20, 1, 18.1}, {0, 0, 1, 0},
                                          pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/7, /*schema_id=*/0, /*level=*/0,
        /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1745251357742ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/embedded_index, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/table_path + "bucket-0", {file_meta});
    ASSERT_OK_AND_ASSIGN(auto expected_data_split, builder.WithTotalBuckets(-1)
                                                       .WithSnapshot(2)
                                                       .IsStreaming(false)
                                                       .RawConvertible(true)
                                                       .Build());
    std::vector<std::shared_ptr<DataSplitImpl>> expected_data_splits = {expected_data_split};
    CheckResult(expected_data_splits, result_data_splits);
}

TEST_F(ScanInteTest, TestScanAppendWithBitmapAndAlterTableWithEmptyResult) {
    std::string table_path =
        paimon::test::GetDataDir() +
        "orc/append_with_bitmap_alter_table.db/append_with_bitmap_alter_table/";
    ScanContextBuilder context_builder(table_path);

    // child1 will remove file1 for schema1
    auto child1 = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f1", FieldType::BIGINT,
                                          Literal(100l));
    // child2 will remove file0 for schema 0
    auto child2 = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f4", FieldType::STRING,
                                          Literal(FieldType::STRING, "David", 5));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({child1, child2}));

    context_builder.SetPredicate(predicate).AddOption(Options::SCAN_SNAPSHOT_ID, "2");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_TRUE(result_plan->Splits().empty());
}

TEST_F(ScanInteTest, TestScanAppendWithTag1) {
    std::string table_path =
        paimon::test::GetDataDir() + "orc/append_table_with_tag.db/append_table_with_tag";
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::SCAN_TAG_NAME, "1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    // check snapshot id
    ASSERT_EQ(1, result_plan->SnapshotId().value());
}

TEST_F(ScanInteTest, TestScanInvalidTag) {
    std::string table_path =
        paimon::test::GetDataDir() + "orc/append_table_with_tag.db/append_table_with_tag";
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::SCAN_TAG_NAME, "unknown");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_NOK_WITH_MSG(table_scan->CreatePlan(), "Tag 'unknown' doesn't exist.");
}

}  // namespace paimon::test
