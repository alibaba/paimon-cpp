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

#include "paimon/core/operation/commit/commit_scanner.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/core/core_options.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/snapshot.h"
#include "paimon/scan_context.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

Snapshot MakeSnapshot() {
    return Snapshot(
        /*id=*/1,
        /*schema_id=*/0,
        /*base_manifest_list=*/"base-manifest-list",
        /*base_manifest_list_size=*/std::nullopt,
        /*delta_manifest_list=*/"delta-manifest-list",
        /*delta_manifest_list_size=*/std::nullopt,
        /*changelog_manifest_list=*/std::nullopt,
        /*changelog_manifest_list_size=*/std::nullopt,
        /*index_manifest=*/std::nullopt,
        /*commit_user=*/"test-user",
        /*commit_identifier=*/1, Snapshot::CommitKind::Append(),
        /*time_millis=*/0,
        /*total_record_count=*/0,
        /*delta_record_count=*/0,
        /*changelog_record_count=*/std::nullopt,
        /*watermark=*/std::nullopt,
        /*statistics=*/std::nullopt,
        /*properties=*/std::nullopt,
        /*next_row_id=*/std::nullopt);
}

BinaryRow CreateIntPartition(int32_t value) {
    BinaryRow row(1);
    BinaryRowWriter writer(&row, 20, GetDefaultPool().get());
    writer.WriteInt(0, value);
    writer.Complete();
    return row;
}

}  // namespace

class CommitScannerTest : public testing::Test {
 protected:
    void SetUp() override {
        schema_ = arrow::schema({arrow::field("pt", arrow::int32())});
        ASSERT_OK_AND_ASSIGN(core_options_, CoreOptions::FromMap({}));
        ASSERT_OK_AND_ASSIGN(partition_computer_,
                             BinaryRowPartitionComputer::Create(
                                 /*partition_keys=*/{"pt"}, schema_,
                                 /*default_part_value=*/"__DEFAULT_PARTITION__",
                                 /*legacy_partition_name_enabled=*/true, GetDefaultPool()));
    }

    CommitScanner CreateScanner(CommitScanner::ScanSupplier scan_supplier) const {
        return CommitScanner(
            /*snapshot_manager=*/nullptr,
            /*schema_manager=*/nullptr,
            /*manifest_list=*/nullptr,
            /*manifest_file=*/nullptr,
            /*index_manifest_file=*/nullptr,
            /*table_schema=*/nullptr, schema_, core_options_,
            /*executor=*/nullptr, GetDefaultPool(), partition_computer_.get(),
            std::move(scan_supplier));
    }

 protected:
    std::shared_ptr<arrow::Schema> schema_;
    CoreOptions core_options_;
    std::unique_ptr<BinaryRowPartitionComputer> partition_computer_;
};

TEST_F(CommitScannerTest, TestReadAllEntriesFromChangedPartitionsEmptyFastExit) {
    bool supplier_called = false;
    CommitScanner scanner = CreateScanner([&supplier_called](const std::shared_ptr<ScanFilter>&)
                                              -> Result<std::unique_ptr<FileStoreScan>> {
        supplier_called = true;
        return Status::Invalid("should not be called");
    });

    ASSERT_OK_AND_ASSIGN(std::vector<ManifestEntry> entries,
                         scanner.ReadAllEntriesFromChangedPartitions(MakeSnapshot(),
                                                                     /*changed_partitions=*/{}));
    ASSERT_TRUE(entries.empty());
    ASSERT_FALSE(supplier_called);
}

TEST_F(CommitScannerTest, TestReadAllEntriesFromPartitionsRequiresSupplier) {
    CommitScanner scanner = CreateScanner(CommitScanner::ScanSupplier{});

    std::vector<std::map<std::string, std::string>> partitions = {{{"pt", "1"}}};
    ASSERT_NOK_WITH_MSG(scanner.ReadAllEntriesFromPartitions(MakeSnapshot(), partitions),
                        "CommitScanner requires non-empty scan supplier");
}

TEST_F(CommitScannerTest, TestReadAllEntriesFromChangedPartitionsBuildsScanFilterPartitions) {
    std::vector<std::map<std::string, std::string>> captured_partition_filters;
    bool supplier_called = false;

    CommitScanner scanner = CreateScanner(
        [&captured_partition_filters, &supplier_called](
            const std::shared_ptr<ScanFilter>& filter) -> Result<std::unique_ptr<FileStoreScan>> {
            supplier_called = true;
            captured_partition_filters = filter->GetPartitionFilters();
            return Status::Invalid("stop after capturing filter");
        });

    std::vector<BinaryRow> changed_partitions = {CreateIntPartition(42)};
    ASSERT_NOK_WITH_MSG(
        scanner.ReadAllEntriesFromChangedPartitions(MakeSnapshot(), changed_partitions),
        "stop after capturing filter");

    ASSERT_TRUE(supplier_called);
    ASSERT_EQ(1u, captured_partition_filters.size());
    ASSERT_EQ(1u, captured_partition_filters[0].size());
    ASSERT_EQ("42", captured_partition_filters[0]["pt"]);
}

}  // namespace paimon::test
