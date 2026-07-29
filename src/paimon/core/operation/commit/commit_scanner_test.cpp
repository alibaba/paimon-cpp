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
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/scan_context.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

Snapshot MakeSnapshot(std::optional<std::string> index_manifest = std::nullopt) {
    return Snapshot(
        /*id=*/1,
        /*schema_id=*/0,
        /*base_manifest_list=*/"base-manifest-list",
        /*base_manifest_list_size=*/std::nullopt,
        /*delta_manifest_list=*/"delta-manifest-list",
        /*delta_manifest_list_size=*/std::nullopt,
        /*changelog_manifest_list=*/std::nullopt,
        /*changelog_manifest_list_size=*/std::nullopt,
        /*index_manifest=*/std::move(index_manifest),
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

IndexManifestEntry CreateIndexEntry(const std::string& file_name, int32_t partition_value) {
    auto index_file = std::make_shared<IndexFileMeta>(
        /*index_type=*/"HASH", file_name, /*file_size=*/10, /*row_count=*/1,
        /*dv_ranges=*/std::nullopt,
        /*external_path=*/std::nullopt);
    return IndexManifestEntry(FileKind::Add(), CreateIntPartition(partition_value),
                              /*bucket=*/0, index_file);
}

}  // namespace

class CommitScannerTest : public testing::Test {
 protected:
    void SetUp() override {
        dir_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        schema_ = arrow::schema({arrow::field("pt", arrow::int32())});
        ASSERT_OK_AND_ASSIGN(core_options_, CoreOptions::FromMap({}));
        ASSERT_OK_AND_ASSIGN(partition_computer_,
                             BinaryRowPartitionComputer::Create(
                                 /*partition_keys=*/{"pt"}, schema_,
                                 /*default_part_value=*/"__DEFAULT_PARTITION__",
                                 /*legacy_partition_name_enabled=*/true, GetDefaultPool()));
    }

    CommitScanner CreateScanner(
        CommitScanner::ScanSupplier scan_supplier,
        const std::shared_ptr<IndexManifestFile>& index_manifest_file = nullptr) const {
        return CommitScanner(
            /*snapshot_manager=*/nullptr,
            /*schema_manager=*/nullptr,
            /*manifest_list=*/nullptr,
            /*manifest_file=*/nullptr,
            /*index_manifest_file=*/index_manifest_file,
            /*table_schema=*/nullptr, schema_, core_options_,
            /*executor=*/nullptr, GetDefaultPool(), partition_computer_.get(),
            std::move(scan_supplier));
    }

    Result<std::shared_ptr<IndexManifestFile>> CreateIndexManifestFile() const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileFormat> file_format,
                               FileFormatFactory::Get("orc", {}));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FileStorePathFactory> path_factory,
            FileStorePathFactory::Create(
                dir_->Str(), schema_, /*partition_keys=*/{"pt"},
                /*default_part_value=*/"__DEFAULT_PARTITION__", file_format->Identifier(),
                /*data_file_prefix=*/"data-",
                /*legacy_partition_name_enabled=*/true, /*external_paths=*/{},
                /*global_index_external_path=*/std::nullopt,
                /*index_file_in_data_file_dir=*/false, GetDefaultPool()));
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<IndexManifestFile> index_manifest_file,
            IndexManifestFile::Create(dir_->GetFileSystem(), file_format, "zstd", path_factory,
                                      /*bucket_mode=*/2, GetDefaultPool(), core_options_));
        return std::shared_ptr<IndexManifestFile>(std::move(index_manifest_file));
    }

 protected:
    std::unique_ptr<UniqueTestDirectory> dir_;
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

TEST_F(CommitScannerTest, TestReadAllIndexEntriesFromPartitions) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexManifestFile> index_manifest_file,
                         CreateIndexManifestFile());
    std::vector<IndexManifestEntry> entries = {CreateIndexEntry("index-1", /*partition_value=*/1),
                                               CreateIndexEntry("index-2", /*partition_value=*/2),
                                               CreateIndexEntry("index-3", /*partition_value=*/3)};
    ASSERT_OK_AND_ASSIGN(
        std::optional<std::string> index_manifest,
        index_manifest_file->WriteIndexFiles(/*previous_index_manifest=*/std::nullopt, entries));
    ASSERT_TRUE(index_manifest);

    CommitScanner scanner = CreateScanner(CommitScanner::ScanSupplier{}, index_manifest_file);
    Snapshot snapshot = MakeSnapshot(index_manifest);

    ASSERT_OK_AND_ASSIGN(std::vector<IndexManifestEntry> unfiltered,
                         scanner.ReadAllIndexEntriesFromPartitions(snapshot, /*partitions=*/{}));
    ASSERT_EQ(3u, unfiltered.size());

    std::vector<std::map<std::string, std::string>> partitions = {
        {{"pt", "2"}}, {{"unknown_partition_key", "value"}}};
    ASSERT_OK_AND_ASSIGN(std::vector<IndexManifestEntry> filtered,
                         scanner.ReadAllIndexEntriesFromPartitions(snapshot, partitions));
    ASSERT_EQ(1u, filtered.size());
    ASSERT_EQ("index-2", filtered[0].index_file->FileName());
}

}  // namespace paimon::test
