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

#include "paimon/core/operation/commit/overwrite_changes_provider.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

BinaryRow CreateIntRow(int32_t value) {
    BinaryRow row(1);
    BinaryRowWriter writer(&row, 20, GetDefaultPool().get());
    writer.WriteInt(0, value);
    writer.Complete();
    return row;
}

ManifestEntry CreateManifestEntry(const std::string& file_name, const FileKind& kind,
                                  int32_t partition_value, int32_t level = 0) {
    auto file_meta = std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/1024, /*row_count=*/8, DataFileMeta::EmptyMinKey(),
        DataFileMeta::EmptyMaxKey(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
        /*min_seq_no=*/0,
        /*max_seq_no=*/0,
        /*schema_id=*/0, level,
        /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(0, 0),
        /*delete_row_count=*/std::nullopt,
        /*embedded_index=*/nullptr, /*file_source=*/std::nullopt,
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);

    return ManifestEntry(kind, CreateIntRow(partition_value), /*bucket=*/0, /*total_buckets=*/1,
                         file_meta);
}

IndexManifestEntry CreateIndexEntry(const std::string& file_name, int32_t partition_value,
                                    const FileKind& kind = FileKind::Add()) {
    auto index_file = std::make_shared<IndexFileMeta>(
        /*index_type=*/"HASH", file_name, /*file_size=*/10, /*row_count=*/1,
        /*dv_ranges=*/std::nullopt,
        /*external_path=*/std::nullopt,
        /*global_index_meta=*/std::nullopt);
    return IndexManifestEntry(kind, CreateIntRow(partition_value), /*bucket=*/0, index_file);
}

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
        /*commit_identifier=*/1, Snapshot::CommitKind::Overwrite(),
        /*time_millis=*/0,
        /*total_record_count=*/0,
        /*delta_record_count=*/0,
        /*changelog_record_count=*/std::nullopt,
        /*watermark=*/std::nullopt,
        /*statistics=*/std::nullopt,
        /*properties=*/std::nullopt,
        /*next_row_id=*/std::nullopt);
}

}  // namespace

TEST(OverwriteChangesProviderTest, TestProvideWithoutLatestSnapshotUsesChangesOnly) {
    std::vector<ManifestEntry> changes = {
        CreateManifestEntry("new-1", FileKind::Add(), /*partition_value=*/1),
        CreateManifestEntry("new-2", FileKind::Delete(), /*partition_value=*/1)};
    std::vector<IndexManifestEntry> index_entries = {
        CreateIndexEntry("index-new", /*partition_value=*/1)};

    int manifest_scan_calls = 0;
    int index_scan_calls = 0;
    OverwriteChangesProvider provider(
        changes, index_entries,
        [&manifest_scan_calls](const Snapshot&) -> Result<std::vector<ManifestEntry>> {
            ++manifest_scan_calls;
            return Status::Invalid("should not call manifest_scan");
        },
        [&index_scan_calls](const Snapshot&) -> Result<std::vector<IndexManifestEntry>> {
            ++index_scan_calls;
            return Status::Invalid("should not call index_scan");
        });

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CommitChanges> provided, provider.Provide(std::nullopt));

    const std::vector<ManifestEntry>& delta_files = provided->delta_files;
    const std::vector<ManifestEntry>& changelog_files = provided->changelog_files;
    const std::vector<IndexManifestEntry>& provided_index_entries = provided->index_entries;

    ASSERT_EQ(0, manifest_scan_calls);
    ASSERT_EQ(0, index_scan_calls);
    ASSERT_TRUE(changelog_files.empty());
    ASSERT_EQ(changes.size(), delta_files.size());
    ASSERT_EQ(index_entries.size(), provided_index_entries.size());
    EXPECT_EQ("new-1", delta_files[0].FileName());
    EXPECT_EQ("new-2", delta_files[1].FileName());
    EXPECT_EQ("index-new", provided_index_entries[0].index_file->FileName());
}

TEST(OverwriteChangesProviderTest, TestProvideWithLatestSnapshotAddsDeletesAndAppendsAllChanges) {
    std::vector<ManifestEntry> changes = {
        CreateManifestEntry("existing-a", FileKind::Add(), /*partition_value=*/1),
        CreateManifestEntry("new-b", FileKind::Add(), /*partition_value=*/1),
        CreateManifestEntry("force-delete-c", FileKind::Delete(), /*partition_value=*/1)};
    std::vector<IndexManifestEntry> index_entries = {
        CreateIndexEntry("index-new", /*partition_value=*/1)};

    OverwriteChangesProvider provider(
        changes, index_entries,
        [](const Snapshot&) -> Result<std::vector<ManifestEntry>> {
            return std::vector<ManifestEntry>{
                CreateManifestEntry("existing-a", FileKind::Add(), /*partition_value=*/1),
                CreateManifestEntry("existing-x", FileKind::Add(), /*partition_value=*/1)};
        },
        [](const Snapshot&) -> Result<std::vector<IndexManifestEntry>> {
            return std::vector<IndexManifestEntry>{
                CreateIndexEntry("index-old", /*partition_value=*/1)};
        });

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CommitChanges> provided,
                         provider.Provide(std::optional<Snapshot>(MakeSnapshot())));

    const std::vector<ManifestEntry>& delta_files = provided->delta_files;
    const std::vector<ManifestEntry>& changelog_files = provided->changelog_files;
    const std::vector<IndexManifestEntry>& provided_index_entries = provided->index_entries;

    ASSERT_TRUE(changelog_files.empty());

    // delta = delete(existing-a), delete(existing-x),
    //         add(existing-a), add(new-b), delete(force-delete-c)
    ASSERT_EQ(5u, delta_files.size());
    EXPECT_TRUE(delta_files[0].Kind() == FileKind::Delete());
    EXPECT_EQ("existing-a", delta_files[0].FileName());
    EXPECT_TRUE(delta_files[1].Kind() == FileKind::Delete());
    EXPECT_EQ("existing-x", delta_files[1].FileName());
    EXPECT_TRUE(delta_files[2].Kind() == FileKind::Add());
    EXPECT_EQ("existing-a", delta_files[2].FileName());
    EXPECT_TRUE(delta_files[3].Kind() == FileKind::Add());
    EXPECT_EQ("new-b", delta_files[3].FileName());
    EXPECT_TRUE(delta_files[4].Kind() == FileKind::Delete());
    EXPECT_EQ("force-delete-c", delta_files[4].FileName());

    // index = provided new + delete old
    ASSERT_EQ(2u, provided_index_entries.size());
    EXPECT_TRUE(provided_index_entries[0].kind == FileKind::Add());
    EXPECT_EQ("index-new", provided_index_entries[0].index_file->FileName());
    EXPECT_TRUE(provided_index_entries[1].kind == FileKind::Delete());
    EXPECT_EQ("index-old", provided_index_entries[1].index_file->FileName());
}

TEST(OverwriteChangesProviderTest, TestProvidePropagatesScanErrors) {
    std::vector<ManifestEntry> changes = {
        CreateManifestEntry("new-1", FileKind::Add(), /*partition_value=*/1)};
    std::vector<IndexManifestEntry> index_entries;

    OverwriteChangesProvider provider_manifest_fail(
        changes, index_entries,
        [](const Snapshot&) -> Result<std::vector<ManifestEntry>> {
            return Status::Invalid("manifest scan failed");
        },
        [](const Snapshot&) -> Result<std::vector<IndexManifestEntry>> {
            return std::vector<IndexManifestEntry>{};
        });

    ASSERT_NOK_WITH_MSG(provider_manifest_fail.Provide(std::optional<Snapshot>(MakeSnapshot())),
                        "manifest scan failed");

    OverwriteChangesProvider provider_index_fail(
        changes, index_entries,
        [](const Snapshot&) -> Result<std::vector<ManifestEntry>> {
            return std::vector<ManifestEntry>{};
        },
        [](const Snapshot&) -> Result<std::vector<IndexManifestEntry>> {
            return Status::Invalid("index scan failed");
        });

    ASSERT_NOK_WITH_MSG(provider_index_fail.Provide(std::optional<Snapshot>(MakeSnapshot())),
                        "index scan failed");
}

}  // namespace paimon::test
