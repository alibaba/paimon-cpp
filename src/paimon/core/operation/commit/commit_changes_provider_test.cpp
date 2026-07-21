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

#include "paimon/core/operation/commit/commit_changes_provider.h"

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
                                  int32_t partition_value) {
    auto file_meta = std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/1024, /*row_count=*/8, DataFileMeta::EmptyMinKey(),
        DataFileMeta::EmptyMaxKey(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
        /*min_seq_no=*/0,
        /*max_seq_no=*/0,
        /*schema_id=*/0, /*level=*/0,
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

IndexManifestEntry CreateIndexEntry(const std::string& file_name, int32_t partition_value) {
    auto index_file = std::make_shared<IndexFileMeta>(
        /*index_type=*/"HASH", file_name, /*file_size=*/10, /*row_count=*/1,
        /*dv_ranges=*/std::nullopt,
        /*external_path=*/std::nullopt,
        /*global_index_meta=*/std::nullopt);

    return IndexManifestEntry(FileKind::Add(), CreateIntRow(partition_value), /*bucket=*/0,
                              index_file);
}

}  // namespace

TEST(CommitChangesProviderTest, TestProvideReturnsGivenEntries) {
    std::vector<ManifestEntry> delta_files = {
        CreateManifestEntry("delta-1", FileKind::Add(), /*partition_value=*/1)};
    std::vector<ManifestEntry> changelog_files = {
        CreateManifestEntry("changelog-1", FileKind::Add(), /*partition_value=*/2)};
    std::vector<IndexManifestEntry> index_entries = {
        CreateIndexEntry("index-1", /*partition_value=*/3)};

    std::shared_ptr<CommitChangesProvider> provider =
        CommitChangesProvider::Provider(delta_files, changelog_files, index_entries);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CommitChanges> provided, provider->Provide(std::nullopt));

    const std::vector<ManifestEntry>& provided_delta = provided->delta_files;
    const std::vector<ManifestEntry>& provided_changelog = provided->changelog_files;
    const std::vector<IndexManifestEntry>& provided_index = provided->index_entries;

    ASSERT_EQ(delta_files.size(), provided_delta.size());
    ASSERT_EQ(changelog_files.size(), provided_changelog.size());
    ASSERT_EQ(index_entries.size(), provided_index.size());
    ASSERT_EQ("delta-1", provided_delta[0].FileName());
    ASSERT_EQ("changelog-1", provided_changelog[0].FileName());
    ASSERT_EQ("index-1", provided_index[0].index_file->FileName());
}

TEST(CommitChangesProviderTest, TestProvideUsesCopiedInputs) {
    std::vector<ManifestEntry> delta_files = {
        CreateManifestEntry("delta-1", FileKind::Add(), /*partition_value=*/1)};
    std::vector<ManifestEntry> changelog_files;
    std::vector<IndexManifestEntry> index_entries;

    std::shared_ptr<CommitChangesProvider> provider =
        CommitChangesProvider::Provider(delta_files, changelog_files, index_entries);

    delta_files.push_back(CreateManifestEntry("delta-2", FileKind::Add(), /*partition_value=*/2));
    changelog_files.push_back(
        CreateManifestEntry("changelog-2", FileKind::Add(), /*partition_value=*/3));
    index_entries.push_back(CreateIndexEntry("index-2", /*partition_value=*/4));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CommitChanges> provided, provider->Provide(std::nullopt));

    ASSERT_EQ(1u, provided->delta_files.size());
    ASSERT_EQ(0u, provided->changelog_files.size());
    ASSERT_EQ(0u, provided->index_entries.size());
    ASSERT_EQ("delta-1", provided->delta_files[0].FileName());
}

}  // namespace paimon::test
