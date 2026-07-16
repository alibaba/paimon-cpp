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

#include "paimon/core/operation/commit/manifest_entry_changes.h"

#include <algorithm>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/core/index/global_index_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/memory/bytes.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class ManifestEntryChangesTest : public testing::Test {
 protected:
    BinaryRow CreateIntRow(int32_t value) const {
        BinaryRow row(1);
        BinaryRowWriter writer(&row, 20, GetDefaultPool().get());
        writer.WriteInt(0, value);
        writer.Complete();
        return row;
    }

    std::shared_ptr<DataFileMeta> CreateDataFileMeta(const std::string& file_name) const {
        return std::make_shared<DataFileMeta>(
            file_name, 1024, 8, DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(),
            SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), /*min_seq_no=*/16,
            /*max_seq_no=*/32,
            /*schema_id=*/1, /*level=*/2,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0),
            /*delete_row_count=*/std::nullopt,
            /*embedded_index=*/nullptr, /*file_source=*/std::nullopt,
            /*external_path=*/std::nullopt,
            /*value_stats_cols=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
    }

    std::shared_ptr<IndexFileMeta> CreateIndexFileMeta(
        const std::string& file_name, const std::string& index_type = "bitmap") const {
        return std::make_shared<IndexFileMeta>(
            index_type, file_name, /*file_size=*/100, /*row_count=*/5,
            /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt,
            /*global_index_meta=*/std::nullopt);
    }

    std::shared_ptr<IndexFileMeta> CreateGlobalIndexFileMeta(const std::string& file_name) const {
        GlobalIndexMeta global_index(/*row_range_start=*/0, /*row_range_end=*/3,
                                     /*index_field_id=*/1, /*extra_field_ids=*/std::nullopt,
                                     std::make_shared<Bytes>("meta", GetDefaultPool().get()));
        return std::make_shared<IndexFileMeta>(
            "HASH", file_name, /*file_size=*/100, /*row_count=*/5,
            /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt, global_index);
    }
};

TEST_F(ManifestEntryChangesTest, TestCollectAndSummary) {
    const BinaryRow partition = CreateIntRow(10);

    DataIncrement data_increment(
        {CreateDataFileMeta("append-add")}, {CreateDataFileMeta("append-del")},
        {CreateDataFileMeta("append-changelog")}, {CreateIndexFileMeta("append-index-add")},
        {CreateIndexFileMeta("append-index-del")});
    CompactIncrement compact_increment(
        {CreateDataFileMeta("compact-before")}, {CreateDataFileMeta("compact-after")},
        {CreateDataFileMeta("compact-changelog")}, {CreateIndexFileMeta("compact-index-add")},
        {CreateIndexFileMeta("compact-index-del")});

    std::shared_ptr<CommitMessage> message = std::make_shared<CommitMessageImpl>(
        partition, /*bucket=*/0, /*total_buckets=*/4, data_increment, compact_increment);

    ManifestEntryChanges changes(/*default_num_bucket=*/8);
    ASSERT_OK(changes.Collect(message));

    ASSERT_EQ(2u, changes.append_table_files.size());
    ASSERT_EQ(1u, changes.append_changelog.size());
    ASSERT_EQ(2u, changes.append_index_files.size());
    ASSERT_EQ(2u, changes.compact_table_files.size());
    ASSERT_EQ(1u, changes.compact_changelog.size());
    ASSERT_EQ(2u, changes.compact_index_files.size());

    EXPECT_TRUE(changes.HasAppendChanges());
    EXPECT_FALSE(changes.HasGlobalIndexFileAdditions());
    EXPECT_TRUE(changes.HasCompactChanges());

    EXPECT_EQ(FileKind::Add(), changes.append_table_files[0].Kind());
    EXPECT_EQ(FileKind::Delete(), changes.append_table_files[1].Kind());
    EXPECT_EQ(4, changes.append_table_files[0].TotalBuckets());

    std::string summary = changes.ToString();
    EXPECT_NE(std::string::npos, summary.find("2 append table files"));
    EXPECT_NE(std::string::npos, summary.find("1 append Changelogs"));
    EXPECT_NE(std::string::npos, summary.find("2 compact index files"));
}

TEST_F(ManifestEntryChangesTest, TestHasGlobalIndexFileAdditions) {
    const BinaryRow partition = CreateIntRow(10);

    DataIncrement data_increment(
        /*new_files=*/{}, /*deleted_files=*/{}, /*changelog_files=*/{},
        /*new_index_files=*/{CreateGlobalIndexFileMeta("append-global-index")},
        /*deleted_index_files=*/{});
    CompactIncrement compact_increment(/*compact_before=*/{}, /*compact_after=*/{},
                                       /*changelog_files=*/{},
                                       /*new_index_files=*/{},
                                       /*deleted_index_files=*/{});

    std::shared_ptr<CommitMessage> message = std::make_shared<CommitMessageImpl>(
        partition, /*bucket=*/0, /*total_buckets=*/4, data_increment, compact_increment);

    ManifestEntryChanges changes(/*default_num_bucket=*/8);
    ASSERT_OK(changes.Collect(message));

    EXPECT_TRUE(changes.HasGlobalIndexFileAdditions());
}

TEST_F(ManifestEntryChangesTest, TestCollectInvalidCommitMessageType) {
    ManifestEntryChanges changes(/*default_num_bucket=*/8);
    std::shared_ptr<CommitMessage> invalid_message = std::make_shared<CommitMessage>();
    ASSERT_NOK_WITH_MSG(changes.Collect(invalid_message),
                        "fail to cast commit message to commit message impl");
}

TEST_F(ManifestEntryChangesTest, TestChangedPartitionsIncludesDvAndGlobalIndex) {
    const BinaryRow partition_data = CreateIntRow(10);
    const BinaryRow partition_dv = CreateIntRow(20);
    const BinaryRow partition_global = CreateIntRow(30);
    const BinaryRow partition_plain_index = CreateIntRow(40);

    std::vector<ManifestEntry> data_changes;
    data_changes.emplace_back(FileKind::Add(), partition_data, /*bucket=*/0, /*total_buckets=*/2,
                              CreateDataFileMeta("data-file"));

    std::vector<IndexManifestEntry> index_changes;
    index_changes.emplace_back(FileKind::Add(), partition_dv, /*bucket=*/0,
                               CreateIndexFileMeta("dv-file", "DELETION_VECTORS"));
    index_changes.emplace_back(FileKind::Add(), partition_global, /*bucket=*/0,
                               CreateGlobalIndexFileMeta("global-index"));
    index_changes.emplace_back(FileKind::Add(), partition_plain_index, /*bucket=*/0,
                               CreateIndexFileMeta("plain-index", "bitmap"));

    std::vector<BinaryRow> changed =
        ManifestEntryChanges::ChangedPartitions(data_changes, index_changes);

    auto contains = [&changed](const BinaryRow& target) {
        return std::find(changed.begin(), changed.end(), target) != changed.end();
    };

    EXPECT_TRUE(contains(partition_data));
    EXPECT_TRUE(contains(partition_dv));
    EXPECT_TRUE(contains(partition_global));
    EXPECT_FALSE(contains(partition_plain_index));
}

}  // namespace paimon::test
