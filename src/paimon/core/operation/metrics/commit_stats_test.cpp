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

#include "paimon/core/operation/metrics/commit_stats.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
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

ManifestEntry CreateEntry(const FileKind& kind, int32_t partition, int32_t bucket,
                          int64_t row_count, int64_t file_size, const std::string& file_name) {
    BinaryRow part = CreateIntRow(partition);
    auto file_meta = std::make_shared<DataFileMeta>(
        file_name, file_size, row_count, DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(),
        SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0,
        /*schema_id=*/1, /*level=*/0,
        /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(0, 0),
        /*delete_row_count=*/std::nullopt,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    return ManifestEntry(kind, part, bucket, /*total_buckets=*/10, file_meta);
}

}  // namespace

TEST(CommitStatsTest, TestCalcChangedPartitionsAndBuckets) {
    std::vector<ManifestEntry> files;
    files.push_back(CreateEntry(FileKind::Add(), /*partition=*/1, /*bucket=*/1,
                                /*row_count=*/201, /*file_size=*/11, "a1"));
    files.push_back(CreateEntry(FileKind::Add(), /*partition=*/2, /*bucket=*/3,
                                /*row_count=*/302, /*file_size=*/12, "a2"));
    files.push_back(CreateEntry(FileKind::Add(), /*partition=*/1, /*bucket=*/1,
                                /*row_count=*/202, /*file_size=*/13, "c1"));
    files.push_back(CreateEntry(FileKind::Add(), /*partition=*/2, /*bucket=*/3,
                                /*row_count=*/301, /*file_size=*/14, "c2"));
    files.push_back(CreateEntry(FileKind::Add(), /*partition=*/1, /*bucket=*/1,
                                /*row_count=*/203, /*file_size=*/15, "k1"));
    files.push_back(CreateEntry(FileKind::Add(), /*partition=*/2, /*bucket=*/3,
                                /*row_count=*/304, /*file_size=*/16, "k2"));
    files.push_back(CreateEntry(FileKind::Delete(), /*partition=*/3, /*bucket=*/5,
                                /*row_count=*/106, /*file_size=*/17, "k3"));
    files.push_back(CreateEntry(FileKind::Add(), /*partition=*/1, /*bucket=*/1,
                                /*row_count=*/205, /*file_size=*/18, "ck1"));
    files.push_back(CreateEntry(FileKind::Add(), /*partition=*/2, /*bucket=*/3,
                                /*row_count=*/307, /*file_size=*/19, "ck2"));

    EXPECT_EQ(3, CommitStats::NumChangedBuckets({files}));
    EXPECT_EQ(3, CommitStats::NumChangedPartitions({files}));
}

TEST(CommitStatsTest, TestFailedAppendSnapshot) {
    CommitStats stats(/*append_table_files=*/{}, /*append_changelog_files=*/{},
                      /*compact_table_files=*/{}, /*compact_changelog_files=*/{},
                      /*commit_duration=*/0, /*generated_snapshots=*/0, /*attempts=*/1,
                      /*last_committed_snapshot_id=*/-1);

    EXPECT_EQ(0, stats.GetTableFilesAdded());
    EXPECT_EQ(0, stats.GetTableFilesDeleted());
    EXPECT_EQ(0, stats.GetTableFilesAppended());
    EXPECT_EQ(0, stats.GetTableFilesCompacted());
    EXPECT_EQ(0, stats.GetChangelogFilesAppended());
    EXPECT_EQ(0, stats.GetChangelogFilesCompacted());
    EXPECT_EQ(0, stats.GetGeneratedSnapshots());
    EXPECT_EQ(0, stats.GetDeltaRecordsAppended());
    EXPECT_EQ(0, stats.GetChangelogRecordsAppended());
    EXPECT_EQ(0, stats.GetDeltaRecordsCompacted());
    EXPECT_EQ(0, stats.GetChangelogRecordsCompacted());
    EXPECT_EQ(0, stats.GetNumPartitionsWritten());
    EXPECT_EQ(0, stats.GetNumBucketsWritten());
    EXPECT_EQ(0, stats.GetDuration());
    EXPECT_EQ(1, stats.GetAttempts());
    EXPECT_EQ(-1, stats.GetLastCommittedSnapshotId());
}

TEST(CommitStatsTest, TestSucceedAllSnapshot) {
    std::vector<ManifestEntry> append_data_files;
    append_data_files.push_back(CreateEntry(FileKind::Add(), 1, 1, 201, 1001, "a1"));
    append_data_files.push_back(CreateEntry(FileKind::Add(), 2, 3, 302, 1002, "a2"));

    std::vector<ManifestEntry> append_changelog_files;
    append_changelog_files.push_back(CreateEntry(FileKind::Add(), 1, 1, 202, 2001, "c1"));
    append_changelog_files.push_back(CreateEntry(FileKind::Add(), 2, 3, 301, 2002, "c2"));

    std::vector<ManifestEntry> compact_data_files;
    compact_data_files.push_back(CreateEntry(FileKind::Add(), 1, 1, 203, 3001, "k1"));
    compact_data_files.push_back(CreateEntry(FileKind::Add(), 2, 3, 304, 3002, "k2"));
    compact_data_files.push_back(CreateEntry(FileKind::Delete(), 3, 5, 106, 3003, "k3"));

    std::vector<ManifestEntry> compact_changelog_files;
    compact_changelog_files.push_back(CreateEntry(FileKind::Add(), 1, 1, 205, 4001, "ck1"));
    compact_changelog_files.push_back(CreateEntry(FileKind::Add(), 2, 3, 307, 4002, "ck2"));

    CommitStats stats(append_data_files, append_changelog_files, compact_data_files,
                      compact_changelog_files,
                      /*commit_duration=*/3000, /*generated_snapshots=*/2, /*attempts=*/2,
                      /*last_committed_snapshot_id=*/10);

    EXPECT_EQ(4, stats.GetTableFilesAdded());
    EXPECT_EQ(1, stats.GetTableFilesDeleted());
    EXPECT_EQ(2, stats.GetTableFilesAppended());
    EXPECT_EQ(3, stats.GetTableFilesCompacted());
    EXPECT_EQ(2, stats.GetChangelogFilesAppended());
    EXPECT_EQ(2, stats.GetChangelogFilesCompacted());
    EXPECT_EQ(2, stats.GetGeneratedSnapshots());
    EXPECT_EQ(503, stats.GetDeltaRecordsAppended());
    EXPECT_EQ(503, stats.GetChangelogRecordsAppended());
    EXPECT_EQ(613, stats.GetDeltaRecordsCompacted());
    EXPECT_EQ(512, stats.GetChangelogRecordsCompacted());
    EXPECT_EQ(3, stats.GetNumPartitionsWritten());
    EXPECT_EQ(3, stats.GetNumBucketsWritten());
    EXPECT_EQ(3000, stats.GetDuration());
    EXPECT_EQ(2, stats.GetAttempts());
    EXPECT_EQ(10, stats.GetLastCommittedSnapshotId());
    EXPECT_EQ(3003, stats.GetCompactionInputFileSize());
    EXPECT_EQ(6003, stats.GetCompactionOutputFileSize());
}

}  // namespace paimon::test
