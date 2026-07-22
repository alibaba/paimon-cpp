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

#include "paimon/core/operation/metrics/commit_metrics.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/operation/metrics/commit_stats.h"
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

TEST(CommitMetricsTest, TestSimple) {
    auto commit_metrics = std::make_shared<MetricsImpl>();
    commit_metrics->SetCounter("some_metric", 100);
    commit_metrics->SetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS, 30);
    ASSERT_OK_AND_ASSIGN(uint64_t counter,
                         commit_metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
    ASSERT_EQ(30, counter);
    ASSERT_OK_AND_ASSIGN(counter, commit_metrics->GetCounter("some_metric"));
    ASSERT_EQ(100, counter);
    auto other = std::make_shared<MetricsImpl>();
    other->SetCounter("some_metric_2", 200);
    other->SetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS, 50);
    commit_metrics->Merge(other);
    ASSERT_OK_AND_ASSIGN(counter, commit_metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
    ASSERT_EQ(80, counter);
    ASSERT_OK_AND_ASSIGN(counter, commit_metrics->GetCounter("some_metric"));
    ASSERT_EQ(100, counter);
    ASSERT_OK_AND_ASSIGN(counter, commit_metrics->GetCounter("some_metric_2"));
    ASSERT_EQ(200, counter);
}

TEST(CommitMetricsTest, TestReportCommitFromStats) {
    auto metrics = std::make_shared<MetricsImpl>();

    std::vector<ManifestEntry> append_table_files;
    append_table_files.push_back(CreateEntry(FileKind::Add(), 1, 1, 201, 1001, "a1"));
    append_table_files.push_back(CreateEntry(FileKind::Delete(), 2, 3, 302, 1002, "a2"));

    std::vector<ManifestEntry> append_changelog_files;
    append_changelog_files.push_back(CreateEntry(FileKind::Add(), 1, 1, 202, 2001, "c1"));
    append_changelog_files.push_back(CreateEntry(FileKind::Add(), 2, 3, 301, 2002, "c2"));

    std::vector<ManifestEntry> compact_table_files;
    compact_table_files.push_back(CreateEntry(FileKind::Add(), 1, 1, 203, 3001, "k1"));
    compact_table_files.push_back(CreateEntry(FileKind::Delete(), 3, 5, 106, 3002, "k2"));

    std::vector<ManifestEntry> compact_changelog_files;
    compact_changelog_files.push_back(CreateEntry(FileKind::Add(), 1, 1, 205, 4001, "ck1"));
    compact_changelog_files.push_back(CreateEntry(FileKind::Add(), 2, 3, 307, 4002, "ck2"));

    CommitStats stats(append_table_files, append_changelog_files, compact_table_files,
                      compact_changelog_files,
                      /*commit_duration=*/3000, /*generated_snapshots=*/2, /*attempts=*/4,
                      /*last_committed_snapshot_id=*/10);
    CommitMetrics::ReportCommit(metrics, stats);

    ASSERT_OK_AND_ASSIGN(auto last_commit_duration,
                         metrics->GetCounter(CommitMetrics::LAST_COMMIT_DURATION));
    EXPECT_EQ(3000, last_commit_duration);
    ASSERT_OK_AND_ASSIGN(auto last_commit_attempts,
                         metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
    EXPECT_EQ(4, last_commit_attempts);
    ASSERT_OK_AND_ASSIGN(auto last_table_files_added,
                         metrics->GetCounter(CommitMetrics::LAST_TABLE_FILES_ADDED));
    EXPECT_EQ(2, last_table_files_added);
    ASSERT_OK_AND_ASSIGN(auto last_table_files_deleted,
                         metrics->GetCounter(CommitMetrics::LAST_TABLE_FILES_DELETED));
    EXPECT_EQ(2, last_table_files_deleted);
    ASSERT_OK_AND_ASSIGN(auto last_table_files_appended,
                         metrics->GetCounter(CommitMetrics::LAST_TABLE_FILES_APPENDED));
    EXPECT_EQ(2, last_table_files_appended);
    ASSERT_OK_AND_ASSIGN(auto last_table_files_compacted,
                         metrics->GetCounter(CommitMetrics::LAST_TABLE_FILES_COMMIT_COMPACTED));
    EXPECT_EQ(2, last_table_files_compacted);
    ASSERT_OK_AND_ASSIGN(auto last_changelog_files_appended,
                         metrics->GetCounter(CommitMetrics::LAST_CHANGELOG_FILES_APPENDED));
    EXPECT_EQ(2, last_changelog_files_appended);
    ASSERT_OK_AND_ASSIGN(auto last_changelog_files_compacted,
                         metrics->GetCounter(CommitMetrics::LAST_CHANGELOG_FILES_COMMIT_COMPACTED));
    EXPECT_EQ(2, last_changelog_files_compacted);
    ASSERT_OK_AND_ASSIGN(auto last_generated_snapshots,
                         metrics->GetCounter(CommitMetrics::LAST_GENERATED_SNAPSHOTS));
    EXPECT_EQ(2, last_generated_snapshots);
    ASSERT_OK_AND_ASSIGN(auto last_delta_records_appended,
                         metrics->GetCounter(CommitMetrics::LAST_DELTA_RECORDS_APPENDED));
    EXPECT_EQ(503, last_delta_records_appended);
    ASSERT_OK_AND_ASSIGN(auto last_changelog_records_appended,
                         metrics->GetCounter(CommitMetrics::LAST_CHANGELOG_RECORDS_APPENDED));
    EXPECT_EQ(503, last_changelog_records_appended);
    ASSERT_OK_AND_ASSIGN(auto last_delta_records_compacted,
                         metrics->GetCounter(CommitMetrics::LAST_DELTA_RECORDS_COMMIT_COMPACTED));
    EXPECT_EQ(309, last_delta_records_compacted);
    ASSERT_OK_AND_ASSIGN(
        auto last_changelog_records_compacted,
        metrics->GetCounter(CommitMetrics::LAST_CHANGELOG_RECORDS_COMMIT_COMPACTED));
    EXPECT_EQ(512, last_changelog_records_compacted);
    ASSERT_OK_AND_ASSIGN(auto last_partitions_written,
                         metrics->GetCounter(CommitMetrics::LAST_PARTITIONS_WRITTEN));
    EXPECT_EQ(3, last_partitions_written);
    ASSERT_OK_AND_ASSIGN(auto last_buckets_written,
                         metrics->GetCounter(CommitMetrics::LAST_BUCKETS_WRITTEN));
    EXPECT_EQ(3, last_buckets_written);
    ASSERT_OK_AND_ASSIGN(auto last_compaction_input_file_size,
                         metrics->GetCounter(CommitMetrics::LAST_COMPACTION_INPUT_FILE_SIZE));
    EXPECT_EQ(3002, last_compaction_input_file_size);
    ASSERT_OK_AND_ASSIGN(auto last_compaction_output_file_size,
                         metrics->GetCounter(CommitMetrics::LAST_COMPACTION_OUTPUT_FILE_SIZE));
    EXPECT_EQ(3001, last_compaction_output_file_size);
    ASSERT_OK_AND_ASSIGN(auto last_committed_snapshot_id,
                         metrics->GetCounter(CommitMetrics::LAST_COMMITTED_SNAPSHOT_ID));
    EXPECT_EQ(10, last_committed_snapshot_id);
}

}  // namespace paimon::test
