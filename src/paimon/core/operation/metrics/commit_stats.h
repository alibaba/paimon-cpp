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

#pragma once

#include <cstdint>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/manifest_entry.h"

namespace paimon {

/// Statistics for a commit. Interface is aligned with Java CommitStats.
class CommitStats {
 public:
    CommitStats(const std::vector<ManifestEntry>& append_table_files,
                const std::vector<ManifestEntry>& append_changelog_files,
                const std::vector<ManifestEntry>& compact_table_files,
                const std::vector<ManifestEntry>& compact_changelog_files, int64_t commit_duration,
                int32_t generated_snapshots, int32_t attempts, int64_t last_committed_snapshot_id) {
        duration_ = commit_duration;
        attempts_ = attempts;
        table_files_appended_ = static_cast<int64_t>(append_table_files.size());
        changelog_files_appended_ = static_cast<int64_t>(append_changelog_files.size());
        changelog_files_compacted_ = static_cast<int64_t>(compact_changelog_files.size());
        changelog_records_compacted_ = RowCounts(compact_changelog_files);
        delta_records_compacted_ = RowCounts(compact_table_files);
        changelog_records_appended_ = RowCounts(append_changelog_files);
        delta_records_appended_ = RowCounts(append_table_files);
        table_files_compacted_ = static_cast<int64_t>(compact_table_files.size());
        generated_snapshots_ = generated_snapshots;
        num_partitions_written_ = NumChangedPartitions({append_table_files, compact_table_files});
        num_buckets_written_ = NumChangedBuckets({append_table_files, compact_table_files});
        last_committed_snapshot_id_ = last_committed_snapshot_id;

        std::vector<ManifestEntry> added_table_files;
        std::vector<ManifestEntry> deleted_table_files;
        for (const auto& entry : append_table_files) {
            if (entry.Kind() == FileKind::Add()) {
                added_table_files.push_back(entry);
            } else if (entry.Kind() == FileKind::Delete()) {
                deleted_table_files.push_back(entry);
            }
        }

        std::vector<ManifestEntry> compact_after_files;
        std::vector<ManifestEntry> compaction_input_files;
        for (const auto& entry : compact_table_files) {
            if (entry.Kind() == FileKind::Add()) {
                compact_after_files.push_back(entry);
            } else if (entry.Kind() == FileKind::Delete()) {
                compaction_input_files.push_back(entry);
            }
        }

        added_table_files.insert(added_table_files.end(), compact_after_files.begin(),
                                 compact_after_files.end());
        deleted_table_files.insert(deleted_table_files.end(), compaction_input_files.begin(),
                                   compaction_input_files.end());

        table_files_added_ = static_cast<int64_t>(added_table_files.size());
        table_files_deleted_ = static_cast<int64_t>(deleted_table_files.size());
        compaction_input_file_size_ = FileSizes(compaction_input_files);
        compaction_output_file_size_ = FileSizes(compact_after_files);
    }

    int64_t GetTableFilesAdded() const {
        return table_files_added_;
    }
    int64_t GetTableFilesDeleted() const {
        return table_files_deleted_;
    }
    int64_t GetTableFilesAppended() const {
        return table_files_appended_;
    }
    int64_t GetTableFilesCompacted() const {
        return table_files_compacted_;
    }
    int64_t GetChangelogFilesAppended() const {
        return changelog_files_appended_;
    }
    int64_t GetChangelogFilesCompacted() const {
        return changelog_files_compacted_;
    }
    int64_t GetGeneratedSnapshots() const {
        return generated_snapshots_;
    }
    int64_t GetDeltaRecordsAppended() const {
        return delta_records_appended_;
    }
    int64_t GetChangelogRecordsAppended() const {
        return changelog_records_appended_;
    }
    int64_t GetDeltaRecordsCompacted() const {
        return delta_records_compacted_;
    }
    int64_t GetChangelogRecordsCompacted() const {
        return changelog_records_compacted_;
    }
    int64_t GetNumPartitionsWritten() const {
        return num_partitions_written_;
    }
    int64_t GetNumBucketsWritten() const {
        return num_buckets_written_;
    }
    int64_t GetDuration() const {
        return duration_;
    }
    int32_t GetAttempts() const {
        return attempts_;
    }
    int64_t GetCompactionInputFileSize() const {
        return compaction_input_file_size_;
    }
    int64_t GetCompactionOutputFileSize() const {
        return compaction_output_file_size_;
    }
    int64_t GetLastCommittedSnapshotId() const {
        return last_committed_snapshot_id_;
    }

    static int64_t NumChangedPartitions(const std::vector<std::vector<ManifestEntry>>& changes) {
        std::unordered_set<BinaryRow> changed_partitions;
        for (const auto& change : changes) {
            for (const auto& entry : change) {
                changed_partitions.insert(entry.Partition());
            }
        }
        return static_cast<int64_t>(changed_partitions.size());
    }

    static int64_t NumChangedBuckets(const std::vector<std::vector<ManifestEntry>>& changes) {
        std::unordered_map<BinaryRow, std::set<int32_t>> changed_partition_buckets;
        for (const auto& change : changes) {
            for (const auto& entry : change) {
                changed_partition_buckets[entry.Partition()].insert(entry.Bucket());
            }
        }

        int64_t num_changed_buckets = 0;
        for (const auto& [_, buckets] : changed_partition_buckets) {
            num_changed_buckets += static_cast<int64_t>(buckets.size());
        }
        return num_changed_buckets;
    }

 private:
    static int64_t RowCounts(const std::vector<ManifestEntry>& files) {
        int64_t row_count = 0;
        for (const auto& entry : files) {
            row_count += entry.File()->row_count;
        }
        return row_count;
    }

    static int64_t FileSizes(const std::vector<ManifestEntry>& files) {
        int64_t file_size = 0;
        for (const auto& entry : files) {
            file_size += entry.File()->file_size;
        }
        return file_size;
    }

 private:
    int64_t duration_ = 0;
    int32_t attempts_ = 0;
    int64_t table_files_appended_ = 0;
    int64_t table_files_added_ = 0;
    int64_t table_files_deleted_ = 0;
    int64_t changelog_files_appended_ = 0;
    int64_t compaction_input_file_size_ = 0;
    int64_t compaction_output_file_size_ = 0;
    int64_t changelog_files_compacted_ = 0;
    int64_t changelog_records_compacted_ = 0;
    int64_t delta_records_compacted_ = 0;
    int64_t changelog_records_appended_ = 0;
    int64_t delta_records_appended_ = 0;
    int64_t table_files_compacted_ = 0;
    int64_t generated_snapshots_ = 0;
    int64_t num_partitions_written_ = 0;
    int64_t num_buckets_written_ = 0;
    int64_t last_committed_snapshot_id_ = -1;
};

}  // namespace paimon
