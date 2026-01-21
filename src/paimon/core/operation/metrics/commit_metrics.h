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

#pragma once

namespace paimon {

/// Metrics to measure a commit.
class CommitMetrics {
 public:
    static constexpr char LAST_COMMIT_DURATION[] = "lastCommitDuration";
    static constexpr char LAST_COMMIT_ATTEMPTS[] = "lastCommitAttempts";
    static constexpr char LAST_TABLE_FILES_ADDED[] = "lastTableFilesAdded";
    static constexpr char LAST_TABLE_FILES_DELETED[] = "lastTableFilesDeleted";
    static constexpr char LAST_TABLE_FILES_APPENDED[] = "lastTableFilesAppended";
    static constexpr char LAST_TABLE_FILES_COMMIT_COMPACTED[] = "lastTableFilesCommitCompacted";
    static constexpr char LAST_CHANGELOG_FILES_APPENDED[] = "lastChangelogFilesAppended";
    static constexpr char LAST_CHANGELOG_FILES_COMMIT_COMPACTED[] =
        "lastChangelogFileCommitCompacted";
    static constexpr char LAST_GENERATED_SNAPSHOTS[] = "lastGeneratedSnapshots";
    static constexpr char LAST_DELTA_RECORDS_APPENDED[] = "lastDeltaRecordsAppended";
    static constexpr char LAST_CHANGELOG_RECORDS_APPENDED[] = "lastChangelogRecordsAppended";
    static constexpr char LAST_DELTA_RECORDS_COMMIT_COMPACTED[] = "lastDeltaRecordsCommitCompacted";
    static constexpr char LAST_CHANGELOG_RECORDS_COMMIT_COMPACTED[] =
        "lastChangelogRecordsCommitCompacted";
    static constexpr char LAST_PARTITIONS_WRITTEN[] = "lastPartitionsWritten";
    static constexpr char LAST_BUCKETS_WRITTEN[] = "lastBucketsWritten";
    static constexpr char LAST_COMPACTION_INPUT_FILE_SIZE[] = "lastCompactionInputFileSize";
    static constexpr char LAST_COMPACTION_OUTPUT_FILE_SIZE[] = "lastCompactionOutputFileSize";
};

}  // namespace paimon
