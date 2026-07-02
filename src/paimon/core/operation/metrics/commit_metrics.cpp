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

#include "paimon/core/operation/metrics/commit_metrics.h"

#include "paimon/core/operation/metrics/commit_stats.h"
#include "paimon/metrics.h"

namespace paimon {

void CommitMetrics::ReportCommit(const std::shared_ptr<Metrics>& metrics,
                                 const CommitStats& commit_stats) {
    metrics->SetCounter(CommitMetrics::LAST_COMMIT_DURATION, commit_stats.GetDuration());
    metrics->SetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS, commit_stats.GetAttempts());
    metrics->SetCounter(CommitMetrics::LAST_TABLE_FILES_ADDED, commit_stats.GetTableFilesAdded());
    metrics->SetCounter(CommitMetrics::LAST_TABLE_FILES_DELETED,
                        commit_stats.GetTableFilesDeleted());
    metrics->SetCounter(CommitMetrics::LAST_TABLE_FILES_APPENDED,
                        commit_stats.GetTableFilesAppended());
    metrics->SetCounter(CommitMetrics::LAST_TABLE_FILES_COMMIT_COMPACTED,
                        commit_stats.GetTableFilesCompacted());
    metrics->SetCounter(CommitMetrics::LAST_CHANGELOG_FILES_APPENDED,
                        commit_stats.GetChangelogFilesAppended());
    metrics->SetCounter(CommitMetrics::LAST_CHANGELOG_FILES_COMMIT_COMPACTED,
                        commit_stats.GetChangelogFilesCompacted());
    metrics->SetCounter(CommitMetrics::LAST_GENERATED_SNAPSHOTS,
                        commit_stats.GetGeneratedSnapshots());
    metrics->SetCounter(CommitMetrics::LAST_DELTA_RECORDS_APPENDED,
                        commit_stats.GetDeltaRecordsAppended());
    metrics->SetCounter(CommitMetrics::LAST_CHANGELOG_RECORDS_APPENDED,
                        commit_stats.GetChangelogRecordsAppended());
    metrics->SetCounter(CommitMetrics::LAST_DELTA_RECORDS_COMMIT_COMPACTED,
                        commit_stats.GetDeltaRecordsCompacted());
    metrics->SetCounter(CommitMetrics::LAST_CHANGELOG_RECORDS_COMMIT_COMPACTED,
                        commit_stats.GetChangelogRecordsCompacted());
    metrics->SetCounter(CommitMetrics::LAST_PARTITIONS_WRITTEN,
                        commit_stats.GetNumPartitionsWritten());
    metrics->SetCounter(CommitMetrics::LAST_BUCKETS_WRITTEN, commit_stats.GetNumBucketsWritten());
    metrics->SetCounter(CommitMetrics::LAST_COMPACTION_INPUT_FILE_SIZE,
                        commit_stats.GetCompactionInputFileSize());
    metrics->SetCounter(CommitMetrics::LAST_COMPACTION_OUTPUT_FILE_SIZE,
                        commit_stats.GetCompactionOutputFileSize());
    metrics->SetCounter(CommitMetrics::LAST_COMMITTED_SNAPSHOT_ID,
                        commit_stats.GetLastCommittedSnapshotId());
}

}  // namespace paimon
