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

#include "paimon/core/operation/abstract_file_store_write.h"

#include <algorithm>
#include <cassert>
#include <map>
#include <optional>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/batch_writer.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/macros.h"
#include "paimon/record_batch.h"
#include "paimon/scan_context.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class Executor;
class MemoryPool;

AbstractFileStoreWrite::AbstractFileStoreWrite(
    const std::shared_ptr<FileStorePathFactory>& file_store_path_factory,
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager, const std::string& commit_user,
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::Schema>& write_schema,
    const std::shared_ptr<arrow::Schema>& partition_schema, const CoreOptions& options,
    bool ignore_previous_files, bool is_streaming_mode, bool ignore_num_bucket_check,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      executor_(executor),
      file_store_path_factory_(file_store_path_factory),
      snapshot_manager_(snapshot_manager),
      schema_manager_(schema_manager),
      commit_user_(commit_user),
      root_path_(root_path),
      schema_(schema),
      write_schema_(write_schema),
      table_schema_(table_schema),
      partition_schema_(partition_schema),
      options_(options),
      ignore_previous_files_(ignore_previous_files),
      is_streaming_mode_(is_streaming_mode),
      ignore_num_bucket_check_(ignore_num_bucket_check),
      metrics_(std::make_shared<MetricsImpl>()),
      logger_(Logger::GetLogger("AbstractFileStoreWrite")) {}

Status AbstractFileStoreWrite::Write(std::unique_ptr<RecordBatch>&& batch) {
    if (PAIMON_UNLIKELY(batch == nullptr)) {
        return Status::Invalid("batch is null pointer");
    }
    // in FileStoreWrite::Create() we have checked the table kind and bucket mode, here we only
    // check the bucket id in batch
    if (options_.GetBucket() == -1) {
        assert(table_schema_->PrimaryKeys().empty());
        if (!batch->HasSpecifiedBucket()) {
            batch->SetBucket(BucketModeDefine::UNAWARE_BUCKET);
        } else if (batch->GetBucket() != BucketModeDefine::UNAWARE_BUCKET) {
            return Status::Invalid(
                fmt::format("batch bucket is {} while options bucket is -1", batch->GetBucket()));
        }
    } else if (options_.GetBucket() == BucketModeDefine::POSTPONE_BUCKET) {
        assert(!table_schema_->PrimaryKeys().empty());
        if (!batch->HasSpecifiedBucket()) {
            batch->SetBucket(BucketModeDefine::POSTPONE_BUCKET);
        } else if (batch->GetBucket() != BucketModeDefine::POSTPONE_BUCKET) {
            return Status::Invalid(
                fmt::format("batch bucket is {} while options bucket is -2", batch->GetBucket()));
        }
    } else {
        assert(options_.GetBucket() > 0);
        if (!(batch->GetBucket() >= 0 && batch->GetBucket() < options_.GetBucket())) {
            return Status::Invalid(
                fmt::format("fixed bucketed mode must specify a bucket which in [0, {}) in "
                            "RecordBatch",
                            options_.GetBucket()));
        }
    }
    // check nullability
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> data,
        arrow::ImportArray(batch->GetData(), arrow::struct_(write_schema_->fields())));
    PAIMON_RETURN_NOT_OK(ArrowUtils::CheckNullabilityMatch(write_schema_, data));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*data, batch->GetData()));

    PAIMON_ASSIGN_OR_RAISE(BinaryRow partition,
                           file_store_path_factory_->ToBinaryRow(batch->GetPartition()))
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BatchWriter> writer,
                           GetWriter(partition, batch->GetBucket()));
    assert(writer);
    return writer->Write(std::move(batch));
}

Result<std::vector<std::shared_ptr<CommitMessage>>> AbstractFileStoreWrite::PrepareCommit(
    bool wait_compaction, int64_t commit_identifier) {
    if (batch_committed_) {
        return Status::Invalid("batch write mode only support one-time committing.");
    }
    if (is_streaming_mode_ == false) {
        // batch write prepare commit will ignore these params
        batch_committed_ = true;
        wait_compaction = true;
        commit_identifier = std::numeric_limits<int64_t>::max();
    }
    int64_t latest_committed_identifier = std::numeric_limits<int64_t>::min();
    for (const auto& kv : writers_) {
        const auto& buckets = kv.second;
        for (const auto& kv : buckets) {
            const auto& writer_container = kv.second;
            latest_committed_identifier = std::max(
                latest_committed_identifier, writer_container.last_modified_commit_identifier);
        }
    }
    if (latest_committed_identifier == std::numeric_limits<int64_t>::min()) {
        // Optimization for the first commit.
        //
        // If this is the first commit, no writer has previous modified commit, so the value of
        // `latestCommittedIdentifier` does not matter.
        //
        // Without this optimization, we may need to scan through all snapshots only to find
        // that there is no previous snapshot by this user, which is very inefficient.
    } else {
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> latest_snapshot,
                               snapshot_manager_->LatestSnapshotOfUser(commit_user_));
        if (latest_snapshot == std::nullopt) {
            latest_committed_identifier = std::numeric_limits<int64_t>::min();
        } else {
            latest_committed_identifier = latest_snapshot.value().CommitIdentifier();
        }
    }

    std::vector<std::shared_ptr<CommitMessage>> result;
    auto metrics = std::make_shared<MetricsImpl>();
    for (auto partition_iter = writers_.begin(); partition_iter != writers_.end();) {
        auto& partition = partition_iter->first;
        auto& buckets = partition_iter->second;
        for (auto bucket_iter = buckets.begin(); bucket_iter != buckets.end();) {
            int32_t bucket = bucket_iter->first;
            WriterContainer<BatchWriter>& writer_container = bucket_iter->second;
            PAIMON_ASSIGN_OR_RAISE(CommitIncrement increment,
                                   writer_container.writer->PrepareCommit(wait_compaction));
            auto committable = std::make_shared<CommitMessageImpl>(
                partition, bucket, writer_container.total_buckets, increment.GetNewFilesIncrement(),
                increment.GetCompactIncrement());
            result.push_back(committable);
            if (committable->IsEmpty()) {
                // Condition 1: There is no more record waiting to be committed. Note that the
                // condition is < (instead of <=), because each commit identifier may have
                // multiple snapshots. We must make sure all snapshots of this identifier are
                // committed.
                // Condition 2: No compaction is in progress. That is, no more changelog will be
                // produced.
                //
                // Condition 3: The writer has no postponed compaction like gentle lookup
                // compaction.
                if (writer_container.last_modified_commit_identifier <
                        latest_committed_identifier &&
                    !writer_container.writer->IsCompacting()) {
                    // Clear writer if no update, and if its latest modification has committed.
                    //
                    // We need a mechanism to clear writers, otherwise there will be more and
                    // more such as yesterday's partition that no longer needs to be written.
                    PAIMON_LOG_DEBUG(logger_,
                                     "Closing writer for partition %s, bucket %d. "
                                     "Writer's last modified identifier is %ld, "
                                     "while latest committed identifier is %ld, "
                                     "current commit identifier is %ld.",
                                     partition.ToString().c_str(), bucket,
                                     writer_container.last_modified_commit_identifier,
                                     latest_committed_identifier, commit_identifier);
                    PAIMON_RETURN_NOT_OK(writer_container.writer->Close());
                    bucket_iter = buckets.erase(bucket_iter);
                } else {
                    metrics->Merge(writer_container.writer->GetMetrics());
                    ++bucket_iter;
                }
            } else {
                writer_container.last_modified_commit_identifier = commit_identifier;
                metrics->Merge(writer_container.writer->GetMetrics());
                ++bucket_iter;
            }
        }

        if (buckets.empty()) {
            partition_iter = writers_.erase(partition_iter);
        } else {
            ++partition_iter;
        }
    }
    metrics_->Overwrite(metrics);
    return result;
}

Status AbstractFileStoreWrite::Close() {
    for (auto& [_, bucket_writers] : writers_) {
        for (auto& [_, writer_container] : bucket_writers) {
            PAIMON_RETURN_NOT_OK(writer_container.writer->Close());
        }
    }
    writers_.clear();
    return Status::OK();
}

std::shared_ptr<Metrics> AbstractFileStoreWrite::GetMetrics() const {
    return metrics_;
}

int32_t AbstractFileStoreWrite::GetDefaultBucketNum() const {
    return options_.GetBucket();
}

Result<int32_t> AbstractFileStoreWrite::ScanExistingFileMetas(
    const Snapshot& snapshot, const BinaryRow& partition, int32_t bucket,
    std::vector<std::shared_ptr<DataFileMeta>>* restore_files) const {
    PAIMON_ASSIGN_OR_RAISE(auto part_values,
                           file_store_path_factory_->GeneratePartitionVector(partition));
    std::map<std::string, std::string> part_values_map;
    for (const auto& [key, value] : part_values) {
        part_values_map[key] = value;
    }
    std::vector<std::map<std::string, std::string>> partition_filters;
    if (!part_values_map.empty()) {
        partition_filters.push_back(part_values_map);
    }
    auto scan_filter = std::make_shared<ScanFilter>(
        /*predicate=*/nullptr, partition_filters, std::optional<int32_t>(bucket),
        /*vector_search=*/nullptr);

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreScan> scan, CreateFileStoreScan(scan_filter));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStoreScan::RawPlan> plan,
                           scan->WithSnapshot(snapshot)->CreatePlan());
    std::vector<ManifestEntry> entries = plan->Files();
    int32_t total_buckets = GetDefaultBucketNum();
    for (auto& entry : entries) {
        if (!ignore_num_bucket_check_ && entry.TotalBuckets() != options_.GetBucket()) {
            return Status::Invalid(fmt::format(
                "Try to write table with a new bucket num {}, but the previous "
                "bucket num is {}. Please switch to batch mode, and perform INSERT OVERWRITE to "
                "rescale current data layout first.",
                options_.GetBucket(), entry.TotalBuckets()));
        }
        total_buckets = entry.TotalBuckets();
        restore_files->push_back(std::move(entry.File()));
    }
    return total_buckets;
}

Result<std::shared_ptr<BatchWriter>> AbstractFileStoreWrite::GetWriter(const BinaryRow& partition,
                                                                       int32_t bucket) {
    auto iter = writers_.find(partition);
    if (PAIMON_UNLIKELY(iter == writers_.end())) {
        PAIMON_ASSIGN_OR_RAISE(auto result,
                               CreateWriter(partition, bucket, ignore_previous_files_));
        int32_t total_buckets = result.first;
        std::shared_ptr<BatchWriter> writer = result.second;
        writers_.emplace(partition,
                         std::unordered_map<int32_t, WriterContainer<BatchWriter>>(
                             {{bucket, WriterContainer<BatchWriter>(writer, total_buckets)}}));
        return writer;
    } else {
        auto& buckets = iter->second;
        auto iter = buckets.find(bucket);
        if (PAIMON_LIKELY(iter != buckets.end())) {
            return iter->second.writer;
        } else {
            PAIMON_ASSIGN_OR_RAISE(auto result,
                                   CreateWriter(partition, bucket, ignore_previous_files_));
            int32_t total_buckets = result.first;
            std::shared_ptr<BatchWriter> writer = result.second;
            buckets.emplace(bucket, WriterContainer<BatchWriter>(writer, total_buckets));
            return writer;
        }
    }
}

}  // namespace paimon
