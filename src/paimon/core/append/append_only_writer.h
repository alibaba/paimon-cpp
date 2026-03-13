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

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/common/data/blob_utils.h"
#include "paimon/core/compact/compact_manager.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/single_file_writer.h"
#include "paimon/core/utils/batch_writer.h"
#include "paimon/result.h"
#include "paimon/status.h"

struct ArrowSchema;
struct ArrowArray;

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class CommitIncrement;
class RecordBatch;
template <typename T, typename R>
class RollingFileWriter;
class LongCounter;
class DataFilePathFactory;
class MemoryPool;
class Metrics;
class FormatStatsExtractor;
class WriterBuilder;

class AppendOnlyWriter : public BatchWriter {
 public:
    AppendOnlyWriter(const CoreOptions& options, int64_t schema_id,
                     const std::shared_ptr<arrow::Schema>& write_schema,
                     const std::optional<std::vector<std::string>>& write_cols,
                     int64_t max_sequence_number,
                     const std::shared_ptr<DataFilePathFactory>& path_factory,
                     const std::shared_ptr<CompactManager>& compact_manager,
                     const std::shared_ptr<MemoryPool>& memory_pool);

    ~AppendOnlyWriter() override;

    Status Write(std::unique_ptr<RecordBatch>&& batch) override;
    Status Compact(bool full_compaction) override {
        return Flush(/*wait_for_latest_compaction=*/true, full_compaction);
    }
    Result<CommitIncrement> PrepareCommit(bool wait_compaction) override;
    Result<bool> CompactNotCompleted() override {
        PAIMON_RETURN_NOT_OK(compact_manager_->TriggerCompaction(/*full_compaction=*/false));
        return compact_manager_->CompactNotCompleted();
    }
    Status Sync() override;
    Status Close() override;
    std::shared_ptr<Metrics> GetMetrics() const override {
        return metrics_;
    }

 private:
    using SingleFileWriterCreator = std::function<
        Result<std::unique_ptr<SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>>()>;
    using RollingFileWriterResult =
        Result<std::unique_ptr<RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>>;

    RollingFileWriterResult CreateRollingRowWriter() const;
    RollingFileWriterResult CreateRollingBlobWriter(
        const BlobUtils::SeparatedSchemas& schemas) const;

    Result<CommitIncrement> DrainIncrement();
    Status Flush(bool wait_for_latest_compaction, bool forced_full_compaction);

    SingleFileWriterCreator GetDataFileWriterCreator(
        const std::shared_ptr<arrow::Schema>& schema,
        const std::optional<std::vector<std::string>>& write_cols) const;

    SingleFileWriterCreator GetBlobFileWriterCreator(
        const std::shared_ptr<WriterBuilder>& writer_builder,
        const std::shared_ptr<FormatStatsExtractor>& stats_extractor,
        const std::optional<std::vector<std::string>>& write_cols) const;

    Status TrySyncLatestCompaction(bool blocking);
    Status UpdateCompactDeletionFile(const std::shared_ptr<CompactDeletionFile>& new_deletion_file);

    CoreOptions options_;
    int64_t schema_id_;
    std::shared_ptr<arrow::Schema> write_schema_;
    std::optional<std::vector<std::string>> write_cols_;
    std::shared_ptr<LongCounter> seq_num_counter_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
    std::shared_ptr<CompactManager> compact_manager_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<Metrics> metrics_;

    std::vector<std::shared_ptr<DataFileMeta>> new_files_;
    std::vector<std::shared_ptr<DataFileMeta>> deleted_files_;
    std::vector<std::shared_ptr<DataFileMeta>> compact_before_;
    std::vector<std::shared_ptr<DataFileMeta>> compact_after_;

    std::shared_ptr<CompactDeletionFile> compact_deletion_file_;
    std::unique_ptr<RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>> writer_;
};

}  // namespace paimon
