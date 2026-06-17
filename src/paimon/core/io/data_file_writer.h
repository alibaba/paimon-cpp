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

#include "arrow/c/abi.h"
#include "paimon/common/utils/long_counter.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/single_file_writer.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class ColumnStats;
class FormatStatsExtractor;
class LongCounter;
class MemoryPool;

class DataFileWriter : public SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>> {
 public:
    /// Callback invoked during BeforeFinish() to finalize file metadata.
    /// Produces an updated schema with per-field metadata (e.g. shredding metadata)
    /// and may perform other finalization work (e.g. reporting stats to cross-file context).
    using MetadataFinalizer = std::function<Result<std::shared_ptr<arrow::Schema>>()>;

    DataFileWriter(const std::string& compression,
                   std::function<Status(::ArrowArray*, ::ArrowArray*)> converter, int64_t schema_id,
                   const std::shared_ptr<LongCounter>& seq_num_counter, FileSource file_source,
                   const std::shared_ptr<FormatStatsExtractor>& stats_extractor,
                   bool is_external_path, const std::optional<std::vector<std::string>>& write_cols,
                   const std::shared_ptr<MemoryPool>& pool);

    /// Sets the metadata finalizer. Called during BeforeFinish() to produce an updated
    /// schema and perform finalization callbacks. Must be set before Close().
    void SetMetadataFinalizer(MetadataFinalizer finalizer);

    Status Write(::ArrowArray* batch) override;

    Result<std::shared_ptr<DataFileMeta>> GetResult() override;

 protected:
    Status BeforeFinish() override;

 private:
    Result<std::vector<std::shared_ptr<ColumnStats>>> GetFieldStats();

    std::shared_ptr<MemoryPool> pool_;
    int64_t schema_id_;
    bool is_external_path_;

    std::shared_ptr<LongCounter> seq_num_counter_;
    FileSource file_source_;
    std::shared_ptr<FormatStatsExtractor> stats_extractor_;
    std::optional<std::vector<std::string>> write_cols_;
    MetadataFinalizer metadata_finalizer_;
};

}  // namespace paimon
