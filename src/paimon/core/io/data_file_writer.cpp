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

#include "paimon/core/io/data_file_writer.h"

#include <cassert>

#include "arrow/c/abi.h"
#include "paimon/common/utils/long_counter.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/stats/simple_stats_converter.h"
#include "paimon/format/format_stats_extractor.h"

namespace paimon {
class MemoryPool;

DataFileWriter::DataFileWriter(
    const std::string& compression, std::function<Status(::ArrowArray*, ::ArrowArray*)> converter,
    int64_t schema_id, const std::shared_ptr<LongCounter>& seq_num_counter, FileSource file_source,
    const std::shared_ptr<FormatStatsExtractor>& stats_extractor, bool is_external_path,
    const std::optional<std::vector<std::string>>& write_cols,
    const std::shared_ptr<MemoryPool>& pool)
    : SingleFileWriter(compression, converter),
      pool_(pool),
      schema_id_(schema_id),
      is_external_path_(is_external_path),
      seq_num_counter_(seq_num_counter),
      file_source_(file_source),
      stats_extractor_(stats_extractor),
      write_cols_(write_cols) {}

Status DataFileWriter::Write(ArrowArray* batch) {
    int64_t record_count = batch->length;
    PAIMON_RETURN_NOT_OK(SingleFileWriter::Write(batch));
    seq_num_counter_->Add(record_count);
    return Status::OK();
}

Result<std::shared_ptr<DataFileMeta>> DataFileWriter::GetResult() {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<ColumnStats>> field_stats, GetFieldStats());
    PAIMON_ASSIGN_OR_RAISE(SimpleStats stats,
                           SimpleStatsConverter::ToBinary(field_stats, pool_.get()));
    // TODO(xinyu.lxy): do not support write value stats cols for now
    std::optional<std::string> final_path;
    if (is_external_path_) {
        PAIMON_ASSIGN_OR_RAISE(Path external_path, PathUtil::ToPath(path_));
        final_path = external_path.ToString();
    }
    return DataFileMeta::ForAppend(
        PathUtil::GetName(path_), output_bytes_, RecordCount(), stats,
        seq_num_counter_->GetValue() - RecordCount(), seq_num_counter_->GetValue() - 1, schema_id_,
        {}, /*embedded_index=*/nullptr, file_source_, /*value_stats_cols=*/std::nullopt, final_path,
        /*first_row_id=*/std::nullopt, write_cols_);
}

Result<std::vector<std::shared_ptr<ColumnStats>>> DataFileWriter::GetFieldStats() {
    if (!closed_) {
        return Status::Invalid("Cannot access metric unless the writer is closed.");
    }
    if (stats_extractor_ == nullptr) {
        assert(false);
        return Status::Invalid("simple stats extractor is null pointer.");
    }
    return stats_extractor_->Extract(fs_, path_, pool_);
}

}  // namespace paimon
