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
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/single_file_writer.h"
#include "paimon/core/key_value.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow
struct ArrowArray;

namespace paimon {
class ColumnStats;
class FormatStatsExtractor;
class BinaryRow;
class InternalRow;
class MemoryPool;
class SimpleStats;

class KeyValueDataFileWriter
    : public SingleFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>> {
 public:
    KeyValueDataFileWriter(const std::string& compression,
                           std::function<Status(KeyValueBatch&&, ::ArrowArray*)> converter,
                           int64_t schema_id, int32_t level, FileSource file_source,
                           const std::vector<std::string>& primary_keys,
                           const std::shared_ptr<FormatStatsExtractor>& stats_extractor,
                           const std::shared_ptr<arrow::Schema>& write_schema,
                           bool is_external_path, const std::shared_ptr<MemoryPool>& pool);

    Status Write(KeyValueBatch batch) override;

    Result<std::shared_ptr<DataFileMeta>> GetResult() override;

 private:
    Result<std::vector<std::shared_ptr<ColumnStats>>> GetFieldStats();

    Status GenerateMinMaxKey(BinaryRow* min_key, BinaryRow* max_key) const;

    Status GenerateKeyValueStats(const std::vector<std::shared_ptr<ColumnStats>>& field_stats,
                                 SimpleStats* key_stats, SimpleStats* value_stats) const;
    Status GenerateKeyStatsWithAllNull(SimpleStats* key_stats) const;

 private:
    std::shared_ptr<MemoryPool> pool_;
    int64_t schema_id_;
    int32_t level_;
    FileSource file_source_;
    std::vector<std::string> primary_keys_;
    std::shared_ptr<FormatStatsExtractor> stats_extractor_;
    std::shared_ptr<arrow::Schema> write_schema_;
    bool is_external_path_;
    bool disable_stats_;

    int64_t delete_row_count_ = 0;
    int64_t min_sequence_number_ = std::numeric_limits<int64_t>::max();
    int64_t max_sequence_number_ = std::numeric_limits<int64_t>::min();
    std::shared_ptr<InternalRow> min_key_;
    std::shared_ptr<InternalRow> max_key_;
};

}  // namespace paimon
