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

#include "paimon/core/io/key_value_data_file_writer.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <optional>
#include <utility>
#include <variant>

#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_array_writer.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/stats/simple_stats_converter.h"
#include "paimon/data/timestamp.h"
#include "paimon/format/format_stats_extractor.h"

struct ArrowArray;

namespace paimon {
class MemoryPool;

KeyValueDataFileWriter::KeyValueDataFileWriter(
    const std::string& compression, std::function<Status(KeyValueBatch&&, ::ArrowArray*)> converter,
    int64_t schema_id, int32_t level, FileSource file_source,
    const std::vector<std::string>& primary_keys,
    const std::shared_ptr<FormatStatsExtractor>& stats_extractor,
    const std::shared_ptr<arrow::Schema>& write_schema, bool is_external_path,
    const std::shared_ptr<MemoryPool>& pool)
    : SingleFileWriter(compression, converter),
      pool_(pool),
      schema_id_(schema_id),
      level_(level),
      file_source_(file_source),
      primary_keys_(primary_keys),
      stats_extractor_(stats_extractor),
      write_schema_(write_schema),
      is_external_path_(is_external_path),
      disable_stats_(stats_extractor == nullptr) {}

Status KeyValueDataFileWriter::Write(KeyValueBatch batch) {
    // update min and max key
    if (!min_key_) {
        min_key_ = batch.min_key;
    }
    max_key_ = batch.max_key;
    // update min/max sequence number
    min_sequence_number_ = std::min(min_sequence_number_, batch.min_sequence_number);
    max_sequence_number_ = std::max(max_sequence_number_, batch.max_sequence_number);
    // update delete row count
    delete_row_count_ += batch.delete_row_count;
    PAIMON_RETURN_NOT_OK(SingleFileWriter::Write(std::move(batch)));
    return Status::OK();
}

Result<std::shared_ptr<DataFileMeta>> KeyValueDataFileWriter::GetResult() {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<ColumnStats>> field_stats, GetFieldStats());
    if (!disable_stats_ && field_stats.size() != static_cast<size_t>(write_schema_->num_fields())) {
        return Status::Invalid("invalid field stats, mismatch with write schema");
    }
    // min/max key
    BinaryRow min_key(primary_keys_.size());
    BinaryRow max_key(primary_keys_.size());
    PAIMON_RETURN_NOT_OK(GenerateMinMaxKey(&min_key, &max_key));

    // key value stats
    SimpleStats key_stats = SimpleStats::EmptyStats();
    SimpleStats value_stats = SimpleStats::EmptyStats();
    if (!disable_stats_) {
        PAIMON_RETURN_NOT_OK(GenerateKeyValueStats(field_stats, &key_stats, &value_stats));
    } else {
        PAIMON_RETURN_NOT_OK(GenerateKeyStatsWithAllNull(&key_stats));
    }
    // TODO(xinyu.lxy): do not support write value stats cols & first_row_id & write_cols for now
    std::optional<std::string> final_path;
    if (is_external_path_) {
        PAIMON_ASSIGN_OR_RAISE(Path external_path, PathUtil::ToPath(path_));
        final_path = external_path.ToString();
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t local_micro, DateTimeUtils::GetCurrentLocalTimeUs());
    return std::make_shared<DataFileMeta>(
        PathUtil::GetName(path_), output_bytes_, RecordCount(), min_key, max_key, key_stats,
        value_stats, min_sequence_number_, max_sequence_number_, schema_id_, level_,
        /*extra_files=*/std::vector<std::optional<std::string>>(),
        Timestamp(/*millisecond=*/local_micro / 1000, /*nano_of_millisecond=*/0), delete_row_count_,
        /*embedded_index=*/nullptr, file_source_,
        /*value_stats_cols=*/std::nullopt, final_path, /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
}

Status KeyValueDataFileWriter::GenerateMinMaxKey(BinaryRow* min_key, BinaryRow* max_key) const {
    BinaryRowWriter min_writer(min_key, /*initial_size=*/1024, pool_.get());
    BinaryRowWriter max_writer(max_key, /*initial_size=*/1024, pool_.get());
    min_writer.Reset();
    max_writer.Reset();
    for (size_t i = 0; i < primary_keys_.size(); ++i) {
        auto data_type = write_schema_->GetFieldByName(primary_keys_[i])->type();
        InternalRow::FieldGetterFunc getter;
        PAIMON_ASSIGN_OR_RAISE(getter,
                               InternalRow::CreateFieldGetter(i, data_type, /*use_view=*/true));
        BinaryRowWriter::FieldSetterFunc setter;
        PAIMON_ASSIGN_OR_RAISE(setter, BinaryRowWriter::CreateFieldSetter(i, data_type));
        setter(getter(*min_key_), &min_writer);
        setter(getter(*max_key_), &max_writer);
    }
    min_writer.Complete();
    max_writer.Complete();
    return Status::OK();
}

Status KeyValueDataFileWriter::GenerateKeyValueStats(
    const std::vector<std::shared_ptr<ColumnStats>>& field_stats, SimpleStats* key_stats,
    SimpleStats* value_stats) const {
    // key stats
    std::vector<std::shared_ptr<ColumnStats>> key_column_stats;
    key_column_stats.reserve(primary_keys_.size());
    for (const auto& key : primary_keys_) {
        int32_t idx = write_schema_->GetFieldIndex(key);
        if (idx == -1) {
            return Status::Invalid(
                fmt::format("cannot find primary key field {} in write schema", key));
        }
        key_column_stats.push_back(field_stats[idx]);
    }
    PAIMON_ASSIGN_OR_RAISE(*key_stats,
                           SimpleStatsConverter::ToBinary(key_column_stats, pool_.get()));
    // value stats
    std::vector<std::shared_ptr<ColumnStats>> value_column_stats(
        field_stats.begin() + SpecialFields::KEY_VALUE_SPECIAL_FIELD_COUNT, field_stats.end());
    PAIMON_ASSIGN_OR_RAISE(*value_stats,
                           SimpleStatsConverter::ToBinary(value_column_stats, pool_.get()));
    return Status::OK();
}

Status KeyValueDataFileWriter::GenerateKeyStatsWithAllNull(SimpleStats* key_stats) const {
    BinaryRow min_values(primary_keys_.size());
    BinaryRow max_values(primary_keys_.size());
    BinaryArray null_counts;

    BinaryRowWriter min_writer(&min_values, /*initial_size=*/0, pool_.get());
    BinaryRowWriter max_writer(&max_values, /*initial_size=*/0, pool_.get());
    BinaryArrayWriter null_counts_writer(&null_counts, primary_keys_.size(), sizeof(int64_t),
                                         pool_.get());
    min_writer.Reset();
    max_writer.Reset();
    null_counts_writer.Reset();

    for (size_t i = 0; i < primary_keys_.size(); ++i) {
        auto data_type = write_schema_->GetFieldByName(primary_keys_[i])->type();
        BinaryRowWriter::FieldSetterFunc setter;
        PAIMON_ASSIGN_OR_RAISE(setter, BinaryRowWriter::CreateFieldSetter(i, data_type));
        setter(NullType(), &min_writer);
        setter(NullType(), &max_writer);
        null_counts_writer.SetNullAt(i);
    }
    min_writer.Complete();
    max_writer.Complete();
    null_counts_writer.Complete();
    *key_stats = SimpleStats(min_values, max_values, null_counts);
    return Status::OK();
}

Result<std::vector<std::shared_ptr<ColumnStats>>> KeyValueDataFileWriter::GetFieldStats() {
    if (!closed_) {
        return Status::Invalid("Cannot access metric unless the writer is closed.");
    }
    if (disable_stats_) {
        return std::vector<std::shared_ptr<ColumnStats>>();
    }
    if (stats_extractor_ == nullptr) {
        assert(false);
        return Status::Invalid("simple stats extractor is null pointer.");
    }
    return stats_extractor_->Extract(fs_, path_, pool_);
}

}  // namespace paimon
