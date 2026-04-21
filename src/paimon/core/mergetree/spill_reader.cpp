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

#include "paimon/core/mergetree/spill_reader.h"

#include "paimon/common/data/columnar/columnar_row_ref.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"

namespace paimon {

SpillReader::SpillReader(const std::shared_ptr<FileSystem>& fs,
                         const std::shared_ptr<arrow::Schema>& key_schema,
                         const std::shared_ptr<arrow::Schema>& value_schema,
                         const std::shared_ptr<MemoryPool>& pool)
    : fs_(fs),
      key_schema_(key_schema),
      value_schema_(value_schema),
      pool_(pool),
      arrow_pool_(GetArrowPool(pool)),
      metrics_(std::make_shared<MetricsImpl>()) {}

Result<std::unique_ptr<SpillReader>> SpillReader::Create(
    const std::shared_ptr<FileSystem>& fs, const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema, const std::shared_ptr<MemoryPool>& pool,
    const FileIOChannel::ID& channel_id) {
    std::unique_ptr<SpillReader> reader(new SpillReader(fs, key_schema, value_schema, pool));
    PAIMON_RETURN_NOT_OK(reader->Open(channel_id));
    return reader;
}

Status SpillReader::Open(const FileIOChannel::ID& channel_id) {
    const std::string& file_path = channel_id.GetPath();
    PAIMON_ASSIGN_OR_RAISE(in_stream_, fs_->Open(file_path));
    PAIMON_ASSIGN_OR_RAISE(auto file_status, fs_->GetFileStatus(file_path));
    uint64_t file_len = file_status->GetLen();
    arrow_input_stream_adapter_ =
        std::make_shared<ArrowInputStreamAdapter>(in_stream_, arrow_pool_, file_len);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        arrow_reader_, arrow::ipc::RecordBatchFileReader::Open(arrow_input_stream_adapter_));
    num_record_batches_ = arrow_reader_->num_record_batches();
    current_batch_index_ = 0;
    return Status::OK();
}

SpillReader::Iterator::Iterator(SpillReader* reader) : reader_(reader) {}

bool SpillReader::Iterator::HasNext() const {
    return cursor_ < reader_->batch_length_;
}

Result<KeyValue> SpillReader::Iterator::Next() {
    PAIMON_ASSIGN_OR_RAISE(const RowKind* row_kind,
                           RowKind::FromByteValue(reader_->row_kind_array_->Value(cursor_)));
    int64_t sequence_number = reader_->sequence_number_array_->Value(cursor_);
    auto key = std::make_unique<ColumnarRowRef>(reader_->key_ctx_, cursor_);
    auto value = std::make_unique<ColumnarRowRef>(reader_->value_ctx_, cursor_);
    cursor_++;
    return KeyValue(row_kind, sequence_number, /*level=*/0, std::move(key), std::move(value));
}

Result<std::unique_ptr<KeyValueRecordReader::Iterator>> SpillReader::NextBatch() {
    Reset();
    if (current_batch_index_ >= num_record_batches_) {
        return std::unique_ptr<KeyValueRecordReader::Iterator>();
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::RecordBatch> record_batch,
                                      arrow_reader_->ReadRecordBatch(current_batch_index_));
    current_batch_index_++;

    batch_length_ = record_batch->num_rows();

    auto sequence_number_col =
        record_batch->GetColumnByName(SpecialFields::SequenceNumber().Name());
    if (!sequence_number_col) {
        return Status::Invalid("cannot find _SEQUENCE_NUMBER column in spill file");
    }
    sequence_number_array_ =
        std::dynamic_pointer_cast<arrow::NumericArray<arrow::Int64Type>>(sequence_number_col);
    if (!sequence_number_array_) {
        return Status::Invalid("cannot cast _SEQUENCE_NUMBER column to int64 arrow array");
    }

    auto value_kind_col = record_batch->GetColumnByName(SpecialFields::ValueKind().Name());
    if (!value_kind_col) {
        return Status::Invalid("cannot find _VALUE_KIND column in spill file");
    }
    row_kind_array_ =
        std::dynamic_pointer_cast<arrow::NumericArray<arrow::Int8Type>>(value_kind_col);
    if (!row_kind_array_) {
        return Status::Invalid("cannot cast _VALUE_KIND column to int8 arrow array");
    }

    arrow::ArrayVector key_fields;
    key_fields.reserve(key_schema_->num_fields());
    for (const auto& key_field : key_schema_->fields()) {
        auto col = record_batch->GetColumnByName(key_field->name());
        if (!col) {
            return Status::Invalid("cannot find key field " + key_field->name() + " in spill file");
        }
        key_fields.emplace_back(col);
    }

    arrow::ArrayVector value_fields;
    value_fields.reserve(value_schema_->num_fields());
    for (const auto& value_field : value_schema_->fields()) {
        auto col = record_batch->GetColumnByName(value_field->name());
        if (!col) {
            return Status::Invalid("cannot find value field " + value_field->name() +
                                   " in spill file");
        }
        value_fields.emplace_back(col);
    }

    key_ctx_ = std::make_shared<ColumnarBatchContext>(key_fields, pool_);
    value_ctx_ = std::make_shared<ColumnarBatchContext>(value_fields, pool_);

    return std::make_unique<SpillReader::Iterator>(this);
}

std::shared_ptr<Metrics> SpillReader::GetReaderMetrics() const {
    return metrics_;
}

void SpillReader::Close() {
    Reset();
    arrow_reader_.reset();
    arrow_input_stream_adapter_.reset();
    if (in_stream_) {
        [[maybe_unused]] auto status = in_stream_->Close();
        in_stream_.reset();
    }
}

void SpillReader::Reset() {
    key_ctx_.reset();
    value_ctx_.reset();
    sequence_number_array_.reset();
    row_kind_array_.reset();
    batch_length_ = 0;
}

}  // namespace paimon
