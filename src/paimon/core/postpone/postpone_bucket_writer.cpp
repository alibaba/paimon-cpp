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

#include "paimon/core/postpone/postpone_bucket_writer.h"

#include <cassert>
#include <cstring>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/util.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/scalar.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/io/key_value_data_file_writer.h"
#include "paimon/core/io/single_file_writer.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/format/file_format.h"
#include "paimon/format/writer_builder.h"
#include "paimon/metrics.h"

namespace paimon {
class InternalRow;
class MemoryPool;

PostponeBucketWriter::PostponeBucketWriter(const std::vector<std::string>& trimmed_primary_keys,
                                           const std::shared_ptr<DataFilePathFactory>& path_factory,
                                           int64_t schema_id,
                                           const std::shared_ptr<arrow::Schema>& value_schema,
                                           const CoreOptions& options,
                                           const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      arrow_pool_(GetArrowPool(pool)),
      trimmed_primary_keys_(trimmed_primary_keys),
      options_(options),
      path_factory_(path_factory),
      schema_id_(schema_id),
      value_type_(arrow::struct_(value_schema->fields())),
      metrics_(std::make_shared<MetricsImpl>()) {
    arrow::FieldVector target_fields;
    target_fields.push_back(
        DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber()));
    target_fields.push_back(DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind()));
    target_fields.insert(target_fields.end(), value_schema->fields().begin(),
                         value_schema->fields().end());
    write_schema_ = arrow::schema(target_fields);
}

Status PostponeBucketWriter::Write(std::unique_ptr<RecordBatch>&& moved_batch) {
    if (moved_batch->GetData()->length == 0) {
        return Status::OK();
    }
    std::unique_ptr<RecordBatch> batch = std::move(moved_batch);
    // check input array
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::StructArray> value_struct_array,
                           CheckAndCastValueArray(batch->GetData()));

    // prepare sequence number & row kind array
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> sequence_number_array,
                           PrepareSequenceNumberArray(value_struct_array->length()));

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> row_kind_array,
                           PrepareRowKindArray(value_struct_array->length(), batch->GetRowKind()));

    // make value array and special array into a write array
    arrow::ArrayVector write_fields = {sequence_number_array, row_kind_array};
    write_fields.insert(write_fields.end(), value_struct_array->fields().begin(),
                        value_struct_array->fields().end());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> write_array,
        arrow::StructArray::Make(write_fields, write_schema_->field_names()));
    // prepare min max key
    std::pair<std::shared_ptr<InternalRow>, std::shared_ptr<InternalRow>> min_max_key;
    PAIMON_ASSIGN_OR_RAISE(min_max_key, PrepareMinMaxKey(value_struct_array));

    // construct KeyValueBatch
    KeyValueBatch key_value_batch;
    key_value_batch.min_sequence_number = KeyValue::UNKNOWN_SEQUENCE;
    key_value_batch.max_sequence_number = KeyValue::UNKNOWN_SEQUENCE;
    key_value_batch.delete_row_count = GetDeleteRowCount(batch->GetRowKind());
    key_value_batch.batch = std::make_unique<ArrowArray>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*write_array, key_value_batch.batch.get()));
    key_value_batch.min_key = min_max_key.first;
    key_value_batch.max_key = min_max_key.second;

    // write KeyValueBatch to RollingFileWriter
    if (!writer_) {
        writer_ = CreateRollingRowWriter();
    }
    PAIMON_RETURN_NOT_OK(writer_->Write(std::move(key_value_batch)));
    return Status::OK();
}

Result<CommitIncrement> PostponeBucketWriter::PrepareCommit(bool wait_compaction) {
    PAIMON_RETURN_NOT_OK(Flush());
    return DrainIncrement();
}

Result<std::shared_ptr<arrow::StructArray>> PostponeBucketWriter::CheckAndCastValueArray(
    ArrowArray* value_array) const {
    if (ArrowArrayIsReleased(value_array)) {
        return Status::Invalid("invalid batch: data is released");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(value_array, value_type_));
    auto value_struct_array =
        arrow::internal::checked_pointer_cast<arrow::StructArray>(arrow_array);
    if (value_struct_array == nullptr) {
        return Status::Invalid("invalid RecordBatch: cannot cast to StructArray");
    }
    return value_struct_array;
}

Result<std::shared_ptr<arrow::Array>> PostponeBucketWriter::PrepareSequenceNumberArray(
    int32_t value_array_length) {
    if (!sequence_number_array_ || sequence_number_array_->length() < value_array_length) {
        auto sequence_number_scalar =
            std::make_shared<arrow::Int64Scalar>(KeyValue::UNKNOWN_SEQUENCE);
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            sequence_number_array_,
            arrow::MakeArrayFromScalar(*sequence_number_scalar, value_array_length,
                                       arrow_pool_.get()));
        return sequence_number_array_;
    }
    assert(sequence_number_array_->length() >= value_array_length);
    return sequence_number_array_->Slice(0, value_array_length);
}

Result<std::shared_ptr<arrow::Array>> PostponeBucketWriter::PrepareRowKindArray(
    int32_t value_array_length, const std::vector<RecordBatch::RowKind>& row_kind_vec) {
    std::shared_ptr<arrow::NumericArray<arrow::Int8Type>> row_kind_array;
    if (!row_kind_array_ || row_kind_array_->length() < value_array_length) {
        auto row_kind_scalar =
            std::make_shared<arrow::Int8Scalar>(RowKind::Insert()->ToByteValue());
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> scalar_array,
            arrow::MakeArrayFromScalar(*row_kind_scalar, value_array_length, arrow_pool_.get()));
        auto typed_row_kind_array =
            arrow::internal::checked_pointer_cast<arrow::NumericArray<arrow::Int8Type>>(
                scalar_array);
        assert(typed_row_kind_array);
        row_kind_array_ = std::move(typed_row_kind_array);
        row_kind_array = row_kind_array_;
    } else {
        assert(row_kind_array_->length() >= value_array_length);
        row_kind_array =
            arrow::internal::checked_pointer_cast<arrow::NumericArray<arrow::Int8Type>>(
                row_kind_array_->Slice(0, value_array_length));
        assert(row_kind_array);
    }

    if (!row_kind_vec.empty()) {
        if (row_kind_vec.size() != static_cast<size_t>(value_array_length)) {
            return Status::Invalid(
                fmt::format("length of row_kind {} mismatches length of value array {}",
                            row_kind_vec.size(), value_array_length));
        }
        memcpy(const_cast<int8_t*>(row_kind_array->raw_values()), row_kind_vec.data(),
               sizeof(int8_t) * value_array_length);
    } else {
        // all are INSERT
        memset(const_cast<int8_t*>(row_kind_array->raw_values()),
               static_cast<int32_t>(RecordBatch::RowKind::INSERT),
               sizeof(int8_t) * value_array_length);
    }
    return row_kind_array;
}

int64_t PostponeBucketWriter::GetDeleteRowCount(
    const std::vector<RecordBatch::RowKind>& row_kind_vec) {
    if (row_kind_vec.empty()) {
        // all are INSERT
        return 0;
    }
    int64_t delete_row_count = 0;
    for (const auto& row_kind : row_kind_vec) {
        if (row_kind == RecordBatch::RowKind::UPDATE_BEFORE ||
            row_kind == RecordBatch::RowKind::DELETE) {
            delete_row_count++;
        }
    }
    return delete_row_count;
}

Result<std::pair<std::shared_ptr<ColumnarRow>, std::shared_ptr<ColumnarRow>>>
PostponeBucketWriter::PrepareMinMaxKey(
    const std::shared_ptr<arrow::StructArray>& value_struct_array) const {
    if (value_struct_array->length() <= 0) {
        return Status::Invalid(
            "in PostponeBucketWriter, for PrepareMinMaxKey, value array should not be empty");
    }
    // same as java, in postpone bucket, we do not sort pk, min and max key simply use the first and
    // last key
    arrow::ArrayVector key_array_vec;
    key_array_vec.reserve(trimmed_primary_keys_.size());
    for (const auto& pk : trimmed_primary_keys_) {
        auto key_array = value_struct_array->GetFieldByName(pk);
        if (key_array == nullptr) {
            return Status::Invalid(
                fmt::format("primary key {} not in input array in PostponeBucketWriter", pk));
        }
        key_array_vec.push_back(key_array);
    }
    return std::make_pair(
        std::make_shared<ColumnarRow>(value_struct_array, key_array_vec, pool_, 0),
        std::make_shared<ColumnarRow>(value_struct_array, key_array_vec, pool_,
                                      value_struct_array->length() - 1));
}

std::unique_ptr<RollingFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>
PostponeBucketWriter::CreateRollingRowWriter() const {
    auto create_file_writer = [&]()
        -> Result<std::unique_ptr<SingleFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>> {
        ::ArrowSchema arrow_schema;
        ScopeGuard guard([&arrow_schema]() { ArrowSchemaRelease(&arrow_schema); });
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*write_schema_, &arrow_schema));
        auto format = options_.GetWriteFileFormat();
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<WriterBuilder> writer_builder,
            format->CreateWriterBuilder(&arrow_schema, options_.GetWriteBatchSize()));
        writer_builder->WithMemoryPool(pool_);
        auto converter = [](KeyValueBatch key_value_batch, ArrowArray* array) -> Status {
            ArrowArrayMove(key_value_batch.batch.get(), array);
            return Status::OK();
        };
        auto writer = std::make_unique<KeyValueDataFileWriter>(
            options_.GetFileCompression(), converter, schema_id_, FileSource::Append(),
            trimmed_primary_keys_, /*stats_extractor=*/nullptr, write_schema_,
            path_factory_->IsExternalPath(), pool_);
        PAIMON_RETURN_NOT_OK(
            writer->Init(options_.GetFileSystem(), path_factory_->NewPath(), writer_builder));
        return writer;
    };
    return std::make_unique<RollingFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>(
        options_.GetTargetFileSize(/*has_primary_key=*/true), create_file_writer);
}

Status PostponeBucketWriter::Flush() {
    if (!writer_) {
        return Status::OK();
    }
    PAIMON_RETURN_NOT_OK(writer_->Close());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> flushed_files,
                           writer_->GetResult());
    new_files_.insert(new_files_.end(), flushed_files.begin(), flushed_files.end());
    metrics_->Merge(writer_->GetMetrics());
    writer_.reset();
    return Status::OK();
}

Result<CommitIncrement> PostponeBucketWriter::DrainIncrement() {
    DataIncrement data_increment(std::move(new_files_), /*deleted_files=*/{}, {});
    CompactIncrement compact_increment({}, {}, {});
    new_files_.clear();
    return CommitIncrement(data_increment, compact_increment);
}

}  // namespace paimon
