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

#include "paimon/core/io/complete_row_tracking_fields_reader.h"

#include <memory>
#include <string>
#include <utility>

#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"

namespace paimon {
CompleteRowTrackingFieldsBatchReader::CompleteRowTrackingFieldsBatchReader(
    std::unique_ptr<FileBatchReader>&& reader, const std::optional<int64_t>& first_row_id,
    int64_t snapshot_id, const std::shared_ptr<MemoryPool>& pool)
    : first_row_id_(first_row_id),
      snapshot_id_(snapshot_id),
      arrow_pool_(GetArrowPool(pool)),
      reader_(std::move(reader)) {}

Status CompleteRowTrackingFieldsBatchReader::SetReadSchema(
    ::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
    const std::optional<RoaringBitmap32>& selection_bitmap) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowSchema> c_file_schema, reader_->GetFileSchema());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> file_schema,
                                      arrow::ImportSchema(c_file_schema.get()));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(read_schema));
    read_schema_ = arrow_schema;
    int32_t row_id_idx = arrow_schema->GetFieldIndex(SpecialFields::RowId().Name());
    if (row_id_idx != -1 && file_schema->GetFieldIndex(SpecialFields::RowId().Name()) == -1) {
        // read special fields but file not exist, remove special fields to format reader
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(arrow_schema, arrow_schema->RemoveField(row_id_idx));
    }
    int32_t sequence_id_idx = arrow_schema->GetFieldIndex(SpecialFields::SequenceNumber().Name());
    if (sequence_id_idx != -1 &&
        file_schema->GetFieldIndex(SpecialFields::SequenceNumber().Name()) == -1) {
        // read special fields but file not exist, remove special fields to format reader
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(arrow_schema, arrow_schema->RemoveField(sequence_id_idx));
    }
    ArrowSchema c_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*arrow_schema, &c_schema));
    return reader_->SetReadSchema(&c_schema, predicate, selection_bitmap);
}

Result<BatchReader::ReadBatchWithBitmap>
CompleteRowTrackingFieldsBatchReader::NextBatchWithBitmap() {
    if (!read_schema_) {
        return Status::Invalid(
            "in CompleteRowTrackingFieldsBatchReader SetReadSchema is supposed to be called before "
            "NextBatch");
    }
    PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap src_array_with_bitmap,
                           reader_->NextBatchWithBitmap());
    if (BatchReader::IsEofBatch(src_array_with_bitmap)) {
        // read finish
        return src_array_with_bitmap;
    }
    auto& [read_batch, bitmap] = src_array_with_bitmap;
    auto& [c_array, c_schema] = read_batch;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> src_array,
                                      arrow::ImportArray(c_array.get(), c_schema.get()));
    auto src_struct_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(src_array);
    assert(src_struct_array);

    // complete row id array
    std::shared_ptr<arrow::Array> row_id_array;
    std::string row_id_field_name = SpecialFields::RowId().Name();
    if (read_schema_->GetFieldIndex(row_id_field_name) != -1) {
        row_id_array = src_struct_array->GetFieldByName(row_id_field_name);
        uint64_t previous_batch_first_row_number = reader_->GetPreviousBatchFirstRowNumber();
        auto row_id_convert_func = [previous_batch_first_row_number,
                                    this](int32_t idx_in_array) -> Result<int64_t> {
            if (first_row_id_ == std::nullopt) {
                return Status::Invalid(
                    "unexpected: read _ROW_ID special field, but first row id is null in meta");
            }
            return first_row_id_.value() + previous_batch_first_row_number + idx_in_array;
        };
        PAIMON_RETURN_NOT_OK(ConvertRowTrackingField(src_struct_array->length(), /*init_value=*/0,
                                                     row_id_convert_func, &row_id_array));
    }

    // complete sequence number array
    std::string sequence_number_field_name = SpecialFields::SequenceNumber().Name();
    std::shared_ptr<arrow::Array> sequence_number_array;
    if (read_schema_->GetFieldIndex(sequence_number_field_name) != -1) {
        sequence_number_array = src_struct_array->GetFieldByName(sequence_number_field_name);
        PAIMON_RETURN_NOT_OK(ConvertRowTrackingField(src_struct_array->length(),
                                                     /*init_value=*/snapshot_id_,
                                                     /*convert_func=*/nullptr,
                                                     &sequence_number_array));
    }

    // re-construct result array with read schema
    arrow::ArrayVector sub_array_vec;
    sub_array_vec.reserve(read_schema_->num_fields());
    for (const auto& field : read_schema_->fields()) {
        const auto& field_name = field->name();
        if (field_name == row_id_field_name) {
            sub_array_vec.push_back(row_id_array);
        } else if (field_name == sequence_number_field_name) {
            sub_array_vec.push_back(sequence_number_array);
        } else {
            // normal data fields
            auto sub_array = src_struct_array->GetFieldByName(field_name);
            if (!sub_array) {
                return Status::Invalid(fmt::format(
                    "Data file does not contain field {} in CompleteRowTrackingFieldsBatchReader",
                    field_name));
            }
            sub_array_vec.push_back(sub_array);
        }
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> target_array,
        arrow::StructArray::Make(sub_array_vec, read_schema_->field_names()));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*target_array, c_array.get(), c_schema.get()));
    return std::move(src_array_with_bitmap);
}

Status CompleteRowTrackingFieldsBatchReader::ConvertRowTrackingField(
    int64_t array_length, int64_t init_value,
    const std::function<Result<int64_t>(int32_t)>& convert_func,
    std::shared_ptr<arrow::Array>* special_array_ptr) const {
    auto& special_array = *special_array_ptr;
    if (!special_array || special_array->null_count() == special_array->length()) {
        // condition1: special field not exist in file
        // condition2: special field all null
        auto scalar = std::make_shared<arrow::Int64Scalar>(init_value);
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            special_array, arrow::MakeArrayFromScalar(*scalar, array_length, arrow_pool_.get()));
        auto typed_special_array =
            arrow::internal::checked_pointer_cast<arrow::NumericArray<arrow::Int64Type>>(
                special_array);
        assert(typed_special_array);
        if (convert_func) {
            auto raw_value_ptr = const_cast<int64_t*>(typed_special_array->raw_values());
            assert(raw_value_ptr);
            // row id = first_row_id_ + previous_batch_first_row_number + idx in batch
            for (int64_t i = 0; i < array_length; i++) {
                PAIMON_ASSIGN_OR_RAISE(raw_value_ptr[i], convert_func(i));
            }
        }
    } else if (special_array->null_count() > 0) {
        // condition3: special field exist, has null
        auto typed_special_array =
            arrow::internal::checked_pointer_cast<arrow::NumericArray<arrow::Int64Type>>(
                special_array);
        auto raw_value_ptr = const_cast<int64_t*>(typed_special_array->raw_values());
        // row id = first_row_id_ + previous_batch_first_row_number + idx in batch
        // sequence number = init_value
        auto null_bitmap_data = const_cast<uint8_t*>(typed_special_array->null_bitmap_data());
        assert(raw_value_ptr && null_bitmap_data);
        for (int64_t i = 0; i < array_length; i++) {
            if (typed_special_array->IsNull(i)) {
                if (convert_func) {
                    PAIMON_ASSIGN_OR_RAISE(raw_value_ptr[i], convert_func(i));
                } else {
                    raw_value_ptr[i] = init_value;
                }
                arrow::bit_util::SetBit(null_bitmap_data, i + typed_special_array->offset());
            }
        }
        typed_special_array->data()->SetNullCount(arrow::kUnknownNullCount);
    }
    return Status::OK();
}

}  // namespace paimon
