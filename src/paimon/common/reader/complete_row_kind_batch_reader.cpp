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

#include "paimon/common/reader/complete_row_kind_batch_reader.h"

#include <cstddef>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/util.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/scalar.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/arrow_memory_pool_adaptor.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/status.h"

namespace paimon {

Result<BatchReader::ReadBatch> CompleteRowKindBatchReader::NextBatch() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                           NextBatchWithBitmap());
    return ReaderUtils::ApplyBitmapToReadBatch(std::move(batch_with_bitmap),
                                               AsArrowMemoryPool(*memory_pool_));
}

Result<BatchReader::ReadBatchWithBitmap> CompleteRowKindBatchReader::NextBatchWithBitmap() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                           reader_->NextBatchWithBitmap());
    if (BatchReader::IsEofBatch(batch_with_bitmap)) {
        return batch_with_bitmap;
    }
    auto& [batch, bitmap] = batch_with_bitmap;
    auto& [c_array, c_schema] = batch;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(c_array.get(), c_schema.get()));
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(arrow_array);
    if (!struct_array) {
        return Status::Invalid("cannot cast array to StructArray in CompleteRowKindBatchReader");
    }
    if (struct_array->GetFieldByName(SpecialFields::ValueKind().Name())) {
        // batch returned by reader_ has value kind, just return
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            arrow::ExportArray(*struct_array, c_array.get(), c_schema.get()));
        return batch_with_bitmap;
    }
    // create value kind array, all are insert
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> row_kind_array,
                           PrepareRowKindArray(struct_array->length()));
    // complete row kind
    UpdateFieldNamesWithRowKind(struct_array);
    arrow::ArrayVector fields_with_row_kind = {row_kind_array};
    fields_with_row_kind.insert(fields_with_row_kind.end(), struct_array->fields().begin(),
                                struct_array->fields().end());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> array_with_row_kind,
        arrow::StructArray::Make(fields_with_row_kind, field_names_with_row_kind_));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*array_with_row_kind, c_array.get(), c_schema.get()));
    return batch_with_bitmap;
}

Result<std::shared_ptr<arrow::Array>> CompleteRowKindBatchReader::PrepareRowKindArray(
    int32_t struct_array_length) {
    if (!row_kind_array_ || row_kind_array_->length() < struct_array_length) {
        auto row_kind_scalar =
            std::make_shared<arrow::Int8Scalar>(RowKind::Insert()->ToByteValue());
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            row_kind_array_, arrow::MakeArrayFromScalar(*row_kind_scalar, struct_array_length,
                                                        AsArrowMemoryPool(*memory_pool_)));
        return row_kind_array_;
    } else {
        return row_kind_array_->Slice(0, struct_array_length);
    }
}

void CompleteRowKindBatchReader::UpdateFieldNamesWithRowKind(
    const std::shared_ptr<arrow::StructArray>& struct_array) {
    if (static_cast<size_t>(struct_array->struct_type()->num_fields()) + 1 ==
        field_names_with_row_kind_.size()) {
        return;
    }
    field_names_with_row_kind_.clear();
    const auto& fields = struct_array->struct_type()->fields();
    field_names_with_row_kind_.reserve(fields.size() + 1);
    field_names_with_row_kind_.push_back(SpecialFields::ValueKind().Name());
    for (const auto& field : fields) {
        field_names_with_row_kind_.push_back(field->name());
    }
}

}  // namespace paimon
