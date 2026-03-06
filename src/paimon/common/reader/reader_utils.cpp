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

#include "paimon/common/reader/reader_utils.h"

#include <cassert>
#include <cstdint>
#include <memory>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {
Result<arrow::ArrayVector> ReaderUtils::GenerateFilteredArrayVector(
    const std::shared_ptr<arrow::Array>& src_array, const RoaringBitmap32& bitmap) {
    if (bitmap.Cardinality() == 0) {
        return Status::Invalid("selection bitmap cannot be empty in GenerateFilteredArrayVector");
    }
    arrow::ArrayVector array_vec;
    auto valid_iter = bitmap.Begin();
    int32_t pos = 0;
    while (valid_iter != bitmap.End() && pos < src_array->length()) {
        int64_t valid_start_pos = *valid_iter;
        for (pos = *valid_iter; pos < src_array->length() && valid_iter != bitmap.End();
             pos++, ++valid_iter) {
            if (pos != *valid_iter) {
                break;
            }
        }
        int64_t valid_end_pos = pos;
        array_vec.push_back(src_array->Slice(valid_start_pos, valid_end_pos - valid_start_pos));
    }
    assert(!array_vec.empty());
    return array_vec;
}

void ReaderUtils::ReleaseReadBatch(BatchReader::ReadBatch&& batch) {
    auto& [c_array, c_schema] = batch;
    if (c_array) {
        ArrowArrayRelease(c_array.get());
    }
    if (c_schema) {
        ArrowSchemaRelease(c_schema.get());
    }
}

Result<BatchReader::ReadBatch> ReaderUtils::ApplyBitmapToReadBatch(
    BatchReader::ReadBatchWithBitmap&& batch_with_bitmap, arrow::MemoryPool* arrow_pool) {
    if (BatchReader::IsEofBatch(batch_with_bitmap)) {
        return std::move(batch_with_bitmap.first);
    }
    BatchReader::ReadBatchWithBitmap moved_batch_with_bitmap = std::move(batch_with_bitmap);
    auto& [batch, bitmap] = moved_batch_with_bitmap;
    auto& [c_array, c_schema] = batch;
    assert(c_array);
    if (bitmap.IsEmpty()) {
        ReleaseReadBatch(std::move(batch));
        return Status::Invalid(
            "NextBatchWithBitmap should always return the result with at least one valid row "
            "except eof");
    }
    if (bitmap.Cardinality() == c_array->length) {
        // indicates all rows in batch are valid
        return std::move(batch);
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(c_array.get(), c_schema.get()));
    PAIMON_ASSIGN_OR_RAISE(arrow::ArrayVector array_vec,
                           GenerateFilteredArrayVector(arrow_array, bitmap));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> result,
                                      arrow::Concatenate(array_vec, arrow_pool));
    assert(result && result->length() > 0);
    std::unique_ptr<ArrowArray> result_c_array = std::make_unique<ArrowArray>();
    std::unique_ptr<ArrowSchema> result_c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*result, result_c_array.get(), result_c_schema.get()));
    return make_pair(std::move(result_c_array), std::move(result_c_schema));
}

BatchReader::ReadBatchWithBitmap ReaderUtils::AddAllValidBitmap(BatchReader::ReadBatch&& batch) {
    if (BatchReader::IsEofBatch(batch)) {
        return BatchReader::MakeEofBatchWithBitmap();
    }
    RoaringBitmap32 all_valid;
    all_valid.AddRange(0, batch.first->length);
    return std::make_pair(std::move(batch), std::move(all_valid));
}

Result<std::shared_ptr<arrow::StructArray>> ReaderUtils::RemoveFieldFromStructArray(
    const std::shared_ptr<arrow::StructArray>& struct_array, const std::string& field_name) {
    auto struct_type = std::static_pointer_cast<arrow::StructType>(struct_array->type());
    int32_t field_idx = struct_type->GetFieldIndex(field_name);
    if (field_idx == -1) {
        return struct_array;
    }
    std::vector<std::shared_ptr<arrow::Array>> new_arrays;
    std::vector<std::shared_ptr<arrow::Field>> new_fields;
    for (int32_t i = 0; i < struct_type->num_fields(); ++i) {
        if (i != field_idx) {
            new_arrays.emplace_back(struct_array->field(i));
            new_fields.emplace_back(struct_type->field(i));
        }
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> array,
        arrow::StructArray::Make(new_arrays, new_fields, struct_array->null_bitmap(),
                                 struct_array->null_count()));
    return array;
}

}  // namespace paimon
