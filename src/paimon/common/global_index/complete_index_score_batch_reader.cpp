/*
 * Copyright 2025-present Alibaba Inc.
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

#include "paimon/common/global_index/complete_index_score_batch_reader.h"

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
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/status.h"
namespace paimon {
CompleteIndexScoreBatchReader::CompleteIndexScoreBatchReader(
    std::unique_ptr<BatchReader>&& reader, const std::vector<float>& scores,
    const std::shared_ptr<MemoryPool>& pool)
    : arrow_pool_(GetArrowPool(pool)), reader_(std::move(reader)), scores_(scores) {}

Result<BatchReader::ReadBatch> CompleteIndexScoreBatchReader::NextBatch() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                           NextBatchWithBitmap());
    return ReaderUtils::ApplyBitmapToReadBatch(std::move(batch_with_bitmap), arrow_pool_.get());
}

void CompleteIndexScoreBatchReader::UpdateScoreFieldIndex(const arrow::StructType* struct_type) {
    if (index_score_field_idx_ != -1) {
        return;
    }
    index_score_field_idx_ = struct_type->GetFieldIndex(SpecialFields::IndexScore().Name());
    field_names_with_score_.reserve(struct_type->num_fields());
    for (const auto& field : struct_type->fields()) {
        field_names_with_score_.push_back(field->name());
    }
}
Result<BatchReader::ReadBatchWithBitmap> CompleteIndexScoreBatchReader::NextBatchWithBitmap() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                           reader_->NextBatchWithBitmap());
    if (BatchReader::IsEofBatch(batch_with_bitmap)) {
        return batch_with_bitmap;
    }
    if (scores_.empty()) {
        // Indicates score field all null.
        return batch_with_bitmap;
    }

    auto& [batch, bitmap] = batch_with_bitmap;
    auto& [c_array, c_schema] = batch;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(c_array.get(), c_schema.get()));
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(arrow_array);
    if (!struct_array) {
        return Status::Invalid("cannot cast array to StructArray in CompleteIndexScoreBatchReader");
    }
    auto struct_type = struct_array->struct_type();
    UpdateScoreFieldIndex(struct_type);

    // prepare index score array
    std::unique_ptr<arrow::ArrayBuilder> index_score_builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::MakeBuilder(
        arrow_pool_.get(), SpecialFields::IndexScore().Type(), &index_score_builder));
    auto typed_builder = dynamic_cast<arrow::FloatBuilder*>(index_score_builder.get());
    assert(typed_builder);
    PAIMON_RETURN_NOT_OK_FROM_ARROW(typed_builder->Reserve(struct_array->length()));
    bool all_not_null = (struct_array->length() == bitmap.Cardinality());
    for (int64_t i = 0; i < struct_array->length(); i++) {
        if (all_not_null || bitmap.Contains(i)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(typed_builder->Append(scores_[score_cursor_++]));
        } else {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(typed_builder->AppendNull());
        }
    }
    std::shared_ptr<arrow::Array> index_score_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(typed_builder->Finish(&index_score_array));
    // update index score array to struct array
    arrow::ArrayVector array_vec = struct_array->fields();
    array_vec[index_score_field_idx_] = index_score_array;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> array_with_score,
                                      arrow::StructArray::Make(array_vec, field_names_with_score_));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*array_with_score, c_array.get(), c_schema.get()));
    return batch_with_bitmap;
}
}  // namespace paimon
