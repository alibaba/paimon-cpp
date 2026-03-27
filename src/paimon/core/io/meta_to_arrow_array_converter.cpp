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
#include "paimon/core/io/meta_to_arrow_array_converter.h"

namespace paimon {
Result<std::unique_ptr<MetaToArrowArrayConverter>> MetaToArrowArrayConverter::Create(
    const std::shared_ptr<arrow::DataType>& meta_data_type,
    const std::shared_ptr<MemoryPool>& pool) {
    auto struct_type = std::dynamic_pointer_cast<arrow::StructType>(meta_data_type);
    if (!struct_type) {
        return Status::Invalid("meta_data_type in MetaToArrowArrayConverter must be struct type");
    }
    auto arrow_pool = GetArrowPool(pool);
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::MakeBuilder(
        arrow_pool.get(), arrow::struct_(struct_type->fields()), &array_builder));

    auto struct_builder =
        arrow::internal::checked_pointer_cast<arrow::StructBuilder>(std::move(array_builder));
    assert(struct_builder);
    std::vector<RowToArrowArrayConverter::AppendValueFunc> appenders;
    appenders.reserve(struct_type->num_fields());
    // first is the root struct array
    int32_t reserve_count = 1;
    for (int32_t i = 0; i < struct_type->num_fields(); i++) {
        PAIMON_ASSIGN_OR_RAISE(
            RowToArrowArrayConverter::AppendValueFunc func,
            AppendField(/*use_view=*/true, struct_builder->field_builder(i), &reserve_count));
        appenders.emplace_back(func);
    }
    return std::unique_ptr<MetaToArrowArrayConverter>(new MetaToArrowArrayConverter(
        reserve_count, std::move(appenders), std::move(struct_builder), std::move(arrow_pool)));
}

Result<std::shared_ptr<arrow::Array>> MetaToArrowArrayConverter::NextBatch(
    const std::vector<BinaryRow>& meta_rows) {
    PAIMON_RETURN_NOT_OK(ResetAndReserve());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        array_builder_->AppendValues(meta_rows.size(), /*valid_bytes=*/nullptr));
    for (size_t i = 0; i < appenders_.size(); i++) {
        for (const auto& row : meta_rows) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(appenders_[i](row, i));
        }
    }

    std::shared_ptr<arrow::Array> array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(array_builder_->Finish(&array));

    int32_t reserve_idx = 0;
    PAIMON_RETURN_NOT_OK(Accumulate(array.get(), &reserve_idx));
    return array;
}

}  // namespace paimon
