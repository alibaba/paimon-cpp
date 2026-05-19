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

#include "paimon/core/io/generic_row_to_arrow_array_converter.h"

#include <cassert>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/array/builder_nested.h"
#include "arrow/memory_pool.h"
#include "arrow/util/checked_cast.h"
#include "paimon/common/utils/arrow/status_utils.h"

namespace paimon {

Result<std::unique_ptr<GenericRowToArrowArrayConverter>> GenericRowToArrowArrayConverter::Create(
    const std::shared_ptr<arrow::Schema>& schema, arrow::MemoryPool* pool) {
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::MakeBuilder(
        pool, std::make_shared<arrow::StructType>(schema->fields()), &array_builder));

    auto struct_builder =
        arrow::internal::checked_pointer_cast<arrow::StructBuilder>(std::move(array_builder));
    assert(struct_builder);
    std::vector<RowToArrowArrayConverter::AppendValueFunc> appenders;
    appenders.reserve(schema->num_fields());
    int32_t reserve_count = 1;
    for (int32_t i = 0; i < schema->num_fields(); ++i) {
        PAIMON_ASSIGN_OR_RAISE(
            RowToArrowArrayConverter::AppendValueFunc func,
            AppendField(/*use_view=*/true, struct_builder->field_builder(i), &reserve_count));
        appenders.emplace_back(std::move(func));
    }
    return std::unique_ptr<GenericRowToArrowArrayConverter>(new GenericRowToArrowArrayConverter(
        reserve_count, std::move(appenders), std::move(struct_builder), nullptr));
}

Result<BatchReader::ReadBatch> GenericRowToArrowArrayConverter::NextBatch(
    const std::vector<GenericRow>& rows) {
    PAIMON_RETURN_NOT_OK(ResetAndReserve());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        array_builder_->AppendValues(rows.size(), /*valid_bytes=*/nullptr));
    for (size_t i = 0; i < appenders_.size(); ++i) {
        for (const auto& row : rows) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(appenders_[i](row, i));
        }
    }

    return FinishAndAccumulate();
}

}  // namespace paimon
