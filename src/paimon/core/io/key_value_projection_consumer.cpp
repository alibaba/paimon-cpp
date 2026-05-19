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

#include "paimon/core/io/key_value_projection_consumer.h"

#include <cassert>
#include <cstddef>
#include <functional>

#include "arrow/api.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/builder_nested.h"
#include "arrow/c/abi.h"
#include "arrow/util/checked_cast.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/key_value.h"
#include "paimon/status.h"

namespace paimon {
class MemoryPool;

Result<std::unique_ptr<KeyValueProjectionConsumer>> KeyValueProjectionConsumer::Create(
    const std::shared_ptr<arrow::Schema>& target_schema,
    const std::vector<int32_t>& target_to_src_mapping, const std::shared_ptr<MemoryPool>& pool) {
    if (static_cast<size_t>(target_schema->num_fields()) != target_to_src_mapping.size()) {
        return Status::Invalid(
            "target_schema and target_to_src_mapping mismatch in KeyValueProjectionConsumer");
    }
    auto arrow_pool = GetArrowPool(pool);
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::MakeBuilder(
        arrow_pool.get(), std::make_shared<arrow::StructType>(target_schema->fields()),
        &array_builder));

    auto struct_builder =
        arrow::internal::checked_pointer_cast<arrow::StructBuilder>(std::move(array_builder));
    assert(struct_builder);
    std::vector<RowToArrowArrayConverter::AppendValueFunc> appenders;
    appenders.reserve(target_to_src_mapping.size());
    // first is the root struct array
    int32_t reserve_count = 1;
    for (int32_t i = 0; i < static_cast<int32_t>(target_to_src_mapping.size()); i++) {
        PAIMON_ASSIGN_OR_RAISE(
            RowToArrowArrayConverter::AppendValueFunc func,
            AppendField(/*use_view=*/true, struct_builder->field_builder(i), &reserve_count));
        appenders.emplace_back(func);
    }
    return std::unique_ptr<KeyValueProjectionConsumer>(new KeyValueProjectionConsumer(
        reserve_count, std::move(appenders), std::move(struct_builder), std::move(arrow_pool),
        target_to_src_mapping));
}

Result<BatchReader::ReadBatch> KeyValueProjectionConsumer::NextBatch(
    const std::vector<KeyValue>& key_value_vec) {
    PAIMON_RETURN_NOT_OK(ResetAndReserve());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        array_builder_->AppendValues(key_value_vec.size(), /*valid_bytes=*/nullptr));
    for (int32_t i = 0; i < static_cast<int32_t>(target_to_src_mapping_.size()); i++) {
        for (const auto& row : key_value_vec) {
            if (target_to_src_mapping_[i] == kSequenceNumberProjection) {
                auto* builder =
                    dynamic_cast<arrow::Int64Builder*>(array_builder_->field_builder(i));
                if (builder == nullptr) {
                    return Status::Invalid("cannot append sequence number to non-int64 field");
                }
                PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(row.sequence_number));
                continue;
            }
            if (target_to_src_mapping_[i] == kValueKindProjection) {
                auto* builder = dynamic_cast<arrow::Int8Builder*>(array_builder_->field_builder(i));
                if (builder == nullptr) {
                    return Status::Invalid("cannot append value kind to non-int8 field");
                }
                PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(row.value_kind->ToByteValue()));
                continue;
            }
            PAIMON_RETURN_NOT_OK_FROM_ARROW(appenders_[i](*(row.value), target_to_src_mapping_[i]));
        }
    }
    return FinishAndAccumulate();
}

}  // namespace paimon
