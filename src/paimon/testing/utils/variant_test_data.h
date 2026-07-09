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

#pragma once

#include <memory>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

namespace paimon::test {

/// Builds a `[id, v]` struct array where `v` holds the variant encodings of `jsons` (nullptr
/// means a null variant) and `id` is a running int32 starting at `id_offset`. `variant_field`
/// must be a variant-marked field (see `VariantTypeUtils::ToArrowField`).
inline Result<std::shared_ptr<arrow::StructArray>> BuildVariantBatch(
    const std::shared_ptr<arrow::Field>& id_field,
    const std::shared_ptr<arrow::Field>& variant_field, const std::vector<const char*>& jsons,
    const std::shared_ptr<MemoryPool>& pool, int32_t id_offset = 0) {
    arrow::Int32Builder id_builder;
    auto value_builder = std::make_shared<arrow::BinaryBuilder>();
    auto metadata_builder = std::make_shared<arrow::BinaryBuilder>();
    arrow::StructBuilder variant_builder(variant_field->type(), arrow::default_memory_pool(),
                                         {value_builder, metadata_builder});
    for (size_t i = 0; i < jsons.size(); ++i) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Append(id_offset + static_cast<int32_t>(i)));
        if (jsons[i] == nullptr) {
            // StructBuilder::AppendNull appends empty values to the child builders itself.
            PAIMON_RETURN_NOT_OK_FROM_ARROW(variant_builder.AppendNull());
        } else {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GenericVariant> variant,
                                   GenericVariant::FromJson(jsons[i], pool));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(variant_builder.Append());
            PAIMON_RETURN_NOT_OK_FROM_ARROW(value_builder->Append(variant->RawValue()));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(metadata_builder->Append(variant->Metadata()));
        }
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> id_array, id_builder.Finish());
    std::shared_ptr<arrow::Array> variant_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(variant_builder.Finish(&variant_array));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> result,
        arrow::StructArray::Make({id_array, variant_array}, {id_field, variant_field}));
    return result;
}

}  // namespace paimon::test
