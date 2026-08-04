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

#include "paimon/core/mergetree/compact/aggregate/field_collect_agg.h"

#include <vector>

#include "arrow/api.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/data/generic_array.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/core/core_options.h"
#include "paimon/core/mergetree/compact/aggregate/field_aggregate_utils.h"
#include "paimon/status.h"

namespace paimon {
namespace {

// TODO(liangjie.liang): Hash VariantType by its type so that this scan, the key lookup in
// FieldMergeMapAgg and the keyed upsert in FieldNestedUpdateAgg stop being O(n^2). Java only pays
// that cost for constructed element types and uses HashSet/HashMap for the rest.
Result<bool> Contains(const std::vector<VariantType>& values, const VariantType& candidate,
                      const std::shared_ptr<arrow::DataType>& element_type) {
    for (const VariantType& value : values) {
        PAIMON_ASSIGN_OR_RAISE(bool equal,
                               FieldAggregateUtils::Equals(value, candidate, element_type));
        if (equal) {
            return true;
        }
    }
    return false;
}

Status AppendArray(const std::shared_ptr<InternalArray>& array,
                   const std::shared_ptr<arrow::DataType>& element_type, bool distinct,
                   std::vector<VariantType>* values) {
    if (!array) {
        return Status::OK();
    }
    for (int32_t i = 0; i < array->Size(); ++i) {
        PAIMON_ASSIGN_OR_RAISE(VariantType value,
                               FieldAggregateUtils::GetValue(*array, i, element_type));
        if (distinct) {
            PAIMON_ASSIGN_OR_RAISE(bool contains, Contains(*values, value, element_type));
            if (contains) {
                continue;
            }
        }
        values->push_back(std::move(value));
    }
    return Status::OK();
}

}  // namespace

Result<std::unique_ptr<FieldCollectAgg>> FieldCollectAgg::Create(
    const std::shared_ptr<arrow::DataType>& field_type, const CoreOptions& options,
    const std::string& field_name, const std::shared_ptr<MemoryPool>& pool) {
    if (field_type->id() != arrow::Type::LIST) {
        return Status::Invalid(
            fmt::format("invalid field type {} for field '{}' of {}, supposed to be array",
                        field_type->ToString(), field_name, NAME));
    }
    std::shared_ptr<arrow::ListType> list_type =
        arrow::internal::checked_pointer_cast<arrow::ListType>(field_type);
    PAIMON_ASSIGN_OR_RAISE(bool distinct, options.FieldCollectAggDistinct(field_name));
    return std::unique_ptr<FieldCollectAgg>(
        new FieldCollectAgg(field_type, list_type->value_type(), distinct, pool));
}

Result<VariantType> FieldCollectAgg::Agg(const VariantType& accumulator,
                                         const VariantType& input_field) {
    return AggImpl(accumulator, input_field);
}

Result<VariantType> FieldCollectAgg::AggReversed(const VariantType& accumulator,
                                                 const VariantType& input_field) {
    return AggImpl(accumulator, input_field);
}

Result<VariantType> FieldCollectAgg::AggImpl(const VariantType& accumulator,
                                             const VariantType& input_field) const {
    bool accumulator_null = DataDefine::IsVariantNull(accumulator);
    bool input_null = DataDefine::IsVariantNull(input_field);
    if (accumulator_null && input_null) {
        return VariantType(NullType());
    }
    if (!distinct_ && (accumulator_null || input_null)) {
        return accumulator_null ? input_field : accumulator;
    }

    std::shared_ptr<InternalArray> accumulator_array =
        accumulator_null ? nullptr
                         : DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(accumulator);
    std::shared_ptr<InternalArray> input_array =
        input_null ? nullptr
                   : DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(input_field);
    std::vector<VariantType> values;
    if (accumulator_array) {
        values.reserve(accumulator_array->Size() + (input_array ? input_array->Size() : 0));
    }
    PAIMON_RETURN_NOT_OK(AppendArray(accumulator_array, element_type_, distinct_, &values));
    PAIMON_RETURN_NOT_OK(AppendArray(input_array, element_type_, distinct_, &values));
    std::vector<std::shared_ptr<InternalArray>> holders;
    if (accumulator_array) {
        holders.push_back(accumulator_array);
    }
    if (input_array) {
        holders.push_back(input_array);
    }
    return VariantType(std::static_pointer_cast<InternalArray>(
        std::make_shared<GenericArray>(std::move(values), std::move(holders))));
}

Result<VariantType> FieldCollectAgg::Retract(const VariantType& accumulator,
                                             const VariantType& input_field) const {
    if (DataDefine::IsVariantNull(accumulator) || DataDefine::IsVariantNull(input_field)) {
        return accumulator;
    }
    auto accumulator_array =
        DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(accumulator);
    auto retract_array = DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(input_field);
    if (retract_array->Size() == 0) {
        return accumulator;
    }

    std::vector<VariantType> retract_values;
    PAIMON_RETURN_NOT_OK(
        AppendArray(retract_array, element_type_, /*distinct=*/false, &retract_values));
    std::vector<VariantType> result_values;
    result_values.reserve(accumulator_array->Size());
    for (int32_t i = 0; i < accumulator_array->Size(); ++i) {
        PAIMON_ASSIGN_OR_RAISE(VariantType candidate,
                               FieldAggregateUtils::GetValue(*accumulator_array, i, element_type_));
        bool removed = false;
        for (auto iter = retract_values.begin(); iter != retract_values.end(); ++iter) {
            PAIMON_ASSIGN_OR_RAISE(bool equal,
                                   FieldAggregateUtils::Equals(candidate, *iter, element_type_));
            if (equal) {
                retract_values.erase(iter);
                removed = true;
                break;
            }
        }
        if (!removed) {
            result_values.push_back(std::move(candidate));
        }
    }
    return VariantType(std::static_pointer_cast<InternalArray>(std::make_shared<GenericArray>(
        std::move(result_values),
        std::vector<std::shared_ptr<InternalArray>>{accumulator_array, retract_array})));
}

}  // namespace paimon
