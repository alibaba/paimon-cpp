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

#pragma once

#include <memory>
#include <string>

#include "paimon/core/mergetree/compact/aggregate/field_aggregator.h"

namespace paimon {
/// bool_or aggregate a field of a row.
class FieldBoolOrAgg : public FieldAggregator {
 public:
    static Result<std::unique_ptr<FieldBoolOrAgg>> Create(
        const std::shared_ptr<arrow::DataType>& field_type) {
        if (field_type->id() != arrow::Type::type::BOOL) {
            return Status::Invalid(
                fmt::format("invalid field type {} for {}, supposed to be boolean",
                            field_type->ToString(), NAME));
        }
        return std::unique_ptr<FieldBoolOrAgg>(new FieldBoolOrAgg(field_type));
    }

    VariantType Agg(const VariantType& accumulator, const VariantType& input_field) override {
        bool accumulator_null = DataDefine::IsVariantNull(accumulator);
        bool input_null = DataDefine::IsVariantNull(input_field);
        if (accumulator_null || input_null) {
            return accumulator_null ? input_field : accumulator;
        }
        bool accumulator_value = DataDefine::GetVariantValue<bool>(accumulator);
        bool input_value = DataDefine::GetVariantValue<bool>(input_field);
        return accumulator_value || input_value;
    }

 public:
    static constexpr char NAME[] = "bool_or";

 private:
    explicit FieldBoolOrAgg(const std::shared_ptr<arrow::DataType>& field_type)
        : FieldAggregator(std::string(NAME), field_type) {}
};
}  // namespace paimon
