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
/// min aggregate a field of a row.
class FieldMinAgg : public FieldAggregator {
 public:
    static Result<std::unique_ptr<FieldMinAgg>> Create(
        const std::shared_ptr<arrow::DataType>& field_type) {
        PAIMON_ASSIGN_OR_RAISE(FieldMinFunc min_func, CreateMinFunc(field_type));
        return std::unique_ptr<FieldMinAgg>(new FieldMinAgg(field_type, min_func));
    }

    VariantType Agg(const VariantType& accumulator, const VariantType& input_field) override {
        bool accumulator_null = DataDefine::IsVariantNull(accumulator);
        bool input_null = DataDefine::IsVariantNull(input_field);
        if (accumulator_null || input_null) {
            return accumulator_null ? input_field : accumulator;
        }
        return min_func_(accumulator, input_field);
    }

 public:
    static constexpr char NAME[] = "min";

 private:
    using FieldMinFunc =
        std::function<VariantType(const VariantType& accumulator, const VariantType& input_field)>;

    FieldMinAgg(const std::shared_ptr<arrow::DataType>& field_type, const FieldMinFunc& min_func)
        : FieldAggregator(std::string(NAME), field_type), min_func_(min_func) {}

    static Result<FieldMinFunc> CreateMinFunc(const std::shared_ptr<arrow::DataType>& field_type) {
        arrow::Type::type type = field_type->id();
        switch (type) {
            case arrow::Type::type::INT8:
            case arrow::Type::type::INT16:
            case arrow::Type::type::INT32:
            case arrow::Type::type::DATE32:
            case arrow::Type::type::INT64:
            case arrow::Type::type::FLOAT:
            case arrow::Type::type::DOUBLE:
            case arrow::Type::type::TIMESTAMP:
            case arrow::Type::type::DECIMAL:
            case arrow::Type::type::STRING:
            case arrow::Type::type::BINARY:
                return FieldMinFunc([](const VariantType& accumulator,
                                       const VariantType& input_field) -> VariantType {
                    return accumulator < input_field ? accumulator : input_field;
                });
            default:
                return Status::Invalid(
                    fmt::format("type {} not support in FieldMinAgg", field_type->ToString()));
        }
    }

 private:
    FieldMinFunc min_func_;
};
}  // namespace paimon
