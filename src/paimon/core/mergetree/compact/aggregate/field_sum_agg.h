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

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <variant>

#include "paimon/common/data/data_define.h"
#include "paimon/core/mergetree/compact/aggregate/field_aggregator.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {
/// sum aggregate a field of a row.
class FieldSumAgg : public FieldAggregator {
 public:
    static Result<std::unique_ptr<FieldSumAgg>> Create(
        const std::shared_ptr<arrow::DataType>& field_type,
        const std::shared_ptr<MemoryPool>& pool) {
        PAIMON_ASSIGN_OR_RAISE(FieldSumFunc sum_func, CreateSumFunc(field_type));
        PAIMON_ASSIGN_OR_RAISE(FieldNegFunc neg_func, CreateNegFunc(field_type));
        return std::unique_ptr<FieldSumAgg>(new FieldSumAgg(field_type, sum_func, neg_func, pool));
    }

    Result<VariantType> Agg(const VariantType& accumulator,
                            const VariantType& input_field) override {
        bool accumulator_null = DataDefine::IsVariantNull(accumulator);
        bool input_null = DataDefine::IsVariantNull(input_field);
        if (accumulator_null || input_null) {
            return accumulator_null ? input_field : accumulator;
        }
        return sum_func_(accumulator, input_field);
    }

    Result<VariantType> Retract(const VariantType& accumulator,
                                const VariantType& input_field) const override {
        bool accumulator_null = DataDefine::IsVariantNull(accumulator);
        bool input_null = DataDefine::IsVariantNull(input_field);
        if (!accumulator_null && !input_null) {
            return sum_func_(accumulator, neg_func_(input_field));
        }
        if (!accumulator_null) {
            return accumulator;
        }
        if (!input_null) {
            return neg_func_(input_field);
        }
        // accumulator and input_field are both null
        return accumulator;
    }

 public:
    static constexpr char NAME[] = "sum";

 private:
    using FieldSumFunc =
        std::function<VariantType(const VariantType& accumulator, const VariantType& input_field)>;
    using FieldNegFunc = std::function<VariantType(const VariantType& input_field)>;

    FieldSumAgg(const std::shared_ptr<arrow::DataType>& field_type, const FieldSumFunc& sum_func,
                const FieldNegFunc& neg_func, const std::shared_ptr<MemoryPool>& pool)
        : FieldAggregator(std::string(NAME), field_type, pool),
          sum_func_(sum_func),
          neg_func_(neg_func) {}

    static Result<FieldSumFunc> CreateSumFunc(const std::shared_ptr<arrow::DataType>& field_type);

    static Result<FieldNegFunc> CreateNegFunc(const std::shared_ptr<arrow::DataType>& field_type);

 private:
    FieldSumFunc sum_func_;
    FieldNegFunc neg_func_;
};
}  // namespace paimon
