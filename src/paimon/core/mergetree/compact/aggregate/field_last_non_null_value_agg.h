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

#include "paimon/common/data/data_define.h"
#include "paimon/core/mergetree/compact/aggregate/field_aggregator.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {
/// last non-null value aggregate a field of a row.
class FieldLastNonNullValueAgg : public FieldAggregator {
 public:
    FieldLastNonNullValueAgg(const std::shared_ptr<arrow::DataType>& field_type,
                             const std::shared_ptr<MemoryPool>& pool)
        : FieldAggregator(std::string(NAME), field_type, pool) {}

    Result<VariantType> Agg(const VariantType& accumulator,
                            const VariantType& input_field) override {
        return DataDefine::IsVariantNull(input_field) ? accumulator : input_field;
    }

    Result<VariantType> Retract(const VariantType& accumulator,
                                const VariantType& input_field) const override {
        return DataDefine::IsVariantNull(input_field) ? accumulator : NullType();
    }

 public:
    static constexpr char NAME[] = "last_non_null_value";
};
}  // namespace paimon
