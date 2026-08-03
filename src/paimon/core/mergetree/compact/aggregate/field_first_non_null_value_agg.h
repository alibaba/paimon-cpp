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

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {
/// first non-null value aggregate a field of a row.
class FieldFirstNonNullValueAgg : public FieldAggregator {
 public:
    explicit FieldFirstNonNullValueAgg(const std::shared_ptr<arrow::DataType>& field_type)
        : FieldAggregator(std::string(NAME), field_type) {}

    Result<VariantType> Agg(const VariantType& accumulator,
                            const VariantType& input_field) override {
        if (!initialized_ && !DataDefine::IsVariantNull(input_field)) {
            initialized_ = true;
            return input_field;
        }
        return accumulator;
    }

    void Reset() override {
        initialized_ = false;
    }

 public:
    static constexpr char NAME[] = "first_non_null_value";

 private:
    bool initialized_ = false;
};
}  // namespace paimon
