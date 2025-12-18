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

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/common/data/data_define.h"
#include "paimon/result.h"

namespace paimon {
/// abstract class of aggregating a field of a row.
class FieldAggregator {
 public:
    virtual ~FieldAggregator() = default;

    FieldAggregator(const std::string& name, const std::shared_ptr<arrow::DataType>& field_type)
        : name_(name), field_type_(field_type) {}

    virtual VariantType Agg(const VariantType& accumulator, const VariantType& input_field) = 0;

    /// reset the aggregator to a clean start state.
    virtual void Reset() {}

    virtual VariantType AggReversed(const VariantType& accumulator,
                                    const VariantType& input_field) {
        return Agg(input_field, accumulator);
    }

    virtual Result<VariantType> Retract(const VariantType& accumulator,
                                        const VariantType& input_field) const {
        return Status::Invalid(fmt::format(
            "Aggregate function {} does not support retraction, if you allow this function to "
            "ignore retraction messages, you can configure fields.field_name.ignore-retract=true.",
            name_));
    }

    const std::string& GetName() const {
        return name_;
    }
    std::shared_ptr<arrow::DataType> GetFieldType() const {
        return field_type_;
    }

 protected:
    std::string name_;
    std::shared_ptr<arrow::DataType> field_type_;
};
}  // namespace paimon
