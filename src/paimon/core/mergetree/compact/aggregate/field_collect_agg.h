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
#include <string>

#include "paimon/core/mergetree/compact/aggregate/field_aggregator.h"

namespace paimon {

class CoreOptions;

/// Concatenates arrays, optionally removing duplicate elements.
class FieldCollectAgg : public FieldAggregator {
 public:
    static constexpr char NAME[] = "collect";

    /// Create a collect aggregator for an array field.
    ///
    /// @param field_type Type of the aggregated field.
    /// @param options Table options containing the distinct setting.
    /// @param field_name Name of the aggregated field.
    /// @return A collect aggregator, or an error Status.
    static Result<std::unique_ptr<FieldCollectAgg>> Create(
        const std::shared_ptr<arrow::DataType>& field_type, const CoreOptions& options,
        const std::string& field_name);

    VariantType Agg(const VariantType& accumulator, const VariantType& input_field) override;
    Result<VariantType> AggResult(const VariantType& accumulator,
                                  const VariantType& input_field) override;
    VariantType AggReversed(const VariantType& accumulator,
                            const VariantType& input_field) override;
    Result<VariantType> AggReversedResult(const VariantType& accumulator,
                                          const VariantType& input_field) override;
    Result<VariantType> Retract(const VariantType& accumulator,
                                const VariantType& input_field) const override;

 private:
    FieldCollectAgg(const std::shared_ptr<arrow::DataType>& field_type,
                    std::shared_ptr<arrow::DataType> element_type, bool distinct)
        : FieldAggregator(NAME, field_type),
          element_type_(std::move(element_type)),
          distinct_(distinct) {}

    Result<VariantType> AggImpl(const VariantType& accumulator,
                                const VariantType& input_field) const;

    std::shared_ptr<arrow::DataType> element_type_;
    bool distinct_;
};

}  // namespace paimon
