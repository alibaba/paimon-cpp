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

/// Unions serialized HyperLogLog sketch fields.
class FieldHllSketchAgg : public FieldAggregator {
 public:
    static constexpr char NAME[] = "hll_sketch";

    /// Create an hll_sketch aggregator for a binary field.
    ///
    /// @param field_type Type of the aggregated field.
    /// @param field_name Name of the aggregated field.
    /// @param pool Pool the unioned sketch bytes are allocated from.
    /// @return An hll_sketch aggregator, or an error Status.
    static Result<std::unique_ptr<FieldHllSketchAgg>> Create(
        const std::shared_ptr<arrow::DataType>& field_type, const std::string& field_name,
        const std::shared_ptr<MemoryPool>& pool);

    Result<VariantType> Agg(const VariantType& accumulator,
                            const VariantType& input_field) override;

 private:
    FieldHllSketchAgg(const std::shared_ptr<arrow::DataType>& field_type,
                      const std::shared_ptr<MemoryPool>& pool)
        : FieldAggregator(NAME, field_type, pool) {}
};

/// Unions serialized Theta sketch fields.
class FieldThetaSketchAgg : public FieldAggregator {
 public:
    static constexpr char NAME[] = "theta_sketch";

    /// Create a theta_sketch aggregator for a binary field.
    ///
    /// @param field_type Type of the aggregated field.
    /// @param field_name Name of the aggregated field.
    /// @param pool Pool the unioned sketch bytes are allocated from.
    /// @return A theta_sketch aggregator, or an error Status.
    static Result<std::unique_ptr<FieldThetaSketchAgg>> Create(
        const std::shared_ptr<arrow::DataType>& field_type, const std::string& field_name,
        const std::shared_ptr<MemoryPool>& pool);

    Result<VariantType> Agg(const VariantType& accumulator,
                            const VariantType& input_field) override;

 private:
    FieldThetaSketchAgg(const std::shared_ptr<arrow::DataType>& field_type,
                        const std::shared_ptr<MemoryPool>& pool)
        : FieldAggregator(NAME, field_type, pool) {}
};

}  // namespace paimon
