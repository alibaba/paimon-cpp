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
#include <utility>

#include "paimon/core/mergetree/compact/aggregate/field_aggregator.h"

namespace paimon {

/// Merges map fields and lets input values overwrite matching accumulator keys.
class FieldMergeMapAgg : public FieldAggregator {
 public:
    static constexpr char NAME[] = "merge_map";

    /// Create a merge_map aggregator for a map field.
    ///
    /// @param field_type Type of the aggregated field.
    /// @param field_name Name of the aggregated field.
    /// @param pool Pool the merged maps are allocated from.
    /// @return A merge_map aggregator, or an error Status.
    static Result<std::unique_ptr<FieldMergeMapAgg>> Create(
        const std::shared_ptr<arrow::DataType>& field_type, const std::string& field_name,
        const std::shared_ptr<MemoryPool>& pool);

    Result<VariantType> Agg(const VariantType& accumulator,
                            const VariantType& input_field) override;
    Result<VariantType> Retract(const VariantType& accumulator,
                                const VariantType& input_field) const override;

 private:
    FieldMergeMapAgg(const std::shared_ptr<arrow::DataType>& field_type,
                     std::shared_ptr<arrow::DataType> key_type,
                     std::shared_ptr<arrow::DataType> value_type,
                     const std::shared_ptr<MemoryPool>& pool)
        : FieldAggregator(NAME, field_type, pool),
          key_type_(std::move(key_type)),
          value_type_(std::move(value_type)) {}

    Result<VariantType> AggImpl(const VariantType& accumulator,
                                const VariantType& input_field) const;

    std::shared_ptr<arrow::DataType> key_type_;
    std::shared_ptr<arrow::DataType> value_type_;
};

}  // namespace paimon
