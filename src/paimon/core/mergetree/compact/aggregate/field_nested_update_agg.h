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
#include <vector>

#include "paimon/core/core_options.h"
#include "paimon/core/mergetree/compact/aggregate/field_aggregator.h"

namespace arrow {
class StructType;
}  // namespace arrow

namespace paimon {

class FieldsComparator;

/// Upserts rows in an ARRAY<STRUCT> using configured nested keys and sequence fields.
class FieldNestedUpdateAgg : public FieldAggregator {
 public:
    static constexpr char NAME[] = "nested_update";

    ~FieldNestedUpdateAgg() override;

    /// Create a nested_update aggregator for an array-of-struct field.
    ///
    /// @param field_type Type of the aggregated field.
    /// @param options Table options describing nested keys, sequences, and limits.
    /// @param field_name Name of the aggregated field.
    /// @param pool Pool the merged nested rows are allocated from.
    /// @return A nested_update aggregator, or an error Status.
    static Result<std::unique_ptr<FieldNestedUpdateAgg>> Create(
        const std::shared_ptr<arrow::DataType>& field_type, const CoreOptions& options,
        const std::string& field_name, const std::shared_ptr<MemoryPool>& pool);

    Result<VariantType> Agg(const VariantType& accumulator,
                            const VariantType& input_field) override;
    Result<VariantType> Retract(const VariantType& accumulator,
                                const VariantType& input_field) const override;

 private:
    FieldNestedUpdateAgg(const std::shared_ptr<arrow::DataType>& field_type,
                         std::shared_ptr<arrow::StructType> row_type,
                         std::vector<int32_t> key_fields,
                         CoreOptions::NestedKeyNullStrategy null_strategy,
                         std::unique_ptr<FieldsComparator> sequence_comparator, int32_t count_limit,
                         const std::shared_ptr<MemoryPool>& pool);

    Result<VariantType> AggImpl(const VariantType& accumulator,
                                const VariantType& input_field) const;
    Result<bool> AcceptKey(const InternalRow& row) const;
    Result<bool> KeysEqual(const InternalRow& lhs, const InternalRow& rhs) const;

    std::shared_ptr<arrow::StructType> row_type_;
    std::vector<int32_t> key_fields_;
    CoreOptions::NestedKeyNullStrategy null_strategy_;
    std::unique_ptr<FieldsComparator> sequence_comparator_;
    int32_t count_limit_;
};

}  // namespace paimon
