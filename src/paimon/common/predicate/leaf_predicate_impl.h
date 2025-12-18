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
#include <vector>

#include "arrow/api.h"
#include "paimon/common/predicate/compound_function.h"
#include "paimon/common/predicate/leaf_function.h"
#include "paimon/common/predicate/literal_converter.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/predicate/leaf_predicate.h"
namespace paimon {
class LeafPredicateImpl : public LeafPredicate, public PredicateFilter {
 public:
    LeafPredicateImpl(const LeafFunction& leaf_function, int32_t field_index,
                      const std::string& field_name, const FieldType& field_type,
                      const std::vector<Literal>& literals)
        : LeafPredicate(leaf_function, field_index, field_name, field_type, literals) {}

    const LeafFunction& GetLeafFunction() const {
        return leaf_function_;
    }

    Result<std::vector<char>> Test(const arrow::Array& array) const override {
        const auto& struct_array = arrow::internal::checked_cast<const arrow::StructArray&>(array);
        if (field_index_ >= static_cast<int32_t>(struct_array.fields().size())) {
            return Status::Invalid(
                fmt::format("field index {} exceed field count {} in struct array", field_index_,
                            struct_array.fields().size()));
        }
        const auto& field_array = struct_array.field(field_index_);
        return leaf_function_.Test(*field_array, literals_);
    }

    Result<bool> Test(const std::shared_ptr<arrow::Schema>& schema,
                      const InternalRow& row) const override {
        if (field_index_ >= row.GetFieldCount()) {
            return Status::Invalid(fmt::format("field index {} exceed field count {} in row",
                                               field_index_, row.GetFieldCount()));
        }
        PAIMON_ASSIGN_OR_RAISE(Literal value, LiteralConverter::ConvertLiteralsFromRow(
                                                  schema, row, field_index_, field_type_));
        return leaf_function_.Test(value, literals_);
    }

    Result<bool> Test(const std::shared_ptr<arrow::Schema>& schema, int64_t row_count,
                      const InternalRow& min_values, const InternalRow& max_values,
                      const InternalArray& null_counts) const override {
        PAIMON_ASSIGN_OR_RAISE(Literal min_value,
                               LiteralConverter::ConvertLiteralsFromRow(schema, min_values,
                                                                        field_index_, field_type_));
        PAIMON_ASSIGN_OR_RAISE(Literal max_value,
                               LiteralConverter::ConvertLiteralsFromRow(schema, max_values,
                                                                        field_index_, field_type_));
        std::optional<int64_t> null_count = null_counts.IsNullAt(field_index_)
                                                ? std::optional<int64_t>()
                                                : null_counts.GetLong(field_index_);
        if (null_count == std::nullopt || row_count != null_count.value()) {
            // not all null
            // min or max is null
            // unknown stats
            if (min_value.IsNull() || max_value.IsNull()) {
                return true;
            }
        }
        return leaf_function_.Test(row_count, min_value, max_value, null_count, literals_);
    }

    std::shared_ptr<LeafPredicateImpl> NewLeafPredicate(int32_t new_field_index) const {
        return std::make_shared<LeafPredicateImpl>(leaf_function_, new_field_index, field_name_,
                                                   field_type_, literals_);
    }

    std::shared_ptr<LeafPredicateImpl> NewLeafPredicate(const std::string& new_field_name) const {
        return std::make_shared<LeafPredicateImpl>(leaf_function_, field_index_, new_field_name,
                                                   field_type_, literals_);
    }
};
}  // namespace paimon
