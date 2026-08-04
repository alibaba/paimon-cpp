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

#include "paimon/core/mergetree/compact/aggregate/field_nested_update_agg.h"

#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/data/generic_array.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/mergetree/compact/aggregate/field_aggregate_utils.h"
#include "paimon/defs.h"
#include "paimon/status.h"

namespace paimon {
namespace {

std::string FieldOptionKey(const std::string& field_name, const char* option) {
    return std::string(Options::FIELDS_PREFIX) + "." + field_name + "." + option;
}

Result<std::vector<int32_t>> ResolveFields(const std::shared_ptr<arrow::StructType>& row_type,
                                           const std::vector<std::string>& names,
                                           const std::string& option_name) {
    std::vector<int32_t> fields;
    fields.reserve(names.size());
    for (const std::string& name : names) {
        int32_t index = row_type->GetFieldIndex(name);
        if (index < 0) {
            return Status::Invalid(fmt::format("Field '{}' configured by '{}' does not exist in {}",
                                               name, option_name, row_type->ToString()));
        }
        fields.push_back(index);
    }
    return fields;
}

std::shared_ptr<InternalArray> MakeRows(std::vector<std::shared_ptr<InternalRow>> rows,
                                        std::vector<std::shared_ptr<InternalArray>> holders) {
    std::vector<VariantType> values;
    values.reserve(rows.size());
    for (std::shared_ptr<InternalRow>& row : rows) {
        values.push_back(std::move(row));
    }
    return std::make_shared<GenericArray>(std::move(values), std::move(holders));
}

void AppendNonNullRows(const std::shared_ptr<InternalArray>& array, int32_t row_fields,
                       int32_t limit, std::vector<std::shared_ptr<InternalRow>>* rows) {
    int32_t added = 0;
    for (int32_t i = 0; i < array->Size() && added < limit; ++i) {
        if (!array->IsNullAt(i)) {
            rows->push_back(array->GetRow(i, row_fields));
            ++added;
        }
    }
}

}  // namespace

FieldNestedUpdateAgg::FieldNestedUpdateAgg(const std::shared_ptr<arrow::DataType>& field_type,
                                           std::shared_ptr<arrow::StructType> row_type,
                                           std::vector<int32_t> key_fields,
                                           CoreOptions::NestedKeyNullStrategy null_strategy,
                                           std::unique_ptr<FieldsComparator> sequence_comparator,
                                           int32_t count_limit,
                                           const std::shared_ptr<MemoryPool>& pool)
    : FieldAggregator(NAME, field_type, pool),
      row_type_(std::move(row_type)),
      key_fields_(std::move(key_fields)),
      null_strategy_(null_strategy),
      sequence_comparator_(std::move(sequence_comparator)),
      count_limit_(count_limit) {}

FieldNestedUpdateAgg::~FieldNestedUpdateAgg() = default;

Result<std::unique_ptr<FieldNestedUpdateAgg>> FieldNestedUpdateAgg::Create(
    const std::shared_ptr<arrow::DataType>& field_type, const CoreOptions& options,
    const std::string& field_name, const std::shared_ptr<MemoryPool>& pool) {
    if (field_type->id() != arrow::Type::LIST) {
        return Status::Invalid(
            fmt::format("invalid field type {} for field '{}' of {}, supposed to be array<struct>",
                        field_type->ToString(), field_name, NAME));
    }
    std::shared_ptr<arrow::ListType> list_type =
        arrow::internal::checked_pointer_cast<arrow::ListType>(field_type);
    if (list_type->value_type()->id() != arrow::Type::STRUCT) {
        return Status::Invalid(
            fmt::format("invalid field type {} for field '{}' of {}, supposed to be array<struct>",
                        field_type->ToString(), field_name, NAME));
    }
    std::shared_ptr<arrow::StructType> row_type =
        arrow::internal::checked_pointer_cast<arrow::StructType>(list_type->value_type());

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> key_names,
                           options.FieldNestedUpdateAggNestedKey(field_name));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> sequence_names,
                           options.FieldNestedUpdateAggNestedSequenceField(field_name));
    bool strategy_configured =
        options.ToMap().count(FieldOptionKey(field_name, Options::NESTED_KEY_NULL_STRATEGY)) > 0;
    if (key_names.empty() && strategy_configured) {
        return Status::Invalid(
            "Option 'fields.<field-name>.nested-key-null-strategy' requires "
            "'fields.<field-name>.nested-key' to be configured.");
    }
    if (key_names.empty() && !sequence_names.empty()) {
        return Status::Invalid(
            "Option 'fields.<field-name>.nested-sequence-field' requires "
            "'fields.<field-name>.nested-key' to be configured.");
    }

    PAIMON_ASSIGN_OR_RAISE(std::vector<int32_t> key_fields,
                           ResolveFields(row_type, key_names, Options::NESTED_KEY));
    PAIMON_ASSIGN_OR_RAISE(std::vector<int32_t> sequence_fields,
                           ResolveFields(row_type, sequence_names, Options::NESTED_SEQUENCE_FIELD));
    std::unique_ptr<FieldsComparator> sequence_comparator;
    if (!sequence_fields.empty()) {
        std::vector<DataField> row_fields;
        row_fields.reserve(row_type->num_fields());
        for (int32_t i = 0; i < row_type->num_fields(); ++i) {
            row_fields.emplace_back(i, row_type->field(i));
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FieldsComparator> comparator,
                               FieldsComparator::Create(row_fields, sequence_fields,
                                                        /*is_ascending_order=*/true));
        sequence_comparator = std::move(comparator);
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions::NestedKeyNullStrategy null_strategy,
                           options.FieldNestedUpdateAggNestedKeyNullStrategy(field_name));
    PAIMON_ASSIGN_OR_RAISE(int32_t count_limit, options.FieldNestedUpdateAggCountLimit(field_name));
    return std::unique_ptr<FieldNestedUpdateAgg>(
        new FieldNestedUpdateAgg(field_type, std::move(row_type), std::move(key_fields),
                                 null_strategy, std::move(sequence_comparator), count_limit, pool));
}

Result<VariantType> FieldNestedUpdateAgg::Agg(const VariantType& accumulator,
                                              const VariantType& input_field) {
    return AggImpl(accumulator, input_field);
}

Result<bool> FieldNestedUpdateAgg::AcceptKey(const InternalRow& row) const {
    bool contains_null = false;
    for (int32_t field : key_fields_) {
        contains_null = contains_null || row.IsNullAt(field);
    }
    if (!contains_null || null_strategy_ == CoreOptions::NestedKeyNullStrategy::MERGE) {
        return true;
    }
    if (null_strategy_ == CoreOptions::NestedKeyNullStrategy::IGNORE) {
        return false;
    }
    return Status::Invalid("Nested key contains null values. Primary key fields must not be null.");
}

Result<bool> FieldNestedUpdateAgg::KeysEqual(const InternalRow& lhs, const InternalRow& rhs) const {
    for (int32_t field : key_fields_) {
        PAIMON_ASSIGN_OR_RAISE(
            VariantType lhs_value,
            FieldAggregateUtils::GetValue(lhs, field, row_type_->field(field)->type()));
        PAIMON_ASSIGN_OR_RAISE(
            VariantType rhs_value,
            FieldAggregateUtils::GetValue(rhs, field, row_type_->field(field)->type()));
        PAIMON_ASSIGN_OR_RAISE(
            bool equal,
            FieldAggregateUtils::Equals(lhs_value, rhs_value, row_type_->field(field)->type()));
        if (!equal) {
            return false;
        }
    }
    return true;
}

Result<VariantType> FieldNestedUpdateAgg::AggImpl(const VariantType& accumulator,
                                                  const VariantType& input_field) const {
    if (DataDefine::IsVariantNull(input_field)) {
        return accumulator;
    }
    auto input = DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(input_field);
    std::shared_ptr<InternalArray> acc =
        DataDefine::IsVariantNull(accumulator)
            ? nullptr
            : DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(accumulator);

    if (key_fields_.empty()) {
        if (acc && acc->Size() >= count_limit_) {
            return accumulator;
        }
        std::vector<std::shared_ptr<InternalRow>> rows;
        if (acc) {
            rows.reserve(acc->Size() + input->Size());
            AppendNonNullRows(acc, row_type_->num_fields(), acc->Size(), &rows);
        }
        int32_t remaining = acc ? count_limit_ - acc->Size() : count_limit_;
        AppendNonNullRows(input, row_type_->num_fields(), remaining, &rows);
        std::vector<std::shared_ptr<InternalArray>> holders;
        if (acc) {
            holders.push_back(acc);
        }
        holders.push_back(input);
        return VariantType(
            std::static_pointer_cast<InternalArray>(MakeRows(std::move(rows), std::move(holders))));
    }

    std::vector<std::shared_ptr<InternalRow>> rows;
    auto add_rows = [&](const std::shared_ptr<InternalArray>& array,
                        bool limit_new_keys) -> Status {
        if (!array) {
            return Status::OK();
        }
        for (int32_t i = 0; i < array->Size(); ++i) {
            if (array->IsNullAt(i)) {
                continue;
            }
            std::shared_ptr<InternalRow> row = array->GetRow(i, row_type_->num_fields());
            PAIMON_ASSIGN_OR_RAISE(bool accept, AcceptKey(*row));
            if (!accept) {
                continue;
            }
            int32_t existing = -1;
            for (int32_t j = 0; j < static_cast<int32_t>(rows.size()); ++j) {
                PAIMON_ASSIGN_OR_RAISE(bool equal, KeysEqual(*rows[j], *row));
                if (equal) {
                    existing = j;
                    break;
                }
            }
            if (existing >= 0) {
                if (!sequence_comparator_ ||
                    sequence_comparator_->CompareTo(*row, *rows[existing]) >= 0) {
                    rows[existing] = std::move(row);
                }
            } else if (!limit_new_keys || static_cast<int32_t>(rows.size()) < count_limit_) {
                rows.push_back(std::move(row));
            }
        }
        return Status::OK();
    };
    PAIMON_RETURN_NOT_OK(add_rows(acc, /*limit_new_keys=*/false));
    PAIMON_RETURN_NOT_OK(add_rows(input, /*limit_new_keys=*/true));
    std::vector<std::shared_ptr<InternalArray>> holders;
    if (acc) {
        holders.push_back(acc);
    }
    holders.push_back(input);
    return VariantType(
        std::static_pointer_cast<InternalArray>(MakeRows(std::move(rows), std::move(holders))));
}

Result<VariantType> FieldNestedUpdateAgg::Retract(const VariantType& accumulator,
                                                  const VariantType& input_field) const {
    if (DataDefine::IsVariantNull(accumulator) || DataDefine::IsVariantNull(input_field)) {
        return accumulator;
    }
    auto acc = DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(accumulator);
    auto retract = DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(input_field);
    std::vector<std::shared_ptr<InternalRow>> rows;

    if (key_fields_.empty()) {
        AppendNonNullRows(acc, row_type_->num_fields(), acc->Size(), &rows);
        for (int32_t i = 0; i < retract->Size(); ++i) {
            if (retract->IsNullAt(i)) {
                continue;
            }
            std::shared_ptr<InternalRow> retract_row = retract->GetRow(i, row_type_->num_fields());
            for (auto iter = rows.begin(); iter != rows.end();) {
                PAIMON_ASSIGN_OR_RAISE(
                    bool equal, FieldAggregateUtils::Equals(VariantType(*iter),
                                                            VariantType(retract_row), row_type_));
                if (equal) {
                    iter = rows.erase(iter);
                } else {
                    ++iter;
                }
            }
        }
        return VariantType(std::static_pointer_cast<InternalArray>(
            MakeRows(std::move(rows), std::vector<std::shared_ptr<InternalArray>>{acc, retract})));
    }

    for (int32_t i = 0; i < acc->Size(); ++i) {
        if (acc->IsNullAt(i)) {
            continue;
        }
        std::shared_ptr<InternalRow> row = acc->GetRow(i, row_type_->num_fields());
        PAIMON_ASSIGN_OR_RAISE(bool accept, AcceptKey(*row));
        if (!accept) {
            continue;
        }
        int32_t existing = -1;
        for (int32_t j = 0; j < static_cast<int32_t>(rows.size()); ++j) {
            PAIMON_ASSIGN_OR_RAISE(bool equal, KeysEqual(*rows[j], *row));
            if (equal) {
                existing = j;
                break;
            }
        }
        if (existing >= 0) {
            rows[existing] = std::move(row);
        } else {
            rows.push_back(std::move(row));
        }
    }

    for (int32_t i = 0; i < retract->Size(); ++i) {
        if (retract->IsNullAt(i)) {
            continue;
        }
        std::shared_ptr<InternalRow> retract_row = retract->GetRow(i, row_type_->num_fields());
        PAIMON_ASSIGN_OR_RAISE(bool accept, AcceptKey(*retract_row));
        if (!accept) {
            continue;
        }
        for (auto iter = rows.begin(); iter != rows.end();) {
            PAIMON_ASSIGN_OR_RAISE(bool equal, KeysEqual(**iter, *retract_row));
            if (equal) {
                iter = rows.erase(iter);
            } else {
                ++iter;
            }
        }
    }
    return VariantType(std::static_pointer_cast<InternalArray>(
        MakeRows(std::move(rows), std::vector<std::shared_ptr<InternalArray>>{acc, retract})));
}

}  // namespace paimon
