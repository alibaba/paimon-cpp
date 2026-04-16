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

#include "paimon/core/mergetree/compact/aggregate/aggregate_merge_function.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <variant>

#include "arrow/api.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/internal_row_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/mergetree/compact/aggregate/field_aggregator_factory.h"
#include "paimon/core/mergetree/compact/aggregate/field_last_non_null_value_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_last_value_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_primary_key_agg.h"

namespace paimon {
Result<std::unique_ptr<AggregateMergeFunction>> AggregateMergeFunction::Create(
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::vector<std::string>& primary_keys, const CoreOptions& options) {
    std::vector<std::unique_ptr<FieldAggregator>> aggregators;
    aggregators.reserve(value_schema->num_fields());
    for (int32_t i = 0; i < value_schema->num_fields(); i++) {
        const auto& field_name = value_schema->field(i)->name();
        const auto& field_type = value_schema->field(i)->type();
        PAIMON_ASSIGN_OR_RAISE(std::string str_agg,
                               GetAggFuncName(field_name, primary_keys, options));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FieldAggregator> agg,
                               FieldAggregatorFactory::CreateFieldAggregator(field_name, field_type,
                                                                             str_agg, options));
        aggregators.push_back(std::move(agg));
    }

    bool remove_record_on_delete = options.AggregationRemoveRecordOnDelete();

    PAIMON_ASSIGN_OR_RAISE(std::vector<InternalRow::FieldGetterFunc> getters,
                           InternalRowUtils::CreateFieldGetters(value_schema, /*use_view=*/true));
    return std::unique_ptr<AggregateMergeFunction>(new AggregateMergeFunction(
        std::move(getters), std::move(aggregators), remove_record_on_delete));
}

Status AggregateMergeFunction::Add(KeyValue&& kv) {
    // When removeRecordOnDelete is enabled, if we receive a DELETE row,
    // mark the current row for deletion and initialize the row with input values.
    if (remove_record_on_delete_ && kv.value_kind == RowKind::Delete()) {
        current_delete_row_ = true;
        row_ = std::make_unique<GenericRow>(getters_.size());
        for (size_t i = 0; i < getters_.size(); i++) {
            row_->SetField(i, getters_[i](*(kv.value)));
        }
        row_->AddDataHolder(std::move(kv.value));
        latest_kv_ = std::move(kv);
        return Status::OK();
    }

    current_delete_row_ = false;
    bool is_retract = kv.value_kind->IsRetract();
    for (size_t i = 0; i < getters_.size(); i++) {
        auto accumulator = getters_[i](*row_);
        auto input_field = getters_[i](*(kv.value));
        VariantType merged_field;
        if (is_retract) {
            PAIMON_ASSIGN_OR_RAISE(merged_field,
                                   aggregators_[i]->Retract(accumulator, input_field));
        } else {
            merged_field = aggregators_[i]->Agg(accumulator, input_field);
        }
        row_->SetField(i, merged_field);
    }
    row_->AddDataHolder(std::move(kv.value));
    latest_kv_ = std::move(kv);
    return Status::OK();
}

Result<std::optional<KeyValue>> AggregateMergeFunction::GetResult() {
    assert(latest_kv_);
    latest_kv_.value().value = std::move(row_);
    latest_kv_.value().value_kind = current_delete_row_ ? RowKind::Delete() : RowKind::Insert();
    latest_kv_.value().level = KeyValue::UNKNOWN_LEVEL;
    return std::move(latest_kv_);
}

Result<std::string> AggregateMergeFunction::GetAggFuncName(
    const std::string& field_name, const std::vector<std::string>& primary_keys,
    const CoreOptions& options) {
    const auto& seq_fields = options.GetSequenceField();
    auto seq_iter = std::find(seq_fields.begin(), seq_fields.end(), field_name);
    if (seq_iter != seq_fields.end()) {
        // no agg for sequence fields, use last_value to do cover
        return std::string(FieldLastValueAgg::NAME);
    }

    auto pk_iter = std::find(primary_keys.begin(), primary_keys.end(), field_name);
    if (pk_iter != primary_keys.end()) {
        return std::string(FieldPrimaryKeyAgg::NAME);
    }

    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> str_agg, options.GetFieldAggFunc(field_name));
    if (str_agg == std::nullopt) {
        str_agg = options.GetFieldsDefaultFunc();
    }
    if (!str_agg) {
        str_agg = std::string(FieldLastNonNullValueAgg::NAME);
    }
    return str_agg.value();
}
}  // namespace paimon
