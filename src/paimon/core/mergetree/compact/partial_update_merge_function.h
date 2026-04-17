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
#include <cassert>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "paimon/common/data/generic_row.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/core_options.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/aggregate/field_aggregator.h"
#include "paimon/core/mergetree/compact/merge_function.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class CoreOptions;
class DataField;
class TableSchema;

/// A `MergeFunction` where key is primary key (unique) and value is the partial record, update
/// non-null fields on merge.
class PartialUpdateMergeFunction : public MergeFunction {
 public:
    static Status ParseSequenceGroupFields(
        const CoreOptions& options,
        std::map<std::string, std::vector<std::string>>* value_field_to_seq_group_field,
        std::set<std::string>* seq_group_key_set);

    static Status CompleteSequenceGroupFields(
        const TableSchema& table_schema,
        const std::map<std::string, std::vector<std::string>>& value_field_to_seq_group_field,
        std::vector<DataField>* value_fields);

    // precondition: value_schema is completed with fields_seq_group
    static Result<std::unique_ptr<PartialUpdateMergeFunction>> Create(
        const std::shared_ptr<arrow::Schema>& value_schema,
        const std::vector<std::string>& primary_keys, const CoreOptions& options,
        const std::map<std::string, std::vector<std::string>>& value_field_to_seq_group_field,
        const std::set<std::string>& seq_group_key_set);

    void Reset() override {
        current_key_.reset();
        row_ = std::make_unique<GenericRow>(getters_.size());
        for (auto& [_, agg] : field_aggregators_) {
            assert(agg);
            agg->Reset();
        }
    }

    Status Add(KeyValue&& kv) override;

    Result<std::optional<KeyValue>> GetResult() override {
        return std::optional<KeyValue>(
            KeyValue(current_delete_row_ ? RowKind::Delete() : RowKind::Insert(), last_seq_num_,
                     KeyValue::UNKNOWN_LEVEL, std::move(current_key_), std::move(row_)));
    };

 private:
    PartialUpdateMergeFunction(
        bool ignore_delete, bool remove_record_on_delete, bool field_sequence_enabled,
        std::vector<InternalRow::FieldGetterFunc>&& getters,
        std::map<int32_t, std::shared_ptr<FieldsComparator>>&& field_comparators,
        std::map<int32_t, std::shared_ptr<FieldAggregator>>&& field_aggregators,
        std::set<int32_t>&& sequence_group_partial_delete);

    static Result<std::map<int32_t, std::shared_ptr<FieldsComparator>>> CreateFieldSeqComparator(
        const std::shared_ptr<arrow::Schema>& value_schema,
        const std::map<std::string, std::vector<std::string>>& value_field_to_seq_group_field);

    static Result<std::map<int32_t, std::shared_ptr<FieldAggregator>>> CreateFieldAggregators(
        const std::shared_ptr<arrow::Schema>& value_schema,
        const std::vector<std::string>& primary_keys, const CoreOptions& options,
        const std::map<std::string, std::vector<std::string>>& value_field_to_seq_group_field,
        const std::set<std::string>& seq_group_key_set);

    bool IsEmptySequenceGroup(const KeyValue& kv,
                              const std::shared_ptr<FieldsComparator>& comparator) const;

    void UpdateNonNullFields(KeyValue&& kv);

    void UpdateWithSequenceGroup(KeyValue&& kv);

    Status RetractWithSequenceGroup(KeyValue&& kv);

 private:
    bool ignore_delete_;
    bool remove_record_on_delete_;
    bool field_sequence_enabled_;
    std::vector<InternalRow::FieldGetterFunc> getters_;
    std::map<int32_t, std::shared_ptr<FieldsComparator>> field_comparators_;
    std::map<int32_t, std::shared_ptr<FieldAggregator>> field_aggregators_;
    std::set<int32_t> sequence_group_partial_delete_;

    std::shared_ptr<InternalRow> current_key_;
    std::unique_ptr<GenericRow> row_;
    int64_t last_seq_num_ = KeyValue::UNKNOWN_SEQUENCE;
    bool current_delete_row_ = false;
};
}  // namespace paimon
