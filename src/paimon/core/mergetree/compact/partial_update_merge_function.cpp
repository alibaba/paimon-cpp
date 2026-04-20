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

#include "paimon/core/mergetree/compact/partial_update_merge_function.h"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <variant>

#include "arrow/type.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/internal_row_utils.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/mergetree/compact/aggregate/field_aggregator_factory.h"
#include "paimon/core/mergetree/compact/aggregate/field_last_non_null_value_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_primary_key_agg.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/defs.h"

namespace paimon {
PartialUpdateMergeFunction::PartialUpdateMergeFunction(
    bool ignore_delete, bool remove_record_on_delete, bool field_sequence_enabled,
    std::vector<InternalRow::FieldGetterFunc>&& getters,
    std::map<int32_t, std::shared_ptr<FieldsComparator>>&& field_comparators,
    std::map<int32_t, std::shared_ptr<FieldAggregator>>&& field_aggregators,
    std::set<int32_t>&& sequence_group_partial_delete)
    : ignore_delete_(ignore_delete),
      remove_record_on_delete_(remove_record_on_delete),
      field_sequence_enabled_(field_sequence_enabled),
      getters_(std::move(getters)),
      field_comparators_(std::move(field_comparators)),
      field_aggregators_(std::move(field_aggregators)),
      sequence_group_partial_delete_(std::move(sequence_group_partial_delete)) {}

Status PartialUpdateMergeFunction::ParseSequenceGroupFields(
    const CoreOptions& options,
    std::map<std::string, std::vector<std::string>>* value_field_to_seq_group_field,
    std::set<std::string>* seq_group_key_set) {
    auto fields_seq_group_map = options.GetFieldsSequenceGroups();
    for (const auto& [key, value] : fields_seq_group_map) {
        auto seq_fields =
            StringUtils::Split(key, Options::FIELDS_SEPARATOR, /*ignore_empty=*/false);
        auto seq_value_fields =
            StringUtils::Split(value, Options::FIELDS_SEPARATOR, /*ignore_empty=*/false);
        for (const auto& field : seq_value_fields) {
            auto iter = value_field_to_seq_group_field->find(field);
            if (iter != value_field_to_seq_group_field->end()) {
                return Status::Invalid(
                    fmt::format("field {} is defined repeatedly by multiple groups: {} and {}",
                                field, key, fmt::join(iter->second, ",")));
            }
            (*value_field_to_seq_group_field)[field] = seq_fields;
        }
        for (const auto& field : seq_fields) {
            (*value_field_to_seq_group_field)[field] = seq_fields;
            seq_group_key_set->insert(field);
        }
    }
    return Status::OK();
}

Status PartialUpdateMergeFunction::CompleteSequenceGroupFields(
    const TableSchema& table_schema,
    const std::map<std::string, std::vector<std::string>>& value_field_to_seq_group_field,
    std::vector<DataField>* value_fields) {
    if (value_field_to_seq_group_field.empty()) {
        // no field sequence group config
        return Status::OK();
    }
    std::set<std::string> value_field_names;
    for (const auto& field : *value_fields) {
        value_field_names.insert(field.Name());
    }

    std::vector<DataField> extra_sequence_fields;
    for (const auto& field : *value_fields) {
        auto iter = value_field_to_seq_group_field.find(field.Name());
        if (iter == value_field_to_seq_group_field.end()) {
            continue;
        }
        const auto& seq_group_fields = iter->second;
        for (const auto& seq_field : seq_group_fields) {
            auto iter_seq = value_field_names.find(seq_field);
            if (iter_seq == value_field_names.end()) {
                // complete sequence group field
                PAIMON_ASSIGN_OR_RAISE(DataField extra_sequence_field,
                                       table_schema.GetField(seq_field));
                extra_sequence_fields.emplace_back(extra_sequence_field);
                value_field_names.insert(seq_field);
            }
        }
    }
    if (!extra_sequence_fields.empty()) {
        value_fields->insert(value_fields->end(), extra_sequence_fields.begin(),
                             extra_sequence_fields.end());
    }
    return Status::OK();
}

Result<std::unique_ptr<PartialUpdateMergeFunction>> PartialUpdateMergeFunction::Create(
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::vector<std::string>& primary_keys, const CoreOptions& options,
    const std::map<std::string, std::vector<std::string>>& value_field_to_seq_group_field,
    const std::set<std::string>& seq_group_key_set) {
    // 1. create field aggregator
    std::map<int32_t, std::shared_ptr<FieldAggregator>> field_aggregators;
    PAIMON_ASSIGN_OR_RAISE(
        field_aggregators,
        CreateFieldAggregators(value_schema, primary_keys, options, value_field_to_seq_group_field,
                               seq_group_key_set));
    // 2. create field seq comparator
    std::map<int32_t, std::shared_ptr<FieldsComparator>> field_comparators;
    PAIMON_ASSIGN_OR_RAISE(field_comparators,
                           CreateFieldSeqComparator(value_schema, value_field_to_seq_group_field));

    // 3. fetch and check retract config
    bool remove_record_on_delete = options.PartialUpdateRemoveRecordOnDelete();
    bool ignore_delete = options.IgnoreDelete();
    if (remove_record_on_delete && ignore_delete) {
        return Status::Invalid(fmt::format(
            "{} and {} have conflicting behavior so should not be enabled at the same time.",
            Options::IGNORE_DELETE, Options::PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE));
    }
    if (remove_record_on_delete && !field_comparators.empty()) {
        return Status::Invalid(
            fmt::format("sequence group and {} have conflicting behavior so should not be "
                        "enabled at the same time.",
                        Options::PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE));
    }

    std::vector<std::string> remove_record_on_sequence_group_fields =
        options.GetPartialUpdateRemoveRecordOnSequenceGroup();
    std::set<int32_t> sequence_group_partial_delete;
    if (!remove_record_on_sequence_group_fields.empty()) {
        if (!ObjectUtils::ContainsAll(seq_group_key_set, remove_record_on_sequence_group_fields)) {
            return Status::Invalid(
                fmt::format("field {} defined in {} option must be part of sequence groups.",
                            fmt::join(remove_record_on_sequence_group_fields, ","),
                            Options::PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP));
        }
        for (const auto& remove_sequence_group_field : remove_record_on_sequence_group_fields) {
            auto field_idx = value_schema->GetFieldIndex(remove_sequence_group_field);
            if (field_idx != -1) {
                sequence_group_partial_delete.insert(field_idx);
            }
        }
    }

    // 4. create data getters
    PAIMON_ASSIGN_OR_RAISE(std::vector<InternalRow::FieldGetterFunc> getters,
                           InternalRowUtils::CreateFieldGetters(value_schema, /*use_view=*/true));
    return std::unique_ptr<PartialUpdateMergeFunction>(new PartialUpdateMergeFunction(
        ignore_delete, remove_record_on_delete,
        /*field_sequence_enabled=*/!value_field_to_seq_group_field.empty(), std::move(getters),
        std::move(field_comparators), std::move(field_aggregators),
        std::move(sequence_group_partial_delete)));
}
Result<std::map<int32_t, std::shared_ptr<FieldsComparator>>>
PartialUpdateMergeFunction::CreateFieldSeqComparator(
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::map<std::string, std::vector<std::string>>& value_field_to_seq_group_field) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> value_data_fields,
                           DataField::ConvertArrowSchemaToDataFields(value_schema));
    std::map<int32_t, std::shared_ptr<FieldsComparator>> field_seq_comparators;
    // seq group fields -> FieldsComparator
    std::map<std::vector<std::string>, std::shared_ptr<FieldsComparator>>
        field_seq_group_comparator_map;
    for (size_t i = 0; i < value_data_fields.size(); i++) {
        const auto& value_field = value_data_fields[i];
        auto iter = value_field_to_seq_group_field.find(value_field.Name());
        if (iter == value_field_to_seq_group_field.end()) {
            continue;
        }
        const auto& seq_group = iter->second;
        auto comparator_iter = field_seq_group_comparator_map.find(seq_group);
        if (comparator_iter != field_seq_group_comparator_map.end()) {
            // comparator has been created
            field_seq_comparators[i] = comparator_iter->second;
            continue;
        }
        // create comparator
        std::vector<int32_t> sort_fields;
        sort_fields.reserve(seq_group.size());
        for (const auto& seq_group_field : seq_group) {
            auto field_idx = value_schema->GetFieldIndex(seq_group_field);
            if (field_idx == -1) {
                return Status::Invalid(
                    fmt::format("cannot find sequence group field {} in value schema, unexpected.",
                                seq_group_field));
            }
            sort_fields.push_back(field_idx);
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FieldsComparator> cmp,
                               FieldsComparator::Create(value_data_fields, sort_fields,
                                                        /*is_ascending_order=*/true));
        field_seq_group_comparator_map[seq_group] = cmp;
        field_seq_comparators[i] = cmp;
    }
    return field_seq_comparators;
}

Result<std::map<int32_t, std::shared_ptr<FieldAggregator>>>
PartialUpdateMergeFunction::CreateFieldAggregators(
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::vector<std::string>& primary_keys, const CoreOptions& options,
    const std::map<std::string, std::vector<std::string>>& value_field_to_seq_group_field,
    const std::set<std::string>& seq_group_key_set) {
    std::map<int32_t, std::shared_ptr<FieldAggregator>> aggregators;
    std::optional<std::string> default_agg_func = options.GetFieldsDefaultFunc();
    for (int32_t i = 0; i < value_schema->num_fields(); i++) {
        const auto& field_name = value_schema->field(i)->name();
        const auto& field_type = value_schema->field(i)->type();
        if (seq_group_key_set.find(field_name) != seq_group_key_set.end()) {
            // no agg for sequence fields
            continue;
        }
        auto primary_key_iter = std::find(primary_keys.begin(), primary_keys.end(), field_name);
        if (primary_key_iter != primary_keys.end()) {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FieldAggregator> agg,
                                   FieldAggregatorFactory::CreateFieldAggregator(
                                       field_name, field_type, FieldPrimaryKeyAgg::NAME, options));
            aggregators[i] = std::move(agg);
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> str_agg,
                               options.GetFieldAggFunc(field_name));
        if (str_agg == std::nullopt) {
            str_agg = default_agg_func;
        }
        if (str_agg) {
            // last_non_null_value doesn't require sequence group
            if (str_agg.value() != FieldLastNonNullValueAgg::NAME &&
                value_field_to_seq_group_field.find(field_name) ==
                    value_field_to_seq_group_field.end()) {
                return Status::Invalid(fmt::format(
                    "Must use sequence group for aggregation functions but not found for field {}",
                    field_name));
            }
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FieldAggregator> agg,
                                   FieldAggregatorFactory::CreateFieldAggregator(
                                       field_name, field_type, str_agg.value(), options));
            aggregators[i] = std::move(agg);
        }
    }

    return aggregators;
}

void PartialUpdateMergeFunction::Reset() {
    current_key_.reset();
    meet_insert_ = false;
    not_null_column_filled_ = false;
    row_ = std::make_unique<GenericRow>(getters_.size());
    last_seq_num_ = 0;
    for (auto& [_, agg] : field_aggregators_) {
        assert(agg);
        agg->Reset();
    }
}

Status PartialUpdateMergeFunction::Add(KeyValue&& moved_kv) {
    // refresh key object to avoid reference overwritten
    KeyValue kv = std::move(moved_kv);
    current_key_ = kv.key;
    current_delete_row_ = false;

    if (kv.value_kind->IsRetract()) {
        // In 0.7- versions, the delete records might be written into data file even when
        // ignore-delete configured, so ignoreDelete still needs to be checked
        if (ignore_delete_) {
            if (!not_null_column_filled_) {
                InitRowAndHoldData(std::move(kv.value));
                not_null_column_filled_ = true;
            }
            return Status::OK();
        }

        last_seq_num_ = kv.sequence_number;

        if (field_sequence_enabled_) {
            // RetractWithSequenceGroup handles InitRow and AddDataHolder internally
            PAIMON_RETURN_NOT_OK(RetractWithSequenceGroup(std::move(kv)));
            return Status::OK();
        }
        if (remove_record_on_delete_) {
            if (kv.value_kind == RowKind::Delete()) {
                current_delete_row_ = true;
                row_ = std::make_unique<GenericRow>(getters_.size());
                InitRowAndHoldData(std::move(kv.value));
            } else if (!not_null_column_filled_) {
                InitRowAndHoldData(std::move(kv.value));
                not_null_column_filled_ = true;
            }
            return Status::OK();
        }

        return Status::Invalid(
            "By default, Partial update can not accept delete records, you can choose one of "
            "the following solutions:\n"
            "1. Configure 'ignore-delete' to ignore delete records.\n"
            "2. Configure 'partial-update.remove-record-on-delete' to remove the whole row "
            "when receiving delete records.\n"
            "3. Configure 'sequence-group's to retract partial columns. Also configure "
            "'partial-update.remove-record-on-sequence-group' to remove the whole row when "
            "receiving deleted records of specified sequence group.");
    }
    last_seq_num_ = kv.sequence_number;
    if (field_comparators_.empty()) {
        UpdateNonNullFields(std::move(kv));
    } else {
        UpdateWithSequenceGroup(std::move(kv));
    }
    meet_insert_ = true;
    not_null_column_filled_ = true;
    return Status::OK();
}

void PartialUpdateMergeFunction::UpdateNonNullFields(KeyValue&& kv) {
    for (size_t i = 0; i < getters_.size(); ++i) {
        VariantType field = getters_[i](*(kv.value));
        if (!DataDefine::IsVariantNull(field)) {
            row_->SetField(i, field);
        }
    }
    row_->AddDataHolder(std::move(kv.value));
}

void PartialUpdateMergeFunction::UpdateWithSequenceGroup(KeyValue&& kv) {
    for (size_t i = 0; i < getters_.size(); ++i) {
        VariantType field = getters_[i](*(kv.value));
        VariantType accumulator = getters_[i](*row_);
        auto comp_iter = field_comparators_.find(i);
        auto agg_iter = field_aggregators_.find(i);
        FieldAggregator* agg =
            (agg_iter == field_aggregators_.end() ? nullptr : agg_iter->second.get());
        if (comp_iter == field_comparators_.end()) {
            if (agg) {
                row_->SetField(i, agg->Agg(accumulator, field));
            } else if (!DataDefine::IsVariantNull(field)) {
                row_->SetField(i, field);
            }
        } else {
            auto& seq_comparator = comp_iter->second;
            assert(seq_comparator);
            if (IsEmptySequenceGroup(kv, seq_comparator)) {
                // skip null sequence group
                continue;
            }
            if (seq_comparator->CompareTo(*(kv.value), *row_) >= 0) {
                int32_t idx = i;
                bool contain_idx = ObjectUtils::Contains(seq_comparator->CompareFields(), idx);
                if (contain_idx) {
                    // Multiple sequence fields should be updated at once.
                    for (const auto& field_idx : seq_comparator->CompareFields()) {
                        row_->SetField(field_idx, getters_[field_idx](*(kv.value)));
                    }
                    continue;
                }
                row_->SetField(i, agg ? agg->Agg(accumulator, field) : field);
            } else if (agg) {
                row_->SetField(i, agg->AggReversed(accumulator, field));
            }
        }
    }
    row_->AddDataHolder(std::move(kv.value));
}

Status PartialUpdateMergeFunction::RetractWithSequenceGroup(KeyValue&& kv) {
    // Initialize row with all field values if this is the first record.
    // InitRow only reads field values (string_view etc.) from kv.value without taking ownership.
    // kv.value remains alive throughout this method, so the views are safe until AddDataHolder
    // at the end transfers ownership to row_.
    if (!not_null_column_filled_) {
        InitRow(*(kv.value));
        not_null_column_filled_ = true;
    }

    std::set<int32_t> updated_sequence_fields;
    for (size_t i = 0; i < getters_.size(); ++i) {
        auto cmp_iter = field_comparators_.find(i);
        if (cmp_iter != field_comparators_.end()) {
            auto& seq_comparator = cmp_iter->second;
            assert(seq_comparator);
            if (IsEmptySequenceGroup(kv, seq_comparator)) {
                // skip null sequence group
                continue;
            }
            auto agg_iter = field_aggregators_.find(i);
            FieldAggregator* agg =
                (agg_iter == field_aggregators_.end() ? nullptr : agg_iter->second.get());
            if (seq_comparator->CompareTo(*(kv.value), *row_) >= 0) {
                int32_t idx = i;
                bool contain_idx = ObjectUtils::Contains(seq_comparator->CompareFields(), idx);
                if (contain_idx) {
                    // Multiple sequence fields should be updated at once.
                    for (const auto& field_idx : seq_comparator->CompareFields()) {
                        if (updated_sequence_fields.find(field_idx) ==
                            updated_sequence_fields.end()) {
                            if (kv.value_kind == RowKind::Delete() &&
                                sequence_group_partial_delete_.find(field_idx) !=
                                    sequence_group_partial_delete_.end()) {
                                current_delete_row_ = true;
                                row_ = std::make_unique<GenericRow>(getters_.size());
                                InitRowAndHoldData(std::move(kv.value));
                                return Status::OK();
                            } else {
                                row_->SetField(field_idx, getters_[field_idx](*(kv.value)));
                                updated_sequence_fields.insert(field_idx);
                            }
                        }
                    }
                } else {
                    if (!agg) {
                        // retract normal field
                        row_->SetField(i, NullType());
                    } else {
                        // retract agg field
                        PAIMON_ASSIGN_OR_RAISE(
                            VariantType ret,
                            agg->Retract(getters_[i](*row_), getters_[i](*(kv.value))));
                        row_->SetField(i, ret);
                    }
                }
            } else if (agg) {
                // retract agg field for old sequence
                PAIMON_ASSIGN_OR_RAISE(VariantType ret,
                                       agg->Retract(getters_[i](*row_), getters_[i](*(kv.value))));
                row_->SetField(i, ret);
            }
        }
    }
    row_->AddDataHolder(std::move(kv.value));
    return Status::OK();
}

void PartialUpdateMergeFunction::InitRow(const InternalRow& value) {
    for (size_t i = 0; i < getters_.size(); ++i) {
        VariantType field = getters_[i](value);
        row_->SetField(i, field);
    }
}

void PartialUpdateMergeFunction::InitRowAndHoldData(std::unique_ptr<InternalRow>&& value) {
    InitRow(*value);
    row_->AddDataHolder(std::move(value));
}

bool PartialUpdateMergeFunction::IsEmptySequenceGroup(
    const KeyValue& kv, const std::shared_ptr<FieldsComparator>& comparator) const {
    for (const auto& field_idx : comparator->CompareFields()) {
        if (!DataDefine::IsVariantNull(getters_[field_idx](*(kv.value)))) {
            return false;
        }
    }
    return true;
}
}  // namespace paimon
