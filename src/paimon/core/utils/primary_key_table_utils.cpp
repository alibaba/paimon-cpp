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

#include "paimon/core/utils/primary_key_table_utils.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <set>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/mergetree/compact/aggregate/aggregate_merge_function.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/first_row_merge_function.h"
#include "paimon/core/mergetree/compact/merge_function.h"
#include "paimon/core/mergetree/compact/partial_update_merge_function.h"
#include "paimon/core/options/merge_engine.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

Result<std::unique_ptr<MergeFunction>> PrimaryKeyTableUtils::CreateMergeFunction(
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::vector<std::string>& primary_keys, const CoreOptions& options) {
    auto merge_engine = options.GetMergeEngine();
    std::unique_ptr<MergeFunction> merge_function;
    if (merge_engine == MergeEngine::DEDUPLICATE) {
        merge_function = std::make_unique<DeduplicateMergeFunction>(options.IgnoreDelete());
    } else if (merge_engine == MergeEngine::FIRST_ROW) {
        merge_function = std::make_unique<FirstRowMergeFunction>(options.IgnoreDelete());
    } else if (merge_engine == MergeEngine::AGGREGATE) {
        PAIMON_ASSIGN_OR_RAISE(merge_function,
                               AggregateMergeFunction::Create(value_schema, primary_keys, options));
    } else if (merge_engine == MergeEngine::PARTIAL_UPDATE) {
        std::map<std::string, std::vector<std::string>> value_field_to_seq_group_field;
        std::set<std::string> seq_group_key_set;
        PAIMON_RETURN_NOT_OK(PartialUpdateMergeFunction::ParseSequenceGroupFields(
            options, &value_field_to_seq_group_field, &seq_group_key_set));
        PAIMON_ASSIGN_OR_RAISE(
            merge_function,
            PartialUpdateMergeFunction::Create(value_schema, primary_keys, options,
                                               value_field_to_seq_group_field, seq_group_key_set));
    } else {
        return Status::Invalid(
            "only support deduplicate/partial-update/aggregation/first-row merge engine.");
    }
    return merge_function;
}

Result<std::unique_ptr<FieldsComparator>> PrimaryKeyTableUtils::CreateSequenceFieldsComparator(
    const std::vector<DataField>& value_fields, const CoreOptions& options) {
    auto sequence_field_names = options.GetSequenceField();
    if (sequence_field_names.empty()) {
        return std::unique_ptr<FieldsComparator>();
    }
    std::map<std::string, int32_t> value_field_name_to_idx =
        ObjectUtils::CreateIdentifierToIndexMap(
            value_fields, [](const DataField& field) -> std::string { return field.Name(); });

    std::vector<int32_t> sort_field_idxs;
    for (const auto& seq_field_name : sequence_field_names) {
        auto iter = value_field_name_to_idx.find(seq_field_name);
        if (iter == value_field_name_to_idx.end()) {
            return Status::Invalid(
                fmt::format("sequence field {} does not in value fields", seq_field_name));
        } else {
            sort_field_idxs.push_back(iter->second);
        }
    }
    return FieldsComparator::Create(value_fields, sort_field_idxs,
                                    options.SequenceFieldSortOrderIsAscending());
}

}  // namespace paimon
