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

#include "paimon/core/utils/field_mapping.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <set>

#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/predicate/compound_predicate_impl.h"
#include "paimon/common/predicate/leaf_predicate_impl.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/core/casting/cast_executor_factory.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/defs.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/status.h"

namespace paimon {

Result<std::shared_ptr<arrow::Schema>> FieldMapping::GetPartitionSchema(
    const std::shared_ptr<arrow::Schema>& schema, const std::vector<std::string>& partition_keys) {
    arrow::FieldVector partition_fields;
    partition_fields.reserve(partition_keys.size());
    for (const auto& partition_key : partition_keys) {
        auto field = schema->GetFieldByName(partition_key);
        if (!field) {
            return Status::Invalid(
                fmt::format("get partition schema failed, cannot find partition key {} in schema",
                            partition_key));
        }
        partition_fields.push_back(field);
    }
    return arrow::schema(partition_fields);
}

Result<std::unique_ptr<FieldMappingBuilder>> FieldMappingBuilder::Create(
    const std::shared_ptr<arrow::Schema>& read_schema,
    const std::vector<std::string>& partition_keys, const std::shared_ptr<Predicate>& predicate) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> read_fields,
                           DataField::ConvertArrowSchemaToDataFields(read_schema));
    return std::unique_ptr<FieldMappingBuilder>(
        new FieldMappingBuilder(read_fields, partition_keys, predicate));
}

Result<std::unique_ptr<FieldMapping>> FieldMappingBuilder::CreateFieldMapping(
    const std::shared_ptr<arrow::Schema>& data_schema) const {
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> data_fields,
                           DataField::ConvertArrowSchemaToDataFields(data_schema));
    return CreateFieldMapping(data_fields);
}

Result<std::unique_ptr<FieldMapping>> FieldMappingBuilder::CreateFieldMapping(
    const std::vector<DataField>& data_fields) const {
    // generate non-exist field info
    std::optional<NonExistFieldInfo> non_exist_field_info = CreateNonExistFieldInfo(data_fields);

    // generate exist field info
    ExistFieldInfo exist_field_info = CreateExistFieldInfo(data_fields);

    // key: partition key, value: partition idx
    std::map<std::string, int32_t> partition_key_to_idx =
        ObjectUtils::CreateIdentifierToIndexMap(partition_keys_);

    PAIMON_ASSIGN_OR_RAISE(
        NonPartitionInfo non_partition_info,
        CreateNonPartitionInfo(data_fields, exist_field_info, partition_key_to_idx));
    PAIMON_ASSIGN_OR_RAISE(std::optional<PartitionInfo> partition_info,
                           CreatePartitionInfo(exist_field_info, partition_key_to_idx));
    return std::make_unique<FieldMapping>(partition_info, non_partition_info, non_exist_field_info);
}

ExistFieldInfo FieldMappingBuilder::CreateExistFieldInfo(
    const std::vector<DataField>& data_fields) const {
    // key:field id, value: {target_idx, read field}
    std::map<int32_t, std::pair<int32_t, DataField>> field_id_to_read_fields;
    for (size_t i = 0; i < read_fields_.size(); i++) {
        const auto& read_field = read_fields_[i];
        field_id_to_read_fields.emplace(read_field.Id(), std::make_pair(i, read_field));
    }

    ExistFieldInfo exist_field_info;
    for (const auto& data_field : data_fields) {
        auto iter = field_id_to_read_fields.find(data_field.Id());
        if (iter != field_id_to_read_fields.end()) {
            const auto& [target_idx, read_field] = iter->second;
            exist_field_info.exist_read_schema.push_back(read_field);
            exist_field_info.exist_data_schema.push_back(data_field);
            exist_field_info.idx_in_target_read_schema.push_back(target_idx);
        }
    }
    return exist_field_info;
}

std::optional<NonExistFieldInfo> FieldMappingBuilder::CreateNonExistFieldInfo(
    const std::vector<DataField>& data_fields) const {
    // key: field id, value: data field
    std::map<int32_t, DataField> field_id_to_data_fields;
    for (const auto& data_field : data_fields) {
        field_id_to_data_fields.emplace(data_field.Id(), data_field);
    }

    NonExistFieldInfo non_exist_field_info;
    for (size_t i = 0; i < read_fields_.size(); i++) {
        const auto& read_field = read_fields_[i];
        auto iter = field_id_to_data_fields.find(read_field.Id());
        if (iter == field_id_to_data_fields.end()) {
            non_exist_field_info.non_exist_read_schema.push_back(read_field);
            non_exist_field_info.idx_in_target_read_schema.push_back(i);
        }
    }
    if (non_exist_field_info.idx_in_target_read_schema.empty()) {
        return std::nullopt;
    }
    return non_exist_field_info;
}

Result<std::vector<std::shared_ptr<CastExecutor>>> FieldMappingBuilder::CreateDataCastExecutors(
    const std::vector<DataField>& read_fields, const std::vector<DataField>& data_fields) {
    assert(read_fields.size() == data_fields.size());
    std::vector<std::shared_ptr<CastExecutor>> cast_executors;
    cast_executors.reserve(read_fields.size());
    for (size_t i = 0; i < read_fields.size(); i++) {
        PAIMON_ASSIGN_OR_RAISE(FieldType read_type,
                               FieldTypeUtils::ConvertToFieldType(read_fields[i].Type()->id()));
        PAIMON_ASSIGN_OR_RAISE(FieldType data_type,
                               FieldTypeUtils::ConvertToFieldType(data_fields[i].Type()->id()));

        if (!read_fields[i].Type()->Equals(data_fields[i].Type())) {
            if (read_type == FieldType::MAP || read_type == FieldType::ARRAY ||
                read_type == FieldType::STRUCT) {
                return Status::Invalid("Only support column type evolution in atomic data type.");
            }
            auto executor_factory = CastExecutorFactory::GetCastExecutorFactory();
            auto cast_executor =
                executor_factory->GetCastExecutor(/*src=*/data_type, /*target=*/read_type);
            if (!cast_executor) {
                return Status::Invalid(
                    fmt::format("CreateDataCastExecutors failed: cannot find cast "
                                "executor for field {} from type {} to {}",
                                read_fields[i].Name(), FieldTypeUtils::FieldTypeToString(data_type),
                                FieldTypeUtils::FieldTypeToString(read_type)));
            }
            cast_executors.push_back(cast_executor);
        } else {
            cast_executors.push_back(nullptr);
        }
    }
    return cast_executors;
}

Result<NonPartitionInfo> FieldMappingBuilder::CreateNonPartitionInfo(
    const std::vector<DataField>& data_fields, const ExistFieldInfo& exist_field_info,
    const std::map<std::string, int32_t>& partition_keys) const {
    NonPartitionInfo non_partition_info;
    for (size_t i = 0; i < exist_field_info.exist_data_schema.size(); i++) {
        const auto& data_field = exist_field_info.exist_data_schema[i];
        const auto& read_field = exist_field_info.exist_read_schema[i];
        auto iter = partition_keys.find(read_field.Name());
        if (iter == partition_keys.end()) {
            non_partition_info.non_partition_read_schema.push_back(read_field);
            non_partition_info.non_partition_data_schema.push_back(data_field);
            non_partition_info.idx_in_target_read_schema.push_back(
                exist_field_info.idx_in_target_read_schema[i]);
        }
    }
    PAIMON_ASSIGN_OR_RAISE(non_partition_info.cast_executors,
                           CreateDataCastExecutors(non_partition_info.non_partition_read_schema,
                                                   non_partition_info.non_partition_data_schema));
    // prepare predicate: exclude the push down predicate with partition key and non-exist fields
    PAIMON_ASSIGN_OR_RAISE(non_partition_info.non_partition_filter, CreateDataFilters(data_fields));
    return non_partition_info;
}

Result<std::shared_ptr<Predicate>> FieldMappingBuilder::CreateDataFilters(
    const std::vector<DataField>& data_fields) const {
    if (!predicate_) {
        return predicate_;
    }
    // data schema mapping: id -> {data field idx, data field}
    std::map<int32_t, std::pair<int32_t, DataField>> field_id_to_data_fields;
    for (size_t i = 0; i < data_fields.size(); i++) {
        const auto& data_field = data_fields[i];
        field_id_to_data_fields.emplace(data_field.Id(), std::make_pair(i, data_field));
    }
    // table schema mapping: name -> read field
    std::map<std::string, DataField> field_name_to_read_fields;
    for (const auto& read_field : read_fields_) {
        field_name_to_read_fields.emplace(read_field.Name(), read_field);
    }
    // reconstruct predicate (e.g., modify field idx, cast literal)
    auto split_predicates = PredicateUtils::SplitAnd(predicate_);
    std::vector<std::shared_ptr<Predicate>> converted_predicates;
    converted_predicates.reserve(split_predicates.size());
    for (const auto& predicate : split_predicates) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<Predicate>> converted,
                               ReconstructPredicateWithDataFields(
                                   predicate, field_name_to_read_fields, field_id_to_data_fields));
        if (converted != std::nullopt) {
            converted_predicates.push_back(converted.value());
        }
    }
    // exclude partition fields in predicate
    std::set<std::string> partition_key_field_name_set(partition_keys_.begin(),
                                                       partition_keys_.end());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<Predicate>> remain_predicates,
                           PredicateUtils::ExcludePredicateWithFields(
                               converted_predicates, partition_key_field_name_set));
    if (remain_predicates.empty()) {
        return std::shared_ptr<Predicate>();
    }
    return PredicateBuilder::And(remain_predicates);
}

Result<std::optional<std::shared_ptr<Predicate>>> FieldMappingBuilder::ReconstructLeafPredicate(
    const std::shared_ptr<LeafPredicateImpl>& leaf_predicate,
    const std::map<std::string, DataField>& field_name_to_read_fields,
    const std::map<int32_t, std::pair<int32_t, DataField>>& field_id_to_data_fields) {
    auto read_field_iter = field_name_to_read_fields.find(leaf_predicate->FieldName());
    if (read_field_iter == field_name_to_read_fields.end()) {
        return Status::Invalid(
            fmt::format("invalid predicate, cannot find field {}", leaf_predicate->FieldName()));
    }
    auto data_field_iter = field_id_to_data_fields.find(read_field_iter->second.Id());
    if (data_field_iter == field_id_to_data_fields.end()) {
        return std::optional<std::shared_ptr<Predicate>>();
    }
    std::vector<Literal> new_literals;
    PAIMON_ASSIGN_OR_RAISE(FieldType read_type, FieldTypeUtils::ConvertToFieldType(
                                                    read_field_iter->second.Type()->id()));
    PAIMON_ASSIGN_OR_RAISE(FieldType data_type, FieldTypeUtils::ConvertToFieldType(
                                                    data_field_iter->second.second.Type()->id()));
    if (read_type != data_type ||
        (!read_field_iter->second.Type()->Equals(data_field_iter->second.second.Type()))) {
        // read type and data type are different, only push down integer predicates
        if (FieldTypeUtils::IsIntegerNumeric(read_type) &&
            FieldTypeUtils::IsIntegerNumeric(data_type) &&
            FieldTypeUtils::IntegerScaleLargerThan(read_type, data_type)) {
            auto executor_factory = CastExecutorFactory::GetCastExecutorFactory();
            auto cast_executor =
                executor_factory->GetCastExecutor(/*src=*/read_type, /*target=*/data_type);
            if (!cast_executor) {
                return Status::Invalid(fmt::format(
                    "ReconstructLeafPredicate failed: cannot find cast "
                    "executor for field {} from type {} to {}",
                    read_field_iter->second.Name(), FieldTypeUtils::FieldTypeToString(read_type),
                    FieldTypeUtils::FieldTypeToString(data_type)));
            }
            new_literals.reserve(leaf_predicate->Literals().size());
            for (const auto& literal : leaf_predicate->Literals()) {
                PAIMON_ASSIGN_OR_RAISE(
                    Literal casted,
                    cast_executor->Cast(literal, data_field_iter->second.second.Type()));
                // ignore if any literal is overflowed.
                if (CastingUtils::IsIntegerLiteralCastedOverflow(literal, casted)) {
                    return std::optional<std::shared_ptr<Predicate>>();
                }
                new_literals.push_back(casted);
            }
        } else {
            return std::optional<std::shared_ptr<Predicate>>();
        }
    } else {
        new_literals = leaf_predicate->Literals();
    }

    const auto& [data_field_idx, data_field] = data_field_iter->second;
    return std::optional<std::shared_ptr<Predicate>>(std::make_shared<LeafPredicateImpl>(
        leaf_predicate->GetLeafFunction(), /*data field idx*/ data_field_idx,
        /*data field name*/ data_field.Name(),
        /*data field type*/ data_type, new_literals));
}

Result<std::optional<std::shared_ptr<Predicate>>>
FieldMappingBuilder::ReconstructPredicateWithDataFields(
    const std::shared_ptr<Predicate>& predicate,
    const std::map<std::string, DataField>& field_name_to_read_fields,
    const std::map<int32_t, std::pair<int32_t, DataField>>& field_id_to_data_fields) {
    if (auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicateImpl>(predicate)) {
        return ReconstructLeafPredicate(leaf_predicate, field_name_to_read_fields,
                                        field_id_to_data_fields);
    } else if (auto compound_predicate =
                   std::dynamic_pointer_cast<CompoundPredicateImpl>(predicate)) {
        std::vector<std::shared_ptr<Predicate>> converted_children;
        for (const auto& child : compound_predicate->Children()) {
            PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<Predicate>> converted_child,
                                   ReconstructPredicateWithDataFields(
                                       child, field_name_to_read_fields, field_id_to_data_fields));
            if (converted_child == std::nullopt) {
                return std::optional<std::shared_ptr<Predicate>>();
            }
            assert(converted_child.value());
            converted_children.push_back(converted_child.value());
        }
        return std::optional<std::shared_ptr<Predicate>>(
            compound_predicate->NewCompoundPredicate(converted_children));
    }
    return Status::Invalid(
        "invalid predicate, cannot convert to leaf predicate or compound predicate");
}

Result<std::optional<PartitionInfo>> FieldMappingBuilder::CreatePartitionInfo(
    const ExistFieldInfo& exist_field_info,
    const std::map<std::string, int32_t>& partition_keys) const {
    if (partition_keys.empty()) {
        return std::optional<PartitionInfo>();
    }
    PartitionInfo partition_info;
    for (size_t i = 0; i < exist_field_info.exist_read_schema.size(); i++) {
        const auto& read_field = exist_field_info.exist_read_schema[i];
        auto iter = partition_keys.find(read_field.Name());
        if (iter != partition_keys.end()) {
            partition_info.partition_read_schema.push_back(read_field);
            partition_info.idx_in_target_read_schema.push_back(
                exist_field_info.idx_in_target_read_schema[i]);
            partition_info.idx_in_partition.push_back(iter->second);
        }
    }
    if (partition_info.idx_in_target_read_schema.empty()) {
        return std::optional<PartitionInfo>();
    }
    PAIMON_ASSIGN_OR_RAISE(partition_info.partition_filter,
                           PredicateUtils::CreatePickedFieldFilter(predicate_, partition_keys));
    return std::optional<PartitionInfo>(partition_info);
}

}  // namespace paimon
