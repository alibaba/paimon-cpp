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

#include "paimon/common/data/variant/variant_shredding_write_plan.h"

#include <utility>

#include "arrow/api.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/data/variant/variant_shredding_utils.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/types/data_type_json_parser.h"
#include "rapidjson/document.h"

namespace paimon {

namespace {

/// Recursively rebuilds `field`, replacing the variant fields planned under `paths` (grouped by
/// their leading index at this level) with their shredded physical types.
Result<std::shared_ptr<arrow::Field>> ReplacePlannedFields(
    const std::shared_ptr<arrow::Field>& field,
    const std::map<std::vector<int32_t>, std::shared_ptr<arrow::DataType>>& paths, size_t depth,
    std::vector<VariantShreddingWritePlan::PlannedColumn>* columns) {
    const auto& struct_type =
        arrow::internal::checked_cast<const arrow::StructType&>(*field->type());
    arrow::FieldVector new_fields = struct_type.fields();
    bool changed = false;
    auto it = paths.begin();
    while (it != paths.end()) {
        int32_t index = it->first[depth];
        // Collect the consecutive paths that descend into the same child.
        std::map<std::vector<int32_t>, std::shared_ptr<arrow::DataType>> child_paths;
        for (; it != paths.end() && it->first[depth] == index; ++it) {
            child_paths.emplace(it->first, it->second);
        }
        if (index < 0 || index >= struct_type.num_fields()) {
            return Status::Invalid(
                fmt::format("variant shredding path index {} is out of bounds", index));
        }
        const std::shared_ptr<arrow::Field>& child = struct_type.field(index);
        auto terminal = child_paths.begin();
        if (terminal->first.size() == depth + 1) {
            // The path terminates at this child: it must be a variant field.
            if (child_paths.size() > 1 || !VariantTypeUtils::IsVariantField(child)) {
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> physical_type,
                                   VariantShreddingUtils::VariantShreddingSchema(terminal->second));
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<VariantSchema> variant_schema,
                                   VariantShreddingUtils::BuildVariantSchema(physical_type));
            columns->push_back(VariantShreddingWritePlan::PlannedColumn{
                terminal->first, std::move(variant_schema), physical_type});
            new_fields[index] = child->WithType(physical_type);
            changed = true;
        } else {
            // The paths descend into a nested struct child.
            if (child->type()->id() != arrow::Type::STRUCT ||
                VariantTypeUtils::IsVariantField(child)) {
                continue;
            }
            size_t planned_before = columns->size();
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> new_child,
                                   ReplacePlannedFields(child, child_paths, depth + 1, columns));
            if (columns->size() > planned_before) {
                new_fields[index] = new_child;
                changed = true;
            }
        }
    }
    if (!changed) {
        return field;
    }
    return field->WithType(arrow::struct_(new_fields));
}

Status CollectPlannedColumns(const std::shared_ptr<arrow::Field>& logical_field,
                             const std::shared_ptr<arrow::Field>& physical_field,
                             std::vector<int32_t>* path,
                             std::vector<VariantShreddingWritePlan::PlannedColumn>* columns) {
    if (logical_field->name() != physical_field->name()) {
        return Status::Invalid(
            fmt::format("variant shredding physical field '{}' does not match logical field '{}'",
                        physical_field->name(), logical_field->name()));
    }
    if (VariantTypeUtils::IsVariantField(logical_field)) {
        if (logical_field->type()->Equals(*physical_field->type())) {
            return Status::OK();
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<VariantSchema> variant_schema,
                               VariantShreddingUtils::BuildVariantSchema(physical_field->type()));
        columns->push_back(VariantShreddingWritePlan::PlannedColumn{
            *path, std::move(variant_schema), physical_field->type()});
        return Status::OK();
    }
    if (logical_field->type()->Equals(*physical_field->type())) {
        return Status::OK();
    }
    if (logical_field->type()->id() != arrow::Type::STRUCT ||
        physical_field->type()->id() != arrow::Type::STRUCT) {
        return Status::Invalid(fmt::format(
            "variant shredding physical type of field '{}' differs outside a Variant column",
            logical_field->name()));
    }
    const auto& logical_type =
        arrow::internal::checked_cast<const arrow::StructType&>(*logical_field->type());
    const auto& physical_type =
        arrow::internal::checked_cast<const arrow::StructType&>(*physical_field->type());
    if (logical_type.num_fields() != physical_type.num_fields()) {
        return Status::Invalid(
            fmt::format("variant shredding physical struct '{}' has a different field count",
                        logical_field->name()));
    }
    for (int32_t i = 0; i < logical_type.num_fields(); ++i) {
        path->push_back(i);
        PAIMON_RETURN_NOT_OK(
            CollectPlannedColumns(logical_type.field(i), physical_type.field(i), path, columns));
        path->pop_back();
    }
    return Status::OK();
}

}  // namespace

Result<std::shared_ptr<VariantShreddingWritePlan>> VariantShreddingWritePlan::Create(
    const std::shared_ptr<arrow::Schema>& logical_schema,
    const std::map<std::string, std::shared_ptr<arrow::DataType>>& column_shredding_types) {
    std::map<std::vector<int32_t>, std::shared_ptr<arrow::DataType>> path_shredding_types;
    for (int32_t i = 0; i < logical_schema->num_fields(); ++i) {
        auto it = column_shredding_types.find(logical_schema->field(i)->name());
        if (it != column_shredding_types.end()) {
            path_shredding_types.emplace(std::vector<int32_t>{i}, it->second);
        }
    }
    return CreateFromPaths(logical_schema, path_shredding_types);
}

Result<std::shared_ptr<VariantShreddingWritePlan>> VariantShreddingWritePlan::CreateFromPaths(
    const std::shared_ptr<arrow::Schema>& logical_schema,
    const std::map<std::vector<int32_t>, std::shared_ptr<arrow::DataType>>& path_shredding_types) {
    std::vector<PlannedColumn> columns;
    auto root_field = arrow::field("root", arrow::struct_(logical_schema->fields()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Field> new_root,
        ReplacePlannedFields(root_field, path_shredding_types, /*depth=*/0, &columns));
    auto physical_schema = arrow::schema(new_root->type()->fields(), logical_schema->metadata());
    return std::shared_ptr<VariantShreddingWritePlan>(new VariantShreddingWritePlan(
        logical_schema, std::move(physical_schema), std::move(columns)));
}

Result<std::shared_ptr<VariantShreddingWritePlan>>
VariantShreddingWritePlan::CreateFromPhysicalSchema(
    const std::shared_ptr<arrow::Schema>& logical_schema,
    const std::shared_ptr<arrow::Schema>& physical_schema) {
    if (logical_schema->num_fields() != physical_schema->num_fields()) {
        return Status::Invalid(
            "variant shredding logical and physical schemas have different field counts");
    }
    std::vector<PlannedColumn> columns;
    std::vector<int32_t> path;
    for (int32_t i = 0; i < logical_schema->num_fields(); ++i) {
        path.push_back(i);
        PAIMON_RETURN_NOT_OK(CollectPlannedColumns(logical_schema->field(i),
                                                   physical_schema->field(i), &path, &columns));
        path.pop_back();
    }
    return std::shared_ptr<VariantShreddingWritePlan>(
        new VariantShreddingWritePlan(logical_schema, physical_schema, std::move(columns)));
}

Result<std::shared_ptr<VariantShreddingWritePlan>> VariantShreddingWritePlan::FromConfiguredSchema(
    const std::shared_ptr<arrow::Schema>& logical_schema,
    const std::string& configured_schema_json) {
    rapidjson::Document doc;
    doc.Parse(configured_schema_json.c_str());
    if (doc.HasParseError()) {
        return Status::Invalid(fmt::format("failed to parse variant shredding schema json: {}",
                                           configured_schema_json));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> configured_field,
                           DataTypeJsonParser::ParseType("shredding_schema", doc));
    if (configured_field->type()->id() != arrow::Type::STRUCT) {
        return Status::Invalid("variant shredding schema must be a ROW type");
    }
    std::map<std::string, std::shared_ptr<arrow::DataType>> column_shredding_types;
    for (const auto& column_field : configured_field->type()->fields()) {
        column_shredding_types.emplace(column_field->name(), column_field->type());
    }
    return Create(logical_schema, column_shredding_types);
}

}  // namespace paimon
