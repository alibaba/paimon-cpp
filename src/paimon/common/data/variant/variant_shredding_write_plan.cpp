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
#include <vector>

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/common/data/variant/variant_shredding_utils.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/types/data_type_json_parser.h"
#include "rapidjson/document.h"

namespace paimon {

Result<std::shared_ptr<VariantShreddingWritePlan>> VariantShreddingWritePlan::Create(
    const std::shared_ptr<arrow::Schema>& logical_schema,
    const std::map<std::string, std::shared_ptr<arrow::DataType>>& column_shredding_types) {
    auto plan = std::shared_ptr<VariantShreddingWritePlan>(new VariantShreddingWritePlan());
    plan->logical_schema_ = logical_schema;
    arrow::FieldVector physical_fields;
    physical_fields.reserve(logical_schema->num_fields());
    for (const auto& field : logical_schema->fields()) {
        auto it = column_shredding_types.find(field->name());
        if (it == column_shredding_types.end() || !VariantTypeUtils::IsVariantField(field)) {
            physical_fields.push_back(field);
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> physical_type,
                               VariantShreddingUtils::VariantShreddingSchema(it->second));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<VariantSchema> variant_schema,
                               VariantShreddingUtils::BuildVariantSchema(physical_type));
        plan->column_schemas_.emplace(field->name(), std::move(variant_schema));
        plan->column_physical_types_.emplace(field->name(), physical_type);
        physical_fields.push_back(field->WithType(physical_type));
    }
    if (plan->column_schemas_.empty()) {
        return Status::Invalid(
            "variant shredding write plan matches no variant column in the write schema");
    }
    plan->physical_schema_ = arrow::schema(physical_fields, logical_schema->metadata());
    return plan;
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
