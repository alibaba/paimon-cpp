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

#include "paimon/common/data/variant/variant_type_utils.h"

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/common/data/variant/variant_defs.h"
#include "paimon/common/types/data_field.h"

namespace paimon {

bool VariantTypeUtils::IsVariantField(const std::shared_ptr<arrow::Field>& field) {
    if (field->type()->id() != arrow::Type::STRUCT) {
        return false;
    }
    if (!field->HasMetadata()) {
        return false;
    }
    return IsVariantMetadata(field->metadata());
}

bool VariantTypeUtils::IsUnshreddedVariantType(const std::shared_ptr<arrow::DataType>& type) {
    if (type == nullptr || type->id() != arrow::Type::STRUCT || type->num_fields() != 2) {
        return false;
    }
    const auto& value_field = type->field(0);
    const auto& metadata_field = type->field(1);
    return value_field->name() == VariantDefs::kValueFieldName &&
           value_field->type()->id() == arrow::Type::BINARY && !value_field->nullable() &&
           metadata_field->name() == VariantDefs::kMetadataFieldName &&
           metadata_field->type()->id() == arrow::Type::BINARY && !metadata_field->nullable();
}

bool VariantTypeUtils::IsVariantMetadata(
    const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) {
    if (!metadata) {
        return false;
    }
    auto extension_name = metadata->Get(VariantDefs::kExtensionTypeKey);
    return extension_name.ok() && *extension_name == VariantDefs::kExtensionTypeValue;
}

std::shared_ptr<arrow::DataType> VariantTypeUtils::UnshreddedStructType() {
    auto value_field =
        arrow::field(VariantDefs::kValueFieldName, arrow::binary(), /*nullable=*/false,
                     arrow::KeyValueMetadata::Make({DataField::FIELD_ID},
                                                   {std::to_string(VariantDefs::kValueFieldId)}));
    auto metadata_field =
        arrow::field(VariantDefs::kMetadataFieldName, arrow::binary(), /*nullable=*/false,
                     arrow::KeyValueMetadata::Make(
                         {DataField::FIELD_ID}, {std::to_string(VariantDefs::kMetadataFieldId)}));
    return arrow::struct_({value_field, metadata_field});
}

std::shared_ptr<arrow::Field> VariantTypeUtils::ToArrowField(
    const std::string& field_name, bool nullable,
    std::unordered_map<std::string, std::string> metadata) {
    metadata[VariantDefs::kExtensionTypeKey] = VariantDefs::kExtensionTypeValue;
    return arrow::field(field_name, UnshreddedStructType(), nullable,
                        std::make_shared<arrow::KeyValueMetadata>(metadata));
}

Status VariantTypeUtils::ValidateVariantShape(const std::shared_ptr<arrow::Field>& field) {
    const auto& type = field->type();
    if (type->id() != arrow::Type::STRUCT) {
        return Status::Invalid(fmt::format("Variant field '{}' must be a struct, but got {}",
                                           field->name(), type->ToString()));
    }
    const auto& struct_type = std::static_pointer_cast<arrow::StructType>(type);
    if (struct_type->num_fields() != 2) {
        return Status::Invalid(
            fmt::format("Variant field '{}' must be a struct<value: binary, metadata: binary>, "
                        "but got {}",
                        field->name(), type->ToString()));
    }
    const auto& value_field = struct_type->field(0);
    const auto& metadata_field = struct_type->field(1);
    if (value_field->name() != VariantDefs::kValueFieldName ||
        value_field->type()->id() != arrow::Type::BINARY || value_field->nullable() ||
        metadata_field->name() != VariantDefs::kMetadataFieldName ||
        metadata_field->type()->id() != arrow::Type::BINARY || metadata_field->nullable()) {
        return Status::Invalid(
            fmt::format("Variant field '{}' must be a struct<value: binary not null, metadata: "
                        "binary not null>, but got {}",
                        field->name(), type->ToString()));
    }
    return Status::OK();
}

bool VariantTypeUtils::ContainsVariantField(const std::shared_ptr<arrow::Field>& field) {
    if (IsVariantField(field)) {
        return true;
    }
    for (const auto& child : field->type()->fields()) {
        if (ContainsVariantField(child)) {
            return true;
        }
    }
    return false;
}

bool VariantTypeUtils::ContainsVariantField(const std::shared_ptr<arrow::Schema>& schema) {
    for (const auto& field : schema->fields()) {
        if (ContainsVariantField(field)) {
            return true;
        }
    }
    return false;
}

}  // namespace paimon
