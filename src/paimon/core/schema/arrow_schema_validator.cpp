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

#include "paimon/core/schema/arrow_schema_validator.h"

#include <string>
#include <vector>

#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/data/variant/variant_access_utils.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/result.h"

namespace arrow {
class KeyValueMetadata;
}  // namespace arrow

namespace paimon {

bool ArrowSchemaValidator::IsNestedType(const std::shared_ptr<arrow::DataType>& data_type) {
    return (data_type->id() == arrow::Type::MAP || data_type->id() == arrow::Type::LIST ||
            data_type->id() == arrow::Type::STRUCT);
}

Status ArrowSchemaValidator::ValidateSchema(const arrow::Schema& schema) {
    // validate no duplicate fields
    PAIMON_RETURN_NOT_OK(ValidateNoRedundantFields(schema.fields()));
    // validate no whitespace-only fields
    PAIMON_RETURN_NOT_OK(ValidateNoWhitespaceOnlyFields(schema.fields()));
    // validate data type
    for (const auto& field : schema.fields()) {
        PAIMON_RETURN_NOT_OK(ValidateField(field));
    }
    return Status::OK();
}

Status ArrowSchemaValidator::ValidateSchemaWithFieldId(const arrow::Schema& schema) {
    PAIMON_RETURN_NOT_OK(ValidateSchema(schema));
    std::set<int32_t> field_id_set;
    for (const auto& field : schema.fields()) {
        PAIMON_ASSIGN_OR_RAISE(DataField data_field,
                               DataField::ConvertArrowFieldToDataField(field));
        auto iter = field_id_set.find(data_field.Id());
        if (iter != field_id_set.end()) {
            return Status::Invalid(
                fmt::format("field id must be unique, duplicate field id {}", data_field.Id()));
        }
        field_id_set.insert(data_field.Id());
        PAIMON_RETURN_NOT_OK(ValidateDataTypeWithFieldId(field->type(), field->metadata(),
                                                         /*allow_blob=*/true, &field_id_set));
    }
    return Status::OK();
}

Status ArrowSchemaValidator::ValidateNoRedundantFields(const arrow::FieldVector& fields) {
    std::set<std::string> field_names;
    for (const auto& field : fields) {
        auto iter = field_names.find(field->name());
        if (iter != field_names.end()) {
            return Status::Invalid(fmt::format(
                "validate schema failed: read schema has duplicate field {}", field->name()));
        }
        field_names.insert(field->name());
        if (IsNestedType(field->type())) {
            PAIMON_RETURN_NOT_OK(ValidateNoRedundantFields(field->type()->fields()));
        }
    }
    return Status::OK();
}

Status ArrowSchemaValidator::ValidateNoWhitespaceOnlyFields(const arrow::FieldVector& fields) {
    for (const auto& field : fields) {
        if (StringUtils::IsNullOrWhitespaceOnly(field->name())) {
            return Status::Invalid(
                fmt::format("validate schema failed: read schema has whitespace-only field"));
        }
        if (IsNestedType(field->type())) {
            PAIMON_RETURN_NOT_OK(ValidateNoWhitespaceOnlyFields(field->type()->fields()));
        }
    }
    return Status::OK();
}

Status ArrowSchemaValidator::ValidateDataTypeWithFieldId(
    const std::shared_ptr<arrow::DataType>& type,
    const std::shared_ptr<const arrow::KeyValueMetadata>& key_value_metadata, bool allow_blob,
    std::set<int32_t>* field_id_set) {
    const auto kind = type->id();
    switch (kind) {
        case arrow::Type::type::BOOL:
        case arrow::Type::type::INT8:
        case arrow::Type::type::INT16:
        case arrow::Type::type::INT32:
        case arrow::Type::type::INT64:
        case arrow::Type::type::FLOAT:
        case arrow::Type::type::DOUBLE:
        case arrow::Type::type::STRING:
        case arrow::Type::type::BINARY:
        case arrow::Type::type::DATE32:
        case arrow::Type::type::DECIMAL128:
        case arrow::Type::type::TIMESTAMP:
            return Status::OK();
        case arrow::Type::type::LIST: {
            const auto& value_field =
                arrow::internal::checked_cast<arrow::BaseListType*>(type.get())->value_field();
            PAIMON_RETURN_NOT_OK(ValidateDataTypeWithFieldId(
                value_field->type(), value_field->metadata(), /*allow_blob=*/false, field_id_set));
            break;
        }
        case arrow::Type::type::STRUCT: {
            if (VariantTypeUtils::IsVariantMetadata(key_value_metadata)) {
                // A variant struct is a leaf type: its value/metadata children carry fixed
                // paimon field ids 0/1 which must not join the global field id uniqueness check.
                break;
            }
            arrow::FieldVector sub_fields =
                arrow::internal::checked_cast<arrow::StructType*>(type.get())->fields();
            for (const auto& sub_field : sub_fields) {
                PAIMON_ASSIGN_OR_RAISE(DataField data_field,
                                       DataField::ConvertArrowFieldToDataField(sub_field));
                auto iter = field_id_set->find(data_field.Id());
                if (iter != field_id_set->end()) {
                    return Status::Invalid(fmt::format(
                        "field id must be unique, duplicate field id {}", data_field.Id()));
                }
                field_id_set->insert(data_field.Id());
                PAIMON_RETURN_NOT_OK(ValidateDataTypeWithFieldId(
                    sub_field->type(), sub_field->metadata(), /*allow_blob=*/false, field_id_set));
            }
            break;
        }
        case arrow::Type::type::MAP: {
            const auto& key_field =
                arrow::internal::checked_cast<arrow::MapType*>(type.get())->key_field();
            const auto& item_field =
                arrow::internal::checked_cast<arrow::MapType*>(type.get())->item_field();
            PAIMON_RETURN_NOT_OK(ValidateDataTypeWithFieldId(
                key_field->type(), key_field->metadata(), /*allow_blob=*/false, field_id_set));
            PAIMON_RETURN_NOT_OK(ValidateDataTypeWithFieldId(
                item_field->type(), item_field->metadata(), /*allow_blob=*/false, field_id_set));
            break;
        }
        case arrow::Type::type::LARGE_BINARY: {
            if (BlobUtils::IsBlobMetadata(key_value_metadata)) {
                if (!allow_blob) {
                    return Status::Invalid("Blob field must be a top-level field.");
                }
                break;
            }
            [[fallthrough]];
        }
        default: {
            return Status::Invalid("Unknown or unsupported arrow type: ", type->ToString());
        }
    }
    return Status::OK();
}

Status ArrowSchemaValidator::ValidateField(const std::shared_ptr<arrow::Field>& field) {
    return ValidateField(field, /*allow_blob=*/true);
}

Status ArrowSchemaValidator::ValidateField(const std::shared_ptr<arrow::Field>& field,
                                           bool allow_blob) {
    const auto kind = field->type()->id();
    switch (kind) {
        case arrow::Type::type::BOOL:
        case arrow::Type::type::INT8:
        case arrow::Type::type::INT16:
        case arrow::Type::type::INT32:
        case arrow::Type::type::INT64:
        case arrow::Type::type::FLOAT:
        case arrow::Type::type::DOUBLE:
        case arrow::Type::type::STRING:
        case arrow::Type::type::BINARY:
        case arrow::Type::type::DATE32:
        case arrow::Type::type::TIMESTAMP:
            break;
        case arrow::Type::type::DECIMAL128:
            PAIMON_RETURN_NOT_OK(DecimalUtils::CheckDecimalType(*field->type()));
            break;
        case arrow::Type::type::LIST: {
            const auto& value_field =
                arrow::internal::checked_cast<const arrow::BaseListType&>(*field->type())
                    .value_field();
            PAIMON_RETURN_NOT_OK(ValidateField(value_field, /*allow_blob=*/false));
            break;
        }
        case arrow::Type::type::STRUCT: {
            if (VariantTypeUtils::IsVariantField(field)) {
                if (VariantAccessUtils::IsVariantAccessType(field->type())) {
                    // A variant column read as a variant-access projection keeps the variant
                    // marker but replaces the type with the projection struct; its children are
                    // cast targets validated by the variant read plans.
                    break;
                }
                PAIMON_RETURN_NOT_OK(VariantTypeUtils::ValidateVariantShape(field));
                break;
            }
            arrow::FieldVector arrow_fields =
                arrow::internal::checked_cast<const arrow::StructType&>(*field->type()).fields();
            for (const auto& sub_field : arrow_fields) {
                PAIMON_RETURN_NOT_OK(ValidateField(sub_field, /*allow_blob=*/false));
            }
            break;
        }
        case arrow::Type::type::MAP: {
            const auto& key_field =
                arrow::internal::checked_cast<const arrow::MapType&>(*field->type()).key_field();
            const auto& item_field =
                arrow::internal::checked_cast<const arrow::MapType&>(*field->type()).item_field();
            if (key_field->nullable()) {
                return Status::Invalid(
                    fmt::format("Map field '{}' has a nullable key.", field->name()));
            }
            PAIMON_RETURN_NOT_OK(ValidateField(key_field, /*allow_blob=*/false));
            PAIMON_RETURN_NOT_OK(ValidateField(item_field, /*allow_blob=*/false));
            break;
        }
        case arrow::Type::type::LARGE_BINARY: {
            if (BlobUtils::IsBlobField(field)) {
                if (!allow_blob) {
                    return Status::Invalid("Blob field must be a top-level field.");
                }
                break;
            }
            [[fallthrough]];
        }
        default: {
            return Status::Invalid("Unknown or unsupported arrow type: ",
                                   field->type()->ToString());
        }
    }
    return Status::OK();
}

bool ArrowSchemaValidator::ContainTimestampWithTimezone(const arrow::DataType& type) {
    const auto kind = type.id();
    switch (kind) {
        case arrow::Type::type::LIST: {
            const auto& value_field =
                arrow::internal::checked_cast<const arrow::ListType&>(type).value_field();
            if (ContainTimestampWithTimezone(*value_field->type())) {
                return true;
            }
            break;
        }
        case arrow::Type::type::STRUCT: {
            arrow::FieldVector arrow_fields =
                arrow::internal::checked_cast<const arrow::StructType&>(type).fields();
            for (const auto& sub_field : arrow_fields) {
                if (ContainTimestampWithTimezone(*sub_field->type())) {
                    return true;
                }
            }
            break;
        }
        case arrow::Type::type::MAP: {
            const auto& key_field =
                arrow::internal::checked_cast<const arrow::MapType&>(type).key_field();
            const auto& item_field =
                arrow::internal::checked_cast<const arrow::MapType&>(type).item_field();
            if (ContainTimestampWithTimezone(*key_field->type())) {
                return true;
            }
            if (ContainTimestampWithTimezone(*item_field->type())) {
                return true;
            }
            break;
        }
        case arrow::Type::type::TIMESTAMP: {
            const auto& ts_type = arrow::internal::checked_cast<const arrow::TimestampType&>(type);
            if (!ts_type.timezone().empty()) {
                return true;
            }
            return false;
        }
        default: {
            return false;
        }
    }
    return false;
}

}  // namespace paimon
