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

#include "paimon/common/utils/arrow/arrow_utils.h"

#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/util/compression.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/string_utils.h"

namespace paimon {
Result<std::shared_ptr<arrow::Schema>> ArrowUtils::DataTypeToSchema(
    const std::shared_ptr<arrow::DataType>& data_type) {
    if (data_type->id() != arrow::Type::STRUCT) {
        return Status::Invalid(
            fmt::format("Expected struct data type, actual data type: {}", data_type->ToString()));
    }
    const auto& struct_type = std::static_pointer_cast<arrow::StructType>(data_type);
    return std::make_shared<arrow::Schema>(struct_type->fields());
}

Result<std::vector<int32_t>> ArrowUtils::CreateProjection(
    const std::shared_ptr<arrow::Schema>& src_schema, const arrow::FieldVector& target_fields) {
    std::vector<int32_t> target_to_src_mapping;
    target_to_src_mapping.reserve(target_fields.size());
    for (const auto& field : target_fields) {
        auto src_field_idx = src_schema->GetFieldIndex(field->name());
        if (src_field_idx < 0) {
            return Status::Invalid(
                fmt::format("Field '{}' not found or duplicate in src schema", field->name()));
        }
        target_to_src_mapping.push_back(src_field_idx);
    }
    return target_to_src_mapping;
}

Status ArrowUtils::CheckNullabilityMatch(const std::shared_ptr<arrow::Schema>& schema,
                                         const std::shared_ptr<arrow::Array>& data) {
    auto struct_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(data);
    if (struct_array->num_fields() != schema->num_fields()) {
        return Status::Invalid(fmt::format(
            "CheckNullabilityMatch failed, data field count {} mismatch schema field count {}",
            struct_array->num_fields(), schema->num_fields()));
    }
    for (int32_t i = 0; i < schema->num_fields(); i++) {
        PAIMON_RETURN_NOT_OK(InnerCheckNullabilityMatch(schema->field(i), struct_array->field(i)));
    }
    return Status::OK();
}

void ArrowUtils::TraverseArray(const std::shared_ptr<arrow::Array>& array) {
    arrow::Type::type type = array->type()->id();
    switch (type) {
        case arrow::Type::type::DICTIONARY: {
            auto* dict_array = arrow::internal::checked_cast<arrow::DictionaryArray*>(array.get());
            [[maybe_unused]] auto dict = dict_array->dictionary();
            return;
        }
        case arrow::Type::type::STRUCT: {
            auto* struct_array = arrow::internal::checked_cast<arrow::StructArray*>(array.get());
            for (const auto& field : struct_array->fields()) {
                TraverseArray(field);
            }
            return;
        }
        case arrow::Type::type::MAP: {
            auto* map_array = arrow::internal::checked_cast<arrow::MapArray*>(array.get());
            TraverseArray(map_array->keys());
            TraverseArray(map_array->items());
            return;
        }
        case arrow::Type::type::LIST: {
            auto* list_array = arrow::internal::checked_cast<arrow::ListArray*>(array.get());
            TraverseArray(list_array->values());
            return;
        }
        default:
            return;
    }
}

bool ArrowUtils::EqualsIgnoreNullable(const std::shared_ptr<arrow::DataType>& type,
                                      const std::shared_ptr<arrow::DataType>& other_type) {
    if (type->id() != other_type->id() || type->num_fields() != other_type->num_fields()) {
        return false;
    }
    for (int32_t i = 0; i < type->num_fields(); ++i) {
        const auto& field = type->field(i);
        const auto& other_field = other_type->field(i);
        if (field->name() != other_field->name()) {
            return false;
        }
        if (!EqualsIgnoreNullable(field->type(), other_field->type())) {
            return false;
        }
    }
    return true;
}

Status ArrowUtils::InnerCheckNullabilityMatch(const std::shared_ptr<arrow::Field>& field,
                                              const std::shared_ptr<arrow::Array>& data) {
    if (PAIMON_UNLIKELY(!field->nullable() && data->null_count() != 0)) {
        return Status::Invalid(fmt::format(
            "CheckNullabilityMatch failed, field {} not nullable while data have null value",
            field->name()));
    }
    auto type = field->type();
    if (type->id() == arrow::Type::STRUCT) {
        auto struct_type = arrow::internal::checked_pointer_cast<arrow::StructType>(field->type());
        auto struct_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(data);
        for (int32_t i = 0; i < struct_type->num_fields(); ++i) {
            PAIMON_RETURN_NOT_OK(
                InnerCheckNullabilityMatch(struct_type->field(i), struct_array->field(i)));
        }
    } else if (type->id() == arrow::Type::LIST) {
        auto list_type = arrow::internal::checked_pointer_cast<arrow::ListType>(field->type());
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(data);
        PAIMON_RETURN_NOT_OK(
            InnerCheckNullabilityMatch(list_type->value_field(), list_array->values()));
    } else if (type->id() == arrow::Type::MAP) {
        auto map_type = arrow::internal::checked_pointer_cast<arrow::MapType>(field->type());
        auto map_array = arrow::internal::checked_pointer_cast<arrow::MapArray>(data);
        PAIMON_RETURN_NOT_OK(InnerCheckNullabilityMatch(map_type->key_field(), map_array->keys()));
        PAIMON_RETURN_NOT_OK(
            InnerCheckNullabilityMatch(map_type->item_field(), map_array->items()));
    }
    return Status::OK();
}

Result<std::shared_ptr<arrow::StructArray>> ArrowUtils::RemoveFieldFromStructArray(
    const std::shared_ptr<arrow::StructArray>& struct_array, const std::string& field_name) {
    auto struct_type = std::static_pointer_cast<arrow::StructType>(struct_array->type());
    int32_t field_idx = struct_type->GetFieldIndex(field_name);
    if (field_idx == -1) {
        return struct_array;
    }
    std::vector<std::shared_ptr<arrow::Array>> new_arrays;
    std::vector<std::shared_ptr<arrow::Field>> new_fields;
    for (int32_t i = 0; i < struct_type->num_fields(); ++i) {
        if (i != field_idx) {
            new_arrays.emplace_back(struct_array->field(i));
            new_fields.emplace_back(struct_type->field(i));
        }
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> array,
        arrow::StructArray::Make(new_arrays, new_fields, struct_array->null_bitmap(),
                                 struct_array->null_count(), struct_array->offset()));
    return array;
}

Result<arrow::Compression::type> ArrowUtils::GetCompressionType(const std::string& compression) {
    std::string normalized = StringUtils::ToLowerCase(compression);
    if (normalized.empty() || normalized == "none") {
        normalized = "uncompressed";
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(arrow::Compression::type compression_type,
                                      arrow::util::Codec::GetCompressionType(normalized));
    return compression_type;
}

}  // namespace paimon
