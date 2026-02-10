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

#include <vector>

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/result.h"

namespace paimon {

class ArrowUtils {
 public:
    ArrowUtils() = delete;
    ~ArrowUtils() = delete;

    static Result<std::shared_ptr<arrow::Schema>> DataTypeToSchema(
        const std::shared_ptr<arrow::DataType>& data_type) {
        if (data_type->id() != arrow::Type::STRUCT) {
            return Status::Invalid(fmt::format("Expected struct data type, actual data type: {}",
                                               data_type->ToString()));
        }
        const auto& struct_type = std::static_pointer_cast<arrow::StructType>(data_type);
        return std::make_shared<arrow::Schema>(struct_type->fields());
    }

    static Result<std::vector<int32_t>> CreateProjection(
        const std::shared_ptr<arrow::Schema>& file_schema, const arrow::FieldVector& read_fields) {
        std::vector<int32_t> target_to_src_mapping;
        target_to_src_mapping.reserve(read_fields.size());
        for (const auto& field : read_fields) {
            auto src_field_idx = file_schema->GetFieldIndex(field->name());
            if (src_field_idx < 0) {
                return Status::Invalid(
                    fmt::format("Field '{}' not found or duplicate in file schema", field->name()));
            }
            target_to_src_mapping.push_back(src_field_idx);
        }
        return target_to_src_mapping;
    }

    static Status CheckNullabilityMatch(const std::shared_ptr<arrow::Schema>& schema,
                                        const std::shared_ptr<arrow::Array>& data) {
        auto struct_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(data);
        if (struct_array->num_fields() != schema->num_fields()) {
            return Status::Invalid(fmt::format(
                "CheckNullabilityMatch failed, data field count {} mismatch schema field count {}",
                struct_array->num_fields(), schema->num_fields()));
        }
        for (int32_t i = 0; i < schema->num_fields(); i++) {
            PAIMON_RETURN_NOT_OK(
                InnerCheckNullabilityMatch(schema->field(i), struct_array->field(i)));
        }
        return Status::OK();
    }

 private:
    static Status InnerCheckNullabilityMatch(const std::shared_ptr<arrow::Field>& field,
                                             const std::shared_ptr<arrow::Array>& data) {
        if (PAIMON_UNLIKELY(!field->nullable() && data->null_count() != 0)) {
            return Status::Invalid(fmt::format(
                "CheckNullabilityMatch failed, field {} not nullable while data have null value",
                field->name()));
        }
        auto type = field->type();
        if (type->id() == arrow::Type::STRUCT) {
            auto struct_type =
                arrow::internal::checked_pointer_cast<arrow::StructType>(field->type());
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
            PAIMON_RETURN_NOT_OK(
                InnerCheckNullabilityMatch(map_type->key_field(), map_array->keys()));
            PAIMON_RETURN_NOT_OK(
                InnerCheckNullabilityMatch(map_type->item_field(), map_array->items()));
        }
        return Status::OK();
    }
};

}  // namespace paimon
