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
        const std::shared_ptr<::arrow::DataType>& data_type) {
        if (data_type->id() != arrow::Type::STRUCT) {
            return Status::Invalid(fmt::format("Expected struct data type, actual data type: {}",
                                               data_type->ToString()));
        }
        const auto& struct_type = std::static_pointer_cast<arrow::StructType>(data_type);
        return std::make_shared<arrow::Schema>(struct_type->fields());
    }

    static Result<std::vector<int32_t>> CreateProjection(
        const std::shared_ptr<::arrow::Schema>& file_schema,
        const arrow::FieldVector& read_fields) {
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
};

}  // namespace paimon
