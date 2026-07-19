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

#include <cstdint>
#include <string>
#include <utility>

#include "arrow/api.h"
#include "arrow/type_fwd.h"
#include "fmt/format.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/defs.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class FieldTypeUtils {
 public:
    FieldTypeUtils() = delete;
    ~FieldTypeUtils() = delete;

    static bool IsIntegerNumeric(const FieldType& type) {
        if (type == FieldType::TINYINT || type == FieldType::SMALLINT || type == FieldType::INT ||
            type == FieldType::BIGINT) {
            return true;
        }
        return false;
    }
    static bool IntegerScaleLargerThan(const FieldType& type, const FieldType& other_type) {
        return (type == FieldType::SMALLINT && other_type == FieldType::TINYINT) ||
               (type == FieldType::INT && other_type != FieldType::BIGINT) ||
               (type == FieldType::BIGINT);
    }

    /// Converts an arrow field to a `FieldType`, disambiguating metadata-marked extension types
    /// (a VARIANT field is physically a STRUCT with the variant metadata marker).
    static Result<FieldType> ConvertToFieldType(const std::shared_ptr<arrow::Field>& field) {
        if (VariantTypeUtils::IsVariantField(field)) {
            return FieldType::VARIANT;
        }
        return ConvertToFieldType(field->type()->id());
    }

    static Result<FieldType> ConvertToFieldType(const arrow::Type::type& arrow_type) {
        switch (arrow_type) {
            case arrow::Type::type::BOOL:
                return FieldType::BOOLEAN;
            case arrow::Type::type::INT8:
                return FieldType::TINYINT;
            case arrow::Type::type::INT16:
                return FieldType::SMALLINT;
            case arrow::Type::type::INT32:
                return FieldType::INT;
            case arrow::Type::type::INT64:
                return FieldType::BIGINT;
            case arrow::Type::type::FLOAT:
                return FieldType::FLOAT;
            case arrow::Type::type::DOUBLE:
                return FieldType::DOUBLE;
            case arrow::Type::type::STRING:
                return FieldType::STRING;
            case arrow::Type::type::BINARY:
                return FieldType::BINARY;
            case arrow::Type::type::LARGE_BINARY:
                return FieldType::BLOB;  // TODO(xinyu): binary to large binary?
            case arrow::Type::type::TIMESTAMP:
                return FieldType::TIMESTAMP;
            case arrow::Type::type::DECIMAL128:
                return FieldType::DECIMAL;
            case arrow::Type::type::DATE32:
                return FieldType::DATE;
            case arrow::Type::type::LIST:
                return FieldType::ARRAY;
            case arrow::Type::type::MAP:
                return FieldType::MAP;
            case arrow::Type::type::STRUCT:
                return FieldType::STRUCT;
            default:
                return Status::Invalid(
                    fmt::format("Not support arrow type {}", static_cast<int32_t>(arrow_type)));
        }
    }

    static std::string FieldTypeToString(const FieldType& type) {
        switch (type) {
            case FieldType::BOOLEAN:
                return "BOOLEAN";
            case FieldType::TINYINT:
                return "TINYINT";
            case FieldType::SMALLINT:
                return "SMALLINT";
            case FieldType::INT:
                return "INT";
            case FieldType::BIGINT:
                return "BIGINT";
            case FieldType::FLOAT:
                return "FLOAT";
            case FieldType::DOUBLE:
                return "DOUBLE";
            case FieldType::STRING:
                return "STRING";
            case FieldType::BINARY:
                return "BINARY";
            case FieldType::BLOB:
                return "BLOB";
            case FieldType::TIMESTAMP:
                return "TIMESTAMP";
            case FieldType::DECIMAL:
                return "DECIMAL";
            case FieldType::DATE:
                return "DATE";
            case FieldType::ARRAY:
                return "ARRAY";
            case FieldType::MAP:
                return "MAP";
            case FieldType::STRUCT:
                return "STRUCT";
            case FieldType::VARIANT:
                return "VARIANT";
            default:
                return "UNKNOWN, type id:" + std::to_string(static_cast<int32_t>(type));
        }
    }
};
}  // namespace paimon
