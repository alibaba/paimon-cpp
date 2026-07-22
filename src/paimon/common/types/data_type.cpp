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

#include "paimon/common/types/data_type.h"

#include <cstdint>
#include <stdexcept>

#include "arrow/api.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/types/array_type.h"
#include "paimon/common/types/map_type.h"
#include "paimon/common/types/row_type.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/status.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon {

DataType::DataType(const std::shared_ptr<arrow::DataType>& type, bool nullable,
                   const std::shared_ptr<const arrow::KeyValueMetadata>& metadata)
    : type_(type), nullable_(nullable), metadata_(metadata) {}

std::unique_ptr<DataType> DataType::Create(
    const std::shared_ptr<arrow::DataType>& type, bool nullable,
    const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) {
    switch (type->id()) {
        case arrow::Type::type::MAP:
            return std::make_unique<MapType>(type, nullable, metadata);
        case arrow::Type::type::LIST:
            return std::make_unique<ArrayType>(type, nullable, metadata);
        case arrow::Type::type::STRUCT:
            if (VariantTypeUtils::IsVariantMetadata(metadata)) {
                // A variant field is physically a struct<value, metadata> but is a scalar
                // VARIANT type in the paimon type system, not a ROW type.
                return std::unique_ptr<DataType>(new DataType(type, nullable, metadata));
            }
            return std::make_unique<RowType>(type, nullable, metadata);
        default:
            return std::unique_ptr<DataType>(new DataType(type, nullable, metadata));
    }
}

std::string DataType::WithNullable(const std::string& type) const {
    if (!nullable_) {
        return type + " NOT NULL";
    }
    return type;
}

rapidjson::Value DataType::ToJson(rapidjson::Document::AllocatorType* allocator) const {
    return RapidJsonUtil::SerializeValue(WithNullable(DataTypeToString(type_)), allocator);
}

void DataType::FromJson(const rapidjson::Value& obj) noexcept(false) {
    throw std::logic_error("NotImplemented: DataType::FromJson");
}

std::string DataType::TimestampToString(const std::shared_ptr<arrow::TimestampType>& type) const {
    auto precision = DateTimeUtils::GetPrecisionFromType(type);
    if (type->timezone().empty()) {
        return fmt::format("TIMESTAMP({})", precision);
    }
    return fmt::format("TIMESTAMP({}) WITH LOCAL TIME ZONE", precision);
}

std::string DataType::DataTypeToString(const std::shared_ptr<arrow::DataType>& type) const {
    switch (type->id()) {
        case arrow::Type::type::BOOL:
            return "BOOLEAN";
        case arrow::Type::type::INT8:
            return "TINYINT";
        case arrow::Type::type::INT16:
            return "SMALLINT";
        case arrow::Type::type::INT32:
            return "INT";
        case arrow::Type::type::INT64:
            return "BIGINT";
        case arrow::Type::type::FLOAT:
            return "FLOAT";
        case arrow::Type::type::DOUBLE:
            return "DOUBLE";
        case arrow::Type::type::STRING:
            return "STRING";
        case arrow::Type::type::BINARY:
            return "BYTES";
        case arrow::Type::type::DATE32:
            return "DATE";
        case arrow::Type::type::DECIMAL128: {
            auto status = DecimalUtils::CheckDecimalType(*type);
            if (!status.ok()) {
                throw std::invalid_argument(status.ToString());
            }
            const uint64_t precision = static_cast<uint64_t>(
                arrow::internal::checked_pointer_cast<arrow::Decimal128Type>(type)->precision());
            const uint64_t scale = static_cast<uint64_t>(
                arrow::internal::checked_pointer_cast<arrow::Decimal128Type>(type)->scale());
            return fmt::format("DECIMAL({}, {})", precision, scale);
        }
        case arrow::Type::type::TIMESTAMP: {
            const auto& timestamp_type =
                arrow::internal::checked_pointer_cast<arrow::TimestampType>(type);
            return TimestampToString(timestamp_type);
        }
        case arrow::Type::type::STRUCT: {
            if (VariantTypeUtils::IsVariantMetadata(metadata_)) {
                return "VARIANT";
            }
            break;
        }
        case arrow::Type::type::LARGE_BINARY: {
            // TODO(xinyu): change binary to large binary?
            if (BlobUtils::IsBlobMetadata(metadata_)) {
                return "BLOB";
            }
            break;
        }
        default:
            break;
    }
    throw std::invalid_argument(fmt::format("unknown type {}", type->ToString()));
}

}  // namespace paimon
