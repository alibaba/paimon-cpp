/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Adapted from Apache Iceberg C++
// https://github.com/apache/iceberg-cpp/blob/main/src/iceberg/avro/avro_direct_encoder.cc

#include "paimon/format/avro/avro_direct_encoder.h"

#include <algorithm>
#include <cstring>

#include "arrow/api.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/format/avro/avro_utils.h"
#include "paimon/result.h"

namespace paimon::avro {

namespace {

// Utility struct for union branch information
struct UnionBranches {
    size_t null_index;
    size_t value_index;
    ::avro::NodePtr value_node;
};

Result<UnionBranches> ValidateUnion(const ::avro::NodePtr& union_node) {
    if (PAIMON_UNLIKELY(union_node->leaves() != 2)) {
        return Status::Invalid(
            fmt::format("Union must have exactly 2 branches, got {}", union_node->leaves()));
    }

    const auto& branch_0 = union_node->leafAt(0);
    const auto& branch_1 = union_node->leafAt(1);

    if (branch_0->type() == ::avro::AVRO_NULL && branch_1->type() != ::avro::AVRO_NULL) {
        return UnionBranches{.null_index = 0, .value_index = 1, .value_node = branch_1};
    }
    if (branch_1->type() == ::avro::AVRO_NULL && branch_0->type() != ::avro::AVRO_NULL) {
        return UnionBranches{.null_index = 1, .value_index = 0, .value_node = branch_0};
    }
    return Status::Invalid("Union must have exactly one null branch");
}

}  // namespace

Status AvroDirectEncoder::EncodeArrowToAvro(const ::avro::NodePtr& avro_node,
                                            const arrow::Array& array, int64_t row_index,
                                            ::avro::Encoder* encoder, EncodeContext* ctx) {
    if (PAIMON_UNLIKELY(row_index < 0 || row_index >= array.length())) {
        return Status::Invalid(
            fmt::format("Row index {} out of bounds {}", row_index, array.length()));
    }

    const bool is_null = array.IsNull(row_index);

    if (avro_node->type() == ::avro::AVRO_UNION) {
        PAIMON_ASSIGN_OR_RAISE(UnionBranches branches, ValidateUnion(avro_node));

        if (is_null) {
            encoder->encodeUnionIndex(branches.null_index);
            encoder->encodeNull();
            return Status::OK();
        }

        encoder->encodeUnionIndex(branches.value_index);
        return EncodeArrowToAvro(branches.value_node, array, row_index, encoder, ctx);
    }

    if (is_null) {
        return Status::Invalid("Null value in non-nullable field");
    }

    switch (avro_node->type()) {
        case ::avro::AVRO_NULL:
            encoder->encodeNull();
            return Status::OK();

        case ::avro::AVRO_BOOL: {
            const auto& bool_array =
                arrow::internal::checked_cast<const arrow::BooleanArray&>(array);
            encoder->encodeBool(bool_array.Value(row_index));
            return Status::OK();
        }

        case ::avro::AVRO_INT: {
            // AVRO_INT can represent: int32, date (days since epoch)
            switch (array.type()->id()) {
                case arrow::Type::INT8: {
                    const auto& int8_array =
                        arrow::internal::checked_cast<const arrow::Int8Array&>(array);
                    encoder->encodeInt(int8_array.Value(row_index));
                    return Status::OK();
                }
                case arrow::Type::INT16: {
                    const auto& int16_array =
                        arrow::internal::checked_cast<const arrow::Int16Array&>(array);
                    encoder->encodeInt(int16_array.Value(row_index));
                    return Status::OK();
                }

                case arrow::Type::INT32: {
                    const auto& int32_array =
                        arrow::internal::checked_cast<const arrow::Int32Array&>(array);
                    encoder->encodeInt(int32_array.Value(row_index));
                    return Status::OK();
                }
                case arrow::Type::DATE32: {
                    const auto& date_array =
                        arrow::internal::checked_cast<const arrow::Date32Array&>(array);
                    encoder->encodeInt(date_array.Value(row_index));
                    return Status::OK();
                }
                default:
                    return Status::Invalid(
                        fmt::format("AVRO_INT expects Int32Array or Date32Array, got {}",
                                    array.type()->ToString()));
            }
        }

        case ::avro::AVRO_LONG: {
            // AVRO_LONG can represent: int64, timestamp
            switch (array.type()->id()) {
                case arrow::Type::INT64: {
                    const auto& int64_array =
                        arrow::internal::checked_cast<const arrow::Int64Array&>(array);
                    encoder->encodeLong(int64_array.Value(row_index));
                    return Status::OK();
                }
                case arrow::Type::TIMESTAMP: {
                    const auto& timestamp_array =
                        arrow::internal::checked_cast<const arrow::TimestampArray&>(array);
                    int64_t timestamp = timestamp_array.Value(row_index);

                    auto ts_type =
                        arrow::internal::checked_pointer_cast<arrow::TimestampType>(array.type());
                    arrow::TimeUnit::type unit = ts_type->unit();
                    const auto& logical_type = avro_node->logicalType().type();

                    // NOTE: Java Avro only support TIMESTAMP_MILLIS && TIMESTAMP_MICROS
                    if (((logical_type == ::avro::LogicalType::TIMESTAMP_MILLIS ||
                          logical_type == ::avro::LogicalType::LOCAL_TIMESTAMP_MILLIS) &&
                         unit == arrow::TimeUnit::MILLI) ||
                        ((logical_type == ::avro::LogicalType::TIMESTAMP_MICROS ||
                          logical_type == ::avro::LogicalType::LOCAL_TIMESTAMP_MICROS) &&
                         unit == arrow::TimeUnit::MICRO) ||
                        ((logical_type == ::avro::LogicalType::TIMESTAMP_NANOS ||
                          logical_type == ::avro::LogicalType::LOCAL_TIMESTAMP_NANOS) &&
                         unit == arrow::TimeUnit::NANO)) {
                        encoder->encodeLong(timestamp);
                    } else if ((logical_type == ::avro::LogicalType::TIMESTAMP_MILLIS ||
                                logical_type == ::avro::LogicalType::LOCAL_TIMESTAMP_MILLIS) &&
                               unit == arrow::TimeUnit::SECOND) {
                        // for arrow second, we need to convert it to avro millisecond
                        encoder->encodeLong(
                            timestamp *
                            DateTimeUtils::CONVERSION_FACTORS[DateTimeUtils::MILLISECOND]);
                    } else {
                        return Status::Invalid(
                            fmt::format("Unsupported timestamp type with avro logical type {} and "
                                        "arrow time unit {}.",
                                        AvroUtils::ToString(avro_node->logicalType()),
                                        DateTimeUtils::GetArrowTimeUnitStr(unit)));
                    }
                    return Status::OK();
                }
                default:
                    return Status::Invalid(
                        fmt::format("AVRO_LONG expects Int64Array, or TimestampArray, got {}",
                                    array.type()->ToString()));
            }
        }

        case ::avro::AVRO_FLOAT: {
            const auto& float_array =
                arrow::internal::checked_cast<const arrow::FloatArray&>(array);
            encoder->encodeFloat(float_array.Value(row_index));
            return Status::OK();
        }

        case ::avro::AVRO_DOUBLE: {
            const auto& double_array =
                arrow::internal::checked_cast<const arrow::DoubleArray&>(array);
            encoder->encodeDouble(double_array.Value(row_index));
            return Status::OK();
        }

        case ::avro::AVRO_STRING: {
            const auto& string_array =
                arrow::internal::checked_cast<const arrow::StringArray&>(array);
            std::string_view value = string_array.GetView(row_index);
            encoder->encodeString(std::string(value));
            return Status::OK();
        }

        case ::avro::AVRO_BYTES: {
            // Handle DECIMAL
            if (avro_node->logicalType().type() == ::avro::LogicalType::DECIMAL) {
                const auto& decimal_array =
                    arrow::internal::checked_cast<const arrow::Decimal128Array&>(array);
                std::string_view decimal_value = decimal_array.GetView(row_index);
                ctx->assign(decimal_value.begin(), decimal_value.end());
                // Arrow Decimal128 bytes are in little-endian order, Avro requires big-endian
                std::reverse(ctx->begin(), ctx->end());
                encoder->encodeBytes(ctx->data(), ctx->size());
                return Status::OK();
            }

            // Handle regular BYTES
            const auto& binary_array =
                arrow::internal::checked_cast<const arrow::BinaryArray&>(array);
            std::string_view value = binary_array.GetView(row_index);
            // TODO(jinli.zjw): need to copy to ctx?
            ctx->assign(value.begin(), value.end());
            encoder->encodeBytes(ctx->data(), ctx->size());
            return Status::OK();
        }

        case ::avro::AVRO_RECORD: {
            if (PAIMON_UNLIKELY(array.type()->id() != arrow::Type::STRUCT)) {
                return Status::Invalid(fmt::format("AVRO_RECORD expects StructArray, got {}",
                                                   array.type()->ToString()));
            }

            const auto& struct_array =
                arrow::internal::checked_cast<const arrow::StructArray&>(array);
            const size_t num_fields = avro_node->leaves();

            if (PAIMON_UNLIKELY(struct_array.num_fields() != static_cast<int>(num_fields))) {
                return Status::Invalid(fmt::format(
                    "Field count mismatch: Arrow struct has {} fields, Avro node has {} fields",
                    struct_array.num_fields(), num_fields));
            }

            for (size_t i = 0; i < num_fields; ++i) {
                const auto& field_node = avro_node->leafAt(i);
                const auto& field_array = struct_array.field(static_cast<int>(i));

                PAIMON_RETURN_NOT_OK(
                    EncodeArrowToAvro(field_node, *field_array, row_index, encoder, ctx));
            }
            return Status::OK();
        }

        case ::avro::AVRO_ARRAY: {
            const auto& element_node = avro_node->leafAt(0);

            // Handle ListArray
            if (array.type()->id() == arrow::Type::LIST) {
                const auto& list_array =
                    arrow::internal::checked_cast<const arrow::ListArray&>(array);

                const auto start = list_array.value_offset(row_index);
                const auto end = list_array.value_offset(row_index + 1);
                const auto length = end - start;

                encoder->arrayStart();
                if (length > 0) {
                    encoder->setItemCount(length);
                    const auto& values = list_array.values();

                    for (int64_t i = start; i < end; ++i) {
                        encoder->startItem();
                        PAIMON_RETURN_NOT_OK(
                            EncodeArrowToAvro(element_node, *values, i, encoder, ctx));
                    }
                }
                encoder->arrayEnd();
                return Status::OK();
            } else if (array.type()->id() == arrow::Type::MAP &&
                       AvroUtils::HasMapLogicalType(avro_node)) {
                // Handle MapArray (for Avro maps with non-string keys)
                if (PAIMON_UNLIKELY(element_node->type() != ::avro::AVRO_RECORD ||
                                    element_node->leaves() != 2)) {
                    return Status::Invalid(
                        fmt::format("Expected AVRO_RECORD for map key-value pair, got {}",
                                    ::avro::toString(element_node->type())));
                }

                const auto& map_array =
                    arrow::internal::checked_cast<const arrow::MapArray&>(array);

                const auto start = map_array.value_offset(row_index);
                const auto end = map_array.value_offset(row_index + 1);
                const auto length = end - start;

                encoder->arrayStart();
                if (length > 0) {
                    encoder->setItemCount(length);
                    const auto& keys = map_array.keys();
                    const auto& values = map_array.items();

                    // The element_node should be a RECORD with "key" and "value" fields
                    for (int64_t i = start; i < end; ++i) {
                        const auto& key_node = element_node->leafAt(0);
                        const auto& value_node = element_node->leafAt(1);

                        encoder->startItem();
                        PAIMON_RETURN_NOT_OK(EncodeArrowToAvro(key_node, *keys, i, encoder, ctx));
                        PAIMON_RETURN_NOT_OK(
                            EncodeArrowToAvro(value_node, *values, i, encoder, ctx));
                    }
                }
                encoder->arrayEnd();
                return Status::OK();
            }

            return Status::Invalid(fmt::format(
                "AVRO_ARRAY must map to ListArray or MapArray, got {}", array.type()->ToString()));
        }

        case ::avro::AVRO_MAP: {
            if (PAIMON_UNLIKELY(array.type()->id() != arrow::Type::MAP)) {
                return Status::Invalid(
                    fmt::format("AVRO_MAP expects MapArray, got {}", array.type()->ToString()));
            }
            const auto& map_array = arrow::internal::checked_cast<const arrow::MapArray&>(array);

            const auto start = map_array.value_offset(row_index);
            const auto end = map_array.value_offset(row_index + 1);
            const auto length = end - start;

            encoder->mapStart();
            if (length > 0) {
                encoder->setItemCount(length);
                const auto& keys = map_array.keys();
                const auto& values = map_array.items();
                const auto& value_node = avro_node->leafAt(1);

                if (PAIMON_UNLIKELY(keys->type()->id() != arrow::Type::STRING)) {
                    return Status::Invalid(fmt::format("AVRO_MAP keys must be StringArray, got {}",
                                                       keys->type()->ToString()));
                }

                for (int64_t i = start; i < end; ++i) {
                    encoder->startItem();
                    const auto& string_array =
                        arrow::internal::checked_cast<const arrow::StringArray&>(*keys);
                    std::string_view key_value = string_array.GetView(i);
                    encoder->encodeString(std::string(key_value));

                    PAIMON_RETURN_NOT_OK(EncodeArrowToAvro(value_node, *values, i, encoder, ctx));
                }
            }
            encoder->mapEnd();
            return Status::OK();
        }

        case ::avro::AVRO_UNION:
            // Already handled above
            return Status::Invalid("Unexpected union handling");
        default:
            return Status::Invalid(
                fmt::format("Unsupported Avro type: {}", ::avro::toString(avro_node->type())));
    }
}

}  // namespace paimon::avro
