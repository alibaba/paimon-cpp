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
// https://github.com/apache/iceberg-cpp/blob/main/src/iceberg/avro/avro_direct_decoder.cc

#include "paimon/format/avro/avro_direct_decoder.h"

#include "arrow/api.h"
#include "arrow/util/checked_cast.h"
#include "avro/Decoder.hh"
#include "avro/Node.hh"
#include "avro/Types.hh"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/format/avro/avro_utils.h"

namespace paimon::avro {

namespace {

/// Forward declaration for mutual recursion.
Status DecodeFieldToBuilder(const ::avro::NodePtr& avro_node,
                            const std::optional<std::set<size_t>>& projection,
                            ::avro::Decoder* decoder, arrow::ArrayBuilder* array_builder,
                            AvroDirectDecoder::DecodeContext* ctx);

/// \brief Skip an Avro value based on its schema without decoding
Status SkipAvroValue(const ::avro::NodePtr& avro_node, ::avro::Decoder* decoder) {
    switch (avro_node->type()) {
        case ::avro::AVRO_NULL:
            decoder->decodeNull();
            return Status::OK();

        case ::avro::AVRO_BOOL:
            decoder->decodeBool();
            return Status::OK();

        case ::avro::AVRO_INT:
            decoder->decodeInt();
            return Status::OK();

        case ::avro::AVRO_LONG:
            decoder->decodeLong();
            return Status::OK();

        case ::avro::AVRO_FLOAT:
            decoder->decodeFloat();
            return Status::OK();

        case ::avro::AVRO_DOUBLE:
            decoder->decodeDouble();
            return Status::OK();

        case ::avro::AVRO_STRING:
            decoder->skipString();
            return Status::OK();

        case ::avro::AVRO_BYTES:
            decoder->skipBytes();
            return Status::OK();

        case ::avro::AVRO_RECORD: {
            // Skip all fields in order
            for (size_t i = 0; i < avro_node->leaves(); ++i) {
                PAIMON_RETURN_NOT_OK(SkipAvroValue(avro_node->leafAt(i), decoder));
            }
            return Status::OK();
        }

        case ::avro::AVRO_ARRAY: {
            const auto& element_node = avro_node->leafAt(0);
            // skipArray() returns count like arrayStart(), must handle all blocks
            int64_t block_count = decoder->skipArray();
            while (block_count > 0) {
                for (int64_t i = 0; i < block_count; ++i) {
                    PAIMON_RETURN_NOT_OK(SkipAvroValue(element_node, decoder));
                }
                block_count = decoder->arrayNext();
            }
            return Status::OK();
        }

        case ::avro::AVRO_MAP: {
            const auto& value_node = avro_node->leafAt(1);
            // skipMap() returns count like mapStart(), must handle all blocks
            int64_t block_count = decoder->skipMap();
            while (block_count > 0) {
                for (int64_t i = 0; i < block_count; ++i) {
                    decoder->skipString();  // Skip key (always string in Avro maps)
                    PAIMON_RETURN_NOT_OK(SkipAvroValue(value_node, decoder));
                }
                block_count = decoder->mapNext();
            }
            return Status::OK();
        }

        case ::avro::AVRO_UNION: {
            const size_t branch_index = decoder->decodeUnionIndex();
            // Validate branch index
            const size_t num_branches = avro_node->leaves();
            if (branch_index >= num_branches) {
                return Status::Invalid(fmt::format("Union branch index {} out of range [0, {})",
                                                   branch_index, num_branches));
            }
            return SkipAvroValue(avro_node->leafAt(branch_index), decoder);
        }

        default:
            return Status::Invalid(fmt::format("Unsupported Avro type for skipping: {}",
                                               AvroUtils::ToString(avro_node)));
    }
}

/// Decode Avro record directly to Arrow struct builder.
Status DecodeStructToBuilder(const ::avro::NodePtr& avro_node,
                             const std::optional<std::set<size_t>>& projection,
                             ::avro::Decoder* decoder, arrow::ArrayBuilder* array_builder,
                             AvroDirectDecoder::DecodeContext* ctx) {
    if (avro_node->type() != ::avro::AVRO_RECORD) {
        return Status::Invalid(
            fmt::format("Expected Avro record, got type: {}", AvroUtils::ToString(avro_node)));
    }

    auto* struct_builder = arrow::internal::checked_cast<arrow::StructBuilder*>(array_builder);
    PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder->Append());

    size_t skipped_fields = 0;
    // Read all Avro fields in order (must maintain decoder position)
    for (size_t avro_idx = 0; avro_idx < avro_node->leaves(); ++avro_idx) {
        if (projection && projection->find(avro_idx) == projection->end()) {
            skipped_fields++;
            PAIMON_RETURN_NOT_OK(SkipAvroValue(avro_node->leafAt(avro_idx), decoder));
        } else {
            // Decode this field
            const auto& avro_field_node = avro_node->leafAt(avro_idx);
            auto* field_builder = struct_builder->field_builder(avro_idx - skipped_fields);
            PAIMON_RETURN_NOT_OK(DecodeFieldToBuilder(avro_field_node, /*projection=*/std::nullopt,
                                                      decoder, field_builder, ctx));
        }
    }

    return Status::OK();
}

/// Decode Avro array directly to Arrow list builder.
Status DecodeListToBuilder(const ::avro::NodePtr& avro_node, ::avro::Decoder* decoder,
                           arrow::ArrayBuilder* array_builder,
                           AvroDirectDecoder::DecodeContext* ctx) {
    if (avro_node->type() != ::avro::AVRO_ARRAY) {
        return Status::Invalid(
            fmt::format("Expected Avro array, got type: {}", AvroUtils::ToString(avro_node)));
    }

    auto* list_builder = arrow::internal::checked_cast<arrow::ListBuilder*>(array_builder);
    PAIMON_RETURN_NOT_OK_FROM_ARROW(list_builder->Append());

    auto* value_builder = list_builder->value_builder();
    const auto& element_node = avro_node->leafAt(0);

    // Read array block count
    int64_t block_count = decoder->arrayStart();
    while (block_count != 0) {
        for (int64_t i = 0; i < block_count; ++i) {
            PAIMON_RETURN_NOT_OK(DecodeFieldToBuilder(element_node, /*projection=*/std::nullopt,
                                                      decoder, value_builder, ctx));
        }
        block_count = decoder->arrayNext();
    }

    return Status::OK();
}

/// Decode Avro map directly to Arrow map builder.
Status DecodeMapToBuilder(const ::avro::NodePtr& avro_node, ::avro::Decoder* decoder,
                          arrow::ArrayBuilder* array_builder,
                          AvroDirectDecoder::DecodeContext* ctx) {
    auto* map_builder = arrow::internal::checked_cast<arrow::MapBuilder*>(array_builder);

    if (avro_node->type() == ::avro::AVRO_MAP) {
        // Handle regular Avro map: map<string, value>
        const auto& key_node = avro_node->leafAt(0);
        const auto& value_node = avro_node->leafAt(1);

        PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder->Append());
        auto* key_builder = map_builder->key_builder();
        auto* item_builder = map_builder->item_builder();

        // Read map block count
        int64_t block_count = decoder->mapStart();
        while (block_count != 0) {
            for (int64_t i = 0; i < block_count; ++i) {
                PAIMON_RETURN_NOT_OK(DecodeFieldToBuilder(key_node, /*projection=*/std::nullopt,
                                                          decoder, key_builder, ctx));
                PAIMON_RETURN_NOT_OK(DecodeFieldToBuilder(value_node, /*projection=*/std::nullopt,
                                                          decoder, item_builder, ctx));
            }
            block_count = decoder->mapNext();
        }
        return Status::OK();
    } else if (avro_node->type() == ::avro::AVRO_ARRAY && AvroUtils::HasMapLogicalType(avro_node)) {
        // Handle array-based map: list<struct<key, value>>
        PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder->Append());
        auto* key_builder = map_builder->key_builder();
        auto* item_builder = map_builder->item_builder();

        const auto& record_node = avro_node->leafAt(0);
        if (record_node->type() != ::avro::AVRO_RECORD || record_node->leaves() != 2) {
            return Status::Invalid(
                fmt::format("Array-based map must contain records with exactly 2 fields, got: {}",
                            AvroUtils::ToString(record_node)));
        }
        const auto& key_node = record_node->leafAt(0);
        const auto& value_node = record_node->leafAt(1);

        // Read array block count
        int64_t block_count = decoder->arrayStart();
        while (block_count != 0) {
            for (int64_t i = 0; i < block_count; ++i) {
                PAIMON_RETURN_NOT_OK(DecodeFieldToBuilder(key_node, /*projection=*/std::nullopt,
                                                          decoder, key_builder, ctx));
                PAIMON_RETURN_NOT_OK(DecodeFieldToBuilder(value_node, /*projection=*/std::nullopt,
                                                          decoder, item_builder, ctx));
            }
            block_count = decoder->arrayNext();
        }
        return Status::OK();
    } else {
        return Status::Invalid(
            fmt::format("Expected Avro map or array with map logical type, got: {}",
                        AvroUtils::ToString(avro_node)));
    }
}

/// Decode Avro data directly to Arrow array builder.
Status DecodeAvroValueToBuilder(const ::avro::NodePtr& avro_node,
                                const std::optional<std::set<size_t>>& projection,
                                ::avro::Decoder* decoder, arrow::ArrayBuilder* array_builder,
                                AvroDirectDecoder::DecodeContext* ctx) {
    auto type = avro_node->type();
    auto logical_type = avro_node->logicalType();

    switch (type) {
        case ::avro::AVRO_BOOL: {
            auto* builder = arrow::internal::checked_cast<arrow::BooleanBuilder*>(array_builder);
            bool value = decoder->decodeBool();
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(value));
            return Status::OK();
        }

        case ::avro::AVRO_INT: {
            int32_t value = decoder->decodeInt();
            auto arrow_type = array_builder->type();
            switch (arrow_type->id()) {
                case arrow::Type::INT8: {
                    auto* builder =
                        arrow::internal::checked_cast<arrow::Int8Builder*>(array_builder);
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(value));
                    return Status::OK();
                }
                case arrow::Type::INT16: {
                    auto* builder =
                        arrow::internal::checked_cast<arrow::Int16Builder*>(array_builder);
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(value));
                    return Status::OK();
                }
                case arrow::Type::INT32: {
                    auto* builder =
                        arrow::internal::checked_cast<arrow::Int32Builder*>(array_builder);
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(value));
                    return Status::OK();
                }
                case arrow::Type::DATE32: {
                    if (logical_type.type() != ::avro::LogicalType::Type::DATE) {
                        return Status::TypeError(
                            fmt::format("Unexpected avro type [{}] with arrow type [{}].", type,
                                        arrow_type->ToString()));
                    }
                    auto* builder =
                        arrow::internal::checked_cast<arrow::Date32Builder*>(array_builder);
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(value));
                    return Status::OK();
                }
                default:
                    return Status::TypeError(
                        fmt::format("Unexpected avro type [{}] with arrow type [{}].", type,
                                    arrow_type->ToString()));
            }
        }

        case ::avro::AVRO_LONG: {
            int64_t value = decoder->decodeLong();
            switch (logical_type.type()) {
                case ::avro::LogicalType::Type::NONE: {
                    auto* builder =
                        arrow::internal::checked_cast<arrow::Int64Builder*>(array_builder);
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(value));
                    return Status::OK();
                }
                case ::avro::LogicalType::Type::TIMESTAMP_MILLIS:
                case ::avro::LogicalType::Type::TIMESTAMP_MICROS:
                case ::avro::LogicalType::Type::TIMESTAMP_NANOS:
                case ::avro::LogicalType::Type::LOCAL_TIMESTAMP_MILLIS:
                case ::avro::LogicalType::Type::LOCAL_TIMESTAMP_MICROS:
                case ::avro::LogicalType::Type::LOCAL_TIMESTAMP_NANOS: {
                    auto* builder =
                        arrow::internal::checked_cast<arrow::TimestampBuilder*>(array_builder);
                    auto ts_type =
                        arrow::internal::checked_cast<arrow::TimestampType*>(builder->type().get());
                    // for arrow second, we need to convert it from avro millisecond
                    if (ts_type->unit() == arrow::TimeUnit::type::SECOND) {
                        value /= DateTimeUtils::CONVERSION_FACTORS[DateTimeUtils::MILLISECOND];
                    }
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(value));
                    return Status::OK();
                }
                default:
                    return Status::TypeError(
                        fmt::format("Unexpected avro type [{}] with arrow type [{}].", type,
                                    array_builder->type()->ToString()));
            }
        }

        case ::avro::AVRO_FLOAT: {
            auto* builder = arrow::internal::checked_cast<arrow::FloatBuilder*>(array_builder);
            float value = decoder->decodeFloat();
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(value));
            return Status::OK();
        }
        case ::avro::AVRO_DOUBLE: {
            auto* builder = arrow::internal::checked_cast<arrow::DoubleBuilder*>(array_builder);
            double value = decoder->decodeDouble();
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(value));
            return Status::OK();
        }
        case ::avro::AVRO_STRING: {
            auto* builder = arrow::internal::checked_cast<arrow::StringBuilder*>(array_builder);
            decoder->decodeString(ctx->string_scratch);
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(ctx->string_scratch));
            return Status::OK();
        }

        case ::avro::AVRO_BYTES: {
            decoder->decodeBytes(ctx->bytes_scratch);
            switch (logical_type.type()) {
                case ::avro::LogicalType::Type::NONE: {
                    auto* builder =
                        arrow::internal::checked_cast<arrow::BinaryBuilder*>(array_builder);
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(
                        builder->Append(ctx->bytes_scratch.data(),
                                        static_cast<int32_t>(ctx->bytes_scratch.size())));
                    return Status::OK();
                }
                case ::avro::LogicalType::Type::DECIMAL: {
                    auto* builder =
                        arrow::internal::checked_cast<arrow::Decimal128Builder*>(array_builder);
                    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                        arrow::Decimal128 decimal,
                        arrow::Decimal128::FromBigEndian(ctx->bytes_scratch.data(),
                                                         ctx->bytes_scratch.size()));
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(decimal));
                    return Status::OK();
                }
                default:
                    return Status::TypeError(
                        fmt::format("Unexpected avro type [{}] with arrow type [{}].", type,
                                    array_builder->type()->ToString()));
            }
        }

        case ::avro::AVRO_RECORD: {
            return DecodeStructToBuilder(avro_node, projection, decoder, array_builder, ctx);
        }
        case ::avro::AVRO_ARRAY: {
            if (AvroUtils::HasMapLogicalType(avro_node)) {
                return DecodeMapToBuilder(avro_node, decoder, array_builder, ctx);
            } else {
                return DecodeListToBuilder(avro_node, decoder, array_builder, ctx);
            }
        }
        case ::avro::AVRO_MAP: {
            return DecodeMapToBuilder(avro_node, decoder, array_builder, ctx);
        }
        default:
            return Status::Invalid(fmt::format("Unsupported avro type: {}", type));
    }
}

Status DecodeFieldToBuilder(const ::avro::NodePtr& avro_node,
                            const std::optional<std::set<size_t>>& projection,
                            ::avro::Decoder* decoder, arrow::ArrayBuilder* array_builder,
                            AvroDirectDecoder::DecodeContext* ctx) {
    if (avro_node->type() == ::avro::AVRO_UNION) {
        const size_t branch_index = decoder->decodeUnionIndex();

        // Validate branch index
        const size_t num_branches = avro_node->leaves();
        if (branch_index >= num_branches) {
            return Status::Invalid(fmt::format("Union branch index {} out of range [0, {})",
                                               branch_index, num_branches));
        }

        const auto& branch_node = avro_node->leafAt(branch_index);
        if (branch_node->type() == ::avro::AVRO_NULL) {
            decoder->decodeNull();
            PAIMON_RETURN_NOT_OK_FROM_ARROW(array_builder->AppendNull());
            return Status::OK();
        } else {
            return DecodeFieldToBuilder(branch_node, projection, decoder, array_builder, ctx);
        }
    }

    return DecodeAvroValueToBuilder(avro_node, projection, decoder, array_builder, ctx);
}

}  // namespace

Status AvroDirectDecoder::DecodeAvroToBuilder(const ::avro::NodePtr& avro_node,
                                              const std::optional<std::set<size_t>>& projection,
                                              ::avro::Decoder* decoder,
                                              arrow::ArrayBuilder* array_builder,
                                              DecodeContext* ctx) {
    return DecodeFieldToBuilder(avro_node, projection, decoder, array_builder, ctx);
}

}  // namespace paimon::avro
