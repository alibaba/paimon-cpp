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

#include "paimon/format/avro/avro_schema_converter.h"

#include <cassert>
#include <cstddef>
#include <utility>
#include <vector>

#include "arrow/util/checked_cast.h"
#include "avro/CustomAttributes.hh"
#include "avro/LogicalType.hh"
#include "avro/Node.hh"
#include "avro/Schema.hh"
#include "avro/Types.hh"
#include "avro/ValidSchema.hh"
#include "fmt/format.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/format/avro/avro_file_format_factory.h"
#include "paimon/format/avro/avro_utils.h"
#include "paimon/macros.h"
#include "paimon/status.h"
namespace paimon::avro {

/// Returns schema with nullable true.
::avro::Schema AvroSchemaConverter::NullableSchema(const ::avro::Schema& schema) {
    assert(schema.type() != ::avro::AVRO_UNION);
    ::avro::UnionSchema union_schema;
    union_schema.addType(::avro::NullSchema());
    union_schema.addType(schema);
    return union_schema;
}

void AvroSchemaConverter::AddRecordField(::avro::RecordSchema* record_schema,
                                         const std::string& field_name,
                                         const ::avro::Schema& field_schema) {
    if (field_schema.type() == ::avro::Type::AVRO_UNION) {
        ::avro::CustomAttributes attrs;
        attrs.addAttribute("default", "null", /*addQuotes=*/false);
        record_schema->addField(field_name, field_schema, attrs);
    } else {
        record_schema->addField(field_name, field_schema);
    }
}

Result<bool> AvroSchemaConverter::CheckUnionType(const ::avro::NodePtr& avro_node) {
    auto type = avro_node->type();
    if (type == ::avro::AVRO_UNION) {
        if (avro_node->leaves() != 2) {
            return Status::Invalid("not support avro union leaves not 2");
        }
        auto node = avro_node->leafAt(0);
        if (node->type() != ::avro::AVRO_NULL) {
            return Status::Invalid("not support avro union first leaf is not avro null");
        }
        return true;
    }
    return false;
}

Result<std::shared_ptr<arrow::DataType>> AvroSchemaConverter::AvroSchemaToArrowDataType(
    const ::avro::ValidSchema& avro_schema) {
    ::avro::NodePtr root = avro_schema.root();
    PAIMON_ASSIGN_OR_RAISE(bool is_union, CheckUnionType(root));
    if (is_union) {
        root = root->leafAt(1);
    }
    if (PAIMON_UNLIKELY(root->type() != ::avro::AVRO_RECORD)) {
        return Status::Invalid("Avro schema root node is not a record type");
    }
    bool nullable = false;
    return GetArrowType(root, &nullable);
}

Result<std::shared_ptr<arrow::Field>> AvroSchemaConverter::GetArrowField(
    const std::string& name, const ::avro::NodePtr& avro_node) {
    bool nullable = false;
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> arrow_type,
                           GetArrowType(avro_node, &nullable));
    return arrow::field(name, std::move(arrow_type), nullable);
}

Result<std::shared_ptr<arrow::DataType>> AvroSchemaConverter::GetArrowType(
    const ::avro::NodePtr& avro_node, bool* nullable) {
    PAIMON_ASSIGN_OR_RAISE(bool is_union, CheckUnionType(avro_node));
    if (is_union) {
        *nullable = true;
        return GetArrowType(avro_node->leafAt(1), nullable);
    }
    auto type = avro_node->type();
    auto logical_type = avro_node->logicalType();
    switch (logical_type.type()) {
        case ::avro::LogicalType::Type::NONE:
            break;
        case ::avro::LogicalType::Type::DATE:
            if (type != ::avro::AVRO_INT) {
                return Status::TypeError("invalid avro date stored as ", toString(type));
            }
            return arrow::date32();
        case ::avro::LogicalType::Type::DECIMAL:
            if (type != ::avro::AVRO_BYTES) {
                return Status::TypeError("invalid avro decimal stored as ", toString(type));
            }
            return arrow::decimal128(logical_type.precision(), logical_type.scale());
        case ::avro::LogicalType::Type::TIMESTAMP_MILLIS: {
            if (type != ::avro::AVRO_LONG) {
                return Status::TypeError("invalid avro timestamp stored as ", toString(type));
            }
            return arrow::timestamp(arrow::TimeUnit::MILLI);
        }
        case ::avro::LogicalType::Type::TIMESTAMP_MICROS: {
            if (type != ::avro::AVRO_LONG) {
                return Status::TypeError("invalid avro timestamp stored as ", toString(type));
            }
            return arrow::timestamp(arrow::TimeUnit::MICRO);
        }
        case ::avro::LogicalType::Type::TIMESTAMP_NANOS: {
            if (type != ::avro::AVRO_LONG) {
                return Status::TypeError("invalid avro timestamp stored as ", toString(type));
            }
            return arrow::timestamp(arrow::TimeUnit::NANO);
        }
        case ::avro::LogicalType::Type::LOCAL_TIMESTAMP_MILLIS: {
            if (type != ::avro::AVRO_LONG) {
                return Status::TypeError("invalid avro timestamp stored as ", toString(type));
            }
            auto timezone = DateTimeUtils::GetLocalTimezoneName();
            return arrow::timestamp(arrow::TimeUnit::MILLI, timezone);
        }
        case ::avro::LogicalType::Type::LOCAL_TIMESTAMP_MICROS: {
            if (type != ::avro::AVRO_LONG) {
                return Status::TypeError("invalid avro timestamp stored as ", toString(type));
            }
            auto timezone = DateTimeUtils::GetLocalTimezoneName();
            return arrow::timestamp(arrow::TimeUnit::MICRO, timezone);
        }
        case ::avro::LogicalType::Type::LOCAL_TIMESTAMP_NANOS: {
            if (type != ::avro::AVRO_LONG) {
                return Status::TypeError("invalid avro timestamp stored as ", toString(type));
            }
            auto timezone = DateTimeUtils::GetLocalTimezoneName();
            return arrow::timestamp(arrow::TimeUnit::NANO, timezone);
        }
        case ::avro::LogicalType::Type::CUSTOM: {
            if (!AvroUtils::HasMapLogicalType(avro_node)) {
                return Status::TypeError("invalid avro logical map type");
            }
            if (type != ::avro::AVRO_ARRAY) {
                return Status::TypeError("invalid avro logical map stored as ", toString(type));
            }
            size_t subtype_count = avro_node->leaves();
            if (subtype_count != 1) {
                return Status::TypeError("invalid avro logical map type");
            }
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> logical_map_field,
                                   GetArrowField("item", avro_node->leafAt(0)));
            auto logical_map_type = logical_map_field->type();
            if (logical_map_type->id() != arrow::Type::STRUCT) {
                return Status::TypeError("invalid avro logical map item type");
            }
            auto struct_type =
                arrow::internal::checked_pointer_cast<arrow::StructType>(logical_map_type);
            const auto& fields = struct_type->fields();
            if (fields.size() != 2) {
                return Status::TypeError("invalid avro logical map struct fields size");
            }
            auto key_field = fields[0]->WithNullable(false);
            auto value_field = fields[1];
            if (key_field->name() != "key" || value_field->name() != "value") {
                return Status::TypeError("invalid avro logical map struct field names");
            }
            return std::make_shared<arrow::MapType>(std::move(key_field), std::move(value_field));
        }
        default:
            return Status::Invalid("invalid avro logical type: ",
                                   AvroUtils::ToString(logical_type));
    }

    size_t subtype_count = avro_node->leaves();
    switch (type) {
        case ::avro::AVRO_BOOL: {
            return arrow::boolean();
        }
        case ::avro::AVRO_INT: {
            return arrow::int32();
        }
        case ::avro::AVRO_LONG: {
            return arrow::int64();
        }
        case ::avro::AVRO_FLOAT: {
            return arrow::float32();
        }
        case ::avro::AVRO_DOUBLE: {
            return arrow::float64();
        }
        case ::avro::AVRO_STRING: {
            return arrow::utf8();
        }
        case ::avro::AVRO_BYTES: {
            return arrow::binary();
        }
        case ::avro::AVRO_ARRAY: {
            if (subtype_count != 1) {
                return Status::TypeError("Invalid Avro List type");
            }
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> child_field,
                                   GetArrowField("item", avro_node->leafAt(0)));
            return arrow::list(std::move(child_field));
        }
        case ::avro::AVRO_MAP: {
            if (subtype_count != 2) {
                return Status::TypeError("Invalid Avro Map type");
            }
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> key_field,
                                   GetArrowField("key", avro_node->leafAt(0)));
            key_field = key_field->WithNullable(false);
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> value_field,
                                   GetArrowField("value", avro_node->leafAt(1)));
            return std::make_shared<arrow::MapType>(std::move(key_field), std::move(value_field));
        }
        case ::avro::AVRO_RECORD: {
            arrow::FieldVector fields(subtype_count);
            for (size_t child = 0; child < subtype_count; ++child) {
                const auto& name = avro_node->nameAt(child);
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> child_field,
                                       GetArrowField(name, avro_node->leafAt(child)));
                fields[child] = std::move(child_field);
            }
            return arrow::struct_(std::move(fields));
        }
        default:
            return Status::TypeError("Unknown Avro type kind: ", toString(type));
    }
}

Result<::avro::Schema> AvroSchemaConverter::ArrowTypeToAvroSchema(
    const std::shared_ptr<arrow::Field>& field, const std::string& row_name) {
    bool nullable = field->nullable();
    auto arrow_type = field->type();
    switch (arrow_type->id()) {
        case arrow::Type::BOOL:
            return nullable ? NullableSchema(::avro::BoolSchema()) : ::avro::BoolSchema();
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
            return nullable ? NullableSchema(::avro::IntSchema()) : ::avro::IntSchema();
        case arrow::Type::INT64:
            return nullable ? NullableSchema(::avro::LongSchema()) : ::avro::LongSchema();
        case arrow::Type::FLOAT:
            return nullable ? NullableSchema(::avro::FloatSchema()) : ::avro::FloatSchema();
        case arrow::Type::DOUBLE:
            return nullable ? NullableSchema(::avro::DoubleSchema()) : ::avro::DoubleSchema();
        case arrow::Type::STRING:
            return nullable ? NullableSchema(::avro::StringSchema()) : ::avro::StringSchema();
        case arrow::Type::BINARY:
            return nullable ? NullableSchema(::avro::BytesSchema()) : ::avro::BytesSchema();
        case arrow::Type::type::DATE32: {
            ::avro::IntSchema date_schema;
            ::avro::LogicalType date_type = ::avro::LogicalType(::avro::LogicalType::DATE);
            date_schema.root()->setLogicalType(date_type);
            return nullable ? NullableSchema(date_schema) : date_schema;
        }
        case arrow::Type::type::TIMESTAMP: {
            const auto& arrow_timestamp_type =
                arrow::internal::checked_pointer_cast<arrow::TimestampType>(arrow_type);
            bool has_timezone = !arrow_timestamp_type->timezone().empty();
            ::avro::LongSchema timestamp_schema;
            switch (arrow_timestamp_type->unit()) {
                // Avro doesn't support seconds, convert to milliseconds
                case arrow::TimeUnit::type::SECOND:
                case arrow::TimeUnit::type::MILLI: {
                    ::avro::LogicalType logical_type = ::avro::LogicalType(
                        has_timezone ? ::avro::LogicalType::LOCAL_TIMESTAMP_MILLIS
                                     : ::avro::LogicalType::TIMESTAMP_MILLIS);
                    timestamp_schema.root()->setLogicalType(logical_type);
                    break;
                }
                case arrow::TimeUnit::type::MICRO: {
                    ::avro::LogicalType logical_type = ::avro::LogicalType(
                        has_timezone ? ::avro::LogicalType::LOCAL_TIMESTAMP_MICROS
                                     : ::avro::LogicalType::TIMESTAMP_MICROS);
                    timestamp_schema.root()->setLogicalType(logical_type);
                    break;
                }
                case arrow::TimeUnit::type::NANO: {
                    ::avro::LogicalType logical_type = ::avro::LogicalType(
                        has_timezone ? ::avro::LogicalType::LOCAL_TIMESTAMP_NANOS
                                     : ::avro::LogicalType::TIMESTAMP_NANOS);
                    timestamp_schema.root()->setLogicalType(logical_type);
                    break;
                }
                default:
                    return Status::Invalid("Unknown TimeUnit in TimestampType");
            }
            return nullable ? NullableSchema(timestamp_schema) : timestamp_schema;
        }
        case arrow::Type::type::DECIMAL128: {
            const auto& arrow_decimal_type =
                arrow::internal::checked_pointer_cast<arrow::Decimal128Type>(arrow_type);
            ::avro::BytesSchema decimal_schema;
            ::avro::LogicalType decimal_type = ::avro::LogicalType(::avro::LogicalType::DECIMAL);
            decimal_type.setPrecision(arrow_decimal_type->precision());
            decimal_type.setScale(arrow_decimal_type->scale());
            decimal_schema.root()->setLogicalType(decimal_type);
            return nullable ? NullableSchema(decimal_schema) : decimal_schema;
        }
        case arrow::Type::LIST: {
            const auto& list_type =
                arrow::internal::checked_pointer_cast<const arrow::ListType>(arrow_type);
            const auto& value_field = list_type->value_field();
            PAIMON_ASSIGN_OR_RAISE(auto value_schema, ArrowTypeToAvroSchema(value_field, row_name));
            ::avro::ArraySchema array_schema(value_schema);
            return nullable ? NullableSchema(array_schema) : array_schema;
        }
        case arrow::Type::STRUCT: {
            const auto& struct_type =
                arrow::internal::checked_pointer_cast<const arrow::StructType>(arrow_type);
            const auto& fields = struct_type->fields();

            ::avro::RecordSchema record_schema(row_name);
            for (const auto& f : fields) {
                PAIMON_ASSIGN_OR_RAISE(auto field_schema,
                                       ArrowTypeToAvroSchema(f, row_name + "_" + f->name()));
                AddRecordField(&record_schema, f->name(), field_schema);
            }
            return nullable ? NullableSchema(record_schema) : record_schema;
        }
        case arrow::Type::MAP: {
            const auto& map_type =
                arrow::internal::checked_pointer_cast<const arrow::MapType>(arrow_type);
            const auto& key_field = map_type->key_field();
            const auto& item_field = map_type->item_field();
            if (key_field->nullable()) {
                return Status::Invalid("Avro Map key cannot be nullable");
            }
            if (key_field->type()->id() == arrow::Type::STRING) {
                PAIMON_ASSIGN_OR_RAISE(auto item_schema,
                                       ArrowTypeToAvroSchema(item_field, row_name));
                ::avro::MapSchema map_schema(item_schema);
                return nullable ? NullableSchema(map_schema) : map_schema;
            } else {
                // convert to list<record<key,value>>
                PAIMON_ASSIGN_OR_RAISE(auto key_schema,
                                       ArrowTypeToAvroSchema(key_field, row_name + "_key"));
                PAIMON_ASSIGN_OR_RAISE(auto item_schema,
                                       ArrowTypeToAvroSchema(item_field, row_name + "_value"));
                ::avro::LogicalType logical_map_type =
                    ::avro::LogicalType(std::make_shared<MapLogicalType>());
                ::avro::RecordSchema record_schema(row_name);
                AddRecordField(&record_schema, "key", key_schema);
                AddRecordField(&record_schema, "value", item_schema);
                ::avro::ArraySchema logical_map_schema(record_schema);
                logical_map_schema.root()->setLogicalType(logical_map_type);
                return nullable ? NullableSchema(logical_map_schema) : logical_map_schema;
            }
        }
        default:
            return Status::Invalid(fmt::format("Not support arrow type '{}' convert to avro",
                                               field->type()->ToString()));
    }
}

Result<::avro::ValidSchema> AvroSchemaConverter::ArrowSchemaToAvroSchema(
    const std::shared_ptr<arrow::Schema>& arrow_schema) {
    // top level row name of avro record, the same as java paimon
    static const std::string kTopLevelRowName = "org.apache.paimon.avro.generated.record";
    ::avro::RecordSchema record_schema(kTopLevelRowName);
    for (const auto& field : arrow_schema->fields()) {
        PAIMON_ASSIGN_OR_RAISE(
            ::avro::Schema field_schema,
            ArrowTypeToAvroSchema(field, kTopLevelRowName + "_" + field->name()));
        AddRecordField(&record_schema, field->name(), field_schema);
    }
    return ::avro::ValidSchema(record_schema);
}

}  // namespace paimon::avro
