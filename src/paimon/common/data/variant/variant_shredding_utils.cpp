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

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed
 * by the Apache Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. */

#include "paimon/common/data/variant/variant_shredding_utils.h"

#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/data/variant/variant_defs.h"
#include "paimon/common/data/variant/variant_type_utils.h"

namespace paimon {

namespace {

Status InvalidVariantShreddingSchema(const std::shared_ptr<arrow::DataType>& type) {
    return Status::Invalid(
        fmt::format("Invalid variant shredding schema: {}", type ? type->ToString() : "null"));
}

// Mirrors the Java `PaimonShreddingUtils.variantShreddingSchema(dataType, isTopLevel,
// isObjectField)`.
Result<std::shared_ptr<arrow::DataType>> VariantShreddingSchemaImpl(
    const std::shared_ptr<arrow::DataType>& data_type, bool is_top_level, bool is_object_field) {
    arrow::FieldVector fields;
    if (is_top_level) {
        fields.push_back(arrow::field(VariantDefs::kMetadataFieldName, arrow::binary(),
                                      /*nullable=*/false));
    }
    switch (data_type->id()) {
        case arrow::Type::LIST: {
            const auto& list_type = std::static_pointer_cast<arrow::ListType>(data_type);
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> element_type,
                                   VariantShreddingSchemaImpl(list_type->value_type(),
                                                              /*is_top_level=*/false,
                                                              /*is_object_field=*/false));
            fields.push_back(
                arrow::field(VariantDefs::kValueFieldName, arrow::binary(), /*nullable=*/true));
            fields.push_back(arrow::field(VariantDefs::kTypedValueFieldName,
                                          arrow::list(element_type), /*nullable=*/true));
            break;
        }
        case arrow::Type::STRUCT: {
            // The field name level is always non-nullable: Variant null values are represented in
            // the "value" column as "00", and missing values are represented by setting both
            // "value" and "typed_value" to null.
            const auto& struct_type = std::static_pointer_cast<arrow::StructType>(data_type);
            arrow::FieldVector shredded_fields;
            for (const auto& field : struct_type->fields()) {
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> field_type,
                                       VariantShreddingSchemaImpl(field->type(),
                                                                  /*is_top_level=*/false,
                                                                  /*is_object_field=*/true));
                shredded_fields.push_back(
                    arrow::field(field->name(), field_type, /*nullable=*/false));
            }
            fields.push_back(
                arrow::field(VariantDefs::kValueFieldName, arrow::binary(), /*nullable=*/true));
            fields.push_back(arrow::field(VariantDefs::kTypedValueFieldName,
                                          arrow::struct_(shredded_fields), /*nullable=*/true));
            break;
        }
        case arrow::Type::NA: {
            // `arrow::null()` denotes an untyped VARIANT leaf in shredding types. It doesn't
            // need a typed column. If there is no typed column, value is required for array
            // elements or top-level fields, but optional for objects (where a null represents a
            // missing field).
            fields.push_back(arrow::field(VariantDefs::kValueFieldName, arrow::binary(),
                                          /*nullable=*/is_object_field));
            break;
        }
        case arrow::Type::STRING:
        case arrow::Type::BOOL:
        case arrow::Type::BINARY:
        case arrow::Type::DECIMAL128:
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::INT64:
        case arrow::Type::FLOAT:
        case arrow::Type::DOUBLE: {
            fields.push_back(
                arrow::field(VariantDefs::kValueFieldName, arrow::binary(), /*nullable=*/true));
            fields.push_back(arrow::field(VariantDefs::kTypedValueFieldName, data_type,
                                          /*nullable=*/true));
            break;
        }
        default:
            return InvalidVariantShreddingSchema(data_type);
    }
    return arrow::struct_(fields);
}

Result<std::shared_ptr<VariantSchema>> BuildVariantSchemaImpl(
    const std::shared_ptr<arrow::DataType>& type, bool top_level) {
    if (type->id() != arrow::Type::STRUCT) {
        return InvalidVariantShreddingSchema(type);
    }
    const auto& struct_type = std::static_pointer_cast<arrow::StructType>(type);
    // The struct must not be empty or contain duplicate field names. The latter is enforced in
    // the loop below.
    if (struct_type->num_fields() == 0) {
        return InvalidVariantShreddingSchema(type);
    }

    auto schema = std::make_shared<VariantSchema>();
    schema->num_fields = struct_type->num_fields();

    for (int32_t i = 0; i < struct_type->num_fields(); ++i) {
        const auto& field = struct_type->field(i);
        const auto& field_type = field->type();
        if (field->name() == VariantDefs::kTypedValueFieldName) {
            if (schema->typed_idx != -1) {
                return InvalidVariantShreddingSchema(type);
            }
            schema->typed_idx = i;
            switch (field_type->id()) {
                case arrow::Type::STRUCT: {
                    const auto& object_type =
                        std::static_pointer_cast<arrow::StructType>(field_type);
                    schema->has_object_schema = true;
                    schema->object_schema.reserve(object_type->num_fields());
                    for (int32_t index = 0; index < object_type->num_fields(); ++index) {
                        const auto& object_field = object_type->field(index);
                        PAIMON_ASSIGN_OR_RAISE(
                            std::shared_ptr<VariantSchema> field_schema,
                            BuildVariantSchemaImpl(object_field->type(), /*top_level=*/false));
                        schema->object_schema.push_back(
                            VariantSchema::ObjectField{object_field->name(), field_schema});
                        auto [it, inserted] =
                            schema->object_schema_map.emplace(object_field->name(), index);
                        if (!inserted) {
                            return InvalidVariantShreddingSchema(type);
                        }
                    }
                    break;
                }
                case arrow::Type::LIST: {
                    const auto& list_type = std::static_pointer_cast<arrow::ListType>(field_type);
                    PAIMON_ASSIGN_OR_RAISE(
                        schema->array_schema,
                        BuildVariantSchemaImpl(list_type->value_type(), /*top_level=*/false));
                    break;
                }
                case arrow::Type::BOOL:
                    schema->scalar_schema =
                        VariantSchema::ScalarType{VariantSchema::ScalarKind::kBoolean};
                    break;
                case arrow::Type::INT8:
                    schema->scalar_schema =
                        VariantSchema::ScalarType{VariantSchema::ScalarKind::kByte};
                    break;
                case arrow::Type::INT16:
                    schema->scalar_schema =
                        VariantSchema::ScalarType{VariantSchema::ScalarKind::kShort};
                    break;
                case arrow::Type::INT32:
                    schema->scalar_schema =
                        VariantSchema::ScalarType{VariantSchema::ScalarKind::kInt};
                    break;
                case arrow::Type::INT64:
                    schema->scalar_schema =
                        VariantSchema::ScalarType{VariantSchema::ScalarKind::kLong};
                    break;
                case arrow::Type::FLOAT:
                    schema->scalar_schema =
                        VariantSchema::ScalarType{VariantSchema::ScalarKind::kFloat};
                    break;
                case arrow::Type::DOUBLE:
                    schema->scalar_schema =
                        VariantSchema::ScalarType{VariantSchema::ScalarKind::kDouble};
                    break;
                case arrow::Type::STRING:
                    schema->scalar_schema =
                        VariantSchema::ScalarType{VariantSchema::ScalarKind::kString};
                    break;
                case arrow::Type::BINARY:
                    schema->scalar_schema =
                        VariantSchema::ScalarType{VariantSchema::ScalarKind::kBinary};
                    break;
                case arrow::Type::DATE32:
                    schema->scalar_schema =
                        VariantSchema::ScalarType{VariantSchema::ScalarKind::kDate};
                    break;
                case arrow::Type::DECIMAL128: {
                    const auto& decimal_type =
                        std::static_pointer_cast<arrow::Decimal128Type>(field_type);
                    schema->scalar_schema =
                        VariantSchema::ScalarType{VariantSchema::ScalarKind::kDecimal,
                                                  decimal_type->precision(), decimal_type->scale()};
                    break;
                }
                default:
                    return InvalidVariantShreddingSchema(type);
            }
        } else if (field->name() == VariantDefs::kValueFieldName) {
            if (schema->variant_idx != -1 || field_type->id() != arrow::Type::BINARY) {
                return InvalidVariantShreddingSchema(type);
            }
            schema->variant_idx = i;
        } else if (field->name() == VariantDefs::kMetadataFieldName) {
            if (schema->top_level_metadata_idx != -1 || field_type->id() != arrow::Type::BINARY) {
                return InvalidVariantShreddingSchema(type);
            }
            schema->top_level_metadata_idx = i;
        } else {
            return InvalidVariantShreddingSchema(type);
        }
    }

    if (top_level != (schema->top_level_metadata_idx >= 0)) {
        return InvalidVariantShreddingSchema(type);
    }
    return schema;
}

}  // namespace

Result<std::shared_ptr<arrow::DataType>> VariantShreddingUtils::VariantShreddingSchema(
    const std::shared_ptr<arrow::DataType>& shredding_type) {
    return VariantShreddingSchemaImpl(shredding_type, /*is_top_level=*/true,
                                      /*is_object_field=*/false);
}

Result<std::shared_ptr<VariantSchema>> VariantShreddingUtils::BuildVariantSchema(
    const std::shared_ptr<arrow::DataType>& struct_type) {
    return BuildVariantSchemaImpl(struct_type, /*top_level=*/true);
}

Result<std::shared_ptr<arrow::DataType>> VariantShreddingUtils::ScalarSchemaToArrowType(
    const VariantSchema::ScalarType& scalar) {
    switch (scalar.kind) {
        case VariantSchema::ScalarKind::kBoolean:
            return arrow::boolean();
        case VariantSchema::ScalarKind::kByte:
            return arrow::int8();
        case VariantSchema::ScalarKind::kShort:
            return arrow::int16();
        case VariantSchema::ScalarKind::kInt:
            return arrow::int32();
        case VariantSchema::ScalarKind::kLong:
            return arrow::int64();
        case VariantSchema::ScalarKind::kFloat:
            return arrow::float32();
        case VariantSchema::ScalarKind::kDouble:
            return arrow::float64();
        case VariantSchema::ScalarKind::kString:
            return arrow::utf8();
        case VariantSchema::ScalarKind::kBinary:
            return arrow::binary();
        case VariantSchema::ScalarKind::kDecimal:
            return arrow::decimal128(scalar.precision, scalar.scale);
        case VariantSchema::ScalarKind::kDate:
            return arrow::date32();
        default:
            return Status::NotImplemented(fmt::format("Unsupported variant scalar kind: {}",
                                                      static_cast<int32_t>(scalar.kind)));
    }
}

bool VariantShreddingUtils::IsShreddedFileType(
    const std::shared_ptr<arrow::DataType>& file_variant_type) {
    if (!file_variant_type || file_variant_type->id() != arrow::Type::STRUCT) {
        return false;
    }
    const auto& struct_type = std::static_pointer_cast<arrow::StructType>(file_variant_type);
    return struct_type->GetFieldByName(VariantDefs::kTypedValueFieldName) != nullptr;
}

}  // namespace paimon
