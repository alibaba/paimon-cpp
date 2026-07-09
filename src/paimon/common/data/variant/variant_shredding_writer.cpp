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

#include "paimon/common/data/variant/variant_shredding_writer.h"

#include <string>
#include <utility>

#include "arrow/api.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/data/variant/variant_builder.h"
#include "paimon/common/data/variant/variant_defs.h"
#include "paimon/common/utils/arrow/status_utils.h"

namespace paimon {

namespace {

// Rescales `unscaled` by `10^power`, failing when the result would exceed the 38-digit
// limit. The precision check must precede each multiplication: a 38-digit value times 10
// overflows the signed 128-bit representation before a post-check could reject it.
Result<__int128_t> ScaleUpUnscaled(__int128_t unscaled, int32_t power) {
    __int128_t result = unscaled;
    for (int32_t i = 0; i < power; ++i) {
        VariantDecimal probe{result, 0};
        if (probe.Precision() >= VariantDefs::kMaxDecimal16Precision) {
            return Status::Invalid("decimal overflow while rescaling");
        }
        result *= 10;
    }
    return result;
}

Status AppendDecimalTo(arrow::ArrayBuilder* builder, __int128_t unscaled) {
    auto* decimal_builder = static_cast<arrow::Decimal128Builder*>(builder);
    arrow::Decimal128 value(static_cast<int64_t>(unscaled >> 64),
                            static_cast<uint64_t>(static_cast<__uint128_t>(unscaled)));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(decimal_builder->Append(value));
    return Status::OK();
}

}  // namespace

VariantShreddedColumnWriter::VariantShreddedColumnWriter(
    const std::shared_ptr<VariantSchema>& schema,
    std::unique_ptr<arrow::ArrayBuilder>&& root_builder)
    : schema_(schema), root_builder_(std::move(root_builder)) {}

Result<std::unique_ptr<VariantShreddedColumnWriter>> VariantShreddedColumnWriter::Create(
    const std::shared_ptr<VariantSchema>& schema,
    const std::shared_ptr<arrow::DataType>& physical_type, arrow::MemoryPool* pool) {
    if (!schema || schema->top_level_metadata_idx < 0) {
        return Status::Invalid("variant shredding schema must contain a top-level metadata field");
    }
    std::unique_ptr<arrow::ArrayBuilder> root_builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::MakeBuilder(pool, physical_type, &root_builder));
    if (root_builder->type()->id() != arrow::Type::STRUCT) {
        return Status::Invalid("variant shredded physical type must be a struct");
    }
    auto writer = std::unique_ptr<VariantShreddedColumnWriter>(
        new VariantShreddedColumnWriter(schema, std::move(root_builder)));
    PAIMON_RETURN_NOT_OK(BuildNode(
        schema, static_cast<arrow::StructBuilder*>(writer->root_builder_.get()), &writer->root_));
    return writer;
}

Status VariantShreddedColumnWriter::BuildNode(const std::shared_ptr<VariantSchema>& schema,
                                              arrow::StructBuilder* group, Node* node) {
    node->schema = schema.get();
    node->group = group;
    if (group->num_children() != schema->num_fields) {
        return Status::Invalid(
            fmt::format("variant shredded builder has {} children but the schema has {} fields",
                        group->num_children(), schema->num_fields));
    }
    if (schema->top_level_metadata_idx >= 0) {
        node->metadata = static_cast<arrow::BinaryBuilder*>(
            group->field_builder(schema->top_level_metadata_idx));
    }
    if (schema->variant_idx >= 0) {
        node->value = static_cast<arrow::BinaryBuilder*>(group->field_builder(schema->variant_idx));
    }
    if (schema->typed_idx >= 0) {
        arrow::ArrayBuilder* typed_builder = group->field_builder(schema->typed_idx);
        if (schema->has_object_schema) {
            node->typed_object = static_cast<arrow::StructBuilder*>(typed_builder);
            node->object_children.resize(schema->object_schema.size());
            for (size_t i = 0; i < schema->object_schema.size(); ++i) {
                auto* child_group = static_cast<arrow::StructBuilder*>(
                    node->typed_object->field_builder(static_cast<int>(i)));
                PAIMON_RETURN_NOT_OK(BuildNode(schema->object_schema[i].schema, child_group,
                                               &node->object_children[i]));
            }
        } else if (schema->array_schema) {
            node->typed_list = static_cast<arrow::ListBuilder*>(typed_builder);
            node->array_element = std::make_unique<Node>();
            auto* element_group =
                static_cast<arrow::StructBuilder*>(node->typed_list->value_builder());
            PAIMON_RETURN_NOT_OK(
                BuildNode(schema->array_schema, element_group, node->array_element.get()));
        } else if (schema->scalar_schema) {
            node->typed_scalar = typed_builder;
        } else {
            return Status::Invalid("variant shredding schema typed_value has no schema");
        }
    }
    return Status::OK();
}

Status VariantShreddedColumnWriter::Append(const GenericVariant& variant) {
    return AppendVariantNode(&root_, variant);
}

Status VariantShreddedColumnWriter::AppendNull() {
    PAIMON_RETURN_NOT_OK_FROM_ARROW(root_.group->AppendNull());
    return Status::OK();
}

Result<std::shared_ptr<arrow::Array>> VariantShreddedColumnWriter::Finish() {
    std::shared_ptr<arrow::Array> array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(root_builder_->Finish(&array));
    return array;
}

Status VariantShreddedColumnWriter::AppendVariantNode(Node* node, const GenericVariant& variant) {
    const VariantSchema& schema = *node->schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(node->group->Append());
    if (schema.top_level_metadata_idx >= 0) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(node->metadata->Append(variant.Metadata()));
    }
    PAIMON_ASSIGN_OR_RAISE(VariantValueType variant_type, variant.GetType());
    if (schema.array_schema != nullptr && variant_type == VariantValueType::kArray) {
        // The array element is always a struct containing untyped and typed fields.
        PAIMON_RETURN_NOT_OK_FROM_ARROW(node->typed_list->Append());
        PAIMON_ASSIGN_OR_RAISE(int32_t size, variant.ArraySize());
        for (int32_t i = 0; i < size; ++i) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GenericVariant> element,
                                   variant.GetElementAtIndex(i));
            PAIMON_RETURN_NOT_OK(AppendVariantNode(node->array_element.get(), *element));
        }
        if (node->value != nullptr) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(node->value->AppendNull());
        }
    } else if (schema.has_object_schema && variant_type == VariantValueType::kObject) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(node->typed_object->Append());
        // Collect any field that exists in the variant, but not in the shredding schema, into a
        // residual variant object that shares the top-level metadata.
        VariantBuilder residual(/*allow_duplicate_keys=*/false);
        std::vector<VariantBuilder::FieldEntry> field_entries;
        std::vector<bool> matched(node->object_children.size(), false);
        int32_t start = residual.GetWritePos();
        PAIMON_ASSIGN_OR_RAISE(int32_t object_size, variant.ObjectSize());
        for (int32_t i = 0; i < object_size; ++i) {
            PAIMON_ASSIGN_OR_RAISE(auto field, variant.GetFieldAtIndex(i));
            if (!field.has_value()) {
                return VariantBinaryUtil::MalformedVariant();
            }
            auto it = schema.object_schema_map.find(field->key);
            if (it != schema.object_schema_map.end()) {
                PAIMON_RETURN_NOT_OK(
                    AppendVariantNode(&node->object_children[it->second], *field->value));
                matched[it->second] = true;
            } else {
                // The field is not shredded. Put it in the untyped value column. The shallow
                // append is needed for correctness, since the metadata ids must stay unchanged.
                PAIMON_ASSIGN_OR_RAISE(int32_t id, variant.GetDictionaryIdAtIndex(i));
                field_entries.emplace_back(field->key, id, residual.GetWritePos() - start);
                PAIMON_RETURN_NOT_OK(
                    residual.ShallowAppendVariant(field->value->RawValue(), field->value->Pos()));
            }
        }
        // Set missing fields to non-null with all fields set to null.
        for (size_t i = 0; i < matched.size(); ++i) {
            if (!matched[i]) {
                PAIMON_RETURN_NOT_OK(AppendMissingNode(&node->object_children[i]));
            }
        }
        if (residual.GetWritePos() != start) {
            PAIMON_RETURN_NOT_OK(residual.FinishWritingObject(start, &field_entries));
            if (node->value == nullptr) {
                return Status::Invalid(
                    "variant shredding schema has no value column for residual fields");
            }
            PAIMON_RETURN_NOT_OK_FROM_ARROW(node->value->Append(residual.ValueWithoutMetadata()));
        } else if (node->value != nullptr) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(node->value->AppendNull());
        }
    } else if (schema.scalar_schema.has_value()) {
        bool shredded = false;
        PAIMON_RETURN_NOT_OK(TryTypedShred(node, variant, variant_type, &shredded));
        if (shredded) {
            if (node->value != nullptr) {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(node->value->AppendNull());
            }
        } else {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(node->typed_scalar->AppendNull());
            if (node->value == nullptr) {
                return Status::Invalid(
                    "variant shredding schema has no value column for untyped values");
            }
            PAIMON_ASSIGN_OR_RAISE(std::string_view value, variant.Value());
            PAIMON_RETURN_NOT_OK_FROM_ARROW(node->value->Append(value));
        }
    } else {
        if (node->typed_list != nullptr) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(node->typed_list->AppendNull());
        } else if (node->typed_object != nullptr) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(node->typed_object->AppendNull());
        } else if (node->typed_scalar != nullptr) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(node->typed_scalar->AppendNull());
        }
        if (node->value == nullptr) {
            return Status::Invalid(
                "variant shredding schema has no value column for untyped values");
        }
        PAIMON_ASSIGN_OR_RAISE(std::string_view value, variant.Value());
        PAIMON_RETURN_NOT_OK_FROM_ARROW(node->value->Append(value));
    }
    return Status::OK();
}

Status VariantShreddedColumnWriter::AppendMissingNode(Node* node) {
    PAIMON_RETURN_NOT_OK_FROM_ARROW(node->group->Append());
    if (node->metadata != nullptr) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(node->metadata->AppendNull());
    }
    if (node->value != nullptr) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(node->value->AppendNull());
    }
    if (node->typed_list != nullptr) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(node->typed_list->AppendNull());
    } else if (node->typed_object != nullptr) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(node->typed_object->AppendNull());
    } else if (node->typed_scalar != nullptr) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(node->typed_scalar->AppendNull());
    }
    return Status::OK();
}

Status VariantShreddedColumnWriter::TryTypedShred(Node* node, const GenericVariant& variant,
                                                  VariantValueType variant_type, bool* shredded) {
    const VariantSchema::ScalarType& target = node->schema->scalar_schema.value();
    *shredded = false;
    switch (variant_type) {
        case VariantValueType::kLong: {
            PAIMON_ASSIGN_OR_RAISE(int64_t value, variant.GetLong());
            switch (target.kind) {
                // Check that the target type can hold the actual value.
                case VariantSchema::ScalarKind::kByte:
                    if (value == static_cast<int8_t>(value)) {
                        PAIMON_RETURN_NOT_OK_FROM_ARROW(
                            static_cast<arrow::Int8Builder*>(node->typed_scalar)
                                ->Append(static_cast<int8_t>(value)));
                        *shredded = true;
                    }
                    break;
                case VariantSchema::ScalarKind::kShort:
                    if (value == static_cast<int16_t>(value)) {
                        PAIMON_RETURN_NOT_OK_FROM_ARROW(
                            static_cast<arrow::Int16Builder*>(node->typed_scalar)
                                ->Append(static_cast<int16_t>(value)));
                        *shredded = true;
                    }
                    break;
                case VariantSchema::ScalarKind::kInt:
                    if (value == static_cast<int32_t>(value)) {
                        PAIMON_RETURN_NOT_OK_FROM_ARROW(
                            static_cast<arrow::Int32Builder*>(node->typed_scalar)
                                ->Append(static_cast<int32_t>(value)));
                        *shredded = true;
                    }
                    break;
                case VariantSchema::ScalarKind::kLong:
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(
                        static_cast<arrow::Int64Builder*>(node->typed_scalar)->Append(value));
                    *shredded = true;
                    break;
                case VariantSchema::ScalarKind::kDecimal: {
                    // If the integer can fit in the given decimal precision, allow it.
                    auto scaled = ScaleUpUnscaled(value, target.scale);
                    if (scaled.ok()) {
                        VariantDecimal probe{scaled.value(), target.scale};
                        if (probe.Precision() <= target.precision) {
                            PAIMON_RETURN_NOT_OK(
                                AppendDecimalTo(node->typed_scalar, scaled.value()));
                            *shredded = true;
                        }
                    }
                    break;
                }
                default:
                    break;
            }
            break;
        }
        case VariantValueType::kDecimal: {
            if (target.kind == VariantSchema::ScalarKind::kDecimal) {
                // Use the original scale so that scale information is retained.
                PAIMON_ASSIGN_OR_RAISE(VariantDecimal value,
                                       VariantBinaryUtil::GetDecimalWithOriginalScale(
                                           variant.RawValue(), variant.Pos()));
                if (value.Precision() <= target.precision && value.scale == target.scale) {
                    PAIMON_RETURN_NOT_OK(AppendDecimalTo(node->typed_scalar, value.unscaled));
                    *shredded = true;
                    break;
                }
                // Convert to the target scale, and see if it fits without losing information.
                int32_t scale_diff = target.scale - value.scale;
                __int128_t rescaled = value.unscaled;
                bool exact = true;
                if (scale_diff >= 0) {
                    auto scaled = ScaleUpUnscaled(value.unscaled, scale_diff);
                    if (scaled.ok()) {
                        rescaled = scaled.value();
                    } else {
                        exact = false;
                    }
                } else {
                    for (int32_t i = 0; i < -scale_diff && exact; ++i) {
                        if (rescaled % 10 != 0) {
                            exact = false;
                        } else {
                            rescaled /= 10;
                        }
                    }
                }
                if (exact) {
                    VariantDecimal probe{rescaled, target.scale};
                    if (probe.Precision() <= target.precision) {
                        PAIMON_RETURN_NOT_OK(AppendDecimalTo(node->typed_scalar, rescaled));
                        *shredded = true;
                    }
                }
            } else if (target.kind == VariantSchema::ScalarKind::kByte ||
                       target.kind == VariantSchema::ScalarKind::kShort ||
                       target.kind == VariantSchema::ScalarKind::kInt ||
                       target.kind == VariantSchema::ScalarKind::kLong) {
                // Check if the decimal happens to be an integer.
                PAIMON_ASSIGN_OR_RAISE(VariantDecimal value, variant.GetDecimal());
                if (value.scale > 0) {
                    break;
                }
                auto scaled = ScaleUpUnscaled(value.unscaled, -value.scale);
                if (!scaled.ok()) {
                    break;
                }
                __int128_t integral = scaled.value();
                if (integral != static_cast<int64_t>(integral)) {
                    break;
                }
                auto long_value = static_cast<int64_t>(integral);
                switch (target.kind) {
                    case VariantSchema::ScalarKind::kByte:
                        if (long_value == static_cast<int8_t>(long_value)) {
                            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                                static_cast<arrow::Int8Builder*>(node->typed_scalar)
                                    ->Append(static_cast<int8_t>(long_value)));
                            *shredded = true;
                        }
                        break;
                    case VariantSchema::ScalarKind::kShort:
                        if (long_value == static_cast<int16_t>(long_value)) {
                            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                                static_cast<arrow::Int16Builder*>(node->typed_scalar)
                                    ->Append(static_cast<int16_t>(long_value)));
                            *shredded = true;
                        }
                        break;
                    case VariantSchema::ScalarKind::kInt:
                        if (long_value == static_cast<int32_t>(long_value)) {
                            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                                static_cast<arrow::Int32Builder*>(node->typed_scalar)
                                    ->Append(static_cast<int32_t>(long_value)));
                            *shredded = true;
                        }
                        break;
                    default:
                        PAIMON_RETURN_NOT_OK_FROM_ARROW(
                            static_cast<arrow::Int64Builder*>(node->typed_scalar)
                                ->Append(long_value));
                        *shredded = true;
                        break;
                }
            }
            break;
        }
        case VariantValueType::kBoolean: {
            if (target.kind == VariantSchema::ScalarKind::kBoolean) {
                PAIMON_ASSIGN_OR_RAISE(bool value, variant.GetBoolean());
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    static_cast<arrow::BooleanBuilder*>(node->typed_scalar)->Append(value));
                *shredded = true;
            }
            break;
        }
        case VariantValueType::kString: {
            if (target.kind == VariantSchema::ScalarKind::kString) {
                PAIMON_ASSIGN_OR_RAISE(std::string_view value, variant.GetString());
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    static_cast<arrow::StringBuilder*>(node->typed_scalar)->Append(value));
                *shredded = true;
            }
            break;
        }
        case VariantValueType::kDouble: {
            if (target.kind == VariantSchema::ScalarKind::kDouble) {
                PAIMON_ASSIGN_OR_RAISE(double value, variant.GetDouble());
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    static_cast<arrow::DoubleBuilder*>(node->typed_scalar)->Append(value));
                *shredded = true;
            }
            break;
        }
        case VariantValueType::kFloat: {
            if (target.kind == VariantSchema::ScalarKind::kFloat) {
                PAIMON_ASSIGN_OR_RAISE(float value, variant.GetFloat());
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    static_cast<arrow::FloatBuilder*>(node->typed_scalar)->Append(value));
                *shredded = true;
            }
            break;
        }
        case VariantValueType::kDate: {
            if (target.kind == VariantSchema::ScalarKind::kDate) {
                PAIMON_ASSIGN_OR_RAISE(int64_t value, variant.GetLong());
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    static_cast<arrow::Date32Builder*>(node->typed_scalar)
                        ->Append(static_cast<int32_t>(value)));
                *shredded = true;
            }
            break;
        }
        case VariantValueType::kBinary: {
            if (target.kind == VariantSchema::ScalarKind::kBinary) {
                PAIMON_ASSIGN_OR_RAISE(std::string_view value, variant.GetBinary());
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    static_cast<arrow::BinaryBuilder*>(node->typed_scalar)->Append(value));
                *shredded = true;
            }
            break;
        }
        default:
            // TIMESTAMP/TIMESTAMP_NTZ/UUID typed columns are not producible by the configured
            // shredding schema; the value stays in the untyped column.
            break;
    }
    return Status::OK();
}

}  // namespace paimon
