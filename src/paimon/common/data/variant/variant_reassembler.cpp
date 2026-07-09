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

#include "paimon/common/data/variant/variant_reassembler.h"

#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/util/checked_cast.h"
#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/common/data/variant/variant_builder.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {

namespace {

// A row view over a shredded struct array, the Arrow analog of the Java `ShreddedRow`.
struct Cursor {
    const arrow::StructArray* array;
    int64_t row;

    bool IsNullAt(int32_t field) const {
        return array->field(field)->IsNull(row);
    }

    std::string_view GetBinary(int32_t field) const {
        return static_cast<const arrow::BinaryArray&>(*array->field(field)).GetView(row);
    }
};

Status Rebuild(const Cursor& cursor, std::string_view metadata, const VariantSchema& schema,
               const std::shared_ptr<MemoryPool>& pool, VariantBuilder* builder);

Status RebuildTypedScalar(const Cursor& cursor, const VariantSchema& schema,
                          VariantBuilder* builder) {
    int32_t typed_idx = schema.typed_idx;
    const arrow::Array& typed_array = *cursor.array->field(typed_idx);
    int64_t row = cursor.row;
    const VariantSchema::ScalarType& scalar = schema.scalar_schema.value();
    switch (scalar.kind) {
        case VariantSchema::ScalarKind::kString:
            return builder->AppendString(
                static_cast<const arrow::StringArray&>(typed_array).GetView(row));
        case VariantSchema::ScalarKind::kByte:
            return builder->AppendLong(
                static_cast<const arrow::Int8Array&>(typed_array).Value(row));
        case VariantSchema::ScalarKind::kShort:
            return builder->AppendLong(
                static_cast<const arrow::Int16Array&>(typed_array).Value(row));
        case VariantSchema::ScalarKind::kInt:
            return builder->AppendLong(
                static_cast<const arrow::Int32Array&>(typed_array).Value(row));
        case VariantSchema::ScalarKind::kLong:
            return builder->AppendLong(
                static_cast<const arrow::Int64Array&>(typed_array).Value(row));
        case VariantSchema::ScalarKind::kFloat:
            return builder->AppendFloat(
                static_cast<const arrow::FloatArray&>(typed_array).Value(row));
        case VariantSchema::ScalarKind::kDouble:
            return builder->AppendDouble(
                static_cast<const arrow::DoubleArray&>(typed_array).Value(row));
        case VariantSchema::ScalarKind::kBoolean:
            return builder->AppendBoolean(
                static_cast<const arrow::BooleanArray&>(typed_array).Value(row));
        case VariantSchema::ScalarKind::kBinary:
            return builder->AppendBinary(
                static_cast<const arrow::BinaryArray&>(typed_array).GetView(row));
        case VariantSchema::ScalarKind::kDecimal: {
            const auto& decimal_array = static_cast<const arrow::Decimal128Array&>(typed_array);
            arrow::Decimal128 value(decimal_array.GetValue(row));
            VariantDecimal decimal;
            decimal.unscaled = (static_cast<__int128_t>(value.high_bits()) << 64) |
                               static_cast<__int128_t>(static_cast<__uint128_t>(value.low_bits()));
            decimal.scale = scalar.scale;
            return builder->AppendDecimal(decimal);
        }
        case VariantSchema::ScalarKind::kDate:
            return builder->AppendDate(
                static_cast<const arrow::Date32Array&>(typed_array).Value(row));
        case VariantSchema::ScalarKind::kTimestampLtz:
            return builder->AppendTimestamp(
                static_cast<const arrow::TimestampArray&>(typed_array).Value(row));
        case VariantSchema::ScalarKind::kTimestampNtz:
            return builder->AppendTimestampNtz(
                static_cast<const arrow::TimestampArray&>(typed_array).Value(row));
        default:
            return Status::NotImplemented("unsupported variant scalar kind in reassembly");
    }
}

// Rebuilds a variant value from the shredded data according to the reconstruction algorithm in
// the parquet-format VariantShredding.md specification, appending the result to `builder`.
Status Rebuild(const Cursor& cursor, std::string_view metadata, const VariantSchema& schema,
               const std::shared_ptr<MemoryPool>& pool, VariantBuilder* builder) {
    int32_t typed_idx = schema.typed_idx;
    int32_t variant_idx = schema.variant_idx;
    if (typed_idx >= 0 && !cursor.IsNullAt(typed_idx)) {
        if (schema.scalar_schema.has_value()) {
            return RebuildTypedScalar(cursor, schema, builder);
        } else if (schema.array_schema != nullptr) {
            const auto& list_array =
                static_cast<const arrow::ListArray&>(*cursor.array->field(typed_idx));
            const auto& element_array =
                static_cast<const arrow::StructArray&>(*list_array.values());
            int64_t element_start = list_array.value_offset(cursor.row);
            int64_t element_end = list_array.value_offset(cursor.row + 1);
            int32_t start = builder->GetWritePos();
            std::vector<int32_t> offsets;
            offsets.reserve(element_end - element_start);
            for (int64_t i = element_start; i < element_end; ++i) {
                offsets.push_back(builder->GetWritePos() - start);
                PAIMON_RETURN_NOT_OK(Rebuild(Cursor{&element_array, i}, metadata,
                                             *schema.array_schema, pool, builder));
            }
            return builder->FinishWritingArray(start, offsets);
        } else {
            const auto& object_array =
                static_cast<const arrow::StructArray&>(*cursor.array->field(typed_idx));
            Cursor object_cursor{&object_array, cursor.row};
            std::vector<VariantBuilder::FieldEntry> fields;
            int32_t start = builder->GetWritePos();
            for (size_t field_idx = 0; field_idx < schema.object_schema.size(); ++field_idx) {
                // Shredded fields must not be null.
                if (object_cursor.IsNullAt(static_cast<int32_t>(field_idx))) {
                    return VariantBinaryUtil::MalformedVariant();
                }
                const std::string& field_name = schema.object_schema[field_idx].name;
                const VariantSchema& field_schema = *schema.object_schema[field_idx].schema;
                const auto& field_array = static_cast<const arrow::StructArray&>(
                    *object_array.field(static_cast<int32_t>(field_idx)));
                Cursor field_cursor{&field_array, cursor.row};
                // If the field doesn't have a non-null `typed_value` or `value`, it is missing.
                if ((field_schema.typed_idx >= 0 &&
                     !field_cursor.IsNullAt(field_schema.typed_idx)) ||
                    (field_schema.variant_idx >= 0 &&
                     !field_cursor.IsNullAt(field_schema.variant_idx))) {
                    int32_t id = builder->AddKey(field_name);
                    fields.emplace_back(field_name, id, builder->GetWritePos() - start);
                    PAIMON_RETURN_NOT_OK(
                        Rebuild(field_cursor, metadata, field_schema, pool, builder));
                }
            }
            if (variant_idx >= 0 && !cursor.IsNullAt(variant_idx)) {
                // Add the leftover fields in the variant binary.
                PAIMON_ASSIGN_OR_RAISE(
                    std::shared_ptr<GenericVariant> leftover,
                    GenericVariant::Create(cursor.GetBinary(variant_idx), metadata, pool));
                PAIMON_ASSIGN_OR_RAISE(VariantValueType leftover_type, leftover->GetType());
                if (leftover_type != VariantValueType::kObject) {
                    return VariantBinaryUtil::MalformedVariant();
                }
                PAIMON_ASSIGN_OR_RAISE(int32_t leftover_size, leftover->ObjectSize());
                for (int32_t i = 0; i < leftover_size; ++i) {
                    PAIMON_ASSIGN_OR_RAISE(auto field, leftover->GetFieldAtIndex(i));
                    if (!field.has_value()) {
                        return VariantBinaryUtil::MalformedVariant();
                    }
                    // `value` must not contain any shredded field.
                    if (schema.object_schema_map.count(field->key) > 0) {
                        return VariantBinaryUtil::MalformedVariant();
                    }
                    int32_t id = builder->AddKey(field->key);
                    fields.emplace_back(field->key, id, builder->GetWritePos() - start);
                    PAIMON_RETURN_NOT_OK(builder->AppendVariant(*field->value));
                }
            }
            return builder->FinishWritingObject(start, &fields);
        }
    } else if (variant_idx >= 0 && !cursor.IsNullAt(variant_idx)) {
        // `typed_value` doesn't exist or is null. Read from `value`.
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<GenericVariant> variant,
            GenericVariant::Create(cursor.GetBinary(variant_idx), metadata, pool));
        return builder->AppendVariant(*variant);
    } else {
        // The variant is missing in a context where it must be present; the data is invalid.
        return VariantBinaryUtil::MalformedVariant();
    }
}

}  // namespace

Status VariantReassembler::RebuildValue(const arrow::StructArray& shredded, int64_t row,
                                        std::string_view metadata, const VariantSchema& schema,
                                        const std::shared_ptr<MemoryPool>& pool,
                                        VariantBuilder* builder) {
    return Rebuild(Cursor{&shredded, row}, metadata, schema, pool, builder);
}

Result<std::shared_ptr<arrow::Array>> VariantReassembler::AssembleVariantArray(
    const std::shared_ptr<arrow::StructArray>& shredded,
    const std::shared_ptr<VariantSchema>& schema, arrow::MemoryPool* pool) {
    if (schema->top_level_metadata_idx < 0) {
        return VariantBinaryUtil::MalformedVariant();
    }
    std::shared_ptr<MemoryPool> paimon_pool = GetDefaultPool();
    auto output_type = VariantTypeUtils::UnshreddedStructType();
    std::unique_ptr<arrow::ArrayBuilder> output_builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::MakeBuilder(pool, output_type, &output_builder));
    auto* struct_builder = static_cast<arrow::StructBuilder*>(output_builder.get());
    auto* value_builder = static_cast<arrow::BinaryBuilder*>(struct_builder->field_builder(0));
    auto* metadata_builder = static_cast<arrow::BinaryBuilder*>(struct_builder->field_builder(1));

    bool unshredded = schema->IsUnshredded();
    for (int64_t row = 0; row < shredded->length(); ++row) {
        if (shredded->IsNull(row)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder->AppendNull());
            continue;
        }
        Cursor cursor{shredded.get(), row};
        if (cursor.IsNullAt(schema->top_level_metadata_idx)) {
            return VariantBinaryUtil::MalformedVariant();
        }
        std::string_view metadata = cursor.GetBinary(schema->top_level_metadata_idx);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder->Append());
        if (unshredded) {
            // Rebuilding is unnecessary for unshredded variants.
            if (cursor.IsNullAt(schema->variant_idx)) {
                return VariantBinaryUtil::MalformedVariant();
            }
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                value_builder->Append(cursor.GetBinary(schema->variant_idx)));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(metadata_builder->Append(metadata));
        } else {
            VariantBuilder builder(/*allow_duplicate_keys=*/false);
            PAIMON_RETURN_NOT_OK(Rebuild(cursor, metadata, *schema, paimon_pool, &builder));
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GenericVariant> variant,
                                   builder.Build(paimon_pool));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(value_builder->Append(variant->RawValue()));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(metadata_builder->Append(variant->Metadata()));
        }
    }
    std::shared_ptr<arrow::Array> result;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(output_builder->Finish(&result));
    return result;
}

}  // namespace paimon
