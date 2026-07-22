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

#include "paimon/common/data/variant/infer_variant_shredding_schema.h"

#include <cmath>
#include <string>
#include <utility>

#include "arrow/api.h"
#include "paimon/common/data/variant/variant_binary_util.h"
#include "paimon/common/data/variant/variant_defs.h"

namespace paimon {

namespace {

constexpr int32_t kMaxRowFieldSize = 1000;

// The inference type lattice. A scalar node holds an arrow type (`arrow::null()` is the untyped
// VARIANT sentinel); object nodes track per-field occurrence counts so that rare fields can be
// dropped in the final schema.
struct SimpleSchema {
    struct Field {
        std::string name;
        std::shared_ptr<SimpleSchema> schema;
        int64_t count;
    };

    bool is_object = false;
    bool is_array = false;
    std::vector<Field> fields;
    std::shared_ptr<SimpleSchema> element;
    std::shared_ptr<arrow::DataType> scalar;

    static std::shared_ptr<SimpleSchema> Variant() {
        auto schema = std::make_shared<SimpleSchema>();
        schema->scalar = arrow::null();
        return schema;
    }

    static std::shared_ptr<SimpleSchema> Scalar(std::shared_ptr<arrow::DataType> type) {
        auto schema = std::make_shared<SimpleSchema>();
        schema->scalar = std::move(type);
        return schema;
    }
};

std::shared_ptr<SimpleSchema> MergeSchema(const std::shared_ptr<SimpleSchema>& s1,
                                          const std::shared_ptr<SimpleSchema>& s2);

// Merges two decimals with possibly different scales.
std::shared_ptr<SimpleSchema> MergeDecimal(const arrow::Decimal128Type& d1,
                                           const arrow::Decimal128Type& d2) {
    int32_t scale = std::max(d1.scale(), d2.scale());
    int32_t range = std::max(d1.precision() - d1.scale(), d2.precision() - d2.scale());
    if (range + scale > VariantDefs::kMaxDecimal16Precision) {
        // Decimal cannot support precision > 38.
        return SimpleSchema::Variant();
    }
    return SimpleSchema::Scalar(arrow::decimal128(range + scale, scale));
}

std::shared_ptr<SimpleSchema> MergeDecimalWithLong(const arrow::Decimal128Type& d) {
    if (d.scale() == 0 && d.precision() <= 18) {
        return SimpleSchema::Scalar(arrow::int64());
    }
    // A long can always fit in a decimal(19, 0).
    auto long_decimal = std::static_pointer_cast<arrow::Decimal128Type>(arrow::decimal128(19, 0));
    return MergeDecimal(d, *long_decimal);
}

std::shared_ptr<SimpleSchema> MergeObjects(const std::shared_ptr<SimpleSchema>& s1,
                                           const std::shared_ptr<SimpleSchema>& s2) {
    auto result = std::make_shared<SimpleSchema>();
    result->is_object = true;
    size_t f1_idx = 0;
    size_t f2_idx = 0;
    while (f1_idx < s1->fields.size() && f2_idx < s2->fields.size() &&
           result->fields.size() < kMaxRowFieldSize) {
        const auto& field1 = s1->fields[f1_idx];
        const auto& field2 = s2->fields[f2_idx];
        int32_t comp = field1.name.compare(field2.name);
        if (comp == 0) {
            result->fields.push_back(SimpleSchema::Field{field1.name,
                                                         MergeSchema(field1.schema, field2.schema),
                                                         field1.count + field2.count});
            ++f1_idx;
            ++f2_idx;
        } else if (comp < 0) {
            result->fields.push_back(field1);
            ++f1_idx;
        } else {
            result->fields.push_back(field2);
            ++f2_idx;
        }
    }
    while (f1_idx < s1->fields.size() && result->fields.size() < kMaxRowFieldSize) {
        result->fields.push_back(s1->fields[f1_idx++]);
    }
    while (f2_idx < s2->fields.size() && result->fields.size() < kMaxRowFieldSize) {
        result->fields.push_back(s2->fields[f2_idx++]);
    }
    return result;
}

std::shared_ptr<SimpleSchema> MergeSchema(const std::shared_ptr<SimpleSchema>& s1,
                                          const std::shared_ptr<SimpleSchema>& s2) {
    // Allow null (missing) to merge into any typed schema.
    if (s1 == nullptr) {
        return s2;
    }
    if (s2 == nullptr) {
        return s1;
    }
    if (s1->is_object && s2->is_object) {
        return MergeObjects(s1, s2);
    }
    if (s1->is_array && s2->is_array) {
        auto result = std::make_shared<SimpleSchema>();
        result->is_array = true;
        result->element = MergeSchema(s1->element, s2->element);
        if (result->element == nullptr) {
            result->element = SimpleSchema::Variant();
        }
        return result;
    }
    if (s1->scalar != nullptr && s2->scalar != nullptr) {
        bool s1_decimal = s1->scalar->id() == arrow::Type::DECIMAL128;
        bool s2_decimal = s2->scalar->id() == arrow::Type::DECIMAL128;
        bool s1_long = s1->scalar->id() == arrow::Type::INT64;
        bool s2_long = s2->scalar->id() == arrow::Type::INT64;
        if (s1_decimal && s2_decimal) {
            return MergeDecimal(static_cast<const arrow::Decimal128Type&>(*s1->scalar),
                                static_cast<const arrow::Decimal128Type&>(*s2->scalar));
        }
        if (s1_decimal && s2_long) {
            return MergeDecimalWithLong(static_cast<const arrow::Decimal128Type&>(*s1->scalar));
        }
        if (s1_long && s2_decimal) {
            return MergeDecimalWithLong(static_cast<const arrow::Decimal128Type&>(*s2->scalar));
        }
        if (s1->scalar->Equals(*s2->scalar)) {
            return s1;
        }
    }
    return SimpleSchema::Variant();
}

// Returns an appropriate schema for shredding a variant value. Unlike a generic schema-of
// expression, the merged types stay consistent with what shredding allows (e.g. an integer and a
// double merge to VARIANT, not double).
Result<std::shared_ptr<SimpleSchema>> SchemaOf(const GenericVariant& variant, int32_t max_depth) {
    PAIMON_ASSIGN_OR_RAISE(VariantValueType type, variant.GetType());
    switch (type) {
        case VariantValueType::kObject: {
            if (max_depth <= 0) {
                return SimpleSchema::Variant();
            }
            PAIMON_ASSIGN_OR_RAISE(int32_t size, variant.ObjectSize());
            auto result = std::make_shared<SimpleSchema>();
            result->is_object = true;
            result->fields.reserve(size);
            for (int32_t i = 0; i < size; ++i) {
                PAIMON_ASSIGN_OR_RAISE(std::optional<GenericVariant::ObjectField> field,
                                       variant.GetFieldAtIndex(i));
                if (!field.has_value()) {
                    return VariantBinaryUtil::MalformedVariant("an object field is missing");
                }
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SimpleSchema> field_schema,
                                       SchemaOf(*field->value, max_depth - 1));
                if (field_schema == nullptr) {
                    field_schema = SimpleSchema::Variant();
                }
                result->fields.push_back(SimpleSchema::Field{field->key, field_schema, 1});
            }
            // According to the variant spec, object fields must be sorted alphabetically.
            for (size_t i = 1; i < result->fields.size(); ++i) {
                if (result->fields[i - 1].name.compare(result->fields[i].name) >= 0) {
                    return Status::Invalid("Variant object fields must be sorted alphabetically");
                }
            }
            return result;
        }
        case VariantValueType::kArray: {
            if (max_depth <= 0) {
                return SimpleSchema::Variant();
            }
            PAIMON_ASSIGN_OR_RAISE(int32_t size, variant.ArraySize());
            std::shared_ptr<SimpleSchema> element_type;
            for (int32_t i = 0; i < size; ++i) {
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GenericVariant> element,
                                       variant.GetElementAtIndex(i));
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SimpleSchema> element_schema,
                                       SchemaOf(*element, max_depth - 1));
                element_type = MergeSchema(element_type, element_schema);
            }
            auto result = std::make_shared<SimpleSchema>();
            result->is_array = true;
            result->element = element_type == nullptr ? SimpleSchema::Variant() : element_type;
            return result;
        }
        case VariantValueType::kNull:
            return std::shared_ptr<SimpleSchema>(nullptr);
        case VariantValueType::kBoolean:
            return SimpleSchema::Scalar(arrow::boolean());
        case VariantValueType::kLong: {
            // Compute the smallest decimal that can contain this value.
            PAIMON_ASSIGN_OR_RAISE(int64_t value, variant.GetLong());
            VariantDecimal decimal{value, 0};
            int32_t precision = decimal.Precision();
            if (precision <= 18) {
                return SimpleSchema::Scalar(arrow::decimal128(precision, 0));
            }
            return SimpleSchema::Scalar(arrow::int64());
        }
        case VariantValueType::kString:
            return SimpleSchema::Scalar(arrow::utf8());
        case VariantValueType::kDouble:
            return SimpleSchema::Scalar(arrow::float64());
        case VariantValueType::kDecimal: {
            PAIMON_ASSIGN_OR_RAISE(VariantDecimal decimal, variant.GetDecimal());
            if (decimal.scale < 0) {
                // GetDecimal strips trailing zeros and can return a negative scale (100.00 ->
                // 1E+2), which neither the shredded parquet type nor reassembly's AppendDecimal
                // can represent; scale the value back up. Bounded by construction: the encoded
                // decimal had at most 38 digits before the point.
                for (; decimal.scale < 0; ++decimal.scale) {
                    decimal.unscaled *= 10;
                }
            }
            int32_t precision = decimal.Precision();
            int32_t scale = decimal.scale;
            // Ensure precision is at least scale (and at least 1) to be valid.
            if (precision < scale) {
                precision = scale;
            }
            if (precision == 0) {
                precision = 1;
            }
            return SimpleSchema::Scalar(arrow::decimal128(precision, scale));
        }
        case VariantValueType::kDate:
        case VariantValueType::kTimestamp:
        case VariantValueType::kTimestampNtz:
            // The shredding schema builder rejects temporal leaf types (as the Java one does),
            // so inferring them would abort the whole write; keep such values in the untyped
            // column instead.
            return SimpleSchema::Scalar(arrow::null());
        case VariantValueType::kFloat:
            return SimpleSchema::Scalar(arrow::float32());
        case VariantValueType::kBinary:
            return SimpleSchema::Scalar(arrow::binary());
        default:
            return SimpleSchema::Variant();
    }
}

// Finalizes the inferred schema: 1) widen integer types to int64, 2) replace empty objects with
// VARIANT, 3) limit the total number of shredded fields in the schema.
std::shared_ptr<arrow::DataType> FinalizeSimpleSchema(
    const std::shared_ptr<SimpleSchema>& schema, int64_t min_cardinality,
    InferVariantShreddingSchema::MaxFields* max_fields) {
    // Every field uses a value column.
    --max_fields->remaining;
    if (max_fields->remaining <= 0) {
        return arrow::null();
    }
    if (schema == nullptr ||
        (schema->scalar != nullptr && schema->scalar->id() == arrow::Type::NA)) {
        return arrow::null();
    }
    if (schema->is_object) {
        arrow::FieldVector new_fields;
        for (const auto& field : schema->fields) {
            if (field.count >= min_cardinality && max_fields->remaining > 0) {
                auto new_type = FinalizeSimpleSchema(field.schema, min_cardinality, max_fields);
                new_fields.push_back(arrow::field(field.name, new_type));
            }
        }
        if (!new_fields.empty()) {
            return arrow::struct_(new_fields);
        }
        return arrow::null();
    }
    if (schema->is_array) {
        auto new_element = FinalizeSimpleSchema(schema->element, min_cardinality, max_fields);
        return arrow::list(new_element);
    }
    switch (schema->scalar->id()) {
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::INT64:
            --max_fields->remaining;
            return arrow::int64();
        case arrow::Type::DECIMAL128: {
            const auto& decimal_type = static_cast<const arrow::Decimal128Type&>(*schema->scalar);
            --max_fields->remaining;
            if (decimal_type.precision() <= 18 && decimal_type.scale() == 0) {
                return arrow::int64();
            }
            if (decimal_type.precision() <= 18) {
                return arrow::decimal128(18, decimal_type.scale());
            }
            return arrow::decimal128(VariantDefs::kMaxDecimal16Precision, decimal_type.scale());
        }
        default:
            // All other scalar types use typed_value.
            --max_fields->remaining;
            return schema->scalar;
    }
}

}  // namespace

Result<std::shared_ptr<arrow::DataType>> InferVariantShreddingSchema::InferColumnShreddingType(
    const std::vector<std::shared_ptr<GenericVariant>>& samples, MaxFields* max_fields) const {
    int64_t num_non_null_values = 0;
    std::shared_ptr<SimpleSchema> simple_schema;
    for (const auto& sample : samples) {
        if (sample == nullptr) {
            continue;
        }
        ++num_non_null_values;
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SimpleSchema> row_schema,
                               SchemaOf(*sample, max_schema_depth_));
        simple_schema = MergeSchema(simple_schema, row_schema);
    }
    // Don't infer a schema for fields that appear in less than min_field_cardinality_ratio of
    // the rows.
    auto min_cardinality = static_cast<int64_t>(
        std::ceil(static_cast<double>(num_non_null_values) * min_field_cardinality_ratio_));
    std::shared_ptr<arrow::DataType> finalized =
        FinalizeSimpleSchema(simple_schema, min_cardinality, max_fields);
    if (finalized->id() == arrow::Type::NA) {
        // The whole column stays unshredded.
        return std::shared_ptr<arrow::DataType>(nullptr);
    }
    return finalized;
}

}  // namespace paimon
