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

#include <algorithm>
#include <cmath>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

#include "arrow/api.h"
#include "arrow/util/checked_cast.h"
#include "paimon/common/data/variant/variant_binary_util.h"
#include "paimon/common/data/variant/variant_defs.h"
#include "paimon/common/data/variant/variant_shredding_write_plan.h"
#include "paimon/common/data/variant/variant_type_utils.h"

namespace paimon {

constexpr int32_t kMaxRowFieldSize = 1000;

// The inference type lattice. A scalar node holds an arrow type (`arrow::null()` is the untyped
// VARIANT sentinel); object nodes track per-field occurrence counts so that rare fields can be
// dropped in the final schema.
struct InferVariantShreddingSchema::SimpleSchema {
    struct Field {
        std::string name;
        std::shared_ptr<SimpleSchema> schema;
        double count;
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

namespace {

using SimpleSchema = InferVariantShreddingSchema::SimpleSchema;

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
    const std::shared_ptr<SimpleSchema>& schema, double min_cardinality,
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

std::shared_ptr<SimpleSchema> ScaleFieldCounts(const std::shared_ptr<SimpleSchema>& schema,
                                               double scale) {
    if (schema == nullptr) {
        return nullptr;
    }
    auto result = std::make_shared<SimpleSchema>(*schema);
    if (schema->is_object) {
        result->fields.clear();
        result->fields.reserve(schema->fields.size());
        for (const auto& field : schema->fields) {
            result->fields.push_back(SimpleSchema::Field{
                field.name, ScaleFieldCounts(field.schema, scale), field.count * scale});
        }
    } else if (schema->is_array) {
        result->element = ScaleFieldCounts(schema->element, scale);
    }
    return result;
}

InferVariantShreddingSchema::ColumnEvidence ScaleToAtMost(
    const InferVariantShreddingSchema::ColumnEvidence& evidence, int32_t max_root_value_count) {
    if (evidence.root_value_count <= 0 ||
        evidence.root_value_count <= static_cast<double>(max_root_value_count)) {
        return evidence;
    }
    double scale = static_cast<double>(max_root_value_count) / evidence.root_value_count;
    return InferVariantShreddingSchema::ColumnEvidence{
        static_cast<double>(max_root_value_count),
        ScaleFieldCounts(evidence.observed_schema, scale)};
}

const SimpleSchema::Field* FindSimpleField(const std::shared_ptr<SimpleSchema>& schema,
                                           const std::string& name) {
    if (schema == nullptr || !schema->is_object) {
        return nullptr;
    }
    auto it = std::lower_bound(schema->fields.begin(), schema->fields.end(), name,
                               [](const SimpleSchema::Field& field, const std::string& target) {
                                   return field.name < target;
                               });
    return it != schema->fields.end() && it->name == name ? &*it : nullptr;
}

std::shared_ptr<arrow::Field> FindArrowField(const std::shared_ptr<arrow::DataType>& type,
                                             const std::string& name) {
    if (type == nullptr || type->id() != arrow::Type::STRUCT) {
        return nullptr;
    }
    return std::static_pointer_cast<arrow::StructType>(type)->GetFieldByName(name);
}

bool IsUntyped(const std::shared_ptr<SimpleSchema>& schema) {
    return schema == nullptr ||
           (schema->scalar != nullptr && schema->scalar->id() == arrow::Type::NA);
}

std::shared_ptr<arrow::DataType> MergeArrowScalars(const std::shared_ptr<arrow::DataType>& first,
                                                   const std::shared_ptr<arrow::DataType>& second) {
    if (first == nullptr) {
        return second;
    }
    if (second == nullptr) {
        return first;
    }
    bool first_decimal = first->id() == arrow::Type::DECIMAL128;
    bool second_decimal = second->id() == arrow::Type::DECIMAL128;
    if (first_decimal && second_decimal) {
        auto merged = MergeDecimal(static_cast<const arrow::Decimal128Type&>(*first),
                                   static_cast<const arrow::Decimal128Type&>(*second));
        return merged->scalar;
    }
    if (first_decimal && second->id() == arrow::Type::INT64) {
        return MergeDecimalWithLong(static_cast<const arrow::Decimal128Type&>(*first))->scalar;
    }
    if (first->id() == arrow::Type::INT64 && second_decimal) {
        return MergeDecimalWithLong(static_cast<const arrow::Decimal128Type&>(*second))->scalar;
    }
    return first->Equals(*second) ? first : arrow::null();
}

std::shared_ptr<arrow::DataType> WidenScalar(const std::shared_ptr<arrow::DataType>& type) {
    if (type == nullptr) {
        return arrow::null();
    }
    if (type->id() == arrow::Type::DECIMAL128) {
        const auto& decimal = static_cast<const arrow::Decimal128Type&>(*type);
        if (decimal.precision() <= 18 && decimal.scale() == 0) {
            return arrow::int64();
        }
        return arrow::decimal128(
            decimal.precision() <= 18 ? 18 : VariantDefs::kMaxDecimal16Precision, decimal.scale());
    }
    return type;
}

bool CompatibleTypeFamilies(const std::shared_ptr<arrow::DataType>& previous,
                            const std::shared_ptr<SimpleSchema>& current) {
    if (previous == nullptr || current == nullptr) {
        return true;
    }
    if (previous->id() == arrow::Type::STRUCT || current->is_object) {
        return previous->id() == arrow::Type::STRUCT && current->is_object;
    }
    if (previous->id() == arrow::Type::LIST || current->is_array) {
        return previous->id() == arrow::Type::LIST && current->is_array;
    }
    if (current->scalar == nullptr) {
        return false;
    }
    return MergeArrowScalars(previous, current->scalar)->id() != arrow::Type::NA;
}

std::shared_ptr<SimpleSchema> SelectedSchemaToSimpleSchema(
    const std::shared_ptr<arrow::DataType>& selected, double field_count) {
    if (selected == nullptr || selected->id() == arrow::Type::NA) {
        return SimpleSchema::Variant();
    }
    if (selected->id() == arrow::Type::STRUCT) {
        auto result = std::make_shared<SimpleSchema>();
        result->is_object = true;
        for (const std::shared_ptr<arrow::Field>& field : selected->fields()) {
            result->fields.push_back(SimpleSchema::Field{
                field->name(), SelectedSchemaToSimpleSchema(field->type(), field_count),
                field_count});
        }
        return result;
    }
    if (selected->id() == arrow::Type::LIST) {
        auto result = std::make_shared<SimpleSchema>();
        result->is_array = true;
        result->element = SelectedSchemaToSimpleSchema(
            std::static_pointer_cast<arrow::ListType>(selected)->value_type(), field_count);
        return result;
    }
    return SimpleSchema::Scalar(selected);
}

std::shared_ptr<arrow::DataType> FinalizeAdaptiveSchema(
    std::shared_ptr<SimpleSchema> combined, const std::shared_ptr<SimpleSchema>& current,
    std::shared_ptr<arrow::DataType> previous_selected, double root_value_count,
    double admission_ratio, double retention_ratio,
    InferVariantShreddingSchema::MaxFields* max_fields) {
    --max_fields->remaining;
    if (max_fields->remaining <= 0) {
        return arrow::null();
    }

    if (current != nullptr && previous_selected != nullptr &&
        !CompatibleTypeFamilies(previous_selected, current)) {
        combined = current;
        previous_selected = nullptr;
    }
    if (IsUntyped(combined)) {
        if (!IsUntyped(current)) {
            combined = current;
        } else if (previous_selected != nullptr) {
            // Match Java's `combined = previousSelected`: retain the previous selection as the
            // input to the remaining recursive width-budget and field-ordering logic.
            combined = SelectedSchemaToSimpleSchema(previous_selected, root_value_count);
        } else {
            return arrow::null();
        }
    }

    if (combined->is_object) {
        struct Candidate {
            const SimpleSchema::Field* field;
            double ratio;
            bool is_new;
        };
        std::vector<Candidate> candidates;
        for (const auto& field : combined->fields) {
            bool is_new = FindArrowField(previous_selected, field.name) == nullptr;
            double threshold = is_new ? admission_ratio : retention_ratio;
            double ratio = root_value_count == 0 ? 0 : field.count / root_value_count;
            if (ratio >= threshold) {
                candidates.push_back(Candidate{&field, ratio, is_new});
            }
        }
        std::sort(candidates.begin(), candidates.end(),
                  [](const Candidate& left, const Candidate& right) {
                      if (left.ratio != right.ratio) {
                          return left.ratio > right.ratio;
                      }
                      if (left.is_new != right.is_new) {
                          return !left.is_new;
                      }
                      return left.field->name < right.field->name;
                  });

        arrow::FieldVector selected;
        for (const Candidate& candidate : candidates) {
            if (max_fields->remaining <= 0) {
                break;
            }
            const SimpleSchema::Field* current_field =
                FindSimpleField(current, candidate.field->name);
            std::shared_ptr<arrow::Field> previous_field =
                FindArrowField(previous_selected, candidate.field->name);
            std::shared_ptr<arrow::DataType> selected_type = FinalizeAdaptiveSchema(
                candidate.field->schema, current_field == nullptr ? nullptr : current_field->schema,
                previous_field == nullptr ? nullptr : previous_field->type(), root_value_count,
                admission_ratio, retention_ratio, max_fields);
            selected.push_back(arrow::field(candidate.field->name, selected_type));
        }
        std::sort(selected.begin(), selected.end(),
                  [](const std::shared_ptr<arrow::Field>& left,
                     const std::shared_ptr<arrow::Field>& right) {
                      return left->name() < right->name();
                  });
        return selected.empty() ? arrow::null() : arrow::struct_(selected);
    }

    if (combined->is_array) {
        std::shared_ptr<SimpleSchema> current_element =
            current != nullptr && current->is_array ? current->element : nullptr;
        std::shared_ptr<arrow::DataType> previous_element;
        if (previous_selected != nullptr && previous_selected->id() == arrow::Type::LIST) {
            previous_element =
                std::static_pointer_cast<arrow::ListType>(previous_selected)->value_type();
        }
        return arrow::list(FinalizeAdaptiveSchema(combined->element, current_element,
                                                  previous_element, root_value_count,
                                                  admission_ratio, retention_ratio, max_fields));
    }

    --max_fields->remaining;
    std::shared_ptr<arrow::DataType> current_scalar =
        current == nullptr ? nullptr : current->scalar;
    if (current_scalar == nullptr) {
        return previous_selected == nullptr ? WidenScalar(combined->scalar) : previous_selected;
    }
    if (previous_selected == nullptr) {
        return WidenScalar(current_scalar);
    }
    std::shared_ptr<arrow::DataType> merged = MergeArrowScalars(previous_selected, current_scalar);
    return merged->id() == arrow::Type::NA ? WidenScalar(current_scalar) : merged;
}

}  // namespace

InferVariantShreddingSchema::InferVariantShreddingSchema(
    const std::shared_ptr<arrow::Schema>& logical_schema, const std::shared_ptr<MemoryPool>& pool,
    int32_t max_schema_width, int32_t max_schema_depth, double min_field_cardinality_ratio)
    : logical_schema_(logical_schema),
      pool_(pool),
      max_schema_width_(max_schema_width),
      max_schema_depth_(max_schema_depth),
      min_field_cardinality_ratio_(min_field_cardinality_ratio) {
    Path current;
    CollectVariantPaths(logical_schema_->fields(), &current, &paths_to_variant_);
}

void InferVariantShreddingSchema::CollectVariantPaths(
    const std::vector<std::shared_ptr<arrow::Field>>& fields, Path* current,
    std::vector<Path>* paths) {
    for (int32_t i = 0; i < static_cast<int32_t>(fields.size()); ++i) {
        const std::shared_ptr<arrow::Field>& field = fields[i];
        current->push_back(i);
        if (VariantTypeUtils::IsVariantField(field)) {
            paths->push_back(*current);
        } else if (field->type()->id() == arrow::Type::STRUCT) {
            CollectVariantPaths(field->type()->fields(), current, paths);
        }
        current->pop_back();
    }
}

Result<std::vector<std::shared_ptr<GenericVariant>>>
InferVariantShreddingSchema::CollectSamplesAtPath(const SampleBatches& sample_batches,
                                                  const Path& path) const {
    std::vector<std::shared_ptr<GenericVariant>> samples;
    for (const std::shared_ptr<arrow::Array>& sample_batch : sample_batches) {
        std::shared_ptr<arrow::Array> column = sample_batch;
        // Child slots below a null struct row are unspecified in Arrow and must not be decoded.
        std::vector<std::shared_ptr<arrow::Array>> ancestors;
        for (int32_t index : path) {
            if (column == nullptr || column->type_id() != arrow::Type::STRUCT) {
                return Status::Invalid("sample batch does not match the variant column path");
            }
            if (column != sample_batch) {
                ancestors.push_back(column);
            }
            column = arrow::internal::checked_cast<const arrow::StructArray&>(*column).field(index);
        }
        if (column == nullptr) {
            return Status::Invalid("sample batch misses the planned variant column");
        }
        const auto& variant_array =
            arrow::internal::checked_cast<const arrow::StructArray&>(*column);
        const auto& value_array =
            arrow::internal::checked_cast<const arrow::BinaryArray&>(*variant_array.field(0));
        const auto& metadata_array =
            arrow::internal::checked_cast<const arrow::BinaryArray&>(*variant_array.field(1));
        for (int64_t row = 0; row < variant_array.length(); ++row) {
            bool row_is_null = variant_array.IsNull(row);
            for (const std::shared_ptr<arrow::Array>& ancestor : ancestors) {
                row_is_null = row_is_null || ancestor->IsNull(row);
            }
            if (row_is_null) {
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<GenericVariant> variant,
                GenericVariant::Create(std::string_view(value_array.GetView(row)),
                                       std::string_view(metadata_array.GetView(row)), pool_));
            samples.push_back(std::move(variant));
        }
    }
    return samples;
}

Result<std::shared_ptr<arrow::Schema>> InferVariantShreddingSchema::CreatePhysicalSchema(
    const SelectedSchemas& selected_schemas) const {
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<VariantShreddingWritePlan> plan,
        VariantShreddingWritePlan::CreateFromPaths(logical_schema_, selected_schemas));
    return plan->PhysicalSchema();
}

Result<std::shared_ptr<arrow::Schema>> InferVariantShreddingSchema::InferSchema(
    const SampleBatches& samples) const {
    PAIMON_ASSIGN_OR_RAISE(AdaptiveInferenceResult result,
                           InferInitial(samples, std::numeric_limits<int32_t>::max()));
    return result.physical_schema;
}

Result<InferVariantShreddingSchema::AdaptiveInferenceResult>
InferVariantShreddingSchema::InferInitial(const SampleBatches& samples,
                                          int32_t effective_sample_size) const {
    MaxFields max_fields = CreateMaxFieldsBudget();
    InferenceEvidence evidence;
    SelectedSchemas selected_schemas;
    for (const Path& path : paths_to_variant_) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<GenericVariant>> column_samples,
                               CollectSamplesAtPath(samples, path));
        PAIMON_ASSIGN_OR_RAISE(
            AdaptiveColumnResult result,
            InferInitialColumn(column_samples, effective_sample_size, &max_fields));
        evidence.columns.emplace(path, std::move(result.evidence));
        selected_schemas.emplace(path, std::move(result.selected_schema));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> physical_schema,
                           CreatePhysicalSchema(selected_schemas));
    return AdaptiveInferenceResult{std::move(physical_schema), std::move(evidence),
                                   std::move(selected_schemas)};
}

Result<InferVariantShreddingSchema::AdaptiveInferenceResult>
InferVariantShreddingSchema::InferAdaptive(const InferenceEvidence& previous_evidence,
                                           const SelectedSchemas& previous_selected_schemas,
                                           const SampleBatches& samples,
                                           int32_t effective_sample_size, double admission_ratio,
                                           double retention_ratio) const {
    MaxFields max_fields = CreateMaxFieldsBudget();
    InferenceEvidence evidence;
    SelectedSchemas selected_schemas;
    for (const Path& path : paths_to_variant_) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<GenericVariant>> column_samples,
                               CollectSamplesAtPath(samples, path));
        auto evidence_it = previous_evidence.columns.find(path);
        ColumnEvidence previous_column;
        if (evidence_it != previous_evidence.columns.end()) {
            previous_column = evidence_it->second;
        }
        auto selected_it = previous_selected_schemas.find(path);
        std::shared_ptr<arrow::DataType> previous_selected =
            selected_it == previous_selected_schemas.end() ? nullptr : selected_it->second;
        PAIMON_ASSIGN_OR_RAISE(AdaptiveColumnResult result,
                               InferAdaptiveColumn(previous_column, previous_selected,
                                                   column_samples, effective_sample_size,
                                                   admission_ratio, retention_ratio, &max_fields));
        evidence.columns.emplace(path, std::move(result.evidence));
        selected_schemas.emplace(path, std::move(result.selected_schema));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> physical_schema,
                           CreatePhysicalSchema(selected_schemas));
    return AdaptiveInferenceResult{std::move(physical_schema), std::move(evidence),
                                   std::move(selected_schemas)};
}

Result<std::shared_ptr<arrow::DataType>> InferVariantShreddingSchema::InferColumnShreddingType(
    const std::vector<std::shared_ptr<GenericVariant>>& samples, MaxFields* max_fields) const {
    PAIMON_ASSIGN_OR_RAISE(ColumnEvidence evidence, AnalyzeColumn(samples));
    double min_cardinality = std::ceil(evidence.root_value_count * min_field_cardinality_ratio_);
    std::shared_ptr<arrow::DataType> finalized =
        FinalizeSimpleSchema(evidence.observed_schema, min_cardinality, max_fields);
    if (finalized->id() == arrow::Type::NA) {
        return std::shared_ptr<arrow::DataType>(nullptr);
    }
    return finalized;
}

Result<InferVariantShreddingSchema::ColumnEvidence> InferVariantShreddingSchema::AnalyzeColumn(
    const std::vector<std::shared_ptr<GenericVariant>>& samples) const {
    ColumnEvidence evidence;
    for (const auto& sample : samples) {
        if (sample == nullptr) {
            continue;
        }
        ++evidence.root_value_count;
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SimpleSchema> row_schema,
                               SchemaOf(*sample, max_schema_depth_));
        evidence.observed_schema = MergeSchema(evidence.observed_schema, row_schema);
    }
    return evidence;
}

Result<InferVariantShreddingSchema::AdaptiveColumnResult>
InferVariantShreddingSchema::InferInitialColumn(
    const std::vector<std::shared_ptr<GenericVariant>>& samples, int32_t effective_sample_size,
    MaxFields* max_fields) const {
    PAIMON_ASSIGN_OR_RAISE(ColumnEvidence evidence, AnalyzeColumn(samples));
    double min_cardinality = std::ceil(evidence.root_value_count * min_field_cardinality_ratio_);
    std::shared_ptr<arrow::DataType> finalized =
        FinalizeSimpleSchema(evidence.observed_schema, min_cardinality, max_fields);
    return AdaptiveColumnResult{ScaleToAtMost(evidence, effective_sample_size), finalized};
}

Result<InferVariantShreddingSchema::AdaptiveColumnResult>
InferVariantShreddingSchema::InferAdaptiveColumn(
    const ColumnEvidence& previous_evidence,
    const std::shared_ptr<arrow::DataType>& previous_selected,
    const std::vector<std::shared_ptr<GenericVariant>>& samples, int32_t effective_sample_size,
    double admission_ratio, double retention_ratio, MaxFields* max_fields) const {
    PAIMON_ASSIGN_OR_RAISE(ColumnEvidence current, AnalyzeColumn(samples));
    ColumnEvidence combined;
    if (current.root_value_count == 0) {
        combined = previous_evidence;
    } else {
        ColumnEvidence bounded_previous = ScaleToAtMost(previous_evidence, effective_sample_size);
        combined.root_value_count = current.root_value_count + bounded_previous.root_value_count;
        combined.observed_schema =
            MergeSchema(bounded_previous.observed_schema, current.observed_schema);
        combined = ScaleToAtMost(combined, effective_sample_size);
    }
    std::shared_ptr<arrow::DataType> selected = FinalizeAdaptiveSchema(
        combined.observed_schema, current.observed_schema, previous_selected,
        combined.root_value_count, admission_ratio, retention_ratio, max_fields);
    return AdaptiveColumnResult{std::move(combined), std::move(selected)};
}

}  // namespace paimon
