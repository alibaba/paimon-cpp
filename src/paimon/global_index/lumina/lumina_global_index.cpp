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

#include "paimon/global_index/lumina/lumina_global_index.h"

#include <cstring>
#include <numeric>
#include <unordered_set>
#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "lumina/api/Dataset.h"
#include "lumina/api/LuminaBuilder.h"
#include "lumina/api/LuminaSearcher.h"
#include "lumina/api/OptionsNormalize.h"
#include "lumina/core/Constants.h"
#include "lumina/core/Status.h"
#include "lumina/core/Types.h"
#include "lumina/extensions/experimental/BuildCombinedExtensionV0.h"
#include "paimon/common/global_index/global_index_utils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/global_index/lumina/lumina_file_reader.h"
#include "paimon/global_index/lumina/lumina_file_writer.h"
#include "paimon/global_index/lumina/lumina_utils.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/leaf_predicate.h"
#include "rapidjson/document.h"
namespace paimon::lumina {
#define CHECK_NOT_NULL(pointer, error_msg)     \
    do {                                       \
        if (!(pointer)) {                      \
            return Status::Invalid(error_msg); \
        }                                      \
    } while (0)

namespace {
using TagDimensionData = ::lumina::extensions::experimental::TagDimensionData;
using TagFilter = ::lumina::extensions::experimental::TagFilter;
using TagValue = ::lumina::extensions::experimental::TagValue;
using TagValues = ::lumina::extensions::experimental::TagValues;

Result<std::string> GetRequiredStringMember(const rapidjson::Value& obj,
                                            const std::string& field_name,
                                            const std::string& tag_label) {
    auto iter = obj.FindMember(field_name.c_str());
    if (iter == obj.MemberEnd()) {
        return Status::Invalid(
            fmt::format("lumina tag_schema {} missing required field: {}", tag_label, field_name));
    }
    if (!iter->value.IsString()) {
        return Status::Invalid(
            fmt::format("lumina tag_schema {} field {} must be string", tag_label, field_name));
    }
    return std::string(iter->value.GetString(), iter->value.GetStringLength());
}

Result<LuminaTagField> ParseTagField(const rapidjson::Value& obj, const std::string& tag_label) {
    if (!obj.IsObject()) {
        return Status::Invalid(fmt::format("lumina tag_schema {} must be object", tag_label));
    }
    if (obj.MemberCount() != 3) {
        return Status::Invalid(fmt::format(
            "lumina tag_schema {} must have exactly 3 fields: key_name, type, value_type",
            tag_label));
    }

    PAIMON_ASSIGN_OR_RAISE(
        std::string key_name,
        GetRequiredStringMember(obj, std::string(::lumina::core::kExtensionTagKName), tag_label));
    PAIMON_ASSIGN_OR_RAISE(
        std::string type,
        GetRequiredStringMember(obj, std::string(::lumina::core::kExtensionTagType), tag_label));
    PAIMON_ASSIGN_OR_RAISE(
        std::string value_type,
        GetRequiredStringMember(obj, std::string(::lumina::core::kExtensionTagVType), tag_label));

    if (key_name.empty()) {
        return Status::Invalid(
            fmt::format("lumina tag_schema {} key_name must not be empty", tag_label));
    }
    if (key_name.size() > ::lumina::core::kMaxTagKNameLength) {
        return Status::Invalid(fmt::format("lumina tag_schema {} key_name exceeds max length {}",
                                           tag_label, ::lumina::core::kMaxTagKNameLength));
    }

    LuminaTagField::Type parsed_type;
    if (type == std::string(::lumina::core::kExtensionTagTypeEnum)) {
        parsed_type = LuminaTagField::Type::ENUM;
    } else if (type == std::string(::lumina::core::kExtensionTagTypeRange)) {
        parsed_type = LuminaTagField::Type::RANGE;
    } else {
        return Status::Invalid(
            fmt::format("lumina tag_schema {} has unsupported type: {}", tag_label, type));
    }

    LuminaTagField::ValueType parsed_value_type;
    if (value_type == std::string(::lumina::core::kExtensionTagVTypeInt64)) {
        parsed_value_type = LuminaTagField::ValueType::INT64;
    } else if (value_type == std::string(::lumina::core::kExtensionTagVTypeDouble)) {
        parsed_value_type = LuminaTagField::ValueType::DOUBLE;
    } else if (value_type == std::string(::lumina::core::kExtensionTagVTypeString)) {
        parsed_value_type = LuminaTagField::ValueType::STRING;
    } else {
        return Status::Invalid(fmt::format("lumina tag_schema {} has unsupported value_type: {}",
                                           tag_label, value_type));
    }
    if (parsed_type == LuminaTagField::Type::RANGE &&
        parsed_value_type == LuminaTagField::ValueType::STRING) {
        return Status::Invalid(fmt::format(
            "lumina tag_schema {} range type does not support string value_type", tag_label));
    }
    return LuminaTagField{key_name, parsed_type, parsed_value_type};
}

Status ValidateTagArrowType(const LuminaTagField& tag_field,
                            const std::shared_ptr<arrow::DataType>& field_type) {
    auto value_type = field_type;
    if (auto list_type = std::dynamic_pointer_cast<arrow::ListType>(field_type)) {
        value_type = list_type->value_type();
    }

    bool compatible = false;
    switch (tag_field.value_type) {
        case LuminaTagField::ValueType::INT64:
            compatible =
                value_type->id() == arrow::Type::INT8 || value_type->id() == arrow::Type::INT16 ||
                value_type->id() == arrow::Type::INT32 || value_type->id() == arrow::Type::INT64;
            break;
        case LuminaTagField::ValueType::DOUBLE:
            compatible =
                value_type->id() == arrow::Type::FLOAT || value_type->id() == arrow::Type::DOUBLE;
            break;
        case LuminaTagField::ValueType::STRING:
            compatible = value_type->id() == arrow::Type::STRING;
            break;
    }
    if (!compatible) {
        return Status::Invalid(
            fmt::format("lumina tag field {} type {} is not compatible with tag_schema value_type",
                        tag_field.name, field_type->ToString()));
    }
    return Status::OK();
}

Status AppendInt64Value(const std::shared_ptr<arrow::Array>& array, int64_t index,
                        std::vector<int64_t>* values) {
    if (array->IsNull(index)) {
        return Status::OK();
    }
    switch (array->type_id()) {
        case arrow::Type::INT8:
            values->push_back(std::dynamic_pointer_cast<arrow::Int8Array>(array)->Value(index));
            break;
        case arrow::Type::INT16:
            values->push_back(std::dynamic_pointer_cast<arrow::Int16Array>(array)->Value(index));
            break;
        case arrow::Type::INT32:
            values->push_back(std::dynamic_pointer_cast<arrow::Int32Array>(array)->Value(index));
            break;
        case arrow::Type::INT64:
            values->push_back(std::dynamic_pointer_cast<arrow::Int64Array>(array)->Value(index));
            break;
        default:
            return Status::Invalid(fmt::format(
                "lumina int64 tag field has unsupported arrow type {}", array->type()->ToString()));
    }
    return Status::OK();
}

Status AppendDoubleValue(const std::shared_ptr<arrow::Array>& array, int64_t index,
                         std::vector<double>* values) {
    if (array->IsNull(index)) {
        return Status::OK();
    }
    switch (array->type_id()) {
        case arrow::Type::FLOAT:
            values->push_back(std::dynamic_pointer_cast<arrow::FloatArray>(array)->Value(index));
            break;
        case arrow::Type::DOUBLE:
            values->push_back(std::dynamic_pointer_cast<arrow::DoubleArray>(array)->Value(index));
            break;
        default:
            return Status::Invalid(
                fmt::format("lumina double tag field has unsupported arrow type {}",
                            array->type()->ToString()));
    }
    return Status::OK();
}

Status AppendStringValue(const std::shared_ptr<arrow::Array>& array, int64_t index,
                         std::vector<std::string>* values) {
    if (array->IsNull(index)) {
        return Status::OK();
    }
    auto string_array = std::dynamic_pointer_cast<arrow::StringArray>(array);
    CHECK_NOT_NULL(string_array,
                   fmt::format("lumina string tag field has unsupported arrow type {}",
                               array->type()->ToString()));
    auto view = string_array->GetView(index);
    values->emplace_back(view.data(), view.size());
    return Status::OK();
}

template <typename ValueType>
Status ExtractTagValues(const std::shared_ptr<arrow::Array>& field_array, int64_t segment_start,
                        int64_t segment_len,
                        Status (*append_value)(const std::shared_ptr<arrow::Array>&, int64_t,
                                               std::vector<ValueType>*),
                        std::vector<std::vector<ValueType>>* values) {
    values->resize(segment_len);
    auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(field_array);
    if (list_array) {
        auto child_values = list_array->values();
        for (int64_t i = 0; i < segment_len; i++) {
            int64_t row = segment_start + i;
            if (list_array->IsNull(row)) {
                continue;
            }
            auto value_start = list_array->value_offset(row);
            auto value_end = list_array->value_offset(row + 1);
            auto& row_values = (*values)[i];
            row_values.reserve(value_end - value_start);
            for (int64_t value_index = value_start; value_index < value_end; value_index++) {
                PAIMON_RETURN_NOT_OK(append_value(child_values, value_index, &row_values));
            }
        }
        return Status::OK();
    }

    for (int64_t i = 0; i < segment_len; i++) {
        PAIMON_RETURN_NOT_OK(append_value(field_array, segment_start + i, &(*values)[i]));
    }
    return Status::OK();
}

Result<TagValue> LiteralToTagValue(const Literal& literal) {
    if (literal.IsNull()) {
        return Status::Invalid("lumina tag predicate does not support null literal");
    }
    switch (literal.GetType()) {
        case FieldType::TINYINT:
            return TagValue(static_cast<int64_t>(literal.GetValue<int8_t>()));
        case FieldType::SMALLINT:
            return TagValue(static_cast<int64_t>(literal.GetValue<int16_t>()));
        case FieldType::INT:
            return TagValue(static_cast<int64_t>(literal.GetValue<int32_t>()));
        case FieldType::BIGINT:
            return TagValue(literal.GetValue<int64_t>());
        case FieldType::FLOAT:
            return TagValue(static_cast<double>(literal.GetValue<float>()));
        case FieldType::DOUBLE:
            return TagValue(literal.GetValue<double>());
        case FieldType::STRING:
            return TagValue(literal.GetValue<std::string>());
        default:
            return Status::Invalid(
                fmt::format("lumina tag predicate does not support literal type {}",
                            static_cast<int32_t>(literal.GetType())));
    }
}

Result<const Literal*> GetSingleLiteral(const std::vector<Literal>& literals,
                                        const std::string& function_name) {
    if (literals.size() != 1) {
        return Status::Invalid(
            fmt::format("lumina tag {} predicate requires one literal", function_name));
    }
    return &literals[0];
}

Result<TagValues> LiteralsToTagValues(const std::vector<Literal>& literals) {
    if (literals.empty()) {
        return Status::Invalid("lumina tag predicate IN requires at least one literal");
    }

    switch (literals[0].GetType()) {
        case FieldType::TINYINT:
        case FieldType::SMALLINT:
        case FieldType::INT:
        case FieldType::BIGINT: {
            std::vector<int64_t> values;
            values.reserve(literals.size());
            for (const auto& literal : literals) {
                PAIMON_ASSIGN_OR_RAISE(TagValue value, LiteralToTagValue(literal));
                auto typed_value = std::get_if<int64_t>(&value);
                CHECK_NOT_NULL(typed_value,
                               "lumina tag predicate IN literals must have the same value type");
                values.push_back(*typed_value);
            }
            return TagValues(std::move(values));
        }
        case FieldType::FLOAT:
        case FieldType::DOUBLE: {
            std::vector<double> values;
            values.reserve(literals.size());
            for (const auto& literal : literals) {
                PAIMON_ASSIGN_OR_RAISE(TagValue value, LiteralToTagValue(literal));
                auto typed_value = std::get_if<double>(&value);
                CHECK_NOT_NULL(typed_value,
                               "lumina tag predicate IN literals must have the same value type");
                values.push_back(*typed_value);
            }
            return TagValues(std::move(values));
        }
        case FieldType::STRING: {
            std::vector<std::string> values;
            values.reserve(literals.size());
            for (const auto& literal : literals) {
                PAIMON_ASSIGN_OR_RAISE(TagValue value, LiteralToTagValue(literal));
                auto typed_value = std::get_if<std::string>(&value);
                CHECK_NOT_NULL(typed_value,
                               "lumina tag predicate IN literals must have the same value type");
                values.push_back(std::move(*typed_value));
            }
            return TagValues(std::move(values));
        }
        default:
            return Status::Invalid(
                fmt::format("lumina tag predicate IN does not support literal type {}",
                            static_cast<int32_t>(literals[0].GetType())));
    }
}

}  // namespace

Result<std::vector<TagDimensionData>> LuminaIndexWriter::ExtractTagDataForSegment(
    const std::shared_ptr<arrow::StructArray>& struct_array,
    const std::vector<LuminaTagField>& tag_fields, int64_t segment_start, int64_t segment_len) {
    std::vector<TagDimensionData> tag_dimensions_data;
    tag_dimensions_data.reserve(tag_fields.size());
    for (const auto& tag_field : tag_fields) {
        auto field_array = struct_array->GetFieldByName(tag_field.name);
        CHECK_NOT_NULL(field_array,
                       fmt::format("lumina tag field {} not in input array", tag_field.name));

        TagDimensionData tag_dimension_data;
        tag_dimension_data.tagkName = tag_field.name;
        switch (tag_field.value_type) {
            case LuminaTagField::ValueType::INT64: {
                std::vector<std::vector<int64_t>> values;
                PAIMON_RETURN_NOT_OK(ExtractTagValues<int64_t>(
                    field_array, segment_start, segment_len, AppendInt64Value, &values));
                tag_dimension_data.values = std::move(values);
                break;
            }
            case LuminaTagField::ValueType::DOUBLE: {
                std::vector<std::vector<double>> values;
                PAIMON_RETURN_NOT_OK(ExtractTagValues<double>(
                    field_array, segment_start, segment_len, AppendDoubleValue, &values));
                tag_dimension_data.values = std::move(values);
                break;
            }
            case LuminaTagField::ValueType::STRING: {
                std::vector<std::vector<std::string>> values;
                PAIMON_RETURN_NOT_OK(ExtractTagValues<std::string>(
                    field_array, segment_start, segment_len, AppendStringValue, &values));
                tag_dimension_data.values = std::move(values);
                break;
            }
        }
        tag_dimensions_data.push_back(std::move(tag_dimension_data));
    }
    return tag_dimensions_data;
}

Result<std::vector<LuminaTagField>> LuminaGlobalIndex::ParseTagSchema(
    const std::map<std::string, std::string>& lumina_options) {
    auto iter = lumina_options.find(std::string(::lumina::core::kExtensionTagSchema));
    if (iter == lumina_options.end()) {
        return std::vector<LuminaTagField>();
    }

    rapidjson::Document document;
    document.Parse(iter->second.c_str());
    if (document.HasParseError()) {
        return Status::Invalid("lumina tag_schema must be a valid JSON string");
    }

    std::vector<LuminaTagField> tag_fields;
    if (document.IsArray()) {
        if (document.Empty()) {
            return Status::Invalid("lumina tag_schema must contain at least one tag definition");
        }
        tag_fields.reserve(document.Size());
        for (rapidjson::SizeType i = 0; i < document.Size(); i++) {
            PAIMON_ASSIGN_OR_RAISE(LuminaTagField field,
                                   ParseTagField(document[i], fmt::format("tag[{}]", i)));
            tag_fields.push_back(std::move(field));
        }
    } else if (document.IsObject()) {
        PAIMON_ASSIGN_OR_RAISE(LuminaTagField field, ParseTagField(document, "tag[0]"));
        tag_fields.push_back(std::move(field));
    } else {
        return Status::Invalid("lumina tag_schema must be an object or array of objects");
    }

    std::unordered_set<std::string> seen_names;
    for (const auto& field : tag_fields) {
        if (!seen_names.insert(field.name).second) {
            return Status::Invalid(
                fmt::format("lumina tag_schema has duplicate key_name: {}", field.name));
        }
    }
    return tag_fields;
}

Status LuminaGlobalIndex::ValidateTagFields(const arrow::StructType& struct_type,
                                            const std::vector<LuminaTagField>& tag_fields) {
    for (const auto& tag_field : tag_fields) {
        auto field = struct_type.GetFieldByName(tag_field.name);
        CHECK_NOT_NULL(
            field, fmt::format("lumina tag field {} not exist in arrow schema", tag_field.name));
        PAIMON_RETURN_NOT_OK(ValidateTagArrowType(tag_field, field->type()));
    }
    return Status::OK();
}

Result<::lumina::extensions::experimental::TagFilter> LuminaIndexReader::PredicateToTagFilter(
    const std::shared_ptr<Predicate>& predicate) {
    if (!predicate) {
        return Status::Invalid("lumina tag predicate must not be null");
    }

    auto compound_predicate = std::dynamic_pointer_cast<CompoundPredicate>(predicate);
    if (compound_predicate) {
        std::vector<::lumina::extensions::experimental::TagFilter> children;
        children.reserve(compound_predicate->Children().size());
        for (const auto& child : compound_predicate->Children()) {
            PAIMON_ASSIGN_OR_RAISE(::lumina::extensions::experimental::TagFilter tag_filter,
                                   PredicateToTagFilter(child));
            children.push_back(std::move(tag_filter));
        }
        if (children.empty()) {
            return Status::Invalid("lumina tag compound predicate must have at least one child");
        }
        if (children.size() == 1) {
            return std::move(children.front());
        }
        switch (compound_predicate->GetFunction().GetType()) {
            case Function::Type::AND:
                return ::lumina::extensions::experimental::TagFilter::And(std::move(children));
            case Function::Type::OR:
                return ::lumina::extensions::experimental::TagFilter::Or(std::move(children));
            default:
                return Status::NotImplemented(
                    fmt::format("lumina tag predicate does not support compound function {}",
                                compound_predicate->GetFunction().ToString()));
        }
    }

    auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate);
    if (!leaf_predicate) {
        return Status::Invalid(
            fmt::format("cannot cast predicate {} to CompoundPredicate or LeafPredicate",
                        predicate->ToString()));
    }

    const auto& literals = leaf_predicate->Literals();
    const auto& field_name = leaf_predicate->FieldName();
    switch (leaf_predicate->GetFunction().GetType()) {
        case Function::Type::EQUAL: {
            PAIMON_ASSIGN_OR_RAISE(const Literal* literal, GetSingleLiteral(literals, "equal"));
            PAIMON_ASSIGN_OR_RAISE(TagValue value, LiteralToTagValue(*literal));
            return ::lumina::extensions::experimental::TagFilter::Eq(field_name, std::move(value));
        }
        case Function::Type::GREATER_THAN: {
            PAIMON_ASSIGN_OR_RAISE(const Literal* literal,
                                   GetSingleLiteral(literals, "greater than"));
            PAIMON_ASSIGN_OR_RAISE(TagValue value, LiteralToTagValue(*literal));
            return ::lumina::extensions::experimental::TagFilter::Gt(field_name, std::move(value));
        }
        case Function::Type::GREATER_OR_EQUAL: {
            PAIMON_ASSIGN_OR_RAISE(const Literal* literal,
                                   GetSingleLiteral(literals, "greater or equal"));
            PAIMON_ASSIGN_OR_RAISE(TagValue value, LiteralToTagValue(*literal));
            return ::lumina::extensions::experimental::TagFilter::Gte(field_name, std::move(value));
        }
        case Function::Type::LESS_THAN: {
            PAIMON_ASSIGN_OR_RAISE(const Literal* literal, GetSingleLiteral(literals, "less than"));
            PAIMON_ASSIGN_OR_RAISE(TagValue value, LiteralToTagValue(*literal));
            return ::lumina::extensions::experimental::TagFilter::Lt(field_name, std::move(value));
        }
        case Function::Type::LESS_OR_EQUAL: {
            PAIMON_ASSIGN_OR_RAISE(const Literal* literal,
                                   GetSingleLiteral(literals, "less or equal"));
            PAIMON_ASSIGN_OR_RAISE(TagValue value, LiteralToTagValue(*literal));
            return ::lumina::extensions::experimental::TagFilter::Lte(field_name, std::move(value));
        }
        case Function::Type::IN: {
            PAIMON_ASSIGN_OR_RAISE(TagValues values, LiteralsToTagValues(literals));
            return ::lumina::extensions::experimental::TagFilter::In(field_name, std::move(values));
        }
        default:
            return Status::NotImplemented(
                fmt::format("lumina tag predicate does not support leaf function {}",
                            leaf_predicate->GetFunction().ToString()));
    }
}

Result<std::shared_ptr<GlobalIndexWriter>> LuminaGlobalIndex::CreateWriter(
    const std::string& field_name, ::ArrowSchema* arrow_schema,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::DataType> arrow_type,
                                      arrow::ImportType(arrow_schema));
    // check data type
    auto struct_type = std::dynamic_pointer_cast<arrow::StructType>(arrow_type);
    CHECK_NOT_NULL(struct_type, "arrow schema must be struct type when create LuminaIndexWriter");
    auto index_field = struct_type->GetFieldByName(field_name);
    CHECK_NOT_NULL(index_field,
                   fmt::format("field {} not exist in arrow schema when create LuminaIndexWriter",
                               field_name));
    auto list_type = std::dynamic_pointer_cast<arrow::ListType>(index_field->type());
    CHECK_NOT_NULL(list_type, "field type must be list[float] when create LuminaIndexWriter");
    if (list_type->value_type()->id() != arrow::Type::type::FLOAT) {
        return Status::Invalid("field type must be list[float] when create LuminaIndexWriter");
    }

    // check options
    auto lumina_options =
        OptionsUtils::FetchOptionsWithPrefix(LuminaDefines::kOptionKeyPrefix, options_);
    PAIMON_ASSIGN_OR_RAISE(std::vector<LuminaTagField> tag_fields, ParseTagSchema(lumina_options));
    PAIMON_RETURN_NOT_OK(ValidateTagFields(*struct_type, tag_fields));
    PAIMON_ASSIGN_OR_RAISE(uint32_t dimension,
                           OptionsUtils::GetValueFromMap<uint32_t>(
                               lumina_options, std::string(::lumina::core::kDimension)));

    PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
        ::lumina::api::BuilderOptions builder_options,
        ::lumina::api::NormalizeBuilderOptions(std::unordered_map<std::string, std::string>(
            lumina_options.begin(), lumina_options.end())));
    auto lumina_pool = std::make_shared<LuminaMemoryPool>(pool);
    return std::make_shared<LuminaIndexWriter>(
        field_name, arrow_type, dimension, file_writer, std::move(builder_options),
        ::lumina::api::IOOptions(), lumina_options, std::move(tag_fields), lumina_pool);
}

Result<LuminaIndexReader::IndexInfo> LuminaIndexReader::GetIndexInfo(
    const GlobalIndexIOMeta& io_meta) {
    auto meta_bytes = io_meta.metadata;
    if (!meta_bytes) {
        return Status::Invalid("Lumina global index must have meta data");
    }
    std::map<std::string, std::string> lumina_write_options;
    PAIMON_RETURN_NOT_OK(RapidJsonUtil::FromJsonString(
        std::string(meta_bytes->data(), meta_bytes->size()), &lumina_write_options));

    // check options
    PAIMON_ASSIGN_OR_RAISE(uint32_t dimension,
                           OptionsUtils::GetValueFromMap<uint32_t>(
                               lumina_write_options, std::string(::lumina::core::kDimension)));
    PAIMON_ASSIGN_OR_RAISE(std::string index_type,
                           OptionsUtils::GetValueFromMap<std::string>(
                               lumina_write_options, std::string(::lumina::core::kIndexType)));
    PAIMON_ASSIGN_OR_RAISE(std::string distance_type_str,
                           OptionsUtils::GetValueFromMap<std::string>(
                               lumina_write_options, std::string(::lumina::core::kDistanceMetric)));
    VectorSearch::DistanceType distance_type = VectorSearch::DistanceType::UNKNOWN;
    if (distance_type_str == ::lumina::core::kDistanceL2) {
        distance_type = VectorSearch::DistanceType::EUCLIDEAN;
    } else if (distance_type_str == ::lumina::core::kDistanceCosine) {
        distance_type = VectorSearch::DistanceType::COSINE;
    } else if (distance_type_str == ::lumina::core::kDistanceInnerProduct) {
        distance_type = VectorSearch::DistanceType::INNER_PRODUCT;
    }
    if (distance_type == VectorSearch::DistanceType::UNKNOWN) {
        return Status::Invalid(
            fmt::format("invalid distance type {} for lumina", distance_type_str));
    }
    bool has_tag = lumina_write_options.find(std::string(::lumina::core::kExtensionTagSchema)) !=
                   lumina_write_options.end();
    return LuminaIndexReader::IndexInfo({dimension, index_type, distance_type, has_tag});
}

Result<std::shared_ptr<GlobalIndexReader>> LuminaGlobalIndex::CreateReader(
    ::ArrowSchema* c_arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_manager,
    const std::vector<GlobalIndexIOMeta>& files, const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(c_arrow_schema));
    if (files.size() != 1) {
        return Status::Invalid("lumina index only has one index file per shard");
    }
    const auto& io_meta = files[0];
    // check data type
    if (arrow_schema->num_fields() != 1) {
        return Status::Invalid("LuminaGlobalIndex now only support one field");
    }
    auto index_field = arrow_schema->field(0);
    auto list_type = std::dynamic_pointer_cast<arrow::ListType>(index_field->type());
    CHECK_NOT_NULL(list_type, "field type must be list[float] when create LuminaIndexReader");
    if (list_type->value_type()->id() != arrow::Type::type::FLOAT) {
        return Status::Invalid("field type must be list[float] when create LuminaIndexReader");
    }

    // get index info from meta
    PAIMON_ASSIGN_OR_RAISE(LuminaIndexReader::IndexInfo index_info,
                           LuminaIndexReader::GetIndexInfo(io_meta));

    auto lumina_pool = std::make_shared<LuminaMemoryPool>(pool);
    ::lumina::core::MemoryResourceConfig memory_resource(lumina_pool.get());

    auto lumina_options =
        OptionsUtils::FetchOptionsWithPrefix(LuminaDefines::kOptionKeyPrefix, options_);
    lumina_options[std::string(::lumina::core::kDimension)] = std::to_string(index_info.dimension);
    lumina_options[std::string(::lumina::core::kIndexType)] = index_info.index_type;

    PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
        ::lumina::api::SearcherOptions searcher_options,
        ::lumina::api::NormalizeSearcherOptions(std::unordered_map<std::string, std::string>(
            lumina_options.begin(), lumina_options.end())));

    PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
        ::lumina::api::LuminaSearcher lumina_searcher,
        ::lumina::api::LuminaSearcher::Create(searcher_options, memory_resource));
    auto searcher = std::make_unique<::lumina::api::LuminaSearcher>(std::move(lumina_searcher));
    // get input stream and open index
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> in,
                           file_manager->GetInputStream(io_meta.file_path));
    auto lumina_file_reader = std::make_unique<LuminaFileReader>(in);
    PAIMON_RETURN_NOT_OK_FROM_LUMINA(
        searcher->Open(std::move(lumina_file_reader), ::lumina::api::IOOptions()));

    // check meta
    if (searcher->GetMeta().dim != index_info.dimension) {
        return Status::Invalid(
            fmt::format("lumina index dimension {} mismatch dimension {} in io meta",
                        searcher->GetMeta().dim, index_info.dimension));
    }
    auto searcher_with_filter = std::make_unique<::lumina::extensions::SearchWithFilterExtension>();
    PAIMON_RETURN_NOT_OK_FROM_LUMINA(searcher->Attach(*searcher_with_filter));
    std::unique_ptr<::lumina::extensions::experimental::SearchWithTagExtension> searcher_with_tag;
    if (index_info.has_tag) {
        searcher_with_tag =
            std::make_unique<::lumina::extensions::experimental::SearchWithTagExtension>();
        PAIMON_RETURN_NOT_OK_FROM_LUMINA(searcher->Attach(*searcher_with_tag));
    }
    return std::make_shared<LuminaIndexReader>(index_info, std::move(searcher),
                                               std::move(searcher_with_filter),
                                               std::move(searcher_with_tag), lumina_pool);
}

Result<std::optional<std::vector<std::string>>> LuminaGlobalIndex::GetExtraFieldNames() const {
    auto lumina_options =
        OptionsUtils::FetchOptionsWithPrefix(LuminaDefines::kOptionKeyPrefix, options_);
    PAIMON_ASSIGN_OR_RAISE(std::vector<LuminaTagField> tag_fields, ParseTagSchema(lumina_options));
    if (tag_fields.empty()) {
        return std::optional<std::vector<std::string>>(std::nullopt);
    }
    std::vector<std::string> field_names;
    field_names.reserve(tag_fields.size());
    for (const auto& tag_field : tag_fields) {
        field_names.push_back(tag_field.name);
    }
    return std::optional<std::vector<std::string>>(std::move(field_names));
}

class LuminaDataset : public ::lumina::api::Dataset {
 public:
    LuminaDataset(int64_t element_count, uint32_t dimension,
                  const std::vector<std::shared_ptr<arrow::FloatArray>>& array_vec,
                  const std::vector<int64_t>& start_ids)
        : element_count_(element_count),
          dimension_(dimension),
          array_vec_(array_vec),
          start_ids_(start_ids) {}

    uint32_t Dim() const noexcept override {
        return dimension_;
    }
    uint64_t TotalSize() const noexcept override {
        return element_count_;
    }

    ::lumina::core::Result<uint64_t> GetNextBatch(
        std::vector<float>& vector_buffer,
        std::vector<::lumina::core::vector_id_t>& id_buffer) noexcept override {
        if (cursor_ >= array_vec_.size()) {
            return ::lumina::core::Result<uint64_t>::Ok(0);
        }
        auto& value_array = array_vec_[cursor_];
        int64_t value_array_length = value_array->length();
        int64_t element_count = value_array_length / dimension_;
        const float* value_ptr = value_array->raw_values();
        vector_buffer.resize(value_array_length);
        memcpy(vector_buffer.data(), value_ptr, sizeof(float) * value_array_length);
        id_buffer.resize(element_count);
        std::iota(id_buffer.begin(), id_buffer.end(),
                  static_cast<::lumina::core::vector_id_t>(start_ids_[cursor_]));

        // release the array when copy to vector_buffer
        value_array.reset();
        cursor_++;
        return ::lumina::core::Result<uint64_t>::Ok(static_cast<uint64_t>(element_count));
    }

 private:
    int64_t element_count_;
    uint32_t dimension_;
    std::vector<std::shared_ptr<arrow::FloatArray>> array_vec_;
    std::vector<int64_t> start_ids_;
    size_t cursor_ = 0;
};

class LuminaDatasetWithTag : public ::lumina::extensions::experimental::DatasetWithTag {
 public:
    LuminaDatasetWithTag(int64_t element_count, uint32_t dimension,
                         const std::vector<std::shared_ptr<arrow::FloatArray>>& array_vec,
                         const std::vector<int64_t>& start_ids,
                         const std::vector<std::vector<TagDimensionData>>& tag_data_vec)
        : element_count_(element_count),
          dimension_(dimension),
          array_vec_(array_vec),
          start_ids_(start_ids),
          tag_data_vec_(tag_data_vec) {}

    uint32_t Dim() const noexcept override {
        return dimension_;
    }
    uint64_t TotalSize() const noexcept override {
        return element_count_;
    }

    ::lumina::core::Result<uint64_t> GetNextBatch(
        std::vector<float>& vector_buffer, std::vector<::lumina::core::vector_id_t>& id_buffer,
        std::vector<TagDimensionData>& tag_dimensions_data) noexcept override {
        if (cursor_ >= array_vec_.size()) {
            return ::lumina::core::Result<uint64_t>::Ok(0);
        }
        auto& value_array = array_vec_[cursor_];
        int64_t value_array_length = value_array->length();
        int64_t element_count = value_array_length / dimension_;
        const float* value_ptr = value_array->raw_values();
        vector_buffer.resize(value_array_length);
        memcpy(vector_buffer.data(), value_ptr, sizeof(float) * value_array_length);
        id_buffer.resize(element_count);
        std::iota(id_buffer.begin(), id_buffer.end(),
                  static_cast<::lumina::core::vector_id_t>(start_ids_[cursor_]));
        tag_dimensions_data = std::move(tag_data_vec_[cursor_]);

        value_array.reset();
        cursor_++;
        return ::lumina::core::Result<uint64_t>::Ok(static_cast<uint64_t>(element_count));
    }

 private:
    int64_t element_count_;
    uint32_t dimension_;
    std::vector<std::shared_ptr<arrow::FloatArray>> array_vec_;
    std::vector<int64_t> start_ids_;
    std::vector<std::vector<TagDimensionData>> tag_data_vec_;
    size_t cursor_ = 0;
};

LuminaIndexWriter::LuminaIndexWriter(
    const std::string& field_name, const std::shared_ptr<arrow::DataType>& arrow_type,
    uint32_t dimension, const std::shared_ptr<GlobalIndexFileWriter>& file_manager,
    ::lumina::api::BuilderOptions&& builder_options, ::lumina::api::IOOptions&& io_options,
    const std::map<std::string, std::string>& lumina_options,
    std::vector<LuminaTagField>&& tag_fields, const std::shared_ptr<LuminaMemoryPool>& pool)
    : pool_(pool),
      field_name_(field_name),
      arrow_type_(arrow_type),
      dimension_(dimension),
      file_manager_(file_manager),
      builder_options_(std::move(builder_options)),
      io_options_(std::move(io_options)),
      lumina_options_(lumina_options),
      tag_fields_(std::move(tag_fields)) {}

Status LuminaIndexWriter::AddBatch(::ArrowArray* arrow_array,
                                   std::vector<int64_t>&& relative_row_ids) {
    PAIMON_RETURN_NOT_OK(
        GlobalIndexUtils::CheckRelativeRowIds(arrow_array, relative_row_ids, count_));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(arrow_array, arrow_type_));
    if (array->null_count() != 0) {
        return Status::Invalid("arrow_array in LuminaIndexWriter is invalid, must not null");
    }
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    CHECK_NOT_NULL(struct_array, "invalid input array in LuminaIndexWriter, must be struct array");
    auto field_array = struct_array->GetFieldByName(field_name_);
    CHECK_NOT_NULL(
        field_array,
        fmt::format("invalid input array in LuminaIndexWriter, field {} not in input array",
                    field_name_));
    int64_t field_length = field_array->length();
    auto list_field_array = std::dynamic_pointer_cast<arrow::ListArray>(field_array);
    CHECK_NOT_NULL(list_field_array,
                   "invalid input array in LuminaIndexWriter, field array must be list array");

    // Split into contiguous non-null segments, skipping null rows in the list field.
    int64_t segment_start = -1;
    for (int64_t i = 0; i <= field_length; i++) {
        bool is_null = (i < field_length) && list_field_array->IsNull(i);
        bool is_end = (i == field_length);

        if (!is_null && !is_end && segment_start == -1) {
            segment_start = i;
        }

        if ((is_null || is_end) && segment_start != -1) {
            int64_t segment_len = i - segment_start;
            // Use value_offset to precisely locate the float range for this segment
            auto value_start_offset = list_field_array->value_offset(segment_start);
            auto value_end_offset = list_field_array->value_offset(segment_start + segment_len);
            int64_t value_length = value_end_offset - value_start_offset;
            auto sliced_values = std::dynamic_pointer_cast<arrow::FloatArray>(
                list_field_array->values()->Slice(value_start_offset, value_length));
            CHECK_NOT_NULL(sliced_values,
                           "invalid sliced value array in LuminaIndexWriter, must be float array");
            if (sliced_values->null_count() != 0) {
                return Status::Invalid(
                    "field value array in LuminaIndexWriter is invalid, must not null");
            }
            if (sliced_values->length() != segment_len * static_cast<int64_t>(dimension_)) {
                return Status::Invalid(fmt::format(
                    "invalid input array in LuminaIndexWriter, length of field array [{}] "
                    "multiplied dimension [{}] must match length of field value array [{}]",
                    segment_len, dimension_, sliced_values->length()));
            }
            std::vector<TagDimensionData> tag_data;
            if (!tag_fields_.empty()) {
                PAIMON_ASSIGN_OR_RAISE(
                    tag_data, ExtractTagDataForSegment(struct_array, tag_fields_, segment_start,
                                                       segment_len));
            }
            array_vec_.push_back(std::move(sliced_values));
            array_start_ids_.push_back(count_ + segment_start);
            if (!tag_fields_.empty()) {
                tag_data_vec_.push_back(std::move(tag_data));
            }
            indexed_count_ += segment_len;
            segment_start = -1;
        }
    }

    count_ += array->length();
    return Status::OK();
}

Result<std::vector<GlobalIndexIOMeta>> LuminaIndexWriter::Finish() {
    if (indexed_count_ == 0) {
        return std::vector<GlobalIndexIOMeta>();
    }
    ::lumina::core::MemoryResourceConfig memory_resource(pool_.get());
    PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
        ::lumina::api::LuminaBuilder builder,
        ::lumina::api::LuminaBuilder::Create(builder_options_, memory_resource));
    // pretrain
    LuminaDataset dataset1(indexed_count_, dimension_, array_vec_, array_start_ids_);
    PAIMON_RETURN_NOT_OK_FROM_LUMINA(builder.PretrainFrom(dataset1));

    // insert data
    if (tag_fields_.empty()) {
        LuminaDataset dataset2(indexed_count_, dimension_, array_vec_, array_start_ids_);
        std::vector<std::shared_ptr<arrow::FloatArray>>().swap(array_vec_);
        PAIMON_RETURN_NOT_OK_FROM_LUMINA(builder.InsertFrom(dataset2));
    } else {
        ::lumina::extensions::experimental::BuildWithTagExtension tag_extension;
        PAIMON_RETURN_NOT_OK_FROM_LUMINA(builder.Attach(tag_extension));
        LuminaDatasetWithTag dataset2(indexed_count_, dimension_, array_vec_, array_start_ids_,
                                      tag_data_vec_);
        std::vector<std::shared_ptr<arrow::FloatArray>>().swap(array_vec_);
        std::vector<std::vector<TagDimensionData>>().swap(tag_data_vec_);
        PAIMON_RETURN_NOT_OK_FROM_LUMINA(tag_extension.InsertFromWithTag(dataset2));
    }

    // dump index
    PAIMON_ASSIGN_OR_RAISE(std::string index_file_name,
                           file_manager_->NewFileName(LuminaDefines::kIdentifier));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> out,
                           file_manager_->NewOutputStream(index_file_name));
    auto file_writer = std::make_unique<LuminaFileWriter>(out);
    PAIMON_RETURN_NOT_OK_FROM_LUMINA(builder.Dump(std::move(file_writer), io_options_));
    // prepare GlobalIndexIOMeta
    PAIMON_ASSIGN_OR_RAISE(int64_t file_size, file_manager_->GetFileSize(index_file_name));
    std::string options_json;
    PAIMON_RETURN_NOT_OK(RapidJsonUtil::ToJsonString(lumina_options_, &options_json));
    auto meta_bytes = std::make_shared<Bytes>(options_json, pool_->GetPaimonPool().get());
    GlobalIndexIOMeta meta(file_manager_->ToPath(index_file_name), file_size,
                           /*metadata=*/meta_bytes);
    return std::vector<GlobalIndexIOMeta>({meta});
}

LuminaIndexReader::LuminaIndexReader(
    const LuminaIndexReader::IndexInfo& index_info,
    std::unique_ptr<::lumina::api::LuminaSearcher>&& searcher,
    std::unique_ptr<::lumina::extensions::SearchWithFilterExtension>&& searcher_with_filter,
    std::unique_ptr<::lumina::extensions::experimental::SearchWithTagExtension>&& searcher_with_tag,
    const std::shared_ptr<LuminaMemoryPool>& pool)
    : index_info_(index_info),
      pool_(pool),
      searcher_(std::move(searcher)),
      searcher_with_filter_(std::move(searcher_with_filter)),
      searcher_with_tag_(std::move(searcher_with_tag)) {}

Result<std::shared_ptr<ScoredGlobalIndexResult>> LuminaIndexReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search) {
    if (vector_search->distance_type &&
        vector_search->distance_type.value() != index_info_.distance_type) {
        return Status::Invalid("distance type for index and search not match");
    }
    if (vector_search->query.size() != index_info_.dimension) {
        return Status::Invalid("dimension for index and search not match");
    }

    auto lumina_options = OptionsUtils::FetchOptionsWithPrefix(LuminaDefines::kOptionKeyPrefix,
                                                               vector_search->options);
    auto index_type_iter = lumina_options.find(std::string(::lumina::core::kIndexType));
    if (index_type_iter != lumina_options.end() &&
        index_type_iter->second != index_info_.index_type) {
        return Status::Invalid("index type for index and search not match");
    }

    lumina_options[std::string(::lumina::core::kTopK)] = std::to_string(vector_search->limit);
    lumina_options[std::string(::lumina::core::kSearchThreadSafeFilter)] = "true";
    PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
        ::lumina::api::SearchOptions search_options,
        ::lumina::api::NormalizeSearchOptions(index_info_.index_type,
                                              std::unordered_map<std::string, std::string>(
                                                  lumina_options.begin(), lumina_options.end())));

    ::lumina::api::Query lumina_query(vector_search->query.data(), vector_search->query.size());
    ::lumina::api::LuminaSearcher::SearchResult search_result;
    if (vector_search->predicate) {
        if (!searcher_with_tag_) {
            return Status::Invalid("lumina index was not built with tag");
        }
        PAIMON_ASSIGN_OR_RAISE(::lumina::extensions::experimental::TagFilter tag_filter,
                               PredicateToTagFilter(vector_search->predicate));
        if (!vector_search->pre_filter) {
            PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
                search_result, searcher_with_tag_->SearchWithTag(lumina_query, tag_filter,
                                                                 search_options, *pool_));
        } else {
            auto lumina_filter = [filter = vector_search->pre_filter](
                                     ::lumina::core::vector_id_t id) -> bool { return filter(id); };
            PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
                search_result,
                searcher_with_tag_->SearchWithTagAndFilter(lumina_query, tag_filter, lumina_filter,
                                                           search_options, *pool_));
        }
    } else if (!vector_search->pre_filter) {
        PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(search_result,
                                           searcher_->Search(lumina_query, search_options, *pool_));
    } else {
        auto lumina_filter = [filter = vector_search->pre_filter](
                                 ::lumina::core::vector_id_t id) -> bool { return filter(id); };
        PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
            search_result, searcher_with_filter_->SearchWithFilter(lumina_query, lumina_filter,
                                                                   search_options, *pool_));
    }

    // prepare BitmapScoredGlobalIndexResult
    std::map<int64_t, float> id_to_score;
    for (const auto& [id, score] : search_result.topk) {
        id_to_score[id] = score;
    }

    RoaringBitmap64 bitmap;
    std::vector<float> scores;
    scores.reserve(id_to_score.size());
    for (const auto& [id, score] : id_to_score) {
        bitmap.Add(id);
        scores.push_back(score);
    }
    return std::make_shared<BitmapScoredGlobalIndexResult>(std::move(bitmap), std::move(scores));
}

}  // namespace paimon::lumina
