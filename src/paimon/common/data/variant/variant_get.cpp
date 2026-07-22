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

#include "paimon/common/data/variant/variant_get.h"

#include <algorithm>
#include <vector>

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/common/data/variant/variant_binary_util.h"
#include "paimon/common/data/variant/variant_builder.h"
#include "paimon/common/data/variant/variant_json_utils.h"
#include "paimon/common/data/variant/variant_path_segment.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/core/casting/cast_executor_factory.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"

namespace paimon {

namespace {

Result<std::optional<Literal>> InvalidCast(const std::shared_ptr<GenericVariant>& variant,
                                           const std::shared_ptr<arrow::DataType>& target_type,
                                           const VariantCastArgs& cast_args) {
    if (cast_args.fail_on_error) {
        PAIMON_ASSIGN_OR_RAISE(std::string json, variant->ToJson(cast_args.zone_id));
        return Status::Invalid(fmt::format("Invalid cast {} to {}", json, target_type->ToString()));
    }
    return std::optional<Literal>(std::nullopt);
}

Result<std::optional<Literal>> CastVariant(const std::shared_ptr<GenericVariant>& variant,
                                           const std::shared_ptr<arrow::DataType>& target_type,
                                           const VariantCastArgs& cast_args) {
    switch (target_type->id()) {
        case arrow::Type::type::STRUCT:
        case arrow::Type::type::LIST:
        case arrow::Type::type::MAP:
            return Status::NotImplemented(fmt::format(
                "variant_get to nested type {} is not supported", target_type->ToString()));
        default:
            break;
    }

    PAIMON_ASSIGN_OR_RAISE(VariantValueType variant_type, variant->GetType());
    if (variant_type == VariantValueType::kNull) {
        return std::optional<Literal>(std::nullopt);
    }

    bool target_is_string = target_type->id() == arrow::Type::type::STRING;
    if (variant_type == VariantValueType::kUuid) {
        // There's no UUID type in Paimon. We only allow it to be cast to string.
        if (target_is_string) {
            PAIMON_ASSIGN_OR_RAISE(std::string_view uuid, variant->GetUuid());
            std::string uuid_str = VariantBinaryUtil::UuidToString(uuid);
            return std::optional<Literal>(
                Literal(FieldType::STRING, uuid_str.data(), uuid_str.size()));
        }
        return InvalidCast(variant, target_type, cast_args);
    }

    std::optional<Literal> input;
    std::shared_ptr<arrow::DataType> input_type;
    switch (variant_type) {
        case VariantValueType::kObject:
        case VariantValueType::kArray: {
            if (target_is_string) {
                PAIMON_ASSIGN_OR_RAISE(std::string json, variant->ToJson(cast_args.zone_id));
                return std::optional<Literal>(Literal(FieldType::STRING, json.data(), json.size()));
            }
            return InvalidCast(variant, target_type, cast_args);
        }
        case VariantValueType::kBoolean: {
            PAIMON_ASSIGN_OR_RAISE(bool value, variant->GetBoolean());
            input = Literal(value);
            input_type = arrow::boolean();
            break;
        }
        case VariantValueType::kLong: {
            PAIMON_ASSIGN_OR_RAISE(int64_t value, variant->GetLong());
            input = Literal(value);
            input_type = arrow::int64();
            break;
        }
        case VariantValueType::kString: {
            PAIMON_ASSIGN_OR_RAISE(std::string_view value, variant->GetString());
            input = Literal(FieldType::STRING, value.data(), value.size());
            input_type = arrow::utf8();
            break;
        }
        case VariantValueType::kDouble: {
            PAIMON_ASSIGN_OR_RAISE(double value, variant->GetDouble());
            if (target_is_string) {
                // Match `GenericVariant::ToJson` and Java's `Double.toString` instead of the
                // arrow cast formatting.
                std::string str = VariantJsonUtils::JavaDoubleToString(value);
                return std::optional<Literal>(Literal(FieldType::STRING, str.data(), str.size()));
            }
            input = Literal(value);
            input_type = arrow::float64();
            break;
        }
        case VariantValueType::kDecimal: {
            PAIMON_ASSIGN_OR_RAISE(VariantDecimal value, variant->GetDecimal());
            if (value.scale < 0) {
                // `paimon::Decimal` requires a non-negative scale; scale the value back up.
                for (; value.scale < 0; ++value.scale) {
                    value.unscaled *= 10;
                }
            }
            int32_t precision = std::max(value.Precision(), value.scale);
            int32_t scale = value.scale;
            input = Literal(Decimal(precision, scale, value.unscaled));
            input_type = arrow::decimal128(precision, scale);
            break;
        }
        case VariantValueType::kDate: {
            PAIMON_ASSIGN_OR_RAISE(int64_t value, variant->GetLong());
            input = Literal(FieldType::DATE, static_cast<int32_t>(value));
            input_type = arrow::date32();
            break;
        }
        case VariantValueType::kFloat: {
            PAIMON_ASSIGN_OR_RAISE(float value, variant->GetFloat());
            if (target_is_string) {
                // Match `GenericVariant::ToJson` and Java's `Float.toString`.
                std::string str = VariantJsonUtils::JavaFloatToString(value);
                return std::optional<Literal>(Literal(FieldType::STRING, str.data(), str.size()));
            }
            input = Literal(value);
            input_type = arrow::float32();
            break;
        }
        case VariantValueType::kBinary: {
            PAIMON_ASSIGN_OR_RAISE(std::string_view value, variant->GetBinary());
            input = Literal(FieldType::BINARY, value.data(), value.size());
            input_type = arrow::binary();
            break;
        }
        case VariantValueType::kTimestamp:
        case VariantValueType::kTimestampNtz: {
            PAIMON_ASSIGN_OR_RAISE(int64_t micros, variant->GetLong());
            // Floor the division so negative epochs keep a non-negative sub-millisecond part.
            int64_t millis = micros / 1000;
            auto sub_micros = static_cast<int32_t>(micros % 1000);
            if (sub_micros < 0) {
                millis -= 1;
                sub_micros += 1000;
            }
            input = Literal(Timestamp::FromEpochMillis(millis, sub_micros * 1000));
            input_type = variant_type == VariantValueType::kTimestamp
                             ? arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")
                             : arrow::timestamp(arrow::TimeUnit::MICRO);
            break;
        }
        default:
            return Status::Invalid(fmt::format("Unsupported variant type in variant_get: {}",
                                               static_cast<int32_t>(variant_type)));
    }

    if (input_type->Equals(*target_type)) {
        return input;
    }

    PAIMON_ASSIGN_OR_RAISE(FieldType input_field_type,
                           FieldTypeUtils::ConvertToFieldType(input_type->id()));
    PAIMON_ASSIGN_OR_RAISE(FieldType target_field_type,
                           FieldTypeUtils::ConvertToFieldType(target_type->id()));
    std::shared_ptr<CastExecutor> executor =
        CastExecutorFactory::GetCastExecutorFactory()->GetCastExecutor(input_field_type,
                                                                       target_field_type);
    if (executor == nullptr) {
        return InvalidCast(variant, target_type, cast_args);
    }
    Result<Literal> cast_result = executor->Cast(*input, target_type);
    if (!cast_result.ok()) {
        return InvalidCast(variant, target_type, cast_args);
    }
    return std::optional<Literal>(std::move(cast_result).value());
}

Status AppendLiteralToBuilder(const Literal& literal,
                              const std::shared_ptr<arrow::DataType>& target_type,
                              arrow::ArrayBuilder* builder) {
    switch (target_type->id()) {
        case arrow::Type::type::BOOL:
            return ToPaimonStatus(
                static_cast<arrow::BooleanBuilder*>(builder)->Append(literal.GetValue<bool>()));
        case arrow::Type::type::INT8:
            return ToPaimonStatus(
                static_cast<arrow::Int8Builder*>(builder)->Append(literal.GetValue<int8_t>()));
        case arrow::Type::type::INT16:
            return ToPaimonStatus(
                static_cast<arrow::Int16Builder*>(builder)->Append(literal.GetValue<int16_t>()));
        case arrow::Type::type::INT32:
            return ToPaimonStatus(
                static_cast<arrow::Int32Builder*>(builder)->Append(literal.GetValue<int32_t>()));
        case arrow::Type::type::INT64:
            return ToPaimonStatus(
                static_cast<arrow::Int64Builder*>(builder)->Append(literal.GetValue<int64_t>()));
        case arrow::Type::type::FLOAT:
            return ToPaimonStatus(
                static_cast<arrow::FloatBuilder*>(builder)->Append(literal.GetValue<float>()));
        case arrow::Type::type::DOUBLE:
            return ToPaimonStatus(
                static_cast<arrow::DoubleBuilder*>(builder)->Append(literal.GetValue<double>()));
        case arrow::Type::type::STRING:
            return ToPaimonStatus(static_cast<arrow::StringBuilder*>(builder)->Append(
                literal.GetValue<std::string>()));
        case arrow::Type::type::BINARY:
            return ToPaimonStatus(static_cast<arrow::BinaryBuilder*>(builder)->Append(
                literal.GetValue<std::string>()));
        case arrow::Type::type::DATE32:
            return ToPaimonStatus(
                static_cast<arrow::Date32Builder*>(builder)->Append(literal.GetValue<int32_t>()));
        case arrow::Type::type::TIMESTAMP: {
            auto timestamp = literal.GetValue<Timestamp>();
            const auto& timestamp_type = static_cast<const arrow::TimestampType&>(*target_type);
            int64_t value;
            switch (timestamp_type.unit()) {
                case arrow::TimeUnit::SECOND:
                    value = timestamp.GetMillisecond() / 1000;
                    break;
                case arrow::TimeUnit::MILLI:
                    value = timestamp.GetMillisecond();
                    break;
                case arrow::TimeUnit::MICRO:
                    value = timestamp.ToMicrosecond();
                    break;
                case arrow::TimeUnit::NANO:
                    value = timestamp.ToNanosecond();
                    break;
                default:
                    return Status::Invalid("Unsupported timestamp unit");
            }
            return ToPaimonStatus(static_cast<arrow::TimestampBuilder*>(builder)->Append(value));
        }
        case arrow::Type::type::DECIMAL128: {
            auto decimal = literal.GetValue<Decimal>();
            arrow::Decimal128 value(static_cast<int64_t>(decimal.HighBits()), decimal.LowBits());
            return ToPaimonStatus(static_cast<arrow::Decimal128Builder*>(builder)->Append(value));
        }
        default:
            return Status::Invalid(
                fmt::format("Unsupported variant_get target type: {}", target_type->ToString()));
    }
}

}  // namespace

Status VariantGetExecutor::CastToBuilder(const std::shared_ptr<GenericVariant>& variant,
                                         const std::shared_ptr<arrow::Field>& target_field,
                                         const VariantCastArgs& cast_args,
                                         const std::shared_ptr<MemoryPool>& pool,
                                         arrow::ArrayBuilder* builder) {
    if (variant == nullptr) {
        return ToPaimonStatus(builder->AppendNull());
    }

    auto invalid_cast = [&]() -> Status {
        if (cast_args.fail_on_error) {
            PAIMON_ASSIGN_OR_RAISE(std::string json, variant->ToJson(cast_args.zone_id));
            return Status::Invalid(
                fmt::format("Invalid cast {} to {}", json, target_field->type()->ToString()));
        }
        return ToPaimonStatus(builder->AppendNull());
    };

    if (VariantTypeUtils::IsVariantField(target_field)) {
        VariantBuilder variant_builder(/*allow_duplicate_keys=*/false);
        PAIMON_RETURN_NOT_OK(variant_builder.AppendVariant(*variant));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GenericVariant> copied, variant_builder.Build(pool));
        auto* struct_builder = static_cast<arrow::StructBuilder*>(builder);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder->Append());
        PAIMON_ASSIGN_OR_RAISE(std::string_view value, copied->Value());
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            static_cast<arrow::BinaryBuilder*>(struct_builder->field_builder(0))->Append(value));
        return ToPaimonStatus(static_cast<arrow::BinaryBuilder*>(struct_builder->field_builder(1))
                                  ->Append(copied->Metadata()));
    }

    PAIMON_ASSIGN_OR_RAISE(VariantValueType variant_type, variant->GetType());
    if (variant_type == VariantValueType::kNull) {
        return ToPaimonStatus(builder->AppendNull());
    }

    switch (target_field->type()->id()) {
        case arrow::Type::type::STRUCT: {
            if (variant_type != VariantValueType::kObject) {
                return invalid_cast();
            }
            auto* struct_builder = static_cast<arrow::StructBuilder*>(builder);
            PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder->Append());
            const auto& struct_type = static_cast<const arrow::StructType&>(*target_field->type());
            for (int i = 0; i < struct_type.num_fields(); ++i) {
                const std::shared_ptr<arrow::Field>& child_field = struct_type.field(i);
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GenericVariant> child,
                                       variant->GetFieldByKey(child_field->name()));
                PAIMON_RETURN_NOT_OK(CastToBuilder(child, child_field, cast_args, pool,
                                                   struct_builder->field_builder(i)));
            }
            return Status::OK();
        }
        case arrow::Type::type::MAP: {
            const auto& map_type = static_cast<const arrow::MapType&>(*target_field->type());
            if (map_type.key_type()->id() != arrow::Type::type::STRING ||
                variant_type != VariantValueType::kObject) {
                return invalid_cast();
            }
            auto* map_builder = static_cast<arrow::MapBuilder*>(builder);
            PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder->Append());
            PAIMON_ASSIGN_OR_RAISE(int32_t object_size, variant->ObjectSize());
            for (int32_t i = 0; i < object_size; ++i) {
                PAIMON_ASSIGN_OR_RAISE(std::optional<GenericVariant::ObjectField> field,
                                       variant->GetFieldAtIndex(i));
                if (!field.has_value()) {
                    return Status::Invalid(fmt::format("Malformed variant object at index {}", i));
                }
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    static_cast<arrow::StringBuilder*>(map_builder->key_builder())
                        ->Append(field->key));
                PAIMON_RETURN_NOT_OK(CastToBuilder(field->value, map_type.item_field(), cast_args,
                                                   pool, map_builder->item_builder()));
            }
            return Status::OK();
        }
        case arrow::Type::type::LIST: {
            if (variant_type != VariantValueType::kArray) {
                return invalid_cast();
            }
            auto* list_builder = static_cast<arrow::ListBuilder*>(builder);
            PAIMON_RETURN_NOT_OK_FROM_ARROW(list_builder->Append());
            const auto& list_type = static_cast<const arrow::ListType&>(*target_field->type());
            PAIMON_ASSIGN_OR_RAISE(int32_t array_size, variant->ArraySize());
            for (int32_t i = 0; i < array_size; ++i) {
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GenericVariant> element,
                                       variant->GetElementAtIndex(i));
                PAIMON_RETURN_NOT_OK(CastToBuilder(element, list_type.value_field(), cast_args,
                                                   pool, list_builder->value_builder()));
            }
            return Status::OK();
        }
        default: {
            PAIMON_ASSIGN_OR_RAISE(std::optional<Literal> literal,
                                   CastVariant(variant, target_field->type(), cast_args));
            if (!literal.has_value()) {
                return ToPaimonStatus(builder->AppendNull());
            }
            return AppendLiteralToBuilder(*literal, target_field->type(), builder);
        }
    }
}

Result<std::shared_ptr<arrow::Array>> VariantGetExecutor::GetAsArrow(
    const std::shared_ptr<GenericVariant>& variant, const std::string& path,
    const std::shared_ptr<arrow::Field>& target_field, const VariantCastArgs& cast_args,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GenericVariant> extracted, ExtractByPath(variant, path));
    std::unique_ptr<arrow::ArrayBuilder> builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::MakeBuilder(arrow_pool.get(), target_field->type(), &builder));
    PAIMON_RETURN_NOT_OK(CastToBuilder(extracted, target_field, cast_args, pool, builder.get()));
    std::shared_ptr<arrow::Array> array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Finish(&array));
    return array;
}

Result<std::shared_ptr<GenericVariant>> VariantGetExecutor::ExtractByPath(
    const std::shared_ptr<GenericVariant>& variant, const std::string& path) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<VariantPathSegment> segments,
                           VariantPathSegment::Parse(path));
    std::shared_ptr<GenericVariant> current = variant;
    for (const VariantPathSegment& segment : segments) {
        PAIMON_ASSIGN_OR_RAISE(VariantValueType type, current->GetType());
        if (segment.kind == VariantPathSegment::Kind::kObjectExtraction &&
            type == VariantValueType::kObject) {
            PAIMON_ASSIGN_OR_RAISE(current, current->GetFieldByKey(segment.key));
        } else if (segment.kind == VariantPathSegment::Kind::kArrayExtraction &&
                   type == VariantValueType::kArray) {
            PAIMON_ASSIGN_OR_RAISE(current, current->GetElementAtIndex(segment.index));
        } else {
            return std::shared_ptr<GenericVariant>(nullptr);
        }
        if (current == nullptr) {
            return std::shared_ptr<GenericVariant>(nullptr);
        }
    }
    return current;
}

Result<std::optional<Literal>> VariantGetExecutor::Get(
    const std::shared_ptr<GenericVariant>& variant, const std::string& path,
    const std::shared_ptr<arrow::DataType>& target_type, const VariantCastArgs& cast_args) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GenericVariant> extracted, ExtractByPath(variant, path));
    if (extracted == nullptr) {
        return std::optional<Literal>(std::nullopt);
    }
    return CastVariant(extracted, target_type, cast_args);
}

}  // namespace paimon
