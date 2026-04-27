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

#include "paimon/core/bucket/bucket_select_converter.h"

#include <cassert>
#include <set>
#include <string>
#include <utility>

#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/core/bucket/bucket_function.h"
#include "paimon/core/bucket/default_bucket_function.h"
#include "paimon/core/bucket/hive_bucket_function.h"
#include "paimon/core/bucket/mod_bucket_function.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_utils.h"

namespace paimon {

Result<std::optional<int32_t>> BucketSelectConverter::Convert(
    const std::shared_ptr<Predicate>& predicate, const std::vector<std::string>& bucket_key_names,
    const std::vector<std::shared_ptr<arrow::DataType>>& bucket_key_arrow_types,
    BucketFunctionType bucket_function_type, int32_t num_buckets, MemoryPool* pool) {
    assert(pool);
    if (!predicate || bucket_key_names.empty() || num_buckets <= 0) {
        return std::optional<int32_t>(std::nullopt);
    }

    if (bucket_key_names.size() != bucket_key_arrow_types.size()) {
        return Status::Invalid(
            "bucket_key_names and bucket_key_arrow_types must have the same size");
    }

    // Derive FieldTypes from Arrow types
    std::vector<FieldType> bucket_key_types;
    bucket_key_types.reserve(bucket_key_arrow_types.size());
    for (const auto& arrow_type : bucket_key_arrow_types) {
        PAIMON_ASSIGN_OR_RAISE(FieldType ft, FieldTypeUtils::ConvertToFieldType(arrow_type->id()));
        bucket_key_types.push_back(ft);
    }

    auto literals_opt = ExtractEqualLiterals(predicate, bucket_key_names);
    if (!literals_opt.has_value()) {
        return std::optional<int32_t>(std::nullopt);
    }

    const auto& literals_map = literals_opt.value();
    auto num_fields = static_cast<int32_t>(bucket_key_names.size());

    // Build a BinaryRow from the extracted literals
    BinaryRow row(num_fields);
    BinaryRowWriter writer(&row, /*initial_size=*/1024, pool);
    writer.Reset();

    for (int32_t i = 0; i < num_fields; i++) {
        const auto& field_name = bucket_key_names[i];
        const auto& literal = literals_map.at(field_name);
        PAIMON_RETURN_NOT_OK(
            WriteLiteralToRow(i, literal, bucket_key_types[i], bucket_key_arrow_types[i], &writer));
    }
    writer.Complete();

    // Create the bucket function and compute the bucket
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<BucketFunction> bucket_function,
        CreateBucketFunction(bucket_function_type, bucket_key_types, bucket_key_arrow_types));
    int32_t bucket = bucket_function->Bucket(row, num_buckets);
    return std::optional<int32_t>(bucket);
}

std::optional<std::map<std::string, Literal>> BucketSelectConverter::ExtractEqualLiterals(
    const std::shared_ptr<Predicate>& predicate, const std::vector<std::string>& bucket_key_names) {
    std::set<std::string> key_set(bucket_key_names.begin(), bucket_key_names.end());
    std::map<std::string, Literal> result;

    auto splits = PredicateUtils::SplitAnd(predicate);
    for (const auto& split : splits) {
        auto* leaf = dynamic_cast<const LeafPredicate*>(split.get());
        if (!leaf) {
            continue;
        }
        // TODO(liangjie.liang): Support IN and OR predicates to enable multi-bucket pruning
        if (leaf->GetFunction().GetType() != Function::Type::EQUAL) {
            continue;
        }
        const auto& field_name = leaf->FieldName();
        if (key_set.find(field_name) == key_set.end()) {
            continue;
        }
        const auto& literals = leaf->Literals();
        if (literals.size() != 1 || literals[0].IsNull()) {
            continue;
        }
        // Only record the first EQUAL for each field
        if (result.find(field_name) == result.end()) {
            result.emplace(field_name, literals[0]);
        }
    }

    // Check all bucket key fields are covered
    for (const auto& key_name : bucket_key_names) {
        if (result.find(key_name) == result.end()) {
            return std::nullopt;
        }
    }
    return result;
}

Status BucketSelectConverter::WriteLiteralToRow(int32_t pos, const Literal& literal,
                                                FieldType field_type,
                                                const std::shared_ptr<arrow::DataType>& arrow_type,
                                                BinaryRowWriter* writer) {
    switch (field_type) {
        case FieldType::BOOLEAN:
            writer->WriteBoolean(pos, literal.GetValue<bool>());
            break;
        case FieldType::TINYINT:
            writer->WriteByte(pos, literal.GetValue<int8_t>());
            break;
        case FieldType::SMALLINT:
            writer->WriteShort(pos, literal.GetValue<int16_t>());
            break;
        case FieldType::INT:
        case FieldType::DATE:
            writer->WriteInt(pos, literal.GetValue<int32_t>());
            break;
        case FieldType::BIGINT:
            writer->WriteLong(pos, literal.GetValue<int64_t>());
            break;
        case FieldType::FLOAT:
            writer->WriteFloat(pos, literal.GetValue<float>());
            break;
        case FieldType::DOUBLE:
            writer->WriteDouble(pos, literal.GetValue<double>());
            break;
        case FieldType::STRING:
        case FieldType::BINARY: {
            auto value = literal.GetValue<std::string>();
            writer->WriteStringView(pos, std::string_view{value});
            break;
        }
        case FieldType::TIMESTAMP: {
            auto ts = literal.GetValue<Timestamp>();
            auto timestamp_type =
                arrow::internal::checked_pointer_cast<arrow::TimestampType>(arrow_type);
            int32_t precision = DateTimeUtils::GetPrecisionFromType(timestamp_type);
            writer->WriteTimestamp(pos, ts, precision);
            break;
        }
        case FieldType::DECIMAL: {
            auto dec = literal.GetValue<Decimal>();
            const auto* decimal_type =
                arrow::internal::checked_cast<const arrow::Decimal128Type*>(arrow_type.get());
            int32_t precision = decimal_type->precision();
            writer->WriteDecimal(pos, dec, precision);
            break;
        }
        default:
            return Status::Invalid(
                fmt::format("unsupported field type {} for bucket select conversion",
                            static_cast<int>(field_type)));
    }
    return Status::OK();
}

Result<std::unique_ptr<BucketFunction>> BucketSelectConverter::CreateBucketFunction(
    BucketFunctionType type, const std::vector<FieldType>& bucket_key_types,
    const std::vector<std::shared_ptr<arrow::DataType>>& bucket_key_arrow_types) {
    switch (type) {
        case BucketFunctionType::DEFAULT:
            return std::unique_ptr<BucketFunction>(std::make_unique<DefaultBucketFunction>());
        case BucketFunctionType::MOD: {
            if (bucket_key_types.size() != 1) {
                return Status::Invalid("MOD bucket function requires exactly one bucket key field");
            }
            return ModBucketFunction::Create(bucket_key_types[0]);
        }
        case BucketFunctionType::HIVE: {
            std::vector<HiveFieldInfo> field_infos;
            field_infos.reserve(bucket_key_types.size());
            for (size_t i = 0; i < bucket_key_types.size(); i++) {
                if (bucket_key_types[i] == FieldType::DECIMAL) {
                    const auto* decimal_type =
                        arrow::internal::checked_cast<const arrow::Decimal128Type*>(
                            bucket_key_arrow_types[i].get());
                    field_infos.emplace_back(bucket_key_types[i], decimal_type->precision(),
                                             decimal_type->scale());
                } else {
                    field_infos.emplace_back(bucket_key_types[i]);
                }
            }
            return HiveBucketFunction::Create(field_infos);
        }
        default:
            return Status::Invalid(
                fmt::format("unsupported bucket function type: {}", static_cast<int>(type)));
    }
}

}  // namespace paimon
