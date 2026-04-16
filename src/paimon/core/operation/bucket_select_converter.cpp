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

#include "paimon/core/operation/bucket_select_converter.h"

#include <cmath>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_utils.h"

namespace paimon {
namespace {

// Split predicate by OR (same logic as SplitAnd but for OR type).
std::vector<std::shared_ptr<Predicate>> SplitOr(const std::shared_ptr<Predicate>& predicate) {
    std::vector<std::shared_ptr<Predicate>> result;
    if (predicate == nullptr) {
        return result;
    }
    if (auto compound = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
        if (compound->GetFunction().GetType() == Function::Type::OR) {
            for (const auto& child : compound->Children()) {
                auto sub = SplitOr(child);
                result.insert(result.end(), sub.begin(), sub.end());
            }
            return result;
        }
    }
    result.push_back(predicate);
    return result;
}

// Write a Literal value into a BinaryRowWriter at the given column position.
// The FieldType determines how the value is serialized.
// @param timestamp_precision: precision for TIMESTAMP type (0=second, 3=milli, 6=micro, 9=nano).
Status WriteLiteralToBinaryRow(BinaryRowWriter* writer, int32_t col_id, const Literal& literal,
                               FieldType field_type, int32_t timestamp_precision = 3) {
    if (literal.IsNull()) {
        writer->SetNullAt(col_id);
        return Status::OK();
    }
    switch (field_type) {
        case FieldType::BOOLEAN:
            writer->WriteBoolean(col_id, literal.GetValue<bool>());
            break;
        case FieldType::TINYINT:
            writer->WriteByte(col_id, literal.GetValue<int8_t>());
            break;
        case FieldType::SMALLINT:
            writer->WriteShort(col_id, literal.GetValue<int16_t>());
            break;
        case FieldType::INT:
            writer->WriteInt(col_id, literal.GetValue<int32_t>());
            break;
        case FieldType::BIGINT:
            writer->WriteLong(col_id, literal.GetValue<int64_t>());
            break;
        case FieldType::FLOAT:
            writer->WriteFloat(col_id, literal.GetValue<float>());
            break;
        case FieldType::DOUBLE:
            writer->WriteDouble(col_id, literal.GetValue<double>());
            break;
        case FieldType::DATE:
            writer->WriteInt(col_id, literal.GetValue<int32_t>());
            break;
        case FieldType::STRING: {
            auto val = literal.GetValue<std::string>();
            writer->WriteStringView(col_id, std::string_view(val));
            break;
        }
        case FieldType::BINARY: {
            auto val = literal.GetValue<std::string>();
            writer->WriteStringView(col_id, std::string_view(val));
            break;
        }
        case FieldType::TIMESTAMP: {
            auto ts = literal.GetValue<Timestamp>();
            writer->WriteTimestamp(col_id, ts, timestamp_precision);
            break;
        }
        case FieldType::DECIMAL: {
            auto dec = literal.GetValue<Decimal>();
            writer->WriteDecimal(col_id, dec, dec.Precision());
            break;
        }
        default:
            return Status::Invalid("unsupported field type for bucket key");
    }
    return Status::OK();
}

}  // namespace

Result<std::optional<std::set<int32_t>>> BucketSelectConverter::Convert(
    const std::shared_ptr<Predicate>& predicate, const std::vector<std::string>& bucket_keys,
    int32_t num_buckets, const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<MemoryPool>& pool) {
    if (!predicate || bucket_keys.empty() || num_buckets <= 0) {
        return std::optional<std::set<int32_t>>(std::nullopt);
    }

    // Build bucket key name set and name->index map
    std::set<std::string> bucket_key_set(bucket_keys.begin(), bucket_keys.end());

    // Per-column collected values: bucket_key_name -> vector<Literal>
    // Each bucket key column must have exactly one AND-child that provides values.
    std::map<std::string, std::vector<Literal>> column_values;

    // Split by AND
    auto and_children = PredicateUtils::SplitAnd(predicate);

    for (const auto& and_child : and_children) {
        // Split by OR
        auto or_children = SplitOr(and_child);

        // All OR branches must reference the same bucket key column with EQUAL/IN
        std::string reference_field;
        std::vector<Literal> values;
        bool valid = true;

        for (const auto& or_child : or_children) {
            auto leaf = std::dynamic_pointer_cast<LeafPredicate>(or_child);
            if (!leaf) {
                valid = false;
                break;
            }
            const auto& field_name = leaf->FieldName();
            if (bucket_key_set.find(field_name) == bucket_key_set.end()) {
                valid = false;
                break;
            }
            if (reference_field.empty()) {
                reference_field = field_name;
            } else if (reference_field != field_name) {
                valid = false;
                break;
            }
            auto func_type = leaf->GetFunction().GetType();
            if (func_type != Function::Type::EQUAL && func_type != Function::Type::IN) {
                valid = false;
                break;
            }
            for (const auto& lit : leaf->Literals()) {
                if (!lit.IsNull()) {
                    values.push_back(lit);
                }
            }
        }

        if (!valid || reference_field.empty()) {
            continue;
        }

        if (column_values.find(reference_field) != column_values.end()) {
            // Repeated equals on same column in AND? Ambiguous, bail out.
            return std::optional<std::set<int32_t>>(std::nullopt);
        }
        column_values[reference_field] = std::move(values);
    }

    // Check all bucket key columns have values
    for (const auto& key : bucket_keys) {
        if (column_values.find(key) == column_values.end()) {
            return std::optional<std::set<int32_t>>(std::nullopt);
        }
    }

    // Check cartesian product size
    int64_t row_count = 1;
    for (const auto& key : bucket_keys) {
        row_count *= static_cast<int64_t>(column_values[key].size());
        if (row_count > kMaxValues) {
            return std::optional<std::set<int32_t>>(std::nullopt);
        }
    }

    // Get field types and timestamp precisions for bucket keys (ordered)
    std::vector<FieldType> field_types;
    std::vector<int32_t> timestamp_precisions;
    field_types.reserve(bucket_keys.size());
    timestamp_precisions.reserve(bucket_keys.size());
    for (const auto& key : bucket_keys) {
        PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema->GetField(key));
        PAIMON_ASSIGN_OR_RAISE(FieldType ft, table_schema->GetFieldType(key));
        field_types.push_back(ft);
        int32_t precision = 3;  // default millisecond
        if (ft == FieldType::TIMESTAMP && field.Type()->id() == arrow::Type::TIMESTAMP) {
            auto ts_type =
                arrow::internal::checked_pointer_cast<arrow::TimestampType>(field.Type());
            precision = DateTimeUtils::GetPrecisionFromType(ts_type);
        }
        timestamp_precisions.push_back(precision);
    }

    int32_t num_fields = static_cast<int32_t>(bucket_keys.size());

    // Compute bucket IDs via cartesian product
    // Use recursive approach to iterate all combinations
    std::set<int32_t> bucket_ids;
    BinaryRow bucket_row(num_fields);
    BinaryRowWriter writer(&bucket_row, /*initial_size=*/1024, pool.get());

    // Build the cartesian product iteratively using indices
    std::vector<int64_t> sizes;
    sizes.reserve(bucket_keys.size());
    for (const auto& key : bucket_keys) {
        sizes.push_back(static_cast<int64_t>(column_values[key].size()));
    }

    for (int64_t combo = 0; combo < row_count; ++combo) {
        writer.Reset();
        int64_t remainder = combo;
        for (int32_t col = num_fields - 1; col >= 0; --col) {
            int64_t idx = remainder % sizes[col];
            remainder /= sizes[col];
            PAIMON_RETURN_NOT_OK(
                WriteLiteralToBinaryRow(&writer, col, column_values[bucket_keys[col]][idx],
                                        field_types[col], timestamp_precisions[col]));
        }
        writer.Complete();
        int32_t bucket = std::abs(bucket_row.HashCode() % num_buckets);
        bucket_ids.insert(bucket);
    }

    return std::optional<std::set<int32_t>>(bucket_ids);
}

}  // namespace paimon
