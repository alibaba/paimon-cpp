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

#include "paimon/core/mergetree/compact/aggregate/field_aggregate_utils.h"

#include <cstring>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/data/data_getters.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_map.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"

namespace paimon {
namespace {

Result<bool> EqualGetters(const DataGetters& lhs, int32_t lhs_pos, const DataGetters& rhs,
                          int32_t rhs_pos, const std::shared_ptr<arrow::DataType>& type) {
    PAIMON_ASSIGN_OR_RAISE(VariantType lhs_value,
                           FieldAggregateUtils::GetValue(lhs, lhs_pos, type));
    PAIMON_ASSIGN_OR_RAISE(VariantType rhs_value,
                           FieldAggregateUtils::GetValue(rhs, rhs_pos, type));
    return FieldAggregateUtils::Equals(lhs_value, rhs_value, type);
}

Result<bool> EqualRows(const std::shared_ptr<InternalRow>& lhs,
                       const std::shared_ptr<InternalRow>& rhs,
                       const std::shared_ptr<arrow::StructType>& type) {
    if (!lhs || !rhs || lhs->GetFieldCount() != rhs->GetFieldCount() ||
        lhs->GetFieldCount() != type->num_fields()) {
        return lhs == rhs;
    }
    PAIMON_ASSIGN_OR_RAISE(const RowKind* lhs_kind, lhs->GetRowKind());
    PAIMON_ASSIGN_OR_RAISE(const RowKind* rhs_kind, rhs->GetRowKind());
    if (lhs_kind != rhs_kind) {
        return false;
    }
    for (int32_t i = 0; i < type->num_fields(); ++i) {
        PAIMON_ASSIGN_OR_RAISE(bool equal, EqualGetters(*lhs, i, *rhs, i, type->field(i)->type()));
        if (!equal) {
            return false;
        }
    }
    return true;
}

Result<bool> EqualArrays(const std::shared_ptr<InternalArray>& lhs,
                         const std::shared_ptr<InternalArray>& rhs,
                         const std::shared_ptr<arrow::ListType>& type) {
    if (!lhs || !rhs || lhs->Size() != rhs->Size()) {
        return lhs == rhs;
    }
    for (int32_t i = 0; i < lhs->Size(); ++i) {
        PAIMON_ASSIGN_OR_RAISE(bool equal, EqualGetters(*lhs, i, *rhs, i, type->value_type()));
        if (!equal) {
            return false;
        }
    }
    return true;
}

Result<bool> EqualMaps(const std::shared_ptr<InternalMap>& lhs,
                       const std::shared_ptr<InternalMap>& rhs,
                       const std::shared_ptr<arrow::MapType>& type) {
    if (!lhs || !rhs || lhs->Size() != rhs->Size()) {
        return lhs == rhs;
    }
    std::shared_ptr<InternalArray> lhs_keys = lhs->KeyArray();
    std::shared_ptr<InternalArray> lhs_values = lhs->ValueArray();
    std::shared_ptr<InternalArray> rhs_keys = rhs->KeyArray();
    std::shared_ptr<InternalArray> rhs_values = rhs->ValueArray();
    std::vector<bool> matched(rhs->Size(), false);
    for (int32_t i = 0; i < lhs->Size(); ++i) {
        bool found = false;
        for (int32_t j = 0; j < rhs->Size(); ++j) {
            if (matched[j]) {
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(bool key_equal,
                                   EqualGetters(*lhs_keys, i, *rhs_keys, j, type->key_type()));
            if (!key_equal) {
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(bool value_equal,
                                   EqualGetters(*lhs_values, i, *rhs_values, j, type->item_type()));
            if (value_equal) {
                matched[j] = true;
                found = true;
                break;
            }
        }
        if (!found) {
            return false;
        }
    }
    return true;
}

}  // namespace

VariantType FieldAggregateUtils::OwnedBinary(const VariantType& value, MemoryPool* pool) {
    if (DataDefine::GetVariantPtr<std::shared_ptr<Bytes>>(value)) {
        return value;
    }
    std::string_view view = DataDefine::GetStringView(value);
    pooled_unique_ptr<Bytes> owned = Bytes::AllocateBytes(view.size(), pool);
    if (!view.empty()) {
        std::memcpy(owned->data(), view.data(), view.size());
    }
    return VariantType(std::shared_ptr<Bytes>(std::move(owned)));
}

Result<VariantType> FieldAggregateUtils::GetValue(const DataGetters& getters, int32_t pos,
                                                  const std::shared_ptr<arrow::DataType>& type) {
    if (getters.IsNullAt(pos)) {
        return VariantType(NullType());
    }
    switch (type->id()) {
        case arrow::Type::BOOL:
            return VariantType(getters.GetBoolean(pos));
        case arrow::Type::INT8:
            return VariantType(getters.GetByte(pos));
        case arrow::Type::INT16:
            return VariantType(getters.GetShort(pos));
        case arrow::Type::DATE32:
            return VariantType(getters.GetDate(pos));
        case arrow::Type::INT32:
            return VariantType(getters.GetInt(pos));
        case arrow::Type::INT64:
            return VariantType(getters.GetLong(pos));
        case arrow::Type::FLOAT:
            return VariantType(getters.GetFloat(pos));
        case arrow::Type::DOUBLE:
            return VariantType(getters.GetDouble(pos));
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
            return VariantType(getters.GetStringView(pos));
        case arrow::Type::TIMESTAMP: {
            std::shared_ptr<arrow::TimestampType> timestamp_type =
                arrow::internal::checked_pointer_cast<arrow::TimestampType>(type);
            return VariantType(
                getters.GetTimestamp(pos, DateTimeUtils::GetPrecisionFromType(timestamp_type)));
        }
        case arrow::Type::DECIMAL128: {
            const auto* decimal_type =
                arrow::internal::checked_cast<const arrow::Decimal128Type*>(type.get());
            return VariantType(
                getters.GetDecimal(pos, decimal_type->precision(), decimal_type->scale()));
        }
        case arrow::Type::LIST:
            return VariantType(getters.GetArray(pos));
        case arrow::Type::MAP:
            return VariantType(getters.GetMap(pos));
        case arrow::Type::STRUCT:
            return VariantType(getters.GetRow(pos, type->num_fields()));
        default:
            return Status::Invalid(
                fmt::format("type {} is not supported by field aggregation", type->ToString()));
    }
}

Result<bool> FieldAggregateUtils::Equals(const VariantType& lhs, const VariantType& rhs,
                                         const std::shared_ptr<arrow::DataType>& type) {
    bool lhs_null = DataDefine::IsVariantNull(lhs);
    bool rhs_null = DataDefine::IsVariantNull(rhs);
    if (lhs_null || rhs_null) {
        return lhs_null && rhs_null;
    }
    switch (type->id()) {
        case arrow::Type::BOOL:
            return DataDefine::GetVariantValue<bool>(lhs) == DataDefine::GetVariantValue<bool>(rhs);
        case arrow::Type::INT8:
            return DataDefine::GetVariantValue<char>(lhs) == DataDefine::GetVariantValue<char>(rhs);
        case arrow::Type::INT16:
            return DataDefine::GetVariantValue<int16_t>(lhs) ==
                   DataDefine::GetVariantValue<int16_t>(rhs);
        case arrow::Type::DATE32:
        case arrow::Type::INT32:
            return DataDefine::GetVariantValue<int32_t>(lhs) ==
                   DataDefine::GetVariantValue<int32_t>(rhs);
        case arrow::Type::INT64:
            return DataDefine::GetVariantValue<int64_t>(lhs) ==
                   DataDefine::GetVariantValue<int64_t>(rhs);
        case arrow::Type::FLOAT:
            return FieldsComparator::CompareFloatingPoint(
                       DataDefine::GetVariantValue<float>(lhs),
                       DataDefine::GetVariantValue<float>(rhs)) == 0;
        case arrow::Type::DOUBLE:
            return FieldsComparator::CompareFloatingPoint(
                       DataDefine::GetVariantValue<double>(lhs),
                       DataDefine::GetVariantValue<double>(rhs)) == 0;
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
            return DataDefine::GetStringView(lhs) == DataDefine::GetStringView(rhs);
        case arrow::Type::TIMESTAMP:
            return DataDefine::GetVariantValue<Timestamp>(lhs) ==
                   DataDefine::GetVariantValue<Timestamp>(rhs);
        case arrow::Type::DECIMAL128:
            return DataDefine::GetVariantValue<Decimal>(lhs) ==
                   DataDefine::GetVariantValue<Decimal>(rhs);
        case arrow::Type::STRUCT:
            return EqualRows(DataDefine::GetVariantValue<std::shared_ptr<InternalRow>>(lhs),
                             DataDefine::GetVariantValue<std::shared_ptr<InternalRow>>(rhs),
                             arrow::internal::checked_pointer_cast<arrow::StructType>(type));
        case arrow::Type::LIST:
            return EqualArrays(DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(lhs),
                               DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(rhs),
                               arrow::internal::checked_pointer_cast<arrow::ListType>(type));
        case arrow::Type::MAP:
            return EqualMaps(DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(lhs),
                             DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(rhs),
                             arrow::internal::checked_pointer_cast<arrow::MapType>(type));
        default:
            return Status::Invalid(
                fmt::format("type {} is not supported by field aggregation", type->ToString()));
    }
}

}  // namespace paimon
