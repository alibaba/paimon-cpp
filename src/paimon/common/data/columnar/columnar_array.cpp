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

#include "paimon/common/data/columnar/columnar_array.h"

#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_decimal.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "fmt/format.h"
#include "paimon/common/data/columnar/columnar_batch_context.h"
#include "paimon/common/data/columnar/columnar_map.h"
#include "paimon/common/data/columnar/columnar_row_ref.h"
#include "paimon/common/utils/date_time_utils.h"

namespace paimon {
Status ColumnarArray::CheckNoNull() const {
    for (int i = 0; i < length_; i++) {
        if (IsNullAt(i)) {
            return Status::Invalid(fmt::format("row {} is null", i));
        }
    }
    return Status::OK();
}

Decimal ColumnarArray::GetDecimal(int32_t pos, int32_t precision, int32_t scale) const {
    using ArrayType = typename arrow::TypeTraits<arrow::Decimal128Type>::ArrayType;
    auto array = arrow::internal::checked_cast<const ArrayType*>(array_);
    assert(array);
    arrow::Decimal128 decimal(array->GetValue(offset_ + pos));
    return Decimal(precision, scale,
                   static_cast<Decimal::int128_t>(decimal.high_bits()) << 64 | decimal.low_bits());
}

Timestamp ColumnarArray::GetTimestamp(int32_t pos, int32_t precision) const {
    using ArrayType = typename arrow::TypeTraits<arrow::TimestampType>::ArrayType;
    auto array = arrow::internal::checked_cast<const ArrayType*>(array_);
    assert(array);
    int64_t data = array->Value(offset_ + pos);
    auto timestamp_type =
        arrow::internal::checked_pointer_cast<arrow::TimestampType>(array->type());
    // for orc format, data is saved as nano, therefore, Timestamp convert should consider precision
    // in arrow array rather than input precision
    DateTimeUtils::TimeType time_type = DateTimeUtils::GetTimeTypeFromArrowType(timestamp_type);
    auto [milli, nano] = DateTimeUtils::TimestampConverter(
        data, time_type, DateTimeUtils::TimeType::MILLISECOND, DateTimeUtils::TimeType::NANOSECOND);
    return Timestamp(milli, nano);
}

std::shared_ptr<InternalArray> ColumnarArray::GetArray(int32_t pos) const {
    auto list_array = arrow::internal::checked_cast<const arrow::ListArray*>(array_);
    assert(list_array);
    int32_t offset = list_array->value_offset(offset_ + pos);
    int32_t length = list_array->value_length(offset_ + pos);
    return std::make_shared<ColumnarArray>(list_array->values(), pool_, offset, length);
}

std::shared_ptr<InternalMap> ColumnarArray::GetMap(int32_t pos) const {
    auto map_array = arrow::internal::checked_cast<const arrow::MapArray*>(array_);
    assert(map_array);
    int32_t offset = map_array->value_offset(offset_ + pos);
    int32_t length = map_array->value_length(offset_ + pos);
    return std::make_shared<ColumnarMap>(map_array->keys(), map_array->items(), pool_, offset,
                                         length);
}

std::shared_ptr<InternalRow> ColumnarArray::GetRow(int32_t pos, int32_t num_fields) const {
    auto struct_array = arrow::internal::checked_cast<const arrow::StructArray*>(array_);
    assert(struct_array);
    auto row_ctx = std::make_shared<ColumnarBatchContext>(nullptr, struct_array->fields(), pool_);
    return std::make_shared<ColumnarRowRef>(std::move(row_ctx), offset_ + pos);
}

Result<std::vector<char>> ColumnarArray::ToBooleanArray() const {
    PAIMON_RETURN_NOT_OK(CheckNoNull());
    std::vector<char> res(length_);
    for (int i = 0; i < length_; i++) {
        bool element = GetBoolean(i);
        res[i] = element ? static_cast<char>(1) : static_cast<char>(0);
    }
    return res;
}

Result<std::vector<char>> ColumnarArray::ToByteArray() const {
    PAIMON_RETURN_NOT_OK(CheckNoNull());
    std::vector<char> res(length_);
    for (int i = 0; i < length_; i++) {
        res[i] = GetByte(i);
    }
    return res;
}

Result<std::vector<int16_t>> ColumnarArray::ToShortArray() const {
    PAIMON_RETURN_NOT_OK(CheckNoNull());
    std::vector<int16_t> res(length_);
    for (int i = 0; i < length_; i++) {
        res[i] = GetShort(i);
    }
    return res;
}

Result<std::vector<int32_t>> ColumnarArray::ToIntArray() const {
    PAIMON_RETURN_NOT_OK(CheckNoNull());
    std::vector<int32_t> res(length_);
    for (int i = 0; i < length_; i++) {
        res[i] = GetInt(i);
    }
    return res;
}

Result<std::vector<int64_t>> ColumnarArray::ToLongArray() const {
    PAIMON_RETURN_NOT_OK(CheckNoNull());
    std::vector<int64_t> res(length_);
    for (int i = 0; i < length_; i++) {
        res[i] = GetLong(i);
    }
    return res;
}

Result<std::vector<float>> ColumnarArray::ToFloatArray() const {
    PAIMON_RETURN_NOT_OK(CheckNoNull());
    std::vector<float> res(length_);
    for (int i = 0; i < length_; i++) {
        res[i] = GetFloat(i);
    }
    return res;
}

Result<std::vector<double>> ColumnarArray::ToDoubleArray() const {
    PAIMON_RETURN_NOT_OK(CheckNoNull());
    std::vector<double> res(length_);
    for (int i = 0; i < length_; i++) {
        res[i] = GetDouble(i);
    }
    return res;
}

}  // namespace paimon
