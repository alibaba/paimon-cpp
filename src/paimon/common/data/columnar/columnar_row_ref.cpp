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

#include "paimon/common/data/columnar/columnar_row_ref.h"

#include <cassert>

#include "arrow/array/array_decimal.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "paimon/common/data/columnar/columnar_array.h"
#include "paimon/common/data/columnar/columnar_map.h"
#include "paimon/common/utils/date_time_utils.h"

namespace paimon {
Decimal ColumnarRowRef::GetDecimal(int32_t pos, int32_t precision, int32_t scale) const {
    using ArrayType = typename arrow::TypeTraits<arrow::Decimal128Type>::ArrayType;
    auto array = arrow::internal::checked_cast<const ArrayType*>(ctx_->array_ptrs[pos]);
    assert(array);
    arrow::Decimal128 decimal(array->GetValue(row_id_));
    return Decimal(precision, scale,
                   static_cast<Decimal::int128_t>(decimal.high_bits()) << 64 | decimal.low_bits());
}

Timestamp ColumnarRowRef::GetTimestamp(int32_t pos, int32_t precision) const {
    using ArrayType = typename arrow::TypeTraits<arrow::TimestampType>::ArrayType;
    auto array = arrow::internal::checked_cast<const ArrayType*>(ctx_->array_ptrs[pos]);
    assert(array);
    int64_t data = array->Value(row_id_);
    auto timestamp_type =
        arrow::internal::checked_pointer_cast<arrow::TimestampType>(array->type());
    // for orc format, data is saved as nano, therefore, Timestamp convert should consider precision
    // in arrow array rather than input precision
    DateTimeUtils::TimeType time_type = DateTimeUtils::GetTimeTypeFromArrowType(timestamp_type);
    auto [milli, nano] = DateTimeUtils::TimestampConverter(
        data, time_type, DateTimeUtils::TimeType::MILLISECOND, DateTimeUtils::TimeType::NANOSECOND);
    return Timestamp(milli, nano);
}

std::shared_ptr<InternalRow> ColumnarRowRef::GetRow(int32_t pos, int32_t num_fields) const {
    auto struct_array =
        arrow::internal::checked_cast<const arrow::StructArray*>(ctx_->array_ptrs[pos]);
    assert(struct_array);
    auto nested_ctx =
        std::make_shared<ColumnarBatchContext>(nullptr, struct_array->fields(), ctx_->pool);
    return std::make_shared<ColumnarRowRef>(std::move(nested_ctx), row_id_);
}

std::shared_ptr<InternalArray> ColumnarRowRef::GetArray(int32_t pos) const {
    auto list_array = arrow::internal::checked_cast<const arrow::ListArray*>(ctx_->array_ptrs[pos]);
    assert(list_array);
    int32_t offset = list_array->value_offset(row_id_);
    int32_t length = list_array->value_length(row_id_);
    return std::make_shared<ColumnarArray>(list_array->values(), ctx_->pool, offset, length);
}

std::shared_ptr<InternalMap> ColumnarRowRef::GetMap(int32_t pos) const {
    auto map_array = arrow::internal::checked_cast<const arrow::MapArray*>(ctx_->array_ptrs[pos]);
    assert(map_array);
    int32_t offset = map_array->value_offset(row_id_);
    int32_t length = map_array->value_length(row_id_);
    return std::make_shared<ColumnarMap>(map_array->keys(), map_array->items(), ctx_->pool, offset,
                                         length);
}

}  // namespace paimon
