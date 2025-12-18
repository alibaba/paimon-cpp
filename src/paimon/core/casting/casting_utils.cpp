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
#include "paimon/core/casting/casting_utils.h"

#include <memory>

namespace paimon {
Result<std::shared_ptr<arrow::Array>> CastingUtils::Cast(
    const std::shared_ptr<arrow::Array>& src_array,
    const std::shared_ptr<arrow::DataType>& target_type, const arrow::compute::CastOptions& options,
    arrow::MemoryPool* pool) {
    auto src_type = src_array->type();
    if (!arrow::compute::CanCast(*src_type, *target_type)) {
        return Status::Invalid(fmt::format("cast arrow array failed: cannot cast from {} to {}",
                                           src_type->ToString(), target_type->ToString()));
    }
    arrow::compute::ExecContext ctx(pool);
    arrow::TypeHolder type_holder(target_type.get());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> casted_array,
                                      arrow::compute::Cast(*src_array, type_holder, options, &ctx));
    return casted_array;
}

Result<std::shared_ptr<arrow::Array>> CastingUtils::TimestampToTimestampWithTimezone(
    const std::shared_ptr<arrow::Array>& src_array,
    const std::shared_ptr<arrow::TimestampType>& target_type, arrow::MemoryPool* pool) {
    auto src_ts_type =
        arrow::internal::checked_pointer_cast<arrow::TimestampType>(src_array->type());
    assert(src_ts_type);
    if (src_ts_type->unit() != target_type->unit()) {
        return Status::Invalid("in timezone converter, time unit of src and target type mismatch");
    }
    if (!src_ts_type->timezone().empty() || target_type->timezone().empty()) {
        return Status::Invalid(
            "in TimestampToTimestampWithTimezone, src value must be local time (no tz), target "
            "value must be UTC (with tz)");
    }
    arrow::compute::ExecContext ctx(pool);
    arrow::compute::AssumeTimezoneOptions options(target_type->timezone());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        arrow::Datum target_array,
        arrow::compute::AssumeTimezone(arrow::Datum(src_array), options, &ctx));
    return target_array.make_array();
}

Result<std::shared_ptr<arrow::Array>> CastingUtils::TimestampWithTimezoneToTimestamp(
    const std::shared_ptr<arrow::Array>& src_array,
    const std::shared_ptr<arrow::TimestampType>& target_type, arrow::MemoryPool* pool) {
    auto src_ts_type =
        arrow::internal::checked_pointer_cast<arrow::TimestampType>(src_array->type());
    assert(src_ts_type);
    if (src_ts_type->unit() != target_type->unit()) {
        return Status::Invalid("in timezone converter, time unit of src and target type mismatch");
    }
    if (src_ts_type->timezone().empty() || !target_type->timezone().empty()) {
        return Status::Invalid(
            "in TimestampWithTimezoneToTimestamp, src value must be UTC (with tz), target value "
            "must be local time (no tz)");
    }
    arrow::compute::ExecContext ctx(pool);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        arrow::Datum target_array, arrow::compute::LocalTimestamp(arrow::Datum(src_array), &ctx));
    return target_array.make_array();
}

int64_t CastingUtils::GetLongValueFromLiteral(const Literal& literal) {
    auto type = literal.GetType();
    switch (type) {
        case FieldType::TINYINT:
            return static_cast<int64_t>(literal.GetValue<int8_t>());
        case FieldType::SMALLINT:
            return static_cast<int64_t>(literal.GetValue<int16_t>());
        case FieldType::INT:
            return static_cast<int64_t>(literal.GetValue<int32_t>());
        case FieldType::BIGINT:
            return literal.GetValue<int64_t>();
        default:
            return -1;
    }
}

}  // namespace paimon
