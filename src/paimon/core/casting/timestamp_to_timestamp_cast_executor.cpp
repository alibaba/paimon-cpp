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

#include "paimon/core/casting/timestamp_to_timestamp_cast_executor.h"

#include <memory>

#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/casting/casting_utils.h"

namespace paimon {
Result<Literal> TimestampToTimestampCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    return Status::Invalid("do not support cast literal from timestamp to timestamp");
}

Result<std::shared_ptr<arrow::Array>> TimestampToTimestampCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();
    options.allow_time_truncate = true;
    auto src_ts_type = arrow::internal::checked_pointer_cast<arrow::TimestampType>(array->type());
    auto target_ts_type = arrow::internal::checked_pointer_cast<arrow::TimestampType>(target_type);
    assert(src_ts_type && target_ts_type);
    std::shared_ptr<arrow::Array> target_array = array;
    // first, handle timezone
    if (src_ts_type->timezone() != target_ts_type->timezone()) {
        auto target_type_with_tz =
            std::make_shared<arrow::TimestampType>(src_ts_type->unit(), target_ts_type->timezone());
        if (src_ts_type->timezone().empty() && !target_type_with_tz->timezone().empty()) {
            PAIMON_ASSIGN_OR_RAISE(target_array, CastingUtils::TimestampToTimestampWithTimezone(
                                                     target_array, target_type_with_tz, pool));
        } else if (!src_ts_type->timezone().empty() && target_type_with_tz->timezone().empty()) {
            PAIMON_ASSIGN_OR_RAISE(target_array, CastingUtils::TimestampWithTimezoneToTimestamp(
                                                     target_array, target_type_with_tz, pool));
        } else {
            // src and target have non-empty different timezone
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(target_array,
                                              target_array->View(target_type_with_tz));
        }
    }
    // second, handle precision
    if (src_ts_type->unit() != target_ts_type->unit()) {
        PAIMON_ASSIGN_OR_RAISE(target_array,
                               CastingUtils::Cast(target_array, target_type, options, pool));
    }
    return target_array;
}
}  // namespace paimon
