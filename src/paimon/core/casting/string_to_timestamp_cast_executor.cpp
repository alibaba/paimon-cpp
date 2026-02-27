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

#include "paimon/core/casting/string_to_timestamp_cast_executor.h"

#include <cassert>
#include <string>
#include <utility>

#include "arrow/compute/cast.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/status.h"

namespace arrow {
class MemoryPool;
class Array;
}  // namespace arrow

namespace paimon {

Result<Literal> StringToTimestampCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    return Status::Invalid("do not support cast literal from string to timestamp");
}

Result<std::shared_ptr<arrow::Array>> StringToTimestampCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    auto timestamp_type = arrow::internal::checked_pointer_cast<arrow::TimestampType>(target_type);
    assert(timestamp_type);
    auto target_type_no_tz = arrow::timestamp(timestamp_type->unit());
    arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> target_array,
                           CastingUtils::Cast(array, target_type_no_tz, options, pool));
    if (!timestamp_type->timezone().empty()) {
        PAIMON_ASSIGN_OR_RAISE(target_array, CastingUtils::TimestampToTimestampWithTimezone(
                                                 target_array, timestamp_type, pool));
    }
    return target_array;
}

}  // namespace paimon
