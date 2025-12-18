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

#pragma once

#include <memory>

#include "paimon/core/casting/cast_executor.h"

namespace paimon {
/// support convert between TIMESTAMP and TIMESTAMP_WITH_LOCAL_TIME_ZONE. Check and adjust if there
/// is the precision changes at the same time.
class PAIMON_EXPORT TimestampToTimestampCastExecutor : public CastExecutor {
 public:
    Result<Literal> Cast(const Literal& literal,
                         const std::shared_ptr<arrow::DataType>& target_type) const override;

    Result<std::shared_ptr<arrow::Array>> Cast(const std::shared_ptr<arrow::Array>& array,
                                               const std::shared_ptr<arrow::DataType>& target_type,
                                               arrow::MemoryPool* pool) const override;
};

}  // namespace paimon
