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

#include "arrow/api.h"
#include "paimon/result.h"

namespace paimon::parquet {

class ParquetTimestampConverter {
 public:
    ParquetTimestampConverter() = delete;
    ~ParquetTimestampConverter() = delete;

    static Result<std::shared_ptr<arrow::DataType>> AdjustTimezone(
        const std::shared_ptr<arrow::DataType>& src_data_type);

    static Result<bool> NeedCastArrayForTimestamp(
        const std::shared_ptr<arrow::DataType>& src_data_type,
        const std::shared_ptr<arrow::DataType>& target_data_type);

    static Result<std::shared_ptr<arrow::Array>> CastArrayForTimestamp(
        const std::shared_ptr<arrow::Array>& array,
        const std::shared_ptr<arrow::DataType>& target_data_type,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool);
};

}  // namespace paimon::parquet
