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

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "paimon/common/data/generic_row.h"
#include "paimon/core/io/row_to_arrow_array_converter.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace arrow {
class MemoryPool;
class StructBuilder;
}  // namespace arrow

namespace paimon {

/// Converts in-memory GenericRow values into a struct Arrow array.
class GenericRowToArrowArrayConverter
    : public RowToArrowArrayConverter<GenericRow, BatchReader::ReadBatch> {
 public:
    static Result<std::unique_ptr<GenericRowToArrowArrayConverter>> Create(
        const std::shared_ptr<arrow::Schema>& schema, arrow::MemoryPool* pool);

    Result<BatchReader::ReadBatch> NextBatch(const std::vector<GenericRow>& rows) override;

 private:
    GenericRowToArrowArrayConverter(int32_t reserve_count, std::vector<AppendValueFunc>&& appenders,
                                    std::unique_ptr<arrow::StructBuilder>&& array_builder,
                                    std::unique_ptr<arrow::MemoryPool>&& arrow_pool)
        : RowToArrowArrayConverter(reserve_count, std::move(appenders), std::move(array_builder),
                                   std::move(arrow_pool)) {}
};

}  // namespace paimon
