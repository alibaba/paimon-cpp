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

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/array/builder_primitive.h"
#include "paimon/core/io/row_to_arrow_array_converter.h"
#include "paimon/core/key_value.h"
#include "paimon/result.h"

namespace arrow {
class MemoryPool;
class Schema;
class StructBuilder;
}  // namespace arrow

namespace paimon {
class MemoryPool;

// project KeyValue.value according to value_schema, output KeyValueBatch, collect delete row count
// and min/max key, also add special fields to output array (e.g., sequence num)
class KeyValueMetaProjectionConsumer : public RowToArrowArrayConverter<KeyValue, KeyValueBatch> {
 public:
    static Result<std::unique_ptr<KeyValueMetaProjectionConsumer>> Create(
        const std::shared_ptr<arrow::Schema>& target_schema,
        const std::shared_ptr<MemoryPool>& pool);

    // target_to_src_mapping is the mapping excluding special fields.
    static Result<std::unique_ptr<KeyValueMetaProjectionConsumer>> Create(
        const std::shared_ptr<arrow::Schema>& target_schema,
        const std::vector<int32_t>& target_to_src_mapping, const std::shared_ptr<MemoryPool>& pool);

    Result<KeyValueBatch> NextBatch(const std::vector<KeyValue>& key_value_vec) override;

 private:
    KeyValueMetaProjectionConsumer(int32_t reserve_count, std::vector<AppendValueFunc>&& appenders,
                                   std::unique_ptr<arrow::StructBuilder>&& array_builder,
                                   std::unique_ptr<arrow::MemoryPool>&& arrow_pool,
                                   const std::vector<int32_t>& target_to_src_mapping,
                                   arrow::Int64Builder* sequence_appender,
                                   arrow::Int8Builder* value_kind_appender)
        : RowToArrowArrayConverter(reserve_count, std::move(appenders), std::move(array_builder),
                                   std::move(arrow_pool)),
          target_to_src_mapping_(target_to_src_mapping),
          sequence_appender_(sequence_appender),
          value_kind_appender_(value_kind_appender) {}

 private:
    std::vector<int32_t> target_to_src_mapping_;
    arrow::Int64Builder* sequence_appender_;
    arrow::Int8Builder* value_kind_appender_;
};
}  // namespace paimon
