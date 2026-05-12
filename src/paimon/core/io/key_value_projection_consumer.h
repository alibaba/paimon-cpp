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

#include "paimon/core/io/row_to_arrow_array_converter.h"
#include "paimon/core/key_value.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/utils/special_field_ids.h"

namespace arrow {
class MemoryPool;
class Schema;
class StructBuilder;
}  // namespace arrow

namespace paimon {
class MemoryPool;
struct KeyValue;

// project KeyValue.value according to target_schema, output is BatchReader::ReadBatch
class KeyValueProjectionConsumer
    : public RowToArrowArrayConverter<KeyValue, BatchReader::ReadBatch> {
 public:
    static constexpr int32_t kSequenceNumberProjection = SpecialFieldIds::SEQUENCE_NUMBER;
    static constexpr int32_t kValueKindProjection = SpecialFieldIds::VALUE_KIND;

    static Result<std::unique_ptr<KeyValueProjectionConsumer>> Create(
        const std::shared_ptr<arrow::Schema>& target_schema,
        const std::vector<int32_t>& target_to_src_mapping, const std::shared_ptr<MemoryPool>& pool);

    Result<BatchReader::ReadBatch> NextBatch(const std::vector<KeyValue>& key_value_vec) override;

    ~KeyValueProjectionConsumer() override = default;

 private:
    KeyValueProjectionConsumer(int32_t reserve_count, std::vector<AppendValueFunc>&& appenders,
                               std::unique_ptr<arrow::StructBuilder>&& array_builder,
                               std::unique_ptr<arrow::MemoryPool>&& arrow_pool,
                               const std::vector<int32_t>& target_to_src_mapping)
        : RowToArrowArrayConverter(reserve_count, std::move(appenders), std::move(array_builder),
                                   std::move(arrow_pool)),
          target_to_src_mapping_(target_to_src_mapping) {}

    std::vector<int32_t> target_to_src_mapping_;
};

}  // namespace paimon
