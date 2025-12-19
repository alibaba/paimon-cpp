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
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/builder_nested.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class DataType;
}  // namespace arrow
namespace paimon {
class BinaryRowWriter;
class MemoryPool;
class TableSchema;
}  // namespace paimon

namespace paimon::test {

class DataGenerator {
 public:
    DataGenerator(const std::shared_ptr<TableSchema>& table_schema,
                  const std::shared_ptr<MemoryPool>& memory_pool);

    Result<std::vector<std::unique_ptr<RecordBatch>>> SplitArrayByPartitionAndBucket(
        const std::vector<BinaryRow>& binary_rows);

 private:
    Result<BinaryRow> ExtractPartialRow(const BinaryRow& binary_row,
                                        const std::vector<DataField>& partition_fields);

    static Status WriteBinaryRow(const BinaryRow& src_row, int32_t src_field_id,
                                 const std::shared_ptr<arrow::DataType>& src_type,
                                 int32_t target_field_id, BinaryRowWriter* target_row_writer);

    static Result<std::shared_ptr<arrow::StructBuilder>> MakeStructBuilder(
        const std::vector<DataField>& fields);

    static Status AppendValue(const BinaryRow& row, int32_t field_id,
                              const std::shared_ptr<arrow::DataType>& type,
                              arrow::StructBuilder* struct_builder);

 private:
    std::shared_ptr<TableSchema> table_schema_;
    std::shared_ptr<MemoryPool> memory_pool_;
};

}  // namespace paimon::test
