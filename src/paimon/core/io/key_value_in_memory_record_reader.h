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
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/merge_function_wrapper.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace paimon {
struct ColumnarBatchContext;
class FieldsComparator;
class MemoryPool;
class Metrics;
template <typename T>
class MergeFunctionWrapper;

class KeyValueInMemoryRecordReader : public KeyValueRecordReader {
 public:
    KeyValueInMemoryRecordReader(
        int64_t last_sequence_num, std::shared_ptr<arrow::StructArray>&& struct_array,
        std::vector<RecordBatch::RowKind>&& row_kinds, const std::vector<std::string>& primary_keys,
        const std::vector<std::string>& user_defined_sequence_fields,
        const std::shared_ptr<FieldsComparator>& key_comparator,
        const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper,
        const std::shared_ptr<MemoryPool>& pool);

    class Iterator : public KeyValueRecordReader::Iterator {
     public:
        explicit Iterator(KeyValueInMemoryRecordReader* reader) : reader_(reader) {}
        bool HasNext() const override {
            return cursor_ < reader_->value_struct_array_->length();
        }
        Result<KeyValue> Next() override;

     private:
        int64_t cursor_ = 0;
        KeyValueInMemoryRecordReader* reader_ = nullptr;
    };

    Result<std::unique_ptr<KeyValueRecordReader::Iterator>> NextBatch() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return std::make_shared<MetricsImpl>();
    }

    void Close() override;

 private:
    Result<std::shared_ptr<arrow::NumericArray<arrow::UInt64Type>>> SortBatch() const;

 private:
    bool visited_ = false;
    int64_t last_sequence_num_ = -1;
    std::vector<std::string> primary_keys_;
    std::vector<std::string> user_defined_sequence_fields_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::StructArray> value_struct_array_;
    std::vector<RecordBatch::RowKind> row_kinds_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper_;

    std::shared_ptr<arrow::NumericArray<arrow::UInt64Type>> sort_indices_;
    std::shared_ptr<ColumnarBatchContext> key_ctx_;
    std::shared_ptr<ColumnarBatchContext> value_ctx_;
};
}  // namespace paimon
