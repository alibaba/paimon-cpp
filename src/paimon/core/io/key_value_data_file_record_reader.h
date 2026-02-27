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

#include "arrow/type_fwd.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/key_value.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace arrow {
class Array;
class Int64Type;
class Int8Type;
class Schema;
class StructArray;
template <typename TypeClass>
class NumericArray;
}  // namespace arrow

namespace paimon {
class MemoryPool;
class Metrics;
struct ColumnarBatchContext;

// Convert the arrow array of data file into a KeyValue object iterator (parsing SEQUENCE_NUMBER and
// VALUE_KIND columns)
class KeyValueDataFileRecordReader : public KeyValueRecordReader {
 public:
    KeyValueDataFileRecordReader(std::unique_ptr<BatchReader>&& reader, int32_t key_arity,
                                 const std::shared_ptr<arrow::Schema>& value_schema, int32_t level,
                                 const std::shared_ptr<MemoryPool>& pool);

    class Iterator : public KeyValueRecordReader::Iterator {
     public:
        explicit Iterator(KeyValueDataFileRecordReader* reader) : reader_(reader) {}
        bool HasNext() const override;
        Result<KeyValue> Next() override;

     private:
        mutable int64_t cursor_ = 0;
        KeyValueDataFileRecordReader* reader_ = nullptr;
    };

    Result<std::unique_ptr<KeyValueRecordReader::Iterator>> NextBatch() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

    void Close() override {
        Reset();
        reader_->Close();
    }

    // virtual for test
    virtual void Reset();

 private:
    // For struct array, arrow is unsafe for fields() and field(); for dict array, arrow is unsafe
    // for dictionary(). Therefore, access array in advance before merge sort and projection to
    // avoid subsequent multi-threading problems.
    static void TraverseArray(const std::shared_ptr<arrow::Array>& array);

 private:
    int32_t key_arity_;
    int32_t level_;
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<BatchReader> reader_;
    std::shared_ptr<arrow::Schema> value_schema_;
    std::vector<std::string> value_names_;
    RoaringBitmap32 selection_bitmap_;
    std::shared_ptr<arrow::StructArray> value_struct_array_;
    arrow::ArrayVector key_fields_;
    arrow::ArrayVector value_fields_;
    std::shared_ptr<arrow::NumericArray<arrow::Int64Type>> sequence_number_array_;
    std::shared_ptr<arrow::NumericArray<arrow::Int8Type>> row_kind_array_;
    std::shared_ptr<ColumnarBatchContext> key_ctx_;
    std::shared_ptr<ColumnarBatchContext> value_ctx_;
};
}  // namespace paimon
