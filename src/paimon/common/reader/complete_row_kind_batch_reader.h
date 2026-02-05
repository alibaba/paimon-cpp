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
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace arrow {
class StructArray;
}  // namespace arrow

namespace paimon {
class MemoryPool;
class Metrics;

class CompleteRowKindBatchReader : public BatchReader {
 public:
    CompleteRowKindBatchReader(std::unique_ptr<BatchReader>&& reader,
                               const std::shared_ptr<MemoryPool>& pool)
        : memory_pool_(pool), reader_(std::move(reader)) {}

    Result<ReadBatch> NextBatch() override;

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;

    void Close() override {
        reader_->Close();
        row_kind_array_.reset();
        field_names_with_row_kind_.clear();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

 private:
    Result<std::shared_ptr<arrow::Array>> PrepareRowKindArray(int32_t struct_array_length);

    void UpdateFieldNamesWithRowKind(const std::shared_ptr<arrow::StructArray>& struct_array);

 private:
    std::shared_ptr<MemoryPool> memory_pool_;
    std::unique_ptr<BatchReader> reader_;
    std::shared_ptr<arrow::Array> row_kind_array_;
    std::vector<std::string> field_names_with_row_kind_;
};
}  // namespace paimon
