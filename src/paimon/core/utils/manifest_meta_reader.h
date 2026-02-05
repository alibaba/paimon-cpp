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

#include "arrow/array/array_base.h"
#include "arrow/c/bridge.h"
#include "arrow/memory_pool.h"
#include "arrow/type.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {
class MemoryPool;
class Metrics;

class PAIMON_EXPORT ManifestMetaReader : public BatchReader {
 public:
    ManifestMetaReader(std::unique_ptr<BatchReader>&& reader,
                       const std::shared_ptr<arrow::DataType>& target_type,
                       const std::shared_ptr<MemoryPool>& pool);

    ~ManifestMetaReader() override {
        DoClose();
    }

    Result<ReadBatch> NextBatch() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }
    void Close() override {
        DoClose();
    }
    void DoClose() {
        reader_->Close();
    }

 private:
    // fill non exist field and correct int precision
    static Result<std::shared_ptr<arrow::Array>> AlignArrayWithSchema(
        const std::shared_ptr<arrow::Array>& src_array,
        const std::shared_ptr<arrow::DataType>& target_type, arrow::MemoryPool* pool);

    std::unique_ptr<BatchReader> reader_;
    std::shared_ptr<arrow::DataType> target_type_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon
