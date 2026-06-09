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

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

#include "arrow/memory_pool.h"
#include "paimon/common/data/blob_view_struct.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace paimon {
class BlobViewResolvingBatchReader : public BatchReader {
 public:
    BlobViewResolvingBatchReader(std::unique_ptr<BatchReader>&& reader,
                                 std::vector<std::string> read_blob_view_fields,
                                 BlobViewResolver resolver,
                                 const std::shared_ptr<MemoryPool>& pool);

    Result<ReadBatch> NextBatch() override;

    void Close() override {
        reader_->Close();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

 private:
    Result<std::shared_ptr<arrow::Array>> ResolveBinaryColumn(
        const std::shared_ptr<arrow::LargeBinaryArray>& blob_view_struct_array);

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<arrow::MemoryPool> arrow_pool_;
    std::unique_ptr<BatchReader> reader_;
    std::set<std::string> read_blob_view_fields_;
    BlobViewResolver resolver_;
};

}  // namespace paimon
