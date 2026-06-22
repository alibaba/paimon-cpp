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

#include "arrow/memory_pool.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace paimon {
class MemoryPool;
class Metrics;
class Predicate;
class PredicateFilter;

class PredicateBatchReader : public BatchReader {
 public:
    static Result<std::unique_ptr<PredicateBatchReader>> Create(
        std::unique_ptr<BatchReader>&& reader, const std::shared_ptr<Predicate>& predicate,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    ~PredicateBatchReader() override = default;

    Result<BatchReader::ReadBatch> NextBatch() override;

    Result<BatchReader::ReadBatchWithBitmap> NextBatchWithBitmap() override;

    void Close() override {
        return reader_->Close();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

 private:
    PredicateBatchReader(std::unique_ptr<BatchReader>&& reader,
                         const std::shared_ptr<PredicateFilter>& predicate_filter,
                         const std::shared_ptr<arrow::MemoryPool>& arrow_pool);
    Result<RoaringBitmap32> Filter(const std::shared_ptr<arrow::Array>& array) const;

 private:
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::unique_ptr<BatchReader> reader_;
    std::shared_ptr<PredicateFilter> predicate_filter_;
};
}  // namespace paimon
