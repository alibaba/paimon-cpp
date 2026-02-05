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

#include <cstddef>
#include <memory>
#include <vector>

#include "arrow/api.h"
#include "paimon/metrics.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace paimon {
class MemoryPool;

/// This reader is to concatenate a list of BatchReaders and read them sequentially. The input list
/// is already sorted by key and sequence number, and the key intervals do not overlap each other.
class ConcatBatchReader : public BatchReader {
 public:
    ConcatBatchReader(std::vector<std::unique_ptr<BatchReader>>&& readers,
                      const std::shared_ptr<MemoryPool>& pool);

    Result<ReadBatch> NextBatch() override;
    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;
    void Close() override;
    std::shared_ptr<Metrics> GetReaderMetrics() const override;

 private:
    std::shared_ptr<MemoryPool> memory_pool_;
    std::vector<std::unique_ptr<BatchReader>> readers_;
    size_t current_;
};
}  // namespace paimon
