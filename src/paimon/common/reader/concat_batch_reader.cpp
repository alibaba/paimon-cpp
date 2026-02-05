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

#include "paimon/common/reader/concat_batch_reader.h"

#include <utility>

#include "arrow/c/abi.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"

namespace paimon {
class MemoryPool;

ConcatBatchReader::ConcatBatchReader(std::vector<std::unique_ptr<BatchReader>>&& readers,
                                     const std::shared_ptr<MemoryPool>& pool)
    : memory_pool_(pool), readers_(std::move(readers)), current_(0) {}

Result<BatchReader::ReadBatch> ConcatBatchReader::NextBatch() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                           NextBatchWithBitmap());
    return ReaderUtils::ApplyBitmapToReadBatch(std::move(batch_with_bitmap),
                                               memory_pool_->AsArrowMemoryPool());
}

void ConcatBatchReader::Close() {
    for (; current_ < readers_.size(); current_++) {
        readers_[current_]->Close();
    }
}

std::shared_ptr<Metrics> ConcatBatchReader::GetReaderMetrics() const {
    return MetricsImpl::CollectReadMetrics(readers_);
}

Result<BatchReader::ReadBatchWithBitmap> ConcatBatchReader::NextBatchWithBitmap() {
    while (current_ < readers_.size()) {
        auto& current_reader = readers_[current_];
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap result,
                               current_reader->NextBatchWithBitmap());
        if (!BatchReader::IsEofBatch(result)) {
            // current reader not eof, just return
            return result;
        }
        // current meets eof, move to next reader
        current_reader->Close();
        current_++;
    }
    // read finish
    return BatchReader::MakeEofBatchWithBitmap();
}

}  // namespace paimon
