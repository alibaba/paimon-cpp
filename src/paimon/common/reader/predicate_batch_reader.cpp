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

#include "paimon/common/reader/predicate_batch_reader.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/memory_pool.h"
#include "fmt/format.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/predicate/predicate.h"
#include "paimon/status.h"

namespace paimon {
class MemoryPool;

PredicateBatchReader::PredicateBatchReader(std::unique_ptr<BatchReader>&& reader,
                                           const std::shared_ptr<PredicateFilter>& predicate_filter,
                                           const std::shared_ptr<MemoryPool>& pool)
    : arrow_pool_(GetArrowPool(pool)),
      reader_(std::move(reader)),
      predicate_filter_(predicate_filter) {}

Result<std::unique_ptr<PredicateBatchReader>> PredicateBatchReader::Create(
    std::unique_ptr<BatchReader>&& reader, const std::shared_ptr<Predicate>& predicate,
    const std::shared_ptr<MemoryPool>& pool) {
    auto predicate_filter = std::dynamic_pointer_cast<PredicateFilter>(predicate);
    if (!predicate_filter) {
        return Status::Invalid(
            fmt::format("predicate {} does not support Test", predicate->ToString()));
    }
    return std::unique_ptr<PredicateBatchReader>(
        new PredicateBatchReader(std::move(reader), predicate_filter, pool));
}

Result<BatchReader::ReadBatch> PredicateBatchReader::NextBatch() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                           NextBatchWithBitmap());
    return ReaderUtils::ApplyBitmapToReadBatch(std::move(batch_with_bitmap), arrow_pool_.get());
}

Result<BatchReader::ReadBatchWithBitmap> PredicateBatchReader::NextBatchWithBitmap() {
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                               reader_->NextBatchWithBitmap());
        if (BatchReader::IsEofBatch(batch_with_bitmap)) {
            return batch_with_bitmap;
        }
        auto& [batch, bitmap] = batch_with_bitmap;
        auto& [c_array, c_schema] = batch;
        assert(c_array);
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 valid_bitmap, Filter(array));
        bitmap &= valid_bitmap;
        if (bitmap.IsEmpty()) {
            continue;
        }
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, c_array.get(), c_schema.get()));
        return batch_with_bitmap;
    }
}

Result<RoaringBitmap32> PredicateBatchReader::Filter(
    const std::shared_ptr<arrow::Array>& array) const {
    PAIMON_ASSIGN_OR_RAISE(std::vector<char> result, predicate_filter_->Test(*array));
    assert(result.size() == static_cast<size_t>(array->length()));
    RoaringBitmap32 is_valid;
    for (int32_t i = 0; i < static_cast<int32_t>(result.size()); i++) {
        if (result[i]) {
            is_valid.Add(i);
        }
    }
    return is_valid;
}

}  // namespace paimon
