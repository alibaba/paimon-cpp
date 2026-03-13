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
#include <string>

#include "arrow/api.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace arrow {
class MemoryPool;
class Array;
class StructArray;
}  // namespace arrow

namespace paimon {
class RoaringBitmap32;

class ReaderUtils {
 public:
    ReaderUtils() = delete;
    ~ReaderUtils() = delete;

    /// @param batch_with_bitmap where the bitmap records the valid row ids in the array
    /// @param arrow_pool a pool for arrow
    /// @return returned array contains all the valid rows in the input array
    /// This function may trigger data copy.
    static Result<BatchReader::ReadBatch> ApplyBitmapToReadBatch(
        BatchReader::ReadBatchWithBitmap&& batch_with_bitmap, arrow::MemoryPool* arrow_pool);
    /// @param batch a read batch
    /// @return return the input batch and a all valid bitmap
    static BatchReader::ReadBatchWithBitmap AddAllValidBitmap(BatchReader::ReadBatch&& batch);

    /// Release the c array and c schema in batch.
    static void ReleaseReadBatch(BatchReader::ReadBatch&& batch);

    /// Split the array into multiple valid sub-arrays according to the bitmap.
    /// Precondition: input bitmap is not empty
    static Result<arrow::ArrayVector> GenerateFilteredArrayVector(
        const std::shared_ptr<arrow::Array>& src_array, const RoaringBitmap32& bitmap);
};
}  // namespace paimon
