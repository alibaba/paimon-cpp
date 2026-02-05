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
#include <utility>
#include <vector>

#include "paimon/predicate/predicate.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/utils/roaring_bitmap32.h"
namespace paimon {
/// The batch reader for a single file supports returning the line number of the last batch read for
/// deletion vector judgment.
class PAIMON_EXPORT FileBatchReader : public BatchReader {
 public:
    /// @return The schema of the file.
    virtual Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const = 0;

    /// Resets the read schema and predicate.
    ///
    /// If `SetReadSchema()` is not called, `NextBatch()` will return data with the file schema.
    /// After resetting the read schema, `NextBatch()` will read data starting from the first row.
    ///
    /// @param read_schema The schema to set for reading.
    /// @param predicate The predicate to apply for filtering data.
    /// @param selection_bitmap The bitmap to apply for filtering data.
    /// @return The status of the operation.
    virtual Status SetReadSchema(::ArrowSchema* read_schema,
                                 const std::shared_ptr<Predicate>& predicate,
                                 const std::optional<RoaringBitmap32>& selection_bitmap) = 0;
    using BatchReader::NextBatch;
    using BatchReader::NextBatchWithBitmap;

    /// Get the row number of the first row in the previously read batch.
    virtual uint64_t GetPreviousBatchFirstRowNumber() const = 0;

    /// Get the number of rows in the file.
    virtual Result<uint64_t> GetNumberOfRows() const = 0;

    /// Get whether or not support read precisely while bitmap pushed down.
    virtual bool SupportPreciseBitmapSelection() const = 0;
};

}  // namespace paimon
