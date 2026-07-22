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

#include "paimon/metrics.h"
#include "paimon/result.h"
#include "paimon/utils/roaring_bitmap32.h"
#include "paimon/visibility.h"

struct ArrowArray;   // IWYU pragma: keep
struct ArrowSchema;  // IWYU pragma: keep

namespace paimon {
/// A batch reader that supports reading batch data into an arrow array.
class PAIMON_EXPORT BatchReader {
 public:
    virtual ~BatchReader() = default;
    using ReadBatch = std::pair<std::unique_ptr<ArrowArray>, std::unique_ptr<ArrowSchema>>;
    using ReadBatchWithBitmap = std::pair<ReadBatch, RoaringBitmap32>;

    /// Retrieves the next batch of data.
    ///
    /// If EOF is reached, returns an OK status with a nullptr array. Returns an error status only
    /// for critical failures (e.g., IO errors). Once an error is returned, this method must not be
    /// retried, as it will repeatedly return the same error code.
    /// \note IMPORTANT: A non-EOF ArrowArray and all its nested child and dictionary arrays must
    /// have offset 0 to avoid potential issues during conversion through the Arrow C Data
    /// Interface.
    ///
    /// @return A result containing a `::ReadBatch`, which consists of a unique pointer to
    /// `ArrowArray` and a unique pointer to `ArrowSchema`. Returned array contains a `_VALUE_KIND`
    /// field (the first field) to indicate the row kind of each row. Deleted or index-filtered rows
    /// are removed.
    virtual Result<ReadBatch> NextBatch() = 0;

    /// Retrieves the next batch of data.
    ///
    /// If EOF is reached, returns an OK status with a nullptr array. Returns an error status only
    /// for critical failures (e.g., IO errors). Once an error is returned, this method must not be
    /// retried, as it will repeatedly return the same error code.
    /// \note IMPORTANT: A non-EOF ArrowArray and all its nested child and dictionary arrays must
    /// have offset 0 to avoid potential issues during conversion through the Arrow C Data
    /// Interface.
    ///
    /// @return A result containing a `::ReadBatch` and a valid bitmap. `::ReadBatch` consists of a
    /// unique pointer to `ArrowArray` and a unique pointer to `ArrowSchema`. Returned array
    /// contains a _VALUE_KIND field (the first field) to indicate the row kind of each row. Deleted
    /// or index-filtered records maybe maintained in `::ReadBatch`, while bitmap indicates valid
    /// row id. If deletion vector or index are enabled, this function is more efficient than
    /// `NextBatch()`. The default implementation calls `NextBatch()` and adds all rows to valid
    /// bitmap. Noted that the returned bitmap has at least one valid row id.
    virtual Result<ReadBatchWithBitmap> NextBatchWithBitmap();

    /// Retrieves the reader's metrics.
    /// Note that calling this method frequently may incur significant performance overhead.
    /// @return A shared pointer to the `Metrics` object.
    virtual std::shared_ptr<Metrics> GetReaderMetrics() const = 0;

    /// Closes the `BatchReader`, releasing any associated resources.
    /// After calling this method, further calls to `NextBatch()` is undefined and should be
    /// avoided.
    virtual void Close() = 0;

    /// Determine whether a `::ReadBatch` or `::ReadBatchWithBitmap` is eof batch, if return true,
    /// all the data has been returned.
    static bool IsEofBatch(const ReadBatch& batch);
    static bool IsEofBatch(const ReadBatchWithBitmap& batch_with_bitmap);

    /// Make an eof batch or batch with bitmap.
    static ReadBatch MakeEofBatch();
    static ReadBatchWithBitmap MakeEofBatchWithBitmap();
};
}  // namespace paimon
