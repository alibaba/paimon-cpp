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

#include <map>
#include <memory>
#include <string>

#include "paimon/status.h"
#include "paimon/type_fwd.h"

struct ArrowArray;

namespace paimon {
/// File format writer, each writer corresponds to a data file.
class PAIMON_EXPORT FormatWriter {
 public:
    virtual ~FormatWriter() = default;

    /// Add a batch of records to the format writer.
    /// @param batch Pointer to an ArrowArray containing the batch data to write.
    /// @return Status indicating success (OK) or failure with error information.
    /// @note The batch must conform to the schema expected by the writer.
    /// @note This method can be called multiple times to write data incrementally.
    /// @note After calling `Finish()`, this method should not be called again.
    virtual Status AddBatch(::ArrowArray* batch) = 0;

    /// Flushes all intermediate buffered data to the format writer.
    ///
    /// @return Error status returned if the encoder cannot be flushed, or if the output stream
    /// return an error.
    virtual Status Flush() = 0;

    /// Finishes the writing. This must flush all internal buffer, finish encoding, and write
    /// footers.
    ///
    /// @note The writer is not expected to handle any more records via `AddBatch()` after
    /// this method is called.
    ///
    /// @warning This method **MUST NOT** close the stream that the writer writes to. Closing
    /// the stream is expected to happen through the invoker of this method afterwards.
    ///
    /// @return Error status returned if the finalization fails.
    virtual Status Finish() = 0;

    /// Check if the writer has reached the `target_size`.
    ///
    /// @param suggested_check Whether it needs to be checked, but subclasses can also decide
    ///                        whether to check it themselves.
    /// @param target_size The size of the target.
    /// @return True if the target size was reached, otherwise false.
    /// @return Error status returned if calculating the length fails.
    virtual Result<bool> ReachTargetSize(bool suggested_check, int64_t target_size) const = 0;

    /// Get metrics of the writer
    /// @return The accumulated writer metrics to current state.
    virtual std::shared_ptr<Metrics> GetWriterMetrics() const = 0;

    /// Adds metadata to the file footer. Values are encoded by each format writer
    /// before being persisted. Must be called before Finish().
    virtual Status AddMetadata(const std::map<std::string, std::string>& metadata) = 0;
};

}  // namespace paimon
