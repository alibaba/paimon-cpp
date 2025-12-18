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

#include "paimon/type_fwd.h"

namespace paimon {

/// File writer to accept one record or a branch of records and generate metadata after closing it.
/// <T> record type.
/// <R> file result to collect.
template <typename T, typename R>
class FileWriter {
 public:
    FileWriter() = default;
    virtual ~FileWriter() = default;

    /// Add one record to this file writer.
    ///
    /// @note If any error occurs during writing, the writer should clean up useless files for
    /// the user.
    ///
    /// @param record to write.
    /// @return Status if encounter any IO error.
    virtual Status Write(T record) = 0;

    /// The total written record count.
    ///
    /// @return record count.
    virtual int64_t RecordCount() const = 0;

    /// Abort to clear orphan file(s) if encounter any error.
    ///
    /// @note This implementation must be reentrant.
    virtual void Abort() = 0;

    virtual Status Close() = 0;

    /// @return the result for this closed file writer.
    virtual Result<R> GetResult() = 0;

    virtual std::shared_ptr<Metrics> GetMetrics() const = 0;
};

}  // namespace paimon
