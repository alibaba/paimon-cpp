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

#include <vector>

#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/visibility.h"
struct ArrowArray;

namespace paimon {
/// Abstract interface for building a global index from Arrow data batches.
class PAIMON_EXPORT GlobalIndexWriter {
 public:
    virtual ~GlobalIndexWriter() = default;

    /// Builds index structures from a batch of columnar data.
    ///
    /// @param arrow_array A valid C ArrowArray pointer representing a struct array.
    ///                    Must not be nullptr, and must conform to the expected schema.
    /// @return `Status::OK()` on success; otherwise, an error indicating malformed
    ///         input, I/O failure, or unsupported type, etc.
    virtual Status AddBatch(::ArrowArray* arrow_array) = 0;

    /// Finalizes the index build process and returns metadata for persisted index.
    virtual Result<std::vector<GlobalIndexIOMeta>> Finish() = 0;
};

}  // namespace paimon
