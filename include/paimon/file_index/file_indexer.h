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

#include "paimon/file_index/file_index_reader.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/file_index/file_index_writer.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {
/// File index interface. To read and write a file index.
class PAIMON_EXPORT FileIndexer {
 public:
    virtual ~FileIndexer() = default;

    /// Create `FileIndexReader` with input stream.
    ///
    /// @param arrow_schema ArrowSchema derived from arrow schema or struct type with
    /// specified indexed field.
    /// @param start Start position of input stream.
    /// @param length Length of index bytes.
    /// @param input_stream Input stream for read index.
    /// @param pool Memory pool for memory allocation.
    /// @return A `FileIndexReader` to read index.
    virtual Result<std::shared_ptr<FileIndexReader>> CreateReader(
        ::ArrowSchema* arrow_schema, int32_t start, int32_t length,
        const std::shared_ptr<InputStream>& input_stream,
        const std::shared_ptr<MemoryPool>& pool) const = 0;

    /// Create `FileIndexWriter` for arrow schema.
    ///
    /// @param arrow_schema ArrowSchema derived from arrow schema or struct type with
    /// specified indexed field.
    /// @param pool Memory pool for memory allocation.
    /// @return A `FileIndexWriter` to write index.
    virtual Result<std::shared_ptr<FileIndexWriter>> CreateWriter(
        ::ArrowSchema* arrow_schema, const std::shared_ptr<MemoryPool>& pool) const = 0;
};

}  // namespace paimon
