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
#include <vector>

#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/global_index/global_index_reader.h"
#include "paimon/global_index/global_index_writer.h"
#include "paimon/global_index/io/global_index_file_reader.h"
#include "paimon/global_index/io/global_index_file_writer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {
/// Interface for creating global index readers and writers.
class PAIMON_EXPORT GlobalIndexer {
 public:
    virtual ~GlobalIndexer() = default;

    /// Creates a writer for building a global index on a specific field.
    ///
    /// @param field_name     Name of the field to be indexed.
    /// @param arrow_schema   Schema of the input Arrow struct array.
    ///                       It must contain the field specified by field_name and may
    ///                       include additional associated fields used during index construction.
    /// @param file_writer    I/O handler for persisting index data to storage.
    /// @param pool           Memory pool for temporary allocations; if nullptr, uses default.
    /// @return A `Result` containing a shared pointer to the created `GlobalIndexWriter`,
    ///         or an error if the field is not found, unsupported, or initialization fails, etc.
    virtual Result<std::shared_ptr<GlobalIndexWriter>> CreateWriter(
        const std::string& field_name, ::ArrowSchema* arrow_schema,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
        const std::shared_ptr<MemoryPool>& pool) const = 0;

    /// Creates a reader for querying a pre-built global index.
    ///
    /// @param arrow_schema   Schema of the indexed data; used to interpret predicate literals.
    /// @param file_reader    I/O handler for reading index artifacts from storage.
    /// @param files          List of index file metadata entries produced during writing.
    /// @param pool           Memory pool for temporary allocations; if nullptr, uses default.
    /// @return A `Result` containing a shared pointer to the created `GlobalIndexReader`,
    ///         or an error if the index cannot be loaded or is incompatible, etc.
    virtual Result<std::shared_ptr<GlobalIndexReader>> CreateReader(
        ::ArrowSchema* arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
        const std::vector<GlobalIndexIOMeta>& files,
        const std::shared_ptr<MemoryPool>& pool) const = 0;
};

}  // namespace paimon
