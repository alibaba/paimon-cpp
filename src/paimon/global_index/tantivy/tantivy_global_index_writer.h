/*
 * Copyright 2026-present Alibaba Inc.
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
#include <vector>

#include "arrow/type.h"
#include "paimon/global_index/global_index_writer.h"
#include "paimon/global_index/io/global_index_file_writer.h"
#include "paimon/global_index/tantivy/tantivy_defs.h"
#include "paimon/global_index/tantivy/tantivy_ffi_handle.h"

namespace paimon::tantivy {

/// Tantivy-backed implementation of GlobalIndexWriter.
///
/// Mirrors LuceneGlobalIndexWriter's lifecycle:
///   Create() → AddBatch()* → Finish()
/// Each shard produces exactly one .index file via the GlobalIndexFileWriter,
/// containing the full packed tantivy on-disk index in a single contiguous blob.
///
/// Indexes written by this class are NOT cross-readable with lucene-fts. The
/// C++ side of this writer is intentionally thin: index construction, segment
/// merging, and packing all happen in Rust behind the FFI boundary.
class TantivyGlobalIndexWriter : public GlobalIndexWriter {
 public:
    static Result<std::shared_ptr<TantivyGlobalIndexWriter>> Create(
        const std::string& field_name, const std::shared_ptr<arrow::DataType>& arrow_type,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
        const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool);

    ~TantivyGlobalIndexWriter() override = default;

    Status AddBatch(::ArrowArray* arrow_array, std::vector<int64_t>&& relative_row_ids) override;

    Result<std::vector<GlobalIndexIOMeta>> Finish() override;

 private:
    TantivyGlobalIndexWriter(const std::string& field_name,
                             const std::shared_ptr<arrow::DataType>& arrow_type, WriterPtr writer,
                             const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
                             const std::map<std::string, std::string>& options,
                             const std::shared_ptr<MemoryPool>& pool);

    std::shared_ptr<MemoryPool> pool_;
    std::string field_name_;
    std::shared_ptr<arrow::DataType> arrow_type_;
    /// Owning handle to the Rust-side writer.
    WriterPtr writer_;
    std::shared_ptr<GlobalIndexFileWriter> file_writer_;
    std::map<std::string, std::string> options_;
    /// Last document index processed (matches caller-passed relative_row_ids).
    int64_t row_id_ = 0;
};

}  // namespace paimon::tantivy
