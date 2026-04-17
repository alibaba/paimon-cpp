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

#include <memory>
#include <string>

#include "arrow/api.h"
#include "paimon/common/global_index/btree/btree_file_footer.h"
#include "paimon/common/global_index/btree/btree_index_meta.h"
#include "paimon/common/sst/sst_file_writer.h"
#include "paimon/global_index/global_index_writer.h"
#include "paimon/global_index/io/global_index_file_writer.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

/// Writer for BTree Global Index files.
/// This writer builds an SST file where each key maps to a list of row IDs.
class BTreeGlobalIndexWriter : public GlobalIndexWriter {
 public:
    /// Factory method that may fail during initialization (e.g.,
    /// Arrow schema import). Use this instead of the constructor.
    static Result<std::shared_ptr<BTreeGlobalIndexWriter>> Create(
        const std::string& field_name, ::ArrowSchema* arrow_schema,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
        const std::shared_ptr<paimon::BlockCompressionFactory>& compression_factory,
        const std::shared_ptr<MemoryPool>& pool, int32_t block_size);

    ~BTreeGlobalIndexWriter() override = default;

    /// Add a batch of data from an Arrow array.
    /// The Arrow array should contain a single column of the indexed field.
    Status AddBatch(::ArrowArray* arrow_array) override;

    /// Finish writing and return the index metadata.
    Result<std::vector<GlobalIndexIOMeta>> Finish() override;

 private:
    BTreeGlobalIndexWriter(
        const std::string& field_name, std::shared_ptr<arrow::DataType> arrow_type,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
        const std::shared_ptr<paimon::BlockCompressionFactory>& compression_factory,
        const std::shared_ptr<MemoryPool>& pool, int32_t block_size);

    // Helper method to write a key-value pair to the SST file
    Status WriteKeyValue(std::shared_ptr<Bytes> key, const std::vector<int64_t>& row_ids);

    // Helper method to serialize row IDs into a Bytes object
    Result<std::shared_ptr<Bytes>> SerializeRowIds(const std::vector<int64_t>& row_ids);

    // Helper method to write null bitmap to the output stream
    Result<std::shared_ptr<BlockHandle>> WriteNullBitmap(const std::shared_ptr<OutputStream>& out);

    // Helper method to compare binary keys for std::map ordering
    int32_t CompareBinaryKeys(const std::shared_ptr<Bytes>& a,
                              const std::shared_ptr<Bytes>& b) const;

 private:
    std::string field_name_;
    std::shared_ptr<arrow::DataType> arrow_type_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<GlobalIndexFileWriter> file_writer_;

    // SST file writer (declared after pool_ to ensure correct destruction order)
    std::unique_ptr<SstFileWriter> sst_writer_;
    std::shared_ptr<OutputStream> output_stream_;
    std::string file_name_;

    // Track first and last keys for index meta
    std::shared_ptr<Bytes> first_key_;
    std::shared_ptr<Bytes> last_key_;

    // Null bitmap tracking
    std::shared_ptr<RoaringBitmap64> null_bitmap_;
    bool has_nulls_;

    // Current row ID counter
    int64_t current_row_id_;
};

}  // namespace paimon
