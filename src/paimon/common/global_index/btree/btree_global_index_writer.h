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
#include "paimon/predicate/literal.h"
#include "paimon/utils/roaring_bitmap64.h"
namespace paimon {

/// Writer for BTree Global Index files.
/// This writer builds an SST file where each key maps to a list of row IDs.
/// Note that users must keep written keys monotonically incremental. All null keys are stored in a
/// separate bitmap, which will be serialized and appended to the file end on close. The layout is
/// as below:
///
///    +-----------------------------------+------+
///    |             Footer                |      |
///    +-----------------------------------+      |
///    |           Index Block             |      +--> Loaded on open
///    +-----------------------------------+      |
///    |        Bloom Filter Block         |      |
///    +-----------------------------------+------+
///    |         Null Bitmap Block         |      |
///    +-----------------------------------+      |
///    |            Data Block             |      |
///    +-----------------------------------+      +--> Loaded on requested
///    |              ......               |      |
///    +-----------------------------------+      |
///    |            Data Block             |      |
///    +-----------------------------------+------+
///
/// For efficiency, we combine entries with the same keys and store a compact list of row ids for
/// each key.
class BTreeGlobalIndexWriter : public GlobalIndexWriter {
 public:
    /// Factory method that may fail during initialization (e.g.,
    /// Arrow schema import). Use this instead of the constructor.
    static Result<std::shared_ptr<BTreeGlobalIndexWriter>> Create(
        const std::string& field_name, const std::shared_ptr<arrow::StructType>& arrow_type,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer, int32_t block_size,
        const std::shared_ptr<paimon::BlockCompressionFactory>& compression_factory,
        const std::shared_ptr<MemoryPool>& pool);

    ~BTreeGlobalIndexWriter() override = default;

    Status AddBatch(::ArrowArray* arrow_array, std::vector<int64_t>&& relative_row_ids) override;

    /// Finish writing and return the index metadata.
    Result<std::vector<GlobalIndexIOMeta>> Finish() override;

 private:
    BTreeGlobalIndexWriter(const std::string& field_name,
                           const std::shared_ptr<arrow::DataType>& arrow_type,
                           const std::shared_ptr<arrow::DataType>& key_type,
                           const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
                           const std::string& index_file_name,
                           const std::shared_ptr<OutputStream>& output_stream,
                           std::unique_ptr<SstFileWriter>&& sst_writer,
                           const std::shared_ptr<MemoryPool>& pool);

    Status Flush();

    Result<std::optional<BlockHandle>> WriteNullBitmap(const std::shared_ptr<OutputStream>& out);

 private:
    std::string field_name_;
    std::shared_ptr<arrow::DataType> arrow_type_;
    std::shared_ptr<arrow::DataType> key_type_;
    std::shared_ptr<MemoryPool> pool_;

    std::shared_ptr<GlobalIndexFileWriter> file_writer_;
    std::string index_file_name_;
    std::shared_ptr<OutputStream> output_stream_;
    std::unique_ptr<SstFileWriter> sst_writer_;

    std::optional<Literal> first_key_;
    std::optional<Literal> last_key_;

    // Null bitmap tracking
    RoaringBitmap64 null_bitmap_;
    std::vector<int64_t> current_row_ids_;
};

}  // namespace paimon
