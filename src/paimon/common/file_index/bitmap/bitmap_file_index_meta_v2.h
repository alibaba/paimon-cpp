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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/common/file_index/bitmap/bitmap_file_index_meta.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class InputStream;
class MemoryPool;
enum class FieldType;
/// When the bitmap-indexed column cardinality is high, using the first version of the bitmap index
/// format will take a lot of time to read the entire dictionary. But in fact we don't need a full
/// dictionary when dealing with a small number of predicates, the performance of predicate hits on
/// the bitmap can be improved by creating a secondary index on the dictionary.
///
/// <pre>
/// Bitmap file index format (V2)
/// +-------------------------------------------------+-----------------
/// ｜ version (1 byte) = 2                           ｜
/// +-------------------------------------------------+
/// ｜ row count (4 bytes int)                        ｜
/// +-------------------------------------------------+
/// ｜ non-null value bitmap number (4 bytes int)     ｜
/// +-------------------------------------------------+
/// ｜ has null value (1 byte)                        ｜
/// +-------------------------------------------------+
/// ｜ null value offset (4 bytes if has null value)  ｜       HEAD
/// +-------------------------------------------------+
/// ｜ null bitmap length (4 bytes if has null value) ｜
/// +-------------------------------------------------+
/// ｜ bitmap index block number (4 bytes int)        ｜
/// +-------------------------------------------------+
/// ｜ value 1 | offset 1                             ｜
/// +-------------------------------------------------+
/// ｜ value 2 | offset 2                             ｜
/// +-------------------------------------------------+
/// ｜ ...                                            ｜
/// +-------------------------------------------------+
/// ｜ bitmap blocks offset (4 bytes int)             ｜
/// +-------------------------------------------------+-----------------
/// ｜ bitmap index block 1                           ｜
/// +-------------------------------------------------+
/// ｜ bitmap index block 2                           ｜  INDEX BLOCKS
/// +-------------------------------------------------+
/// ｜ ...                                            ｜
/// +-------------------------------------------------+-----------------
/// ｜ serialized bitmap 1                            ｜
/// +-------------------------------------------------+
/// ｜ serialized bitmap 2                            ｜
/// +-------------------------------------------------+  BITMAP BLOCKS
/// ｜ serialized bitmap 3                            ｜
/// +-------------------------------------------------+
/// ｜ ...                                            ｜
/// +-------------------------------------------------+-----------------
///
/// index block format:
/// +-------------------------------------------------+
/// ｜ entry number (4 bytes int)                     ｜
/// +-------------------------------------------------+
/// ｜ value 1 | offset 1 | length 1                  ｜
/// +-------------------------------------------------+
/// ｜ value 2 | offset 2 | length 2                  ｜
/// +-------------------------------------------------+
/// ｜ ...                                            ｜
/// +-------------------------------------------------+
/// </pre>
class BitmapFileIndexMetaV2 : public BitmapFileIndexMeta {
 public:
    // used for read
    BitmapFileIndexMetaV2(const FieldType& type, int32_t total_length,
                          const std::shared_ptr<MemoryPool>& pool);
    // used for write
    BitmapFileIndexMetaV2(const FieldType& type, int32_t row_count, bool has_null_value,
                          const Entry& null_value_entry, std::vector<Entry>&& write_entries,
                          const std::map<std::string, std::string>& options,
                          const std::shared_ptr<MemoryPool>& pool);

    Result<const BitmapFileIndexMeta::Entry*> FindEntry(const Literal& bitmap_id) override;
    Status Deserialize(const std::shared_ptr<InputStream>& input_stream) override;
    Status Serialize(const std::shared_ptr<MemorySegmentOutputStream>& output_stream) override;

 private:
    class BitmapIndexBlock;
    BitmapIndexBlock* FindBlock(const Literal& bitmap_id);
    static constexpr int64_t DEFAULT_INDEX_BLOCK_SIZE = 16 * 1024;  // 16KB

 private:
    std::map<std::string, std::string> options_;
    int64_t index_block_start_ = -1;
    int64_t block_size_limit_ = DEFAULT_INDEX_BLOCK_SIZE;
    std::vector<std::unique_ptr<BitmapIndexBlock>> index_blocks_;
};

class BitmapFileIndexMetaV2::BitmapIndexBlock {
 public:
    // used for read
    BitmapIndexBlock(BitmapFileIndexMetaV2* outer, const Literal& key, int32_t offset,
                     const std::shared_ptr<InputStream>& input_stream, MemoryPool* pool);
    // used for write
    BitmapIndexBlock(BitmapFileIndexMetaV2* outer, int32_t offset, MemoryPool* pool);

    Result<const Entry*> FindEntry(const Literal& bitmap_id);

    Result<bool> TryAdd(const Entry& entry);

 private:
    Status TryDeserialize();

    static Result<int32_t> GetKeyBytes(const Literal& literal);

 public:
    Literal key;
    int32_t offset = -1;
    int32_t serialized_bytes = sizeof(int32_t);
    std::vector<Entry> entry_list;

 private:
    bool is_deserialized_ = false;
    std::shared_ptr<InputStream> input_stream_;
    BitmapFileIndexMetaV2* outer_ = nullptr;
    MemoryPool* pool_;
};

}  // namespace paimon
