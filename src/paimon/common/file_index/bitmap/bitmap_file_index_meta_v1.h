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
#include <memory>
#include <unordered_map>
#include <vector>

#include "paimon/common/file_index/bitmap/bitmap_file_index_meta.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class InputStream;
class Literal;
class MemoryPool;
enum class FieldType;
///
///
/// <pre>
/// Bitmap file index format (V1)
/// +-------------------------------------------------+-----------------
/// | version (1 byte)                                |
/// +-------------------------------------------------+
/// | row count (4 bytes int)                         |
/// +-------------------------------------------------+
/// | non-null value bitmap number (4 bytes int)      |
/// +-------------------------------------------------+
/// | has null value (1 byte)                         |
/// +-------------------------------------------------+
/// | null value offset (4 bytes if has null value)   |       HEAD
/// +-------------------------------------------------+
/// | value 1 | offset 1                              |
/// +-------------------------------------------------+
/// | value 2 | offset 2                              |
/// +-------------------------------------------------+
/// | value 3 | offset 3                              |
/// +-------------------------------------------------+
/// | ...                                             |
/// +-------------------------------------------------+-----------------
/// | serialized bitmap 1                             |
/// +-------------------------------------------------+
/// | serialized bitmap 2                             |
/// +-------------------------------------------------+       BODY
/// | serialized bitmap 3                             |
/// +-------------------------------------------------+
/// | ...                                             |
/// +-------------------------------------------------+-----------------
///
/// value x:                       var bytes for any data type (as bitmap identifier)
/// offset:                        4 bytes int (when it is negative, it represents that there is
/// only one value and its position is the inverse of the negative value)
/// </pre>

class BitmapFileIndexMetaV1 : public BitmapFileIndexMeta {
 public:
    // used for read
    BitmapFileIndexMetaV1(const FieldType& type, int32_t start, int32_t total_length,
                          const std::shared_ptr<MemoryPool>& pool);
    // used for write
    BitmapFileIndexMetaV1(const FieldType& type, int32_t row_count, bool has_null_value,
                          const Entry& null_value_entry, std::vector<Entry>&& write_entries,
                          const std::shared_ptr<MemoryPool>& pool);

    Result<const BitmapFileIndexMeta::Entry*> FindEntry(const Literal& bitmap_id) override;

    Status Deserialize(const std::shared_ptr<InputStream>& input_stream) override;
    Status Serialize(const std::shared_ptr<MemorySegmentOutputStream>& output_stream) override;

 private:
    int32_t start_ = -1;
    std::unordered_map<Literal, BitmapFileIndexMeta::Entry> entries_;
};

}  // namespace paimon
