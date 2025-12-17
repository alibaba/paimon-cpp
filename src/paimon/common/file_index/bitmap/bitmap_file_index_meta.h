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
#include <functional>
#include <memory>
#include <vector>

#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class DataInputStream;
class InputStream;
class MemoryPool;
enum class FieldType;

class BitmapFileIndexMeta {
 public:
    struct Entry {
        Entry(const Literal& key, int32_t offset, int32_t length);
        Literal key;
        int32_t offset;
        int32_t length;
    };
    virtual ~BitmapFileIndexMeta() = default;
    // used for read
    BitmapFileIndexMeta(const FieldType& type, int32_t total_length,
                        const std::shared_ptr<MemoryPool>& pool);
    // used for write
    BitmapFileIndexMeta(const FieldType& type, int32_t row_count, bool has_null_value,
                        const Entry& null_value_entry, std::vector<Entry>&& write_entries,
                        const std::shared_ptr<MemoryPool>& pool);

    int32_t GetRowCount() const {
        return row_count_;
    }
    int64_t GetBodyStart() const {
        return body_start_;
    }

    virtual Result<const Entry*> FindEntry(const Literal& bitmap_id) = 0;
    virtual Status Deserialize(const std::shared_ptr<InputStream>& input_stream) = 0;
    virtual Status Serialize(const std::shared_ptr<MemorySegmentOutputStream>& output_stream) = 0;

 protected:
    Result<std::function<Result<Literal>()>> GetValueReader(
        const std::shared_ptr<DataInputStream>& in, bool move_body_start);

    Result<std::function<void(const Literal&)>> GetValueWriter(
        const std::shared_ptr<MemorySegmentOutputStream>& output_stream) const;

    template <typename T>
    Result<T> ReadAndMoveBodyStart(const std::shared_ptr<DataInputStream>& in,
                                   bool move_body_start = true) {
        PAIMON_ASSIGN_OR_RAISE(T value, in->ReadValue<T>());
        if (move_body_start) {
            body_start_ += sizeof(T);
        }
        return value;
    }

 protected:
    FieldType data_type_;
    int32_t row_count_ = -1;
    bool has_null_value_ = false;
    int64_t body_start_ = -1;
    int32_t total_length_ = -1;
    Entry null_value_entry_;
    // @note use only for serialize
    std::vector<Entry> write_entries_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon
