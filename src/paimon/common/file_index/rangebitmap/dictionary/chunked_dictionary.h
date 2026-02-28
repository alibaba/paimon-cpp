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

#include <functional>
#include <memory>
#include <vector>

#include "paimon/common/file_index/rangebitmap/dictionary/chunk.h"
#include "paimon/common/file_index/rangebitmap/dictionary/dictionary.h"
#include "paimon/common/file_index/rangebitmap/dictionary/key_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"

namespace paimon {

class InputStream;
class MemoryPool;

class ChunkedDictionary final : public Dictionary {
 public:
    static constexpr int8_t CURRENT_VERSION = 1;

    Result<int32_t> Find(const Literal& key) override;

    Result<Literal> Find(int32_t code) override;

    Result<std::shared_ptr<Chunk>> GetChunk(int32_t index);

    class Appender final : public Dictionary::Appender {
     public:
        Appender(const std::shared_ptr<MemoryPool>& pool,
                 const std::shared_ptr<KeyFactory>& key_factory, int32_t chunk_size_bytes);
        Status AppendSorted(const Literal& key, int32_t code) override;
        Result<PAIMON_UNIQUE_PTR<Bytes>> Serialize() override;

     private:
        Status Flush();

        std::shared_ptr<MemoryPool> pool_;
        std::shared_ptr<KeyFactory> key_factory_;
        int32_t chunk_size_bytes_;
        std::optional<Literal> last_key_;
        std::optional<int32_t> last_code_;
        std::unique_ptr<Chunk> chunk_;
        int32_t size_;
        int32_t key_offset_;
        int32_t chunks_offset_;
        std::unique_ptr<MemorySegmentOutputStream> chunks_output_;
        std::unique_ptr<MemorySegmentOutputStream> keys_output_;
        std::unique_ptr<MemorySegmentOutputStream> offsets_output_;
    };

    static Result<std::unique_ptr<ChunkedDictionary>> Create(
        const std::shared_ptr<MemoryPool>& pool, FieldType field_type,
        const std::shared_ptr<InputStream>& input_stream, int64_t offset);

    ChunkedDictionary(const std::shared_ptr<MemoryPool>& pool,
                      const std::shared_ptr<InputStream>& input_stream,
                      const std::shared_ptr<KeyFactory>& factory, int32_t size,
                      int32_t offsets_length, int32_t chunks_length, int64_t body_offset);

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<KeyFactory> factory_;

    std::shared_ptr<InputStream> input_stream_;
    int32_t size_;            // number of chunks
    int32_t offsets_length_;  // bytes length of offsets
    int32_t chunks_length_;   // bytes length of chunks
    int64_t body_offset_;     // where offsets start

    // for lazy loading
    PAIMON_UNIQUE_PTR<Bytes> offsets_bytes_;
    PAIMON_UNIQUE_PTR<Bytes> chunks_bytes_;

    // mmap chunks cache
    std::vector<std::shared_ptr<Chunk>> chunks_cache_;
};

}  // namespace paimon
