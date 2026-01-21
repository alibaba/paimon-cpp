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

#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/core/key_value.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace paimon {

/// Trailer of a block.
class BlockTrailer {
 public:
    static std::unique_ptr<BlockTrailer> ReadBlockTrailer(std::shared_ptr<MemorySliceInput>& input);

 public:
    BlockTrailer(int8_t compression_type, int32_t crc32c)
        : crc32c_(crc32c), compression_type_(compression_type) {}

    ~BlockTrailer() = default;

    int32_t Crc32c() const;
    int8_t CompressionType() const;

    std::string ToString() const;
    std::shared_ptr<MemorySlice> WriteBlockTrailer(MemoryPool* pool);

 public:
    static constexpr int32_t ENCODED_LENGTH = 5;

 private:
    int32_t crc32c_;
    int8_t compression_type_;
};
}  // namespace paimon
