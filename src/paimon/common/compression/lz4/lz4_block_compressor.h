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

#include <lz4.h>

#include "fmt/format.h"
#include "paimon/common/compression/block_compressor.h"

namespace paimon {

/// Encode data into LZ4 format (not compatible with the LZ4 Frame format).
class Lz4BlockCompressor : public BlockCompressor {
 public:
    int32_t GetMaxCompressedSize(int32_t src_size) override {
        return BlockCompressor::HEADER_LENGTH + LZ4_compressBound(src_size);
    }

    Result<int32_t> Compress(const char* src, int32_t src_length, char* dst,
                             int32_t dst_length) override {
        int32_t compressed_size =
            LZ4_compress_default(src, dst + BlockCompressor::HEADER_LENGTH, src_length,
                                 dst_length - BlockCompressor::HEADER_LENGTH);
        if (compressed_size < 0) {
            return Status::IOError(fmt::format("Compression failed with code {}", compressed_size));
        }
        WriteIntLE(compressed_size, dst);
        WriteIntLE(src_length, dst + 4);
        return BlockCompressor::HEADER_LENGTH + compressed_size;
    }
};
}  // namespace paimon
