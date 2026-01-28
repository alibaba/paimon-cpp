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
#include "paimon/common/compression/block_decompressor.h"

namespace paimon {

/// Decode data written with {@link Lz4BlockCompressor}.
class Lz4BlockDecompressor : public BlockDecompressor {
 public:
    Result<int32_t> Decompress(const char* src, int32_t src_length, char* dst,
                               int32_t dst_length) override {
        auto compressed_len = ReadIntLE(src);
        auto original_len = ReadIntLE(src + 4);
        PAIMON_RETURN_NOT_OK(ValidateLength(compressed_len, original_len));

        if (dst_length < original_len) {
            return Status::IOError(
                fmt::format("Buffer length too small, compressed_len= {}, original_len={}",
                            compressed_len, original_len));
        }

        if (src_length - HEADER_LENGTH < compressed_len) {
            return Status::IOError("Source data is not integral for decompression.");
        }

        int32_t decompressed_size =
            LZ4_decompress_safe(src + HEADER_LENGTH, dst, src_length - HEADER_LENGTH, dst_length);
        if (decompressed_size != original_len) {
            return Status::IOError(fmt::format("Input is corrupted, expected {}, but got {}",
                                               original_len, decompressed_size));
        }
        return decompressed_size;
    }
};
}  // namespace paimon
