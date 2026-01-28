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

#include <zstd.h>

#include "fmt/format.h"
#include "paimon/common/compression/block_compressor.h"

namespace paimon {

/// A {@link BlockCompressor} for zstd.
class ZstdBlockCompressor : public BlockCompressor {
 public:
    explicit ZstdBlockCompressor(int32_t level) : level_(level) {}

    int32_t GetMaxCompressedSize(int32_t src_size) override {
        return ZSTD_compressBound(src_size);
    }

    Result<int32_t> Compress(const char* src, int32_t src_length, char* dst,
                             int32_t dst_length) override {
        size_t const compressed_size = ZSTD_compress(dst, dst_length, src, src_length, level_);
        if (ZSTD_isError(compressed_size)) {
            return Status::IOError(fmt::format("Compression failed with code {}", compressed_size));
        }
        return compressed_size;
    }

 private:
    int32_t level_;
};
}  // namespace paimon
