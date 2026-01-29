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
#include "paimon/common/compression/block_decompressor.h"

namespace paimon {

/// Decode data written with {@link ZstdBlockDecompressor}.
class ZstdBlockDecompressor : public BlockDecompressor {
 public:
    Result<int32_t> Decompress(const char* src, int32_t src_length, char* dst,
                               int32_t dst_length) override {
        int32_t decompressed_size = ZSTD_decompress(dst, dst_length, src, src_length);
        if (ZSTD_isError(decompressed_size)) {
            return Status::IOError(
                fmt::format("Input is corrupted with return code {}", decompressed_size));
        }
        return decompressed_size;
    }
};
}  // namespace paimon
