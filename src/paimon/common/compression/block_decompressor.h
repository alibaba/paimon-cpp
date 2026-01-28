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

#include "paimon/result.h"

namespace paimon {

/// A decompressor which decompresses a block each time.
class BlockDecompressor {
 public:
    static int32_t ReadIntLE(const char* buf);

    static Status ValidateLength(int32_t compressed_len, int32_t original_len);

 public:
    virtual ~BlockDecompressor() = default;

 public:
    /**
     * Decompress data read from src, and write the decompressed data to dst.
     *
     * @param src Compressed data to read from
     * @param src_length The length of data which want to be decompressed
     * @param dst The target to write decompressed data
     * @param dst_length The max length of data
     * @return Length of decompressed data
     */
    virtual Result<int32_t> Decompress(const char* src, int32_t src_length, char* dst,
                                       int32_t dst_length) = 0;

 public:
    /// We put two integers before each compressed block, the first integer represents the
    /// compressed length of the block, and the second one represents the original length of the
    /// block.
    static constexpr int32_t HEADER_LENGTH = 8;
};
}  // namespace paimon
