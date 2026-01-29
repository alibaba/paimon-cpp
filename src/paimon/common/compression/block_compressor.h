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

/// A compressor which compresses a whole byte array each time.
class BlockCompressor {
 public:
    static void WriteIntLE(int32_t val, char* buf);

 public:
    virtual ~BlockCompressor() = default;

 public:
    /**
     * Get the max compressed size for a given original size.
     * @param src_size The original size
     * @return The max compressed size
     */
    virtual int32_t GetMaxCompressedSize(int32_t src_size) = 0;

    /**
     * Compress data read from src, and write the compressed data to dst.
     *
     * @param src Uncompressed data to read from
     * @param src_length The length of data which want to be compressed
     * @param dst The target to write compressed data
     * @param dst_length The max length of data
     * @return Length of compressed data
     */
    /// Compress data read from src, and write the compressed data to dst.
    virtual Result<int32_t> Compress(const char* src, int32_t src_length, char* dst,
                                     int32_t dst_length) = 0;

 public:
    /**
     * We put two integers before each compressed block, the first integer represents the compressed
     * length of the block, and the second one represents the original length of the block.
     */
    static constexpr int32_t HEADER_LENGTH = 8;
};
}  // namespace paimon
