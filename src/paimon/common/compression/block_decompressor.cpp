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

#include "paimon/common/compression/block_decompressor.h"

#include "fmt/format.h"
namespace paimon {

int32_t BlockDecompressor::ReadIntLE(const char* buf) {
    return (buf[0] & 0xFF) | ((buf[1] & 0xFF) << 8) | ((buf[2] & 0xFF) << 16) |
           ((buf[3] & 0xFF) << 24);
}

Status BlockDecompressor::ValidateLength(int32_t compressed_len, int32_t original_len) {
    if (original_len < 0 || compressed_len < 0 || (original_len == 0 && compressed_len != 0) ||
        (original_len != 0 && compressed_len == 0)) {
        return Status::IOError(
            fmt::format("Input is corrupted, compressed_len={}, , original_len={}", compressed_len,
                        original_len));
    }
    return Status::OK();
}

}  // namespace paimon
