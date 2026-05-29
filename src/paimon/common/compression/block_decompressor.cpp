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
    return static_cast<int32_t>(static_cast<uint32_t>(static_cast<uint8_t>(buf[0])) |
                                (static_cast<uint32_t>(static_cast<uint8_t>(buf[1])) << 8) |
                                (static_cast<uint32_t>(static_cast<uint8_t>(buf[2])) << 16) |
                                (static_cast<uint32_t>(static_cast<uint8_t>(buf[3])) << 24));
}

Status BlockDecompressor::ValidateLength(int32_t compressed_len, int32_t original_len) {
    if (original_len < 0 || compressed_len < 0 || (original_len == 0 && compressed_len != 0) ||
        (original_len != 0 && compressed_len == 0)) {
        return Status::Invalid(
            fmt::format("Input is corrupted, compressed_len={}, , original_len={}", compressed_len,
                        original_len));
    }
    return Status::OK();
}

}  // namespace paimon
