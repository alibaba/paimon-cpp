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

#include "paimon/common/compression/block_compression_type.h"
#include "paimon/common/compression/block_compressor.h"
#include "paimon/common/compression/block_decompressor.h"
#include "paimon/result.h"

namespace paimon {

/// Each compression codec has an implementation of {@link BlockCompressionFactory} to create
/// compressors and decompressors.
class BlockCompressionFactory {
 public:
    static Result<std::shared_ptr<BlockCompressionFactory>> Create(
        BlockCompressionType compression);

    BlockCompressionFactory() = default;
    virtual ~BlockCompressionFactory() = default;

 public:
    virtual BlockCompressionType GetCompressionType() const = 0;

    virtual std::shared_ptr<BlockCompressor> GetCompressor() = 0;

    virtual std::shared_ptr<BlockDecompressor> GetDecompressor() = 0;

 private:
    // Align java implementation
    static constexpr int32_t ZSTD_COMPRESSION_LEVEL = 1;
};
}  // namespace paimon
