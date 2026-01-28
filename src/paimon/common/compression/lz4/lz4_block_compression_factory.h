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

#include "paimon/common/compression/block_compression_factory.h"
#include "paimon/common/compression/lz4/lz4_block_compressor.h"
#include "paimon/common/compression/lz4/lz4_block_decompressor.h"
#include "paimon/result.h"

namespace paimon {

/// Implementation of {@link BlockCompressionFactory} for lz4 codec.
class Lz4BlockCompressionFactory : public BlockCompressionFactory {
 public:
    BlockCompressionType GetCompressionType() const override {
        return BlockCompressionType::LZ4;
    }

    std::shared_ptr<BlockCompressor> GetCompressor() override {
        return std::make_shared<Lz4BlockCompressor>();
    }

    std::shared_ptr<BlockDecompressor> GetDecompressor() override {
        return std::make_shared<Lz4BlockDecompressor>();
    }
};
}  // namespace paimon
