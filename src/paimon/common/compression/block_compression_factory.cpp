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

#include "paimon/common/compression/block_compression_factory.h"

#include "paimon/common/compression/lz4/lz4_block_compression_factory.h"
#include "paimon/common/compression/none_block_compression_factory.h"
#include "paimon/common/compression/zstd/zstd_block_compression_factory.h"

namespace paimon {

Result<std::shared_ptr<BlockCompressionFactory>> BlockCompressionFactory::Create(
    BlockCompressionType compression) {
    switch (compression) {
        case BlockCompressionType::NONE:
            return std::make_shared<NoneBlockCompressionFactory>();
        case BlockCompressionType::LZ4:
            return std::make_shared<Lz4BlockCompressionFactory>();
        case BlockCompressionType::ZSTD:
            return std::make_shared<ZstdBlockCompressionFactory>(ZSTD_COMPRESSION_LEVEL);
        default:
            // TODO(liangzi): LZO support
            return Status::Invalid("Unsupported compression type: " +
                                   std::to_string(static_cast<int8_t>(compression)));
    }
}

}  // namespace paimon
