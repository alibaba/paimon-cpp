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

#include "fmt/format.h"
#include "paimon/common/compression/lz4/lz4_block_compression_factory.h"
#include "paimon/common/compression/none_block_compression_factory.h"
#include "paimon/common/compression/zstd/zstd_block_compression_factory.h"
#include "paimon/common/utils/string_utils.h"

namespace paimon {

Result<std::shared_ptr<BlockCompressionFactory>> BlockCompressionFactory::Create(
    const CompressOptions& compression) {
    auto compress = StringUtils::ToLowerCase(compression.compress);
    if (compress == "none") {
        return std::make_shared<NoneBlockCompressionFactory>();
    } else if (compress == "zstd") {
        return std::make_shared<ZstdBlockCompressionFactory>(compression.zstd_level);
    } else if (compress == "lz4") {
        return std::make_shared<Lz4BlockCompressionFactory>();
    }
    // TODO(liangzi): LZO support
    return Status::Invalid(fmt::format("Unsupported compression type: {}", compress));
}

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
            return Status::Invalid(
                fmt::format("Unsupported compression type: {}", static_cast<int32_t>(compression)));
    }
}
}  // namespace paimon
