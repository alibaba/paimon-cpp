/*
 * Copyright 2024-present Alibaba Inc.
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

#include "paimon/common/sst/block_trailer.h"

namespace paimon {

std::unique_ptr<BlockTrailer> BlockTrailer::ReadBlockTrailer(
    std::unique_ptr<MemorySliceInput>& input) {
    auto compress = input->ReadUnsignedByte();
    auto crc32c = input->ReadInt();
    return std::make_unique<BlockTrailer>(compress, crc32c);
}

BlockTrailer::BlockTrailer(int8_t compression_type, int32_t crc32c)
    : compression_type_(compression_type), crc32c_(crc32c) {}

int32_t BlockTrailer::Crc32c() const {
    return crc32c_;
}

char BlockTrailer::CompressionType() const {
    return compression_type_;
}

std::string BlockTrailer::ToString() const {
    return "BlockTrailer{compression_type=" + std::to_string(compression_type_) + ", crc32c_=0x" +
           std::to_string(crc32c_) + '}';
}

std::shared_ptr<Bytes> BlockTrailer::ToBytes() {
    std::shared_ptr<Bytes> bytes = std::make_shared<Bytes>(ENCODED_LENGTH);
    // todo
    return bytes;
}
}  // namespace paimon
