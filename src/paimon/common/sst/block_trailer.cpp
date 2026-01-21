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

#include "paimon/common/sst/block_trailer.h"

#include "paimon/common/memory/memory_slice_output.h"

namespace paimon {

std::unique_ptr<BlockTrailer> BlockTrailer::ReadBlockTrailer(
    std::shared_ptr<MemorySliceInput>& input) {
    auto compress = input->ReadUnsignedByte();
    auto crc32c = input->ReadInt();
    return std::make_unique<BlockTrailer>(compress, crc32c);
}

int32_t BlockTrailer::Crc32c() const {
    return crc32c_;
}

int8_t BlockTrailer::CompressionType() const {
    return compression_type_;
}

std::string BlockTrailer::ToString() const {
    std::stringstream sstream;
    sstream << std::hex << crc32c_;
    return "BlockTrailer{compression_type=" + std::to_string(compression_type_) + ", crc32c_=0x" +
           sstream.str() + "}";
}

std::shared_ptr<MemorySlice> BlockTrailer::WriteBlockTrailer(MemoryPool* pool) {
    auto output = std::make_shared<MemorySliceOutput>(ENCODED_LENGTH, pool);
    output->WriteValue(compression_type_);
    output->WriteValue(crc32c_);
    return output->ToSlice();
}
}  // namespace paimon
