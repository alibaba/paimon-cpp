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

#include "paimon/common/sst/sst_file_writer.h"

#include "paimon/common/sst/sst_file_utils.h"
#include "paimon/common/utils/crc32c.h"
#include "paimon/common/utils/murmurhash_utils.h"

namespace paimon {
SstFileWriter::SstFileWriter(const std::shared_ptr<OutputStream>& out,
                             const std::shared_ptr<MemoryPool>& pool,
                             const std::shared_ptr<BloomFilter>& bloom_filter, int32_t block_size,
                             const std::shared_ptr<BlockCompressionFactory>& factory)
    : out_(out), pool_(pool), bloom_filter_(bloom_filter), block_size_(block_size) {
    data_block_writer_ =
        std::make_unique<BlockWriter>(static_cast<int32_t>(block_size * 1.1), pool);
    index_block_writer_ =
        std::make_unique<BlockWriter>(BlockHandle::MAX_ENCODED_LENGTH * 1024, pool);
    compression_type_ = factory->GetCompressionType();
    compressor_ = factory->GetCompressor();
}

Status SstFileWriter::Write(std::shared_ptr<Bytes>&& key, std::shared_ptr<Bytes>&& value) {
    data_block_writer_->Write(key, value);
    last_key_ = key;
    if (data_block_writer_->Memory() > block_size_) {
        PAIMON_RETURN_NOT_OK(Flush());
    }
    if (bloom_filter_.get()) {
        PAIMON_RETURN_NOT_OK(bloom_filter_->AddHash(MurmurHashUtils::HashBytes(key)));
    }
    return Status::OK();
}

Status SstFileWriter::Write(std::shared_ptr<MemorySlice>& slice) {
    auto data = slice->ReadStringView();
    return WriteBytes(data.data(), data.size());
}

Status SstFileWriter::Flush() {
    if (data_block_writer_->Size() == 0) {
        return Status::OK();
    }

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BlockHandle> handle,
                           FlushBlockWriter(data_block_writer_));

    auto slice = handle->WriteBlockHandle(pool_.get());
    auto value = slice->CopyBytes(pool_.get());
    index_block_writer_->Write(last_key_, value);
    return Status::OK();
}

Result<std::shared_ptr<BlockHandle>> SstFileWriter::WriteIndexBlock() {
    return FlushBlockWriter(index_block_writer_);
}

Result<std::shared_ptr<BloomFilterHandle>> SstFileWriter::WriteBloomFilter() {
    if (!bloom_filter_.get()) {
        return Status::Invalid("bloom_filter_ should be set before writing");
    }
    auto data = bloom_filter_->GetBitSet()->ToSlice()->ReadStringView();
    auto handle = std::make_shared<BloomFilterHandle>(out_->GetPos().value(), data.size(),
                                                      bloom_filter_->ExpectedEntries());

    PAIMON_RETURN_NOT_OK(WriteBytes(data.data(), data.size()));

    return handle;
}

Status SstFileWriter::WriteFooter(const std::shared_ptr<BlockHandle>& index_block_handle,
                                  const std::shared_ptr<BloomFilterHandle>& bloom_filter_handle) {
    auto footer = std::make_shared<BlockFooter>(index_block_handle, bloom_filter_handle);
    auto slice = footer->WriteBlockFooter(pool_.get());
    auto data = slice->ReadStringView();
    PAIMON_RETURN_NOT_OK(WriteBytes(data.data(), data.size()));
    return Status::OK();
}

Result<std::shared_ptr<BlockHandle>> SstFileWriter::FlushBlockWriter(
    std::unique_ptr<BlockWriter>& writer) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::MemorySlice> memory_slice, writer->Finish());

    auto view = memory_slice->ReadStringView();

    std::shared_ptr<Bytes> buffer;
    BlockCompressionType compression_type = BlockCompressionType::NONE;
    if (compressor_.get()) {
        auto new_size = compressor_->GetMaxCompressedSize(view.size());
        // 5 bytes for original length
        buffer = std::make_shared<Bytes>(new_size + 5, pool_.get());
        PAIMON_ASSIGN_OR_RAISE(int32_t offset, WriteVarLenInt(buffer->data(), view.size()));
        PAIMON_ASSIGN_OR_RAISE(int32_t actual_size, compressor_->Compress(view.data(), view.size(),
                                                                          buffer->data() + offset,
                                                                          buffer->size() - offset));
        actual_size += offset;
        // Don't use the compressed data if compressed less than 12.5%,
        if (static_cast<size_t>(actual_size) < view.size() - (view.size() / 8)) {
            compression_type = compression_type_;
            view = std::string_view{buffer->data(), static_cast<size_t>(actual_size)};
        }
    }

    auto crc32c = CRC32C::calculate(view.data(), view.size());
    auto compression_val = static_cast<char>(static_cast<int32_t>(compression_type) & 0xFF);
    crc32c = CRC32C::calculate(&compression_val, 1, crc32c);
    auto trailer_memory_slice =
        std::make_shared<BlockTrailer>(static_cast<int8_t>(compression_type), crc32c)
            ->WriteBlockTrailer(pool_.get());
    auto block_handle = std::make_shared<BlockHandle>(out_->GetPos().value_or(0), view.size());

    // 1. write data
    PAIMON_RETURN_NOT_OK(WriteBytes(view.data(), view.size()));

    // 2. write trailer
    auto trailer_data = trailer_memory_slice->ReadStringView();
    PAIMON_RETURN_NOT_OK(WriteBytes(trailer_data.data(), trailer_data.size()));

    writer->Reset();

    return block_handle;
}

Status SstFileWriter::WriteBytes(const char* data, size_t size) {
    PAIMON_RETURN_NOT_OK(out_->Write(data, size));
    return Status::OK();
}

Result<int32_t> SstFileWriter::WriteVarLenInt(char* bytes, int32_t value) {
    if (value < 0) {
        return Status::Invalid("negative value: v=" + std::to_string(value));
    }
    int i = 0;
    while ((value & ~0x7F) != 0) {
        bytes[i++] = (static_cast<char>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    bytes[i++] = static_cast<char>(value);
    return i;
}
}  // namespace paimon
