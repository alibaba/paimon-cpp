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
namespace paimon {
class MemoryPool;

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
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::MemorySlice> block_data, writer->Finish());

    auto size = block_data->Length();
    // todo attempt to compress the block
    auto view = block_data->ReadStringView();
    auto crc32 = arrow::internal::crc32(0, view.data(), view.size());
    auto trailer = std::make_shared<BlockTrailer>(0, crc32)->WriteBlockTrailer(pool_.get());
    auto trailer_data = trailer->ReadStringView();

    auto block_handle = std::make_shared<BlockHandle>(out_->GetPos().value_or(0), size);

    // 1. write data
    PAIMON_RETURN_NOT_OK(WriteBytes(view.data(), view.size()));

    // 2. write trailer
    PAIMON_RETURN_NOT_OK(WriteBytes(trailer_data.data(), trailer_data.size()));

    writer->Reset();
    return block_handle;
}

Status SstFileWriter::WriteBytes(const char* data, size_t size) {
    PAIMON_RETURN_NOT_OK(out_->Write(data, size));
    return Status::OK();
}
}  // namespace paimon
