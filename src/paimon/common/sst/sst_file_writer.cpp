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

#include "paimon/common/sst/sst_file_writer.h"

#include "arrow/util/crc32.h"
#include "paimon/common/sst/block_trailer.h"
#include "paimon/common/utils/murmurhash_utils.h"

namespace paimon {
class MemoryPool;

Status SstFileWriter::Write(std::shared_ptr<Bytes>& key, std::shared_ptr<Bytes>& value) {
    Write(std::move(key), std::move(value));
}

Status SstFileWriter::Write(std::shared_ptr<Bytes>&& key, std::shared_ptr<Bytes>&& value) {
    data_block_writer_->Write(key, value);
    last_key_ = key;
    if (data_block_writer_->Size() > block_size_) {
        auto res = Flush();
        if (!res.ok()) {
            return res;
        }
    }
    if (bloom_filter_) {
        bloom_filter_->AddHash(MurmurHashUtils::HashBytes(key));
    }
    record_count_++;
}

Status SstFileWriter::Flush() {
    if (data_block_writer_->Size() == 0) {
        return Status::OK();
    }
    auto handle = FlushBlockWriter(data_block_writer_);
    if (!handle.ok()) {
        return handle.status();
    }

    auto bytes = handle.value()->ToBytes();
    index_block_writer_->Write(last_key_, bytes);
}

Result<std::shared_ptr<BlockHandle>> SstFileWriter::WriteIndexBlock() {
    return FlushBlockWriter(index_block_writer_);
}

Result<std::shared_ptr<BloomFilterHandle>> SstFileWriter::WriteBloomFilter() {
    if (!bloom_filter_.get()) {
        return Status::OK();
    }
    auto bitSet = bloom_filter_->GetBitSet();
    auto segment = bitSet->GetMemorySegment();

    auto handle = std::make_shared<BloomFilterHandle>(out_->GetPos(), bitSet->GetBitLength(),
                                                      bloom_filter_->GetExpectedEntries());
    auto bytes = std::make_shared<Bytes>(segment->GetArray());
    WriteBytes(bytes);
    return handle;
}

Result<std::shared_ptr<BlockHandle>> SstFileWriter::FlushBlockWriter(
    std::unique_ptr<BlockWriter>& writer) {
    auto ret = writer->Finish();
    if (!ret.ok()) {
        return ret.status();
    }
    auto size = writer->Size();

    // todo attempt to compress the block
    auto& memory_slice = ret.value();
    auto bytes = memory_slice->GetHeapMemory();
    auto crc32 = arrow::internal::crc32(0, bytes->data(), bytes->size());
    auto trailer = std::make_shared<BlockTrailer>(0, crc32)->ToBytes();

    auto block_handle = std::make_shared<BlockHandle>(out_->GetPos().value_or(0), size);

    // 1. write data
    WriteBytes(bytes);

    // write trailer
    WriteBytes(trailer);

    writer->Reset();
    return block_handle;
}

Status SstFileWriter::WriteBytes(std::shared_ptr<Bytes>& bytes) {
    out_->Write(bytes->data(), bytes->size());
}
}  // namespace paimon
