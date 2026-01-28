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
#include "paimon/common/sst/sst_file_reader.h"

#include "paimon/common/sst/sst_file_utils.h"
#include "paimon/common/utils/crc32c.h"
#include "paimon/common/utils/murmurhash_utils.h"

namespace paimon {

Result<std::shared_ptr<SstFileReader>> SstFileReader::Create(
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<paimon::FileSystem>& fs,
    const std::string& file_path,
    std::function<int32_t(const std::shared_ptr<MemorySlice>&, const std::shared_ptr<MemorySlice>&)>
        comparator) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> in, fs->Open(file_path));
    PAIMON_ASSIGN_OR_RAISE(uint64_t file_len, in->Length());
    auto block_cache =
        std::make_shared<BlockCache>(file_path, in, pool, std::make_unique<CacheManager>());

    // read footer
    auto segment = block_cache->GetBlock(file_len - BlockFooter::ENCODED_LENGTH,
                                         BlockFooter::ENCODED_LENGTH, true);
    if (!segment.get()) {
        return Status::Invalid("Read footer error");
    }
    auto slice = MemorySlice::Wrap(segment);
    auto input = slice->ToInput();
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::BlockFooter> footer,
                           BlockFooter::ReadBlockFooter(input));

    // read bloom filter directly now
    auto bloom_filter_handle = footer->GetBloomFilterHandle();
    std::shared_ptr<paimon::BloomFilter> bloom_filter = nullptr;
    if (bloom_filter_handle->ExpectedEntries() || bloom_filter_handle->Size() ||
        bloom_filter_handle->Offset()) {
        bloom_filter = std::make_shared<BloomFilter>(bloom_filter_handle->ExpectedEntries(),
                                                     bloom_filter_handle->Size());
        PAIMON_RETURN_NOT_OK(bloom_filter->SetMemorySegment(block_cache->GetBlock(
            bloom_filter_handle->Offset(), bloom_filter_handle->Size(), true)));
    }

    // create index block reader
    auto index_block_handle = footer->GetIndexBlockHandle();
    auto trailer_data =
        block_cache->GetBlock(index_block_handle->Offset() + index_block_handle->Size(),
                              BlockTrailer::ENCODED_LENGTH, true);
    auto trailer_input = MemorySlice::Wrap(trailer_data)->ToInput();
    auto trailer = BlockTrailer::ReadBlockTrailer(trailer_input);
    auto block_data =
        block_cache->GetBlock(index_block_handle->Offset(), index_block_handle->Size(), true);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::MemorySegment> uncompressed_data,
                           DecompressBlock(block_data, trailer, pool));
    auto reader = BlockReader::Create(MemorySlice::Wrap(uncompressed_data), comparator);

    return std::make_shared<SstFileReader>(pool, block_cache, bloom_filter, reader, comparator);
}

SstFileReader::SstFileReader(
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<BlockCache>& block_cache,
    const std::shared_ptr<BloomFilter>& bloom_filter,
    const std::shared_ptr<paimon::BlockReader>& index_block_reader,
    std::function<int32_t(const std::shared_ptr<MemorySlice>&, const std::shared_ptr<MemorySlice>&)>
        comparator)
    : pool_(pool),
      block_cache_(block_cache),
      bloom_filter_(bloom_filter),
      index_block_reader_(index_block_reader),
      comparator_(std::move(comparator)) {}

std::unique_ptr<SstFileIterator> SstFileReader::CreateIterator() {
    return std::make_unique<SstFileIterator>(this, index_block_reader_->Iterator());
}

std::shared_ptr<Bytes> SstFileReader::Lookup(std::shared_ptr<Bytes> key) {
    if (bloom_filter_.get() && !bloom_filter_->TestHash(MurmurHashUtils::HashBytes(key))) {
        return nullptr;
    }
    auto key_slice = MemorySlice::Wrap(key);
    // seek the index to the block containing the key
    auto index_block_iterator = index_block_reader_->Iterator();
    index_block_iterator->SeekTo(key_slice);
    // if indexIterator does not have a next, it means the key does not exist in this iterator
    if (index_block_iterator->HasNext()) {
        // seek the current iterator to the key
        auto current = GetNextBlock(index_block_iterator);
        if (!current.ok() || !current.value()) {
            return nullptr;
        }
        auto& it = current.value();
        if (it->SeekTo(key_slice)) {
            auto ret = it->Next();
            if (!ret.ok()) {
                return nullptr;
            }
            return ret.value()->value_->CopyBytes(pool_.get());
        }
    }
    return nullptr;
}

Result<std::unique_ptr<BlockIterator>> SstFileReader::GetNextBlock(
    std::unique_ptr<BlockIterator>& index_iterator) {
    PAIMON_ASSIGN_OR_RAISE(auto ret, index_iterator->Next());
    auto& slice = ret->value_;
    auto input = slice->ToInput();
    PAIMON_ASSIGN_OR_RAISE(auto reader, ReadBlock(BlockHandle::ReadBlockHandle(input), false));
    return reader->Iterator();
}

Result<std::shared_ptr<BlockReader>> SstFileReader::ReadBlock(std::shared_ptr<BlockHandle>&& handle,
                                                              bool index) {
    auto block_handle = handle;
    return ReadBlock(block_handle, index);
}

Result<std::shared_ptr<BlockReader>> SstFileReader::ReadBlock(
    const std::shared_ptr<BlockHandle>& handle, bool index) {
    auto trailer_data = block_cache_->GetBlock(handle->Offset() + handle->Size(),
                                               BlockTrailer::ENCODED_LENGTH, true);
    auto trailer_input = MemorySlice::Wrap(trailer_data)->ToInput();
    auto trailer = BlockTrailer::ReadBlockTrailer(trailer_input);
    auto block_data = block_cache_->GetBlock(handle->Offset(), handle->Size(), index);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::MemorySegment> uncompressed_data,
                           DecompressBlock(block_data, trailer, pool_));
    return BlockReader::Create(MemorySlice::Wrap(uncompressed_data), comparator_);
}

Result<std::shared_ptr<paimon::MemorySegment>> SstFileReader::DecompressBlock(
    const std::shared_ptr<paimon::MemorySegment>& compressed_data,
    const std::unique_ptr<BlockTrailer>& trailer, const std::shared_ptr<MemoryPool>& pool) {
    auto input_memory = compressed_data->GetHeapMemory();

    // check crc32c
    auto crc32c_code = CRC32C::calculate(input_memory->data(), input_memory->size());
    auto compression_val =
        static_cast<char>(static_cast<int32_t>(trailer->CompressionType()) & 0xFF);
    crc32c_code = CRC32C::calculate(&compression_val, 1, crc32c_code);
    if (trailer->Crc32c() != static_cast<int32_t>(crc32c_code)) {
        return Status::IOError("Expected crc32c(" + SstFileUtils::ToHexString(trailer->Crc32c()) +
                               ") but found crc32c(" + SstFileUtils::ToHexString(crc32c_code) +
                               ")");
    }

    // decompress data
    PAIMON_ASSIGN_OR_RAISE(auto factory, BlockCompressionFactory::Create(
                                             SstFileUtils::From(trailer->CompressionType())));
    if (!factory || factory->GetCompressionType() == BlockCompressionType::NONE) {
        return compressed_data;
    } else {
        auto decompressor = factory->GetDecompressor();
        auto input = MemorySlice::Wrap(compressed_data)->ToInput();
        auto output = MemorySegment::AllocateHeapMemory(input->ReadVarLenInt(), pool.get());
        auto output_memory = output.GetHeapMemory();
        PAIMON_ASSIGN_OR_RAISE(
            int uncompressed_length,
            decompressor->Decompress(input_memory->data() + input->Position(), input->Available(),
                                     output_memory->data(), output_memory->size()));
        if (static_cast<size_t>(uncompressed_length) != output_memory->size()) {
            return Status::Invalid("Invalid data");
        }
        return std::make_shared<MemorySegment>(output);
    }
}

SstFileIterator::SstFileIterator(SstFileReader* reader,
                                 std::unique_ptr<BlockIterator> index_iterator)
    : reader_(reader), index_iterator_(std::move(index_iterator)) {}

Status SstFileIterator::SeekTo(std::shared_ptr<Bytes>& key) {
    auto key_slice = MemorySlice::Wrap(key);
    index_iterator_->SeekTo(key_slice);
    if (index_iterator_->HasNext()) {
        PAIMON_ASSIGN_OR_RAISE(data_iterator_, reader_->GetNextBlock(index_iterator_));
        // The index block entry key is the last key of the corresponding data block.
        // If there is some index entry key >= target key, the related data block must
        // also contain some key >= target key, which means seeked_data_block.HasNext()
        // must be true
        data_iterator_->SeekTo(key_slice);
    } else {
        data_iterator_.reset();
    }
    return Status::OK();
}
}  // namespace paimon
