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

#include "paimon/common/utils/murmurhash_utils.h"

namespace paimon {

Result<std::shared_ptr<SstFileReader>> SstFileReader::Create(
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<BlockCache>& block_cache,
    int64_t file_len,
    std::function<int32_t(const std::shared_ptr<MemorySlice>&, const std::shared_ptr<MemorySlice>&)>
        comparator) {
    // read footer
    auto segment = block_cache->GetBlock(file_len - BlockFooter::ENCODED_LENGTH,
                                         BlockFooter::ENCODED_LENGTH, true);
    if (!segment.get()) {
        return Status::Invalid("Read footer error");
    }
    auto slice = MemorySlice::Wrap(segment);
    auto input = slice->ToInput();
    auto footer = BlockFooter::ReadBlockFooter(input);
    if (!footer.get()) {
        return Status::Invalid("Read footer error");
    }

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

    return std::make_shared<SstFileReader>(pool, block_cache, footer->GetIndexBlockHandle(),
                                           bloom_filter, comparator);
}

SstFileReader::SstFileReader(
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<BlockCache>& block_cache,
    const std::shared_ptr<BlockHandle>& index_block_handle,
    const std::shared_ptr<BloomFilter>& bloom_filter,
    std::function<int32_t(const std::shared_ptr<MemorySlice>&, const std::shared_ptr<MemorySlice>&)>
        comparator)
    : pool_(pool), block_cache_(block_cache), bloom_filter_(bloom_filter), comparator_(comparator) {
    index_block_reader_ = ReadBlock(index_block_handle, true);
}

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
        if (!current.get()) {
            return nullptr;
        }
        if (current->SeekTo(key_slice)) {
            auto ret = current->Next();
            if (!ret.ok()) {
                return nullptr;
            }
            return ret.value()->value_->CopyBytes(pool_.get());
        }
    }
    return nullptr;
}

std::unique_ptr<BlockIterator> SstFileReader::GetNextBlock(
    std::unique_ptr<BlockIterator>& index_iterator) {
    auto ret = index_iterator->Next();
    if (!ret.ok()) {
        return nullptr;
    }
    auto& slice = ret.value()->value_;
    auto input = slice->ToInput();
    return ReadBlock(BlockHandle::ReadBlockHandle(input), false)->Iterator();
}

std::shared_ptr<BlockReader> SstFileReader::ReadBlock(std::shared_ptr<BlockHandle>&& handle,
                                                      bool index) {
    auto block_handle = handle;
    return ReadBlock(block_handle, index);
}

std::shared_ptr<BlockReader> SstFileReader::ReadBlock(const std::shared_ptr<BlockHandle>& handle,
                                                      bool index) {
    // read block trailer
    auto trailer_data = block_cache_->GetBlock(handle->Offset() + handle->Size(),
                                               BlockTrailer::ENCODED_LENGTH, true);
    auto trailer_input = MemorySlice::Wrap(trailer_data)->ToInput();
    auto trailer = BlockTrailer::ReadBlockTrailer(trailer_input);

    auto block_data = block_cache_->GetBlock(handle->Offset(), handle->Size(), index);
    return BlockReader::Create(MemorySlice::Wrap(block_data), comparator_);
}

SstFileIterator::SstFileIterator(SstFileReader* reader,
                                 std::unique_ptr<BlockIterator> index_iterator)
    : reader_(reader), index_iterator_(std::move(index_iterator)) {}

/**
 * Seek to the position of the record whose key is exactly equal to or greater than the
 * specified key.
 */
void SstFileIterator::SeekTo(std::shared_ptr<Bytes>& key) {
    auto key_slice = MemorySlice::Wrap(key);
    index_iterator_->SeekTo(key_slice);
    if (index_iterator_->HasNext()) {
        data_iterator_ = reader_->GetNextBlock(index_iterator_);
        // The index block entry key is the last key of the corresponding data block.
        // If there is some index entry key >= targetKey, the related data block must
        // also contain some key >= target key, which means seekedDataBlock.hasNext()
        // must be true
        data_iterator_->SeekTo(key_slice);
    } else {
        data_iterator_.reset();
    }
}
}  // namespace paimon
