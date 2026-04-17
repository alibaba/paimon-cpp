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

#include "fmt/format.h"
#include "paimon/common/lookup/sort/sort_lookup_store_footer.h"
#include "paimon/common/sst/sst_file_utils.h"
#include "paimon/common/utils/crc32c.h"
#include "paimon/common/utils/murmurhash_utils.h"
namespace paimon {

Result<std::shared_ptr<SstFileReader>> SstFileReader::Create(
    const std::shared_ptr<InputStream>& in, const BlockHandle& index_block_handle,
    const std::shared_ptr<BloomFilterHandle>& bloom_filter_handle,
    MemorySlice::SliceComparator comparator, const std::shared_ptr<CacheManager>& cache_manager,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::string file_path, in->GetUri());
    auto block_cache = std::make_shared<BlockCache>(file_path, in, cache_manager, pool);

    // read bloom filter directly now
    std::shared_ptr<BloomFilter> bloom_filter = nullptr;
    if (bloom_filter_handle && (bloom_filter_handle->ExpectedEntries() ||
                                bloom_filter_handle->Size() || bloom_filter_handle->Offset())) {
        bloom_filter = std::make_shared<BloomFilter>(bloom_filter_handle->ExpectedEntries(),
                                                     bloom_filter_handle->Size());
        PAIMON_ASSIGN_OR_RAISE(
            MemorySegment bloom_filter_data,
            block_cache->GetBlock(bloom_filter_handle->Offset(), bloom_filter_handle->Size(),
                                  /*is_index=*/true, /*decompress_func=*/nullptr));
        PAIMON_RETURN_NOT_OK(bloom_filter->SetMemorySegment(bloom_filter_data));
    }

    // create index block reader
    PAIMON_ASSIGN_OR_RAISE(
        MemorySegment trailer_data,
        block_cache->GetBlock(index_block_handle.Offset() + index_block_handle.Size(),
                              BlockTrailer::ENCODED_LENGTH, /*is_index=*/true,
                              /*decompress_func=*/nullptr));
    auto trailer_slice = MemorySlice::Wrap(trailer_data);
    auto trailer_input = trailer_slice.ToInput();
    std::shared_ptr<BlockTrailer> trailer = BlockTrailer::ReadBlockTrailer(&trailer_input);
    PAIMON_ASSIGN_OR_RAISE(
        MemorySegment block_data,
        block_cache->GetBlock(index_block_handle.Offset(), index_block_handle.Size(), true,
                              [pool, trailer](const MemorySegment& seg) -> Result<MemorySegment> {
                                  return DecompressBlock(seg, trailer, pool);
                              }));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BlockReader> reader,
                           BlockReader::Create(MemorySlice::Wrap(block_data), comparator));
    return std::shared_ptr<SstFileReader>(
        new SstFileReader(pool, block_cache, bloom_filter, reader, comparator));
}

Result<std::shared_ptr<SstFileReader>> SstFileReader::CreateFromStream(
    const std::shared_ptr<InputStream>& in, MemorySlice::SliceComparator comparator,
    const std::shared_ptr<CacheManager>& cache_manager, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(uint64_t file_len, in->Length());
    PAIMON_RETURN_NOT_OK(
        in->Seek(file_len - SortLookupStoreFooter::ENCODED_LENGTH, SeekOrigin::FS_SEEK_SET));
    auto footer_bytes = Bytes::AllocateBytes(SortLookupStoreFooter::ENCODED_LENGTH, pool.get());
    PAIMON_RETURN_NOT_OK(in->Read(footer_bytes->data(), footer_bytes->size()));
    auto footer_segment = MemorySegment::Wrap(std::move(footer_bytes));
    auto footer_slice = MemorySlice::Wrap(footer_segment);
    auto footer_input = footer_slice.ToInput();
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<SortLookupStoreFooter> read_footer,
                           SortLookupStoreFooter::ReadSortLookupStoreFooter(&footer_input));
    return SstFileReader::Create(in, read_footer->GetIndexBlockHandle(),
                                 read_footer->GetBloomFilterHandle(), std::move(comparator),
                                 cache_manager, pool);
}

SstFileReader::SstFileReader(const std::shared_ptr<MemoryPool>& pool,
                             const std::shared_ptr<BlockCache>& block_cache,
                             const std::shared_ptr<BloomFilter>& bloom_filter,
                             const std::shared_ptr<BlockReader>& index_block_reader,
                             MemorySlice::SliceComparator comparator)
    : pool_(pool),
      block_cache_(block_cache),
      bloom_filter_(bloom_filter),
      index_block_reader_(index_block_reader),
      comparator_(std::move(comparator)) {}

std::unique_ptr<SstFileIterator> SstFileReader::CreateIterator() {
    return std::make_unique<SstFileIterator>(this, index_block_reader_->Iterator());
}

std::unique_ptr<BlockIterator> SstFileReader::CreateIndexIterator() {
    return index_block_reader_->Iterator();
}

Result<std::shared_ptr<Bytes>> SstFileReader::Lookup(const std::shared_ptr<Bytes>& key) {
    if (bloom_filter_.get() && !bloom_filter_->TestHash(MurmurHashUtils::HashBytes(key))) {
        return std::shared_ptr<Bytes>();
    }
    auto key_slice = MemorySlice::Wrap(key);
    // seek the index to the block containing the key
    auto index_block_iterator = index_block_reader_->Iterator();
    PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] bool _, index_block_iterator->SeekTo(key_slice));
    // if indexIterator does not have a next, it means the key does not exist in this iterator
    if (index_block_iterator->HasNext()) {
        // seek the current iterator to the key
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BlockIterator> current,
                               GetNextBlock(index_block_iterator));
        PAIMON_ASSIGN_OR_RAISE(bool success, current->SeekTo(key_slice));
        if (success) {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BlockEntry> ret, current->Next());
            return ret->value.CopyBytes(pool_.get());
        }
    }
    return std::shared_ptr<Bytes>();
}

Result<std::unique_ptr<BlockIterator>> SstFileReader::GetNextBlock(
    std::unique_ptr<BlockIterator>& index_iterator) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BlockEntry> block_entry, index_iterator->Next());
    auto block_input = block_entry->value.ToInput();
    PAIMON_ASSIGN_OR_RAISE(BlockHandle block_handle, BlockHandle::ReadBlockHandle(&block_input));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BlockReader> reader, ReadBlock(block_handle, false));
    return reader->Iterator();
}

Result<std::shared_ptr<BlockReader>> SstFileReader::ReadBlock(const BlockHandle& handle,
                                                              bool index) {
    PAIMON_ASSIGN_OR_RAISE(
        MemorySegment trailer_data,
        block_cache_->GetBlock(handle.Offset() + handle.Size(), BlockTrailer::ENCODED_LENGTH,
                               /*is_index=*/true, /*decompress_func=*/nullptr));
    auto trailer_slice = MemorySlice::Wrap(trailer_data);
    auto trailer_input = trailer_slice.ToInput();
    std::shared_ptr<paimon::BlockTrailer> trailer = BlockTrailer::ReadBlockTrailer(&trailer_input);
    PAIMON_ASSIGN_OR_RAISE(
        MemorySegment block_data,
        block_cache_->GetBlock(handle.Offset(), handle.Size(), index,
                               [this, trailer](const MemorySegment& seg) -> Result<MemorySegment> {
                                   return DecompressBlock(seg, trailer, pool_);
                               }));
    return BlockReader::Create(MemorySlice::Wrap(block_data), comparator_);
}

Result<MemorySegment> SstFileReader::DecompressBlock(const MemorySegment& compressed_data,
                                                     const std::shared_ptr<BlockTrailer>& trailer,
                                                     const std::shared_ptr<MemoryPool>& pool) {
    auto input_memory = compressed_data.GetHeapMemory();

    // check crc32c
    auto crc32c_code = CRC32C::calculate(input_memory->data(), input_memory->size());
    auto compression_val =
        static_cast<char>(static_cast<int32_t>(trailer->CompressionType()) & 0xFF);
    crc32c_code = CRC32C::calculate(&compression_val, 1, crc32c_code);
    if (trailer->Crc32c() != static_cast<int32_t>(crc32c_code)) {
        return Status::Invalid(fmt::format("Expected crc32c({}) but found crc32c({})",
                                           SstFileUtils::ToHexString(trailer->Crc32c()),
                                           SstFileUtils::ToHexString(crc32c_code)));
    }

    // decompress data
    PAIMON_ASSIGN_OR_RAISE(BlockCompressionType compress_type,
                           SstFileUtils::From(trailer->CompressionType()));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BlockCompressionFactory> factory,
                           BlockCompressionFactory::Create(compress_type));
    if (!factory || factory->GetCompressionType() == BlockCompressionType::NONE) {
        return compressed_data;
    } else {
        auto decompressor = factory->GetDecompressor();
        auto slice = MemorySlice::Wrap(compressed_data);
        auto input = slice.ToInput();
        PAIMON_ASSIGN_OR_RAISE(int32_t uncompressed_size, input.ReadVarLenInt());
        auto output = MemorySegment::AllocateHeapMemory(uncompressed_size, pool.get());
        auto output_memory = output.GetHeapMemory();
        PAIMON_ASSIGN_OR_RAISE(
            int32_t actual_uncompressed_size,
            decompressor->Decompress(input_memory->data() + input.Position(), input.Available(),
                                     output_memory->data(), output_memory->size()));
        if (static_cast<size_t>(actual_uncompressed_size) != output_memory->size()) {
            return Status::Invalid(fmt::format(
                "Invalid data: expect uncompressed size {}, actual uncompressed size {}",
                output_memory->size(), actual_uncompressed_size));
        }
        return output;
    }
}

Status SstFileReader::Close() {
    // TODO(xinyu.lxy): support close FileBasedBloomFilter
    block_cache_->Close();
    return Status::OK();
}

SstFileIterator::SstFileIterator(SstFileReader* reader,
                                 std::unique_ptr<BlockIterator> index_iterator)
    : reader_(reader), index_iterator_(std::move(index_iterator)) {}

Status SstFileIterator::SeekTo(const std::shared_ptr<Bytes>& key) {
    auto key_slice = MemorySlice::Wrap(key);
    PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] bool index_success, index_iterator_->SeekTo(key_slice));
    if (index_iterator_->HasNext()) {
        PAIMON_ASSIGN_OR_RAISE(data_iterator_, reader_->GetNextBlock(index_iterator_));
        // The index block entry key is the last key of the corresponding data block.
        // If there is some index entry key >= target key, the related data block must
        // also contain some key >= target key, which means seeked_data_block.HasNext()
        // must be true
        PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] bool data_success,
                               data_iterator_->SeekTo(key_slice));
    } else {
        data_iterator_.reset();
    }
    return Status::OK();
}
}  // namespace paimon
