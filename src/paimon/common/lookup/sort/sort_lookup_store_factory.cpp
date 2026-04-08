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

#include "paimon/common/lookup/sort/sort_lookup_store_factory.h"

#include "paimon/common/lookup/sort/sort_lookup_store_footer.h"
#include "paimon/common/memory/memory_slice.h"
#include "paimon/memory/bytes.h"
namespace paimon {
Result<std::unique_ptr<LookupStoreWriter>> SortLookupStoreFactory::CreateWriter(
    const std::shared_ptr<paimon::FileSystem>& fs, const std::string& file_path,
    const std::shared_ptr<BloomFilter>& bloom_filter,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> out,
                           fs->Create(file_path, /*overwrite=*/false));
    return std::make_unique<SortLookupStoreWriter>(
        out,
        std::make_shared<SstFileWriter>(out, bloom_filter, block_size_, compression_factory_, pool),
        pool);
}

Result<std::unique_ptr<LookupStoreReader>> SortLookupStoreFactory::CreateReader(
    const std::shared_ptr<paimon::FileSystem>& fs, const std::string& file_path,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> in, fs->Open(file_path));
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
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<SstFileReader> reader,
        SstFileReader::Create(pool, in, read_footer->GetIndexBlockHandle(),
                              read_footer->GetBloomFilterHandle(), comparator_, cache_manager_));
    return std::make_unique<SortLookupStoreReader>(in, reader);
}

Status SortLookupStoreReader::Close() {
    PAIMON_RETURN_NOT_OK(reader_->Close());
    return in_->Close();
}

Status SortLookupStoreWriter::Close() {
    PAIMON_RETURN_NOT_OK(writer_->Flush());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BloomFilterHandle> bloom_filter_handle,
                           writer_->WriteBloomFilter());
    PAIMON_ASSIGN_OR_RAISE(BlockHandle index_block_handle, writer_->WriteIndexBlock());
    SortLookupStoreFooter footer(index_block_handle, bloom_filter_handle);
    auto slice = footer.WriteSortLookupStoreFooter(pool_.get());
    PAIMON_RETURN_NOT_OK(writer_->WriteSlice(slice));
    PAIMON_RETURN_NOT_OK(out_->Close());
    return Status::OK();
}

}  // namespace paimon