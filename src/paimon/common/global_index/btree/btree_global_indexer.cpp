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
#include "paimon/common/global_index/btree/btree_global_indexer.h"

#include "paimon/common/global_index/btree/btree_file_footer.h"
#include "paimon/common/global_index/btree/btree_index_meta.h"
#include "paimon/common/memory/memory_slice.h"
#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/common/sst/block_cache.h"
#include "paimon/common/sst/block_handle.h"
#include "paimon/common/utils/crc32c.h"
#include "paimon/common/utils/roaring_navigable_map64.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/global_index/bitmap_global_index_result.h"

namespace paimon {
Result<std::shared_ptr<GlobalIndexReader>> BTreeGlobalIndexer::CreateReader(
    ::ArrowSchema* arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::vector<GlobalIndexIOMeta>& files, const std::shared_ptr<MemoryPool>& pool) const {
    if (files.size() != 1) {
        return Status::Invalid(
            "invalid GlobalIndexIOMeta for BTreeGlobalIndex, exist multiple metas");
    }
    const auto& meta = files[0];
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> in,
                           file_reader->GetInputStream(meta.file_path));

    // prepare file footer
    auto cache_manager = std::make_shared<CacheManager>();
    auto block_cache = std::make_shared<BlockCache>(meta.file_path, in, pool.get(), cache_manager);
    auto segment = block_cache->GetBlock(meta.file_size - BTreeFileFooter::ENCODED_LENGTH,
                                         BTreeFileFooter::ENCODED_LENGTH, true);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BTreeFileFooter> footer,
                           BTreeFileFooter::Read(MemorySlice::Wrap(segment)->ToInput()));

    auto index_meta = BTreeIndexMeta::Deserialize(meta.metadata, pool.get());

    return std::make_shared<BTreeGlobalIndexReader>(file_reader, files, pool);
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexer::ToGlobalIndexResult(
    int64_t range_end, const std::shared_ptr<FileIndexResult>& result) {
    if (auto remain = std::dynamic_pointer_cast<Remain>(result)) {
        return std::make_shared<BitmapGlobalIndexResult>([range_end]() -> Result<RoaringBitmap64> {
            RoaringBitmap64 bitmap;
            bitmap.AddRange(0, range_end + 1);
            return bitmap;
        });
    } else if (auto skip = std::dynamic_pointer_cast<Skip>(result)) {
        return std::make_shared<BitmapGlobalIndexResult>(
            []() -> Result<RoaringBitmap64> { return RoaringBitmap64(); });
    } else if (auto bitmap_result = std::dynamic_pointer_cast<BitmapIndexResult>(result)) {
        return std::make_shared<BitmapGlobalIndexResult>(
            [bitmap_result]() -> Result<RoaringBitmap64> {
                PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* bitmap, bitmap_result->GetBitmap());
                return RoaringBitmap64(*bitmap);
            });
    }
    return Status::Invalid(
        "invalid FileIndexResult, supposed to be Remain or Skip or BitmapIndexResult");
}

Result<std::shared_ptr<RoaringNavigableMap64>> BTreeGlobalIndexer::ReadNullBitmap(
    const std::shared_ptr<BlockCache>& cache, const std::shared_ptr<BlockHandle>& block_handle) {
    auto null_bitmap = std::make_shared<RoaringNavigableMap64>();
    if (block_handle == nullptr) {
        return null_bitmap;
    }

    // Read bytes and crc value
    auto segment = cache->GetBlock(block_handle->Offset(), block_handle->Size() + 4, false);
    
    auto slice = MemorySlice::Wrap(segment);
    auto slice_input = slice->ToInput();
    
    // Read null bitmap data
    auto null_bitmap_slice = slice_input->ReadSlice(block_handle->Size());
    auto null_bitmap_bytes = null_bitmap_slice->GetHeapMemory();
    
    // Calculate CRC32C checksum
    uint32_t crc_value = CRC32C::calculate(reinterpret_cast<const char*>(null_bitmap_bytes->data()), 
                                         null_bitmap_bytes->size());
    
    // Read expected CRC value
    int32_t expected_crc_value = slice_input->ReadInt();
    
    // Verify CRC checksum
    if (crc_value != static_cast<uint32_t>(expected_crc_value)) {
        return Status::Invalid("CRC check failure during decoding null bitmap");
    }
    
    // Deserialize null bitmap
    try {
        std::vector<uint8_t> data(null_bitmap_bytes->data(), 
                                null_bitmap_bytes->data() + null_bitmap_bytes->size());
        null_bitmap->Deserialize(data);
    } catch (const std::exception& e) {
        return Status::Invalid("Fail to deserialize null bitmap but crc check passed, "
                             "this means the ser/de algorithms not match: " + std::string(e.what()));
    }
    
    return null_bitmap;
}

}  // namespace paimon
