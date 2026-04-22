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
#include "paimon/common/global_index/btree/btree_global_indexer.h"

#include <memory>
#include <string>

#include "arrow/c/bridge.h"
#include "paimon/common/compression/block_compression_factory.h"
#include "paimon/common/global_index/btree/btree_file_footer.h"
#include "paimon/common/global_index/btree/btree_global_index_reader.h"
#include "paimon/common/global_index/btree/btree_global_index_writer.h"
#include "paimon/common/global_index/btree/btree_index_meta.h"
#include "paimon/common/global_index/btree/key_serializer.h"
#include "paimon/common/memory/memory_slice.h"
#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/common/options/memory_size.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/crc32c.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/preconditions.h"
#include "paimon/core/options/compress_options.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/memory/bytes.h"
#include "paimon/utils/roaring_bitmap64.h"
namespace paimon {
Result<std::shared_ptr<GlobalIndexWriter>> BTreeGlobalIndexer::CreateWriter(
    const std::string& field_name, ::ArrowSchema* arrow_schema,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::DataType> arrow_type,
                                      arrow::ImportType(arrow_schema));
    // check data type
    auto struct_type = std::dynamic_pointer_cast<arrow::StructType>(arrow_type);
    PAIMON_RETURN_NOT_OK(Preconditions::CheckNotNull(
        struct_type, "arrow schema must be struct type when create BTreeGlobalIndexWriter"));

    // parse options
    PAIMON_ASSIGN_OR_RAISE(
        std::string block_size_str,
        OptionsUtils::GetValueFromMap<std::string>(options_, BtreeDefs::kBtreeIndexBlockSize,
                                                   BtreeDefs::kDefaultBtreeIndexBlockSize));
    PAIMON_ASSIGN_OR_RAISE(int32_t block_size, MemorySize::ParseBytes(block_size_str));
    PAIMON_ASSIGN_OR_RAISE(
        std::string compress_str,
        OptionsUtils::GetValueFromMap<std::string>(options_, BtreeDefs::kBtreeIndexCompression,
                                                   BtreeDefs::kDefaultBtreeIndexCompression));
    PAIMON_ASSIGN_OR_RAISE(
        int32_t compress_level,
        OptionsUtils::GetValueFromMap<int32_t>(options_, BtreeDefs::kBtreeIndexCompressionLevel,
                                               BtreeDefs::kDefaultBtreeIndexCompressionLevel));
    CompressOptions compress_options{compress_str, compress_level};
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::BlockCompressionFactory> compression_factory,
                           BlockCompressionFactory::Create(compress_options));
    return BTreeGlobalIndexWriter::Create(field_name, struct_type, file_writer, block_size,
                                          compression_factory, pool);
}

Result<std::shared_ptr<GlobalIndexReader>> BTreeGlobalIndexer::CreateReader(
    ::ArrowSchema* arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::vector<GlobalIndexIOMeta>& files, const std::shared_ptr<MemoryPool>& pool) const {
    // Get field type from arrow schema
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> schema,
                                      arrow::ImportSchema(arrow_schema));
    if (files.size() != 1) {
        return Status::Invalid(
            "invalid GlobalIndexIOMeta for BTreeGlobalIndex, exist multiple metas");
    }
    const auto& meta = files[0];
    if (schema->num_fields() != 1) {
        return Status::Invalid(
            "invalid schema for BTreeGlobalIndexReader, supposed to have single field.");
    }
    auto key_type = schema->field(0)->type();
    // Create comparator based on field type
    auto comparator = KeySerializer::CreateComparator(key_type, pool);
    // get min, max key from meta data
    auto index_meta = BTreeIndexMeta::Deserialize(meta.metadata, pool.get());
    std::optional<Literal> min_key;
    std::optional<Literal> max_key;
    if (index_meta->FirstKey()) {
        PAIMON_ASSIGN_OR_RAISE(
            min_key, KeySerializer::DeserializeKey(MemorySlice::Wrap(index_meta->FirstKey()),
                                                   key_type, pool.get()));
    }
    if (index_meta->LastKey()) {
        PAIMON_ASSIGN_OR_RAISE(
            max_key, KeySerializer::DeserializeKey(MemorySlice::Wrap(index_meta->LastKey()),
                                                   key_type, pool.get()));
    }

    // parse read options
    PAIMON_ASSIGN_OR_RAISE(
        std::string cache_size_str,
        OptionsUtils::GetValueFromMap<std::string>(options_, BtreeDefs::kBtreeIndexCacheSize,
                                                   BtreeDefs::kDefaultBtreeIndexCacheSize));
    PAIMON_ASSIGN_OR_RAISE(int64_t cache_size, MemorySize::ParseBytes(cache_size_str));

    PAIMON_ASSIGN_OR_RAISE(
        double high_priority_pool_ratio,
        OptionsUtils::GetValueFromMap<double>(options_, BtreeDefs::kBtreeIndexHighPriorityPoolRatio,
                                              BtreeDefs::kDefaultBtreeIndexHighPriorityPoolRatio));
    // TODO(xinyu.lxy): pass cache_manager from param.
    auto cache_manager = std::make_shared<CacheManager>(cache_size, high_priority_pool_ratio);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> in,
                           file_reader->GetInputStream(meta.file_path));
    auto block_cache = std::make_shared<BlockCache>(meta.file_path, in, cache_manager, pool);
    // read footer
    PAIMON_ASSIGN_OR_RAISE(MemorySegment segment,
                           block_cache->GetBlock(meta.file_size - BTreeFileFooter::kEncodingLength,
                                                 BTreeFileFooter::kEncodingLength, true,
                                                 /*decompress_func=*/nullptr));
    auto footer_slice = MemorySlice::Wrap(segment);
    auto footer_input = footer_slice.ToInput();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BTreeFileFooter> footer,
                           BTreeFileFooter::Read(&footer_input));
    // prepare null_bitmap
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 null_bitmap,
                           ReadNullBitmap(block_cache, footer->GetNullBitmapHandle(), pool.get()));

    // Close the temporary block_cache to remove its entries from the shared LRU cache.
    // This prevents use-after-free: the eviction callback captures `this` (the BlockCache),
    // and if the BlockCache is destroyed without closing, a later eviction would invoke
    // the callback on a dangling pointer.
    block_cache->Close();

    // create SST file reader with footer information
    // TODO(xinyu.lxy): pass block cache to SstFileReader rather than cache_manager
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<SstFileReader> sst_file_reader,
        SstFileReader::Create(in, footer->GetIndexBlockHandle(), footer->GetBloomFilterHandle(),
                              comparator, cache_manager, pool));

    return std::make_shared<BTreeGlobalIndexReader>(sst_file_reader, std::move(null_bitmap),
                                                    min_key, max_key, key_type, pool);
}

Result<RoaringBitmap64> BTreeGlobalIndexer::ReadNullBitmap(
    const std::shared_ptr<BlockCache>& cache, const std::optional<BlockHandle>& block_handle,
    MemoryPool* pool) {
    RoaringBitmap64 null_bitmap;
    if (!block_handle.has_value()) {
        return null_bitmap;
    }

    // read bytes and crc value
    PAIMON_ASSIGN_OR_RAISE(
        MemorySegment segment,
        cache->GetBlock(block_handle->Offset(), block_handle->Size() + 4, /*is_index=*/false,
                        /*decompress_func=*/nullptr));

    auto slice = MemorySlice::Wrap(segment);
    auto slice_input = slice.ToInput();

    // read null bitmap data
    auto null_bitmap_bytes = slice_input.ReadSlice(block_handle->Size()).CopyBytes(pool);
    // Calculate CRC32C checksum
    uint32_t crc_value = CRC32C::calculate(null_bitmap_bytes->data(), null_bitmap_bytes->size());
    int32_t expected_crc_value = slice_input.ReadInt();

    // Verify CRC checksum
    if (crc_value != static_cast<uint32_t>(expected_crc_value)) {
        return Status::Invalid(fmt::format(
            "CRC check failure during decoding null bitmap. Expected: {}, Calculated: {}",
            expected_crc_value, crc_value));
    }

    // deserialize null bitmap
    PAIMON_RETURN_NOT_OK(
        null_bitmap.Deserialize(null_bitmap_bytes->data(), null_bitmap_bytes->size()));
    return null_bitmap;
}

}  // namespace paimon
