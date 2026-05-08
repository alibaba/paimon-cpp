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

#include "paimon/common/global_index/btree/lazy_filtered_btree_reader.h"

#include <future>
#include <utility>

#include "paimon/common/executor/future.h"
#include "paimon/common/global_index/btree/btree_file_footer.h"
#include "paimon/common/global_index/btree/btree_global_index_reader.h"
#include "paimon/common/global_index/btree/btree_index_meta.h"
#include "paimon/common/global_index/btree/key_serializer.h"
#include "paimon/common/global_index/union_global_index_reader.h"
#include "paimon/common/memory/memory_slice.h"
#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/common/sst/block_cache.h"
#include "paimon/common/sst/sst_file_reader.h"
#include "paimon/common/utils/crc32c.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/io/buffered_input_stream.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {
LazyFilteredBTreeReader::LazyFilteredBTreeReader(
    std::optional<int32_t> read_buffer_size, const std::vector<GlobalIndexIOMeta>& files,
    const std::shared_ptr<arrow::DataType>& key_type,
    const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::shared_ptr<CacheManager>& cache_manager, const std::shared_ptr<MemoryPool>& pool,
    const std::shared_ptr<Executor>& executor)
    : read_buffer_size_(read_buffer_size),
      pool_(pool),
      file_selector_(files, key_type, pool),
      key_type_(key_type),
      file_reader_(file_reader),
      cache_manager_(cache_manager),
      executor_(executor) {}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitIsNotNull() {
    return DispatchVisit(
        [this]() { return file_selector_.VisitIsNotNull(); },
        [](const std::shared_ptr<GlobalIndexReader>& reader) { return reader->VisitIsNotNull(); });
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitIsNull() {
    return DispatchVisit(
        [this]() { return file_selector_.VisitIsNull(); },
        [](const std::shared_ptr<GlobalIndexReader>& reader) { return reader->VisitIsNull(); });
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitEqual(
    const Literal& literal) {
    return DispatchVisit([this, &literal]() { return file_selector_.VisitEqual(literal); },
                         [&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
                             return reader->VisitEqual(literal);
                         });
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitNotEqual(
    const Literal& literal) {
    return DispatchVisit([this, &literal]() { return file_selector_.VisitNotEqual(literal); },
                         [&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
                             return reader->VisitNotEqual(literal);
                         });
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitLessThan(
    const Literal& literal) {
    return DispatchVisit([this, &literal]() { return file_selector_.VisitLessThan(literal); },
                         [&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
                             return reader->VisitLessThan(literal);
                         });
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitLessOrEqual(
    const Literal& literal) {
    return DispatchVisit([this, &literal]() { return file_selector_.VisitLessOrEqual(literal); },
                         [&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
                             return reader->VisitLessOrEqual(literal);
                         });
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitGreaterThan(
    const Literal& literal) {
    return DispatchVisit([this, &literal]() { return file_selector_.VisitGreaterThan(literal); },
                         [&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
                             return reader->VisitGreaterThan(literal);
                         });
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitGreaterOrEqual(
    const Literal& literal) {
    return DispatchVisit([this, &literal]() { return file_selector_.VisitGreaterOrEqual(literal); },
                         [&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
                             return reader->VisitGreaterOrEqual(literal);
                         });
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitIn(
    const std::vector<Literal>& literals) {
    return DispatchVisit([this, &literals]() { return file_selector_.VisitIn(literals); },
                         [&literals](const std::shared_ptr<GlobalIndexReader>& reader) {
                             return reader->VisitIn(literals);
                         });
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    return DispatchVisit([this, &literals]() { return file_selector_.VisitNotIn(literals); },
                         [&literals](const std::shared_ptr<GlobalIndexReader>& reader) {
                             return reader->VisitNotIn(literals);
                         });
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitStartsWith(
    const Literal& prefix) {
    return DispatchVisit([this, &prefix]() { return file_selector_.VisitStartsWith(prefix); },
                         [&prefix](const std::shared_ptr<GlobalIndexReader>& reader) {
                             return reader->VisitStartsWith(prefix);
                         });
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitEndsWith(
    const Literal& suffix) {
    return DispatchVisit([this, &suffix]() { return file_selector_.VisitEndsWith(suffix); },
                         [&suffix](const std::shared_ptr<GlobalIndexReader>& reader) {
                             return reader->VisitEndsWith(suffix);
                         });
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitContains(
    const Literal& literal) {
    return DispatchVisit([this, &literal]() { return file_selector_.VisitContains(literal); },
                         [&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
                             return reader->VisitContains(literal);
                         });
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitLike(
    const Literal& literal) {
    return DispatchVisit([this, &literal]() { return file_selector_.VisitLike(literal); },
                         [&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
                             return reader->VisitLike(literal);
                         });
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> LazyFilteredBTreeReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search) {
    return Status::Invalid("LazyFilteredBTreeReader does not support vector search");
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>& full_text_search) {
    return Status::Invalid("LazyFilteredBTreeReader does not support full text search");
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::DispatchVisit(
    SelectAction select_files, ReaderAction action) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<GlobalIndexIOMeta> selected_files, select_files());
    if (selected_files.empty()) {
        return std::make_shared<BitmapGlobalIndexResult>([]() { return RoaringBitmap64(); });
    }

    // Create a UnionGlobalIndexReader from cached readers for the selected files
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexReader> union_reader,
                           CreateUnionReader(selected_files));

    // Delegate the action to the union reader
    return action(union_reader);
}

Result<std::shared_ptr<GlobalIndexReader>> LazyFilteredBTreeReader::CreateUnionReader(
    const std::vector<GlobalIndexIOMeta>& files) {
    std::vector<std::shared_ptr<GlobalIndexReader>> readers;
    readers.reserve(files.size());
    for (const auto& meta : files) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexReader> reader, GetOrCreateReader(meta));
        readers.push_back(std::move(reader));
    }

    return std::make_shared<UnionGlobalIndexReader>(std::move(readers), executor_);
}

Result<std::shared_ptr<GlobalIndexReader>> LazyFilteredBTreeReader::GetOrCreateReader(
    const GlobalIndexIOMeta& meta) {
    auto iterator = reader_cache_.find(meta.file_path);
    if (iterator != reader_cache_.end()) {
        return iterator->second;
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexReader> reader, CreateSingleReader(meta));
    reader_cache_[meta.file_path] = reader;
    return reader;
}

Result<std::shared_ptr<GlobalIndexReader>> LazyFilteredBTreeReader::CreateSingleReader(
    const GlobalIndexIOMeta& meta) {
    // Create comparator based on field type
    auto comparator = KeySerializer::CreateComparator(key_type_, pool_);

    // Get min/max key slices from meta data (keep as slices; Create() will deserialize)
    auto index_meta = BTreeIndexMeta::Deserialize(meta.metadata, pool_.get());
    std::optional<MemorySlice> min_key_slice;
    std::optional<MemorySlice> max_key_slice;
    if (index_meta->FirstKey()) {
        min_key_slice = MemorySlice::Wrap(index_meta->FirstKey());
    }
    if (index_meta->LastKey()) {
        max_key_slice = MemorySlice::Wrap(index_meta->LastKey());
    }

    // Open input stream and create block cache
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input_stream,
                           file_reader_->GetInputStream(meta.file_path));
    if (read_buffer_size_) {
        input_stream = std::make_shared<BufferedInputStream>(
            input_stream, read_buffer_size_.value(), pool_.get());
    }

    auto block_cache =
        std::make_shared<BlockCache>(meta.file_path, input_stream, cache_manager_, pool_);

    // Read footer
    PAIMON_ASSIGN_OR_RAISE(MemorySegment footer_segment,
                           block_cache->GetBlock(meta.file_size - BTreeFileFooter::kEncodingLength,
                                                 BTreeFileFooter::kEncodingLength, true,
                                                 /*decompress_func=*/nullptr));
    auto footer_slice = MemorySlice::Wrap(footer_segment);
    auto footer_input = footer_slice.ToInput();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BTreeFileFooter> footer,
                           BTreeFileFooter::Read(&footer_input));

    // Read null bitmap
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 null_bitmap,
                           ReadNullBitmap(block_cache, footer->GetNullBitmapHandle()));

    // Create SST file reader
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<SstFileReader> sst_file_reader,
        SstFileReader::Create(footer->GetIndexBlockHandle(), footer->GetBloomFilterHandle(),
                              comparator, block_cache, pool_));

    return BTreeGlobalIndexReader::Create(sst_file_reader, std::move(null_bitmap), min_key_slice,
                                          max_key_slice, key_type_, pool_);
}

Result<RoaringBitmap64> LazyFilteredBTreeReader::ReadNullBitmap(
    const std::shared_ptr<BlockCache>& cache, const std::optional<BlockHandle>& block_handle) {
    RoaringBitmap64 null_bitmap;
    if (!block_handle.has_value()) {
        return null_bitmap;
    }

    // Read bytes and CRC value
    PAIMON_ASSIGN_OR_RAISE(
        MemorySegment segment,
        cache->GetBlock(block_handle->Offset(), block_handle->Size() + 4, /*is_index=*/false,
                        /*decompress_func=*/nullptr));

    auto slice = MemorySlice::Wrap(segment);
    auto slice_input = slice.ToInput();

    // Read null bitmap data
    auto null_bitmap_bytes = slice_input.ReadSliceView(block_handle->Size()).CopyBytes(pool_.get());

    // Calculate and verify CRC32C checksum
    uint32_t calculated_crc =
        CRC32C::calculate(null_bitmap_bytes->data(), null_bitmap_bytes->size());
    int32_t expected_crc = slice_input.ReadInt();
    if (calculated_crc != static_cast<uint32_t>(expected_crc)) {
        return Status::Invalid(fmt::format(
            "CRC check failure during decoding null bitmap. Expected: {}, Calculated: {}",
            expected_crc, calculated_crc));
    }

    // Deserialize null bitmap
    PAIMON_RETURN_NOT_OK(
        null_bitmap.Deserialize(null_bitmap_bytes->data(), null_bitmap_bytes->size()));
    return null_bitmap;
}

}  // namespace paimon
