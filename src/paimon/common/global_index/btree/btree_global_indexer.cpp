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

#include <cstring>
#include <memory>
#include <string>

#include "arrow/c/bridge.h"
#include "paimon/common/compression/block_compression_factory.h"
#include "paimon/common/global_index/btree/btree_file_footer.h"
#include "paimon/common/global_index/btree/btree_global_index_reader.h"
#include "paimon/common/global_index/btree/btree_global_index_writer.h"
#include "paimon/common/global_index/btree/btree_index_meta.h"
#include "paimon/common/memory/memory_slice.h"
#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/common/options/memory_size.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/crc32c.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/core/options/compress_options.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/memory/bytes.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

// Helper function to get cache size from options with default value
static Result<int64_t> GetBTreeIndexCacheSize(const std::map<std::string, std::string>& options) {
    auto str_result =
        OptionsUtils::GetValueFromMap<std::string>(options, Options::BTREE_INDEX_CACHE_SIZE);
    if (!str_result.ok()) {
        return 128 * 1024 * 1024;
    }
    return MemorySize::ParseBytes(str_result.value());
}

// Helper function to get high priority pool ratio from options with default value
static Result<double> GetBTreeIndexHighPriorityPoolRatio(
    const std::map<std::string, std::string>& options) {
    return OptionsUtils::GetValueFromMap<double>(
        options, Options::BTREE_INDEX_HIGH_PRIORITY_POOL_RATIO, 0.1);
}

Result<std::shared_ptr<GlobalIndexWriter>> BTreeGlobalIndexer::CreateWriter(
    const std::string& field_name, ::ArrowSchema* arrow_schema,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::shared_ptr<MemoryPool>& pool) const {
    // Read block size from options (default: 64 KB)
    auto block_size_str_result =
        OptionsUtils::GetValueFromMap<std::string>(options_, Options::BTREE_INDEX_BLOCK_SIZE);
    int32_t block_size = 64 * 1024;  // default 64 KB
    if (block_size_str_result.ok()) {
        PAIMON_ASSIGN_OR_RAISE(int64_t parsed_size,
                               MemorySize::ParseBytes(block_size_str_result.value()));
        block_size = static_cast<int32_t>(parsed_size);
    }
    // Read compression options
    auto compress_str = OptionsUtils::GetValueFromMap<std::string>(
        options_, Options::BTREE_INDEX_COMPRESSION, "none");
    auto compress_level =
        OptionsUtils::GetValueFromMap<int32_t>(options_, Options::BTREE_INDEX_COMPRESSION_LEVEL, 1);
    CompressOptions compress_options{compress_str.value(), compress_level.value()};
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::BlockCompressionFactory> compression_factory,
                           BlockCompressionFactory::Create(compress_options));

    PAIMON_ASSIGN_OR_RAISE(auto writer,
                           BTreeGlobalIndexWriter::Create(field_name, arrow_schema, file_writer,
                                                          compression_factory, pool, block_size));
    return writer;
}

// Create a comparator function based on field type
// Keys are stored in binary format to match Java's DataOutputStream format
static std::function<int32_t(const MemorySlice&, const MemorySlice&)> CreateComparator(
    FieldType field_type, const std::shared_ptr<arrow::DataType>& arrow_type) {
    // For numeric types, compare as binary values in little-endian format
    // to match Java's DataOutputStream.writeInt/writeLong format
    switch (field_type) {
        case FieldType::INT:
        case FieldType::DATE:
            return [](const MemorySlice& a, const MemorySlice& b) -> int32_t {
                int32_t a_val = a.ReadInt(0);
                int32_t b_val = b.ReadInt(0);
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::BIGINT:
            return [](const MemorySlice& a, const MemorySlice& b) -> int32_t {
                int64_t a_val = a.ReadLong(0);
                int64_t b_val = b.ReadLong(0);
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::TIMESTAMP: {
            int32_t precision = Timestamp::MILLIS_PRECISION;
            if (arrow_type->id() == arrow::Type::TIMESTAMP) {
                auto ts_type = std::static_pointer_cast<arrow::TimestampType>(arrow_type);
                precision = DateTimeUtils::GetPrecisionFromType(ts_type);
            }
            if (Timestamp::IsCompact(precision)) {
                // compact: compare as int64 (millisecond only)
                return [](const MemorySlice& a, const MemorySlice& b) -> int32_t {
                    int64_t a_val = a.ReadLong(0);
                    int64_t b_val = b.ReadLong(0);
                    if (a_val < b_val) return -1;
                    if (a_val > b_val) return 1;
                    return 0;
                };
            } else {
                // non-compact: compare millisecond first, then nanoOfMillisecond
                return [](const MemorySlice& a, const MemorySlice& b) -> int32_t {
                    auto a_input = a.ToInput();
                    auto b_input = b.ToInput();
                    int64_t a_milli = a_input.ReadLong();
                    int64_t b_milli = b_input.ReadLong();
                    if (a_milli < b_milli) return -1;
                    if (a_milli > b_milli) return 1;
                    auto a_nano = a_input.ReadVarLenInt();
                    auto b_nano = b_input.ReadVarLenInt();
                    if (a_nano.ok() && b_nano.ok()) {
                        if (a_nano.value() < b_nano.value()) return -1;
                        if (a_nano.value() > b_nano.value()) return 1;
                    }
                    return 0;
                };
            }
        }
        case FieldType::SMALLINT:
            return [](const MemorySlice& a, const MemorySlice& b) -> int32_t {
                int16_t a_val = a.ReadShort(0);
                int16_t b_val = b.ReadShort(0);
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::TINYINT:
            return [](const MemorySlice& a, const MemorySlice& b) -> int32_t {
                int8_t a_val = a.ReadByte(0);
                int8_t b_val = b.ReadByte(0);
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::FLOAT:
            return [](const MemorySlice& a, const MemorySlice& b) -> int32_t {
                // Read float from bytes (little-endian)
                float a_val, b_val;
                std::memcpy(&a_val, a.ReadStringView().data(), sizeof(float));
                std::memcpy(&b_val, b.ReadStringView().data(), sizeof(float));
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::DOUBLE:
            return [](const MemorySlice& a, const MemorySlice& b) -> int32_t {
                // Read double from bytes (little-endian)
                double a_val, b_val;
                std::memcpy(&a_val, a.ReadStringView().data(), sizeof(double));
                std::memcpy(&b_val, b.ReadStringView().data(), sizeof(double));
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::BOOLEAN:
            return [](const MemorySlice& a, const MemorySlice& b) -> int32_t {
                if (a.Length() == 0 || b.Length() == 0) return 0;
                int8_t a_val = a.ReadByte(0);
                int8_t b_val = b.ReadByte(0);
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::STRING:
        case FieldType::BINARY:
        default:
            // For string/binary types, use lexicographic comparison
            return [](const MemorySlice& a, const MemorySlice& b) -> int32_t {
                size_t min_len =
                    std::min(static_cast<size_t>(a.Length()), static_cast<size_t>(b.Length()));
                int cmp = memcmp(a.ReadStringView().data(), b.ReadStringView().data(), min_len);
                if (cmp != 0) return cmp < 0 ? -1 : 1;
                if (a.Length() < b.Length()) return -1;
                if (a.Length() > b.Length()) return 1;
                return 0;
            };
    }
}
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

    // Get field type from arrow schema
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> schema,
                                      arrow::ImportSchema(arrow_schema));
    if (schema->num_fields() != 1) {
        return Status::Invalid(
            "invalid schema for BTreeGlobalIndexReader, supposed to have single field.");
    }
    auto arrow_type = schema->field(0)->type();
    PAIMON_ASSIGN_OR_RAISE(FieldType field_type,
                           FieldTypeUtils::ConvertToFieldType(arrow_type->id()));

    // Create comparator based on field type
    auto comparator = CreateComparator(field_type, arrow_type);

    // Wrap the comparator to return Result<int32_t>
    MemorySlice::SliceComparator result_comparator =
        [comparator](const MemorySlice& a, const MemorySlice& b) -> Result<int32_t> {
        return comparator(a, b);
    };

    // Read BTree file footer first
    PAIMON_ASSIGN_OR_RAISE(int64_t cache_size, GetBTreeIndexCacheSize(options_));
    PAIMON_ASSIGN_OR_RAISE(double high_priority_pool_ratio,
                           GetBTreeIndexHighPriorityPoolRatio(options_));
    auto cache_manager = std::make_shared<CacheManager>(cache_size, high_priority_pool_ratio);
    auto block_cache = std::make_shared<BlockCache>(meta.file_path, in, cache_manager, pool);
    PAIMON_ASSIGN_OR_RAISE(MemorySegment segment,
                           block_cache->GetBlock(meta.file_size - BTreeFileFooter::ENCODED_LENGTH,
                                                 BTreeFileFooter::ENCODED_LENGTH, true,
                                                 /*decompress_func=*/nullptr));
    auto footer_slice = MemorySlice::Wrap(segment);
    auto footer_input = footer_slice.ToInput();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BTreeFileFooter> footer,
                           BTreeFileFooter::Read(&footer_input));

    // Create SST file reader with footer information
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<SstFileReader> sst_file_reader,
        SstFileReader::Create(in, *footer->GetIndexBlockHandle(), footer->GetBloomFilterHandle(),
                              result_comparator, cache_manager, pool));

    // prepare null_bitmap
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RoaringBitmap64> null_bitmap,
                           ReadNullBitmap(block_cache, footer->GetNullBitmapHandle()));

    auto index_meta = BTreeIndexMeta::Deserialize(meta.metadata, pool.get());

    // Convert Bytes to MemorySlice for keys
    MemorySlice min_key_slice(MemorySegment(), 0, 0);
    MemorySlice max_key_slice(MemorySegment(), 0, 0);
    bool has_min_key = false;
    if (index_meta->FirstKey()) {
        min_key_slice = MemorySlice::Wrap(index_meta->FirstKey());
        has_min_key = true;
    }
    if (index_meta->LastKey()) {
        max_key_slice = MemorySlice::Wrap(index_meta->LastKey());
    }

    // Get timestamp precision if applicable
    int32_t ts_precision = Timestamp::MILLIS_PRECISION;
    if (arrow_type->id() == arrow::Type::TIMESTAMP) {
        auto ts_type = std::static_pointer_cast<arrow::TimestampType>(arrow_type);
        ts_precision = DateTimeUtils::GetPrecisionFromType(ts_type);
    }

    return std::make_shared<BTreeGlobalIndexReader>(sst_file_reader, null_bitmap, min_key_slice,
                                                    max_key_slice, has_min_key, files, pool,
                                                    comparator, ts_precision);
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

Result<std::shared_ptr<RoaringBitmap64>> BTreeGlobalIndexer::ReadNullBitmap(
    const std::shared_ptr<BlockCache>& cache, const std::shared_ptr<BlockHandle>& block_handle) {
    auto null_bitmap = std::make_shared<RoaringBitmap64>();
    if (block_handle == nullptr) {
        return null_bitmap;
    }

    // Read bytes and crc value
    PAIMON_ASSIGN_OR_RAISE(auto segment,
                           cache->GetBlock(block_handle->Offset(), block_handle->Size() + 4, false,
                                           /*decompress_func=*/nullptr));

    auto slice = MemorySlice::Wrap(segment);
    auto slice_input = slice.ToInput();

    // Read null bitmap data
    auto null_bitmap_slice = slice_input.ReadSlice(block_handle->Size());
    auto null_bitmap_view = null_bitmap_slice.ReadStringView();

    // Calculate CRC32C checksum
    uint32_t crc_value = CRC32C::calculate(null_bitmap_view.data(), null_bitmap_view.size());

    // Read expected CRC value (stored as native uint32_t)
    auto crc_slice = slice_input.ReadSlice(sizeof(uint32_t));
    uint32_t expected_crc_value;
    std::memcpy(&expected_crc_value, crc_slice.ReadStringView().data(), sizeof(expected_crc_value));

    // Verify CRC checksum
    if (crc_value != expected_crc_value) {
        return Status::Invalid("CRC check failure during decoding null bitmap. Expected: " +
                               std::to_string(expected_crc_value) +
                               ", Calculated: " + std::to_string(crc_value));
    }

    // Deserialize null bitmap
    PAIMON_RETURN_NOT_OK(
        null_bitmap->Deserialize(null_bitmap_view.data(), null_bitmap_view.size()));

    return null_bitmap;
}

}  // namespace paimon
