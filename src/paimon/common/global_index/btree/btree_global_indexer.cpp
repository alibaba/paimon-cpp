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
#include "paimon/common/global_index/btree/btree_file_footer.h"
#include "paimon/common/global_index/btree/btree_global_index_writer.h"
#include "paimon/common/global_index/btree/btree_index_meta.h"
#include "paimon/common/memory/memory_slice.h"
#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/crc32c.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/common/utils/roaring_navigable_map64.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/memory/bytes.h"
#include "paimon/predicate/literal.h"

namespace paimon {

Result<std::shared_ptr<GlobalIndexWriter>> BTreeGlobalIndexer::CreateWriter(
    const std::string& field_name, ::ArrowSchema* arrow_schema,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::shared_ptr<MemoryPool>& pool) const {
    return std::make_shared<BTreeGlobalIndexWriter>(field_name, arrow_schema, file_writer, pool);
}

// Forward declarations for helper functions
static Result<MemorySlice> LiteralToMemorySlice(const Literal& literal, MemoryPool* pool);

// Create a comparator function based on field type
// Keys are stored in binary format to match Java's DataOutputStream format
static std::function<int32_t(const MemorySlice&, const MemorySlice&)> CreateComparator(
    FieldType field_type) {
    // For numeric types, compare as binary values in little-endian format
    // to match Java's DataOutputStream.writeInt/writeLong format
    switch (field_type) {
        case FieldType::INT:
            return [](const MemorySlice& a, const MemorySlice& b) -> int32_t {
                if (a.Length() < static_cast<int32_t>(sizeof(int32_t)) ||
                    b.Length() < static_cast<int32_t>(sizeof(int32_t))) {
                    size_t min_len =
                        std::min(static_cast<size_t>(a.Length()), static_cast<size_t>(b.Length()));
                    int cmp = memcmp(a.ReadStringView().data(), b.ReadStringView().data(), min_len);
                    if (cmp != 0) return cmp < 0 ? -1 : 1;
                    if (a.Length() < b.Length()) return -1;
                    if (a.Length() > b.Length()) return 1;
                    return 0;
                }
                int32_t a_val = a.ReadInt(0);
                int32_t b_val = b.ReadInt(0);
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::BIGINT:
        case FieldType::DATE:
        case FieldType::TIMESTAMP:
            return [](const MemorySlice& a, const MemorySlice& b) -> int32_t {
                if (a.Length() < static_cast<int32_t>(sizeof(int64_t)) ||
                    b.Length() < static_cast<int32_t>(sizeof(int64_t))) {
                    size_t min_len =
                        std::min(static_cast<size_t>(a.Length()), static_cast<size_t>(b.Length()));
                    int cmp = memcmp(a.ReadStringView().data(), b.ReadStringView().data(), min_len);
                    if (cmp != 0) return cmp < 0 ? -1 : 1;
                    if (a.Length() < b.Length()) return -1;
                    if (a.Length() > b.Length()) return 1;
                    return 0;
                }
                int64_t a_val = a.ReadLong(0);
                int64_t b_val = b.ReadLong(0);
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::SMALLINT:
            return [](const MemorySlice& a, const MemorySlice& b) -> int32_t {
                if (a.Length() < static_cast<int32_t>(sizeof(int16_t)) ||
                    b.Length() < static_cast<int32_t>(sizeof(int16_t))) {
                    size_t min_len =
                        std::min(static_cast<size_t>(a.Length()), static_cast<size_t>(b.Length()));
                    int cmp = memcmp(a.ReadStringView().data(), b.ReadStringView().data(), min_len);
                    if (cmp != 0) return cmp < 0 ? -1 : 1;
                    if (a.Length() < b.Length()) return -1;
                    if (a.Length() > b.Length()) return 1;
                    return 0;
                }
                int16_t a_val = a.ReadShort(0);
                int16_t b_val = b.ReadShort(0);
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::TINYINT:
            return [](const MemorySlice& a, const MemorySlice& b) -> int32_t {
                if (a.Length() < 1 || b.Length() < 1) {
                    size_t min_len =
                        std::min(static_cast<size_t>(a.Length()), static_cast<size_t>(b.Length()));
                    int cmp = memcmp(a.ReadStringView().data(), b.ReadStringView().data(), min_len);
                    if (cmp != 0) return cmp < 0 ? -1 : 1;
                    if (a.Length() < b.Length()) return -1;
                    if (a.Length() > b.Length()) return 1;
                    return 0;
                }
                int8_t a_val = a.ReadByte(0);
                int8_t b_val = b.ReadByte(0);
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::FLOAT:
            return [](const MemorySlice& a, const MemorySlice& b) -> int32_t {
                if (a.Length() < static_cast<int32_t>(sizeof(float)) ||
                    b.Length() < static_cast<int32_t>(sizeof(float))) {
                    size_t min_len =
                        std::min(static_cast<size_t>(a.Length()), static_cast<size_t>(b.Length()));
                    int cmp = memcmp(a.ReadStringView().data(), b.ReadStringView().data(), min_len);
                    if (cmp != 0) return cmp < 0 ? -1 : 1;
                    if (a.Length() < b.Length()) return -1;
                    if (a.Length() > b.Length()) return 1;
                    return 0;
                }
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
                if (a.Length() < static_cast<int32_t>(sizeof(double)) ||
                    b.Length() < static_cast<int32_t>(sizeof(double))) {
                    size_t min_len =
                        std::min(static_cast<size_t>(a.Length()), static_cast<size_t>(b.Length()));
                    int cmp = memcmp(a.ReadStringView().data(), b.ReadStringView().data(), min_len);
                    if (cmp != 0) return cmp < 0 ? -1 : 1;
                    if (a.Length() < b.Length()) return -1;
                    if (a.Length() > b.Length()) return 1;
                    return 0;
                }
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
    auto comparator = CreateComparator(field_type);

    // Wrap the comparator to return Result<int32_t>
    MemorySlice::SliceComparator result_comparator =
        [comparator](const MemorySlice& a, const MemorySlice& b) -> Result<int32_t> {
        return comparator(a, b);
    };

    // Read BTree file footer first
    auto cache_manager = std::make_shared<CacheManager>(1024 * 1024, 0.0);
    auto block_cache = std::make_shared<BlockCache>(meta.file_path, in, cache_manager, pool);
    PAIMON_ASSIGN_OR_RAISE(MemorySegment segment,
                           block_cache->GetBlock(meta.file_size - BTreeFileFooter::ENCODED_LENGTH,
                                                 BTreeFileFooter::ENCODED_LENGTH, true,
                                                 /*decompress_func=*/nullptr));
    auto footer_slice = MemorySlice::Wrap(segment);
    auto footer_input = footer_slice.ToInput();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BTreeFileFooter> footer,
                           BTreeFileFooter::Read(footer_input));

    // Create SST file reader with footer information
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<SstFileReader> sst_file_reader,
        SstFileReader::Create(pool, in, *footer->GetIndexBlockHandle(),
                              footer->GetBloomFilterHandle(), result_comparator, cache_manager));

    // prepare null_bitmap
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RoaringNavigableMap64> null_bitmap,
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

    return std::make_shared<BTreeGlobalIndexReader>(sst_file_reader, null_bitmap, min_key_slice,
                                                    max_key_slice, has_min_key, files, pool,
                                                    comparator);
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

    // Read expected CRC value (stored as uint32_t in little-endian)
    uint32_t expected_crc_value = 0;
    for (int i = 0; i < 4; ++i) {
        expected_crc_value |= static_cast<uint32_t>(static_cast<uint8_t>(slice_input.ReadByte()))
                              << (i * 8);
    }

    // Verify CRC checksum
    if (crc_value != expected_crc_value) {
        return Status::Invalid("CRC check failure during decoding null bitmap. Expected: " +
                               std::to_string(expected_crc_value) +
                               ", Calculated: " + std::to_string(crc_value));
    }

    // Deserialize null bitmap
    try {
        std::vector<uint8_t> data(
            reinterpret_cast<const uint8_t*>(null_bitmap_view.data()),
            reinterpret_cast<const uint8_t*>(null_bitmap_view.data()) + null_bitmap_view.size());
        null_bitmap->Deserialize(data);
    } catch (const std::exception& e) {
        return Status::Invalid(
            "Fail to deserialize null bitmap but crc check passed, "
            "this means the serialization/deserialization algorithms not match: " +
            std::string(e.what()));
    }

    return null_bitmap;
}

BTreeGlobalIndexReader::BTreeGlobalIndexReader(
    const std::shared_ptr<SstFileReader>& sst_file_reader,
    const std::shared_ptr<RoaringNavigableMap64>& null_bitmap, const MemorySlice& min_key,
    const MemorySlice& max_key, bool has_min_key, const std::vector<GlobalIndexIOMeta>& files,
    const std::shared_ptr<MemoryPool>& pool,
    std::function<int32_t(const MemorySlice&, const MemorySlice&)> comparator)
    : sst_file_reader_(sst_file_reader),
      null_bitmap_(null_bitmap),
      min_key_(min_key),
      max_key_(max_key),
      has_min_key_(has_min_key),
      files_(files),
      pool_(pool),
      comparator_(std::move(comparator)) {}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitIsNotNull() {
    return std::make_shared<BitmapGlobalIndexResult>([this]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result, AllNonNullRows());
        return result.GetBitmap();
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitIsNull() {
    return std::make_shared<BitmapGlobalIndexResult>(
        [this]() -> Result<RoaringBitmap64> { return null_bitmap_->GetBitmap(); });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitStartsWith(
    const Literal& prefix) {
    return std::make_shared<BitmapGlobalIndexResult>([this, &prefix]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(auto prefix_slice, LiteralToMemorySlice(prefix, pool_.get()));

        auto prefix_type = prefix.GetType();

        if (prefix_type == FieldType::STRING || prefix_type == FieldType::BINARY) {
            auto prefix_bytes = prefix_slice.GetHeapMemory();
            if (!prefix_bytes || prefix_bytes->size() == 0) {
                PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result, AllNonNullRows());
                return result.GetBitmap();
            }

            std::string upper_bound_str(prefix_bytes->data(), prefix_bytes->size());
            bool overflow = true;
            for (int i = static_cast<int>(upper_bound_str.size()) - 1; i >= 0 && overflow; --i) {
                unsigned char c = static_cast<unsigned char>(upper_bound_str[i]);
                if (c < 0xFF) {
                    upper_bound_str[i] = c + 1;
                    overflow = false;
                } else {
                    upper_bound_str[i] = 0x00;
                }
            }

            if (!overflow) {
                auto upper_bytes = Bytes::AllocateBytes(upper_bound_str, pool_.get());
                auto upper_bound_slice =
                    MemorySlice::Wrap(std::shared_ptr<Bytes>(upper_bytes.release()));
                PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result,
                                       RangeQuery(prefix_slice, upper_bound_slice, true, false));
                return result.GetBitmap();
            } else {
                // If overflow (all bytes were 0xFF), use max_key_ as upper bound
                PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result,
                                       RangeQuery(prefix_slice, max_key_, true, false));
                return result.GetBitmap();
            }
        }

        return RoaringBitmap64();
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitEndsWith(
    const Literal& suffix) {
    return std::make_shared<BitmapGlobalIndexResult>([this]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result, AllNonNullRows());
        return result.GetBitmap();
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitContains(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>([this]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result, AllNonNullRows());
        return result.GetBitmap();
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitLike(
    const Literal& literal) {
    if (literal.IsNull()) {
        return Status::Invalid("LIKE pattern cannot be null");
    }

    std::string pattern = literal.GetValue<std::string>();

    bool is_prefix_pattern = false;
    std::string prefix;

    size_t first_wildcard = pattern.find_first_of("_%");

    if (first_wildcard != std::string::npos) {
        if (pattern[first_wildcard] == '%' && first_wildcard == pattern.length() - 1) {
            bool has_wildcard_in_prefix = false;
            for (size_t i = 0; i < first_wildcard; ++i) {
                if (pattern[i] == '_' || pattern[i] == '%') {
                    has_wildcard_in_prefix = true;
                    break;
                }
            }
            if (!has_wildcard_in_prefix) {
                is_prefix_pattern = true;
                prefix = pattern.substr(0, first_wildcard);
            }
        }
    }

    if (is_prefix_pattern) {
        Literal prefix_literal(FieldType::STRING, prefix.c_str(), prefix.length());
        return VisitStartsWith(prefix_literal);
    }

    return std::make_shared<BitmapGlobalIndexResult>([this]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result, AllNonNullRows());
        return result.GetBitmap();
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitLessThan(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>([this, &literal]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(auto literal_slice, LiteralToMemorySlice(literal, pool_.get()));
        PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result,
                               RangeQuery(min_key_, literal_slice, true, false));
        return result.GetBitmap();
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitGreaterOrEqual(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>([this, &literal]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(auto literal_slice, LiteralToMemorySlice(literal, pool_.get()));
        PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result,
                               RangeQuery(literal_slice, max_key_, true, true));
        return result.GetBitmap();
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitNotEqual(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>([this, &literal]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result, AllNonNullRows());
        PAIMON_ASSIGN_OR_RAISE(auto literal_slice, LiteralToMemorySlice(literal, pool_.get()));
        PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 equal_result,
                               RangeQuery(literal_slice, literal_slice, true, true));
        result.AndNot(equal_result);
        return result.GetBitmap();
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitLessOrEqual(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>([this, &literal]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(auto literal_slice, LiteralToMemorySlice(literal, pool_.get()));
        PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result,
                               RangeQuery(min_key_, literal_slice, true, true));
        return result.GetBitmap();
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitEqual(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>([this, &literal]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(auto literal_slice, LiteralToMemorySlice(literal, pool_.get()));
        PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result,
                               RangeQuery(literal_slice, literal_slice, true, true));
        return result.GetBitmap();
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitGreaterThan(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>([this, &literal]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(auto literal_slice, LiteralToMemorySlice(literal, pool_.get()));
        PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result,
                               RangeQuery(literal_slice, max_key_, false, true));
        return result.GetBitmap();
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitIn(
    const std::vector<Literal>& literals) {
    return std::make_shared<BitmapGlobalIndexResult>([this,
                                                      &literals]() -> Result<RoaringBitmap64> {
        RoaringNavigableMap64 result;
        for (const auto& literal : literals) {
            PAIMON_ASSIGN_OR_RAISE(auto literal_slice, LiteralToMemorySlice(literal, pool_.get()));
            PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 literal_result,
                                   RangeQuery(literal_slice, literal_slice, true, true));
            result.Or(literal_result);
        }
        return result.GetBitmap();
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    return std::make_shared<BitmapGlobalIndexResult>(
        [this, &literals]() -> Result<RoaringBitmap64> {
            PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result, AllNonNullRows());

            PAIMON_ASSIGN_OR_RAISE(auto in_result_ptr, VisitIn(literals));
            PAIMON_ASSIGN_OR_RAISE(auto in_iterator, in_result_ptr->CreateIterator());

            RoaringNavigableMap64 in_navigable;
            while (in_iterator->HasNext()) {
                in_navigable.Add(in_iterator->Next());
            }

            result.AndNot(in_navigable);
            return result.GetBitmap();
        });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitBetween(const Literal& from,
                                                                                const Literal& to) {
    return std::make_shared<BitmapGlobalIndexResult>(
        [this, &from, &to]() -> Result<RoaringBitmap64> {
            PAIMON_ASSIGN_OR_RAISE(auto from_slice, LiteralToMemorySlice(from, pool_.get()));
            PAIMON_ASSIGN_OR_RAISE(auto to_slice, LiteralToMemorySlice(to, pool_.get()));
            PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result,
                                   RangeQuery(from_slice, to_slice, true, true));
            return result.GetBitmap();
        });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitNotBetween(
    const Literal& from, const Literal& to) {
    return std::make_shared<BitmapGlobalIndexResult>(
        [this, &from, &to]() -> Result<RoaringBitmap64> {
            PAIMON_ASSIGN_OR_RAISE(auto from_slice, LiteralToMemorySlice(from, pool_.get()));
            PAIMON_ASSIGN_OR_RAISE(auto to_slice, LiteralToMemorySlice(to, pool_.get()));
            PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 lower_result,
                                   RangeQuery(min_key_, from_slice, true, false));
            PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 upper_result,
                                   RangeQuery(to_slice, max_key_, false, true));
            lower_result.Or(upper_result);
            return lower_result.GetBitmap();
        });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitAnd(
    const std::vector<Result<std::shared_ptr<GlobalIndexResult>>>& children) {
    return std::make_shared<BitmapGlobalIndexResult>([&children]() -> Result<RoaringBitmap64> {
        if (children.empty()) {
            return Status::Invalid("VisitAnd called with no children");
        }

        auto first_result_status = children[0];
        if (!first_result_status.ok()) {
            return first_result_status.status();
        }
        auto first_result = std::move(first_result_status).value();
        PAIMON_ASSIGN_OR_RAISE(auto first_iterator, first_result->CreateIterator());

        RoaringNavigableMap64 result_bitmap;
        while (first_iterator->HasNext()) {
            result_bitmap.Add(first_iterator->Next());
        }

        for (size_t i = 1; i < children.size(); ++i) {
            auto child_status = children[i];
            if (!child_status.ok()) {
                return child_status.status();
            }
            auto child = std::move(child_status).value();
            PAIMON_ASSIGN_OR_RAISE(auto child_iterator, child->CreateIterator());

            RoaringNavigableMap64 child_bitmap;
            while (child_iterator->HasNext()) {
                child_bitmap.Add(child_iterator->Next());
            }

            result_bitmap.And(child_bitmap);
        }

        return result_bitmap.GetBitmap();
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitOr(
    const std::vector<Result<std::shared_ptr<GlobalIndexResult>>>& children) {
    return std::make_shared<BitmapGlobalIndexResult>([&children]() -> Result<RoaringBitmap64> {
        RoaringNavigableMap64 result_bitmap;

        for (const auto& child_status : children) {
            if (!child_status.ok()) {
                return child_status.status();
            }
            auto child = std::move(child_status).value();
            PAIMON_ASSIGN_OR_RAISE(auto child_iterator, child->CreateIterator());

            while (child_iterator->HasNext()) {
                result_bitmap.Add(child_iterator->Next());
            }
        }

        return result_bitmap.GetBitmap();
    });
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> BTreeGlobalIndexReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search) {
    return Status::NotImplemented("Vector search not supported in BTree index");
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>& full_text_search) {
    return Status::NotImplemented("Full text search not supported in BTree index");
}

Result<RoaringNavigableMap64> BTreeGlobalIndexReader::RangeQuery(const MemorySlice& lower_bound,
                                                                 const MemorySlice& upper_bound,
                                                                 bool lower_inclusive,
                                                                 bool upper_inclusive) {
    RoaringNavigableMap64 result;

    // Create an index block iterator to iterate through data blocks
    auto index_iterator = sst_file_reader_->CreateIndexIterator();

    // Seek iterator to the lower bound
    auto lower_bytes = lower_bound.GetHeapMemory();

    if (lower_bytes) {
        PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] bool seek_result,
                               index_iterator->SeekTo(lower_bound));
    }

    // Check if there are any blocks to read
    if (!index_iterator->HasNext()) {
        return result;
    }

    bool first_block = true;

    while (index_iterator->HasNext()) {
        // Get the next data block
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BlockIterator> data_iterator,
                               sst_file_reader_->GetNextBlock(index_iterator));

        if (!data_iterator || !data_iterator->HasNext()) {
            break;
        }

        // For the first block, we need to seek within the block to the exact position
        if (first_block && lower_bytes) {
            PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] bool found, data_iterator->SeekTo(lower_bound));
            first_block = false;

            if (!data_iterator->HasNext()) {
                continue;
            }
        }

        // Iterate through entries in the data block
        while (data_iterator->HasNext()) {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BlockEntry> entry, data_iterator->Next());

            // Compare key with bounds using the comparator
            const auto& comparator = comparator_;
            int cmp_lower = comparator ? comparator(entry->key, lower_bound) : 0;

            // Check lower bound
            if (!lower_inclusive && cmp_lower == 0) {
                continue;
            }

            // Check upper bound
            int cmp_upper = comparator ? comparator(entry->key, upper_bound) : 0;

            if (cmp_upper > 0 || (!upper_inclusive && cmp_upper == 0)) {
                return result;
            }

            // Deserialize row IDs from the value
            auto value_bytes = entry->value.CopyBytes(pool_.get());
            auto value_slice = MemorySlice::Wrap(value_bytes);
            auto value_input = value_slice.ToInput();

            // Read row IDs. The format is: [num_row_ids (VarLenLong)][row_id1 (VarLenLong)]...
            // Use VarLenLong to match Java's DataOutputStream.writeVarLong format
            PAIMON_ASSIGN_OR_RAISE(int64_t num_row_ids, value_input.ReadVarLenLong());

            for (int64_t i = 0; i < num_row_ids; i++) {
                PAIMON_ASSIGN_OR_RAISE(int64_t row_id, value_input.ReadVarLenLong());
                result.Add(row_id);
            }
        }
    }

    return result;
}

Result<RoaringNavigableMap64> BTreeGlobalIndexReader::AllNonNullRows() {
    if (files_.empty()) {
        return RoaringNavigableMap64();
    }

    int64_t total_rows = files_[0].range_end + 1;
    uint64_t null_count = null_bitmap_->GetLongCardinality();

    const double NULL_RATIO_THRESHOLD = 0.1;
    const int64_t MAX_ROWS_FOR_SUBTRACTION = 10000000;

    bool use_subtraction = (total_rows <= MAX_ROWS_FOR_SUBTRACTION) &&
                           (null_count < static_cast<uint64_t>(total_rows * NULL_RATIO_THRESHOLD));

    if (use_subtraction) {
        RoaringNavigableMap64 result;
        result.AddRange(Range(0, total_rows - 1));
        result.AndNot(*null_bitmap_);
        return result;
    }

    if (!has_min_key_) {
        return RoaringNavigableMap64();
    }
    return RangeQuery(min_key_, max_key_, true, true);
}

// Helper function to convert Literal to MemorySlice
static Result<MemorySlice> LiteralToMemorySlice(const Literal& literal, MemoryPool* pool) {
    if (literal.IsNull()) {
        return Status::Invalid("Cannot convert null literal to MemorySlice for btree index query");
    }

    auto type = literal.GetType();

    // Handle string/binary types
    if (type == FieldType::STRING || type == FieldType::BINARY) {
        try {
            std::string str_value = literal.GetValue<std::string>();
            auto bytes = Bytes::AllocateBytes(str_value, pool);
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert string/binary literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    // Handle integer types
    if (type == FieldType::BIGINT) {
        try {
            int64_t value = literal.GetValue<int64_t>();
            auto bytes = Bytes::AllocateBytes(8, pool);
            bytes->data()[0] = static_cast<char>(value & 0xFF);
            bytes->data()[1] = static_cast<char>((value >> 8) & 0xFF);
            bytes->data()[2] = static_cast<char>((value >> 16) & 0xFF);
            bytes->data()[3] = static_cast<char>((value >> 24) & 0xFF);
            bytes->data()[4] = static_cast<char>((value >> 32) & 0xFF);
            bytes->data()[5] = static_cast<char>((value >> 40) & 0xFF);
            bytes->data()[6] = static_cast<char>((value >> 48) & 0xFF);
            bytes->data()[7] = static_cast<char>((value >> 56) & 0xFF);
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert bigint literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    if (type == FieldType::INT) {
        try {
            int32_t value = literal.GetValue<int32_t>();
            auto bytes = Bytes::AllocateBytes(4, pool);
            bytes->data()[0] = static_cast<char>(value & 0xFF);
            bytes->data()[1] = static_cast<char>((value >> 8) & 0xFF);
            bytes->data()[2] = static_cast<char>((value >> 16) & 0xFF);
            bytes->data()[3] = static_cast<char>((value >> 24) & 0xFF);
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert int literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    if (type == FieldType::TINYINT) {
        try {
            int8_t value = literal.GetValue<int8_t>();
            auto bytes = Bytes::AllocateBytes(1, pool);
            bytes->data()[0] = static_cast<char>(value);
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert tinyint literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    if (type == FieldType::SMALLINT) {
        try {
            int16_t value = literal.GetValue<int16_t>();
            auto bytes = Bytes::AllocateBytes(2, pool);
            bytes->data()[0] = static_cast<char>(value & 0xFF);
            bytes->data()[1] = static_cast<char>((value >> 8) & 0xFF);
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert smallint literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    if (type == FieldType::BOOLEAN) {
        try {
            bool value = literal.GetValue<bool>();
            // Convert to string "1" or "0" to match the format used in BTreeGlobalIndexWriter
            std::string str_value = value ? "1" : "0";
            auto bytes = Bytes::AllocateBytes(str_value, pool);
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert boolean literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    if (type == FieldType::FLOAT) {
        try {
            float value = literal.GetValue<float>();
            // Convert to string to match the format used in BTreeGlobalIndexWriter
            std::string str_value = std::to_string(value);
            auto bytes = Bytes::AllocateBytes(str_value, pool);
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert float literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    if (type == FieldType::DOUBLE) {
        try {
            double value = literal.GetValue<double>();
            // Convert to string to match the format used in BTreeGlobalIndexWriter
            std::string str_value = std::to_string(value);
            auto bytes = Bytes::AllocateBytes(str_value, pool);
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert double literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    if (type == FieldType::DATE) {
        try {
            int32_t value = literal.GetValue<int32_t>();
            // Convert to string to match the format used in BTreeGlobalIndexWriter
            std::string str_value = std::to_string(value);
            auto bytes = Bytes::AllocateBytes(str_value, pool);
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert date literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    if (type == FieldType::TIMESTAMP) {
        try {
            int64_t value = literal.GetValue<int64_t>();
            // Convert to string to match the format used in BTreeGlobalIndexWriter
            std::string str_value = std::to_string(value);
            auto bytes = Bytes::AllocateBytes(str_value, pool);
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert timestamp literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    if (type == FieldType::DECIMAL) {
        try {
            Decimal decimal_value = literal.GetValue<Decimal>();
            auto bytes = Bytes::AllocateBytes(16, pool);
            uint64_t high_bits = decimal_value.HighBits();
            uint64_t low_bits = decimal_value.LowBits();
            for (int i = 0; i < 8; ++i) {
                bytes->data()[i] = static_cast<char>((high_bits >> (56 - i * 8)) & 0xFF);
            }
            for (int i = 0; i < 8; ++i) {
                bytes->data()[8 + i] = static_cast<char>((low_bits >> (56 - i * 8)) & 0xFF);
            }
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert decimal literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    return Status::NotImplemented("Literal type " + FieldTypeUtils::FieldTypeToString(type) +
                                  " not yet supported in btree index");
}

}  // namespace paimon
