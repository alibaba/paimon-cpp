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
    return std::make_shared<BTreeGlobalIndexWriter>(field_name, file_writer, pool);
}

// Forward declarations for helper functions
static Result<std::shared_ptr<MemorySlice>> LiteralToMemorySlice(const Literal& literal,
                                                                 MemoryPool* pool);

// Create a comparator function based on field type
static std::function<int32_t(const std::shared_ptr<MemorySlice>&,
                             const std::shared_ptr<MemorySlice>&)>
CreateComparator(FieldType field_type) {
    switch (field_type) {
        case FieldType::STRING:
        case FieldType::BINARY:
            // String/binary comparison: lexicographic order
            return [](const std::shared_ptr<MemorySlice>& a,
                      const std::shared_ptr<MemorySlice>& b) -> int32_t {
                if (!a || !b) return 0;
                auto a_bytes = a->GetHeapMemory();
                auto b_bytes = b->GetHeapMemory();
                if (!a_bytes || !b_bytes) return 0;
                size_t min_len = std::min(a_bytes->size(), b_bytes->size());
                int cmp = memcmp(a_bytes->data(), b_bytes->data(), min_len);
                if (cmp != 0) return cmp < 0 ? -1 : 1;
                if (a_bytes->size() < b_bytes->size()) return -1;
                if (a_bytes->size() > b_bytes->size()) return 1;
                return 0;
            };
        case FieldType::BIGINT:
            // int64_t comparison
            return [](const std::shared_ptr<MemorySlice>& a,
                      const std::shared_ptr<MemorySlice>& b) -> int32_t {
                if (!a || !b) return 0;
                auto a_bytes = a->GetHeapMemory();
                auto b_bytes = b->GetHeapMemory();
                if (!a_bytes || !b_bytes || a_bytes->size() < 8 || b_bytes->size() < 8) return 0;
                int64_t a_val, b_val;
                memcpy(&a_val, a_bytes->data(), sizeof(int64_t));
                memcpy(&b_val, b_bytes->data(), sizeof(int64_t));
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::INT:
            // int32_t comparison
            return [](const std::shared_ptr<MemorySlice>& a,
                      const std::shared_ptr<MemorySlice>& b) -> int32_t {
                if (!a || !b) return 0;
                auto a_bytes = a->GetHeapMemory();
                auto b_bytes = b->GetHeapMemory();
                if (!a_bytes || !b_bytes || a_bytes->size() < 4 || b_bytes->size() < 4) return 0;
                int32_t a_val, b_val;
                memcpy(&a_val, a_bytes->data(), sizeof(int32_t));
                memcpy(&b_val, b_bytes->data(), sizeof(int32_t));
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::SMALLINT:
            // int16_t comparison
            return [](const std::shared_ptr<MemorySlice>& a,
                      const std::shared_ptr<MemorySlice>& b) -> int32_t {
                if (!a || !b) return 0;
                auto a_bytes = a->GetHeapMemory();
                auto b_bytes = b->GetHeapMemory();
                if (!a_bytes || !b_bytes || a_bytes->size() < 2 || b_bytes->size() < 2) return 0;
                int16_t a_val, b_val;
                memcpy(&a_val, a_bytes->data(), sizeof(int16_t));
                memcpy(&b_val, b_bytes->data(), sizeof(int16_t));
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::TINYINT:
            // int8_t comparison
            return [](const std::shared_ptr<MemorySlice>& a,
                      const std::shared_ptr<MemorySlice>& b) -> int32_t {
                if (!a || !b) return 0;
                auto a_bytes = a->GetHeapMemory();
                auto b_bytes = b->GetHeapMemory();
                if (!a_bytes || !b_bytes || a_bytes->size() < 1 || b_bytes->size() < 1) return 0;
                int8_t a_val = a_bytes->data()[0];
                int8_t b_val = b_bytes->data()[0];
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::BOOLEAN:
            // bool comparison
            return [](const std::shared_ptr<MemorySlice>& a,
                      const std::shared_ptr<MemorySlice>& b) -> int32_t {
                if (!a || !b) return 0;
                auto a_bytes = a->GetHeapMemory();
                auto b_bytes = b->GetHeapMemory();
                if (!a_bytes || !b_bytes || a_bytes->size() < 1 || b_bytes->size() < 1) return 0;
                bool a_val = a_bytes->data()[0] != 0;
                bool b_val = b_bytes->data()[0] != 0;
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::FLOAT:
            // float comparison
            return [](const std::shared_ptr<MemorySlice>& a,
                      const std::shared_ptr<MemorySlice>& b) -> int32_t {
                if (!a || !b) return 0;
                auto a_bytes = a->GetHeapMemory();
                auto b_bytes = b->GetHeapMemory();
                if (!a_bytes || !b_bytes || a_bytes->size() < 4 || b_bytes->size() < 4) return 0;
                float a_val, b_val;
                memcpy(&a_val, a_bytes->data(), sizeof(float));
                memcpy(&b_val, b_bytes->data(), sizeof(float));
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::DOUBLE:
            // double comparison
            return [](const std::shared_ptr<MemorySlice>& a,
                      const std::shared_ptr<MemorySlice>& b) -> int32_t {
                if (!a || !b) return 0;
                auto a_bytes = a->GetHeapMemory();
                auto b_bytes = b->GetHeapMemory();
                if (!a_bytes || !b_bytes || a_bytes->size() < 8 || b_bytes->size() < 8) return 0;
                double a_val, b_val;
                memcpy(&a_val, a_bytes->data(), sizeof(double));
                memcpy(&b_val, b_bytes->data(), sizeof(double));
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::DATE:
            // Date comparison (stored as int32_t days since epoch)
            return [](const std::shared_ptr<MemorySlice>& a,
                      const std::shared_ptr<MemorySlice>& b) -> int32_t {
                if (!a || !b) return 0;
                auto a_bytes = a->GetHeapMemory();
                auto b_bytes = b->GetHeapMemory();
                if (!a_bytes || !b_bytes || a_bytes->size() < 4 || b_bytes->size() < 4) return 0;
                int32_t a_val, b_val;
                memcpy(&a_val, a_bytes->data(), sizeof(int32_t));
                memcpy(&b_val, b_bytes->data(), sizeof(int32_t));
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::TIMESTAMP:
            // Timestamp comparison (stored as int64_t)
            return [](const std::shared_ptr<MemorySlice>& a,
                      const std::shared_ptr<MemorySlice>& b) -> int32_t {
                if (!a || !b) return 0;
                auto a_bytes = a->GetHeapMemory();
                auto b_bytes = b->GetHeapMemory();
                if (!a_bytes || !b_bytes || a_bytes->size() < 8 || b_bytes->size() < 8) return 0;
                int64_t a_val, b_val;
                memcpy(&a_val, a_bytes->data(), sizeof(int64_t));
                memcpy(&b_val, b_bytes->data(), sizeof(int64_t));
                if (a_val < b_val) return -1;
                if (a_val > b_val) return 1;
                return 0;
            };
        case FieldType::DECIMAL:
            // Decimal comparison (stored as 16 bytes big-endian for DECIMAL128)
            // Big-endian storage ensures correct lexicographic byte comparison for signed values
            return [](const std::shared_ptr<MemorySlice>& a,
                      const std::shared_ptr<MemorySlice>& b) -> int32_t {
                if (!a || !b) return 0;
                auto a_bytes = a->GetHeapMemory();
                auto b_bytes = b->GetHeapMemory();
                if (!a_bytes || !b_bytes) return 0;
                // Both should be 16 bytes for DECIMAL128
                if (a_bytes->size() < 16 || b_bytes->size() < 16) {
                    // Fallback to lexicographic comparison for truncated data
                    size_t min_len = std::min(a_bytes->size(), b_bytes->size());
                    int cmp = memcmp(a_bytes->data(), b_bytes->data(), min_len);
                    if (cmp != 0) return cmp < 0 ? -1 : 1;
                    if (a_bytes->size() < b_bytes->size()) return -1;
                    if (a_bytes->size() > b_bytes->size()) return 1;
                    return 0;
                }
                // For big-endian signed int128, direct byte comparison works correctly
                // because the sign bit is in the first byte
                int cmp = memcmp(a_bytes->data(), b_bytes->data(), 16);
                if (cmp < 0) return -1;
                if (cmp > 0) return 1;
                return 0;
            };
        default:
            // Default: lexicographic comparison
            return [](const std::shared_ptr<MemorySlice>& a,
                      const std::shared_ptr<MemorySlice>& b) -> int32_t {
                if (!a || !b) return 0;
                auto a_bytes = a->GetHeapMemory();
                auto b_bytes = b->GetHeapMemory();
                if (!a_bytes || !b_bytes) return 0;
                size_t min_len = std::min(a_bytes->size(), b_bytes->size());
                int cmp = memcmp(a_bytes->data(), b_bytes->data(), min_len);
                if (cmp != 0) return cmp < 0 ? -1 : 1;
                if (a_bytes->size() < b_bytes->size()) return -1;
                if (a_bytes->size() > b_bytes->size()) return 1;
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

    // prepare file footer
    auto block_cache = std::make_shared<BlockCache>(meta.file_path, in, pool, std::make_unique<CacheManager>());
    PAIMON_ASSIGN_OR_RAISE(auto segment,
                           block_cache->GetBlock(meta.file_size - BTreeFileFooter::ENCODED_LENGTH,
                                                 BTreeFileFooter::ENCODED_LENGTH, true));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BTreeFileFooter> footer,
                           BTreeFileFooter::Read(MemorySlice::Wrap(segment)->ToInput()));

    // prepare null_bitmap and sst_file_reader
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RoaringNavigableMap64> null_bitmap,
                           ReadNullBitmap(block_cache, footer->GetNullBitmapHandle()));

    // Wrap the comparator to return Result<int32_t>
    MemorySlice::SliceComparator result_comparator =
        [comparator](const std::shared_ptr<MemorySlice>& a,
                     const std::shared_ptr<MemorySlice>& b) -> Result<int32_t> {
        return comparator(a, b);
    };
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SstFileReader> sst_file_reader,
                           SstFileReader::Create(pool, in, result_comparator));

    auto index_meta = BTreeIndexMeta::Deserialize(meta.metadata, pool.get());

    // Convert Bytes to MemorySlice for keys
    std::shared_ptr<MemorySlice> min_key_slice;
    std::shared_ptr<MemorySlice> max_key_slice;
    if (index_meta->FirstKey()) {
        min_key_slice = MemorySlice::Wrap(index_meta->FirstKey());
    }
    if (index_meta->LastKey()) {
        max_key_slice = MemorySlice::Wrap(index_meta->LastKey());
    }

    return std::make_shared<BTreeGlobalIndexReader>(sst_file_reader, null_bitmap, min_key_slice,
                                                    max_key_slice, files, pool, comparator);
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
                           cache->GetBlock(block_handle->Offset(), block_handle->Size() + 4, false));

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
        return Status::Invalid(
            "Fail to deserialize null bitmap but crc check passed, "
            "this means the ser/de algorithms not match: " +
            std::string(e.what()));
    }

    return null_bitmap;
}

BTreeGlobalIndexReader::BTreeGlobalIndexReader(
    const std::shared_ptr<SstFileReader>& sst_file_reader,
    const std::shared_ptr<RoaringNavigableMap64>& null_bitmap,
    const std::shared_ptr<MemorySlice>& min_key, const std::shared_ptr<MemorySlice>& max_key,
    const std::vector<GlobalIndexIOMeta>& files, const std::shared_ptr<MemoryPool>& pool,
    std::function<int32_t(const std::shared_ptr<MemorySlice>&, const std::shared_ptr<MemorySlice>&)>
        comparator)
    : sst_file_reader_(sst_file_reader),
      null_bitmap_(null_bitmap),
      min_key_(min_key),
      max_key_(max_key),
      files_(files),
      pool_(pool),
      comparator_(std::move(comparator)) {}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitIsNotNull() {
    // nulls are stored separately in null bitmap.
    return std::make_shared<BitmapGlobalIndexResult>([this]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result, AllNonNullRows());
        return result.GetBitmap();
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitIsNull() {
    // nulls are stored separately in null bitmap.
    return std::make_shared<BitmapGlobalIndexResult>(
        [this]() -> Result<RoaringBitmap64> { return null_bitmap_->GetBitmap(); });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitStartsWith(
    const Literal& prefix) {
    // Use btree index for startsWith: find all keys >= prefix and < prefix_upper_bound
    // For string prefix "abc", the upper bound should be "abd" (increment last char)
    // This ensures we only get keys that actually start with the prefix
    return std::make_shared<BitmapGlobalIndexResult>([this, &prefix]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(auto prefix_slice, LiteralToMemorySlice(prefix, pool_.get()));

        auto prefix_type = prefix.GetType();

        // For string/binary types, compute the upper bound for prefix matching
        if (prefix_type == FieldType::STRING || prefix_type == FieldType::BINARY) {
            auto prefix_bytes = prefix_slice->GetHeapMemory();
            if (!prefix_bytes || prefix_bytes->size() == 0) {
                // Empty prefix matches all non-null rows
                PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result, AllNonNullRows());
                return result.GetBitmap();
            }

            // Compute upper bound: increment the last byte of the prefix
            // For example, "abc" -> "abd", "ab\xFF" -> "ac"
            std::string upper_bound_str(prefix_bytes->data(), prefix_bytes->size());
            bool overflow = true;
            for (int i = static_cast<int>(upper_bound_str.size()) - 1; i >= 0 && overflow; --i) {
                unsigned char c = static_cast<unsigned char>(upper_bound_str[i]);
                if (c < 0xFF) {
                    upper_bound_str[i] = c + 1;
                    overflow = false;
                } else {
                    upper_bound_str[i] = 0x00;
                    // Continue to increment previous byte
                }
            }

            std::shared_ptr<MemorySlice> upper_bound_slice;
            if (!overflow) {
                auto upper_bytes = Bytes::AllocateBytes(upper_bound_str, pool_.get());
                upper_bound_slice =
                    MemorySlice::Wrap(std::shared_ptr<Bytes>(upper_bytes.release()));
            }
            // If overflow (all bytes were 0xFF), use max_key_ as upper bound

            // Execute range query [prefix, upper_bound)
            PAIMON_ASSIGN_OR_RAISE(
                RoaringNavigableMap64 result,
                RangeQuery(prefix_slice, upper_bound_slice ? upper_bound_slice : max_key_, true,
                           false));  // lower_inclusive=true, upper_inclusive=false
            return result.GetBitmap();
        }

        // For non-string types, startsWith doesn't make semantic sense
        // Return empty result for non-string types
        return RoaringBitmap64();
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitEndsWith(
    const Literal& suffix) {
    // BTree index is not efficient for EndsWith queries as it requires checking all keys.
    // Return all non-null rows as fallback; the upper layer will perform exact filtering.
    // Note: This is a conservative approach that doesn't prune any rows.
    return std::make_shared<BitmapGlobalIndexResult>([this]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result, AllNonNullRows());
        return result.GetBitmap();
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitContains(
    const Literal& literal) {
    // BTree index is not efficient for Contains queries as it requires checking all keys.
    // Return all non-null rows as fallback; the upper layer will perform exact filtering.
    // Note: This is a conservative approach that doesn't prune any rows.
    return std::make_shared<BitmapGlobalIndexResult>([this]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result, AllNonNullRows());
        return result.GetBitmap();
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitLike(
    const Literal& literal) {
    // BTree index can efficiently handle LIKE patterns of the form "prefix%".
    // For other patterns (e.g., "%suffix", "%contains%"), return all non-null rows as fallback.
    if (literal.IsNull()) {
        return Status::Invalid("LIKE pattern cannot be null");
    }

    // Get the pattern string
    std::string pattern = literal.GetValue<std::string>();

    // Check if pattern is of the form "prefix%" (starts with a literal prefix and ends with %)
    // The prefix must not contain any wildcard characters (_ or %)
    // Escape sequences with \ are not supported in this simple implementation
    bool is_prefix_pattern = false;
    std::string prefix;

    // Find the position of the first wildcard character
    size_t first_wildcard = pattern.find_first_of("_%");

    if (first_wildcard != std::string::npos) {
        // Check if the pattern is exactly "prefix%" form
        // - First wildcard must be '%'
        // - It must be at the end of the pattern
        // - No other wildcards before it
        if (pattern[first_wildcard] == '%' && first_wildcard == pattern.length() - 1) {
            // Check if there are any wildcards in the prefix part
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
    } else {
        // No wildcards at all - this is an exact match, not a prefix pattern
        // We could optimize this to VisitEqual, but for simplicity, fall through to fallback
    }

    if (is_prefix_pattern) {
        // Use VisitStartsWith for prefix% patterns
        Literal prefix_literal(FieldType::STRING, prefix.c_str(), prefix.length());
        return VisitStartsWith(prefix_literal);
    }

    // For other patterns, return all non-null rows as fallback
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
            // Get all non-null rows first
            PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 result, AllNonNullRows());

            // Get the IN result and convert to navigable map
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
    return std::make_shared<BitmapGlobalIndexResult>(
        [this, &children]() -> Result<RoaringBitmap64> {
            if (children.empty()) {
                return Status::Invalid("VisitAnd called with no children");
            }

            // Start with the first child result
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

            // AND with remaining children
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
    return std::make_shared<BitmapGlobalIndexResult>(
        [this, &children]() -> Result<RoaringBitmap64> {
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

Result<RoaringNavigableMap64> BTreeGlobalIndexReader::RangeQuery(
    const std::shared_ptr<MemorySlice>& lower_bound,
    const std::shared_ptr<MemorySlice>& upper_bound, bool lower_inclusive, bool upper_inclusive) {
    // Create an SST file iterator to iterate through data blocks
    auto sst_iterator = sst_file_reader_->CreateIterator();

    // Seek iterator to the lower bound
    if (lower_bound) {
        auto lower_bytes = lower_bound->GetHeapMemory();
        PAIMON_RETURN_NOT_OK(sst_iterator->SeekTo(lower_bytes));
    }

    RoaringNavigableMap64 result;

    // Iterate through all relevant data blocks using GetNextBlock
    std::unique_ptr<BlockIterator> index_iterator;
    bool first_block = true;

    while (true) {
        // Get the next data block
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BlockIterator> data_iterator,
                               sst_file_reader_->GetNextBlock(index_iterator));

        if (!data_iterator || !data_iterator->HasNext()) {
            break;
        }

        // For the first block, we need to seek within the block to the exact position
        if (first_block && lower_bound) {
            PAIMON_ASSIGN_OR_RAISE(bool found, data_iterator->SeekTo(lower_bound));
            first_block = false;

            // After seeking, check if we still have data
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
                // Skip if key equals lower bound and lower is not inclusive
                continue;
            }

            // Check upper bound
            int cmp_upper = comparator ? comparator(entry->key, upper_bound) : 0;
            if (cmp_upper > 0 || (!upper_inclusive && cmp_upper == 0)) {
                // Key is beyond upper bound, we're done
                return result;
            }

            // Deserialize row IDs from the value
            // The value should contain an array of int64_t row IDs
            auto value_bytes = entry->value->CopyBytes(pool_.get());
            auto value_slice = MemorySlice::Wrap(value_bytes);
            auto value_input = value_slice->ToInput();

            // Read row IDs. The format is: [length][row_id1][row_id2]...
            // where length is the number of row IDs (varint)
            int64_t num_row_ids = value_input->ReadVarLenLong();

            for (int64_t i = 0; i < num_row_ids; i++) {
                int64_t row_id = value_input->ReadLong();
                result.Add(row_id);
            }
        }
    }

    return result;
}

Result<RoaringNavigableMap64> BTreeGlobalIndexReader::AllNonNullRows() {
    // Optimization: when null values are few, construct the result by subtracting
    // null_bitmap from a full range bitmap, instead of traversing all data blocks.
    //
    // We use a threshold: if null count is less than 10% of total rows, use the
    // subtraction approach; otherwise, traverse all data blocks.

    if (files_.empty()) {
        return RoaringNavigableMap64();
    }

    // Get total row count from range_end (inclusive last row id)
    int64_t total_rows = files_[0].range_end + 1;
    uint64_t null_count = null_bitmap_->GetLongCardinality();

    // Threshold: use subtraction if null count < 10% of total rows
    // and total rows is not too large (to avoid memory issues with huge bitmaps)
    const double NULL_RATIO_THRESHOLD = 0.1;
    const int64_t MAX_ROWS_FOR_SUBTRACTION = 10000000;  // 10 million rows max

    bool use_subtraction =
        (total_rows <= MAX_ROWS_FOR_SUBTRACTION) &&
        (null_count < static_cast<uint64_t>(total_rows * NULL_RATIO_THRESHOLD));

    if (use_subtraction) {
        // Build full range bitmap [0, range_end]
        RoaringNavigableMap64 result;
        result.AddRange(Range(0, total_rows - 1));
        // Subtract null bitmap
        result.AndNot(*null_bitmap_);
        return result;
    }

    // Fallback: traverse all data blocks
    // This is more efficient when there are many null values
    if (!min_key_) {
        return RoaringNavigableMap64();
    }
    return RangeQuery(min_key_, max_key_, true, true);
}

// Helper function to convert Literal to MemorySlice
static Result<std::shared_ptr<MemorySlice>> LiteralToMemorySlice(const Literal& literal,
                                                                 MemoryPool* pool) {
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
            auto bytes = Bytes::AllocateBytes(sizeof(value), pool);
            memcpy(bytes->data(), &value, sizeof(value));
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert bigint literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    if (type == FieldType::INT) {
        try {
            int32_t value = literal.GetValue<int32_t>();
            auto bytes = Bytes::AllocateBytes(sizeof(value), pool);
            memcpy(bytes->data(), &value, sizeof(value));
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert int literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    // Handle other numeric types similarly
    if (type == FieldType::TINYINT) {
        try {
            int8_t value = literal.GetValue<int8_t>();
            auto bytes = Bytes::AllocateBytes(sizeof(value), pool);
            memcpy(bytes->data(), &value, sizeof(value));
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert tinyint literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    if (type == FieldType::SMALLINT) {
        try {
            int16_t value = literal.GetValue<int16_t>();
            auto bytes = Bytes::AllocateBytes(sizeof(value), pool);
            memcpy(bytes->data(), &value, sizeof(value));
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert smallint literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    // Handle boolean
    if (type == FieldType::BOOLEAN) {
        try {
            bool value = literal.GetValue<bool>();
            auto bytes = Bytes::AllocateBytes(1, pool);
            bytes->data()[0] = value ? 1 : 0;
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert boolean literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    // Handle float
    if (type == FieldType::FLOAT) {
        try {
            float value = literal.GetValue<float>();
            auto bytes = Bytes::AllocateBytes(sizeof(value), pool);
            memcpy(bytes->data(), &value, sizeof(value));
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert float literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    // Handle double
    if (type == FieldType::DOUBLE) {
        try {
            double value = literal.GetValue<double>();
            auto bytes = Bytes::AllocateBytes(sizeof(value), pool);
            memcpy(bytes->data(), &value, sizeof(value));
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert double literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    // Handle date (stored as int32_t days since epoch)
    if (type == FieldType::DATE) {
        try {
            int32_t value = literal.GetValue<int32_t>();
            auto bytes = Bytes::AllocateBytes(sizeof(value), pool);
            memcpy(bytes->data(), &value, sizeof(value));
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert date literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    // Handle timestamp (stored as int64_t)
    if (type == FieldType::TIMESTAMP) {
        try {
            // Timestamp is stored as int64_t (milliseconds or microseconds depending on precision)
            int64_t value = literal.GetValue<int64_t>();
            auto bytes = Bytes::AllocateBytes(sizeof(value), pool);
            memcpy(bytes->data(), &value, sizeof(value));
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert timestamp literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    // Handle decimal (DECIMAL128 stored as 16 bytes big-endian)
    if (type == FieldType::DECIMAL) {
        try {
            // Get the Decimal value and serialize as big-endian int128
            Decimal decimal_value = literal.GetValue<Decimal>();
            auto bytes = Bytes::AllocateBytes(16, pool);
            // Store as big-endian for correct lexicographic comparison
            // High 64 bits first, then low 64 bits
            uint64_t high_bits = decimal_value.HighBits();
            uint64_t low_bits = decimal_value.LowBits();
            // Write high bits (bytes 0-7)
            for (int i = 0; i < 8; ++i) {
                bytes->data()[i] = static_cast<char>((high_bits >> (56 - i * 8)) & 0xFF);
            }
            // Write low bits (bytes 8-15)
            for (int i = 0; i < 8; ++i) {
                bytes->data()[8 + i] = static_cast<char>((low_bits >> (56 - i * 8)) & 0xFF);
            }
            return MemorySlice::Wrap(std::shared_ptr<Bytes>(bytes.release()));
        } catch (const std::exception& e) {
            return Status::Invalid("Failed to convert decimal literal to MemorySlice: " +
                                   std::string(e.what()));
        }
    }

    // For unhandled types, return error for now
    return Status::NotImplemented("Literal type " + FieldTypeUtils::FieldTypeToString(type) +
                                  " not yet supported in btree index");
}

}  // namespace paimon
