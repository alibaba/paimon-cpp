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

#include <memory>
#include <string>

#include "paimon/common/global_index/btree/btree_file_footer.h"
#include "paimon/common/global_index/btree/btree_index_meta.h"
#include "paimon/common/memory/memory_slice.h"
#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/common/utils/crc32c.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/common/utils/roaring_navigable_map64.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/memory/bytes.h"
#include "paimon/predicate/literal.h"

namespace paimon {

// Forward declarations for helper functions
static Result<std::shared_ptr<MemorySlice>> LiteralToMemorySlice(const Literal& literal,
                                                                 MemoryPool* pool);
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
    auto block_cache = std::make_shared<BlockCache>(meta.file_path, in, pool, cache_manager);
    auto segment = block_cache->GetBlock(meta.file_size - BTreeFileFooter::ENCODED_LENGTH,
                                         BTreeFileFooter::ENCODED_LENGTH, true);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BTreeFileFooter> footer,
                           BTreeFileFooter::Read(MemorySlice::Wrap(segment)->ToInput()));

    // prepare null_bitmap and sst_file_reader
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RoaringNavigableMap64> null_bitmap,
                           ReadNullBitmap(block_cache, footer->GetNullBitmapHandle()));
    std::shared_ptr<paimon::FileSystem> fs;
    std::function<int32_t(const std::shared_ptr<MemorySlice>&, const std::shared_ptr<MemorySlice>&)>
        comparator;
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SstFileReader> sst_file_reader,
                           SstFileReader::Create(pool, fs, meta.file_path, comparator));

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
    // Use btree index for startsWith: find all keys >= prefix and check if they start with prefix
    return std::make_shared<BitmapGlobalIndexResult>([this, &prefix]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(auto prefix_slice, LiteralToMemorySlice(prefix, pool_.get()));

        // Search for keys >= prefix
        PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 all_candidates,
                               RangeQuery(prefix_slice, max_key_, true, true));

        // If no comparator or prefix is empty, return all candidates
        if (!comparator_ || prefix_slice->Length() == 0) {
            return all_candidates.GetBitmap();
        }

        // Filter to only keep keys that actually start with prefix
        RoaringNavigableMap64 result;

        // We need to iterate through the keys and check if they start with prefix
        // This is a simplified approach - in a full implementation, we'd need to properly
        // iterate through the btree to check prefixes

        // For now, return all candidates if the index type is string/binary
        // The exact filtering would require being able to read and compare the keys
        auto prefix_type = prefix.GetType();
        if (prefix_type == FieldType::STRING || prefix_type == FieldType::BINARY) {
            // In a real implementation, we would iterate through candidates and check each key
            // For simplicity, we're using the btree range query which gives us keys >= prefix
            // The comparator would help determine which ones actually start with prefix
            return all_candidates.GetBitmap();
        }

        // For non-string types, startsWith doesn't make much sense, return all non-null rows
        PAIMON_ASSIGN_OR_RAISE(RoaringNavigableMap64 all_rows, AllNonNullRows());
        return all_rows.GetBitmap();
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
    // Create an index block iterator to iterate through data blocks
    auto index_block_reader = sst_file_reader_->GetIndexBlockReader();
    auto index_iterator = index_block_reader->Iterator();

    // Seek index iterator to the lower bound
    index_iterator->SeekTo(lower_bound);

    RoaringNavigableMap64 result;

    // Iterate through all relevant data blocks
    bool first_block = true;

    while (index_iterator->HasNext()) {
        // Get the next data block
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BlockIterator> data_iterator,
                               sst_file_reader_->GetNextBlock(index_iterator));

        if (!data_iterator || !data_iterator->HasNext()) {
            continue;
        }

        // For the first block, we need to seek within the block to the exact position
        if (first_block) {
            data_iterator->SeekTo(lower_bound);
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
            int cmp_lower = comparator ? comparator(entry->key_, lower_bound) : 0;

            // Check lower bound
            if (!lower_inclusive && cmp_lower == 0) {
                // Skip if key equals lower bound and lower is not inclusive
                continue;
            }

            // Check upper bound
            int cmp_upper = comparator ? comparator(entry->key_, upper_bound) : 0;
            if (cmp_upper > 0 || (!upper_inclusive && cmp_upper == 0)) {
                // Key is beyond upper bound, we're done
                return result;
            }

            // Deserialize row IDs from the value
            // The value should contain an array of int64_t row IDs
            auto value_bytes = entry->value_->CopyBytes(pool_.get());
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
    // Traverse all data to avoid returning null values, which is very advantageous in
    // situations where there are many null values
    // TODO do not traverse all data if less null values
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

    // For unhandled types, return error for now
    return Status::NotImplemented("Literal type " + FieldTypeUtils::FieldTypeToString(type) +
                                  " not yet supported in btree index");
}

}  // namespace paimon
