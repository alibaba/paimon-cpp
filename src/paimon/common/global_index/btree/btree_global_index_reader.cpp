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

#include "paimon/common/global_index/btree/btree_global_index_reader.h"

#include "fmt/format.h"
#include "paimon/common/global_index/btree/key_serializer.h"
#include "paimon/common/memory/memory_slice.h"
#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/memory/bytes.h"
#include "paimon/predicate/literal.h"
namespace paimon {

Result<std::shared_ptr<BTreeGlobalIndexReader>> BTreeGlobalIndexReader::Create(
    const std::shared_ptr<SstFileReader>& sst_file_reader, RoaringBitmap64&& null_bitmap,
    const std::optional<MemorySlice>& min_key_slice,
    const std::optional<MemorySlice>& max_key_slice,
    const std::shared_ptr<arrow::DataType>& key_type, const std::shared_ptr<MemoryPool>& pool) {
    std::optional<Literal> min_key;
    std::optional<Literal> max_key;
    if (min_key_slice) {
        PAIMON_ASSIGN_OR_RAISE(
            min_key, KeySerializer::DeserializeKey(min_key_slice.value(), key_type, pool.get()));
    }
    if (max_key_slice) {
        PAIMON_ASSIGN_OR_RAISE(
            max_key, KeySerializer::DeserializeKey(max_key_slice.value(), key_type, pool.get()));
    }
    return std::shared_ptr<BTreeGlobalIndexReader>(new BTreeGlobalIndexReader(
        sst_file_reader, std::move(null_bitmap), std::move(min_key), std::move(max_key),
        min_key_slice, max_key_slice, key_type, pool));
}

BTreeGlobalIndexReader::BTreeGlobalIndexReader(
    const std::shared_ptr<SstFileReader>& sst_file_reader, RoaringBitmap64&& null_bitmap,
    std::optional<Literal> min_key, std::optional<Literal> max_key,
    std::optional<MemorySlice> min_key_slice, std::optional<MemorySlice> max_key_slice,
    const std::shared_ptr<arrow::DataType>& key_type, const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      sst_file_reader_(sst_file_reader),
      null_bitmap_(std::move(null_bitmap)),
      min_key_(std::move(min_key)),
      max_key_(std::move(max_key)),
      min_key_slice_(std::move(min_key_slice)),
      max_key_slice_(std::move(max_key_slice)),
      key_type_(key_type),
      comparator_(KeySerializer::CreateComparator(key_type, pool)) {}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitIsNotNull() {
    return std::make_shared<BitmapGlobalIndexResult>(
        [reader = shared_from_this()]() -> Result<RoaringBitmap64> {
            return reader->AllNonNullRows();
        });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitIsNull() {
    return std::make_shared<BitmapGlobalIndexResult>(
        [reader = shared_from_this()]() -> Result<RoaringBitmap64> {
            return reader->null_bitmap_;
        });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitStartsWith(
    const Literal& prefix) {
    if (prefix.IsNull()) {
        return Status::Invalid("StartsWith pattern cannot be null");
    }

    if (prefix.GetType() == FieldType::STRING) {
        auto prefix_str = prefix.GetValue<std::string>();
        if (prefix_str.empty()) {
            return std::make_shared<BitmapGlobalIndexResult>(
                [reader = shared_from_this()]() -> Result<RoaringBitmap64> {
                    return reader->AllNonNullRows();
                });
        }

        // Compute the exclusive upper bound for the prefix range.
        // Increment the last byte; carry over if it overflows 0xFF.
        std::string upper_str = prefix_str;
        bool overflow = true;
        for (int32_t i = static_cast<int32_t>(upper_str.size()) - 1; i >= 0 && overflow; --i) {
            auto c = static_cast<unsigned char>(upper_str[i]);
            if (c < 0xFF) {
                upper_str[i] = static_cast<char>(c + 1);
                overflow = false;
            } else {
                upper_str[i] = 0x00;
            }
        }

        if (!overflow) {
            Literal upper_bound(FieldType::STRING, upper_str.data(), upper_str.size());
            return std::make_shared<BitmapGlobalIndexResult>(
                [reader = shared_from_this(), prefix = prefix,
                 upper_bound = std::move(upper_bound)]() -> Result<RoaringBitmap64> {
                    return reader->RangeQuery(prefix, upper_bound, /*from_inclusive=*/true,
                                              /*to_inclusive=*/false);
                });
        }

        // All bytes were 0xFF, use max_key_ as upper bound
        return std::make_shared<BitmapGlobalIndexResult>(
            [reader = shared_from_this(), prefix = prefix]() -> Result<RoaringBitmap64> {
                return reader->RangeQuery(prefix, reader->max_key_, /*from_inclusive=*/true,
                                          /*to_inclusive=*/true);
            });
    }

    return std::make_shared<BitmapGlobalIndexResult>(
        [reader = shared_from_this()]() -> Result<RoaringBitmap64> {
            return reader->AllNonNullRows();
        });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitEndsWith(
    const Literal& suffix) {
    return std::make_shared<BitmapGlobalIndexResult>(
        [reader = shared_from_this()]() -> Result<RoaringBitmap64> {
            return reader->AllNonNullRows();
        });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitContains(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>(
        [reader = shared_from_this()]() -> Result<RoaringBitmap64> {
            return reader->AllNonNullRows();
        });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitLike(
    const Literal& literal) {
    if (literal.IsNull()) {
        return Status::Invalid("LIKE pattern cannot be null");
    }
    if (literal.GetType() == FieldType::STRING) {
        auto pattern = literal.GetValue<std::string>();

        bool is_prefix_pattern = false;
        std::string prefix;

        size_t first_wildcard = pattern.find_first_of("_%");

        if (first_wildcard != std::string::npos && pattern[first_wildcard] == '%' &&
            first_wildcard == pattern.length() - 1) {
            is_prefix_pattern = true;
            prefix = pattern.substr(0, first_wildcard);
        }

        if (is_prefix_pattern) {
            Literal prefix_literal(FieldType::STRING, prefix.data(), prefix.length());
            return VisitStartsWith(prefix_literal);
        }
    }
    return std::make_shared<BitmapGlobalIndexResult>(
        [reader = shared_from_this()]() -> Result<RoaringBitmap64> {
            return reader->AllNonNullRows();
        });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitLessThan(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>(
        [reader = shared_from_this(), literal = literal]() -> Result<RoaringBitmap64> {
            return reader->RangeQuery(reader->min_key_, literal, /*from_inclusive=*/true,
                                      /*to_inclusive=*/false);
        });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitGreaterOrEqual(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>(
        [reader = shared_from_this(), literal = literal]() -> Result<RoaringBitmap64> {
            return reader->RangeQuery(literal, reader->max_key_, /*from_inclusive=*/true,
                                      /*to_inclusive=*/true);
        });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitNotEqual(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>(
        [reader = shared_from_this(), literal = literal]() -> Result<RoaringBitmap64> {
            PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result, reader->AllNonNullRows());
            PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 equal_result,
                                   reader->RangeQuery(literal, literal, /*from_inclusive=*/true,
                                                      /*to_inclusive=*/true));
            result -= equal_result;
            return result;
        });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitLessOrEqual(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>(
        [reader = shared_from_this(), literal = literal]() -> Result<RoaringBitmap64> {
            return reader->RangeQuery(reader->min_key_, literal, /*from_inclusive=*/true,
                                      /*to_inclusive=*/true);
        });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitEqual(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>(
        [reader = shared_from_this(), literal = literal]() -> Result<RoaringBitmap64> {
            return reader->RangeQuery(literal, literal, /*from_inclusive=*/true,
                                      /*to_inclusive=*/true);
        });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitGreaterThan(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>(
        [reader = shared_from_this(), literal = literal]() -> Result<RoaringBitmap64> {
            return reader->RangeQuery(literal, reader->max_key_, /*from_inclusive=*/false,
                                      /*to_inclusive=*/true);
        });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitIn(
    const std::vector<Literal>& literals) {
    return std::make_shared<BitmapGlobalIndexResult>(
        [reader = shared_from_this(), literals = literals]() -> Result<RoaringBitmap64> {
            RoaringBitmap64 result;
            for (const auto& literal : literals) {
                PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 literal_result,
                                       reader->RangeQuery(literal, literal, /*from_inclusive=*/true,
                                                          /*to_inclusive=*/true));
                result |= literal_result;
            }
            return result;
        });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    return std::make_shared<BitmapGlobalIndexResult>(
        [reader = shared_from_this(), literals = literals]() -> Result<RoaringBitmap64> {
            PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result, reader->AllNonNullRows());
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> in_result,
                                   reader->VisitIn(literals));
            auto* typed_in_result = dynamic_cast<BitmapGlobalIndexResult*>(in_result.get());
            if (!typed_in_result) {
                return Status::Invalid(
                    "VisitIn should return BitmapGlobalIndexResult in BTreeGlobalIndexReader");
            }
            PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* in_bitmap, typed_in_result->GetBitmap());
            result -= (*in_bitmap);
            return result;
        });
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> BTreeGlobalIndexReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search) {
    return Status::Invalid("Vector search not supported in BTree index");
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>& full_text_search) {
    return Status::Invalid("Full text search not supported in BTree index");
}

Result<RoaringBitmap64> BTreeGlobalIndexReader::RangeQuery(const std::optional<Literal>& from,
                                                           const std::optional<Literal>& to,
                                                           bool from_inclusive, bool to_inclusive) {
    if (!from || !to) {
        return RoaringBitmap64();
    }

    // Create an index block iterator to iterate through data blocks
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> from_bytes,
                           KeySerializer::SerializeKey(from.value(), key_type_, pool_.get()));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> to_bytes,
                           KeySerializer::SerializeKey(to.value(), key_type_, pool_.get()));
    MemorySlice from_slice = MemorySlice::Wrap(from_bytes);
    MemorySlice to_slice = MemorySlice::Wrap(to_bytes);

    // Determine if we can skip lower/upper bound checks using cached serialized min/max keys.
    // When from == min_key_, all entries are >= from, so skip lower bound comparison.
    // When to == max_key_, all entries are <= to, so skip upper bound comparison.
    bool skip_from_check = false;
    bool skip_to_check = false;
    if (min_key_slice_) {
        PAIMON_ASSIGN_OR_RAISE(int32_t cmp_min, comparator_(from_slice, min_key_slice_.value()));
        skip_from_check = (cmp_min == 0) && from_inclusive;
    }
    if (max_key_slice_) {
        PAIMON_ASSIGN_OR_RAISE(int32_t cmp_max, comparator_(to_slice, max_key_slice_.value()));
        skip_to_check = (cmp_max == 0) && to_inclusive;
    }

    RoaringBitmap64 result;

    // Per-data-block row-id buffer. Collect row-ids within a single data
    // block and flush them with one AddMany call.
    std::vector<int64_t> block_row_ids;

    auto index_iterator = sst_file_reader_->CreateIndexIterator();
    PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] bool seek_result, index_iterator->SeekTo(from_slice));

    bool first_block = true;
    // After SeekTo positions at the first entry >= from, only entries in the first
    // block before the seek position could be < from. Once we pass them, all subsequent
    // entries are guaranteed >= from, so we can skip the lower bound check.
    bool passed_from_bound = skip_from_check;

    while (index_iterator->HasNext()) {
        // Get the next data block
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BlockIterator> data_iterator,
                               sst_file_reader_->GetNextBlock(index_iterator));

        if (!data_iterator || !data_iterator->HasNext()) {
            assert(false);
            break;
        }

        // For the first block, we need to seek within the block to the exact position
        if (first_block) {
            PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] bool found, data_iterator->SeekTo(from_slice));
            first_block = false;
        }

        block_row_ids.clear();

        if (skip_to_check && passed_from_bound) {
            // Fast path: no boundary checks needed, skip key parsing entirely
            while (data_iterator->HasNext()) {
                PAIMON_ASSIGN_OR_RAISE(MemorySlice value, data_iterator->SkipKeyAndReadValue());
                PAIMON_RETURN_NOT_OK(DeserializeRowIds(value, &block_row_ids));
            }
        } else if (skip_to_check) {
            // Only need to check lower bound (first few entries in first block)
            while (data_iterator->HasNext()) {
                PAIMON_ASSIGN_OR_RAISE(BlockEntry entry, data_iterator->Next());
                if (!passed_from_bound) {
                    PAIMON_ASSIGN_OR_RAISE(int32_t cmp_from, comparator_(entry.key, from_slice));
                    if (!from_inclusive && cmp_from == 0) {
                        continue;
                    }
                    if (cmp_from > 0) {
                        passed_from_bound = true;
                    }
                }
                PAIMON_RETURN_NOT_OK(DeserializeRowIds(entry.value, &block_row_ids));
            }
            passed_from_bound = true;
        } else if (passed_from_bound) {
            // Only need to check upper bound
            bool reached_end = false;
            while (data_iterator->HasNext()) {
                PAIMON_ASSIGN_OR_RAISE(BlockEntry entry, data_iterator->Next());
                PAIMON_ASSIGN_OR_RAISE(int32_t cmp_to, comparator_(entry.key, to_slice));
                if (cmp_to > 0 || (!to_inclusive && cmp_to == 0)) {
                    reached_end = true;
                    break;
                }
                PAIMON_RETURN_NOT_OK(DeserializeRowIds(entry.value, &block_row_ids));
            }
            if (!block_row_ids.empty()) {
                result.AddMany(block_row_ids.size(), block_row_ids.data());
            }
            if (reached_end) {
                return result;
            }
            continue;
        } else {
            // Need to check both bounds
            bool reached_end = false;
            while (data_iterator->HasNext()) {
                PAIMON_ASSIGN_OR_RAISE(BlockEntry entry, data_iterator->Next());
                if (!passed_from_bound) {
                    PAIMON_ASSIGN_OR_RAISE(int32_t cmp_from, comparator_(entry.key, from_slice));
                    if (!from_inclusive && cmp_from == 0) {
                        continue;
                    }
                    if (cmp_from > 0) {
                        passed_from_bound = true;
                    }
                }
                PAIMON_ASSIGN_OR_RAISE(int32_t cmp_to, comparator_(entry.key, to_slice));
                if (cmp_to > 0 || (!to_inclusive && cmp_to == 0)) {
                    reached_end = true;
                    break;
                }
                PAIMON_RETURN_NOT_OK(DeserializeRowIds(entry.value, &block_row_ids));
            }
            passed_from_bound = true;
            if (!block_row_ids.empty()) {
                result.AddMany(block_row_ids.size(), block_row_ids.data());
            }
            if (reached_end) {
                return result;
            }
            continue;
        }

        // Flush this block's row-ids in one batched call
        if (!block_row_ids.empty()) {
            result.AddMany(block_row_ids.size(), block_row_ids.data());
        }
    }

    return result;
}

Status BTreeGlobalIndexReader::DeserializeRowIds(const MemorySlice& slice,
                                                 std::vector<int64_t>* out) const {
    auto input = slice.ToInput();
    PAIMON_ASSIGN_OR_RAISE(int32_t num_row_ids, input.ReadVarLenInt());
    if (num_row_ids <= 0) {
        return Status::Invalid(fmt::format(
            "Invalid row id length {} in DeserializeRowIds for BTreeGlobalIndexReader, must > 0",
            num_row_ids));
    }
    out->reserve(out->size() + static_cast<size_t>(num_row_ids));
    for (int32_t i = 0; i < num_row_ids; i++) {
        PAIMON_ASSIGN_OR_RAISE(int64_t row_id, input.ReadVarLenLong());
        out->push_back(row_id);
    }
    return Status::OK();
}

Result<RoaringBitmap64> BTreeGlobalIndexReader::AllNonNullRows() {
    // Traverse all data to avoid returning null values, which is very advantageous in
    // situations where there are many null values
    // TODO(xinyu.lxy) do not traverse all data if less null values
    if (!min_key_) {
        return RoaringBitmap64();
    }
    return RangeQuery(min_key_, max_key_, /*from_inclusive=*/true, /*to_inclusive=*/true);
}

}  // namespace paimon
