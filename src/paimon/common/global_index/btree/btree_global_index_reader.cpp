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

#include <cstring>

#include "paimon/common/memory/memory_slice.h"
#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/common/memory/memory_slice_output.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/memory/bytes.h"
#include "paimon/predicate/literal.h"
namespace paimon {

// Helper function to convert Literal to MemorySlice
static Result<MemorySlice> LiteralToMemorySlice(const Literal& literal, MemoryPool* pool,
                                                int32_t ts_precision) {
    if (literal.IsNull()) {
        return Status::Invalid("Cannot convert null literal to MemorySlice for btree index query");
    }

    auto type = literal.GetType();

    // Handle string/binary types
    if (type == FieldType::STRING || type == FieldType::BINARY) {
        auto str_value = literal.GetValue<std::string>();
        auto bytes = std::make_shared<Bytes>(str_value, pool);
        return MemorySlice::Wrap(bytes);
    }

    // Handle integer types
    if (type == FieldType::BIGINT) {
        auto value = literal.GetValue<int64_t>();
        auto bytes = std::make_shared<Bytes>(8, pool);
        memcpy(bytes->data(), &value, sizeof(int64_t));
        return MemorySlice::Wrap(bytes);
    }

    if (type == FieldType::INT) {
        auto value = literal.GetValue<int32_t>();
        auto bytes = std::make_shared<Bytes>(4, pool);
        memcpy(bytes->data(), &value, sizeof(int32_t));
        return MemorySlice::Wrap(bytes);
    }

    if (type == FieldType::TINYINT) {
        auto value = literal.GetValue<int8_t>();
        auto bytes = std::make_shared<Bytes>(1, pool);
        bytes->data()[0] = static_cast<char>(value);
        return MemorySlice::Wrap(bytes);
    }

    if (type == FieldType::SMALLINT) {
        auto value = literal.GetValue<int16_t>();
        auto bytes = std::make_shared<Bytes>(2, pool);
        memcpy(bytes->data(), &value, sizeof(int16_t));
        return MemorySlice::Wrap(bytes);
    }

    if (type == FieldType::BOOLEAN) {
        bool value = literal.GetValue<bool>();
        auto bytes = std::make_shared<Bytes>(1, pool);
        bytes->data()[0] = value ? 1 : 0;
        return MemorySlice::Wrap(bytes);
    }

    if (type == FieldType::FLOAT) {
        auto value = literal.GetValue<float>();
        auto bytes = std::make_shared<Bytes>(sizeof(float), pool);
        memcpy(bytes->data(), &value, sizeof(float));
        return MemorySlice::Wrap(bytes);
    }

    if (type == FieldType::DOUBLE) {
        auto value = literal.GetValue<double>();
        auto bytes = std::make_shared<Bytes>(sizeof(double), pool);
        memcpy(bytes->data(), &value, sizeof(double));
        return MemorySlice::Wrap(bytes);
    }

    if (type == FieldType::DATE) {
        // DATE is stored as int32_t to match Java's writeInt
        auto value = literal.GetValue<int32_t>();
        auto bytes = std::make_shared<Bytes>(sizeof(int32_t), pool);
        memcpy(bytes->data(), &value, sizeof(int32_t));
        return MemorySlice::Wrap(bytes);
    }

    if (type == FieldType::TIMESTAMP) {
        auto ts = literal.GetValue<Timestamp>();
        if (Timestamp::IsCompact(ts_precision)) {
            // compact: writeLong(millisecond)
            int64_t value = ts.GetMillisecond();
            auto bytes = std::make_shared<Bytes>(sizeof(int64_t), pool);
            memcpy(bytes->data(), &value, sizeof(int64_t));
            return MemorySlice::Wrap(bytes);
        } else {
            // non-compact: writeLong(millisecond) + writeVarLenInt(nanoOfMillisecond)
            MemorySliceOutput ts_out(13, pool);
            ts_out.WriteValue(ts.GetMillisecond());
            PAIMON_RETURN_NOT_OK(ts_out.WriteVarLenInt(ts.GetNanoOfMillisecond()));
            return ts_out.ToSlice();
        }
    }

    if (type == FieldType::DECIMAL) {
        auto decimal_value = literal.GetValue<Decimal>();
        auto bytes = std::make_shared<Bytes>(16, pool);
        uint64_t high_bits = decimal_value.HighBits();
        uint64_t low_bits = decimal_value.LowBits();
        for (int i = 0; i < 8; ++i) {
            bytes->data()[i] = static_cast<char>((high_bits >> (56 - i * 8)) & 0xFF);
        }
        for (int i = 0; i < 8; ++i) {
            bytes->data()[8 + i] = static_cast<char>((low_bits >> (56 - i * 8)) & 0xFF);
        }
        return MemorySlice::Wrap(bytes);
    }

    return Status::NotImplemented("Literal type " + FieldTypeUtils::FieldTypeToString(type) +
                                  " not yet supported in btree index");
}

BTreeGlobalIndexReader::BTreeGlobalIndexReader(
    const std::shared_ptr<SstFileReader>& sst_file_reader,
    const std::shared_ptr<RoaringBitmap64>& null_bitmap, const MemorySlice& min_key,
    const MemorySlice& max_key, bool has_min_key, const std::vector<GlobalIndexIOMeta>& files,
    const std::shared_ptr<MemoryPool>& pool,
    std::function<int32_t(const MemorySlice&, const MemorySlice&)> comparator, int32_t ts_precision)
    : sst_file_reader_(sst_file_reader),
      null_bitmap_(null_bitmap),
      min_key_(min_key),
      max_key_(max_key),
      has_min_key_(has_min_key),
      files_(files),
      pool_(pool),
      comparator_(std::move(comparator)),
      ts_precision_(ts_precision) {}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitIsNotNull() {
    return std::make_shared<BitmapGlobalIndexResult>([this]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result, AllNonNullRows());
        return result;
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitIsNull() {
    return std::make_shared<BitmapGlobalIndexResult>(
        [this]() -> Result<RoaringBitmap64> { return *null_bitmap_; });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitStartsWith(
    const Literal& prefix) {
    return std::make_shared<BitmapGlobalIndexResult>([this, &prefix]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(auto prefix_slice,
                               LiteralToMemorySlice(prefix, pool_.get(), ts_precision_));

        auto prefix_type = prefix.GetType();

        if (prefix_type == FieldType::STRING || prefix_type == FieldType::BINARY) {
            auto prefix_bytes = prefix_slice.GetHeapMemory();
            if (!prefix_bytes || prefix_bytes->size() == 0) {
                PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result, AllNonNullRows());
                return result;
            }

            std::string upper_bound_str(prefix_bytes->data(), prefix_bytes->size());
            bool overflow = true;
            for (int i = static_cast<int>(upper_bound_str.size()) - 1; i >= 0 && overflow; --i) {
                auto c = static_cast<unsigned char>(upper_bound_str[i]);
                if (c < 0xFF) {
                    upper_bound_str[i] = c + 1;
                    overflow = false;
                } else {
                    upper_bound_str[i] = 0x00;
                }
            }

            if (!overflow) {
                auto upper_bytes = std::make_shared<Bytes>(upper_bound_str, pool_.get());
                auto upper_bound_slice = MemorySlice::Wrap(upper_bytes);
                PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result,
                                       RangeQuery(prefix_slice, upper_bound_slice, true, false));
                return result;
            } else {
                // If overflow (all bytes were 0xFF), use max_key_ as upper bound
                PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result,
                                       RangeQuery(prefix_slice, max_key_, true, false));
                return result;
            }
        }

        return RoaringBitmap64();
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitEndsWith(
    const Literal& suffix) {
    return std::make_shared<BitmapGlobalIndexResult>([this]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result, AllNonNullRows());
        return result;
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitContains(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>([this]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result, AllNonNullRows());
        return result;
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitLike(
    const Literal& literal) {
    if (literal.IsNull()) {
        return Status::Invalid("LIKE pattern cannot be null");
    }

    auto pattern = literal.GetValue<std::string>();

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
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result, AllNonNullRows());
        return result;
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitLessThan(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>([this, &literal]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(auto literal_slice,
                               LiteralToMemorySlice(literal, pool_.get(), ts_precision_));
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result,
                               RangeQuery(min_key_, literal_slice, true, false));
        return result;
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitGreaterOrEqual(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>([this, &literal]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(auto literal_slice,
                               LiteralToMemorySlice(literal, pool_.get(), ts_precision_));
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result,
                               RangeQuery(literal_slice, max_key_, true, true));
        return result;
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitNotEqual(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>([this, &literal]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result, AllNonNullRows());
        PAIMON_ASSIGN_OR_RAISE(auto literal_slice,
                               LiteralToMemorySlice(literal, pool_.get(), ts_precision_));
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 equal_result,
                               RangeQuery(literal_slice, literal_slice, true, true));
        result -= equal_result;
        return result;
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitLessOrEqual(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>([this, &literal]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(auto literal_slice,
                               LiteralToMemorySlice(literal, pool_.get(), ts_precision_));
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result,
                               RangeQuery(min_key_, literal_slice, true, true));
        return result;
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitEqual(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>([this, &literal]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(auto literal_slice,
                               LiteralToMemorySlice(literal, pool_.get(), ts_precision_));
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result,
                               RangeQuery(literal_slice, literal_slice, true, true));
        return result;
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitGreaterThan(
    const Literal& literal) {
    return std::make_shared<BitmapGlobalIndexResult>([this, &literal]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(auto literal_slice,
                               LiteralToMemorySlice(literal, pool_.get(), ts_precision_));
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result,
                               RangeQuery(literal_slice, max_key_, false, true));
        return result;
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitIn(
    const std::vector<Literal>& literals) {
    return std::make_shared<BitmapGlobalIndexResult>(
        [this, &literals]() -> Result<RoaringBitmap64> {
            RoaringBitmap64 result;
            for (const auto& literal : literals) {
                PAIMON_ASSIGN_OR_RAISE(auto literal_slice,
                                       LiteralToMemorySlice(literal, pool_.get(), ts_precision_));
                PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 literal_result,
                                       RangeQuery(literal_slice, literal_slice, true, true));
                result |= literal_result;
            }
            return result;
        });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    return std::make_shared<BitmapGlobalIndexResult>(
        [this, &literals]() -> Result<RoaringBitmap64> {
            PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result, AllNonNullRows());

            PAIMON_ASSIGN_OR_RAISE(auto in_result_ptr, VisitIn(literals));
            PAIMON_ASSIGN_OR_RAISE(auto in_iterator, in_result_ptr->CreateIterator());

            RoaringBitmap64 in_bitmap;
            while (in_iterator->HasNext()) {
                in_bitmap.Add(in_iterator->Next());
            }

            result -= in_bitmap;
            return result;
        });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitBetween(const Literal& from,
                                                                                const Literal& to) {
    return std::make_shared<BitmapGlobalIndexResult>([this, &from,
                                                      &to]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(auto from_slice,
                               LiteralToMemorySlice(from, pool_.get(), ts_precision_));
        PAIMON_ASSIGN_OR_RAISE(auto to_slice, LiteralToMemorySlice(to, pool_.get(), ts_precision_));
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result,
                               RangeQuery(from_slice, to_slice, true, true));
        return result;
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitNotBetween(
    const Literal& from, const Literal& to) {
    return std::make_shared<BitmapGlobalIndexResult>([this, &from,
                                                      &to]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(auto from_slice,
                               LiteralToMemorySlice(from, pool_.get(), ts_precision_));
        PAIMON_ASSIGN_OR_RAISE(auto to_slice, LiteralToMemorySlice(to, pool_.get(), ts_precision_));
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 lower_result,
                               RangeQuery(min_key_, from_slice, true, false));
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 upper_result,
                               RangeQuery(to_slice, max_key_, false, true));
        lower_result |= upper_result;
        return lower_result;
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

        RoaringBitmap64 result_bitmap;
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

            RoaringBitmap64 child_bitmap;
            while (child_iterator->HasNext()) {
                child_bitmap.Add(child_iterator->Next());
            }

            result_bitmap &= child_bitmap;
        }

        return result_bitmap;
    });
}

Result<std::shared_ptr<GlobalIndexResult>> BTreeGlobalIndexReader::VisitOr(
    const std::vector<Result<std::shared_ptr<GlobalIndexResult>>>& children) {
    return std::make_shared<BitmapGlobalIndexResult>([&children]() -> Result<RoaringBitmap64> {
        RoaringBitmap64 result_bitmap;

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

        return result_bitmap;
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

Result<RoaringBitmap64> BTreeGlobalIndexReader::RangeQuery(const MemorySlice& lower_bound,
                                                           const MemorySlice& upper_bound,
                                                           bool lower_inclusive,
                                                           bool upper_inclusive) {
    RoaringBitmap64 result;

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

    // Compare key with bounds using the comparator
    if (!comparator_) {
        return Status::Invalid("Comparator is not set for BTreeGlobalIndexReader");
    }

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
            int cmp_lower = comparator_(entry->key, lower_bound);

            // Check lower bound
            if (!lower_inclusive && cmp_lower == 0) {
                continue;
            }

            // Check upper bound
            int cmp_upper = comparator_(entry->key, upper_bound);

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

Result<RoaringBitmap64> BTreeGlobalIndexReader::AllNonNullRows() {
    if (files_.empty()) {
        return RoaringBitmap64();
    }

    int64_t total_rows = files_[0].range_end + 1;
    uint64_t null_count = null_bitmap_->Cardinality();

    const double NULL_RATIO_THRESHOLD = 0.1;
    const int64_t MAX_ROWS_FOR_SUBTRACTION = 10000000;

    bool use_subtraction = (total_rows <= MAX_ROWS_FOR_SUBTRACTION) &&
                           (null_count < static_cast<uint64_t>(total_rows * NULL_RATIO_THRESHOLD));

    if (use_subtraction) {
        RoaringBitmap64 result;
        result.AddRange(0, total_rows);
        result -= *null_bitmap_;
        return result;
    }

    if (!has_min_key_) {
        return RoaringBitmap64();
    }
    return RangeQuery(min_key_, max_key_, true, true);
}

}  // namespace paimon
