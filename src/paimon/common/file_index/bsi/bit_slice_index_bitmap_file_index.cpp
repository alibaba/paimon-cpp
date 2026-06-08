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

#include "paimon/common/file_index/bsi/bit_slice_index_bitmap_file_index.h"

#include <cassert>
#include <climits>
#include <cstddef>
#include <cstdint>

#include "fmt/format.h"
#include "paimon/common/file_index/bsi/bit_slice_index_roaring_bitmap.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/utils/roaring_bitmap32.h"
namespace {
// Safe absolute value for int64_t that avoids undefined behavior when value == INT64_MIN.
// This mirrors Java's Math.abs() wrapping semantics.
inline int64_t SafeAbs(int64_t value) {
    if (value == INT64_MIN) {
        return INT64_MIN;
    }
    return value < 0 ? -value : value;
}

}  // namespace
namespace paimon {
class MemoryPool;

BitSliceIndexBitmapFileIndex::BitSliceIndexBitmapFileIndex(
    const std::map<std::string, std::string>& options) {}

Result<std::shared_ptr<FileIndexReader>> BitSliceIndexBitmapFileIndex::CreateReader(
    ::ArrowSchema* c_arrow_schema, int32_t start, int32_t length,
    const std::shared_ptr<InputStream>& input_stream,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(c_arrow_schema));
    if (arrow_schema->num_fields() != 1) {
        return Status::Invalid(
            "invalid schema for BitSliceIndexBitmapFileIndexReader, supposed to have single "
            "field.");
    }
    auto arrow_type = arrow_schema->field(0)->type();

    PAIMON_RETURN_NOT_OK(input_stream->Seek(start, SeekOrigin::FS_SEEK_SET));
    auto bytes = std::make_unique<Bytes>(length, pool.get());
    PAIMON_ASSIGN_OR_RAISE(int32_t actual_read_len,
                           input_stream->Read(bytes->data(), bytes->size()));
    if (static_cast<size_t>(actual_read_len) != bytes->size()) {
        return Status::Invalid(
            fmt::format("create reader for BitSliceIndexBitmapFileIndex failed, expected read len "
                        "{}, actual read len {}",
                        bytes->size(), actual_read_len));
    }
    auto byte_array_input_stream =
        std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    DataInputStream data_input_stream(byte_array_input_stream);
    PAIMON_ASSIGN_OR_RAISE(int8_t version, data_input_stream.ReadValue<int8_t>());
    if (version > VERSION_1) {
        return Status::Invalid(fmt::format(
            "read bsi index file fail, do not support version {}, please update plugin version",
            version));
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t row_number, data_input_stream.ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(bool has_positive, data_input_stream.ReadValue<bool>());
    std::shared_ptr<BitSliceIndexRoaringBitmap> positive = BitSliceIndexRoaringBitmap::Empty();
    if (has_positive) {
        PAIMON_ASSIGN_OR_RAISE(positive,
                               BitSliceIndexRoaringBitmap::Create(byte_array_input_stream));
    }
    PAIMON_ASSIGN_OR_RAISE(bool has_negative, data_input_stream.ReadValue<bool>());
    std::shared_ptr<BitSliceIndexRoaringBitmap> negative = BitSliceIndexRoaringBitmap::Empty();
    if (has_negative) {
        PAIMON_ASSIGN_OR_RAISE(negative,
                               BitSliceIndexRoaringBitmap::Create(byte_array_input_stream));
    }
    PAIMON_ASSIGN_OR_RAISE(BitSliceIndexBitmapFileIndex::ValueMapperType value_mapper,
                           GetValueMapper(arrow_type));
    return std::make_shared<BitSliceIndexBitmapFileIndexReader>(row_number, value_mapper, positive,
                                                                negative);
}

// precondition, literal is not null
Result<BitSliceIndexBitmapFileIndex::ValueMapperType> BitSliceIndexBitmapFileIndex::GetValueMapper(
    const std::shared_ptr<arrow::DataType>& arrow_type) {
    PAIMON_ASSIGN_OR_RAISE(FieldType field_type,
                           FieldTypeUtils::ConvertToFieldType(arrow_type->id()));
    switch (field_type) {
        case FieldType::TINYINT:
            return BitSliceIndexBitmapFileIndex::ValueMapperType(
                [](const Literal& literal) -> Result<int64_t> {
                    return GetValueFromLiteral<int8_t>(literal);
                });
        case FieldType::SMALLINT:
            return BitSliceIndexBitmapFileIndex::ValueMapperType(
                [](const Literal& literal) -> Result<int64_t> {
                    return GetValueFromLiteral<int16_t>(literal);
                });
        case FieldType::DATE:
        case FieldType::INT:
            return BitSliceIndexBitmapFileIndex::ValueMapperType(
                [](const Literal& literal) -> Result<int64_t> {
                    return GetValueFromLiteral<int32_t>(literal);
                });
        case FieldType::BIGINT:
            return BitSliceIndexBitmapFileIndex::ValueMapperType(
                [](const Literal& literal) -> Result<int64_t> {
                    return GetValueFromLiteral<int64_t>(literal);
                });
        case FieldType::TIMESTAMP: {
            auto ts_type = arrow::internal::checked_pointer_cast<arrow::TimestampType>(arrow_type);
            int64_t precision = DateTimeUtils::GetPrecisionFromType(ts_type);
            assert(precision >= 0);
            return BitSliceIndexBitmapFileIndex::ValueMapperType(
                [precision](const Literal& literal) -> Result<int64_t> {
                    if (literal.IsNull()) {
                        return Status::Invalid(
                            "literal cannot be null when GetValue in BitSliceIndexBitmapFileIndex");
                    }
                    if (precision <= Timestamp::MILLIS_PRECISION) {
                        return literal.GetValue<Timestamp>().GetMillisecond();
                    }
                    return literal.GetValue<Timestamp>().ToMicrosecond();
                });
        }
        default:
            // TODO(xinyu.lxy): support decimal
            return Status::Invalid(
                "BitSliceIndexBitmapFileIndex only support TINYINT/SMALLINT/INT/BIGINT/DATE");
    }
}

BitSliceIndexBitmapFileIndexReader::BitSliceIndexBitmapFileIndexReader(
    int32_t row_number, const BitSliceIndexBitmapFileIndex::ValueMapperType& value_mapper,
    const std::shared_ptr<BitSliceIndexRoaringBitmap>& positive,
    const std::shared_ptr<BitSliceIndexRoaringBitmap>& negative)
    : row_number_(row_number),
      value_mapper_(value_mapper),
      positive_(positive),
      negative_(negative) {}

Result<std::shared_ptr<FileIndexResult>> BitSliceIndexBitmapFileIndexReader::VisitGreaterThan(
    const Literal& literal) {
    BitmapIndexResult::BitmapSupplier bitmap_supplier =
        [literal = literal, reader = shared_from_this()]() -> Result<RoaringBitmap32> {
        PAIMON_ASSIGN_OR_RAISE(int64_t value, reader->value_mapper_(literal));
        if (value == INT64_MIN) {
            // Everything is greater than INT64_MIN (writer cannot store it)
            return RoaringBitmap32::Or(reader->positive_->IsNotNull(),
                                       reader->negative_->IsNotNull());
        } else if (value >= 0) {
            return reader->positive_->GreaterThan(value);
        } else {
            PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 b1, reader->negative_->LessThan(SafeAbs(value)));
            RoaringBitmap32 b2 = reader->positive_->IsNotNull();
            b1 |= b2;
            return b1;
        }
    };
    return std::make_shared<BitmapIndexResult>(bitmap_supplier);
}

Result<std::shared_ptr<FileIndexResult>> BitSliceIndexBitmapFileIndexReader::VisitGreaterOrEqual(
    const Literal& literal) {
    BitmapIndexResult::BitmapSupplier bitmap_supplier =
        [literal = literal, reader = shared_from_this()]() -> Result<RoaringBitmap32> {
        PAIMON_ASSIGN_OR_RAISE(int64_t value, reader->value_mapper_(literal));
        if (value == INT64_MIN) {
            // All non-null rows satisfy x >= INT64_MIN
            return RoaringBitmap32::Or(reader->positive_->IsNotNull(),
                                       reader->negative_->IsNotNull());
        } else if (value >= 0) {
            return reader->positive_->GreaterOrEqual(value);
        } else {
            PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 b1,
                                   reader->negative_->LessOrEqual(SafeAbs(value)));
            RoaringBitmap32 b2 = reader->positive_->IsNotNull();
            b1 |= b2;
            return b1;
        }
    };
    return std::make_shared<BitmapIndexResult>(bitmap_supplier);
}

Result<std::shared_ptr<FileIndexResult>> BitSliceIndexBitmapFileIndexReader::VisitLessThan(
    const Literal& literal) {
    BitmapIndexResult::BitmapSupplier bitmap_supplier =
        [literal = literal, reader = shared_from_this()]() -> Result<RoaringBitmap32> {
        PAIMON_ASSIGN_OR_RAISE(int64_t value, reader->value_mapper_(literal));
        if (value == INT64_MIN) {
            // Nothing is less than INT64_MIN
            return RoaringBitmap32();
        } else if (value < 0) {
            return reader->negative_->GreaterThan(SafeAbs(value));
        } else {
            PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 b1, reader->positive_->LessThan(value));
            RoaringBitmap32 b2 = reader->negative_->IsNotNull();
            b1 |= b2;
            return b1;
        }
    };
    return std::make_shared<BitmapIndexResult>(bitmap_supplier);
}
Result<std::shared_ptr<FileIndexResult>> BitSliceIndexBitmapFileIndexReader::VisitLessOrEqual(
    const Literal& literal) {
    BitmapIndexResult::BitmapSupplier bitmap_supplier =
        [literal = literal, reader = shared_from_this()]() -> Result<RoaringBitmap32> {
        PAIMON_ASSIGN_OR_RAISE(int64_t value, reader->value_mapper_(literal));
        if (value == INT64_MIN) {
            // Writer cannot store INT64_MIN, so no row can match
            return RoaringBitmap32();
        } else if (value < 0) {
            return reader->negative_->GreaterOrEqual(SafeAbs(value));
        } else {
            PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 b1, reader->positive_->LessOrEqual(value));
            RoaringBitmap32 b2 = reader->negative_->IsNotNull();
            b1 |= b2;
            return b1;
        }
    };
    return std::make_shared<BitmapIndexResult>(bitmap_supplier);
}

Result<std::shared_ptr<FileIndexResult>> BitSliceIndexBitmapFileIndexReader::VisitEqual(
    const Literal& literal) {
    return VisitIn({literal});
}
Result<std::shared_ptr<FileIndexResult>> BitSliceIndexBitmapFileIndexReader::VisitNotEqual(
    const Literal& literal) {
    return VisitNotIn({literal});
}

Result<std::shared_ptr<FileIndexResult>> BitSliceIndexBitmapFileIndexReader::VisitIn(
    const std::vector<Literal>& literals) {
    BitmapIndexResult::BitmapSupplier bitmap_supplier =
        [literals = literals, reader = shared_from_this()]() -> Result<RoaringBitmap32> {
        std::vector<RoaringBitmap32> result_bitmaps;
        result_bitmaps.reserve(literals.size());
        for (const auto& literal : literals) {
            PAIMON_ASSIGN_OR_RAISE(int64_t value, reader->value_mapper_(literal));
            if (value == INT64_MIN) {
                // Writer cannot store INT64_MIN, so no row can match it
                result_bitmaps.emplace_back();
            } else if (value < 0) {
                PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 equal,
                                       reader->negative_->Equal(SafeAbs(value)));
                result_bitmaps.emplace_back(std::move(equal));
            } else {
                PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 equal, reader->positive_->Equal(value));
                result_bitmaps.emplace_back(std::move(equal));
            }
        }
        return RoaringBitmap32::FastUnion(result_bitmaps);
    };
    return std::make_shared<BitmapIndexResult>(bitmap_supplier);
}

Result<std::shared_ptr<FileIndexResult>> BitSliceIndexBitmapFileIndexReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    BitmapIndexResult::BitmapSupplier bitmap_supplier =
        [literals = literals, reader = shared_from_this()]() -> Result<RoaringBitmap32> {
        auto ebm =
            RoaringBitmap32::Or(reader->positive_->IsNotNull(), reader->negative_->IsNotNull());
        std::vector<RoaringBitmap32> result_bitmaps;
        result_bitmaps.reserve(literals.size());
        for (const auto& literal : literals) {
            PAIMON_ASSIGN_OR_RAISE(int64_t value, reader->value_mapper_(literal));
            if (value == INT64_MIN) {
                // Writer cannot store INT64_MIN, so no row can match it
                result_bitmaps.emplace_back();
            } else if (value < 0) {
                PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 equal,
                                       reader->negative_->Equal(SafeAbs(value)));
                result_bitmaps.emplace_back(std::move(equal));
            } else {
                PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 equal, reader->positive_->Equal(value));
                result_bitmaps.emplace_back(std::move(equal));
            }
        }
        auto in = RoaringBitmap32::FastUnion(result_bitmaps);
        ebm -= in;
        return ebm;
    };
    return std::make_shared<BitmapIndexResult>(bitmap_supplier);
}

Result<std::shared_ptr<FileIndexResult>> BitSliceIndexBitmapFileIndexReader::VisitIsNull() {
    BitmapIndexResult::BitmapSupplier bitmap_supplier =
        [reader = shared_from_this()]() -> Result<RoaringBitmap32> {
        auto res =
            RoaringBitmap32::Or(reader->positive_->IsNotNull(), reader->negative_->IsNotNull());
        res.Flip(0, reader->row_number_);
        return res;
    };
    return std::make_shared<BitmapIndexResult>(bitmap_supplier);
}

Result<std::shared_ptr<FileIndexResult>> BitSliceIndexBitmapFileIndexReader::VisitIsNotNull() {
    BitmapIndexResult::BitmapSupplier bitmap_supplier =
        [reader = shared_from_this()]() -> Result<RoaringBitmap32> {
        return RoaringBitmap32::Or(reader->positive_->IsNotNull(), reader->negative_->IsNotNull());
    };
    return std::make_shared<BitmapIndexResult>(bitmap_supplier);
}

}  // namespace paimon
