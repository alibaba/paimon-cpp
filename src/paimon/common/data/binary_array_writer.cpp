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

#include "paimon/common/data/binary_array_writer.h"

#include <memory>
#include <utility>

#include "paimon/common/data/binary_array.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/memory/bytes.h"

namespace paimon {
class MemoryPool;

BinaryArrayWriter::BinaryArrayWriter(BinaryArray* array, int32_t num_elements, int32_t element_size,
                                     MemoryPool* pool) {
    null_bits_size_in_bytes_ = BinaryArray::CalculateHeaderInBytes(num_elements);
    fixed_size_ =
        RoundNumberOfBytesToNearestWord(null_bits_size_in_bytes_ + element_size * num_elements);
    cursor_ = fixed_size_;
    num_elements_ = num_elements;
    auto bytes = Bytes::AllocateBytes(fixed_size_, pool);
    segment_ = MemorySegment::Wrap(std::move(bytes));
    segment_.PutValue<int32_t>(0, num_elements);
    array_ = array;
    pool_ = pool;
}

void BinaryArrayWriter::Reset() {
    cursor_ = fixed_size_;
    for (int32_t i = 0; i < null_bits_size_in_bytes_; i += 8) {
        segment_.PutValue<int64_t>(i, 0L);
    }
    segment_.PutValue<int32_t>(0, num_elements_);
}

void BinaryArrayWriter::SetNullBit(int32_t ordinal) {
    MemorySegmentUtils::BitSet(&segment_, 4, ordinal);
}

void BinaryArrayWriter::SetNullAt(int32_t ordinal) {
    SetNullValue<int64_t>(ordinal);
}

void BinaryArrayWriter::SetNullAt(int32_t ordinal, const arrow::Type::type& arrow_type) {
    switch (arrow_type) {
        case arrow::Type::type::BOOL:
            SetNullValue<bool>(ordinal);
            return;
        case arrow::Type::type::INT8:
            SetNullValue<int8_t>(ordinal);
            return;
        case arrow::Type::type::INT16:
            SetNullValue<int16_t>(ordinal);
            return;
        case arrow::Type::type::DATE32:
        case arrow::Type::type::INT32:
            SetNullValue<int32_t>(ordinal);
            return;
        case arrow::Type::type::INT64:
            SetNullValue<int64_t>(ordinal);
            return;
        case arrow::Type::type::FLOAT:
            SetNullValue<float>(ordinal);
            return;
        case arrow::Type::type::DOUBLE:
            SetNullValue<double>(ordinal);
            return;
        default:
            SetNullAt(ordinal);
            return;
    }
}

int32_t BinaryArrayWriter::GetElementOffset(int32_t pos, int32_t element_size) const {
    return null_bits_size_in_bytes_ + element_size * pos;
}

int32_t BinaryArrayWriter::GetFieldOffset(int32_t pos) const {
    return GetElementOffset(pos, 8);
}

void BinaryArrayWriter::SetOffsetAndSize(int32_t pos, int32_t offset, int64_t size) {
    const int64_t offset_and_size = (static_cast<int64_t>(offset) << 32) | size;
    segment_.PutValue<int64_t>(GetElementOffset(pos, 8), offset_and_size);
}

void BinaryArrayWriter::AfterGrow() {
    array_->PointTo(segment_, 0, segment_.Size());
}

void BinaryArrayWriter::Complete() {
    array_->PointTo(segment_, 0, cursor_);
}

int32_t BinaryArrayWriter::GetNumElements() const {
    return num_elements_;
}

int32_t BinaryArrayWriter::GetElementSize(const arrow::Type::type& arrow_type) {
    switch (arrow_type) {
        case arrow::Type::type::BOOL:
            return sizeof(bool);
        case arrow::Type::type::INT8:
            return sizeof(int8_t);
        case arrow::Type::type::INT16:
            return sizeof(int16_t);
        case arrow::Type::type::DATE32:
        case arrow::Type::type::INT32:
            return sizeof(int32_t);
        case arrow::Type::type::INT64:
            return sizeof(int64_t);
        case arrow::Type::type::FLOAT:
            return sizeof(float);
        case arrow::Type::type::DOUBLE:
            return sizeof(double);
        default:
            return 8;
    }
}

}  // namespace paimon
