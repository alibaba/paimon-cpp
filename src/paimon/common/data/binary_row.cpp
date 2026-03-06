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

#include "paimon/common/data/binary_row.h"

#include <cstdint>

#include "paimon/common/data/binary_data_read_utils.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/io/byte_order.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {
const int64_t BinaryRow::FIRST_BYTE_ZERO =
    (SystemByteOrder() == ByteOrder::PAIMON_LITTLE_ENDIAN) ? (~0xFFL) : (~(0xFFL << 56L));

const BinaryRow& BinaryRow::EmptyRow() {
    static const BinaryRow empty_row = GetEmptyRow();
    return empty_row;
}

BinaryRow::BinaryRow(int32_t arity)
    : arity_(arity), null_bits_size_in_bytes_(CalculateBitSetWidthInBytes(arity)) {
    assert(arity_ >= 0);
}

int32_t BinaryRow::CalculateBitSetWidthInBytes(int32_t arity) {
    return ((arity + 63 + HEADER_SIZE_IN_BITS) / 64) * 8;
}

int32_t BinaryRow::CalculateFixPartSizeInBytes(int32_t arity) {
    return CalculateBitSetWidthInBytes(arity) + 8 * arity;
}

int32_t BinaryRow::GetFieldOffset(int32_t pos) const {
    return offset_ + null_bits_size_in_bytes_ + pos * 8;
}

void BinaryRow::AssertIndexIsValid(int32_t index) const {
    assert(index >= 0);
    assert(index < arity_);
}

int32_t BinaryRow::GetFixedLengthPartSize() const {
    return null_bits_size_in_bytes_ + 8 * arity_;
}

BinaryRow BinaryRow::GetEmptyRow() {
    BinaryRow row(0);
    int32_t size = row.GetFixedLengthPartSize();
    auto bytes = Bytes::AllocateBytes(size, GetDefaultPool().get());
    row.PointTo(MemorySegment::Wrap(std::move(bytes)), 0, size);
    return row;
}

Result<const RowKind*> BinaryRow::GetRowKind() const {
    char kind_value = MemorySegmentUtils::GetValue<char>(segments_, offset_);
    return RowKind::FromByteValue(kind_value);
}

void BinaryRow::SetRowKind(const RowKind* kind) {
    MemorySegmentUtils::SetValue<char>(&segments_, offset_, kind->ToByteValue());
}

void BinaryRow::SetTotalSize(int32_t size_in_bytes) {
    size_in_bytes_ = size_in_bytes;
}

bool BinaryRow::IsNullAt(int32_t pos) const {
    AssertIndexIsValid(pos);
    return MemorySegmentUtils::BitGet(segments_, offset_, pos + HEADER_SIZE_IN_BITS);
}

void BinaryRow::SetNotNullAt(int32_t i) {
    AssertIndexIsValid(i);
    MemorySegmentUtils::BitUnSet(&segments_, offset_, i + HEADER_SIZE_IN_BITS);
}

void BinaryRow::SetNullAt(int32_t i) {
    AssertIndexIsValid(i);
    MemorySegmentUtils::BitSet(&segments_, offset_, i + HEADER_SIZE_IN_BITS);
    // We must set the fixed length part zero.
    // 1.Only int/long/boolean...(Fix length type) will invoke this SetNullAt.
    // 2.Set to zero in order to equals and hash operation bytes calculation.
    MemorySegmentUtils::SetValue<int64_t>(&segments_, GetFieldOffset(i), 0);
}

void BinaryRow::SetInt(int32_t pos, int32_t value) {
    AssertIndexIsValid(pos);
    SetNotNullAt(pos);
    MemorySegmentUtils::SetValue<int32_t>(&segments_, GetFieldOffset(pos), value);
}

void BinaryRow::SetLong(int32_t pos, int64_t value) {
    AssertIndexIsValid(pos);
    SetNotNullAt(pos);
    MemorySegmentUtils::SetValue<int64_t>(&segments_, GetFieldOffset(pos), value);
}

void BinaryRow::SetDouble(int32_t pos, double value) {
    AssertIndexIsValid(pos);
    SetNotNullAt(pos);
    MemorySegmentUtils::SetValue<double>(&segments_, GetFieldOffset(pos), value);
}

void BinaryRow::SetBoolean(int32_t pos, bool value) {
    AssertIndexIsValid(pos);
    SetNotNullAt(pos);
    MemorySegmentUtils::SetValue<bool>(&segments_, GetFieldOffset(pos), value);
}

void BinaryRow::SetShort(int32_t pos, int16_t value) {
    AssertIndexIsValid(pos);
    SetNotNullAt(pos);
    MemorySegmentUtils::SetValue<int16_t>(&segments_, GetFieldOffset(pos), value);
}

void BinaryRow::SetByte(int32_t pos, char value) {
    AssertIndexIsValid(pos);
    SetNotNullAt(pos);
    MemorySegmentUtils::SetValue<char>(&segments_, GetFieldOffset(pos), value);
}

void BinaryRow::SetFloat(int32_t pos, float value) {
    AssertIndexIsValid(pos);
    SetNotNullAt(pos);
    MemorySegmentUtils::SetValue<float>(&segments_, GetFieldOffset(pos), value);
}

bool BinaryRow::GetBoolean(int32_t pos) const {
    AssertIndexIsValid(pos);
    return MemorySegmentUtils::GetValue<bool>(segments_, GetFieldOffset(pos));
}

char BinaryRow::GetByte(int32_t pos) const {
    AssertIndexIsValid(pos);
    return MemorySegmentUtils::GetValue<char>(segments_, GetFieldOffset(pos));
}

int16_t BinaryRow::GetShort(int32_t pos) const {
    AssertIndexIsValid(pos);
    return MemorySegmentUtils::GetValue<int16_t>(segments_, GetFieldOffset(pos));
}

int32_t BinaryRow::GetInt(int32_t pos) const {
    AssertIndexIsValid(pos);
    return MemorySegmentUtils::GetValue<int32_t>(segments_, GetFieldOffset(pos));
}

int32_t BinaryRow::GetDate(int32_t pos) const {
    return GetInt(pos);
}

int64_t BinaryRow::GetLong(int32_t pos) const {
    AssertIndexIsValid(pos);
    return MemorySegmentUtils::GetValue<int64_t>(segments_, GetFieldOffset(pos));
}

float BinaryRow::GetFloat(int32_t pos) const {
    AssertIndexIsValid(pos);
    return MemorySegmentUtils::GetValue<float>(segments_, GetFieldOffset(pos));
}

double BinaryRow::GetDouble(int32_t pos) const {
    AssertIndexIsValid(pos);
    return MemorySegmentUtils::GetValue<double>(segments_, GetFieldOffset(pos));
}

BinaryString BinaryRow::GetString(int32_t pos) const {
    AssertIndexIsValid(pos);
    int32_t field_offset = GetFieldOffset(pos);
    const auto offset_and_len = MemorySegmentUtils::GetValue<int64_t>(segments_, field_offset);
    return BinaryDataReadUtils::ReadBinaryString(segments_, offset_, field_offset, offset_and_len);
}

Decimal BinaryRow::GetDecimal(int32_t pos, int32_t precision, int32_t scale) const {
    AssertIndexIsValid(pos);
    int32_t field_offset = GetFieldOffset(pos);
    if (Decimal::IsCompact(precision)) {
        return Decimal::FromUnscaledLong(
            MemorySegmentUtils::GetValue<int64_t>(segments_, field_offset), precision, scale);
    }
    const auto offset_and_size = MemorySegmentUtils::GetValue<int64_t>(segments_, field_offset);
    return BinaryDataReadUtils::ReadDecimal(segments_, offset_, offset_and_size, precision, scale);
}

Timestamp BinaryRow::GetTimestamp(int32_t pos, int32_t precision) const {
    AssertIndexIsValid(pos);
    int32_t field_offset = GetFieldOffset(pos);
    if (Timestamp::IsCompact(precision)) {
        return Timestamp::FromEpochMillis(
            MemorySegmentUtils::GetValue<int64_t>(segments_, field_offset));
    }
    const auto offset_and_nano_of_milli =
        MemorySegmentUtils::GetValue<int64_t>(segments_, field_offset);
    return BinaryDataReadUtils::ReadTimestampData(segments_, offset_, offset_and_nano_of_milli);
}

std::shared_ptr<Bytes> BinaryRow::GetBinary(int32_t pos) const {
    AssertIndexIsValid(pos);
    int32_t field_offset = GetFieldOffset(pos);
    const auto offset_and_len = MemorySegmentUtils::GetValue<int64_t>(segments_, field_offset);
    return BinarySection::ReadBinary(segments_, offset_, field_offset, offset_and_len,
                                     GetDefaultPool().get());
}

std::shared_ptr<InternalArray> BinaryRow::GetArray(int32_t pos) const {
    AssertIndexIsValid(pos);
    return BinaryDataReadUtils::ReadArrayData(segments_, offset_, GetLong(pos));
}

std::shared_ptr<InternalMap> BinaryRow::GetMap(int32_t pos) const {
    AssertIndexIsValid(pos);
    return BinaryDataReadUtils::ReadMapData(segments_, offset_, GetLong(pos));
}

std::shared_ptr<InternalRow> BinaryRow::GetRow(int32_t pos, int32_t num_fields) const {
    AssertIndexIsValid(pos);
    return BinaryDataReadUtils::ReadRowData(segments_, num_fields, offset_, GetLong(pos));
}

/// The bit is 1 when the field is null. Default is 0.
bool BinaryRow::AnyNull() const {
    // Skip the header.
    if ((MemorySegmentUtils::GetValue<int64_t>(segments_, offset_) & FIRST_BYTE_ZERO) != 0) {
        return true;
    }
    for (int32_t i = 8; i < null_bits_size_in_bytes_; i += 8) {
        if (MemorySegmentUtils::GetValue<int64_t>(segments_, offset_ + i) != 0) {
            return true;
        }
    }
    return false;
}

bool BinaryRow::AnyNull(const std::vector<int32_t>& fields) const {
    for (int32_t field : fields) {
        if (IsNullAt(field)) {
            return true;
        }
    }
    return false;
}

BinaryRow BinaryRow::Copy(MemoryPool* pool) const {
    BinaryRow row(arity_);
    Copy(&row, pool);
    return row;
}

void BinaryRow::Copy(BinaryRow* reuse, MemoryPool* pool) const {
    CopyInternal(reuse, pool);
}

void BinaryRow::CopyInternal(BinaryRow* reuse, MemoryPool* pool) const {
    std::shared_ptr<Bytes> bytes =
        MemorySegmentUtils::CopyToBytes(segments_, offset_, size_in_bytes_, pool);
    reuse->PointTo(MemorySegment::Wrap(bytes), 0, size_in_bytes_);
}

void BinaryRow::Clear() {
    segments_.clear();
    offset_ = 0;
    size_in_bytes_ = 0;
}

bool BinaryRow::operator==(const BinaryRow& other) const {
    if (this == &other) {
        return true;
    }
    return size_in_bytes_ == other.size_in_bytes_ &&
           MemorySegmentUtils::Equals(segments_, offset_, other.segments_, other.offset_,
                                      size_in_bytes_);
}

int32_t BinaryRow::HashCode() const {
    assert(segments_.size() > 0);
    if (segments_.size() == 1) {
        return MemorySegmentUtils::HashByWords(segments_, offset_, size_in_bytes_, nullptr);
    }
    return MemorySegmentUtils::HashByWords(segments_, offset_, size_in_bytes_,
                                           GetDefaultPool().get());
}

}  // namespace paimon
