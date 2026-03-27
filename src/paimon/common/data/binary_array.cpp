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

#include "paimon/common/data/binary_array.h"

#include <cassert>
#include <cstddef>
#include <utility>

#include "paimon/common/data/binary_array_writer.h"
#include "paimon/common/data/binary_data_read_utils.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {

int32_t BinaryArray::CalculateHeaderInBytes(int32_t num_fields) {
    return 4 + ((num_fields + 31) / 32) * 4;
}

void BinaryArray::AssertIndexIsValid(int32_t ordinal) const {
    assert(ordinal >= 0);
    assert(ordinal < size_);
}
int32_t BinaryArray::GetElementOffset(int32_t ordinal, int32_t element_size) const {
    return element_offset_ + ordinal * element_size;
}

void BinaryArray::PointTo(const MemorySegment& segment, int32_t offset, int32_t size_in_bytes) {
    auto size = MemorySegmentUtils::GetValue<int32_t>({segment}, offset);
    assert(size >= 0);
    size_ = size;
    segment_ = segment;
    offset_ = offset;
    size_in_bytes_ = size_in_bytes;
    element_offset_ = offset_ + CalculateHeaderInBytes(size_);
}

bool BinaryArray::IsNullAt(int32_t pos) const {
    AssertIndexIsValid(pos);
    return MemorySegmentUtils::BitGet({segment_}, offset_ + 4, pos);
}

int64_t BinaryArray::GetLong(int32_t pos) const {
    AssertIndexIsValid(pos);
    return MemorySegmentUtils::GetValue<int64_t>({segment_}, GetElementOffset(pos, 8));
}

int32_t BinaryArray::GetInt(int32_t pos) const {
    AssertIndexIsValid(pos);
    return MemorySegmentUtils::GetValue<int32_t>({segment_}, GetElementOffset(pos, 4));
}
int32_t BinaryArray::GetDate(int32_t pos) const {
    return GetInt(pos);
}

BinaryString BinaryArray::GetString(int32_t pos) const {
    AssertIndexIsValid(pos);
    int32_t field_offset = GetElementOffset(pos, 8);
    const auto offset_and_size = MemorySegmentUtils::GetValue<int64_t>({segment_}, field_offset);
    return BinaryDataReadUtils::ReadBinaryString(segment_, offset_, field_offset, offset_and_size);
}

std::string_view BinaryArray::GetStringView(int32_t pos) const {
    BinaryString binary_string = GetString(pos);
    return binary_string.GetStringView();
}

Decimal BinaryArray::GetDecimal(int32_t pos, int32_t precision, int32_t scale) const {
    AssertIndexIsValid(pos);
    if (Decimal::IsCompact(precision)) {
        return Decimal::FromUnscaledLong(
            MemorySegmentUtils::GetValue<int64_t>({segment_}, GetElementOffset(pos, 8)), precision,
            scale);
    }

    int32_t field_offset = GetElementOffset(pos, 8);
    const auto offset_and_size = MemorySegmentUtils::GetValue<int64_t>({segment_}, field_offset);
    return BinaryDataReadUtils::ReadDecimal(segment_, offset_, offset_and_size, precision, scale);
}

Timestamp BinaryArray::GetTimestamp(int32_t pos, int32_t precision) const {
    AssertIndexIsValid(pos);

    if (Timestamp::IsCompact(precision)) {
        return Timestamp::FromEpochMillis(
            MemorySegmentUtils::GetValue<int64_t>({segment_}, GetElementOffset(pos, 8)));
    }

    int32_t field_offset = GetElementOffset(pos, 8);
    const auto offset_and_nano_of_milli =
        MemorySegmentUtils::GetValue<int64_t>({segment_}, field_offset);
    return BinaryDataReadUtils::ReadTimestampData(segment_, offset_, offset_and_nano_of_milli);
}

std::shared_ptr<Bytes> BinaryArray::GetBinary(int32_t pos) const {
    AssertIndexIsValid(pos);
    int32_t field_offset = GetElementOffset(pos, 8);
    const auto offset_and_size = MemorySegmentUtils::GetValue<int64_t>({segment_}, field_offset);
    return BinarySection::ReadBinary(segment_, offset_, field_offset, offset_and_size,
                                     GetDefaultPool().get());
}

std::shared_ptr<InternalArray> BinaryArray::GetArray(int32_t pos) const {
    AssertIndexIsValid(pos);
    return BinaryDataReadUtils::ReadArrayData(segment_, offset_, GetLong(pos));
}

std::shared_ptr<InternalMap> BinaryArray::GetMap(int32_t pos) const {
    AssertIndexIsValid(pos);
    return BinaryDataReadUtils::ReadMapData(segment_, offset_, GetLong(pos));
}

std::shared_ptr<InternalRow> BinaryArray::GetRow(int32_t pos, int32_t num_fields) const {
    AssertIndexIsValid(pos);
    int32_t field_offset = GetElementOffset(pos, 8);
    const auto offset_and_size = MemorySegmentUtils::GetValue<int64_t>({segment_}, field_offset);
    return BinaryDataReadUtils::ReadRowData(segment_, num_fields, offset_, offset_and_size);
}

bool BinaryArray::GetBoolean(int32_t pos) const {
    AssertIndexIsValid(pos);
    return MemorySegmentUtils::GetValue<bool>({segment_}, GetElementOffset(pos, 1));
}

char BinaryArray::GetByte(int32_t pos) const {
    AssertIndexIsValid(pos);
    return MemorySegmentUtils::GetValue<char>({segment_}, GetElementOffset(pos, 1));
}

int16_t BinaryArray::GetShort(int32_t pos) const {
    AssertIndexIsValid(pos);
    return MemorySegmentUtils::GetValue<int16_t>({segment_},
                                                 GetElementOffset(pos, sizeof(int16_t)));
}

float BinaryArray::GetFloat(int32_t pos) const {
    AssertIndexIsValid(pos);
    return MemorySegmentUtils::GetValue<float>({segment_}, GetElementOffset(pos, sizeof(float)));
}

double BinaryArray::GetDouble(int32_t pos) const {
    AssertIndexIsValid(pos);
    return MemorySegmentUtils::GetValue<double>({segment_}, GetElementOffset(pos, sizeof(double)));
}

bool BinaryArray::AnyNull() const {
    for (int32_t i = offset_ + 4; i < element_offset_; i += 4) {
        if (MemorySegmentUtils::GetValue<int32_t>({segment_}, i) != 0) {
            return true;
        }
    }
    return false;
}

Result<std::vector<char>> BinaryArray::ToBooleanArray() const {
    PAIMON_RETURN_NOT_OK(CheckNoNull());
    std::vector<char> values;
    values.resize(size_);
    MemorySegmentUtils::CopyToUnsafe({segment_}, element_offset_,
                                     const_cast<void*>(static_cast<const void*>(values.data())),
                                     size_);
    return values;
}

Result<std::vector<char>> BinaryArray::ToByteArray() const {
    PAIMON_RETURN_NOT_OK(CheckNoNull());
    std::vector<char> values;
    values.resize(size_);
    MemorySegmentUtils::CopyToUnsafe({segment_}, element_offset_,
                                     const_cast<void*>(static_cast<const void*>(values.data())),
                                     size_);
    return values;
}

Result<std::vector<int16_t>> BinaryArray::ToShortArray() const {
    PAIMON_RETURN_NOT_OK(CheckNoNull());
    std::vector<int16_t> values;
    values.resize(size_);
    MemorySegmentUtils::CopyToUnsafe({segment_}, element_offset_,
                                     const_cast<void*>(static_cast<const void*>(values.data())),
                                     size_ * sizeof(int16_t));
    return values;
}

Result<std::vector<int32_t>> BinaryArray::ToIntArray() const {
    PAIMON_RETURN_NOT_OK(CheckNoNull());
    std::vector<int32_t> values;
    values.resize(size_);
    MemorySegmentUtils::CopyToUnsafe({segment_}, element_offset_,
                                     const_cast<void*>(static_cast<const void*>(values.data())),
                                     size_ * sizeof(int32_t));
    return values;
}

Result<std::vector<int64_t>> BinaryArray::ToLongArray() const {
    PAIMON_RETURN_NOT_OK(CheckNoNull());
    std::vector<int64_t> values;
    values.resize(size_);
    MemorySegmentUtils::CopyToUnsafe({segment_}, element_offset_,
                                     const_cast<void*>(static_cast<const void*>(values.data())),
                                     size_ * sizeof(int64_t));
    return values;
}

Result<std::vector<float>> BinaryArray::ToFloatArray() const {
    PAIMON_RETURN_NOT_OK(CheckNoNull());
    std::vector<float> values;
    values.resize(size_);
    MemorySegmentUtils::CopyToUnsafe({segment_}, element_offset_,
                                     const_cast<void*>(static_cast<const void*>(values.data())),
                                     size_ * sizeof(float));
    return values;
}

Result<std::vector<double>> BinaryArray::ToDoubleArray() const {
    PAIMON_RETURN_NOT_OK(CheckNoNull());
    std::vector<double> values;
    values.resize(size_);
    MemorySegmentUtils::CopyToUnsafe({segment_}, element_offset_,
                                     const_cast<void*>(static_cast<const void*>(values.data())),
                                     size_ * sizeof(double));
    return values;
}

BinaryArray BinaryArray::Copy(MemoryPool* pool) const {
    BinaryArray array;
    Copy(&array, pool);
    return array;
}

void BinaryArray::Copy(BinaryArray* reuse, MemoryPool* pool) const {
    std::shared_ptr<Bytes> bytes =
        MemorySegmentUtils::CopyToBytes({segment_}, offset_, size_in_bytes_, pool);
    reuse->PointTo(MemorySegment::Wrap(bytes), 0, size_in_bytes_);
}

BinaryArray BinaryArray::FromIntArray(const std::vector<int32_t>& arr, MemoryPool* pool) {
    BinaryArray array;
    BinaryArrayWriter writer = BinaryArrayWriter(&array, arr.size(), sizeof(int32_t), pool);
    for (size_t i = 0; i < arr.size(); i++) {
        int32_t v = arr[i];
        writer.WriteInt(i, v);
    }
    writer.Complete();
    return array;
}

BinaryArray BinaryArray::FromLongArray(const std::vector<int64_t>& arr, MemoryPool* pool) {
    BinaryArray array;
    BinaryArrayWriter writer = BinaryArrayWriter(&array, arr.size(), sizeof(int64_t), pool);
    for (size_t i = 0; i < arr.size(); i++) {
        int64_t v = arr[i];
        writer.WriteLong(i, v);
    }
    writer.Complete();
    return array;
}

BinaryArray BinaryArray::FromLongArray(const InternalArray* arr, MemoryPool* pool) {
    assert(arr);
    auto cast_array = dynamic_cast<const BinaryArray*>(arr);
    if (cast_array) {
        return *cast_array;
    }

    BinaryArray array;
    BinaryArrayWriter writer = BinaryArrayWriter(&array, arr->Size(), 8, pool);
    std::vector<bool> is_null(arr->Size(), false);
    // accessing the null bit first makes memory access more concentrated
    for (int32_t i = 0; i < arr->Size(); i++) {
        is_null[i] = arr->IsNullAt(i);
    }
    for (int32_t i = 0; i < arr->Size(); i++) {
        if (is_null[i]) {
            writer.SetNullValue<int64_t>(i);
        } else {
            writer.WriteLong(i, arr->GetLong(i));
        }
    }
    writer.Complete();
    return array;
}

}  // namespace paimon
