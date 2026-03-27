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

#pragma once

#include <cassert>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "paimon/common/data/binary_section.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_map.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class MemorySegment;

/// A binary implementation of `InternalArray` which is backed by a single `MemorySegment`.
/// For fields that hold fixed-length primitive types, such as long, double or int, they are
/// stored compacted in bytes, just like the original C array.
///
/// @note: Unlike the Java implementation where data may span multiple MemorySegments,
/// in this C++ implementation all data resides within a single MemorySegment.
///
/// The binary layout of BinaryArray:
/// [size(int)] + [null bits(4-byte word boundaries)] + [values or offset&length] + [variable length
/// part].

class BinaryArray final : public BinarySection, public InternalArray {
 public:
    BinaryArray() = default;

    static int32_t CalculateHeaderInBytes(int32_t num_fields);
    int32_t Size() const override {
        return size_;
    }
    void PointTo(const MemorySegment& segment, int32_t offset, int32_t size_in_bytes) override;
    bool IsNullAt(int32_t pos) const override;

    bool GetBoolean(int32_t pos) const override;
    char GetByte(int32_t pos) const override;
    int16_t GetShort(int32_t pos) const override;
    int32_t GetInt(int32_t pos) const override;
    int32_t GetDate(int32_t pos) const override;
    int64_t GetLong(int32_t pos) const override;
    float GetFloat(int32_t pos) const override;
    double GetDouble(int32_t pos) const override;
    BinaryString GetString(int32_t pos) const override;
    std::string_view GetStringView(int32_t pos) const override;

    Decimal GetDecimal(int32_t pos, int32_t precision, int32_t scale) const override;
    Timestamp GetTimestamp(int32_t pos, int32_t precision) const override;
    std::shared_ptr<Bytes> GetBinary(int32_t pos) const override;
    std::shared_ptr<InternalArray> GetArray(int32_t pos) const override;
    std::shared_ptr<InternalMap> GetMap(int32_t pos) const override;
    std::shared_ptr<InternalRow> GetRow(int32_t pos, int32_t num_fields) const override;

    bool AnyNull() const;

    Result<std::vector<char>> ToBooleanArray() const override;
    Result<std::vector<char>> ToByteArray() const override;
    Result<std::vector<int16_t>> ToShortArray() const override;
    Result<std::vector<int32_t>> ToIntArray() const override;
    Result<std::vector<int64_t>> ToLongArray() const override;
    Result<std::vector<float>> ToFloatArray() const override;
    Result<std::vector<double>> ToDoubleArray() const override;

    BinaryArray Copy(MemoryPool* pool) const;
    void Copy(BinaryArray* reuse, MemoryPool* pool) const;

    int32_t HashCode() const override {
        return MemorySegmentUtils::HashByWords({segment_}, offset_, size_in_bytes_,
                                               GetDefaultPool().get());
    }

    static BinaryArray FromIntArray(const std::vector<int32_t>& arr, MemoryPool* pool);
    static BinaryArray FromLongArray(const std::vector<int64_t>& arr, MemoryPool* pool);
    static BinaryArray FromLongArray(const InternalArray* arr, MemoryPool* pool);

 private:
    void AssertIndexIsValid(int32_t ordinal) const;
    int32_t GetElementOffset(int32_t ordinal, int32_t element_size) const;
    Status CheckNoNull() const {
        if (AnyNull()) {
            return Status::Invalid("Primitive array must not contain a null value.");
        }
        return Status::OK();
    }

 private:
    /// The number of elements in this array.
    int32_t size_ = 0;
    /// The position to start storing array elements.
    int32_t element_offset_ = -1;
};

}  // namespace paimon
