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

#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

#include "paimon/common/data/binary_writer.h"
#include "paimon/common/memory/memory_segment.h"

namespace paimon {
class BinaryArray;
class BinaryRow;
class BinaryString;
class Bytes;
class Decimal;
class MemoryPool;
class Timestamp;

/// Use the special format to write data to a `MemorySegment` (its capacity grows
/// automatically). If write a format binary: 1. New a writer. 2. Write each field by `WriteXXX()`
/// or `SetNullAt()`. (Variable length fields can not be written repeatedly.) 3. Invoke
/// `Complete()`. If want to reuse this writer, please invoke `Reset()` first.
class AbstractBinaryWriter : public BinaryWriter {
 public:
    void WriteString(int32_t pos, const BinaryString& value) override;

    void WriteBinary(int32_t pos, const Bytes& bytes) override;

    void WriteDecimal(int32_t pos, const std::optional<Decimal>& value, int32_t precision) override;

    void WriteTimestamp(int32_t pos, const std::optional<Timestamp>& value,
                        int32_t precision) override;

    void WriteArray(int32_t pos, const BinaryArray& value) override;

    void WriteRow(int32_t pos, const BinaryRow& value) override;

    void WriteMap(int32_t pos, const BinaryMap& input) override;

    void WriteStringView(int32_t pos, const std::string_view& view) override;

    const MemorySegment& GetSegments() const {
        return segment_;
    }

 protected:
    static int32_t RoundNumberOfBytesToNearestWord(int32_t num_bytes);

    /// Set offset and size to fix len part.
    virtual void SetOffsetAndSize(int32_t pos, int32_t offset, int64_t size) = 0;

    /// Get field offset.
    virtual int32_t GetFieldOffset(int32_t pos) const = 0;

    /// After grow, need point to new memory.
    virtual void AfterGrow() = 0;

    virtual void SetNullBit(int32_t ordinal) = 0;

    void ZeroOutPaddingBytes(int32_t num_bytes);
    void EnsureCapacity(int32_t needed_size);

 private:
    /// Increases the capacity to ensure that it can hold at least the minimum capacity argument.
    void Grow(int32_t min_capacity);

    void WriteBytes(int32_t pos, const Bytes& bytes);

    template <typename T>
    void WriteBytesToVarLenPart(int32_t pos, const T& bytes, int32_t len);

    template <typename T>
    static void WriteBytesToFixLenPart(MemorySegment* segment, int32_t field_offset, const T& bytes,
                                       int32_t len);

    void WriteSegmentsToVarLenPart(int32_t pos, const std::vector<MemorySegment>& segments,
                                   int32_t offset, int32_t size);
    void WriteMultiSegmentsToVarLenPart(const std::vector<MemorySegment>& segments, int32_t offset,
                                        int32_t size);

 protected:
    int32_t cursor_ = 0;
    MemoryPool* pool_ = nullptr;
    MemorySegment segment_;
};

}  // namespace paimon
