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

#include "arrow/api.h"
#include "paimon/common/data/abstract_binary_writer.h"
#include "paimon/common/memory/memory_segment.h"
namespace paimon {
class BinaryArray;
class MemoryPool;

/// Writer for binary array. See `BinaryArray`.
class BinaryArrayWriter : public AbstractBinaryWriter {
 public:
    BinaryArrayWriter(BinaryArray* array, int32_t num_elements, int32_t element_size,
                      MemoryPool* pool);

    static int32_t GetElementSize(const arrow::Type::type& arrow_type);

    /// First, call `Reset()` before set/write value.
    void Reset() override;
    void SetNullBit(int32_t ordinal) override;

    template <typename T>
    void SetNullValue(int32_t ordinal) {
        SetNullBit(ordinal);
        // put zero into the corresponding field when set null
        segment_.PutValue<T>(GetElementOffset(ordinal, sizeof(T)), static_cast<T>(0));
    }

    void SetNullAt(int32_t ordinal, const arrow::Type::type& arrow_type);

    void WriteBoolean(int32_t pos, bool value) override {
        segment_.PutValue<bool>(GetElementOffset(pos, 1), value);
    }

    void WriteByte(int32_t pos, int8_t value) override {
        segment_.PutValue<int8_t>(GetElementOffset(pos, 1), value);
    }

    void WriteShort(int32_t pos, int16_t value) override {
        segment_.PutValue<int16_t>(GetElementOffset(pos, 2), value);
    }

    void WriteInt(int32_t pos, int32_t value) override {
        segment_.PutValue<int32_t>(GetElementOffset(pos, 4), value);
    }

    void WriteLong(int32_t pos, int64_t value) override {
        segment_.PutValue<int64_t>(GetElementOffset(pos, 8), value);
    }

    void WriteFloat(int32_t pos, float value) override {
        segment_.PutValue<float>(GetElementOffset(pos, 4), value);
    }

    void WriteDouble(int32_t pos, double value) override {
        segment_.PutValue<double>(GetElementOffset(pos, 8), value);
    }

    void SetNullAt(int32_t ordinal) override;
    int32_t GetFieldOffset(int32_t pos) const override;
    void SetOffsetAndSize(int32_t pos, int32_t offset, int64_t size) override;
    void AfterGrow() override;
    /// Finally, complete write to set real size to row.
    void Complete() override;
    int32_t GetNumElements() const;

 private:
    int32_t GetElementOffset(int32_t pos, int32_t element_size) const;

 private:
    int32_t null_bits_size_in_bytes_;
    BinaryArray* array_;
    int32_t num_elements_;
    int32_t fixed_size_;
};

}  // namespace paimon
