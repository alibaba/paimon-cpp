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

#pragma once
#include <functional>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_writer.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/common/memory/memory_slice.h"
#include "paimon/common/utils/var_length_int_utils.h"
namespace paimon {
class RowCompactedSerializer {
 public:
    static Result<std::unique_ptr<RowCompactedSerializer>> Create(
        const std::shared_ptr<arrow::Schema>& schema, const std::shared_ptr<MemoryPool>& pool);

    static int32_t CalculateBitSetInBytes(int32_t arity) {
        return (arity + 7 + BinaryRow::HEADER_SIZE_IN_BITS) / 8;
    }

    Result<std::shared_ptr<Bytes>> SerializeToBytes(const InternalRow& row);

    Result<std::unique_ptr<InternalRow>> Deserialize(const std::shared_ptr<Bytes>& bytes);

    static Result<MemorySlice::SliceComparator> CreateSliceComparator(
        const std::shared_ptr<arrow::Schema>& schema, const std::shared_ptr<MemoryPool>& pool);

 private:
    class RowWriter {
     public:
        RowWriter(int32_t header_size_in_bytes, const std::shared_ptr<MemoryPool>& pool);

        void Reset() {
            position_ = header_size_in_bytes_;
            std::memset(buffer_->data(), 0, header_size_in_bytes_);
        }

        void WriteRowKind(const RowKind& kind) {
            (*buffer_)[0] = static_cast<char>(kind.ToByteValue());
        }

        void SetNullAt(int32_t pos) {
            MemorySegmentUtils::BitSet(&segment_, 0, pos + BinaryRow::HEADER_SIZE_IN_BITS);
        }

        template <typename T>
        Status WriteValue(const T& value) {
            EnsureCapacity(sizeof(T));
            segment_.PutValue<T>(position_, value);
            position_ += sizeof(T);
            return Status::OK();
        }

        Status WriteString(const BinaryString& value) {
            return WriteSegments(value.GetSegments(), value.GetOffset(), value.GetSizeInBytes());
        }

        template <typename T>
        Status WriteBinary(const T* bytes) {
            PAIMON_RETURN_NOT_OK(WriteUnsignedInt(bytes->size()));
            EnsureCapacity(bytes->size());
            memcpy(buffer_->data() + position_, bytes->data(), bytes->size());
            position_ += bytes->size();
            return Status::OK();
        }

        Status WriteDecimal(const Decimal& value, int32_t precision);
        Status WriteTimestamp(const Timestamp& value, int32_t precision);

        Status WriteArray(const std::shared_ptr<InternalArray>& value,
                          const std::shared_ptr<arrow::DataType>& type);
        Status WriteMap(const std::shared_ptr<InternalMap>& value,
                        const std::shared_ptr<arrow::DataType>& type);

        Status WriteRow(const std::shared_ptr<InternalRow>& value,
                        const std::shared_ptr<RowCompactedSerializer>& serializer);

        std::shared_ptr<Bytes> CopyBuffer() const;

     private:
        Status WriteUnsignedInt(int32_t value);
        Status WriteSegments(const std::vector<MemorySegment>& segments, int32_t off, int32_t len);
        void EnsureCapacity(int32_t size);
        void Grow(int32_t min_capacity_add);
        void SetBuffer(std::shared_ptr<Bytes> new_buffer);

     private:
        const int32_t header_size_in_bytes_;
        std::shared_ptr<MemoryPool> pool_;
        std::shared_ptr<Bytes> buffer_;
        MemorySegment segment_;
        int32_t position_ = 0;
    };

    class RowReader {
     public:
        RowReader(int32_t header_size_in_bytes, const std::shared_ptr<MemoryPool>& pool)
            : header_size_in_bytes_(header_size_in_bytes), pool_(pool) {}

        void PointTo(const std::shared_ptr<Bytes>& bytes);

        void PointTo(const MemorySegment& segment, int32_t offset);

        Result<const RowKind*> ReadRowKind() const;

        bool IsNullAt(int32_t pos) const {
            return MemorySegmentUtils::BitGet(segment_, offset_,
                                              pos + BinaryRow::HEADER_SIZE_IN_BITS);
        }

        template <typename T>
        T ReadValue() {
            T value = segment_.GetValue<T>(position_);
            position_ += sizeof(T);
            return value;
        }

        Result<BinaryString> ReadString();

        Result<std::shared_ptr<Bytes>> ReadBinary();

        Result<Decimal> ReadDecimal(int32_t precision, int32_t scale);

        Result<Timestamp> ReadTimestamp(int32_t precision);

        Result<std::shared_ptr<InternalArray>> ReadArray();
        Result<std::shared_ptr<InternalMap>> ReadMap();
        Result<std::shared_ptr<InternalRow>> ReadRow(
            const std::shared_ptr<RowCompactedSerializer>& serializer);

     private:
        Result<int32_t> ReadUnsignedInt() {
            const auto* bytes = segment_.GetArray();
            return VarLengthIntUtils::DecodeInt(bytes, &position_);
        }

     private:
        const int32_t header_size_in_bytes_;
        std::shared_ptr<MemoryPool> pool_;
        MemorySegment segment_;
        std::vector<MemorySegment> segments_;
        int32_t offset_ = 0;
        int32_t position_ = 0;
    };

    using FieldWriter = std::function<Status(int32_t, const VariantType&, RowWriter*)>;
    using FieldReader = std::function<Result<VariantType>(int32_t, RowReader*)>;

 private:
    RowCompactedSerializer(const std::shared_ptr<arrow::Schema>& schema,
                           std::vector<InternalRow::FieldGetterFunc>&& getters,
                           std::vector<FieldWriter>&& writers, std::vector<FieldReader>&& readers,
                           const std::shared_ptr<MemoryPool>& pool);

    static Result<FieldReader> CreateFieldReader(const std::shared_ptr<arrow::DataType>& field_type,
                                                 const std::shared_ptr<MemoryPool>& pool);
    static Result<FieldWriter> CreateFieldWriter(const std::shared_ptr<arrow::DataType>& field_type,
                                                 const std::shared_ptr<MemoryPool>& pool);

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<InternalRow::FieldGetterFunc> getters_;
    std::vector<FieldWriter> writers_;
    std::vector<FieldReader> readers_;
    std::unique_ptr<RowWriter> row_writer_;
    std::unique_ptr<RowReader> row_reader_;
};
}  // namespace paimon
