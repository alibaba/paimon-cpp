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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include "paimon/common/data/binary_section.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/data_setters.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_map.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/murmurhash_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/result.h"

namespace paimon {
class Bytes;
class MemoryPool;

/// An implementation of `InternalRow` which is backed by a single `MemorySegment`.
/// A Row has two parts: Fixed-length part and variable-length part.
///
/// Fixed-length part contains 1 byte header and null bit set and field values. Null bit
/// set is used for null tracking and is aligned to 8-byte word boundaries. Field values
/// hold fixed-length primitive types and variable-length values which can be stored in 8
/// bytes inside. If it does not fit the variable-length field, then store the length and
/// offset of variable-length part.
///
/// @note: Unlike the Java implementation where variable-length data may span multiple
/// MemorySegments, in this C++ implementation both the fixed-length part and the
/// variable-length part reside within a single MemorySegment.
class BinaryRow final : public BinarySection, public InternalRow, public DataSetters {
 public:
    BinaryRow() : BinaryRow(0) {}
    explicit BinaryRow(int32_t arity);

    static constexpr int32_t HEADER_SIZE_IN_BITS = 8;
    static const BinaryRow& EmptyRow();
    static int32_t CalculateBitSetWidthInBytes(int32_t arity);
    static int32_t CalculateFixPartSizeInBytes(int32_t arity);

    int32_t GetFixedLengthPartSize() const;
    int32_t GetFieldCount() const override {
        return arity_;
    }
    Result<const RowKind*> GetRowKind() const override;

    void SetRowKind(const RowKind* kind) override;
    void SetTotalSize(int32_t size_in_bytes);
    bool IsNullAt(int32_t pos) const override;
    void SetNullAt(int32_t i) override;

    void SetByte(int32_t pos, char value) override;
    void SetBoolean(int32_t pos, bool value) override;
    void SetShort(int32_t pos, int16_t value) override;
    void SetInt(int32_t pos, int32_t value) override;
    void SetLong(int32_t pos, int64_t value) override;
    void SetFloat(int32_t pos, float value) override;
    void SetDouble(int32_t pos, double value) override;

    char GetByte(int32_t pos) const override;
    bool GetBoolean(int32_t pos) const override;
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
    /// The bit is 1 when the field is null. Default is 0.
    bool AnyNull() const;
    bool AnyNull(const std::vector<int32_t>& fields) const;
    BinaryRow Copy(MemoryPool* pool) const;
    void Copy(BinaryRow* reuse, MemoryPool* pool) const;
    void Clear();
    bool operator==(const BinaryRow& other) const;
    // TODO(liancheng.lsz): single column to be implemented

    std::string ToString() const override {
        std::stringstream ss;
        ss << std::hex << static_cast<uint32_t>(HashCode());
        return "BinaryRow@" + ss.str();
    }

    int32_t HashCode() const override;

 private:
    static BinaryRow GetEmptyRow();
    int32_t GetFieldOffset(int32_t pos) const;
    void AssertIndexIsValid(int32_t index) const;
    void SetNotNullAt(int32_t i);
    void CopyInternal(BinaryRow* reuse, MemoryPool* pool) const;

    static const int64_t FIRST_BYTE_ZERO;

 private:
    int32_t arity_;
    int32_t null_bits_size_in_bytes_;
};

}  // namespace paimon

namespace std {
/// for std::unordered_map<pair<paimon::BinaryRow, int32_t>>
template <>
struct hash<std::pair<paimon::BinaryRow, int32_t>> {
    size_t operator()(const std::pair<paimon::BinaryRow, int32_t>& partition_bucket) const {
        const auto& [partition, bucket] = partition_bucket;
        return paimon::MurmurHashUtils::HashUnsafeBytes(reinterpret_cast<const void*>(&bucket), 0,
                                                        sizeof(bucket), partition.HashCode());
    }
};

/// for std::unordered_map<std::tuple<paimon::BinaryRow, int32_t, std::string>, ...>
template <>
struct hash<std::tuple<paimon::BinaryRow, int32_t, std::string>> {
    size_t operator()(
        const std::tuple<paimon::BinaryRow, int32_t, std::string>& partition_bucket_type) const {
        const auto& [partition, bucket, index_type] = partition_bucket_type;
        size_t hash = paimon::MurmurHashUtils::HashUnsafeBytes(
            reinterpret_cast<const void*>(&bucket), 0, sizeof(bucket), partition.HashCode());
        return paimon::MurmurHashUtils::HashUnsafeBytes(index_type.data(), 0, index_type.size(),
                                                        hash);
    }
};

template <>
struct hash<paimon::BinaryRow> {
    size_t operator()(const paimon::BinaryRow& row) const {
        return row.HashCode();
    }
};

}  // namespace std
