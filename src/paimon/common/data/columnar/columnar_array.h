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

#include "arrow/array/array_base.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/columnar/columnar_utils.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_map.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class BinaryType;
class BooleanType;
class Date32Type;
class DoubleType;
class FloatType;
class Int16Type;
class Int32Type;
class Int64Type;
class Int8Type;
class StringType;
}  // namespace arrow

namespace paimon {
class Bytes;
class MemoryPool;

/// Columnar array to support access to vector column data.
///
/// NOTE: This class holds a non-owning raw pointer to the underlying arrow::Array for efficiency.
/// The caller must ensure that the pointed-to Array outlives this ColumnarArray instance.
/// Typically, lifetime is guaranteed by the owning ColumnarBatchContext or the parent
/// arrow container (e.g., ListArray, MapArray) that holds the shared_ptr.
class ColumnarArray : public InternalArray {
 public:
    ColumnarArray(const arrow::Array* array, const std::shared_ptr<MemoryPool>& pool,
                  int32_t offset, int32_t length)
        : pool_(pool), array_(array), offset_(offset), length_(length) {
        assert(array_);
        assert(array_->length() >= offset + length);
    }

    int32_t Size() const override {
        return length_;
    }

    bool IsNullAt(int32_t pos) const override {
        return array_->IsNull(offset_ + pos);
    }

    bool GetBoolean(int32_t pos) const override {
        return ColumnarUtils::GetGenericValue<arrow::BooleanType, bool>(array_, offset_ + pos);
    }

    char GetByte(int32_t pos) const override {
        return ColumnarUtils::GetGenericValue<arrow::Int8Type, char>(array_, offset_ + pos);
    }

    int16_t GetShort(int32_t pos) const override {
        return ColumnarUtils::GetGenericValue<arrow::Int16Type, int16_t>(array_, offset_ + pos);
    }

    int32_t GetInt(int32_t pos) const override {
        return ColumnarUtils::GetGenericValue<arrow::Int32Type, int32_t>(array_, offset_ + pos);
    }

    int32_t GetDate(int32_t pos) const override {
        return ColumnarUtils::GetGenericValue<arrow::Date32Type, int32_t>(array_, offset_ + pos);
    }

    int64_t GetLong(int32_t pos) const override {
        return ColumnarUtils::GetGenericValue<arrow::Int64Type, int64_t>(array_, offset_ + pos);
    }

    float GetFloat(int32_t pos) const override {
        return ColumnarUtils::GetGenericValue<arrow::FloatType, float>(array_, offset_ + pos);
    }

    double GetDouble(int32_t pos) const override {
        return ColumnarUtils::GetGenericValue<arrow::DoubleType, double>(array_, offset_ + pos);
    }

    BinaryString GetString(int32_t pos) const override {
        auto bytes = ColumnarUtils::GetBytes<arrow::StringType>(array_, offset_ + pos, pool_.get());
        return BinaryString::FromBytes(bytes);
    }

    std::string_view GetStringView(int32_t pos) const override {
        return ColumnarUtils::GetView(array_, offset_ + pos);
    }

    Decimal GetDecimal(int32_t pos, int32_t precision, int32_t scale) const override;

    Timestamp GetTimestamp(int32_t pos, int32_t precision) const override;

    std::shared_ptr<Bytes> GetBinary(int32_t pos) const override {
        return ColumnarUtils::GetBytes<arrow::BinaryType>(array_, offset_ + pos, pool_.get());
    }

    std::shared_ptr<InternalArray> GetArray(int32_t pos) const override;

    std::shared_ptr<InternalMap> GetMap(int32_t pos) const override;

    std::shared_ptr<InternalRow> GetRow(int32_t pos, int32_t num_fields) const override;

    Result<std::vector<char>> ToBooleanArray() const override;

    Result<std::vector<char>> ToByteArray() const override;

    Result<std::vector<int16_t>> ToShortArray() const override;

    Result<std::vector<int32_t>> ToIntArray() const override;

    Result<std::vector<int64_t>> ToLongArray() const override;

    Result<std::vector<float>> ToFloatArray() const override;

    Result<std::vector<double>> ToDoubleArray() const override;

 private:
    Status CheckNoNull() const;

 private:
    std::shared_ptr<MemoryPool> pool_;
    const arrow::Array* array_;
    int32_t offset_;
    int32_t length_;
};
}  // namespace paimon
