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
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "fmt/format.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/columnar/columnar_utils.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_map.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/result.h"

namespace arrow {
class StructArray;
}  // namespace arrow

namespace paimon {
class Bytes;
class MemoryPool;

/// Columnar row to support access to vector column data. It is a row view in arrow::Array.
class ColumnarRow : public InternalRow {
 public:
    /// @brief Construct a ColumnarRow without holding ownership of the underlying arrays.
    /// @warning The caller MUST ensure the data source (e.g., RecordBatch or parent StructArray)
    /// outlives this ColumnarRow. The internal array_vec_ stores raw pointers only; if the
    /// source is freed first, these pointers will dangle. This design is intentional for
    /// performance—avoiding per-row shared_ptr ref-count overhead on the hot read path.
    ColumnarRow(const arrow::ArrayVector& array_vec, const std::shared_ptr<MemoryPool>& pool,
                int64_t row_id)
        : ColumnarRow(/*struct_array holder*/ nullptr, array_vec, pool, row_id) {}

    /// @brief Construct a ColumnarRow that holds shared ownership of a StructArray.
    /// @note When struct_array is non-null it keeps the underlying buffers alive, making it safe
    /// to outlive the original batch. Prefer this overload when the row may escape the scope of
    /// its parent container.
    ColumnarRow(const std::shared_ptr<arrow::StructArray>& struct_array,
                const arrow::ArrayVector& array_vec, const std::shared_ptr<MemoryPool>& pool,
                int64_t row_id)
        : struct_array_(struct_array), pool_(pool), row_id_(row_id) {
        array_vec_.reserve(array_vec.size());
        for (const auto& array : array_vec) {
            array_vec_.push_back(array.get());
        }
    }

    Result<const RowKind*> GetRowKind() const override {
        return row_kind_;
    }

    void SetRowKind(const RowKind* kind) override {
        row_kind_ = kind;
    }

    int32_t GetFieldCount() const override {
        return array_vec_.size();
    }

    bool IsNullAt(int32_t pos) const override {
        return array_vec_[pos]->IsNull(row_id_);
    }
    bool GetBoolean(int32_t pos) const override {
        return ColumnarUtils::GetGenericValue<arrow::BooleanType, bool>(array_vec_[pos], row_id_);
    }

    char GetByte(int32_t pos) const override {
        return ColumnarUtils::GetGenericValue<arrow::Int8Type, char>(array_vec_[pos], row_id_);
    }

    int16_t GetShort(int32_t pos) const override {
        return ColumnarUtils::GetGenericValue<arrow::Int16Type, int16_t>(array_vec_[pos], row_id_);
    }

    int32_t GetInt(int32_t pos) const override {
        return ColumnarUtils::GetGenericValue<arrow::Int32Type, int32_t>(array_vec_[pos], row_id_);
    }

    int32_t GetDate(int32_t pos) const override {
        return ColumnarUtils::GetGenericValue<arrow::Date32Type, int32_t>(array_vec_[pos], row_id_);
    }

    int64_t GetLong(int32_t pos) const override {
        return ColumnarUtils::GetGenericValue<arrow::Int64Type, int64_t>(array_vec_[pos], row_id_);
    }

    float GetFloat(int32_t pos) const override {
        return ColumnarUtils::GetGenericValue<arrow::FloatType, float>(array_vec_[pos], row_id_);
    }

    double GetDouble(int32_t pos) const override {
        return ColumnarUtils::GetGenericValue<arrow::DoubleType, double>(array_vec_[pos], row_id_);
    }

    /// @note `GetString()` and `GetBinary()` will deep copy string data to pool, use
    /// `GetStringView()` to avoid copy
    BinaryString GetString(int32_t pos) const override {
        auto bytes =
            ColumnarUtils::GetBytes<arrow::StringType>(array_vec_[pos], row_id_, pool_.get());
        return BinaryString::FromBytes(bytes);
    }

    std::string_view GetStringView(int32_t pos) const override {
        return ColumnarUtils::GetView(array_vec_[pos], row_id_);
    }

    Decimal GetDecimal(int32_t pos, int32_t precision, int32_t scale) const override;

    Timestamp GetTimestamp(int32_t pos, int32_t precision) const override;

    std::shared_ptr<Bytes> GetBinary(int32_t pos) const override {
        return ColumnarUtils::GetBytes<arrow::BinaryType>(array_vec_[pos], row_id_, pool_.get());
    }

    std::shared_ptr<InternalRow> GetRow(int32_t pos, int32_t num_fields) const override;

    std::shared_ptr<InternalArray> GetArray(int32_t pos) const override;

    std::shared_ptr<InternalMap> GetMap(int32_t pos) const override;

    std::string ToString() const override {
        return fmt::format("ColumnarRow, row_id {}", row_id_);
    }

 private:
    /// @note `struct_array_` is the data holder for columnar row, ensure that the data life
    /// cycle is consistent with the columnar row, `array_vec_` maybe a subset of
    /// `struct_array_`, so `struct_array_` cannot be used for `GetXXX()`
    std::shared_ptr<arrow::StructArray> struct_array_;
    std::shared_ptr<MemoryPool> pool_;
    std::vector<const arrow::Array*> array_vec_;
    const RowKind* row_kind_ = RowKind::Insert();
    int64_t row_id_;
};
}  // namespace paimon
