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

#include <memory>
#include <utility>
#include <vector>

#include "paimon/common/data/data_define.h"
#include "paimon/common/data/internal_array.h"

namespace paimon {

/// GenericArray is a generic implementation of `InternalArray` backed by a vector of VariantType.

/// @note Holders keep source arrays alive for non-owning string and binary elements. That is not
/// enough when a source is itself a view, in which case the caller must keep the data alive.
class GenericArray : public InternalArray {
 public:
    /// Create an array from materialized values and optional source-array holders.
    ///
    /// @param values Values stored in the array.
    /// @param holders Source arrays retained for non-owning string and binary values.
    explicit GenericArray(std::vector<VariantType> values,
                          std::vector<std::shared_ptr<InternalArray>> holders = {})
        : values_(std::move(values)), holders_(std::move(holders)) {}

    int32_t Size() const override;
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

    Result<std::vector<char>> ToBooleanArray() const override;
    Result<std::vector<char>> ToByteArray() const override;
    Result<std::vector<int16_t>> ToShortArray() const override;
    Result<std::vector<int32_t>> ToIntArray() const override;
    Result<std::vector<int64_t>> ToLongArray() const override;
    Result<std::vector<float>> ToFloatArray() const override;
    Result<std::vector<double>> ToDoubleArray() const override;

 private:
    template <typename T>
    Result<std::vector<T>> ToPrimitiveArray() const;

    const VariantType& ValueAt(int32_t pos) const;

    std::vector<VariantType> values_;
    std::vector<std::shared_ptr<InternalArray>> holders_;
};

}  // namespace paimon
