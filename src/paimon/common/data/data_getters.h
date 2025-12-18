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

#include <memory>

#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/data_define.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"

namespace paimon {
class Bytes;
class Decimal;
class InternalRow;
class InternalMap;
class InternalArray;
class Timestamp;
/// Getters to get data.
class DataGetters {
 public:
    virtual ~DataGetters() = default;

    /// @return true if the element is null at the given position.
    virtual bool IsNullAt(int32_t pos) const = 0;

    /// @return the boolean value at the given position.
    virtual bool GetBoolean(int32_t pos) const = 0;

    /// @return the byte value at the given position.
    virtual char GetByte(int32_t pos) const = 0;

    /// @return the short value at the given position.
    virtual int16_t GetShort(int32_t pos) const = 0;

    /// @return the integer value at the given position.
    virtual int32_t GetInt(int32_t pos) const = 0;

    /// @return the date integer value at the given position.
    virtual int32_t GetDate(int32_t pos) const = 0;

    /// @return the long value at the given position.
    virtual int64_t GetLong(int32_t pos) const = 0;

    /// @return the float value at the given position.
    virtual float GetFloat(int32_t pos) const = 0;

    /// @return the double value at the given position.
    virtual double GetDouble(int32_t pos) const = 0;

    /// @return the string value at the given position.
    virtual BinaryString GetString(int32_t pos) const = 0;

    /// @return the string view value at the given position. Original string data must be valid.
    virtual std::string_view GetStringView(int32_t pos) const = 0;

    /// @return the decimal value at the given position.
    /// The precision and scale are required to determine whether the decimal value
    /// was stored in a compact representation (see `Decimal`).
    virtual Decimal GetDecimal(int32_t pos, int32_t precision, int32_t scale) const = 0;

    /// @return the timestamp value at the given position.
    /// The precision is required to determine whether the timestamp value was stored
    /// in a compact representation (see `Timestamp`).
    virtual Timestamp GetTimestamp(int32_t pos, int32_t precision) const = 0;

    /// @return the binary value at the given position.
    virtual std::shared_ptr<Bytes> GetBinary(int32_t pos) const = 0;

    /// @return the array value at the given position.
    virtual std::shared_ptr<InternalArray> GetArray(int32_t pos) const = 0;

    /// @return the map value at the given position.
    virtual std::shared_ptr<InternalMap> GetMap(int32_t pos) const = 0;

    /// @return the row value at the given position.
    /// The number of fields is required to correctly extract the row.
    virtual std::shared_ptr<InternalRow> GetRow(int32_t pos, int32_t num_fields) const = 0;
};
}  // namespace paimon
