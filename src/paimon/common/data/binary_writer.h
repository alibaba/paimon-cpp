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
#include <string>
#include <vector>

namespace paimon {

class Bytes;
class BinaryString;
class BinaryRow;
class BinaryArray;
class BinaryMap;
class Timestamp;
class Decimal;

/// Writer to write a composite data format, like row, array. 1. Invoke `Reset()`. 2. Write each
/// field by `WriteXX()` or `SetNullAt()`. (Same field can not be written repeatedly.) 3. Invoke
/// `Complete()`.
class BinaryWriter {
 public:
    virtual ~BinaryWriter() = default;
    /// Reset writer to prepare next write.
    virtual void Reset() = 0;

    ///  Set null to this field.
    virtual void SetNullAt(int32_t pos) = 0;

    virtual void WriteBoolean(int32_t pos, bool value) = 0;

    virtual void WriteByte(int32_t pos, int8_t value) = 0;

    virtual void WriteShort(int32_t pos, int16_t value) = 0;

    virtual void WriteInt(int32_t pos, int32_t value) = 0;

    virtual void WriteLong(int32_t pos, int64_t value) = 0;

    virtual void WriteFloat(int32_t pos, float value) = 0;

    virtual void WriteDouble(int32_t pos, double value) = 0;

    virtual void WriteString(int32_t pos, const BinaryString& value) = 0;

    virtual void WriteBinary(int32_t pos, const Bytes& bytes) = 0;

    virtual void WriteStringView(int32_t pos, const std::string_view& view) = 0;

    virtual void WriteDecimal(int32_t pos, const std::optional<Decimal>& value,
                              int32_t precision) = 0;

    virtual void WriteTimestamp(int32_t pos, const std::optional<Timestamp>& value,
                                int32_t precision) = 0;

    virtual void WriteArray(int32_t pos, const BinaryArray& value) = 0;

    virtual void WriteRow(int32_t pos, const BinaryRow& value) = 0;

    virtual void WriteMap(int32_t pos, const BinaryMap& input) = 0;

    /// Finally, complete write to set real size to binary.
    virtual void Complete() = 0;
};

}  // namespace paimon
