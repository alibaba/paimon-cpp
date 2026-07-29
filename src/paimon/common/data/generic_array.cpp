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

#include "paimon/common/data/generic_array.h"

#include <cassert>
#include <string>

#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"

namespace paimon {

int32_t GenericArray::Size() const {
    return static_cast<int32_t>(values_.size());
}

const VariantType& GenericArray::ValueAt(int32_t pos) const {
    assert(pos >= 0 && static_cast<size_t>(pos) < values_.size());
    return values_[pos];
}

bool GenericArray::IsNullAt(int32_t pos) const {
    return DataDefine::IsVariantNull(ValueAt(pos));
}

bool GenericArray::GetBoolean(int32_t pos) const {
    return DataDefine::GetVariantValue<bool>(ValueAt(pos));
}

char GenericArray::GetByte(int32_t pos) const {
    return DataDefine::GetVariantValue<char>(ValueAt(pos));
}

int16_t GenericArray::GetShort(int32_t pos) const {
    return DataDefine::GetVariantValue<int16_t>(ValueAt(pos));
}

int32_t GenericArray::GetInt(int32_t pos) const {
    return DataDefine::GetVariantValue<int32_t>(ValueAt(pos));
}

int32_t GenericArray::GetDate(int32_t pos) const {
    return GetInt(pos);
}

int64_t GenericArray::GetLong(int32_t pos) const {
    return DataDefine::GetVariantValue<int64_t>(ValueAt(pos));
}

float GenericArray::GetFloat(int32_t pos) const {
    return DataDefine::GetVariantValue<float>(ValueAt(pos));
}

double GenericArray::GetDouble(int32_t pos) const {
    return DataDefine::GetVariantValue<double>(ValueAt(pos));
}

BinaryString GenericArray::GetString(int32_t pos) const {
    const VariantType& value = ValueAt(pos);
    if (const auto* string = DataDefine::GetVariantPtr<BinaryString>(value)) {
        return *string;
    }
    return BinaryString::FromString(std::string(DataDefine::GetStringView(value)),
                                    GetDefaultPool().get());
}

std::string_view GenericArray::GetStringView(int32_t pos) const {
    return DataDefine::GetStringView(ValueAt(pos));
}

Decimal GenericArray::GetDecimal(int32_t pos, int32_t precision, int32_t scale) const {
    return DataDefine::GetVariantValue<Decimal>(ValueAt(pos));
}

Timestamp GenericArray::GetTimestamp(int32_t pos, int32_t precision) const {
    return DataDefine::GetVariantValue<Timestamp>(ValueAt(pos));
}

std::shared_ptr<Bytes> GenericArray::GetBinary(int32_t pos) const {
    const VariantType& value = ValueAt(pos);
    if (const auto* bytes = DataDefine::GetVariantPtr<std::shared_ptr<Bytes>>(value)) {
        return *bytes;
    }
    return std::shared_ptr<Bytes>(Bytes::AllocateBytes(
        std::string(DataDefine::GetStringView(value)), GetDefaultPool().get()));
}

std::shared_ptr<InternalArray> GenericArray::GetArray(int32_t pos) const {
    return DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(ValueAt(pos));
}

std::shared_ptr<InternalMap> GenericArray::GetMap(int32_t pos) const {
    return DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(ValueAt(pos));
}

std::shared_ptr<InternalRow> GenericArray::GetRow(int32_t pos, int32_t num_fields) const {
    return DataDefine::GetVariantValue<std::shared_ptr<InternalRow>>(ValueAt(pos));
}

template <typename T>
Result<std::vector<T>> GenericArray::ToPrimitiveArray() const {
    std::vector<T> result;
    result.reserve(values_.size());
    for (const VariantType& value : values_) {
        if (DataDefine::IsVariantNull(value)) {
            return Status::Invalid("Primitive array must not contain a null value.");
        }
        result.push_back(DataDefine::GetVariantValue<T>(value));
    }
    return result;
}

Result<std::vector<char>> GenericArray::ToBooleanArray() const {
    std::vector<char> result;
    result.reserve(values_.size());
    for (const VariantType& value : values_) {
        if (DataDefine::IsVariantNull(value)) {
            return Status::Invalid("Primitive array must not contain a null value.");
        }
        result.push_back(DataDefine::GetVariantValue<bool>(value));
    }
    return result;
}

Result<std::vector<char>> GenericArray::ToByteArray() const {
    return ToPrimitiveArray<char>();
}

Result<std::vector<int16_t>> GenericArray::ToShortArray() const {
    return ToPrimitiveArray<int16_t>();
}

Result<std::vector<int32_t>> GenericArray::ToIntArray() const {
    return ToPrimitiveArray<int32_t>();
}

Result<std::vector<int64_t>> GenericArray::ToLongArray() const {
    return ToPrimitiveArray<int64_t>();
}

Result<std::vector<float>> GenericArray::ToFloatArray() const {
    return ToPrimitiveArray<float>();
}

Result<std::vector<double>> GenericArray::ToDoubleArray() const {
    return ToPrimitiveArray<double>();
}

}  // namespace paimon
