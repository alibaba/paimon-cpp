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
#include <string_view>
#include <vector>

#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_map.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class Bytes;
class InternalMap;
class InternalRow;

/// An implementation of `InternalArray` which provides a projected view of the underlying
/// `InternalArray`.
class ProjectedArray : public InternalArray {
 public:
    ProjectedArray(const std::shared_ptr<InternalArray>& array, const std::vector<int32_t>& mapping)
        : array_(array), mapping_(mapping) {
        assert(array_);
    }

    int32_t Size() const override {
        return mapping_.size();
    }

    bool IsNullAt(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        if (mapping_[pos] < 0) {
            return true;
        }
        return array_->IsNullAt(mapping_[pos]);
    }

    bool GetBoolean(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return array_->GetBoolean(mapping_[pos]);
    }

    char GetByte(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return array_->GetByte(mapping_[pos]);
    }

    int16_t GetShort(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return array_->GetShort(mapping_[pos]);
    }

    int32_t GetInt(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return array_->GetInt(mapping_[pos]);
    }

    int32_t GetDate(int32_t pos) const override {
        return GetInt(pos);
    }

    int64_t GetLong(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return array_->GetLong(mapping_[pos]);
    }

    float GetFloat(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return array_->GetFloat(mapping_[pos]);
    }

    double GetDouble(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return array_->GetDouble(mapping_[pos]);
    }

    BinaryString GetString(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return array_->GetString(mapping_[pos]);
    }

    std::string_view GetStringView(int32_t pos) const override {
        assert(false);
        assert(static_cast<size_t>(pos) < mapping_.size());
        return array_->GetStringView(mapping_[pos]);
    }

    Decimal GetDecimal(int32_t pos, int32_t precision, int32_t scale) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return array_->GetDecimal(mapping_[pos], precision, scale);
    }

    Timestamp GetTimestamp(int32_t pos, int32_t precision) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return array_->GetTimestamp(mapping_[pos], precision);
    }

    std::shared_ptr<Bytes> GetBinary(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return array_->GetBinary(mapping_[pos]);
    }

    std::shared_ptr<InternalArray> GetArray(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return array_->GetArray(mapping_[pos]);
    }

    std::shared_ptr<InternalMap> GetMap(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return array_->GetMap(mapping_[pos]);
    }

    std::shared_ptr<InternalRow> GetRow(int32_t pos, int32_t num_fields) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return array_->GetRow(mapping_[pos], num_fields);
    }

    Result<std::vector<char>> ToBooleanArray() const override {
        return Status::Invalid("projected array do not support convert to boolean array");
    }
    Result<std::vector<char>> ToByteArray() const override {
        return Status::Invalid("projected array do not support convert to byte array");
    }
    Result<std::vector<int16_t>> ToShortArray() const override {
        return Status::Invalid("projected array do not support convert to short array");
    }
    Result<std::vector<int32_t>> ToIntArray() const override {
        return Status::Invalid("projected array do not support convert to int array");
    }
    Result<std::vector<int64_t>> ToLongArray() const override {
        return Status::Invalid("projected array do not support convert to long array");
    }
    Result<std::vector<float>> ToFloatArray() const override {
        return Status::Invalid("projected array do not support convert to float array");
    }
    Result<std::vector<double>> ToDoubleArray() const override {
        return Status::Invalid("projected array do not support convert to double array");
    }

 private:
    std::shared_ptr<InternalArray> array_;
    std::vector<int32_t> mapping_;
};

}  // namespace paimon
