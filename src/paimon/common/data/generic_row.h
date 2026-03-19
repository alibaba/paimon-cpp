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

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/result.h"

namespace paimon {
class Bytes;
class InternalArray;
class InternalMap;
/// An internal data structure representing data of row.

/// GenericRow is a generic implementation of `InternalRow` which is backed by an array of
/// VariantType. A GenericRow can have an arbitrary number of fields of different types. The fields
/// in a row can be accessed by position (0-based) using either the generic `GetField()` or
/// type-specific getters (such as `GetInt()`). A field can be updated by the generic `SetField()`.

/// @note All fields of this data structure must be internal data structures. See `InternalRow`
/// for more information about internal data structures. The fields in GenericRow can be null for
/// representing nullability. Noted that GenericRow does not support nested type (i.e., Map, Array,
/// Struct).
class GenericRow : public InternalRow {
 public:
    /// Creates an instance of GenericRow with given number of fields.
    /// Initially, all fields are set to null. By default, the row describes a
    /// `RowKind::Insert()` in a changelog.
    /// @note All fields of the row must be internal data structures.
    /// @param arity number of fields
    explicit GenericRow(int32_t arity) : GenericRow(RowKind::Insert(), arity) {}

    /// Creates an instance of GenericRow with given kind and number of fields.
    /// Initially, all fields are set to null.
    /// @note All fields of the row must be internal data structures.
    /// @param kind kind of change that this row describes in a changelog
    /// @param arity number of fields
    GenericRow(const RowKind* kind, int32_t arity) : kind_(kind) {
        fields_.resize(arity);
    }

    /// Sets the field value at the given position.
    /// @note The given field value must be an internal data structures. Otherwise the
    /// GenericRow is corrupted. See `InternalRow` for more information about internal data
    /// structures. The field value can be null for representing nullability.
    void SetField(int32_t pos, const VariantType& value) {
        assert(static_cast<size_t>(pos) < fields_.size());
        fields_[pos] = value;
    }

    /// @return the field value at the given position.
    /// @note The returned value is in internal data structure. See `InternalRow`
    /// for more information about internal data structures.
    /// The returned field value can be null for representing nullability.
    const VariantType& GetField(int32_t pos) const {
        assert(static_cast<size_t>(pos) < fields_.size());
        return fields_[pos];
    }

    int32_t GetFieldCount() const override {
        return fields_.size();
    }

    Result<const RowKind*> GetRowKind() const override {
        return kind_;
    }

    void SetRowKind(const RowKind* kind) override {
        kind_ = kind;
    }

    bool IsNullAt(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < fields_.size());
        return DataDefine::IsVariantNull(fields_[pos]);
    }

    void AddDataHolder(std::unique_ptr<InternalRow>&& holder) {
        holders_.push_back(std::move(holder));
    }

    bool GetBoolean(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < fields_.size());
        return DataDefine::GetVariantValue<bool>(fields_[pos]);
    }

    char GetByte(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < fields_.size());
        return DataDefine::GetVariantValue<char>(fields_[pos]);
    }

    int16_t GetShort(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < fields_.size());
        return DataDefine::GetVariantValue<int16_t>(fields_[pos]);
    }

    int32_t GetInt(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < fields_.size());
        return DataDefine::GetVariantValue<int32_t>(fields_[pos]);
    }

    int32_t GetDate(int32_t pos) const override {
        return GetInt(pos);
    }

    int64_t GetLong(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < fields_.size());
        return DataDefine::GetVariantValue<int64_t>(fields_[pos]);
    }

    float GetFloat(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < fields_.size());
        return DataDefine::GetVariantValue<float>(fields_[pos]);
    }

    double GetDouble(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < fields_.size());
        return DataDefine::GetVariantValue<double>(fields_[pos]);
    }

    BinaryString GetString(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < fields_.size());
        return DataDefine::GetVariantValue<BinaryString>(fields_[pos]);
    }

    std::string_view GetStringView(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < fields_.size());
        return DataDefine::GetVariantValue<std::string_view>(fields_[pos]);
    }

    Decimal GetDecimal(int32_t pos, int32_t precision, int32_t scale) const override {
        assert(static_cast<size_t>(pos) < fields_.size());
        return DataDefine::GetVariantValue<Decimal>(fields_[pos]);
    }

    Timestamp GetTimestamp(int32_t pos, int32_t precision) const override {
        assert(static_cast<size_t>(pos) < fields_.size());
        return DataDefine::GetVariantValue<Timestamp>(fields_[pos]);
    }

    std::shared_ptr<Bytes> GetBinary(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < fields_.size());
        return DataDefine::GetVariantValue<std::shared_ptr<Bytes>>(fields_[pos]);
    }

    std::shared_ptr<InternalArray> GetArray(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < fields_.size());
        return DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(fields_[pos]);
    }

    std::shared_ptr<InternalMap> GetMap(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < fields_.size());
        return DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(fields_[pos]);
    }

    std::shared_ptr<InternalRow> GetRow(int32_t pos, int32_t num_fields) const override {
        assert(static_cast<size_t>(pos) < fields_.size());
        return DataDefine::GetVariantValue<std::shared_ptr<InternalRow>>(fields_[pos]);
    }

    std::string ToString() const override {
        std::string ret = "(";
        for (size_t i = 0; i < fields_.size(); i++) {
            if (i != 0) {
                ret.append(",");
            }
            ret.append(DataDefine::VariantValueToString(fields_[i]));
        }
        ret.append(")");
        return ret;
    }

    bool operator==(const GenericRow& other) const {
        if (this == &other) {
            return true;
        }
        return *kind_ == *other.kind_ && fields_ == other.fields_;
    }

    /// Creates an instance of GenericRow with given field values.
    static std::unique_ptr<GenericRow> Of(const std::vector<VariantType>& values) {
        auto row = std::make_unique<GenericRow>(values.size());
        for (size_t i = 0; i < values.size(); i++) {
            row->SetField(i, values[i]);
        }
        return row;
    }

 private:
    /// The array to store the actual internal format values.
    std::vector<VariantType> fields_;
    /// As GenericRow only holds string view for string data to avoid deep copy, original data must
    /// be held in holders_
    std::vector<std::unique_ptr<InternalRow>> holders_;
    /// The kind of change that a row describes in a changelog.
    const RowKind* kind_;
};

}  // namespace paimon
