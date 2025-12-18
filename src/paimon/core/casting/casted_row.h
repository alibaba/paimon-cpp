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
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/type_fwd.h"
#include "fmt/format.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/casting/cast_executor.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {
class DataField;
class InternalArray;
class InternalMap;
class Literal;
class RowKind;

/// An implementation of `InternalRow` which provides a casted view of the underlying `InternalRow`.
///
/// It reads data from underlying `InternalRow` according to source logical type and casts
/// it with specific `CastExecutor`.
/// @note When use CastedRow, we need to catch exceptions
class CastedRow : public InternalRow {
 public:
    static Result<std::unique_ptr<CastedRow>> Create(
        const std::vector<std::shared_ptr<CastExecutor>>& cast_executors,
        const std::vector<DataField>& src_fields, const std::vector<DataField>& target_fields,
        const std::shared_ptr<InternalRow>& row);

    int32_t GetFieldCount() const override {
        return row_->GetFieldCount();
    }

    Result<const RowKind*> GetRowKind() const override {
        return row_->GetRowKind();
    }

    void SetRowKind(const RowKind* kind) override {
        row_->SetRowKind(kind);
    }

    bool IsNullAt(int32_t pos) const override {
        return row_->IsNullAt(pos);
    }

    template <typename T>
    T GetValue(int32_t pos, const std::shared_ptr<arrow::DataType>& target_type) const {
        assert(static_cast<size_t>(pos) < field_getters_.size());
        auto value = field_getters_[pos](*row_);
        if (!cast_executors_[pos]) {
            return DataDefine::GetVariantValue<T>(value);
        }
        return CastValue<T>(pos, value, target_type);
    }

    template <typename T>
    T CastValue(int32_t pos, const VariantType& value,
                const std::shared_ptr<arrow::DataType>& target_type) const {
        auto literal = DataDefine::VariantValueToLiteral(value, src_types_[pos]);
        if (!literal.ok()) {
            throw std::invalid_argument(literal.status().ToString());
        }
        Result<Literal> cast_ret = cast_executors_[pos]->Cast(literal.value(), target_type);
        if (!cast_ret.ok()) {
            throw std::invalid_argument(cast_ret.status().ToString());
        }
        return cast_ret.value().GetValue<T>();
    }

    template <typename T>
    T GetNestedValue(int32_t pos) const {
        assert(static_cast<size_t>(pos) < field_getters_.size());
        auto value = field_getters_[pos](*row_);
        assert(!cast_executors_[pos]);
        return DataDefine::GetVariantValue<T>(value);
    }

    bool GetBoolean(int32_t pos) const override {
        return GetValue<bool>(pos, arrow::boolean());
    }

    char GetByte(int32_t pos) const override {
        // TODO(liancheng.lsz): use char rather than int8_t
        assert(static_cast<size_t>(pos) < field_getters_.size());
        auto value = field_getters_[pos](*row_);
        if (!cast_executors_[pos]) {
            return DataDefine::GetVariantValue<char>(value);
        }
        return CastValue<int8_t>(pos, value, arrow::int8());
    }

    int16_t GetShort(int32_t pos) const override {
        return GetValue<int16_t>(pos, arrow::int16());
    }

    int32_t GetInt(int32_t pos) const override {
        return GetValue<int32_t>(pos, arrow::int32());
    }

    int32_t GetDate(int32_t pos) const override {
        return GetValue<int32_t>(pos, arrow::date32());
    }

    int64_t GetLong(int32_t pos) const override {
        return GetValue<int64_t>(pos, arrow::int64());
    }

    float GetFloat(int32_t pos) const override {
        return GetValue<float>(pos, arrow::float32());
    }

    double GetDouble(int32_t pos) const override {
        return GetValue<double>(pos, arrow::float64());
    }

    BinaryString GetString(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < field_getters_.size());
        auto value = field_getters_[pos](*row_);
        if (!cast_executors_[pos]) {
            return DataDefine::GetVariantValue<BinaryString>(value);
        }
        auto str = CastValue<std::string>(pos, value, arrow::utf8());
        return BinaryString::FromString(str, GetDefaultPool().get());
    }

    std::string_view GetStringView(int32_t pos) const override {
        assert(false);
        throw std::invalid_argument("cannot get string view in casted row");
    }

    Decimal GetDecimal(int32_t pos, int32_t precision, int32_t scale) const override {
        return GetValue<Decimal>(pos, arrow::decimal128(precision, scale));
    }

    Timestamp GetTimestamp(int32_t pos, int32_t precision) const override {
        // timestamp does not support casting
        Result<std::shared_ptr<arrow::DataType>> ts_type =
            DateTimeUtils::GetTypeFromPrecision(precision, /*with_timezone=*/false);
        if (!ts_type.ok()) {
            throw std::invalid_argument(ts_type.status().ToString());
        }
        return GetValue<Timestamp>(pos, ts_type.value());
    }

    std::shared_ptr<Bytes> GetBinary(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < field_getters_.size());
        auto value = field_getters_[pos](*row_);
        if (!cast_executors_[pos]) {
            return DataDefine::GetVariantValue<std::shared_ptr<Bytes>>(value);
        }
        auto str = CastValue<std::string>(pos, value, arrow::binary());
        return std::make_shared<Bytes>(str, GetDefaultPool().get());
    }

    std::shared_ptr<InternalArray> GetArray(int32_t pos) const override {
        return GetNestedValue<std::shared_ptr<InternalArray>>(pos);
    }

    std::shared_ptr<InternalMap> GetMap(int32_t pos) const override {
        return GetNestedValue<std::shared_ptr<InternalMap>>(pos);
    }

    std::shared_ptr<InternalRow> GetRow(int32_t pos, int32_t num_fields) const override {
        return GetNestedValue<std::shared_ptr<InternalRow>>(pos);
    }

    std::string ToString() const override {
        return fmt::format("casted row, inner row = {}", row_->ToString());
    }

 private:
    CastedRow(const std::vector<std::shared_ptr<CastExecutor>>& cast_executors,
              std::vector<InternalRow::FieldGetterFunc>&& field_getters,
              std::vector<arrow::Type::type>&& src_types, const std::shared_ptr<InternalRow>& row)
        : cast_executors_(cast_executors),
          field_getters_(std::move(field_getters)),
          src_types_(std::move(src_types)),
          row_(row) {
        assert(row_);
    }

 private:
    std::vector<std::shared_ptr<CastExecutor>> cast_executors_;
    std::vector<InternalRow::FieldGetterFunc> field_getters_;
    std::vector<arrow::Type::type> src_types_;
    std::shared_ptr<InternalRow> row_;
};
}  // namespace paimon
