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
#include <string>
#include <string_view>
#include <vector>

#include "fmt/format.h"
#include "fmt/ranges.h"
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

/// An implementation of `InternalRow` which provides a projected view of the underlying
/// `InternalRow`.
///
/// Projection includes both reducing the accessible fields and reordering them.
///
/// @note This class supports only top-level projections, not nested projections.
class ProjectedRow : public InternalRow {
 public:
    // e.g., mapping = [2, 1, -1],
    // GetInt(pos = 0) return inner row->GetInt(pos = 2)
    // -1 in mapping indicates null, IsNullAt(pos = 2) return true
    ProjectedRow(const std::shared_ptr<InternalRow>& row, const std::vector<int32_t>& mapping)
        : row_(row), mapping_(mapping) {
        assert(row_);
    }

    int32_t GetFieldCount() const override {
        return mapping_.size();
    }

    Result<const RowKind*> GetRowKind() const override {
        return row_->GetRowKind();
    }

    void SetRowKind(const RowKind* kind) override {
        row_->SetRowKind(kind);
    }

    bool IsNullAt(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        if (mapping_[pos] < 0) {
            return true;
        }
        return row_->IsNullAt(mapping_[pos]);
    }

    bool GetBoolean(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return row_->GetBoolean(mapping_[pos]);
    }

    char GetByte(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return row_->GetByte(mapping_[pos]);
    }

    int16_t GetShort(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return row_->GetShort(mapping_[pos]);
    }

    int32_t GetInt(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return row_->GetInt(mapping_[pos]);
    }

    int32_t GetDate(int32_t pos) const override {
        return GetInt(pos);
    }

    int64_t GetLong(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return row_->GetLong(mapping_[pos]);
    }

    float GetFloat(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return row_->GetFloat(mapping_[pos]);
    }

    double GetDouble(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return row_->GetDouble(mapping_[pos]);
    }

    BinaryString GetString(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return row_->GetString(mapping_[pos]);
    }

    std::string_view GetStringView(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return row_->GetStringView(mapping_[pos]);
    }

    Decimal GetDecimal(int32_t pos, int32_t precision, int32_t scale) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return row_->GetDecimal(mapping_[pos], precision, scale);
    }

    Timestamp GetTimestamp(int32_t pos, int32_t precision) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return row_->GetTimestamp(mapping_[pos], precision);
    }

    std::shared_ptr<Bytes> GetBinary(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return row_->GetBinary(mapping_[pos]);
    }

    std::shared_ptr<InternalArray> GetArray(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return row_->GetArray(mapping_[pos]);
    }

    std::shared_ptr<InternalMap> GetMap(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return row_->GetMap(mapping_[pos]);
    }

    std::shared_ptr<InternalRow> GetRow(int32_t pos, int32_t num_fields) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        return row_->GetRow(mapping_[pos], num_fields);
    }

    std::string ToString() const override {
        auto row_kind = row_->GetRowKind();
        std::string row_kind_str =
            row_kind.ok() ? row_kind.value()->ShortString() : "unknown row kind";
        return fmt::format("{} {{ indexMapping={}, mutableRow={} }}", row_kind_str,
                           fmt::join(mapping_, ", "), row_->ToString());
    }

 private:
    std::shared_ptr<InternalRow> row_;
    std::vector<int32_t> mapping_;
};

}  // namespace paimon
