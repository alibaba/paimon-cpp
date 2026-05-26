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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "fmt/format.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/columnar/columnar_batch_context.h"
#include "paimon/common/data/columnar/columnar_utils.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_map.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/result.h"

namespace paimon {
class Bytes;

/// Columnar row view which shares batch-level context to reduce per-row overhead.
/// Uses pre-cached column metadata for fast field access without virtual function calls.
class ColumnarRowRef : public InternalRow {
 public:
    ColumnarRowRef(std::shared_ptr<ColumnarBatchContext> ctx, int64_t row_id)
        : ctx_(std::move(ctx)), cached_meta_ptr_(ctx_->cached_meta.data()), row_id_(row_id) {}

    Result<const RowKind*> GetRowKind() const override {
        return row_kind_;
    }

    void SetRowKind(const RowKind* kind) override {
        row_kind_ = kind;
    }

    int32_t GetFieldCount() const override {
        return static_cast<int32_t>(ctx_->array_vec.size());
    }

    bool IsNullAt(int32_t pos) const override {
        return cached_meta_ptr_[pos].IsNull(row_id_);
    }

    bool GetBoolean(int32_t pos) const override {
        return cached_meta_ptr_[pos].GetBool(row_id_);
    }

    char GetByte(int32_t pos) const override {
        return static_cast<char>(cached_meta_ptr_[pos].GetFixed<int8_t>(row_id_));
    }

    int16_t GetShort(int32_t pos) const override {
        return cached_meta_ptr_[pos].GetFixed<int16_t>(row_id_);
    }

    int32_t GetInt(int32_t pos) const override {
        return cached_meta_ptr_[pos].GetFixed<int32_t>(row_id_);
    }

    int32_t GetDate(int32_t pos) const override {
        return cached_meta_ptr_[pos].GetFixed<int32_t>(row_id_);
    }

    int64_t GetLong(int32_t pos) const override {
        return cached_meta_ptr_[pos].GetFixed<int64_t>(row_id_);
    }

    float GetFloat(int32_t pos) const override {
        return cached_meta_ptr_[pos].GetFixed<float>(row_id_);
    }

    double GetDouble(int32_t pos) const override {
        return cached_meta_ptr_[pos].GetFixed<double>(row_id_);
    }

    BinaryString GetString(int32_t pos) const override {
        auto bytes = ColumnarUtils::GetBytes<arrow::StringType>(ctx_->array_vec[pos].get(), row_id_,
                                                                ctx_->pool.get());
        return BinaryString::FromBytes(bytes);
    }

    std::string_view GetStringView(int32_t pos) const override {
        auto& meta = cached_meta_ptr_[pos];
        if (meta.values_data && meta.offsets) {
            return meta.GetVarLenView(row_id_);
        }
        // Fallback for dictionary-encoded or uncached types
        return ColumnarUtils::GetView(ctx_->array_vec[pos].get(), row_id_);
    }

    Decimal GetDecimal(int32_t pos, int32_t precision, int32_t scale) const override;

    Timestamp GetTimestamp(int32_t pos, int32_t precision) const override;

    std::shared_ptr<Bytes> GetBinary(int32_t pos) const override {
        return ColumnarUtils::GetBytes<arrow::BinaryType>(ctx_->array_vec[pos].get(), row_id_,
                                                          ctx_->pool.get());
    }

    std::shared_ptr<InternalRow> GetRow(int32_t pos, int32_t num_fields) const override;

    std::shared_ptr<InternalArray> GetArray(int32_t pos) const override;

    std::shared_ptr<InternalMap> GetMap(int32_t pos) const override;

    std::string ToString() const override {
        return fmt::format("ColumnarRowRef, row_id {}", row_id_);
    }

 private:
    std::shared_ptr<ColumnarBatchContext> ctx_;
    const CachedColumnMeta* cached_meta_ptr_;
    const RowKind* row_kind_ = RowKind::Insert();
    int64_t row_id_;
};
}  // namespace paimon
