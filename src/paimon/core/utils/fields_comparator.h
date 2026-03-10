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
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/types/data_field.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {
class DataField;

/// A `Comparator` that compares the file store key.
class FieldsComparator {
 public:
    static Result<std::unique_ptr<FieldsComparator>> Create(
        const std::vector<DataField>& input_data_field, bool is_ascending_order, bool use_view);

    static Result<std::unique_ptr<FieldsComparator>> Create(
        const std::vector<DataField>& input_data_field, const std::vector<int32_t>& sort_fields,
        bool is_ascending_order, bool use_view);

    int32_t CompareTo(const InternalRow& lhs, const InternalRow& rhs) const;

    const std::vector<int32_t>& CompareFields() const {
        return sort_fields_;
    }

    using VariantComparatorFunc =
        std::function<int32_t(const VariantType& lhs, const VariantType& rhs)>;

    static Result<VariantComparatorFunc> CompareVariant(
        int32_t field_idx, const std::shared_ptr<arrow::DataType>& input_type, bool use_view);

    /// Java-compatible ordering for floating-point types:
    /// -infinity < -0.0 < +0.0 < +infinity < NaN == NaN
    /// for range index and sst key comparator
    template <typename T>
    static int32_t CompareFloatingPoint(T a, T b) {
        const bool a_nan = std::isnan(a);
        const bool b_nan = std::isnan(b);
        if (a_nan && b_nan) {
            return 0;
        }
        if (a_nan) {
            return 1;
        }
        if (b_nan) {
            return -1;
        }
        if (a == b) {
            const bool a_neg = std::signbit(a);
            const bool b_neg = std::signbit(b);
            if (a_neg == b_neg) {
                return 0;
            }
            return a_neg ? -1 : 1;  // -0.0 < +0.0
        }
        return a < b ? -1 : 1;
    }

 private:
    using FieldComparatorFunc =
        std::function<int32_t(const InternalRow& lhs, const InternalRow& rhs)>;

    FieldsComparator(bool is_ascending_order, const std::vector<int32_t>& sort_fields,
                     std::vector<FieldComparatorFunc>&& comparators)
        : is_ascending_order_(is_ascending_order),
          sort_fields_(sort_fields),
          comparators_(std::move(comparators)) {
        assert(comparators_.size() == sort_fields_.size());
    }

    static Result<FieldComparatorFunc> CompareField(
        int32_t field_idx, const std::shared_ptr<arrow::DataType>& input_type, bool use_view);

 private:
    bool is_ascending_order_;
    std::vector<int32_t> sort_fields_;
    std::vector<FieldComparatorFunc> comparators_;
};
}  // namespace paimon
