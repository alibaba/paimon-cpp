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

#include "paimon/core/utils/fields_comparator.h"

#include <cstddef>
#include <string>

#include "arrow/api.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"

namespace paimon {
Result<std::unique_ptr<FieldsComparator>> FieldsComparator::Create(
    const std::vector<DataField>& input_data_field, bool is_ascending_order, bool use_view) {
    std::vector<int32_t> sort_fields;
    sort_fields.reserve(input_data_field.size());
    for (int32_t i = 0; i < static_cast<int32_t>(input_data_field.size()); i++) {
        sort_fields.push_back(i);
    }
    return Create(input_data_field, sort_fields, is_ascending_order, use_view);
}

Result<std::unique_ptr<FieldsComparator>> FieldsComparator::Create(
    const std::vector<DataField>& input_data_field, const std::vector<int32_t>& sort_fields,
    bool is_ascending_order, bool use_view) {
    std::vector<FieldComparatorFunc> comparators;
    comparators.reserve(sort_fields.size());
    for (const auto& sort_field_idx : sort_fields) {
        const auto& type = input_data_field[sort_field_idx].Type();
        PAIMON_ASSIGN_OR_RAISE(FieldComparatorFunc cmp,
                               CompareField(sort_field_idx, type, use_view));
        comparators.emplace_back(cmp);
    }
    return std::unique_ptr<FieldsComparator>(
        new FieldsComparator(is_ascending_order, sort_fields, std::move(comparators)));
}

int32_t FieldsComparator::CompareTo(const InternalRow& lhs, const InternalRow& rhs) const {
    // in default comparator, null is first (not smallest)
    int32_t null_is_last_ret = -1;
    for (size_t i = 0; i < sort_fields_.size(); i++) {
        bool lhs_null = lhs.IsNullAt(sort_fields_[i]);
        bool rhs_null = rhs.IsNullAt(sort_fields_[i]);
        if (lhs_null && rhs_null) {
            // Continue to compare the next element
        } else if (lhs_null) {
            return null_is_last_ret;
        } else if (rhs_null) {
            return -null_is_last_ret;
        } else {
            int32_t comp = comparators_[i](lhs, rhs);
            if (comp != 0) {
                return is_ascending_order_ ? comp : -comp;
            }
        }
    }
    return 0;
}

Result<FieldsComparator::FieldComparatorFunc> FieldsComparator::CompareField(
    int32_t field_idx, const std::shared_ptr<arrow::DataType>& input_type, bool use_view) {
    arrow::Type::type type = input_type->id();
    switch (type) {
        case arrow::Type::type::BOOL:
            return FieldsComparator::FieldComparatorFunc(
                [field_idx](const InternalRow& lhs, const InternalRow& rhs) -> int32_t {
                    bool lvalue = lhs.GetBoolean(field_idx);
                    bool rvalue = rhs.GetBoolean(field_idx);
                    return lvalue == rvalue ? 0 : (lvalue < rvalue ? -1 : 1);
                });
        case arrow::Type::type::INT8:
            return FieldsComparator::FieldComparatorFunc(
                [field_idx](const InternalRow& lhs, const InternalRow& rhs) -> int32_t {
                    int8_t lvalue = lhs.GetByte(field_idx);
                    int8_t rvalue = rhs.GetByte(field_idx);
                    return lvalue == rvalue ? 0 : (lvalue < rvalue ? -1 : 1);
                });
        case arrow::Type::type::INT16:
            return FieldsComparator::FieldComparatorFunc(
                [field_idx](const InternalRow& lhs, const InternalRow& rhs) -> int32_t {
                    int16_t lvalue = lhs.GetShort(field_idx);
                    int16_t rvalue = rhs.GetShort(field_idx);
                    return lvalue == rvalue ? 0 : (lvalue < rvalue ? -1 : 1);
                });
        case arrow::Type::type::DATE32:
            return FieldsComparator::FieldComparatorFunc(
                [field_idx](const InternalRow& lhs, const InternalRow& rhs) -> int32_t {
                    int32_t lvalue = lhs.GetDate(field_idx);
                    int32_t rvalue = rhs.GetDate(field_idx);
                    return lvalue == rvalue ? 0 : (lvalue < rvalue ? -1 : 1);
                });

        case arrow::Type::type::INT32:
            return FieldsComparator::FieldComparatorFunc(
                [field_idx](const InternalRow& lhs, const InternalRow& rhs) -> int32_t {
                    int32_t lvalue = lhs.GetInt(field_idx);
                    int32_t rvalue = rhs.GetInt(field_idx);
                    return lvalue == rvalue ? 0 : (lvalue < rvalue ? -1 : 1);
                });
        case arrow::Type::type::INT64:
            return FieldsComparator::FieldComparatorFunc(
                [field_idx](const InternalRow& lhs, const InternalRow& rhs) -> int32_t {
                    int64_t lvalue = lhs.GetLong(field_idx);
                    int64_t rvalue = rhs.GetLong(field_idx);
                    return lvalue == rvalue ? 0 : (lvalue < rvalue ? -1 : 1);
                });
        case arrow::Type::type::FLOAT:
            return FieldsComparator::FieldComparatorFunc(
                [field_idx](const InternalRow& lhs, const InternalRow& rhs) -> int32_t {
                    float lvalue = lhs.GetFloat(field_idx);
                    float rvalue = rhs.GetFloat(field_idx);
                    return lvalue == rvalue ? 0 : (lvalue < rvalue ? -1 : 1);
                });
        case arrow::Type::type::DOUBLE:
            return FieldsComparator::FieldComparatorFunc(
                [field_idx](const InternalRow& lhs, const InternalRow& rhs) -> int32_t {
                    double lvalue = lhs.GetDouble(field_idx);
                    double rvalue = rhs.GetDouble(field_idx);
                    return lvalue == rvalue ? 0 : (lvalue < rvalue ? -1 : 1);
                });
        case arrow::Type::type::STRING: {
            if (use_view) {
                return FieldsComparator::FieldComparatorFunc(
                    [field_idx](const InternalRow& lhs, const InternalRow& rhs) -> int32_t {
                        auto lvalue = lhs.GetStringView(field_idx);
                        auto rvalue = rhs.GetStringView(field_idx);
                        int32_t cmp = lvalue.compare(rvalue);
                        return cmp == 0 ? 0 : (cmp > 0 ? 1 : -1);
                    });
            } else {
                return FieldsComparator::FieldComparatorFunc(
                    [field_idx](const InternalRow& lhs, const InternalRow& rhs) -> int32_t {
                        auto lvalue = lhs.GetString(field_idx);
                        auto rvalue = rhs.GetString(field_idx);
                        int32_t cmp = lvalue.CompareTo(rvalue);
                        return cmp == 0 ? 0 : (cmp > 0 ? 1 : -1);
                    });
            }
        }
        case arrow::Type::type::BINARY: {
            // TODO(xinyu.lxy): may use 64byte compare
            if (use_view) {
                return FieldsComparator::FieldComparatorFunc(
                    [field_idx](const InternalRow& lhs, const InternalRow& rhs) -> int32_t {
                        auto lvalue = lhs.GetStringView(field_idx);
                        auto rvalue = rhs.GetStringView(field_idx);
                        int32_t cmp = lvalue.compare(rvalue);
                        return cmp == 0 ? 0 : (cmp > 0 ? 1 : -1);
                    });
            } else {
                return FieldsComparator::FieldComparatorFunc(
                    [field_idx](const InternalRow& lhs, const InternalRow& rhs) -> int32_t {
                        auto lvalue = lhs.GetBinary(field_idx);
                        auto rvalue = rhs.GetBinary(field_idx);
                        int32_t cmp = lvalue->compare(*rvalue);
                        return cmp == 0 ? 0 : (cmp > 0 ? 1 : -1);
                    });
            }
        }
        case arrow::Type::type::TIMESTAMP: {
            auto timestamp_type =
                arrow::internal::checked_pointer_cast<arrow::TimestampType>(input_type);
            assert(timestamp_type);
            int32_t precision = DateTimeUtils::GetPrecisionFromType(timestamp_type);
            return FieldsComparator::FieldComparatorFunc(
                [field_idx, precision](const InternalRow& lhs, const InternalRow& rhs) -> int32_t {
                    Timestamp lvalue = lhs.GetTimestamp(field_idx, precision);
                    Timestamp rvalue = rhs.GetTimestamp(field_idx, precision);
                    return lvalue == rvalue ? 0 : (lvalue < rvalue ? -1 : 1);
                });
        }
        case arrow::Type::type::DECIMAL: {
            auto* decimal_type =
                arrow::internal::checked_cast<arrow::Decimal128Type*>(input_type.get());
            assert(decimal_type);
            auto precision = decimal_type->precision();
            auto scale = decimal_type->scale();
            return FieldsComparator::FieldComparatorFunc(
                [field_idx, precision, scale](const InternalRow& lhs,
                                              const InternalRow& rhs) -> int32_t {
                    Decimal lvalue = lhs.GetDecimal(field_idx, precision, scale);
                    Decimal rvalue = rhs.GetDecimal(field_idx, precision, scale);
                    int32_t cmp = lvalue.CompareTo(rvalue);
                    return cmp == 0 ? 0 : (cmp > 0 ? 1 : -1);
                });
        }
        default:
            return Status::NotImplemented(fmt::format("Do not support comparing {} type in idx {}",
                                                      input_type->ToString(), field_idx));
    }
}

}  // namespace paimon
