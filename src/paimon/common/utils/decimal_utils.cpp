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

#include "paimon/common/utils/decimal_utils.h"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <utility>

#include "arrow/api.h"
#include "arrow/util/basic_decimal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "fmt/format.h"

namespace paimon {
Status DecimalUtils::CheckDecimalType(const arrow::DataType& type) {
    auto* decimal_type = dynamic_cast<const arrow::Decimal128Type*>(&type);
    if (!decimal_type) {
        return Status::Invalid(fmt::format("Invalid decimal type: {}", type.ToString()));
    }
    if (decimal_type->precision() > Decimal::MAX_PRECISION ||
        decimal_type->precision() < Decimal::MIN_PRECISION) {
        return Status::Invalid(fmt::format("Invalid decimal type, precision must in range [{}, {}]",
                                           Decimal::MIN_PRECISION, Decimal::MAX_PRECISION));
    }
    if (decimal_type->precision() < decimal_type->scale()) {
        return Status::Invalid(fmt::format("Invalid decimal type {}, precision must >= scale",
                                           decimal_type->ToString()));
    }
    return Status::OK();
}

std::optional<arrow::Decimal128> DecimalUtils::RescaleDecimalWithOverflowCheck(
    const arrow::Decimal128& src_decimal, int32_t src_scale, int32_t target_precision,
    int32_t target_scale) {
    arrow::Decimal128 target_decimal = src_decimal;
    if (src_scale < target_scale) {
        int32_t delta_scale = target_scale - src_scale;
        arrow::BasicDecimal128 min_bound = arrow::BasicDecimal128::GetMinSentinel();
        arrow::BasicDecimal128 max_bound = arrow::BasicDecimal128::GetMaxSentinel();
        min_bound /= arrow::BasicDecimal128::GetScaleMultiplier(delta_scale);
        max_bound /= arrow::BasicDecimal128::GetScaleMultiplier(delta_scale);
        // scaled_decimal may be overflow 128 bits
        // noted that, arrow::Decimal.Rescale() is not safe
        if (src_decimal > max_bound || src_decimal < min_bound) {
            return std::nullopt;
        }
        target_decimal = src_decimal.IncreaseScaleBy(delta_scale);
    } else if (src_scale > target_scale) {
        target_decimal = src_decimal.ReduceScaleBy(src_scale - target_scale, /*round=*/true);
    }
    if (!target_decimal.FitsInPrecision(target_precision)) {
        return std::nullopt;
    }
    return target_decimal;
}

Result<Decimal::int128_t> DecimalUtils::StrToInt128(const std::string& str) {
    try {
        size_t length = str.length();
        if (length == 0) {
            return Status::Invalid("invalid string: [], cannot convert to int128");
        }
        bool is_negative = str[0] == '-';
        size_t posn = is_negative ? 1 : 0;
        if (posn == length) {
            return Status::Invalid(
                fmt::format("invalid string: [{}], cannot convert to int128", str));
        }

        Decimal::uint128_t magnitude = 0;
        Decimal::uint128_t max_magnitude = (static_cast<Decimal::uint128_t>(1) << 127) - 1;
        if (is_negative) {
            max_magnitude += 1;
        }
        while (posn < length) {
            size_t group = std::min(18ul, length - posn);
            for (size_t i = 0; i < group; ++i) {
                if (!std::isdigit(static_cast<unsigned char>(str[posn + i]))) {
                    return Status::Invalid(
                        fmt::format("invalid string: [{}], cannot convert to int128", str));
                }
            }
            uint64_t chunk = std::stoull(str.substr(posn, group));
            uint64_t multiple = 1;
            for (size_t i = 0; i < group; ++i) {
                multiple *= 10;
            }
            if (magnitude > (max_magnitude - chunk) / multiple) {
                return Status::Invalid(
                    fmt::format("invalid string: [{}], cannot convert to int128", str));
            }
            magnitude = magnitude * multiple + chunk;
            posn += group;
        }
        if (is_negative) {
            if (magnitude == max_magnitude) {
                auto max_value =
                    static_cast<Decimal::int128_t>((static_cast<Decimal::uint128_t>(1) << 127) - 1);
                return -max_value - 1;
            }
            return -static_cast<Decimal::int128_t>(magnitude);
        }
        return static_cast<Decimal::int128_t>(magnitude);
    } catch (...) {
        return Status::Invalid(fmt::format("invalid string: [{}], cannot convert to int128", str));
    }
}

}  // namespace paimon
