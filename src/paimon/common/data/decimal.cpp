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

#include "paimon/data/decimal.h"

#include <algorithm>
#include <cmath>
#include <cstdlib>

#include "arrow/api.h"
#include "arrow/scalar.h"
#include "arrow/util/decimal.h"
#include "paimon/io/byte_order.h"
#include "paimon/memory/bytes.h"

namespace paimon {
const int64_t Decimal::POWERS_OF_TEN[MAX_COMPACT_PRECISION + 1] = {1,
                                                                   10,
                                                                   100,
                                                                   1000,
                                                                   10000,
                                                                   100000,
                                                                   1000000l,
                                                                   10000000l,
                                                                   100000000l,
                                                                   1000000000l,
                                                                   10000000000l,
                                                                   100000000000l,
                                                                   1000000000000l,
                                                                   10000000000000l,
                                                                   100000000000000l,
                                                                   1000000000000000l,
                                                                   10000000000000000l,
                                                                   100000000000000000l,
                                                                   1000000000000000000l};
const Decimal::int128_t Decimal::INT128_MAXIMUM_VALUE = static_cast<Decimal::int128_t>(
    static_cast<Decimal::uint128_t>(0x7fffffffffffffffULL) << 64 | 0xffffffffffffffff);
const Decimal::int128_t Decimal::INT128_MINIMUM_VALUE =
    static_cast<Decimal::int128_t>(static_cast<Decimal::uint128_t>(0x8000000000000000ULL) << 64);

std::string Decimal::ToString() const {
    auto type = arrow::decimal128(Precision(), Scale());
    arrow::Decimal128Scalar scalar(arrow::Decimal128(HighBits(), LowBits()), type);
    return scalar.ToString();
}

std::vector<char> Decimal::ToUnscaledBytes() const {
    bool positive = value_ >= 0;
    int32_t valid_bytes = 0;
    if (positive) {
        int32_t leading_zero_bytes = count_leading_zero_bytes(value_);
        valid_bytes = sizeof(value_) - leading_zero_bytes;
    } else {
        int32_t leading_all_ones_bytes = count_leading_all_ones_bytes(value_);
        valid_bytes = sizeof(value_) - leading_all_ones_bytes;
    }
    if (valid_bytes == 0) {
        // if value_ == 0, return one byte with 0;
        // if value_ == 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF, return one byte with 0xFF
        valid_bytes = 1;
    } else {
        // java BigInteger use highest significant bit to determine if the number is positive or
        // negative, e.g., a positive BigInteger 0xFF, will return two bytes [0x00, 0xFF]
        bool highest_significant_bit =
            (static_cast<Decimal::uint128_t>(value_) >> ((valid_bytes - 1) * 8)) & 0x80;
        if ((positive && highest_significant_bit) || (!positive && !highest_significant_bit)) {
            valid_bytes += 1;
        }
    }
    std::vector<char> bytes(valid_bytes);
    memcpy(bytes.data(), &value_, valid_bytes);
    if (SystemByteOrder() == ByteOrder::PAIMON_LITTLE_ENDIAN) {
        std::reverse(bytes.data(), bytes.data() + bytes.size());
    }
    return bytes;
}

Decimal Decimal::FromUnscaledBytes(int32_t precision, int32_t scale, Bytes* bytes) {
    if (SystemByteOrder() == ByteOrder::PAIMON_LITTLE_ENDIAN) {
        std::reverse(bytes->data(), bytes->data() + bytes->size());
    }
    Decimal::int128_t value = 0;
    for (size_t i = 0; i < bytes->size(); ++i) {
        value |= static_cast<Decimal::uint128_t>(static_cast<uint8_t>((*bytes)[i])) << (8 * i);
    }
    // for negative
    if ((*bytes)[bytes->size() - 1] & 0x80) {
        for (size_t i = bytes->size(); i < sizeof(Decimal::int128_t); ++i) {
            value |= static_cast<Decimal::uint128_t>(0xFFull) << (8 * i);
        }
    }
    return Decimal(precision, scale, value);
}

int32_t Decimal::clz_u128(uint128_t u) {
    uint64_t hi = u >> 64;
    if (hi != 0) {
        return __builtin_clzll(hi);
    }
    uint64_t lo = u;
    if (lo != 0) {
        return __builtin_clzll(lo) + 64;
    }
    return 128;
}

int32_t Decimal::count_leading_zero_bytes(uint128_t u) {
    if (u == 0) {
        return sizeof(u);
    }
    int32_t leading_zeros = clz_u128(u);
    return leading_zeros / 8;
}

int32_t Decimal::count_leading_all_ones_bytes(uint128_t u) {
    if (u == 0) {
        return 0;
    }
    int32_t count = 0;
    for (int32_t i = sizeof(uint128_t) - 1; i >= 0; i--) {
        if (((u >> (i * 8)) & 0xFF) == 0xFF) {
            count++;
        } else {
            break;
        }
    }
    return count;
}

Decimal::int128_t Decimal::DownScaleInt128(Decimal::int128_t value, int32_t scale) {
    while (scale > 0) {
        int32_t step = std::min(std::abs(scale), MAX_COMPACT_PRECISION);
        value /= POWERS_OF_TEN[step];
        scale -= step;
    }
    return value;
}

Decimal::int128_t Decimal::ScaleInt128(Decimal::int128_t value, int32_t scale, bool* overflow) {
    *overflow = false;
    while (scale > 0) {
        int32_t step = std::min(scale, MAX_COMPACT_PRECISION);
        if (value > 0 && INT128_MAXIMUM_VALUE / POWERS_OF_TEN[step] < value) {
            *overflow = true;
            return INT128_MAXIMUM_VALUE;
        } else if (value < 0 && INT128_MINIMUM_VALUE / POWERS_OF_TEN[step] > value) {
            *overflow = true;
            return INT128_MINIMUM_VALUE;
        }

        value *= POWERS_OF_TEN[step];
        scale -= step;
    }
    return value;
}

int32_t Decimal::CompareTo(const Decimal& other) const {
    auto l_value = value_;
    auto l_scale = scale_;
    auto r_value = other.value_;
    auto r_scale = other.scale_;

    bool l_positive = l_value >= 0;
    bool r_positive = r_value >= 0;
    if (l_positive && !r_positive) {
        return 1;
    } else if (!l_positive && r_positive) {
        return -1;
    }

    // compare integral parts
    Decimal::int128_t l_integral = DownScaleInt128(l_value, l_scale);
    Decimal::int128_t r_integral = DownScaleInt128(r_value, r_scale);

    if (l_integral < r_integral) {
        return -1;
    } else if (l_integral > r_integral) {
        return 1;
    }

    // integral parts are equal, continue comparing fractional parts
    // unnecessary to check overflow here because the scaled number will not
    // exceed original ones
    bool overflow = false, positive = l_value >= 0;
    l_value -= ScaleInt128(l_integral, l_scale, &overflow);
    r_value -= ScaleInt128(r_integral, r_scale, &overflow);

    int32_t diff = l_scale - r_scale;
    if (diff > 0) {
        r_value = ScaleInt128(r_value, diff, &overflow);
        if (overflow) {
            return positive ? -1 : 1;
        }
    } else {
        l_value = ScaleInt128(l_value, -diff, &overflow);
        if (overflow) {
            return positive ? 1 : -1;
        }
    }

    if (l_value < r_value) {
        return -1;
    } else if (l_value > r_value) {
        return 1;
    } else {
        return 0;
    }
}

}  // namespace paimon
