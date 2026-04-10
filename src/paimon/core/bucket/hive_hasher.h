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
#include <cstring>

#include "paimon/data/decimal.h"

namespace paimon {

/// Hive-compatible hash utility functions.
/// This class provides hash functions that are compatible with Hive's ObjectInspectorUtils
/// hash implementation, ensuring consistent bucket assignment between Paimon C++ and Java.
class HiveHasher {
 public:
    /// Hash an int value (identity function, same as Hive).
    static int32_t HashInt(int32_t input) {
        return input;
    }

    /// Hash a long value (same as Java's Long.hashCode).
    static int32_t HashLong(int64_t input) {
        return static_cast<int32_t>(input ^ (static_cast<uint64_t>(input) >> 32));
    }

    /// Hash a byte array.
    static int32_t HashBytes(const char* bytes, int32_t length) {
        int32_t result = 0;
        for (int32_t i = 0; i < length; i++) {
            result = (result * 31) + static_cast<int32_t>(bytes[i]);
        }
        return result;
    }

    /// Normalize a Decimal value for Hive-compatible hashing.
    /// This implements the same logic as HiveHasher.normalizeDecimal in Java.
    ///
    /// The normalization process:
    /// 1. Strip trailing zeros
    /// 2. Check if integer digits exceed max precision (38)
    /// 3. Limit scale to min(38, min(38 - intDigits, currentScale))
    /// 4. Round if necessary using HALF_UP
    ///
    /// @param decimal The decimal value to normalize.
    /// @return The hash code of the normalized decimal, computed as Java BigDecimal.hashCode().
    static int32_t HashDecimal(const Decimal& decimal) {
        // Java BigDecimal.hashCode() = unscaledValue.intValue() * 31 + scale
        // For compact decimals (precision <= 18), we can use the long value directly.
        // For non-compact decimals, we need to handle the 128-bit value.

        // First normalize: strip trailing zeros and limit scale
        int32_t scale = decimal.Scale();
        auto value = decimal.Value();

        // Strip trailing zeros
        if (value == 0) {
            // BigDecimal.ZERO.hashCode() = 0 * 31 + 0 = 0
            return 0;
        }

        // Strip trailing zeros by dividing by 10 while remainder is 0
        while (scale > 0 && value != 0) {
            auto quotient = value / 10;
            auto remainder = value - quotient * 10;
            if (remainder != 0) {
                break;
            }
            value = quotient;
            scale--;
        }

        // After stripping, check if value is zero
        if (value == 0) {
            return 0;
        }

        // Count integer digits
        auto abs_value = value < 0 ? -value : value;
        int32_t total_digits = 0;
        auto temp = abs_value;
        while (temp > 0) {
            temp /= 10;
            total_digits++;
        }
        int32_t int_digits = total_digits - scale;

        if (int_digits > HIVE_DECIMAL_MAX_PRECISION) {
            // Overflow, return 0 (null equivalent)
            return 0;
        }

        int32_t max_scale = HIVE_DECIMAL_MAX_SCALE;
        if (HIVE_DECIMAL_MAX_PRECISION - int_digits < max_scale) {
            max_scale = HIVE_DECIMAL_MAX_PRECISION - int_digits;
        }
        if (scale < max_scale) {
            max_scale = scale;
        }

        if (scale > max_scale) {
            // Need to round: scale down with HALF_UP rounding
            int32_t scale_diff = scale - max_scale;
            for (int32_t i = 0; i < scale_diff; i++) {
                auto quotient = value / 10;
                auto remainder = value - quotient * 10;
                if (remainder < 0) remainder = -remainder;
                if (remainder >= 5) {
                    value = quotient + (value < 0 ? -1 : 1);
                } else {
                    value = quotient;
                }
            }
            scale = max_scale;

            // Strip trailing zeros again after rounding
            while (scale > 0 && value != 0) {
                auto quotient = value / 10;
                auto remainder = value - quotient * 10;
                if (remainder != 0) {
                    break;
                }
                value = quotient;
                scale--;
            }

            if (value == 0) {
                return 0;
            }
        }

        // Compute Java BigDecimal.hashCode():
        // hashCode = intValue(unscaledValue) * 31 + scale
        // intValue() returns the low 32 bits of the value
        auto int_value = static_cast<int32_t>(static_cast<int64_t>(value));
        return int_value * 31 + scale;
    }

 private:
    static constexpr int32_t HIVE_DECIMAL_MAX_PRECISION = 38;
    static constexpr int32_t HIVE_DECIMAL_MAX_SCALE = 38;
};

}  // namespace paimon
