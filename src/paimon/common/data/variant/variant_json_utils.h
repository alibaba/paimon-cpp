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
#include <string>
#include <string_view>

#include "paimon/result.h"

namespace paimon {

/// Helpers for rendering variant values as JSON text, matching the output of the Java
/// implementation (`GenericVariant.toJsonImpl`) character-for-character.
class VariantJsonUtils {
 public:
    VariantJsonUtils() = delete;
    ~VariantJsonUtils() = delete;

    /// Appends `str` as a quoted JSON string with escaping into `out`.
    static void AppendEscapedJson(std::string_view str, std::string* out);

    /// Formats a double like `java.lang.Double.toString`, e.g. `1.0`, `-0.001`, `1.0E7`.
    static std::string JavaDoubleToString(double value);

    /// Formats a float like `java.lang.Float.toString`.
    static std::string JavaFloatToString(float value);

    /// Formats days-since-epoch like `java.time.LocalDate.toString`, e.g. `2024-01-15`.
    static std::string DateToString(int32_t days_since_epoch);

    /// Formats microseconds-since-epoch at the given offset like the Java formatter
    /// `yyyy-MM-dd HH:mm:ss[.fraction]` (fraction with trailing zeros trimmed), optionally
    /// followed by a `+HH:MM` offset suffix.
    static std::string TimestampToString(int64_t micros_since_epoch, int32_t offset_seconds,
                                         bool with_offset);

    /// Resolves the UTC offset (in seconds) of `zone_id` at the given instant. Supports fixed
    /// offsets (`+08:00`, `-05:30`, optionally prefixed with `UTC`/`GMT`), `Z`, `UTC`, `GMT`,
    /// and IANA region ids such as `Asia/Shanghai`.
    static Result<int32_t> GetZoneOffsetSeconds(const std::string& zone_id,
                                                int64_t micros_since_epoch);
};

}  // namespace paimon
