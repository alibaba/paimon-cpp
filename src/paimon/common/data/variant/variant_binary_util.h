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

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed
 * by the Apache Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. */

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "paimon/result.h"

namespace paimon {

/// The value type of a variant value. It is determined by the header byte but not a 1:1 mapping
/// (for example, INT1/2/4/8 all map to `kLong`).
enum class VariantValueType {
    kObject,
    kArray,
    kNull,
    kBoolean,
    kLong,
    kString,
    kDouble,
    kDecimal,
    kDate,
    kTimestamp,
    kTimestampNtz,
    kFloat,
    kBinary,
    kUuid,
};

/// An arbitrary-precision (up to 38 digits) decimal value decoded from a variant binary. The
/// numeric value is `unscaled * 10^(-scale)`. Unlike `paimon::Decimal`, `scale` may become
/// negative after `StripTrailingZeros` (mirroring `java.math.BigDecimal`).
struct VariantDecimal {
    __int128_t unscaled = 0;
    int32_t scale = 0;

    /// Number of decimal digits in the unscaled value (a value of zero has precision 1).
    int32_t Precision() const;

    /// Removes trailing zero digits from the unscaled value, increasing `10^-scale` accordingly.
    VariantDecimal StripTrailingZeros() const;

    /// Plain (non-scientific) string representation, e.g. `-12.340` or `100`.
    std::string ToPlainString() const;

    bool operator==(const VariantDecimal& other) const {
        return unscaled == other.unscaled && scale == other.scale;
    }
};

/// Static functions for manipulating variant binaries. See `VariantDefs` for the binary format.
class VariantBinaryUtil {
 public:
    VariantBinaryUtil() = delete;
    ~VariantBinaryUtil() = delete;

    /// The decoded layout of a variant object value.
    struct ObjectInfo {
        /// Number of object fields.
        int32_t num_elements;
        /// The integer size of the field id list.
        int32_t id_size;
        /// The integer size of the offset list.
        int32_t offset_size;
        /// The starting index of the field id list in the variant value.
        int32_t id_start;
        /// The starting index of the offset list in the variant value.
        int32_t offset_start;
        /// The starting index of field data in the variant value.
        int32_t data_start;
    };

    /// The decoded layout of a variant array value.
    struct ArrayInfo {
        /// Number of array elements.
        int32_t num_elements;
        /// The integer size of the offset list.
        int32_t offset_size;
        /// The starting index of the offset list in the variant value.
        int32_t offset_start;
        /// The starting index of element data in the variant value.
        int32_t data_start;
    };

    static Status MalformedVariant();
    static Status UnknownPrimitiveTypeInVariant(int32_t id);
    static Status VariantConstructorSizeLimit();
    static Status UnexpectedType(VariantValueType type);

    /// Checks the validity of an index `pos` in a buffer of `length` bytes. Returns
    /// `MALFORMED_VARIANT` if it is out of bound.
    static Status CheckIndex(int32_t pos, int32_t length);

    /// Writes the least significant `num_bytes` bytes in `value` into
    /// `bytes[pos, pos + num_bytes)` in little endian.
    static void WriteLong(uint8_t* bytes, int32_t pos, int64_t value, int32_t num_bytes);

    /// Reads a little-endian signed long value from `bytes[pos, pos + num_bytes)`.
    static Result<int64_t> ReadLong(std::string_view bytes, int32_t pos, int32_t num_bytes);

    /// Reads a little-endian unsigned int value from `bytes[pos, pos + num_bytes)`. The value
    /// must fit into a non-negative int32.
    static Result<int32_t> ReadUnsigned(std::string_view bytes, int32_t pos, int32_t num_bytes);

    /// Adds a buffer-relative element `offset` to `base` in 64-bit and validates the result
    /// stays inside `buffer_size`, guarding the 32-bit addition against corrupted offsets.
    static Result<int32_t> CheckedElementPos(int32_t base, int32_t offset, size_t buffer_size) {
        int64_t pos = static_cast<int64_t>(base) + offset;
        if (pos >= static_cast<int64_t>(buffer_size)) {
            return MalformedVariant();
        }
        return static_cast<int32_t>(pos);
    }

    static uint8_t PrimitiveHeader(int32_t type);
    static uint8_t ShortStrHeader(int32_t size);
    static uint8_t ObjectHeader(bool large_size, int32_t id_size, int32_t offset_size);
    static uint8_t ArrayHeader(bool large_size, int32_t offset_size);

    /// Gets the type info bits from the variant value `value[pos...]`.
    static Result<int32_t> GetTypeInfo(std::string_view value, int32_t pos);

    /// Gets the value type of the variant value `value[pos...]`. It is only legal to call `Get*`
    /// if `GetType` returns the corresponding type (for example, it is only legal to call
    /// `GetLong` if `GetType` returns `kLong`).
    static Result<VariantValueType> GetType(std::string_view value, int32_t pos);

    /// Computes the size in bytes of the variant value `value[pos...]`. `value.size() - pos` is
    /// an upper bound of the size, but the actual size can be smaller.
    static Result<int32_t> ValueSize(std::string_view value, int32_t pos);

    static Result<bool> GetBoolean(std::string_view value, int32_t pos);

    /// Gets a long value from the variant value `value[pos...]`. It is only legal to call it if
    /// `GetType` returns one of `kLong/kDate/kTimestamp/kTimestampNtz`. If the type is `kDate`,
    /// the return value is guaranteed to fit into an int32 and represents the number of days from
    /// the Unix epoch. If the type is `kTimestamp/kTimestampNtz`, the return value represents the
    /// number of microseconds from the Unix epoch.
    static Result<int64_t> GetLong(std::string_view value, int32_t pos);

    static Result<double> GetDouble(std::string_view value, int32_t pos);

    /// Gets a decimal value from the variant value `value[pos...]`, keeping the stored scale.
    static Result<VariantDecimal> GetDecimalWithOriginalScale(std::string_view value, int32_t pos);

    /// Gets a decimal value from the variant value `value[pos...]` with trailing zeros stripped.
    static Result<VariantDecimal> GetDecimal(std::string_view value, int32_t pos);

    static Result<float> GetFloat(std::string_view value, int32_t pos);

    /// Gets a binary value from the variant value `value[pos...]`. The returned view aliases
    /// `value` and remains valid only as long as the underlying buffer.
    static Result<std::string_view> GetBinary(std::string_view value, int32_t pos);

    /// Gets a string value from the variant value `value[pos...]`. The returned view aliases
    /// `value` and remains valid only as long as the underlying buffer.
    static Result<std::string_view> GetString(std::string_view value, int32_t pos);

    /// Gets a UUID value (16 bytes, big-endian) from the variant value `value[pos...]`. The
    /// returned view aliases `value`.
    static Result<std::string_view> GetUuid(std::string_view value, int32_t pos);

    /// Formats a 16-byte big-endian UUID as the canonical lower-case string, e.g.
    /// `123e4567-e89b-12d3-a456-426614174000`.
    static std::string UuidToString(std::string_view uuid_bytes);

    /// Decodes the layout of the variant object value `value[pos...]`.
    static Result<ObjectInfo> GetObjectInfo(std::string_view value, int32_t pos);

    /// Decodes the layout of the variant array value `value[pos...]`.
    static Result<ArrayInfo> GetArrayInfo(std::string_view value, int32_t pos);

    /// Gets the key at `id` in the variant metadata. An out-of-bound `id` is considered a
    /// malformed variant because it is read from the corresponding variant value.
    static Result<std::string_view> GetMetadataKey(std::string_view metadata, int32_t id);
};

}  // namespace paimon
