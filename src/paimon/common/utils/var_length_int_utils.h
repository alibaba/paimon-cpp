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

#include "fmt/format.h"
#include "paimon/macros.h"
#include "paimon/result.h"
namespace paimon {

/// Variable-length integer encoding/decoding utilities.
///
/// Encoding format (same as protobuf unsigned varint):
///   - Each byte stores 7 payload bits in bits [6:0].
///   - Bit 7 (0x80) is the continuation flag: 1 = more bytes follow, 0 = last byte.
///   - A varint32 uses at most 5 bytes; a varint64 uses at most 9 bytes.
///
/// Based on the LongPacker from PalDB (https://github.com/linkedin/PalDB),
/// licensed under Apache 2.0.
class VarLengthIntUtils {
 public:
    VarLengthIntUtils() = delete;
    ~VarLengthIntUtils() = delete;

    static constexpr int32_t kMaxVarIntSize = 5;
    static constexpr int32_t kMaxVarLongSize = 9;

    // ==================== Encoding (writes to char*) ====================

    /// Encodes a non-negative int32 as varint into `dest`.
    /// Returns the number of bytes written.
    static Result<int32_t> EncodeInt(int32_t value, char* dest) {
        if (PAIMON_UNLIKELY(value < 0)) {
            return Status::Invalid(
                fmt::format("negative value: v={} for VarLengthInt Encoding", value));
        }
        int32_t num_bytes = 0;
        while ((value & ~0x7F) != 0) {
            dest[num_bytes] = static_cast<char>((value & 0x7F) | 0x80);
            value >>= 7;
            ++num_bytes;
        }
        dest[num_bytes] = static_cast<char>(value);
        return num_bytes + 1;
    }

    /// Encodes a non-negative int64 as varint into `dest`.
    /// Returns the number of bytes written.
    static Result<int32_t> EncodeLong(int64_t value, char* dest) {
        if (PAIMON_UNLIKELY(value < 0)) {
            return Status::Invalid(
                fmt::format("negative value: v={} for VarLengthInt Encoding", value));
        }
        int32_t num_bytes = 0;
        while ((value & ~0x7FLL) != 0) {
            dest[num_bytes] = static_cast<char>(static_cast<int32_t>(value & 0x7F) | 0x80);
            value >>= 7;
            ++num_bytes;
        }
        dest[num_bytes] = static_cast<char>(value);
        return num_bytes + 1;
    }

    // ==================== Decoding (reads from const char*) ====================

    /// Decodes a varint32 from `data` at `*offset`, advancing `*offset` past the consumed bytes.
    /// Inlines a 1-byte fast path (values 0-127), which is the most common case.
    static inline Result<int32_t> DecodeInt(const char* data, int32_t* offset) {
        auto first_byte = static_cast<uint8_t>(data[*offset]);
        if (PAIMON_LIKELY((first_byte & 0x80) == 0)) {
            ++(*offset);
            return static_cast<int32_t>(first_byte);
        }
        // Multi-byte: fall through to generic loop
        int32_t result = 0;
        for (int32_t shift = 0; shift < 32; shift += 7) {
            auto byte_val = static_cast<uint8_t>(data[*offset]);
            ++(*offset);
            result |= static_cast<int32_t>(byte_val & 0x7F) << shift;
            if ((byte_val & 0x80) == 0) {
                return result;
            }
        }
        return Status::Invalid("Malformed varint32: too many continuation bytes");
    }

    /// Decodes a varint64 from `data` at `*offset`, advancing `*offset` past the consumed bytes.
    /// Inlines a 1-byte fast path (values 0-127), which is the most common case.
    static inline Result<int64_t> DecodeLong(const char* data, int32_t* offset) {
        auto first_byte = static_cast<uint8_t>(data[*offset]);
        if (PAIMON_LIKELY((first_byte & 0x80) == 0)) {
            ++(*offset);
            return static_cast<int64_t>(first_byte);
        }
        // Multi-byte: fall through to generic loop
        int64_t result = 0;
        for (int32_t shift = 0; shift < 64; shift += 7) {
            auto byte_val = static_cast<uint8_t>(data[*offset]);
            ++(*offset);
            result |= static_cast<int64_t>(byte_val & 0x7F) << shift;
            if ((byte_val & 0x80) == 0) {
                return result;
            }
        }
        return Status::Invalid("Malformed varint64: too many continuation bytes");
    }
};

}  // namespace paimon
