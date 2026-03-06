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

#include "fmt/format.h"
#include "paimon/memory/bytes.h"
#include "paimon/result.h"

namespace paimon {
/// This file is based on source code of LongPacker from the PalDB Project
/// (https://github.com/linkedin/PalDB), licensed by the Apache Software Foundation (ASF) under the
/// Apache License, Version 2.0. See the NOTICE file distributed with this work for additional
/// information regarding copyright ownership.
/// Utils for encoding int/long to var length bytes.
class VarLengthIntUtils {
 public:
    VarLengthIntUtils() = delete;
    ~VarLengthIntUtils() = delete;

    static constexpr int32_t kMaxVarIntSize = 5;
    static constexpr int32_t kMaxVarLongSize = 9;

    /// Returns encoding bytes length.
    static Result<int32_t> EncodeInt(int32_t offset, int32_t value, Bytes* bytes) {
        if (value < 0) {
            return Status::Invalid(
                fmt::format("negative value: v={} for VarLengthInt Encoding", value));
        }

        int32_t i = 1;
        while ((value & ~0x7F) != 0) {
            (*bytes)[i + offset - 1] = static_cast<char>((value & 0x7F) | 0x80);
            value >>= 7;
            ++i;
        }
        (*bytes)[i + offset - 1] = static_cast<char>(value);
        return i;
    }

    static Result<int32_t> DecodeInt(const Bytes* bytes, int32_t* offset) {
        int32_t result = 0;
        for (int32_t shift = 0; shift < 32; shift += 7) {
            auto b = static_cast<int32_t>((*bytes)[*offset]);
            ++(*offset);

            result |= (b & 0x7Fu) << shift;

            if ((b & 0x80u) == 0) {
                return result;
            }
        }
        return Status::Invalid("Malformed integer for VarLengthInt Decoding");
    }

    /// Returns encoding bytes length.
    static Result<int32_t> EncodeLong(int64_t value, Bytes* bytes) {
        if (value < 0) {
            return Status::Invalid(
                fmt::format("negative value: v={} for VarLengthInt Encoding", value));
        }

        int32_t i = 1;
        while ((value & ~0x7FLL) != 0) {
            (*bytes)[i - 1] = static_cast<char>(static_cast<int32_t>(value & 0x7F) | 0x80);
            value >>= 7;
            ++i;
        }
        (*bytes)[i - 1] = static_cast<char>(value);
        return i;
    }

    static Result<int64_t> DecodeLong(const Bytes* bytes, int32_t index) {
        int64_t result = 0;
        for (int32_t shift = 0; shift < 64; shift += 7) {
            auto b = static_cast<int64_t>((*bytes)[index++]);

            result |= (b & 0x7FLL) << shift;

            if ((b & 0x80u) == 0) {
                return result;
            }
        }
        return Status::Invalid("Malformed long for VarLengthInt Decoding");
    }
};
}  // namespace paimon
