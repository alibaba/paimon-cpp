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

//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Adapted from RocksDB
// https://github.com/facebook/rocksdb/blob/main/util/math.h

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace paimon {
// Swaps between big and little endian. Can be used in combination with the
// little-endian encoding/decoding functions in coding_lean.h and coding.h to
// encode/decode big endian.
template <typename T>
inline T EndianSwapValue(T v) {
    static_assert(std::is_standard_layout_v<T> && std::is_trivially_copyable_v<T>,
                  "Type must be standard-layout and trivially copyable (e.g., integral or "
                  "floating-point types).");
    if constexpr (sizeof(T) == 1) {
        return v;
    } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double> ||
                         std::is_integral_v<T>) {
        using UintType = std::conditional_t<
            sizeof(T) == 2, uint16_t,
            std::conditional_t<sizeof(T) == 4, uint32_t,
                               std::conditional_t<sizeof(T) == 8, uint64_t, void> > >;

        static_assert(!std::is_same_v<UintType, void>,
                      "Unsupported size: only 4-byte and 8-byte types are supported.");

        UintType int_repr;
        std::memcpy(&int_repr, &v, sizeof(T));

#ifdef _MSC_VER
        if constexpr (sizeof(T) == 2) {
            int_repr = _byteswap_ushort(static_cast<uint16_t>(int_repr));
        } else if constexpr (sizeof(T) == 4) {
            int_repr = _byteswap_ulong(static_cast<uint32_t>(int_repr));
        } else if constexpr (sizeof(T) == 8) {
            int_repr = _byteswap_uint64(static_cast<uint64_t>(int_repr));
        }
#else
        if constexpr (sizeof(T) == 2) {
            int_repr = __builtin_bswap16(static_cast<uint16_t>(int_repr));
        } else if constexpr (sizeof(T) == 4) {
            int_repr = __builtin_bswap32(static_cast<uint32_t>(int_repr));
        } else if constexpr (sizeof(T) == 8) {
            int_repr = __builtin_bswap64(static_cast<uint64_t>(int_repr));
        }
#endif
        T result;
        std::memcpy(&result, &int_repr, sizeof(T));
        return result;
    } else {
        // Fallback for unsupported sizes (e.g., 16-bit integers on some platforms)
        T ret_val{};
        for (std::size_t i = 0; i < sizeof(T); ++i) {
            reinterpret_cast<unsigned char*>(&ret_val)[sizeof(T) - 1 - i] =
                reinterpret_cast<unsigned char*>(&v)[i];
        }
        return ret_val;
    }
}

}  // namespace paimon
