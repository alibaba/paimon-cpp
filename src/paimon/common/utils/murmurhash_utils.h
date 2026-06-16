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

/*
 * xxHash - Extremely Fast Hash algorithm
 * Header File
 * Copyright (C) 2012-2023 Yann Collet
 *
 * BSD 2-Clause License (https://www.opensource.org/licenses/bsd-license.php)
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 *      copyright notice, this list of conditions and the following disclaimer
 *      in the documentation and/or other materials provided with the
 *      distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * You can contact the author at:
 *   - xxHash homepage: https://www.xxhash.com
 *   - xxHash source repository: https://github.com/Cyan4973/xxHash
 */

// MMH_rotl32 utility is adapted from xxHash
// https://github.com/Cyan4973/xxHash/blob/dev/xxhash.h

#pragma once
#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>

#include "paimon/common/memory/memory_segment.h"
#include "paimon/memory/bytes.h"

namespace paimon {

#ifdef __has_builtin
#define MMH_HAS_BUILTIN(x) __has_builtin(x)
#else
#define MMH_HAS_BUILTIN(x) 0
#endif
/*!
 * @internal
 * @def MMH_rotl32(x,r)
 * @brief 32-bit rotate left.
 *
 * @param x The 32-bit integer to be rotated.
 * @param r The number of bits to rotate.
 * @pre
 *   @p r > 0 && @p r < 32
 * @note
 *   @p x and @p r may be evaluated multiple times.
 * @return The rotated result.
 */
#if !defined(NO_CLANG_BUILTIN) && MMH_HAS_BUILTIN(__builtin_rotateleft32) && \
    MMH_HAS_BUILTIN(__builtin_rotateleft64)
#define MMH_rotl32 __builtin_rotateleft32
/* Note: although _rotl exists for minGW (GCC under windows), performance seems poor */
#elif defined(_MSC_VER)
#define MMH_rotl32(x, r) _rotl(x, r)
#else
#define MMH_rotl32(x, r) (((x) << (r)) | ((x) >> (32 - (r))))
#endif

class MurmurHashUtils {
 public:
    static constexpr int32_t DEFAULT_SEED = 42;

    MurmurHashUtils() = delete;

    /// Hash unsafe bytes, length must be aligned to 4 bytes.
    ///
    /// @param base base unsafe object
    /// @param offset offset for unsafe object
    /// @param length_in_bytes length in bytes
    /// @return hash code
    static int32_t HashUnsafeBytesByWords(const void* base, int64_t offset,
                                          int32_t length_in_bytes) {
        return HashUnsafeBytesByWords(base, offset, length_in_bytes, DEFAULT_SEED);
    }

    /// Hash bytes.
    static int32_t HashBytesPositive(const std::shared_ptr<Bytes>& bytes) {
        return HashBytes(bytes) & 0x7fffffff;
    }

    /// Hash bytes.
    static int32_t HashBytes(const std::shared_ptr<Bytes>& bytes) {
        return HashUnsafeBytes(reinterpret_cast<const void*>(bytes->data()), 0, bytes->size(),
                               DEFAULT_SEED);
    }

    static int32_t HashUnsafeBytes(const void* base, int64_t offset, int32_t length_in_bytes,
                                   int32_t seed) {
        assert(length_in_bytes >= 0);
        int32_t length_aligned = length_in_bytes - length_in_bytes % 4;
        int32_t h1 = HashUnsafeBytesByInt(base, offset, length_aligned, seed);
        for (int32_t i = length_aligned; i < length_in_bytes; i++) {
            int32_t half_word = GetByte(base, offset + i);
            int32_t k1 = MixK1(half_word);
            h1 = MixH1(h1, k1);
        }
        return Fmix(h1, length_in_bytes);
    }

    /// Hash unsafe bytes.
    ///
    /// @param base base unsafe object
    /// @param offset offset for unsafe object
    /// @param length_in_bytes length in bytes
    /// @return hash code
    static int32_t HashUnsafeBytes(const void* base, int64_t offset, int32_t length_in_bytes) {
        return HashUnsafeBytes(base, offset, length_in_bytes, DEFAULT_SEED);
    }

    /// Hash bytes in MemorySegment, length must be aligned to 4 bytes.
    ///
    /// @param segment segment.
    /// @param offset offset for MemorySegment
    /// @param length_in_bytes length in MemorySegment
    /// @return hash code
    static int32_t HashBytesByWords(const MemorySegment& segment, int32_t offset,
                                    int32_t length_in_bytes) {
        return HashBytesByWords(segment, offset, length_in_bytes, DEFAULT_SEED);
    }

    /// Hash bytes in MemorySegment.
    ///
    /// @param segment segment.
    /// @param offset offset for MemorySegment
    /// @param length_in_bytes length in MemorySegment
    /// @return hash code
    static int32_t HashBytes(const MemorySegment& segment, int32_t offset,
                             int32_t length_in_bytes) {
        return HashBytes(segment, offset, length_in_bytes, DEFAULT_SEED);
    }

 private:
    static int32_t HashUnsafeBytesByWords(const void* base, int64_t offset, int32_t length_in_bytes,
                                          int32_t seed) {
        int32_t h1 = HashUnsafeBytesByInt(base, offset, length_in_bytes, seed);
        return Fmix(h1, length_in_bytes);
    }

    static int32_t HashBytesByWords(const MemorySegment& segment, int32_t offset,
                                    int32_t length_in_bytes, int32_t seed) {
        int32_t h1 = HashBytesByInt(segment, offset, length_in_bytes, seed);
        return Fmix(h1, length_in_bytes);
    }

    static int32_t HashBytes(const MemorySegment& segment, int32_t offset, int32_t length_in_bytes,
                             int32_t seed) {
        int32_t length_aligned = length_in_bytes - length_in_bytes % 4;
        int32_t h1 = HashBytesByInt(segment, offset, length_aligned, seed);
        for (int32_t i = length_aligned; i < length_in_bytes; i++) {
            auto byte = static_cast<uint8_t>(segment.Get(offset + i));
            int32_t signed_byte = byte < 128 ? byte : static_cast<int32_t>(byte) - 256;
            int32_t k1 = MixK1(signed_byte);
            h1 = MixH1(h1, k1);
        }
        return Fmix(h1, length_in_bytes);
    }

    static int32_t HashUnsafeBytesByInt(const void* base, int64_t offset, int32_t length_in_bytes,
                                        int32_t seed) {
        assert(length_in_bytes % 4 == 0);
        int32_t h1 = seed;
        for (int32_t i = 0; i < length_in_bytes; i += 4) {
            int32_t half_word = GetInt(base, offset + i);
            int32_t k1 = MixK1(half_word);
            h1 = MixH1(h1, k1);
        }
        return h1;
    }

    static int32_t HashBytesByInt(const MemorySegment& segment, int32_t offset,
                                  int32_t length_in_bytes, int32_t seed) {
        assert(length_in_bytes % 4 == 0);
        int32_t h1 = seed;
        for (int32_t i = 0; i < length_in_bytes; i += 4) {
            auto half_word = segment.GetValue<int32_t>(offset + i);
            int32_t k1 = MixK1(half_word);
            h1 = MixH1(h1, k1);
        }
        return h1;
    }

    static int32_t MixK1(uint32_t k1) {
        k1 *= C1;
        k1 = MMH_rotl32(k1, 15);
        k1 *= C2;
        return k1;
    }

    static int32_t MixH1(uint32_t h1, uint32_t k1) {
        h1 ^= k1;
        h1 = MMH_rotl32(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;
        return h1;
    }

    // Finalization mix - force all bits of a hash block to avalanche
    static int32_t Fmix(uint32_t h1, uint32_t length) {
        h1 ^= length;
        return Fmix(h1);
    }

    static int32_t GetInt(const void* base, int64_t offset) {
        int32_t value;
        std::memcpy(&value, static_cast<const char*>(base) + offset, sizeof(int32_t));
        return value;
    }

    static int32_t GetByte(const void* base, int64_t offset) {
        uint8_t value;
        std::memcpy(&value, static_cast<const char*>(base) + offset, sizeof(uint8_t));
        return value < 128 ? value : static_cast<int32_t>(value) - 256;
    }

 public:
    static int32_t Fmix(uint32_t h) {
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;
        return h;
    }

 private:
    static constexpr int32_t C1 = 0xcc9e2d51;
    static constexpr int32_t C2 = 0x1b873593;
};

}  // namespace paimon
