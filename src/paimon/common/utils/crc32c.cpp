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

#include "paimon/common/utils/crc32c.h"

#include "arrow/util/crc32.h"

namespace paimon {
uint32_t CRC32C::calculate(const char* data, size_t length, uint32_t crc) {
#if defined(PAIMON_HAVE_SSE4_2)
    return crc32c_hw(data, length, crc);
#else
    return arrow::internal::crc32(crc, data, length);
#endif
}

#if defined(PAIMON_HAVE_SSE4_2)
uint32_t CRC32C::crc32c_hw(const char* data, size_t length, uint32_t crc) {
    crc = ~crc;

    while (length && (reinterpret_cast<uintptr_t>(data) & 7)) {
        crc = _mm_crc32_u8(crc, *data++);
        length--;
    }

    while (length >= 8) {
        crc = _mm_crc32_u64(crc, *reinterpret_cast<const uint64_t*>(data));
        data += 8;
        length -= 8;
    }

    while (length >= 4) {
        crc = _mm_crc32_u32(crc, *reinterpret_cast<const uint32_t*>(data));
        data += 4;
        length -= 4;
    }

    while (length >= 2) {
        crc = _mm_crc32_u16(crc, *reinterpret_cast<const uint16_t*>(data));
        data += 2;
        length -= 2;
    }

    while (length--) {
        crc = _mm_crc32_u8(crc, *data++);
    }

    return ~crc;
}
#endif
}  // namespace paimon
