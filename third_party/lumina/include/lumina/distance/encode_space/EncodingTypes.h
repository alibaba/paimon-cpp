/*
 * Copyright 2025-present Alibaba Inc.
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

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <type_traits>

#include <lumina/core/Constants.h>
#include <lumina/mpl/EnumHelper.h>
#include <lumina/mpl/TypeList.h>

namespace lumina::dist::encode_space {

// -- Encoding enum --
enum class EncodingE { rawf32, sq8, pq, dummy };

// -- Read-only view of encoded data --
struct EncodedRow {
    const std::byte* data {nullptr};
    uint64_t bytes {0};
    const std::byte* aux {nullptr};
    uint64_t auxBytes {0};
};

struct EncodedBatch {
    const std::byte* base {nullptr};
    uint64_t stride {0};
    const std::byte* auxBase {nullptr};
    uint64_t auxStride {0};
    uint64_t n {0};
};

// -- Mirror Metric's type system: provide compile-time tags for encoding --
template <EncodingE E>
struct EncodingT {
    static constexpr std::string_view Name = "dummy";
};

template <>
struct EncodingT<EncodingE::rawf32> {
    static constexpr std::string_view Name = core::kEncodingRawf32;
};

template <>
struct EncodingT<EncodingE::sq8> {
    static constexpr std::string_view Name = core::kEncodingSQ8;
};

template <>
struct EncodingT<EncodingE::pq> {
    static constexpr std::string_view Name = core::kEncodingPQ;
};

using RawF32 = EncodingT<EncodingE::rawf32>;
using SQ8 = EncodingT<EncodingE::sq8>;
using PQ = EncodingT<EncodingE::pq>;

} // namespace lumina::dist::encode_space
