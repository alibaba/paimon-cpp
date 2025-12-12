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

#include <concepts>
#include <cstdint>
#include <span>
#include <utility>

#include "lumina/core/ErrorCodes.h"

namespace lumina::dist::encode_space {

struct EncodedRowBuilder {
    std::span<std::byte> data;
    std::span<std::byte> aux;
};

struct EncodedBatchBuilder {
    std::span<std::byte> data;
    std::span<std::byte> aux;
    uint64_t n {0};
    uint32_t stride {0};
    uint32_t auxStride {0};
};

template <class Tag, class... Args>
concept TagInvocable = requires(Tag t, Args&&... args) { TagInvoke(t, static_cast<Args&&>(args)...); };

template <class Tag, class... Args>
using TagInvokeResult = decltype(TagInvoke(std::declval<Tag>(), std::declval<Args>()...));

struct EncodeTag {
    template <class ModelT, class Src>
        requires TagInvocable<EncodeTag, const ModelT&, Src, EncodedRowBuilder>
    constexpr auto operator()(const ModelT& model, Src src, EncodedRowBuilder out) const
        noexcept(noexcept(TagInvoke(std::declval<EncodeTag>(), model, src, out)))
            -> TagInvokeResult<EncodeTag, const ModelT&, Src, EncodedRowBuilder>
    {
        return TagInvoke(*this, model, src, out);
    }
};

inline constexpr EncodeTag Encode {};

struct EncodeBatchTag {
    template <class ModelT, class Src, class... Ctx>
        requires TagInvocable<EncodeBatchTag, const ModelT&, Src, EncodedBatchBuilder, const Ctx&...>
    constexpr auto operator()(const ModelT& model, Src src, EncodedBatchBuilder out, const Ctx&... ctx) const
        noexcept(noexcept(TagInvoke(std::declval<EncodeBatchTag>(), model, src, out, ctx...)))
            -> TagInvokeResult<EncodeBatchTag, const ModelT&, Src, EncodedBatchBuilder, const Ctx&...>
    {
        return TagInvoke(*this, model, src, out, ctx...);
    }
};

inline constexpr EncodeBatchTag EncodeBatch {};

}
