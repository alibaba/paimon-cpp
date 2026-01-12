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

#include <cstdint>
#include <lumina/distance/Metric.h>
#include <lumina/distance/MetricDistance.h>
#include <lumina/distance/encode_space/EncodingTypes.h>
#include <span>

namespace lumina::dist {

struct PrepareTag {
    template <class M, class E, class... Ctx>
        requires TagInvocable<PrepareTag, M, E, std::span<const float>, Ctx&&...>
    constexpr auto operator()(const M& m, const E& e, std::span<const float> q, Ctx&&... ctx) const
        noexcept(noexcept(TagInvoke(std::declval<PrepareTag>(), m, e, q, std::forward<Ctx>(ctx)...)))
            -> TagInvokeResult<PrepareTag, M, E, std::span<const float>, Ctx&&...>
    {
        return TagInvoke(*this, m, e, q, ctx...);
    }
};
inline constexpr PrepareTag Prepare {};

struct EvalEncodedTag {
    template <class M, class E, class S, class R>
        requires TagInvocable<EvalEncodedTag, M, E, const S&, const R&>
    constexpr auto operator()(const M& m, const E& e, const S& s, const R& r) const
        noexcept(noexcept(TagInvoke(std::declval<EvalEncodedTag>(), m, e, s, r)))
            -> TagInvokeResult<EvalEncodedTag, M, E, const S&, const R&>
    {
        return TagInvoke(*this, m, e, s, r);
    }
};
inline constexpr EvalEncodedTag EvalEncoded {};

struct BatchEvalEncodedTag {
    template <class M, class E, class S>
        requires TagInvocable<BatchEvalEncodedTag, M, E, const S&, const encode_space::EncodedBatch&, float*>
    constexpr void operator()(const M& m, const E& e, const S& s, const encode_space::EncodedBatch& b, float* out) const
        noexcept(noexcept(TagInvoke(std::declval<BatchEvalEncodedTag>(), m, e, s, b, out)))
    {
        TagInvoke(*this, m, e, s, b, out);
    }
};
inline constexpr BatchEvalEncodedTag BatchEvalEncoded {};

} // namespace lumina::dist
