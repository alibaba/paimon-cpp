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
#include <lumina/core/Constants.h>
#include <lumina/core/Types.h>
#include <lumina/distance/Metric.h>
#include <span>
#include <string_view>
#include <utility>

namespace lumina::dist {

constexpr float kDefaultEps = 1e-9;

template <class Tag, class... Args>
concept TagInvocable = requires(Tag t, Args&&... args) {
    TagInvoke(t, static_cast<Args&&>(args)...); // Find implementation via ADL
};

template <class Tag, class... Args>
using TagInvokeResult = decltype(TagInvoke(std::declval<Tag>(), std::declval<Args>()...));

template <class M>
concept IsMetricT = requires {
    {
        M::Name
    } -> std::convertible_to<std::string_view>;
    {
        M::LowerIsBetter
    } -> std::convertible_to<bool>;
};

// -- CPO: Eval --
struct EvalTag {
    template <IsMetricT M>
        requires(TagInvocable<EvalTag, M, std::span<const float>, std::span<const float>>)
    constexpr auto operator()(const M& m, std::span<const float> a, std::span<const float> b) const
        noexcept(noexcept(TagInvoke(std::declval<EvalTag>(), m, a, b)))
            -> TagInvokeResult<EvalTag, M, std::span<const float>, std::span<const float>>
    {
        return TagInvoke(*this, m, a, b);
    }
};
inline constexpr EvalTag EvalF {};

// -- CPO: BatchEval --
struct BatchEvalTag {
    template <IsMetricT M>
        requires(TagInvocable<BatchEvalTag, M, const float*, const float*, core::dimension_t, uint64_t, float*>)
    constexpr void operator()(const M& m, const float* q, const float* base, core::dimension_t dim, uint64_t n,
                              float* out) const
        noexcept(noexcept(TagInvoke(std::declval<BatchEvalTag>(), m, q, base, dim, n, out)))
    {
        TagInvoke(*this, m, q, base, dim, n, out);
    }

    // Final fallback: loop over EvalF
    template <IsMetricT M>
        requires(!TagInvocable<BatchEvalTag, M, const float*, const float*, core::dimension_t, uint64_t, float*>)
    constexpr void operator()(const M& m, const float* q, const float* base, core::dimension_t dim, uint64_t n,
                              float* out) const noexcept
    {
        for (uint64_t i = 0; i < n; ++i) {
            out[i] = EvalF(m, std::span<const float>(q, dim), std::span<const float>(base + i * dim, dim));
        }
    }
};
inline constexpr BatchEvalTag BatchEvalF {};

// check dist val
struct IsMinDistTag {
    template <IsMetricT M>
        requires TagInvocable<IsMinDistTag, M, float>
    constexpr auto operator()(const M& m, float dist) const
        noexcept(noexcept(TagInvoke(std::declval<IsMinDistTag>(), m, dist))) -> TagInvokeResult<IsMinDistTag, M, float>
    {
        return TagInvoke(*this, m, dist);
    }
};
inline constexpr IsMinDistTag IsMinDistF {};

} // namespace lumina::dist
