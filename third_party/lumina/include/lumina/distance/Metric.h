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
#include <lumina/core/Constants.h>
#include <span>
#include <string_view>
#include <utility>

namespace lumina::dist {

enum class MetricE { l2, ip, cosine, dummy };

template <MetricE E>
struct MetricT {
    static constexpr std::string_view Name = "dummy";
    static constexpr bool LowerIsBetter = true;
};

template <>
struct MetricT<MetricE::l2> {
    static constexpr std::string_view Name = core::kDistanceL2;
    static constexpr bool LowerIsBetter = true;
};

template <>
struct MetricT<MetricE::ip> {
    static constexpr std::string_view Name = core::kDistanceInnerProduct;
    static constexpr bool LowerIsBetter = false;
};

// Cosine distance is 1 - cos<a, b>.
template <>
struct MetricT<MetricE::cosine> {
    static constexpr std::string_view Name = core::kDistanceCosine;
    static constexpr bool LowerIsBetter = true;
};

using L2 = MetricT<MetricE::l2>;
using IP = MetricT<MetricE::ip>;
using Cosine = MetricT<MetricE::cosine>;

} // namespace lumina::dist
