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
