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

#include <string_view>
#include "lumina/mpl/EnumHelper.h"
#include "lumina/mpl/TypeList.h"

namespace lumina::core {

enum class IndexE {
    Bruteforce,
    DiskANN,
    IVF,
    Demo
};

template <IndexE E>
struct IndexTypeT {
    static constexpr IndexE Value = E;
    static constexpr std::string_view Name = []() {
        if constexpr (E == IndexE::Bruteforce) return "bruteforce";
        else if constexpr (E == IndexE::DiskANN) return "diskann";
        else if constexpr (E == IndexE::IVF) return "ivf";
        else if constexpr (E == IndexE::Demo) return "demo";
        else return "unknown";
    }();
};

template <typename T>
struct IndexTypeMapFunc {
    using Type = IndexTypeT<T::Value>;
};

using IndexTypeList = mpl::EnumHelper::EnumTypeList<IndexE>::Map<IndexTypeMapFunc>;

[[nodiscard]] inline constexpr IndexE Str2IndexE(std::string_view str) noexcept {
    IndexE ret = static_cast<IndexE>(-1);
    auto find = [&ret, str]<typename T>() {
        if (T::Name == str) {
            ret = T::Value;
        }
    };
    mpl::ForEachType<IndexTypeList>::Run(find);
    return ret;
}

inline constexpr bool IsValidIndexE(IndexE e) {
    return e != static_cast<IndexE>(-1);
}

}
