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
#include <lumina/core/Macro.h>

#if LUMINA_CXX_VER >= 202002L
template <class T>
using LuminaRemoveCvrefT = std::remove_cvref_t<T>;
#else
template <class T>
using LuminaRemoveCvrefT = std::remove_cv_t<std::remove_reference_t<T>>;
#endif

namespace lumina::mpl {
template <class T>
inline constexpr bool Stringable =
    std::is_same_v<LuminaRemoveCvrefT<T>, std::string> || std::is_same_v<LuminaRemoveCvrefT<T>, std::string_view>;

template <class T, class... Args>
inline constexpr bool LuminaConstructibleFrom =
#if LUMINA_CXX_VER >= 202002L
    std::constructible_from<T, Args...>;
#else
    std::is_constructible_v<T, Args...>;
#endif

}
