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
#include <type_traits>
#include <utility>

namespace lumina::mpl {

template <auto T>
struct Enum2Type {
    static inline constexpr auto Value = T;
};

template <typename... Ts>
struct TypeList {
    using Type = TypeList;
    static inline constexpr std::size_t Size = sizeof...(Ts);

    template <template <typename...> typename T>
    using ExportTo = T<Ts...>;

    template <template <typename> typename F>
    using Map = TypeList<typename F<Ts>::Type...>;
};

template <auto Arr, std::size_t... Is>
auto EnumArrToTypeList(std::index_sequence<Is...>) -> TypeList<Enum2Type<Arr[Is]>...>;

template <typename List>
struct ForEachType;

template <typename... Ts>
struct ForEachType<TypeList<Ts...>> {
    template <typename F>
    static constexpr void Run(F&& f)
    {
        ((void)f.template operator()<Ts>(), ...);
    }

    template <typename F, typename... Args>
    static constexpr void RunWith(F&& f, Args&&... args)
    {
        ((void)f.template operator()<Ts>(std::forward<Args>(args)...), ...);
    }

    template <typename F, typename... Args>
    static constexpr void RunValueLike(F&& f, Args&&... args)
    {
        ((void)f(std::type_identity<Ts> {}, std::forward<Args>(args)...), ...);
    }
};

}
