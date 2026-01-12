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
#include <array>
#include <cstddef>
#include <string_view>
#include <type_traits>
#include <utility>

namespace lumina::mpl {

template <typename T, typename Tag>
struct PhantomWrapper {
    template <typename... Args>
    constexpr explicit PhantomWrapper(Args&&... args) noexcept(std::is_nothrow_constructible_v<T, Args...>)
        : _val(std::forward<Args>(args)...)
    {
    }
    T& Get() { return _val; }
    const T& Get() const { return _val; }

private:
    T _val;
};

} // namespace lumina::mpl
