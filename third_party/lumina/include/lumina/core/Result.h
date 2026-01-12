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
#include <cassert>
#include <lumina/core/Macro.h>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include <lumina/core/Status.h>

namespace lumina::core {

template <typename T>
class [[nodiscard]] Result
{
public:
    Result() = delete;
    static_assert(!std::is_same_v<T, Status>, "Result<Status> is not supported; use Status directly.");
    [[nodiscard]] bool IsOk() const noexcept { return std::holds_alternative<T>(_v); }

    [[nodiscard]] const Status& GetStatus() const LUMINA_LIFETIME_BOUND
    {
        if (IsOk()) {
            static const auto status = Status::Ok();
            return status;
        }
        return std::get<1>(_v);
    }

    [[nodiscard]] Status TakeStatus() && noexcept
    {
        if (IsOk()) {
            return Status::Ok();
        }
        return std::move(std::get<1>(_v));
    }

    [[nodiscard]] const T& Value() const noexcept LUMINA_LIFETIME_BOUND
    {
        auto* p = std::get_if<T>(&_v);
        assert(p);
        return *p;
    }

    [[nodiscard]] T& Value() noexcept LUMINA_LIFETIME_BOUND
    {
        auto* p = std::get_if<T>(&_v);
        assert(p);
        return *p;
    }

    [[nodiscard]] T&& TakeValue() && noexcept LUMINA_LIFETIME_BOUND
    {
        auto* p = std::get_if<T>(&_v);
        assert(p);
        return std::move(*p);
    }

    [[nodiscard]] explicit operator bool() const noexcept { return IsOk(); }

    template <class... Args>
    static Result Ok(Args&&... args)
    {
        return Result(std::in_place_index<0>, std::forward<Args>(args)...);
    }

    static Result Err(Status s)
    {
        assert(!s.IsOk());
        return Result(std::in_place_index<1>, std::move(s));
    }

private:
    template <class... Args>
    explicit Result(std::in_place_index_t<0>, Args&&... args) : _v(std::in_place_index<0>, std::forward<Args>(args)...)
    {
    }

    explicit Result(std::in_place_index_t<1>, Status&& s) : _v(std::in_place_index<1>, std::move(s)) {}

    std::variant<T, Status> _v;
};

} // namespace lumina::core
