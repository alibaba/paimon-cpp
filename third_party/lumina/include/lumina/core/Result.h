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
#include <string>
#include <utility>

#include "Status.h"

namespace lumina::core {

template <typename T>
struct [[nodiscard]] Result {
    [[nodiscard]] bool IsOk() const noexcept { return status.IsOk(); }
    [[nodiscard]] const Status& GetStatus() const noexcept { return status; }
    [[nodiscard]] Status&& TakeStatus() && noexcept { return std::move(status); }
    [[nodiscard]] T& Value() & noexcept { return value; }
    [[nodiscard]] const T& Value() const& noexcept { return value; }
    [[nodiscard]] T&& TakeValue() && noexcept { return std::move(value); }
    [[nodiscard]] explicit operator bool() const noexcept { return status.IsOk(); }
    static Result Ok(T v) { return {std::move(v), Status::Ok()}; }
    template <typename... Args>
    static Result Ok(Args&&... args)
    {
        return {T(std::forward<Args>(args)...), Status::Ok()};
    }
    static Result Err(Status s) { return {{}, std::move(s)}; }
    T value {};
    Status status {};
};

}
