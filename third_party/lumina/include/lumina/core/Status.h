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

#include <lumina/core/ErrorCodes.h>

namespace lumina::core {

class [[nodiscard]] Status
{
public:
    explicit Status(ErrorCode c, std::string m) : _code(c), _msg(std::move(m)) {}
    explicit Status(ErrorCode c) : _code(c), _msg({}) {}
    explicit Status() noexcept : _code(ErrorCode::Unavailable), _msg({}) {}
    static Status Ok() { return Status(ErrorCode::Ok); }
    static Status InvalidArgument() { return Status(ErrorCode::InvalidArgument); }
    static Status IoError() { return Status(ErrorCode::IoError); }
    static Status IoError(std::string m) { return Status(ErrorCode::IoError, std::move(m)); }
    static Status InvalidState(std::string m) { return Status(ErrorCode::FailedPrecondition, std::move(m)); }
    static Status InvalidState() { return Status(ErrorCode::FailedPrecondition, {}); }
    [[nodiscard]] bool IsOk() const noexcept { return _code == ErrorCode::Ok; }
    [[nodiscard]] bool operator!() const noexcept { return _code != ErrorCode::Ok; }
    [[nodiscard]] ErrorCode Code() const noexcept { return _code; }
    const std::string& Message() const noexcept { return _msg; }

private:
    ErrorCode _code;
    std::string _msg;
};

// TODO(feishi.wzj) add log
#define LUMINA_RETURN_IF_ERROR(expr)                                                                                   \
    do {                                                                                                               \
        auto _s = (expr);                                                                                              \
        if (!_s.IsOk()) {                                                                                              \
            return _s;                                                                                                 \
        }                                                                                                              \
    } while (0)

} // namespace lumina::core
