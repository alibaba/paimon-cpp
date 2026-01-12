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
#include <cstdint>

namespace lumina::core { inline namespace v1 {
// Type-safe enum with int32 backing for cross-language/protocol transport.
enum class ErrorCode : int32_t {
    Ok = 0,
    InvalidArgument = 1,
    NotFound = 2,
    AlreadyExists = 3,
    NotSupported = 4,
    IoError = 5,
    Timeout = 6,
    ResourceExhausted = 7,
    Unavailable = 8,
    PermissionDenied = 9,
    Unauthenticated = 10,
    Internal = 11,
    Corruption = 12,
    Cancelled = 13,
    DeadlineExceeded = 14,
    Conflict = 15,
    OutOfMemory = 16,
    PartialFailure = 17,
    RateLimited = 18,
    FailedPrecondition = 19,
    // 19..∞ append-only; do not change existing values.
};

constexpr int32_t ToInt(ErrorCode c) noexcept { return static_cast<int32_t>(c); }

constexpr bool IsRetryable(ErrorCode c) noexcept
{
    switch (c) {
    case ErrorCode::Unavailable:
    case ErrorCode::Timeout:
    case ErrorCode::ResourceExhausted:
        return true;
    default:
        return false;
    }
}

}} // namespace lumina::core::v1
