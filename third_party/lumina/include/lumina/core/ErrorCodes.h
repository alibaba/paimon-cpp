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

}} // namespace lumina::core::v1
