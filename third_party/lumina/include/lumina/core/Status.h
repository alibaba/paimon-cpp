#pragma once
#include <lumina/core/ErrorCodes.h>
#include <string>
#include <utility>

namespace lumina::core {

class [[nodiscard]] Status
{
public:
    explicit Status(ErrorCode c, std::string m) : _code(c), _msg(std::move(m)) {}
    explicit Status(ErrorCode c) : _code(c), _msg({}) {}
    explicit Status() noexcept : _code(ErrorCode::Unavailable), _msg({}) {}
    static Status Ok() noexcept { return Status(ErrorCode::Ok); }
    static Status InvalidArgument() noexcept { return Status(ErrorCode::InvalidArgument); }
    static Status IoError() noexcept { return Status(ErrorCode::IoError); }
    static Status IoError(std::string m) noexcept { return Status(ErrorCode::IoError, std::move(m)); }
    static Status InvalidState(std::string m) noexcept { return Status(ErrorCode::FailedPrecondition, std::move(m)); }
    static Status InvalidState() noexcept { return Status(ErrorCode::FailedPrecondition, {}); }
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
