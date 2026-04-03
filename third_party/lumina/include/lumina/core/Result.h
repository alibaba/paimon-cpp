#pragma once
#include <cassert>
#include <lumina/core/Macro.h>
#include <lumina/core/Status.h>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

namespace lumina::core {

template <typename T>
class [[nodiscard]] Result
{
public:
    Result() = delete;
    static_assert(!std::is_same_v<T, Status>, "Result<Status> is not supported; use Status directly.");
    [[nodiscard]] bool IsOk() const noexcept { return std::holds_alternative<T>(_v); }

    [[nodiscard]] const Status& GetStatus() const noexcept LUMINA_LIFETIME_BOUND
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
    static Result Ok(Args&&... args) noexcept
    {
        return Result(std::in_place_index<0>, std::forward<Args>(args)...);
    }

    static Result Err(Status s) noexcept
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
