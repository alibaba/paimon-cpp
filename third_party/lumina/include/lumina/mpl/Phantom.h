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
