#pragma once
#include <lumina/core/Macro.h>

#if LUMINA_CXX_VER >= 202002L
template <class T>
using LuminaRemoveCvrefT = std::remove_cvref_t<T>;
#else
template <class T>
using LuminaRemoveCvrefT = std::remove_cv_t<std::remove_reference_t<T>>;
#endif

namespace lumina::mpl {
template <class T>
inline constexpr bool Stringable =
    std::is_same_v<LuminaRemoveCvrefT<T>, std::string> || std::is_same_v<LuminaRemoveCvrefT<T>, std::string_view>;

template <class T, class... Args>
inline constexpr bool LuminaConstructibleFrom =
#if LUMINA_CXX_VER >= 202002L
    std::constructible_from<T, Args...>;
#else
    std::is_constructible_v<T, Args...>;
#endif

} // namespace lumina::mpl
