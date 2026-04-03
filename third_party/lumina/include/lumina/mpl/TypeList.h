#pragma once

#include <cstddef>
#include <type_traits>
#include <utility>

namespace lumina::mpl {

template <auto T>
struct Enum2Type {
    // static_assert(std::is_enum<T>::value, "T must be an enum type");
    static inline constexpr auto Value = T;
};

// Compile-time list of types.
template <typename... Ts>
struct TypeList {
    using Type = TypeList;
    static inline constexpr std::size_t Size = sizeof...(Ts);

    template <template <typename...> typename T>
    using ExportTo = T<Ts...>;

    template <template <typename> typename F>
    using Map = TypeList<typename F<Ts>::Type...>; // F<T> must provide using Type
};

template <auto Arr, std::size_t... Is>
auto EnumArrToTypeList(std::index_sequence<Is...>) -> TypeList<Enum2Type<Arr[Is]>...>;

// Helper to expand a TypeList and invoke callback per type.
template <typename List>
struct ForEachType;

template <typename... Ts>
struct ForEachType<TypeList<Ts...>> {
    // Require the functor to expose template< typename T > void operator()().
    template <typename F>
    static constexpr void Run(F&& f)
    {
        ((void)f.template operator()<Ts>(), ...);
    }

    // Pass runtime arguments along to F::operator()<T>(args...).
    template <typename F, typename... Args>
    static constexpr void RunWith(F&& f, Args&&... args)
    {
        ((void)f.template operator()<Ts>(std::forward<Args>(args)...), ...);
    }

    // Invoke with std::type_identity<T> to avoid template call operators.
    template <typename F, typename... Args>
    static constexpr void RunValueLike(F&& f, Args&&... args)
    {
        ((void)f(std::type_identity<Ts> {}, std::forward<Args>(args)...), ...);
    }
};

} // namespace lumina::mpl
