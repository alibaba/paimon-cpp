#pragma once

#include <lumina/core/Constants.h>
#include <lumina/mpl/EnumHelper.h>
#include <lumina/mpl/TypeList.h>
#include <string_view>

namespace lumina::core {

enum class IndexE { bruteforce, diskANN, ivf, demo };

template <IndexE E>
struct IndexTypeT {
    static constexpr IndexE Value = E;
    static constexpr std::string_view Name = []() {
        if constexpr (E == IndexE::bruteforce) {
            return kIndexTypeBruteforce;
        } else if constexpr (E == IndexE::diskANN) {
            return kIndexTypeDiskANN;
        } else if constexpr (E == IndexE::ivf) {
            return kIndexTypeIvf;
        } else if constexpr (E == IndexE::demo) {
            return kIndexTypeDemo;
        } else {
            return "dummy";
        }
    }();
};

// Map function for EnumTypeList
template <typename T> // T is Enum2Type<E>
struct IndexTypeMapFunc {
    using Type = IndexTypeT<T::Value>;
};

using IndexTypeList = mpl::EnumHelper::EnumTypeList<IndexE>::Map<IndexTypeMapFunc>;

[[nodiscard]] inline constexpr IndexE Str2IndexE(std::string_view str) noexcept
{
    IndexE ret = static_cast<IndexE>(-1); // Invalid
    auto find = [&ret, str]<typename T>() {
        if (T::Name == str) {
            ret = T::Value;
        }
    };
    mpl::ForEachType<IndexTypeList>::Run(find);
    return ret; // Returns -1 (casted) if check fails, need safe handling or fallback
}

// Helper to check validity
inline constexpr bool IsValidIndexE(IndexE e) { return e != static_cast<IndexE>(-1); }

} // namespace lumina::core
