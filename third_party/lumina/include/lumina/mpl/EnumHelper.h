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

// C++20 — Minimal, header-only: EnumName + EnumValues + EnumIterator/EnumRange
#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <lumina/mpl/TypeList.h>
#include <string_view>
#include <type_traits>
#include <utility>

namespace lumina::mpl {

struct EnumHelper final {
private:
    // ----- implementation details (hidden) -----

    template <typename E, E V>
    static consteval std::string_view Pretty()
    {
#if defined(__clang__) || defined(__GNUC__)
        return {__PRETTY_FUNCTION__, sizeof(__PRETTY_FUNCTION__) - 1};
#elif defined(_MSC_VER)
        return {__FUNCSIG__, sizeof(__FUNCSIG__) - 1};
#else
#error "Unsupported compiler for EnumHelper"
#endif
    }

    // --- robust token extraction across compilers ---
    static consteval void Trim(std::string_view& s)
    {
        while (!s.empty() && (s.front() == ' ')) {
            s.remove_prefix(1);
        }
        while (!s.empty() && (s.back() == ' ')) {
            s.remove_suffix(1);
        }
    }

#if defined(_MSC_VER)
    // __FUNCSIG__: "... Pretty<enum NS::E,NS::E::Name>(void)" or "... Pretty<enum NS::E,(NS::E)3>(void)"
    static consteval std::string_view ValueToken(std::string_view s)
    {
        // Take the last template argument (V).
        auto rangle = s.rfind('>');
        if (rangle == std::string_view::npos) {
            return {};
        }
        auto comma = s.rfind(',', rangle);
        if (comma == std::string_view::npos) {
            return {};
        }
        std::string_view t = s.substr(comma + 1, rangle - comma - 1);
        Trim(t);
        // If "(Type)123", return the numeric part.
        if (!t.empty() && t.front() == '(') {
            if (auto rp = t.rfind(')'); rp != std::string_view::npos && rp + 1 < t.size()) {
                return t.substr(rp + 1);
            }
        }
        // Otherwise, take the last segment of the enum name.
        if (auto p = t.rfind("::"); p != std::string_view::npos) {
            return t.substr(p + 2);
        }
        return t;
    }
#else
    // GCC/Clang: "__PRETTY_FUNCTION__" contains a "V = ..." fragment.
    static consteval std::string_view ValueToken(std::string_view s)
    {
        auto p = s.find("V = ");
        if (p == std::string_view::npos) {
            p = s.find("V =");
        }
        if (p == std::string_view::npos) {
            return {};
        }
        p += 4; // Skip "V = ".
        std::string_view t = s.substr(p);
        // Trim at ']' or ';' (format varies by version).
        if (auto rb = t.find(']'); rb != std::string_view::npos) {
            t = t.substr(0, rb);
        }
        if (auto sc = t.find(';'); sc != std::string_view::npos) {
            t = t.substr(0, sc);
        }
        Trim(t);
        // "(Type)123" => extract number; "NS::E::Name" => take last segment.
        if (!t.empty() && t.front() == '(') {
            if (auto rp = t.rfind(')'); rp != std::string_view::npos && rp + 1 < t.size()) {
                return t.substr(rp + 1);
            }
        }
        if (auto q = t.rfind("::"); q != std::string_view::npos) {
            return t.substr(q + 2);
        }
        return t;
    }
#endif

    static consteval bool LooksNumeric(std::string_view tok, long long v)
    {
        if (tok.empty()) {
            return true;
        }
        std::size_t i = 0;
        bool neg = false;
        if (tok[i] == '+' || tok[i] == '-') {
            neg = (tok[i] == '-');
            ++i;
        }
        if (i == tok.size()) {
            return true;
        }
        long long x = 0;
        for (; i < tok.size(); ++i) {
            const char c = tok[i];
            if (c < '0' || c > '9') {
                return false; // Non-digit means named.
            }
            x = x * 10 + (c - '0');
        }
        if (neg) {
            x = -x;
        }
        return x == v; // Pure number equal to underlying value => unnamed.
    }

    template <auto v>
    static consteval bool Declared()
    {
        using E = decltype(v);
        static_assert(std::is_enum_v<E>, "EnumHelper::Declared requires enum type");
        using U = std::underlying_type_t<E>;
        const auto sig = Pretty<E, static_cast<E>(v)>();
        const auto tok = ValueToken(sig);
        if (tok.empty()) {
            return false;
        }
        if (LooksNumeric(tok, static_cast<long long>(static_cast<U>(v)))) {
            return false;
        }
        return true;
    }

    // Build a compact value array from an index pack (avoid non-constant template args).
    template <typename E, int L, int R, int... Is>
    static consteval auto BuildValuesImpl(std::integer_sequence<int, Is...>)
    {
        constexpr std::size_t N = (std::size_t(Declared<static_cast<E>(L + Is)>()) + ... + 0);

        std::array<E, N> out {};
        std::size_t i = 0;
        ((Declared<static_cast<E>(L + Is)>() ? (out[i++] = static_cast<E>(L + Is), 0) : 0), ...);
        return out;
    }

public:
    // ----- customization point -----
    template <class E>
    struct ScanLimit {
        static constexpr int value = 32;
    };

    // ----- public API -----

    // EnumName: return the explicitly declared enumerator name (empty if unnamed/out of range).
    template <auto v>
    static constexpr std::string_view EnumName() noexcept
    {
        if constexpr (!Declared<v>()) {
            return {};
        }
        using E = decltype(v);
        return ValueToken(Pretty<E, static_cast<E>(v)>());
    }

    // EnumValues: return a compact std::array<E, N> (compile-time constant).
    // Strategy: collect explicitly declared enum values in [-ScanLimit, ScanLimit) or [0, ScanLimit).
    template <typename E>
    static consteval auto EnumValues()
    {
        static_assert(std::is_enum_v<E>, "EnumHelper::EnumValues requires enum type");
        using U = std::underlying_type_t<E>;
        constexpr int L = std::is_signed_v<U> ? -ScanLimit<E>::value : 0;
        constexpr int R = ScanLimit<E>::value;
        static_assert(R > L, "Scan range must be non-empty");
        return BuildValuesImpl<E, L, R>(std::make_integer_sequence<int, R - L> {});
    }

    template <class E, auto Arr = EnumHelper::EnumValues<E>()>
    using EnumTypeList = decltype(EnumArrToTypeList<Arr>(std::make_index_sequence<Arr.size()> {}));

    // Read-only iterator usable at compile time.
    template <class E>
    struct Iterator {
        const E* p {};
        constexpr E operator*() const noexcept { return *p; }
        constexpr Iterator& operator++() noexcept
        {
            ++p;
            return *this;
        }
        friend constexpr bool operator==(Iterator a, Iterator b) noexcept { return a.p == b.p; }
        friend constexpr bool operator!=(Iterator a, Iterator b) noexcept { return !(a == b); }
    };

    // Range-for friendly enum range (shared static compile-time array to avoid dangling).
    template <class E>
    struct Range {
        inline static constexpr auto kValues = EnumValues<E>();            // Single storage
        static constexpr const auto& values() noexcept { return kValues; } // Return by reference
        constexpr auto begin() const noexcept { return Iterator<E> {values().data()}; }
        constexpr auto end() const noexcept { return Iterator<E> {values().data() + values().size()}; }
    };
};

} // namespace lumina::mpl
