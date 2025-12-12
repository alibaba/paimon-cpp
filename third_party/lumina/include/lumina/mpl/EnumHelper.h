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
    static consteval std::string_view ValueToken(std::string_view s)
    {
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
        if (!t.empty() && t.front() == '(') {
            if (auto rp = t.rfind(')'); rp != std::string_view::npos && rp + 1 < t.size()) {
                return t.substr(rp + 1);
            }
        }
        if (auto p = t.rfind("::"); p != std::string_view::npos) {
            return t.substr(p + 2);
        }
        return t;
    }
#else
    static consteval std::string_view ValueToken(std::string_view s)
    {
        auto p = s.find("V = ");
        if (p == std::string_view::npos) {
            p = s.find("V =");
        }
        if (p == std::string_view::npos) {
            return {};
        }
        p += 4;
        std::string_view t = s.substr(p);
        if (auto rb = t.find(']'); rb != std::string_view::npos) {
            t = t.substr(0, rb);
        }
        if (auto sc = t.find(';'); sc != std::string_view::npos) {
            t = t.substr(0, sc);
        }
        Trim(t);
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
                return false;
            }
            x = x * 10 + (c - '0');
        }
        if (neg) {
            x = -x;
        }
        return x == v;
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
    template <class E>
    struct ScanLimit {
        static constexpr int value = 32;
    };


    template <auto v>
    static constexpr std::string_view EnumName() noexcept
    {
        if constexpr (!Declared<v>()) {
            return {};
        }
        using E = decltype(v);
        return ValueToken(Pretty<E, static_cast<E>(v)>());
    }

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

    template <class E>
    struct Range {
        inline static constexpr auto kValues = EnumValues<E>();
        static constexpr const auto& values() noexcept { return kValues; }
        constexpr auto begin() const noexcept { return Iterator<E> {values().data()}; }
        constexpr auto end() const noexcept { return Iterator<E> {values().data() + values().size()}; }
    };
};

}
