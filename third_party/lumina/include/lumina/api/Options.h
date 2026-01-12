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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>

#include <lumina/core/Result.h>
#include <lumina/core/Status.h>
#include <lumina/mpl/Concepts.h>
#include <lumina/telemetry/Log.h>

namespace lumina::api {

enum class OptionsType {
    Search,
    Searcher,
    Builder,
    Quantizer,
    IO,
};

template <OptionsType T>
class Options
{
public:
    Options() = default;
    ~Options() = default;

    using Value = std::variant<int64_t, double, bool, std::string>;
    using Map = std::unordered_map<std::string, Value>;

    // -- Set: fluent API --
    template <typename K, typename V>
    std::enable_if_t<lumina::mpl::Stringable<K>, Options&> Set(K&& key, V&& v)
    {
        if constexpr (lumina::mpl::LuminaConstructibleFrom<Value, V&&>) {
            _values[std::string(key)] = std::forward<V>(v);
        } else if constexpr (mpl::Stringable<V&&>) {
            _values[std::string(key)] = std::string(std::forward<V>(v));
        } else {
            static_assert(std::is_same_v<void, std::void_t<K>> && false);
        }
        return *this;
    }

    int64_t GetInt(std::string_view key, int64_t def) const noexcept
    {
        auto v = Get<int64_t>(key);
        return v ? *v : def;
    }
    double GetDouble(std::string_view key, double def) const noexcept
    {
        auto v = Get<double>(key);
        return v ? *v : def;
    }
    bool GetBool(std::string_view key, bool def) const noexcept
    {
        auto v = Get<bool>(key);
        return v ? *v : def;
    }
    std::string GetString(std::string_view key, std::string def) const
    {
        auto v = Get<std::string>(key);
        return v ? *v : std::move(def);
    }

    template <class V>
    bool HasValueOfType(std::string_view key) const
    {
        auto it = _values.find(std::string(key));
        if (it == _values.end()) {
            return false;
        }
        if (auto p = std::get_if<V>(&it->second)) {
            return true;
        }
        return false;
    }

    template <OptionsType V>
    Options& MergeFrom(const Options<V>& other)
    {
        for (const auto& kv : other._values) {
            _values.emplace(kv.first, kv.second);
        }
        return *this;
    }

    template <OptionsType V = T>
    Options<V> Derive(std::string_view keyPrefix) const
    {
        Options<V> options;
        for (const auto& kv : _values) {
            if (kv.first.compare(0, keyPrefix.size(), keyPrefix) == 0) {
                options.Set(kv.first, kv.second);
            }
        }
        return options;
    }

    // Direct access to all key/value pairs (read-only)
    const Map& Values() const noexcept { return _values; }

    // Debug string representation of all options
    std::string DebugString() const
    {
        std::ostringstream oss;
        oss << "{";
        bool first = true;
        for (const auto& kv : _values) {
            if (!first) {
                oss << ", ";
            }
            oss << "\"" << kv.first << "\":";
            std::visit(
                [&oss](const auto& value) {
                    using ValueType = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<ValueType, int64_t>) {
                        oss << value;
                    } else if constexpr (std::is_same_v<ValueType, double>) {
                        oss << value;
                    } else if constexpr (std::is_same_v<ValueType, bool>) {
                        oss << (value ? "true" : "false");
                    } else if constexpr (std::is_same_v<ValueType, std::string>) {
                        oss << "\"" << value << "\"";
                    }
                },
                kv.second);
            first = false;
        }
        oss << "}";
        return oss.str();
    }

private:
    template <class V>
    std::optional<V> Get(std::string_view key) const
    {
        auto it = _values.find(std::string(key));
        if (it == _values.end()) {
            return std::nullopt;
        }
        if (auto p = std::get_if<V>(&it->second)) {
            return *p;
        }
        return std::nullopt;
    }

    Map _values;

    template <OptionsType V>
    friend class Options;
};

using SearchOptions = Options<OptionsType::Search>;
using SearcherOptions = Options<OptionsType::Searcher>;
using BuilderOptions = Options<OptionsType::Builder>;
using QuantizerOptions = Options<OptionsType::Quantizer>;
using IOOptions = Options<OptionsType::IO>;

} // namespace lumina::api
