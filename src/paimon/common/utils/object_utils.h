/*
 * Copyright 2024-present Alibaba Inc.
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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "paimon/result.h"
#include "paimon/traits.h"
namespace paimon {
/// Utils for objects.
class ObjectUtils {
 public:
    ObjectUtils() = delete;
    ~ObjectUtils() = delete;

    template <typename T, typename U>
    static bool ContainsAll(const T& all, const U& contains) {
        using V = typename T::value_type;
        std::unordered_set<V> all_set(all.begin(), all.end());
        for (const auto& element : contains) {
            if (all_set.find(element) == all_set.end()) {
                return false;
            }
        }
        return true;
    }

    template <typename T>
    static bool Contains(const std::vector<T>& all, const T& target) {
        for (const auto& v : all) {
            if (v == target) {
                return true;
            }
        }
        return false;
    }

    template <typename T>
    static std::unordered_set<T> DuplicateItems(const std::vector<T>& entries) {
        std::unordered_map<T, int32_t> counts;
        for (const auto& entry : entries) {
            counts[entry]++;
        }

        std::unordered_set<T> duplicates;
        for (const auto& entry : counts) {
            if (entry.second > 1) {
                duplicates.insert(entry.first);
            }
        }
        return duplicates;
    }

    template <typename T>
    static bool TEST_Equal(const std::vector<T>& lhs, const std::vector<T>& rhs) {
        if (lhs.size() != rhs.size()) {
            return false;
        }
        for (size_t i = 0; i < lhs.size(); i++) {
            if constexpr (is_pointer<T>::value) {
                if (!lhs[i]->TEST_Equal(*rhs[i])) {
                    return false;
                }
            } else {
                if (!lhs[i].TEST_Equal(rhs[i])) {
                    return false;
                }
            }
        }
        return true;
    }

    template <typename T>
    static bool Equal(const std::vector<T>& lhs, const std::vector<T>& rhs) {
        if (lhs.size() != rhs.size()) {
            return false;
        }
        for (size_t i = 0; i < lhs.size(); i++) {
            if constexpr (is_pointer<T>::value) {
                if (!(*(lhs[i]) == *(rhs[i]))) {
                    return false;
                }
            } else {
                if (!(lhs[i] == rhs[i])) {
                    return false;
                }
            }
        }
        return true;
    }

    // used to create an identifier to index map, param func is used to convert T to identifier I
    // e.g., T = DataField, I = std::string, return std::map<std::string, int32_t>
    template <typename T, typename Func>
    static auto CreateIdentifierToIndexMap(const std::vector<T>& vec, const Func& func)
        -> std::map<decltype(func(std::declval<T>())), int32_t> {
        using KeyType = decltype(func(std::declval<T>()));
        std::map<KeyType, int32_t> result;
        for (int32_t i = 0; i < static_cast<int32_t>(vec.size()); ++i) {
            result[func(vec[i])] = i;
        }
        return result;
    }

    template <typename T>
    static std::map<T, int32_t> CreateIdentifierToIndexMap(const std::vector<T>& vec) {
        std::map<T, int32_t> index_map;
        for (int32_t i = 0; i < static_cast<int32_t>(vec.size()); i++) {
            index_map[vec[i]] = i;
        }
        return index_map;
    }

    /// Precondition: U and T must be pointer and U::value can move to T::value
    template <typename T, typename U>
    static std::vector<T> MoveVector(std::vector<U>&& input) {
        static_assert(is_pointer<U>::value && is_pointer<T>::value &&
                          std::is_convertible_v<value_type_traits_t<U>, value_type_traits_t<T>>,
                      "U and T must be pointer and U::value can move to T::value");
        std::vector<T> result;
        result.reserve(input.size());
        for (auto& item : input) {
            result.push_back(std::move(item));
        }
        input.clear();
        return result;
    }
};
}  // namespace paimon
