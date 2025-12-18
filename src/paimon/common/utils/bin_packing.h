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

#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

namespace paimon {
/// Contains bin packing implementations.
class BinPacking {
 public:
    BinPacking() = delete;
    ~BinPacking() = delete;

    template <typename T>
    static std::vector<std::vector<T>> PackForOrdered(
        std::vector<T>&& items, const std::function<int64_t(const T&)>& weight_func,
        int64_t target_weight) {
        std::vector<std::vector<T>> packed;
        std::vector<T> bin_items;
        int64_t bin_weight = 0;
        for (auto& item : items) {
            int64_t weight = weight_func(item);
            // when get a much big item or total weight enough, we check the binItems size. If
            // greater than zero, we pack it
            if (bin_weight + weight > target_weight && bin_items.size() > 0) {
                packed.emplace_back(std::move(bin_items));
                bin_items.clear();
                bin_weight = 0;
            }
            bin_weight += weight;
            bin_items.push_back(std::move(item));
        }
        if (bin_items.size() > 0) {
            packed.push_back(std::move(bin_items));
        }
        return packed;
    }
};
}  // namespace paimon
