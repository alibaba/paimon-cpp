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
#include <iterator>
#include <list>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace paimon {

template <typename K, typename V>
class LinkedHashMap {
 public:
    using IteratorType = typename std::list<std::pair<K, V>>::iterator;
    using ConstIteratorType = typename std::list<std::pair<K, V>>::const_iterator;

    LinkedHashMap() = default;
    LinkedHashMap(const LinkedHashMap& other) {
        order_ = other.order_;
        map_.clear();
        for (auto iter = order_.begin(); iter != order_.end(); ++iter) {
            map_[iter->first] = iter;
        }
    }

    LinkedHashMap& operator=(const LinkedHashMap& other) {
        order_ = other.order_;
        map_.clear();
        for (auto iter = order_.begin(); iter != order_.end(); ++iter) {
            map_[iter->first] = iter;
        }
        return *this;
    }

    bool operator==(const LinkedHashMap& other) const {
        if (this == &other) {
            return true;
        }
        return map_.size() == other.map_.size() && order_ == other.order_;
    }

    bool operator!=(const LinkedHashMap& other) const {
        return !(*this == other);
    }

    size_t size() const {
        return order_.size();
    }

    bool empty() const {
        return order_.empty();
    }

    IteratorType erase(const K& key) {
        auto map_iter = map_.find(key);
        if (map_iter == map_.end()) {
            return order_.end();
        }
        IteratorType order_iter = map_iter->second;
        map_.erase(map_iter);
        return order_.erase(order_iter);
    }

    IteratorType insert(const K& key, const V& value) {
        auto iter = map_.find(key);
        if (iter != map_.end()) {
            return iter->second;
        }
        return unsafe_insert(key, value);
    }

    IteratorType insert_or_assign(const K& key, const V& value) {
        auto iter = map_.find(key);
        if (iter != map_.end()) {
            // if key exists, update its value
            iter->second->second = value;
            return iter->second;
        }
        return unsafe_insert(key, value);
    }

    ConstIteratorType find(const K& key) const {
        auto iter = map_.find(key);
        if (iter != map_.end()) {
            return iter->second;
        }
        return order_.end();
    }

    ConstIteratorType begin() const {
        return order_.begin();
    }

    ConstIteratorType end() const {
        return order_.end();
    }

    V& operator[](const K& key) {
        auto iter = map_.find(key);
        if (iter != map_.end()) {
            // if key exists, return its value reference
            return iter->second->second;
        }
        return unsafe_insert(key, V())->second;
    }

    std::unordered_set<K> key_set() const {
        std::unordered_set<K> key_set;
        for (const auto& [key, _] : order_) {
            key_set.insert(key);
        }
        return key_set;
    }

    std::vector<K> key_vec() const {
        std::vector<K> key_vec;
        key_vec.reserve(order_.size());
        for (const auto& [key, _] : order_) {
            key_vec.push_back(key);
        }
        return key_vec;
    }

 private:
    IteratorType unsafe_insert(const K& key, const V& value) {
        auto new_iter = order_.insert(order_.end(), {key, value});
        map_[key] = new_iter;
        return new_iter;
    }

    // map_ {key, iterator in order}
    std::unordered_map<K, IteratorType> map_;
    std::list<std::pair<K, V>> order_;
};

}  // namespace paimon
