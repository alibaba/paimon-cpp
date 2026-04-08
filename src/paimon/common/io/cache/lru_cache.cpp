/*
 * Copyright 2026-present Alibaba Inc.
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

#include "paimon/common/io/cache/lru_cache.h"

namespace paimon {

LruCache::LruCache(int64_t max_weight) : max_weight_(max_weight), current_weight_(0) {}

Result<std::shared_ptr<CacheValue>> LruCache::Get(
    const std::shared_ptr<CacheKey>& key,
    std::function<Result<std::shared_ptr<CacheValue>>(const std::shared_ptr<CacheKey>&)> supplier) {
    {
        std::unique_lock<std::shared_mutex> write_lock(mutex_);
        auto cached = FindAndPromote(key);
        if (cached) {
            return cached.value();
        }
    }
    // Cache miss: load via supplier (outside lock)
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<CacheValue> value, supplier(key));
    if (GetWeight(value) > max_weight_) {
        return value;
    }

    std::unique_lock<std::shared_mutex> write_lock(mutex_);
    // Another thread may have inserted the key while we were loading
    auto cached = FindAndPromote(key);
    if (cached) {
        return cached.value();
    }

    Insert(key, value);
    EvictIfNeeded();
    return value;
}

void LruCache::Put(const std::shared_ptr<CacheKey>& key, const std::shared_ptr<CacheValue>& value) {
    if (GetWeight(value) > max_weight_) {
        return;
    }
    std::unique_lock<std::shared_mutex> write_lock(mutex_);

    auto it = lru_map_.find(key);
    if (it != lru_map_.end()) {
        // Update existing entry: adjust weight
        current_weight_ -= GetWeight(it->second->second);
        it->second->second = value;
        current_weight_ += GetWeight(value);
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
    } else {
        Insert(key, value);
    }

    EvictIfNeeded();
}

void LruCache::Invalidate(const std::shared_ptr<CacheKey>& key) {
    std::unique_lock<std::shared_mutex> write_lock(mutex_);

    auto it = lru_map_.find(key);
    if (it != lru_map_.end()) {
        RemoveEntry(it->second);
    }
}

void LruCache::InvalidateAll() {
    std::unique_lock<std::shared_mutex> write_lock(mutex_);

    while (!lru_list_.empty()) {
        RemoveEntry(std::prev(lru_list_.end()));
    }
    current_weight_ = 0;
}

size_t LruCache::Size() const {
    std::shared_lock<std::shared_mutex> read_lock(mutex_);
    return lru_map_.size();
}

int64_t LruCache::GetCurrentWeight() const {
    std::shared_lock<std::shared_mutex> read_lock(mutex_);
    return current_weight_;
}

int64_t LruCache::GetMaxWeight() const {
    return max_weight_;
}

std::optional<std::shared_ptr<CacheValue>> LruCache::FindAndPromote(
    const std::shared_ptr<CacheKey>& key) {
    auto it = lru_map_.find(key);
    if (it != lru_map_.end()) {
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
        return it->second->second;
    }
    return std::nullopt;
}

void LruCache::Insert(const std::shared_ptr<CacheKey>& key,
                      const std::shared_ptr<CacheValue>& value) {
    // Insert at front of LRU list
    lru_list_.emplace_front(key, value);
    lru_map_[key] = lru_list_.begin();
    current_weight_ += GetWeight(value);
}

void LruCache::RemoveEntry(LruList::iterator list_it) {
    auto entry_key = list_it->first;
    auto entry_value = list_it->second;
    current_weight_ -= GetWeight(entry_value);
    lru_map_.erase(entry_key);
    lru_list_.erase(list_it);

    if (entry_value) {
        entry_value->OnEvict(entry_key);
    }
}

void LruCache::EvictIfNeeded() {
    while (current_weight_ > max_weight_ && !lru_list_.empty()) {
        RemoveEntry(std::prev(lru_list_.end()));
    }
}

int64_t LruCache::GetWeight(const std::shared_ptr<CacheValue>& value) {
    if (!value) {
        return 0;
    }
    return value->GetSegment().Size();
}
}  // namespace paimon
