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

#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <utility>

#include "fmt/format.h"
#include "paimon/result.h"
#include "paimon/traits.h"

namespace paimon {
/// A generic LRU cache with support for weight-based eviction, time-based expiration,
/// and removal callbacks.
///
/// Uses std::list + unordered_map for O(1) get/put/evict:
/// - list stores entries in LRU order (most recently used at front)
/// - map stores key -> list::iterator for O(1) lookup
///
/// @tparam K Key type
/// @tparam V Value type
/// @tparam Hash Hash function for K (default: std::hash<K>)
/// @tparam KeyEqual Equality function for K (default: std::equal_to<K>)
///
/// @note Thread-safe: all public methods are protected by a read-write lock.
template <typename K, typename V, typename Hash = std::hash<K>,
          typename KeyEqual = std::equal_to<K>>
class GenericLruCache {
 public:
    /// Cause of a cache entry removal, passed to the removal callback.
    enum class RemovalCause {
        EXPLICIT,  // Removed by Invalidate() or InvalidateAll()
        SIZE,      // Evicted because total weight exceeded max_weight
        EXPIRED,   // Evicted because the entry expired (expireAfterAccess)
        REPLACED   // Replaced by a new value for the same key via Put()
    };

    using WeighFunc = std::function<int64_t(const K&, const V&)>;
    using RemovalCallback = std::function<void(const K&, const V&, RemovalCause)>;

    /// Configuration options for the cache.
    struct Options {
        /// Maximum total weight of all entries. Entries are evicted (LRU) when exceeded.
        int64_t max_weight = INT64_MAX;

        /// Time in milliseconds after last access before an entry expires.
        /// A value < 0 disables expiration.
        int64_t expire_after_access_ms = -1;

        /// Function to compute the weight of an entry. If nullptr, each entry has weight 1.
        WeighFunc weigh_func = nullptr;

        /// Callback invoked when an entry is removed (evicted, invalidated, or replaced).
        /// If nullptr, no callback is invoked.
        RemovalCallback removal_callback = nullptr;
    };

    explicit GenericLruCache(Options options) : options_(std::move(options)) {}

    /// Look up a key in the cache. On hit, promotes the entry to the front (most recently
    /// used) and updates its access time. Returns std::nullopt on miss or if the entry
    /// has expired.
    std::optional<V> GetIfPresent(const K& key) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        return FindPromoteOrExpire(key);
    }

    /// Look up a key. On miss, load via the supplier, insert into cache, and return.
    /// If the supplier returns an error, the error is propagated and nothing is cached.
    Result<V> Get(const K& key, std::function<Result<V>(const K&)> supplier) {
        {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            auto cached = FindPromoteOrExpire(key);
            if (cached.has_value()) {
                return std::move(cached.value());
            }
        }

        // Cache miss: load via supplier outside the lock
        PAIMON_ASSIGN_OR_RAISE(V value, supplier(key));
        int64_t weight = ComputeWeight(key, value);
        if (weight > options_.max_weight) {
            return value;
        }

        std::unique_lock<std::shared_mutex> lock(mutex_);
        // Double-check: another thread may have inserted while we were loading
        auto cached = FindPromoteOrExpire(key);
        if (cached.has_value()) {
            return std::move(cached.value());
        }

        InsertEntry(key, value, weight);
        EvictIfNeeded();
        return value;
    }

    /// Insert or update an entry. If the key already exists, the old value is replaced
    /// and the REPLACED callback is invoked. Triggers eviction if needed.
    /// @return Status::Invalid if the entry's weight exceeds max_weight, Status::OK otherwise.
    Status Put(const K& key, V value) {
        int64_t weight = ComputeWeight(key, value);
        if (weight > options_.max_weight) {
            return Status::Invalid(
                fmt::format("Entry weight {} exceeds cache max weight {}, entry will not be cached",
                            weight, options_.max_weight));
        }

        std::unique_lock<std::shared_mutex> lock(mutex_);
        auto it = lru_map_.find(key);
        if (it != lru_map_.end()) {
            if (ValuesEqual(it->second->value, value)) {
                Promote(it->second);
                return Status::OK();
            }
            ReplaceEntry(it->second, std::move(value), weight);
        } else {
            InsertEntry(key, std::move(value), weight);
        }

        EvictIfNeeded();
        return Status::OK();
    }

    /// Remove a specific entry. Invokes the EXPLICIT removal callback if the key exists.
    void Invalidate(const K& key) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        auto it = lru_map_.find(key);
        if (it != lru_map_.end()) {
            RemoveEntry(it->second, RemovalCause::EXPLICIT);
        }
    }

    /// Remove all entries. Each entry's EXPLICIT removal callback is invoked.
    void InvalidateAll() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        while (!lru_list_.empty()) {
            RemoveEntry(std::prev(lru_list_.end()), RemovalCause::EXPLICIT);
        }
        current_weight_ = 0;
    }

    /// @return The number of entries currently in the cache.
    size_t Size() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return lru_map_.size();
    }

    /// @return The current total weight of all entries.
    int64_t GetCurrentWeight() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return current_weight_;
    }

    /// @return The maximum weight configured for this cache.
    int64_t GetMaxWeight() const {
        return options_.max_weight;
    }

 private:
    struct CacheEntry {
        K key;
        V value;
        int64_t weight;
        std::chrono::steady_clock::time_point last_access_time;
    };

    using EntryList = std::list<CacheEntry>;
    using EntryMap = std::unordered_map<K, typename EntryList::iterator, Hash, KeyEqual>;

    /// Look up a key, promote if found and not expired, or remove if expired.
    /// Must be called with mutex_ held.
    /// @return The value if found and valid, std::nullopt otherwise.
    std::optional<V> FindPromoteOrExpire(const K& key) {
        auto it = lru_map_.find(key);
        if (it == lru_map_.end()) {
            return std::nullopt;
        }
        auto list_it = it->second;
        if (IsExpired(list_it->last_access_time)) {
            RemoveEntry(list_it, RemovalCause::EXPIRED);
            return std::nullopt;
        }
        Promote(list_it);
        return list_it->value;
    }

    /// Move an entry to the front of the LRU list and update its access time.
    void Promote(typename EntryList::iterator list_it) {
        list_it->last_access_time = std::chrono::steady_clock::now();
        lru_list_.splice(lru_list_.begin(), lru_list_, list_it);
    }

    /// Insert a new entry at the front of the LRU list.
    void InsertEntry(const K& key, V value, int64_t weight) {
        lru_list_.emplace_front(
            CacheEntry{key, std::move(value), weight, std::chrono::steady_clock::now()});
        lru_map_[key] = lru_list_.begin();
        current_weight_ += weight;
    }

    /// Compare two values for equality. For pointers, compares the underlying
    /// pointer first, then dereferences and compares the pointed-to objects.
    /// For other types, uses operator==.
    static bool ValuesEqual(const V& lhs, const V& rhs) {
        if constexpr (is_pointer<V>::value) {
            if (lhs == rhs) {
                return true;
            }
            if (!lhs || !rhs) {
                return false;
            }
            return *lhs == *rhs;
        } else {
            return lhs == rhs;
        }
    }

    /// Replace the value of an existing entry, invoke the REPLACED callback for the old value,
    /// and promote the entry to the front.
    void ReplaceEntry(typename EntryList::iterator list_it, V new_value, int64_t new_weight) {
        current_weight_ -= list_it->weight;

        K key = list_it->key;
        V old_value = std::move(list_it->value);
        list_it->value = std::move(new_value);
        list_it->weight = new_weight;
        list_it->last_access_time = std::chrono::steady_clock::now();
        current_weight_ += new_weight;
        lru_list_.splice(lru_list_.begin(), lru_list_, list_it);

        InvokeCallback(key, old_value, RemovalCause::REPLACED);
    }

    /// Remove an entry from the cache and invoke the removal callback.
    void RemoveEntry(typename EntryList::iterator list_it, RemovalCause cause) {
        lru_map_.erase(list_it->key);
        K key = std::move(list_it->key);
        V value = std::move(list_it->value);
        current_weight_ -= list_it->weight;
        lru_list_.erase(list_it);
        InvokeCallback(key, value, cause);
    }

    /// Evict expired entries from the tail, then evict by weight if still over capacity.
    void EvictIfNeeded() {
        EvictExpired();
        while (current_weight_ > options_.max_weight && !lru_list_.empty()) {
            RemoveEntry(std::prev(lru_list_.end()), RemovalCause::SIZE);
        }
    }

    /// Evict expired entries from the tail of the LRU list.
    /// Since the tail has the oldest access time, we can stop as soon as we find
    /// a non-expired entry.
    void EvictExpired() {
        if (options_.expire_after_access_ms < 0) {
            return;
        }
        auto now = std::chrono::steady_clock::now();
        while (!lru_list_.empty()) {
            auto it = std::prev(lru_list_.end());
            if (!IsExpired(it->last_access_time, now)) {
                break;
            }
            RemoveEntry(it, RemovalCause::EXPIRED);
        }
    }

    /// Compute the weight of an entry using the configured weigh function.
    int64_t ComputeWeight(const K& key, const V& value) const {
        if (options_.weigh_func) {
            return options_.weigh_func(key, value);
        }
        return 1;
    }

    /// Check if an entry has expired based on its last access time.
    bool IsExpired(
        const std::chrono::steady_clock::time_point& last_access_time,
        const std::chrono::steady_clock::time_point& now = std::chrono::steady_clock::now()) const {
        if (options_.expire_after_access_ms < 0) {
            return false;
        }
        auto elapsed =
            std::chrono::duration_cast<std::chrono::milliseconds>(now - last_access_time);
        return elapsed.count() >= options_.expire_after_access_ms;
    }

    /// Invoke the removal callback if one is configured.
    void InvokeCallback(const K& key, const V& value, RemovalCause cause) {
        if (options_.removal_callback) {
            options_.removal_callback(key, value, cause);
        }
    }

    Options options_;
    int64_t current_weight_ = 0;
    EntryList lru_list_;
    EntryMap lru_map_;
    mutable std::shared_mutex mutex_;
};

}  // namespace paimon
