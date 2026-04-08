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
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "paimon/visibility.h"

namespace paimon {

class CacheValue;

class PAIMON_EXPORT CacheKey {
 public:
    static std::shared_ptr<CacheKey> ForPosition(const std::string& file_path, int64_t position,
                                                 int32_t length, bool is_index);

 public:
    virtual ~CacheKey() = default;

    virtual bool IsIndex() const = 0;

    virtual bool Equals(const CacheKey& other) const = 0;

    virtual size_t HashCode() const = 0;
};

class PositionCacheKey : public CacheKey {
 public:
    PositionCacheKey(const std::string& file_path, int64_t position, int32_t length, bool is_index)
        : file_path_(file_path), position_(position), length_(length), is_index_(is_index) {}

    bool IsIndex() const override;
    size_t HashCode() const override;
    bool Equals(const CacheKey& other) const override;
    int64_t Position() const;
    int32_t Length() const;

 private:
    static constexpr uint64_t HASH_CONSTANT = 0x9e3779b97f4a7c15ULL;

    const std::string file_path_;
    const int64_t position_;
    const int32_t length_;
    const bool is_index_;
};

struct CacheKeyHash {
    size_t operator()(const std::shared_ptr<CacheKey>& key) const {
        return key ? key->HashCode() : 0;
    }
};

struct CacheKeyEqual {
    bool operator()(const std::shared_ptr<CacheKey>& lhs,
                    const std::shared_ptr<CacheKey>& rhs) const {
        if (lhs == rhs) {
            return true;
        }
        if (!lhs || !rhs) {
            return false;
        }
        return lhs->Equals(*rhs);
    }
};

}  // namespace paimon
