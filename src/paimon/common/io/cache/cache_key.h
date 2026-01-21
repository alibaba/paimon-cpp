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
#include <functional>
#include <memory>
#include <string>

#include "paimon/status.h"

namespace paimon {

class CacheKey {
 public:
    static std::shared_ptr<CacheKey> ForPosition(const std::string& file_path, int64_t position,
                                                 int32_t length, bool is_index);

 public:
    virtual ~CacheKey() = default;

    virtual bool IsIndex() = 0;
};

class PositionCacheKey : public CacheKey {
 public:
    PositionCacheKey(const std::string& file_path, int64_t position, int32_t length, bool is_index)
        : file_path_(file_path), position_(position), length_(length), is_index_(is_index) {}

    bool IsIndex() override;

    int64_t Position() const;
    int32_t Length() const;

    bool operator==(const PositionCacheKey& other) const;
    size_t HashCode() const;

 private:
    static constexpr uint64_t HASH_CONSTANT = 0x9e3779b97f4a7c15ULL;

    const std::string file_path_;
    const int64_t position_;
    const int32_t length_;
    const bool is_index_;
};
}  // namespace paimon

namespace std {
template <>
struct hash<paimon::PositionCacheKey> {
    size_t operator()(const paimon::PositionCacheKey& key) const {
        return key.HashCode();
    }
};
}  // namespace std
