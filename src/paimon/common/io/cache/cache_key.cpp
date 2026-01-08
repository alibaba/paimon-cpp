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

#include "paimon/common/io/cache/cache_key.h"

namespace paimon {

std::shared_ptr<CacheKey> CacheKey::ForPosition(const std::string& file_path, int64_t position,
                                                int32_t length, bool is_index) {
    return std::make_shared<PositionCacheKey>(file_path, position, length, is_index);
}

bool PositionCacheKey::IsIndex() {
    return is_index_;
}

int64_t PositionCacheKey::Position() {
    return position_;
}

int32_t PositionCacheKey::Length() {
    return length_;
}

bool PositionCacheKey::operator==(const PositionCacheKey& other) const {
    return file_path_ == other.file_path_ && position_ == other.position_ &&

           length_ == other.length_ && is_index_ == other.is_index_;
}

size_t PositionCacheKey::HashCode() const {
    size_t seed = 0;
    seed ^=
        std::hash<std::string>{}(file_path_) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
    seed ^= std::hash<int64_t>{}(position_) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
    seed ^= std::hash<int32_t>{}(length_) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
    seed ^= std::hash<bool>{}(is_index_) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
    return seed;
}
}  // namespace paimon
