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

#include "paimon/common/io/cache/cache_key.h"

namespace paimon {
namespace {

class SnapshotLiveManifestEntriesCacheKey : public CacheKey {
 public:
    SnapshotLiveManifestEntriesCacheKey(const std::string& table_path, const std::string& branch,
                                        int32_t bucket)
        : CacheKey(CacheKind::SNAPSHOT_LIVE_MANIFEST),
          table_path_(table_path),
          branch_(branch),
          bucket_(bucket) {}

    bool IsIndex() const override {
        return false;
    }

    bool Equals(const CacheKey& other) const override {
        const auto* rhs = dynamic_cast<const SnapshotLiveManifestEntriesCacheKey*>(&other);
        if (!rhs) {
            return false;
        }
        return table_path_ == rhs->table_path_ && branch_ == rhs->branch_ &&
               bucket_ == rhs->bucket_ && GetKind() == rhs->GetKind();
    }

    size_t HashCode() const override {
        size_t seed = 0;
        seed ^= std::hash<std::string>{}(table_path_) + HASH_CONSTANT + (seed << 6) + (seed >> 2);
        seed ^= std::hash<std::string>{}(branch_) + HASH_CONSTANT + (seed << 6) + (seed >> 2);
        seed ^= std::hash<int32_t>{}(bucket_) + HASH_CONSTANT + (seed << 6) + (seed >> 2);
        seed ^= std::hash<int32_t>{}(static_cast<int32_t>(GetKind())) + HASH_CONSTANT +
                (seed << 6) + (seed >> 2);
        return seed;
    }

 private:
    static constexpr uint64_t HASH_CONSTANT = 0x9e3779b97f4a7c15ULL;

    const std::string table_path_;
    const std::string branch_;
    const int32_t bucket_;
};

}  // namespace

std::shared_ptr<CacheKey> CacheKey::ForPosition(const std::string& file_path, int64_t position,
                                                int32_t length, bool is_index) {
    return std::make_shared<PositionCacheKey>(file_path, position, length, is_index,
                                              CacheKind::DEFAULT);
}

std::shared_ptr<CacheKey> CacheKey::ForKind(const std::string& file_path, int64_t position,
                                            int32_t length, CacheKind kind) {
    auto key = std::make_shared<PositionCacheKey>(file_path, position, length,
                                                  /*is_index=*/false, kind);
    return key;
}

std::shared_ptr<CacheKey> CacheKey::ForSnapshotLiveManifestEntries(const std::string& table_path,
                                                                   const std::string& branch,
                                                                   int32_t bucket) {
    return std::make_shared<SnapshotLiveManifestEntriesCacheKey>(table_path, branch, bucket);
}

bool PositionCacheKey::IsIndex() const {
    return is_index_;
}

int64_t PositionCacheKey::Position() const {
    return position_;
}

int32_t PositionCacheKey::Length() const {
    return length_;
}

bool PositionCacheKey::Equals(const CacheKey& other) const {
    const auto* rhs = dynamic_cast<const PositionCacheKey*>(&other);
    if (!rhs) {
        return false;
    }
    return file_path_ == rhs->file_path_ && position_ == rhs->position_ &&
           length_ == rhs->length_ && is_index_ == rhs->is_index_ && GetKind() == rhs->GetKind();
}

size_t PositionCacheKey::HashCode() const {
    size_t seed = 0;
    seed ^= std::hash<std::string>{}(file_path_) + HASH_CONSTANT + (seed << 6) + (seed >> 2);
    seed ^= std::hash<int64_t>{}(position_) + HASH_CONSTANT + (seed << 6) + (seed >> 2);
    seed ^= std::hash<int32_t>{}(length_) + HASH_CONSTANT + (seed << 6) + (seed >> 2);
    seed ^= std::hash<bool>{}(is_index_) + HASH_CONSTANT + (seed << 6) + (seed >> 2);
    seed ^= std::hash<int32_t>{}(static_cast<int32_t>(GetKind())) + HASH_CONSTANT + (seed << 6) +
            (seed >> 2);
    return seed;
}

}  // namespace paimon
