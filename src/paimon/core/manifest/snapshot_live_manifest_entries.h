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

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <vector>

#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/result.h"

namespace paimon {
class Bytes;
class MemoryPool;
class MemorySegment;

/// Live manifest entries retained for one bucket across multiple snapshots.
///
/// This value object owns merged live manifest entries by snapshot id. It does not own or access a
/// cache; callers are responsible for storing the serialized bytes in the cache layer.
class SnapshotLiveManifestEntries {
 public:
    struct Entry {
        int64_t snapshot_id;
        std::shared_ptr<const std::vector<ManifestEntry>> entries;
    };

    explicit SnapshotLiveManifestEntries(int32_t max_snapshots);

    std::optional<Entry> LatestBeforeOrEqual(int64_t snapshot_id) const;
    void Put(int64_t snapshot_id, std::vector<ManifestEntry>&& entries);
    size_t Size() const;

    Result<std::shared_ptr<Bytes>> Serialize(const std::shared_ptr<MemoryPool>& pool) const;
    static Result<SnapshotLiveManifestEntries> Deserialize(const MemorySegment& segment,
                                                           int32_t max_snapshots,
                                                           const std::shared_ptr<MemoryPool>& pool);

 private:
    void EvictIfNeeded();

    std::map<int64_t, std::shared_ptr<const std::vector<ManifestEntry>>> entries_by_snapshot_;
    int32_t max_snapshots_;
};

}  // namespace paimon
