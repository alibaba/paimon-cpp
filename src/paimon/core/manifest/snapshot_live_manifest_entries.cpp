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

#include "paimon/core/manifest/snapshot_live_manifest_entries.h"

#include <algorithm>
#include <cstring>
#include <utility>

#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/core/manifest/manifest_entry_serializer.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/memory/memory_segment.h"

namespace paimon {
namespace {

constexpr int32_t kMagic = 0x534d4543;  // SMEC

size_t NormalizeMaxSnapshots(int32_t max_snapshots) {
    return static_cast<size_t>(std::max(0, max_snapshots));
}

std::shared_ptr<Bytes> ToBytes(const MemorySegmentOutputStream& out,
                               const std::shared_ptr<MemoryPool>& pool) {
    auto bytes = Bytes::AllocateBytes(static_cast<size_t>(out.CurrentSize()), pool.get());
    int64_t offset = 0;
    for (const auto& segment : out.Segments()) {
        int64_t copy_size =
            std::min<int64_t>(segment.Size(), static_cast<int64_t>(bytes->size()) - offset);
        if (copy_size <= 0) {
            break;
        }
        std::memcpy(bytes->data() + offset, segment.Data(), static_cast<size_t>(copy_size));
        offset += copy_size;
    }
    return bytes;
}

}  // namespace

SnapshotLiveManifestEntries::SnapshotLiveManifestEntries(int32_t max_snapshots)
    : max_snapshots_(max_snapshots) {}

std::optional<SnapshotLiveManifestEntries::Entry> SnapshotLiveManifestEntries::LatestBeforeOrEqual(
    int64_t snapshot_id) const {
    auto iter = entries_by_snapshot_.upper_bound(snapshot_id);
    if (iter == entries_by_snapshot_.begin()) {
        return std::optional<Entry>();
    }
    --iter;
    return Entry{iter->first, iter->second};
}

void SnapshotLiveManifestEntries::Put(int64_t snapshot_id, std::vector<ManifestEntry>&& entries) {
    if (NormalizeMaxSnapshots(max_snapshots_) == 0) {
        return;
    }
    entries_by_snapshot_[snapshot_id] =
        std::make_shared<const std::vector<ManifestEntry>>(std::move(entries));
    EvictIfNeeded();
}

size_t SnapshotLiveManifestEntries::Size() const {
    return entries_by_snapshot_.size();
}

Result<std::shared_ptr<Bytes>> SnapshotLiveManifestEntries::Serialize(
    const std::shared_ptr<MemoryPool>& pool) const {
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    out.WriteValue<int32_t>(kMagic);
    out.WriteValue<int32_t>(static_cast<int32_t>(entries_by_snapshot_.size()));

    ManifestEntrySerializer serializer(pool);
    for (const auto& [snapshot_id, entries] : entries_by_snapshot_) {
        out.WriteValue<int64_t>(snapshot_id);
        PAIMON_RETURN_NOT_OK(serializer.SerializeList(*entries, &out));
    }
    return ToBytes(out, pool);
}

Result<SnapshotLiveManifestEntries> SnapshotLiveManifestEntries::Deserialize(
    const MemorySegment& segment, int32_t max_snapshots, const std::shared_ptr<MemoryPool>& pool) {
    SnapshotLiveManifestEntries snapshot_live_manifest_entries(max_snapshots);
    if (segment.Data() == nullptr || segment.Size() == 0) {
        return snapshot_live_manifest_entries;
    }

    auto bytes = segment.GetOrCreateHeapMemory(pool.get());
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    DataInputStream in(input_stream);

    PAIMON_ASSIGN_OR_RAISE(int32_t magic, in.ReadValue<int32_t>());
    if (magic != kMagic) {
        return Status::Invalid("invalid snapshot live manifest entries magic");
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t snapshot_count, in.ReadValue<int32_t>());
    if (snapshot_count < 0) {
        return Status::Invalid("snapshot live manifest entries snapshot count is negative");
    }

    ManifestEntrySerializer serializer(pool);
    for (int32_t i = 0; i < snapshot_count; i++) {
        PAIMON_ASSIGN_OR_RAISE(int64_t snapshot_id, in.ReadValue<int64_t>());
        PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestEntry> entries, serializer.DeserializeList(&in));
        snapshot_live_manifest_entries.entries_by_snapshot_[snapshot_id] =
            std::make_shared<const std::vector<ManifestEntry>>(std::move(entries));
    }
    snapshot_live_manifest_entries.EvictIfNeeded();
    return snapshot_live_manifest_entries;
}

void SnapshotLiveManifestEntries::EvictIfNeeded() {
    size_t max_snapshots = NormalizeMaxSnapshots(max_snapshots_);
    while (entries_by_snapshot_.size() > max_snapshots) {
        entries_by_snapshot_.erase(entries_by_snapshot_.begin());
    }
}

}  // namespace paimon
