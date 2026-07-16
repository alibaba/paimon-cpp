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

#include "paimon/core/operation/commit/overwrite_changes_provider.h"

#include <utility>

#include "paimon/core/manifest/file_kind.h"

namespace paimon {

OverwriteChangesProvider::OverwriteChangesProvider(std::vector<ManifestEntry> changes,
                                                   std::vector<IndexManifestEntry> index_entries,
                                                   ManifestScan manifest_scan, IndexScan index_scan)
    : changes_(std::move(changes)),
      index_entries_(std::move(index_entries)),
      manifest_scan_(std::move(manifest_scan)),
      index_scan_(std::move(index_scan)) {}

Result<std::shared_ptr<CommitChanges>> OverwriteChangesProvider::Provide(
    const std::optional<Snapshot>& latest_snapshot) const {
    std::vector<ManifestEntry> delta_files;
    std::vector<ManifestEntry> changelog_files;
    std::vector<IndexManifestEntry> index_entries = index_entries_;

    if (!latest_snapshot) {
        delta_files.insert(delta_files.end(), changes_.begin(), changes_.end());
        return std::make_shared<CommitChanges>(std::move(delta_files), std::move(changelog_files),
                                               std::move(index_entries));
    }

    PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestEntry> entries,
                           manifest_scan_(latest_snapshot.value()));
    for (const auto& entry : entries) {
        delta_files.emplace_back(FileKind::Delete(), entry.Partition(), entry.Bucket(),
                                 entry.TotalBuckets(), entry.File());
    }

    delta_files.insert(delta_files.end(), changes_.begin(), changes_.end());

    PAIMON_ASSIGN_OR_RAISE(std::vector<IndexManifestEntry> previous_index_entries,
                           index_scan_(latest_snapshot.value()));
    for (const auto& entry : previous_index_entries) {
        index_entries.emplace_back(FileKind::Delete(), entry.partition, entry.bucket,
                                   entry.index_file);
    }

    return std::make_shared<CommitChanges>(std::move(delta_files), std::move(changelog_files),
                                           std::move(index_entries));
}

}  // namespace paimon
