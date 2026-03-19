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

#include "paimon/core/index/index_file_handler.h"

#include <functional>
#include <optional>

#include "paimon/core/snapshot.h"
#include "paimon/status.h"

namespace paimon {

Result<IndexFileHandler::IndexFileMetaGroups> IndexFileHandler::Scan(
    const Snapshot& snapshot, const std::string& index_type,
    const std::unordered_set<BinaryRow>& partitions) const {
    IndexFileHandler::IndexFileMetaGroups result;
    std::function<Result<bool>(const IndexManifestEntry&)> filter =
        [&](const IndexManifestEntry& entry) -> bool {
        if (entry.index_file->IndexType() == index_type &&
            partitions.find(entry.partition) != partitions.end()) {
            return true;
        }
        return false;
    };

    PAIMON_ASSIGN_OR_RAISE(std::vector<IndexManifestEntry> index_entries, Scan(snapshot, filter));
    for (const auto& entry : index_entries) {
        std::pair<BinaryRow, int32_t> key(entry.partition, entry.bucket);
        result[key].push_back(entry.index_file);
    }
    return result;
}

Result<std::vector<IndexManifestEntry>> IndexFileHandler::Scan(
    const Snapshot& snapshot, std::function<Result<bool>(const IndexManifestEntry&)> filter) const {
    const std::optional<std::string>& index_manifest = snapshot.IndexManifest();
    if (index_manifest == std::nullopt) {
        return std::vector<IndexManifestEntry>();
    }
    std::vector<IndexManifestEntry> index_entries;
    PAIMON_RETURN_NOT_OK(
        index_manifest_file_->Read(index_manifest.value(), filter, &index_entries));
    return index_entries;
}

Result<std::vector<std::shared_ptr<IndexFileMeta>>> IndexFileHandler::Scan(
    const Snapshot& snapshot, const std::string& index_type, const BinaryRow& partition,
    int32_t bucket) const {
    PAIMON_ASSIGN_OR_RAISE(IndexFileHandler::IndexFileMetaGroups index_file_meta_groups,
                           Scan(snapshot, index_type, {partition}));
    std::pair<BinaryRow, int32_t> key(partition, bucket);
    auto iter = index_file_meta_groups.find(key);
    if (iter != index_file_meta_groups.end()) {
        return iter->second;
    }
    return std::vector<std::shared_ptr<IndexFileMeta>>{};
}

}  // namespace paimon
