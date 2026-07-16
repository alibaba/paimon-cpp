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
#include <optional>
#include <string>
#include <vector>

#include "paimon/commit_message.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/status.h"

namespace paimon {

/// Detailed changes from `CommitMessage`s.
class ManifestEntryChanges {
 public:
    explicit ManifestEntryChanges(int32_t default_num_bucket);

    Status Collect(const std::shared_ptr<CommitMessage>& message);

    bool HasAppendChanges() const;

    bool HasGlobalIndexFileAdditions() const;

    bool HasCompactChanges() const;

    std::string ToString() const;

    static std::vector<BinaryRow> ChangedPartitions(
        const std::vector<ManifestEntry>& data_file_changes,
        const std::vector<IndexManifestEntry>& index_file_changes);

 public:
    std::vector<ManifestEntry> append_table_files;
    std::vector<ManifestEntry> append_changelog;
    std::vector<IndexManifestEntry> append_index_files;
    std::vector<ManifestEntry> compact_table_files;
    std::vector<ManifestEntry> compact_changelog;
    std::vector<IndexManifestEntry> compact_index_files;

 private:
    ManifestEntry MakeEntry(const FileKind& kind,
                            const std::shared_ptr<CommitMessageImpl>& commit_message,
                            const std::shared_ptr<DataFileMeta>& file) const;

 private:
    int32_t default_num_bucket_;
};

}  // namespace paimon
