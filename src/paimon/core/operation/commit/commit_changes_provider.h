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

#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/snapshot.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

struct CommitChanges {
    CommitChanges() = default;

    CommitChanges(std::vector<ManifestEntry> delta, std::vector<ManifestEntry> changelog,
                  std::vector<IndexManifestEntry> index)
        : delta_files(std::move(delta)),
          changelog_files(std::move(changelog)),
          index_entries(std::move(index)) {}

    std::vector<ManifestEntry> delta_files;
    std::vector<ManifestEntry> changelog_files;
    std::vector<IndexManifestEntry> index_entries;
};

class CommitChangesProvider {
 public:
    virtual ~CommitChangesProvider() = default;

    static std::shared_ptr<CommitChangesProvider> Provider(
        std::vector<ManifestEntry> delta_files, std::vector<ManifestEntry> changelog_files,
        std::vector<IndexManifestEntry> index_entries);

    virtual Result<std::shared_ptr<CommitChanges>> Provide(
        const std::optional<Snapshot>& latest_snapshot) const = 0;
};

}  // namespace paimon
