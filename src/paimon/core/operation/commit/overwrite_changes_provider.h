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
#include <vector>

#include "paimon/core/operation/commit/commit_changes_provider.h"

namespace paimon {

class OverwriteChangesProvider final : public CommitChangesProvider {
 public:
    using ManifestScan =
        std::function<Result<std::vector<ManifestEntry>>(const Snapshot& snapshot)>;
    using IndexScan =
        std::function<Result<std::vector<IndexManifestEntry>>(const Snapshot& snapshot)>;

    OverwriteChangesProvider(std::vector<ManifestEntry> changes,
                             std::vector<IndexManifestEntry> index_entries,
                             ManifestScan manifest_scan, IndexScan index_scan);

    Result<std::shared_ptr<CommitChanges>> Provide(
        const std::optional<Snapshot>& latest_snapshot) const override;

 private:
    std::vector<ManifestEntry> changes_;
    std::vector<IndexManifestEntry> index_entries_;
    ManifestScan manifest_scan_;
    IndexScan index_scan_;
};

}  // namespace paimon
