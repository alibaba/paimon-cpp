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

#include "paimon/core/operation/commit/commit_changes_provider.h"

#include <utility>

namespace paimon {

namespace {

class FixedInputCommitChangesProvider final : public CommitChangesProvider {
 public:
    FixedInputCommitChangesProvider(std::vector<ManifestEntry> delta_files,
                                    std::vector<ManifestEntry> changelog_files,
                                    std::vector<IndexManifestEntry> index_entries)
        : commit_changes_(std::make_shared<CommitChanges>(
              std::move(delta_files), std::move(changelog_files), std::move(index_entries))) {}

    Result<std::shared_ptr<CommitChanges>> Provide(const std::optional<Snapshot>&) const override {
        return commit_changes_;
    }

 private:
    std::shared_ptr<CommitChanges> commit_changes_;
};

}  // namespace

std::shared_ptr<CommitChangesProvider> CommitChangesProvider::Provider(
    std::vector<ManifestEntry> delta_files, std::vector<ManifestEntry> changelog_files,
    std::vector<IndexManifestEntry> index_entries) {
    return std::make_shared<FixedInputCommitChangesProvider>(
        std::move(delta_files), std::move(changelog_files), std::move(index_entries));
}

}  // namespace paimon
