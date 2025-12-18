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

#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
/// IndexManifestFile Handler.
class IndexManifestFileHandler {
 public:
    IndexManifestFileHandler() = delete;
    ~IndexManifestFileHandler() = delete;

    static Result<std::string> Write(const std::optional<std::string>& previous_index_manifest,
                                     const std::vector<IndexManifestEntry>& new_index_entries,
                                     IndexManifestFile* index_manifest_file);

 private:
    class IndexManifestFileCombiner {
     public:
        virtual ~IndexManifestFileCombiner() = default;
        virtual std::vector<IndexManifestEntry> Combine(
            const std::vector<IndexManifestEntry>& prev_index_files,
            const std::vector<IndexManifestEntry>& new_index_files) const = 0;
    };

    /// Combine previous and new global index files by file name.
    class GlobalFileNameCombiner : public IndexManifestFileCombiner {
     public:
        std::vector<IndexManifestEntry> Combine(
            const std::vector<IndexManifestEntry>& prev_index_files,
            const std::vector<IndexManifestEntry>& new_index_files) const override;
    };

    static std::map<std::string, std::vector<IndexManifestEntry>> SeparateIndexEntries(
        const std::vector<IndexManifestEntry>& index_entries);

    static Result<std::unique_ptr<IndexManifestFileCombiner>> GetIndexManifestFileCombine(
        const std::string& index_type);
};
}  // namespace paimon
