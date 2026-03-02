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

#include "paimon/core/mergetree/compact/compact_rewriter.h"

namespace paimon {

/// Common implementation of `CompactRewriter`.
class AbstractCompactRewriter : public CompactRewriter {
 public:
    Result<CompactResult> Upgrade(int32_t output_level,
                                  const std::shared_ptr<DataFileMeta>& file) const override {
        PAIMON_ASSIGN_OR_RAISE(auto upgraded_file, file->Upgrade(output_level));
        return CompactResult({file}, {upgraded_file});
    }

    virtual Status Close() {
        return Status::OK();
    }

 protected:
    static std::vector<std::shared_ptr<DataFileMeta>> ExtractFilesFromSections(
        const std::vector<std::vector<SortedRun>>& sections) {
        std::vector<std::shared_ptr<DataFileMeta>> files;
        for (const auto& section : sections) {
            for (const auto& sorted_run : section) {
                auto files_in_run = sorted_run.Files();
                files.insert(files.end(), files_in_run.begin(), files_in_run.end());
            }
        }
        return files;
    }
};

}  // namespace paimon
