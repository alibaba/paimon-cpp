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

#include <memory>
#include <vector>

#include "paimon/core/io/data_file_meta.h"
#include "paimon/status.h"

namespace paimon {
class CompactDeletionFile;

/// Result of compaction.
class CompactResult {
 public:
    CompactResult() = default;
    CompactResult(const std::vector<std::shared_ptr<DataFileMeta>>& before,
                  const std::vector<std::shared_ptr<DataFileMeta>>& after)
        : CompactResult(before, after, {}) {}

    CompactResult(const std::vector<std::shared_ptr<DataFileMeta>>& before,
                  const std::vector<std::shared_ptr<DataFileMeta>>& after,
                  const std::vector<std::shared_ptr<DataFileMeta>>& changelog)
        : before_(before), after_(after), changelog_(changelog) {}

    const std::vector<std::shared_ptr<DataFileMeta>>& Before() const {
        return before_;
    }

    const std::vector<std::shared_ptr<DataFileMeta>>& After() const {
        return after_;
    }

    const std::vector<std::shared_ptr<DataFileMeta>>& Changelog() const {
        return changelog_;
    }

    std::shared_ptr<CompactDeletionFile> DeletionFile() const {
        return deletion_file_;
    }

    void SetDeletionFile(const std::shared_ptr<CompactDeletionFile>& deletion_file) {
        deletion_file_ = deletion_file;
    }

    Status Merge(const CompactResult& other) {
        before_.insert(before_.end(), other.Before().begin(), other.Before().end());
        after_.insert(after_.end(), other.After().begin(), other.After().end());
        changelog_.insert(changelog_.end(), other.Changelog().begin(), other.Changelog().end());

        if (deletion_file_ != nullptr || other.deletion_file_ != nullptr) {
            return Status::NotImplemented(
                "There is a bug, deletion file can't be set before merge.");
        }
        return Status::OK();
    }

 private:
    std::vector<std::shared_ptr<DataFileMeta>> before_;
    std::vector<std::shared_ptr<DataFileMeta>> after_;
    std::vector<std::shared_ptr<DataFileMeta>> changelog_;
    std::shared_ptr<CompactDeletionFile> deletion_file_;
};

}  // namespace paimon
