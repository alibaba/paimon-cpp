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
#include <string>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/result.h"

namespace paimon {

class AppendOnlyFileStoreWrite;
class CommitMessage;
class CoreOptions;

/// Compaction task for append-only unaware-bucket tables.
///
/// This task holds the partition and the list of files to compact (compact_before),
/// performs the actual compaction via `AppendOnlyFileStoreWrite::CompactRewrite`,
/// and produces a `CommitMessage` containing the `CompactIncrement`.
class AppendCompactTask {
 public:
    AppendCompactTask(const BinaryRow& partition,
                      const std::vector<std::shared_ptr<DataFileMeta>>& files);

    ~AppendCompactTask() = default;

    const BinaryRow& Partition() const {
        return partition_;
    }

    const std::vector<std::shared_ptr<DataFileMeta>>& CompactBefore() const {
        return compact_before_;
    }

    const std::vector<std::shared_ptr<DataFileMeta>>& CompactAfter() const {
        return compact_after_;
    }

    Result<std::shared_ptr<CommitMessage>> DoCompact(const CoreOptions& options,
                                                     AppendOnlyFileStoreWrite* write);

    std::string ToString() const;

 private:
    BinaryRow partition_;
    std::vector<std::shared_ptr<DataFileMeta>> compact_before_;
    std::vector<std::shared_ptr<DataFileMeta>> compact_after_;
};

}  // namespace paimon
