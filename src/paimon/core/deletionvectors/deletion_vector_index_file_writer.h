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

#include <map>
#include <memory>

#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"

namespace paimon {

/// Writer for deletion vector index file.
class DeletionVectorIndexFileWriter {
 public:
    DeletionVectorIndexFileWriter(const std::shared_ptr<FileSystem>& fs,
                                  const std::shared_ptr<IndexPathFactory>& path_factory,
                                  const std::shared_ptr<MemoryPool>& pool)
        : index_path_factory_(path_factory), fs_(fs), pool_(pool) {}

    /// The deletion file of the bucketed table is updated according to the bucket. If a compaction
    /// occurs and there is no longer a deletion file, an empty deletion file needs to be generated
    /// to overwrite the old file.
    /// TODO(yonghao.fyh): We can consider sending a message to delete the deletion file in the
    /// future.
    Result<std::shared_ptr<IndexFileMeta>> WriteSingleFile(
        const std::map<std::string, std::shared_ptr<DeletionVector>>& input);

 private:
    std::shared_ptr<IndexPathFactory> index_path_factory_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon
