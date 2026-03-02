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

#include "fmt/format.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/index/deletion_vector_meta.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/fs/file_system.h"

namespace paimon {

/// Writer to write deletion file.
class DeletionFileWriter {
 public:
    static Result<std::unique_ptr<DeletionFileWriter>> Create(
        const std::shared_ptr<IndexPathFactory>& path_factory,
        const std::shared_ptr<FileSystem>& fs, const std::shared_ptr<MemoryPool>& pool);

    Result<int64_t> GetPos() const {
        return out_->GetPos();
    }

    Status Write(const std::string& key, const std::shared_ptr<DeletionVector>& deletion_vector);

    Status Close() {
        PAIMON_RETURN_NOT_OK(out_->Flush());
        PAIMON_ASSIGN_OR_RAISE(output_bytes_, out_->GetPos());
        return out_->Close();
    }

    Result<std::unique_ptr<IndexFileMeta>> GetResult() const;

 private:
    DeletionFileWriter(const std::string& path, bool is_external_path,
                       std::shared_ptr<OutputStream>& out, const std::shared_ptr<MemoryPool>& pool)
        : path_(path), is_external_path_(is_external_path), out_(std::move(out)), pool_(pool) {}

    std::string path_;
    bool is_external_path_;
    std::shared_ptr<OutputStream> out_;
    std::shared_ptr<MemoryPool> pool_;
    LinkedHashMap<std::string, DeletionVectorMeta> dv_metas_;
    int64_t output_bytes_ = -1;
};

}  // namespace paimon
