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

#include "paimon/common/io/data_output_stream.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/index/deletion_vector_meta.h"
#include "paimon/fs/file_system.h"

namespace paimon {

/// Writer to write deletion file.
class DeletionFileWriter {
 public:
    static Result<std::unique_ptr<DeletionFileWriter>> Create(
        const std::shared_ptr<IndexPathFactory>& path_factory,
        const std::shared_ptr<FileSystem>& fs, const std::shared_ptr<MemoryPool>& pool) {
        std::string path = path_factory->NewPath();
        bool is_external_path = path_factory->IsExternalPath();
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> out,
                               fs->Create(path, /*overwrite=*/true));
        DataOutputStream output_stream(out);
        PAIMON_RETURN_NOT_OK(
            output_stream.WriteValue<int8_t>(DeletionVectorsIndexFile::VERSION_ID_V1));
        return std::unique_ptr<DeletionFileWriter>(
            new DeletionFileWriter(path, is_external_path, out, pool));
    }

    Result<int64_t> GetPos() const {
        return out_->GetPos();
    }

    Status Write(const std::string& key, const std::shared_ptr<DeletionVector>& deletion_vector) {
        PAIMON_ASSIGN_OR_RAISE(int32_t start, out_->GetPos());
        DataOutputStream output_stream(out_);
        PAIMON_ASSIGN_OR_RAISE(int32_t length, deletion_vector->SerializeTo(pool_, &output_stream));
        dv_metas_.insert(key,
                         DeletionVectorMeta(key, start, length, deletion_vector->GetCardinality()));
        return Status::OK();
    }

    Status Close() {
        return out_->Close();
    }

    Result<std::unique_ptr<IndexFileMeta>> GetResult() const {
        PAIMON_ASSIGN_OR_RAISE(int32_t length, GetPos());
        return std::make_unique<IndexFileMeta>(
            DeletionVectorsIndexFile::DELETION_VECTORS_INDEX, PathUtil::GetName(path_), length,
            dv_metas_.size(), dv_metas_,
            is_external_path_ ? std::optional<std::string>(path_) : std::optional<std::string>());
    }

 private:
    DeletionFileWriter(const std::string& path, bool is_external_path,
                       std::shared_ptr<OutputStream>& out, const std::shared_ptr<MemoryPool>& pool)
        : path_(path), is_external_path_(is_external_path), out_(std::move(out)), pool_(pool) {}

    std::string path_;
    bool is_external_path_;
    std::shared_ptr<OutputStream> out_;
    std::shared_ptr<MemoryPool> pool_;
    LinkedHashMap<std::string, DeletionVectorMeta> dv_metas_;
};

}  // namespace paimon
