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

#include "paimon/core/deletionvectors/deletion_file_writer.h"

#include "paimon/common/io/data_output_stream.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"

namespace paimon {

Result<std::unique_ptr<DeletionFileWriter>> DeletionFileWriter::Create(
    const std::shared_ptr<IndexPathFactory>& path_factory, const std::shared_ptr<FileSystem>& fs,
    const std::shared_ptr<MemoryPool>& pool) {
    std::string path = path_factory->NewPath();
    bool is_external_path = path_factory->IsExternalPath();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> out, fs->Create(path, /*overwrite=*/true));
    DataOutputStream output_stream(out);
    PAIMON_RETURN_NOT_OK(output_stream.WriteValue<int8_t>(DeletionVectorsIndexFile::VERSION_ID_V1));
    return std::unique_ptr<DeletionFileWriter>(
        new DeletionFileWriter(path, is_external_path, out, pool));
}

Status DeletionFileWriter::Write(const std::string& key,
                                 const std::shared_ptr<DeletionVector>& deletion_vector) {
    PAIMON_ASSIGN_OR_RAISE(int64_t start, out_->GetPos());
    if (start < 0 || start > std::numeric_limits<int32_t>::max()) {
        return Status::Invalid(fmt::format("Output position {} out of int32 range.", start));
    }
    DataOutputStream output_stream(out_);
    PAIMON_ASSIGN_OR_RAISE(int32_t length, deletion_vector->SerializeTo(pool_, &output_stream));
    dv_metas_.insert(key, DeletionVectorMeta(key, static_cast<int32_t>(start), length,
                                             deletion_vector->GetCardinality()));
    return Status::OK();
}

Result<std::unique_ptr<IndexFileMeta>> DeletionFileWriter::GetResult() const {
    int64_t length = output_bytes_;
    if (length < 0 || length > std::numeric_limits<int32_t>::max()) {
        return Status::Invalid(
            fmt::format("Deletion file result length {} out of int32 range.", length));
    }
    std::optional<std::string> final_path;
    if (is_external_path_) {
        PAIMON_ASSIGN_OR_RAISE(Path external_path, PathUtil::ToPath(path_));
        final_path = external_path.ToString();
    }
    return std::make_unique<IndexFileMeta>(DeletionVectorsIndexFile::DELETION_VECTORS_INDEX,
                                           PathUtil::GetName(path_), length, dv_metas_.size(),
                                           dv_metas_, final_path);
}

}  // namespace paimon
