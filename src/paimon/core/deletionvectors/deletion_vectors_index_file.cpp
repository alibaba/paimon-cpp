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

#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/core/deletionvectors/deletion_vector_index_file_writer.h"

namespace paimon {

Result<std::shared_ptr<IndexFileMeta>> DeletionVectorsIndexFile::WriteSingleFile(
    const std::map<std::string, std::shared_ptr<DeletionVector>>& input) {
    return CreateWriter()->WriteSingleFile(input);
}

std::shared_ptr<DeletionVectorIndexFileWriter> DeletionVectorsIndexFile::CreateWriter() const {
    return std::make_shared<DeletionVectorIndexFileWriter>(fs_, path_factory_, pool_);
}

Result<std::map<std::string, std::shared_ptr<DeletionVector>>>
DeletionVectorsIndexFile::ReadAllDeletionVectors(
    const std::shared_ptr<IndexFileMeta>& file_meta) const {
    std::optional<LinkedHashMap<std::string, DeletionVectorMeta>> deletion_vector_metas =
        file_meta->DvRanges();
    if (deletion_vector_metas == std::nullopt) {
        return Status::Invalid(
            fmt::format("Read all deletion vectors failed from IndexFileMeta '{}'. Deletion vector "
                        "metas is null",
                        file_meta->FileName()));
    }

    std::map<std::string, std::shared_ptr<DeletionVector>> deletion_vectors;
    std::string file_path = path_factory_->ToPath(file_meta);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input_stream, fs_->Open(file_path));
    auto data_input_stream = std::make_shared<DataInputStream>(input_stream);
    PAIMON_RETURN_NOT_OK(CheckVersion(data_input_stream));
    for (const auto& [_, deletion_vector_meta] : deletion_vector_metas.value()) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<DeletionVector> dv,
            DeletionVector::Read(data_input_stream.get(),
                                 static_cast<int64_t>(deletion_vector_meta.GetLength()),
                                 pool_.get()));
        deletion_vectors[deletion_vector_meta.GetDataFileName()] = dv;
    }
    return deletion_vectors;
}

Result<std::map<std::string, std::shared_ptr<DeletionVector>>>
DeletionVectorsIndexFile::ReadAllDeletionVectors(
    const std::vector<std::shared_ptr<IndexFileMeta>>& index_files) const {
    std::map<std::string, std::shared_ptr<DeletionVector>> deletion_vectors;
    for (const auto& index_file : index_files) {
        std::map<std::string, std::shared_ptr<DeletionVector>> partial_deletion_vectors;
        PAIMON_ASSIGN_OR_RAISE(partial_deletion_vectors, ReadAllDeletionVectors(index_file));
        for (const auto& [data_file_name, dv] : partial_deletion_vectors) {
            deletion_vectors[data_file_name] = dv;
        }
    }
    return deletion_vectors;
}

Status DeletionVectorsIndexFile::CheckVersion(const std::shared_ptr<DataInputStream>& in) {
    PAIMON_ASSIGN_OR_RAISE(int8_t version, in->ReadValue<int8_t>());
    if (version != VERSION_ID_V1) {
        return Status::Invalid(fmt::format(
            "Version not match, actual version: {}, expected version: {}", version, VERSION_ID_V1));
    }
    return Status::OK();
}

}  // namespace paimon
