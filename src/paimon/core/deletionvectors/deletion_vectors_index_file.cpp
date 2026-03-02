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

#include "paimon/core/deletionvectors/deletion_vector_index_file_writer.h"

namespace paimon {

Result<std::shared_ptr<IndexFileMeta>> DeletionVectorsIndexFile::WriteSingleFile(
    const std::map<std::string, std::shared_ptr<DeletionVector>>& input) {
    return CreateWriter()->WriteSingleFile(input);
}

std::shared_ptr<DeletionVectorIndexFileWriter> DeletionVectorsIndexFile::CreateWriter() const {
    return std::make_shared<DeletionVectorIndexFileWriter>(fs_, path_factory_,
                                                           target_size_per_index_file_, pool_);
}

}  // namespace paimon
