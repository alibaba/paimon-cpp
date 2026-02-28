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

#include "paimon/core/deletionvectors/deletion_vector_index_file_writer.h"

#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/deletionvectors/deletion_file_writer.h"

namespace paimon {

Result<std::shared_ptr<IndexFileMeta>> DeletionVectorIndexFileWriter::WriteSingleFile(
    const std::map<std::string, std::shared_ptr<DeletionVector>>& input) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<DeletionFileWriter> writer,
                           DeletionFileWriter::Create(index_path_factory_, fs_, pool_));
    ScopeGuard guard([&]() {
        if (writer) {
            (void)writer->Close();
        }
    });
    for (const auto& [key, value] : input) {
        PAIMON_RETURN_NOT_OK(writer->Write(key, value));
    }
    guard.Release();
    PAIMON_RETURN_NOT_OK(writer->Close());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexFileMeta> result, writer->GetResult());
    return result;
}

}  // namespace paimon
