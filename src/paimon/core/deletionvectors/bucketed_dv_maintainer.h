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
#include <optional>
#include <string>

#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/index/index_file_meta.h"

namespace paimon {

// Maintainer of deletionVectors index.
class BucketedDvMaintainer {
 public:
    BucketedDvMaintainer(
        const std::shared_ptr<DeletionVectorsIndexFile>& dv_index_file,
        const std::map<std::string, std::shared_ptr<DeletionVector>>& deletion_vectors)
        : dv_index_file_(dv_index_file),
          deletion_vectors_(deletion_vectors),
          bitmap64_(dv_index_file->Bitmap64()) {}

    std::optional<std::shared_ptr<DeletionVector>> DeletionVectorOf(
        const std::string& file_name) const {
        if (auto it = deletion_vectors_.find(file_name); it != deletion_vectors_.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    void RemoveDeletionVectorOf(const std::string& file_name) {
        if (deletion_vectors_.erase(file_name) > 0) {
            modified_ = true;
        }
    }

    Result<std::optional<std::shared_ptr<IndexFileMeta>>> WriteDeletionVectorsIndex() {
        if (modified_) {
            modified_ = false;
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexFileMeta> result,
                                   dv_index_file_->WriteSingleFile(deletion_vectors_));
            return std::make_optional<std::shared_ptr<IndexFileMeta>>(result);
        }
        return std::optional<std::shared_ptr<IndexFileMeta>>();
    }

    std::shared_ptr<DeletionVectorsIndexFile> DvIndexFile() const {
        return dv_index_file_;
    }

 private:
    std::shared_ptr<DeletionVectorsIndexFile> dv_index_file_;
    std::map<std::string, std::shared_ptr<DeletionVector>> deletion_vectors_;
    bool bitmap64_;
    bool modified_ = false;
};

}  // namespace paimon
