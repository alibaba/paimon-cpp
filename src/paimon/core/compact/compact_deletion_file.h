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
#include <optional>

#include "paimon/core/deletionvectors/bucketed_dv_maintainer.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/result.h"

namespace paimon {

class CompactDeletionFile {
 public:
    virtual ~CompactDeletionFile() = default;

    static Result<std::shared_ptr<CompactDeletionFile>> GenerateFiles(
        const std::shared_ptr<BucketedDvMaintainer>& maintainer);

    virtual std::optional<std::shared_ptr<IndexFileMeta>> GetOrCompute() = 0;

    virtual Result<std::shared_ptr<CompactDeletionFile>> MergeOldFile(
        const std::shared_ptr<CompactDeletionFile>& old) = 0;

    virtual void Clean() = 0;
};

class GeneratedDeletionFile : public CompactDeletionFile,
                              public std::enable_shared_from_this<GeneratedDeletionFile> {
 public:
    GeneratedDeletionFile(const std::shared_ptr<IndexFileMeta>& deletion_file,
                          const std::shared_ptr<DeletionVectorsIndexFile>& dv_index_file)
        : deletion_file_(deletion_file), dv_index_file_(dv_index_file) {}

    std::optional<std::shared_ptr<IndexFileMeta>> GetOrCompute() override {
        get_invoked_ = true;
        return deletion_file_ ? std::optional<std::shared_ptr<IndexFileMeta>>(deletion_file_)
                              : std::nullopt;
    }

    Result<std::shared_ptr<CompactDeletionFile>> MergeOldFile(
        const std::shared_ptr<CompactDeletionFile>& old) override {
        auto derived = dynamic_cast<GeneratedDeletionFile*>(old.get());
        if (derived == nullptr) {
            return Status::Invalid("old should be a GeneratedDeletionFile, but it is not");
        }
        if (derived->get_invoked_) {
            return Status::Invalid("old should not be get, this is a bug.");
        }
        if (deletion_file_ == nullptr) {
            return old;
        }
        old->Clean();
        return shared_from_this();
    }

    void Clean() override {
        if (deletion_file_ != nullptr) {
            dv_index_file_->Delete(deletion_file_);
        }
    }

 private:
    std::shared_ptr<IndexFileMeta> deletion_file_;
    std::shared_ptr<DeletionVectorsIndexFile> dv_index_file_;
    bool get_invoked_ = false;
};

inline Result<std::shared_ptr<CompactDeletionFile>> CompactDeletionFile::GenerateFiles(
    const std::shared_ptr<BucketedDvMaintainer>& maintainer) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<IndexFileMeta>> file,
                           maintainer->WriteDeletionVectorsIndex());
    return std::make_shared<GeneratedDeletionFile>(file.value_or(nullptr),
                                                   maintainer->DvIndexFile());
}

}  // namespace paimon
