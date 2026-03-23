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

/// Deletion File from compaction.
class CompactDeletionFile {
 public:
    virtual ~CompactDeletionFile() = default;

    /// Used by async compaction, when compaction task is completed, deletions file will be
    /// generated immediately, so when updateCompactResult, we need to merge old deletion files
    /// (just delete them).
    static Result<std::shared_ptr<CompactDeletionFile>> GenerateFiles(
        const std::shared_ptr<BucketedDvMaintainer>& maintainer);

    /// For sync compaction, only create deletion files when prepare commit.
    static Result<std::shared_ptr<CompactDeletionFile>> LazyGeneration(
        const std::shared_ptr<BucketedDvMaintainer>& maintainer);

    virtual Result<std::optional<std::shared_ptr<IndexFileMeta>>> GetOrCompute() = 0;

    virtual Result<std::shared_ptr<CompactDeletionFile>> MergeOldFile(
        const std::shared_ptr<CompactDeletionFile>& old) = 0;

    virtual void Clean() = 0;
};

/// A generated files implementation of `CompactDeletionFile`.
class GeneratedDeletionFile : public CompactDeletionFile,
                              public std::enable_shared_from_this<GeneratedDeletionFile> {
 public:
    GeneratedDeletionFile(const std::shared_ptr<IndexFileMeta>& deletion_file,
                          const std::shared_ptr<DeletionVectorsIndexFile>& dv_index_file)
        : deletion_file_(deletion_file), dv_index_file_(dv_index_file) {}

    Result<std::optional<std::shared_ptr<IndexFileMeta>>> GetOrCompute() override {
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

/// A lazy generation implementation of `CompactDeletionFile`.
class LazyCompactDeletionFile : public CompactDeletionFile,
                                public std::enable_shared_from_this<LazyCompactDeletionFile> {
 public:
    explicit LazyCompactDeletionFile(const std::shared_ptr<BucketedDvMaintainer>& maintainer)
        : maintainer_(maintainer) {}

    Result<std::optional<std::shared_ptr<IndexFileMeta>>> GetOrCompute() override {
        generated_ = true;
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<CompactDeletionFile> generated,
                               CompactDeletionFile::GenerateFiles(maintainer_));
        return generated->GetOrCompute();
    }

    Result<std::shared_ptr<CompactDeletionFile>> MergeOldFile(
        const std::shared_ptr<CompactDeletionFile>& old) override {
        auto derived = dynamic_cast<LazyCompactDeletionFile*>(old.get());
        if (derived == nullptr) {
            return Status::Invalid("old should be a LazyCompactDeletionFile, but it is not");
        }
        if (derived->generated_) {
            return Status::Invalid("old should not be generated, this is a bug.");
        }
        return shared_from_this();
    }

    void Clean() override {}

 private:
    std::shared_ptr<BucketedDvMaintainer> maintainer_;
    bool generated_ = false;
};

inline Result<std::shared_ptr<CompactDeletionFile>> CompactDeletionFile::GenerateFiles(
    const std::shared_ptr<BucketedDvMaintainer>& maintainer) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<IndexFileMeta>> file,
                           maintainer->WriteDeletionVectorsIndex());
    return std::make_shared<GeneratedDeletionFile>(file.value_or(nullptr),
                                                   maintainer->DvIndexFile());
}

inline Result<std::shared_ptr<CompactDeletionFile>> CompactDeletionFile::LazyGeneration(
    const std::shared_ptr<BucketedDvMaintainer>& maintainer) {
    return std::make_shared<LazyCompactDeletionFile>(maintainer);
}

}  // namespace paimon
