/*
 * Copyright 2024-present Alibaba Inc.
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

#include "paimon/write_context.h"

#include <utility>

#include "paimon/common/utils/path_util.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

WriteContext::WriteContext(const std::string& root_path, const std::string& commit_user,
                           bool is_streaming_mode, bool ignore_num_bucket_check,
                           bool ignore_previous_files, const std::optional<int32_t>& write_id,
                           const std::string& branch, const std::vector<std::string>& write_schema,
                           const std::shared_ptr<MemoryPool>& memory_pool,
                           const std::shared_ptr<Executor>& executor,
                           const std::shared_ptr<FileSystem>& specific_file_system,
                           const std::map<std::string, std::string>& fs_scheme_to_identifier_map,

                           const std::map<std::string, std::string>& options)
    : root_path_(root_path),
      commit_user_(commit_user),
      branch_(branch),
      is_streaming_mode_(is_streaming_mode),
      ignore_num_bucket_check_(ignore_num_bucket_check),
      ignore_previous_files_(ignore_previous_files),
      write_id_(write_id),
      write_schema_(write_schema),
      memory_pool_(memory_pool),
      executor_(executor),
      specific_file_system_(specific_file_system),
      fs_scheme_to_identifier_map_(fs_scheme_to_identifier_map),
      options_(options) {}

WriteContext::~WriteContext() = default;

class WriteContextBuilder::Impl {
 public:
    friend class WriteContextBuilder;

    void Reset() {
        write_id_ = std::nullopt;
        is_streaming_mode_ = false;
        ignore_num_bucket_check_ = false;
        ignore_previous_files_ = false;
        memory_pool_ = GetDefaultPool();
        executor_ = CreateDefaultExecutor();
        branch_ = BranchManager::DEFAULT_MAIN_BRANCH;
        write_schema_.clear();
        fs_scheme_to_identifier_map_.clear();
        specific_file_system_.reset();
        options_.clear();
    }

 private:
    std::string root_path_;
    std::string commit_user_;
    std::string branch_ = BranchManager::DEFAULT_MAIN_BRANCH;
    std::optional<int32_t> write_id_;
    bool is_streaming_mode_ = false;
    bool ignore_num_bucket_check_ = false;
    bool ignore_previous_files_ = false;
    std::vector<std::string> write_schema_;
    std::shared_ptr<MemoryPool> memory_pool_ = GetDefaultPool();
    std::shared_ptr<Executor> executor_ = CreateDefaultExecutor();
    std::map<std::string, std::string> fs_scheme_to_identifier_map_;
    std::shared_ptr<FileSystem> specific_file_system_;
    std::map<std::string, std::string> options_;
};

WriteContextBuilder::WriteContextBuilder(const std::string& root_path,
                                         const std::string& commit_user)
    : impl_(std::make_unique<Impl>()) {
    impl_->root_path_ = root_path;
    impl_->commit_user_ = commit_user;
}

WriteContextBuilder::~WriteContextBuilder() = default;

WriteContextBuilder& WriteContextBuilder::AddOption(const std::string& key,
                                                    const std::string& value) {
    impl_->options_[key] = value;
    return *this;
}

WriteContextBuilder& WriteContextBuilder::SetOptions(
    const std::map<std::string, std::string>& opts) {
    impl_->options_ = opts;
    return *this;
}

WriteContextBuilder& WriteContextBuilder::WithStreamingMode(bool is_streaming_mode) {
    impl_->is_streaming_mode_ = is_streaming_mode;
    return *this;
}

WriteContextBuilder& WriteContextBuilder::WithMemoryPool(
    const std::shared_ptr<MemoryPool>& memory_pool) {
    impl_->memory_pool_ = memory_pool;
    return *this;
}

WriteContextBuilder& WriteContextBuilder::WithIgnorePreviousFiles(bool ignore_previous_files) {
    impl_->ignore_previous_files_ = ignore_previous_files;
    return *this;
}

WriteContextBuilder& WriteContextBuilder::WithExecutor(const std::shared_ptr<Executor>& executor) {
    impl_->executor_ = executor;
    return *this;
}

WriteContextBuilder& WriteContextBuilder::WithWriteId(int32_t write_id) {
    impl_->write_id_ = write_id;
    return *this;
}

WriteContextBuilder& WriteContextBuilder::WithBranch(const std::string& branch) {
    impl_->branch_ = branch;
    return *this;
}

WriteContextBuilder& WriteContextBuilder::WithWriteSchema(
    const std::vector<std::string>& write_schema) {
    impl_->write_schema_ = write_schema;
    return *this;
}

WriteContextBuilder& WriteContextBuilder::WithFileSystemSchemeToIdentifierMap(
    const std::map<std::string, std::string>& fs_scheme_to_identifier_map) {
    impl_->fs_scheme_to_identifier_map_ = fs_scheme_to_identifier_map;
    return *this;
}

WriteContextBuilder& WriteContextBuilder::WithFileSystem(
    const std::shared_ptr<FileSystem>& file_system) {
    impl_->specific_file_system_ = file_system;
    return *this;
}

Result<std::unique_ptr<WriteContext>> WriteContextBuilder::Finish() {
    PAIMON_ASSIGN_OR_RAISE(impl_->root_path_, PathUtil::NormalizePath(impl_->root_path_));
    if (impl_->root_path_.empty()) {
        return Status::Invalid("root path is empty");
    }
    auto ctx = std::make_unique<WriteContext>(
        impl_->root_path_, impl_->commit_user_, impl_->is_streaming_mode_,
        impl_->ignore_num_bucket_check_, impl_->ignore_previous_files_, impl_->write_id_,
        impl_->branch_, impl_->write_schema_, impl_->memory_pool_, impl_->executor_,
        impl_->specific_file_system_, impl_->fs_scheme_to_identifier_map_, impl_->options_);
    impl_->Reset();
    return ctx;
}

}  // namespace paimon
