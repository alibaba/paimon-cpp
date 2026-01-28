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

#include "paimon/commit_context.h"

#include <utility>

#include "paimon/common/utils/path_util.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

CommitContext::CommitContext(const std::string& root_path, const std::string& commit_user,
                             bool ignore_empty_commit, bool use_rest_catalog_commit,
                             const std::shared_ptr<MemoryPool>& memory_pool,
                             const std::shared_ptr<Executor>& executor,
                             const std::shared_ptr<FileSystem>& specific_file_system,
                             const std::map<std::string, std::string>& options)
    : root_path_(root_path),
      commit_user_(commit_user),
      ignore_empty_commit_(ignore_empty_commit),
      use_rest_catalog_commit_(use_rest_catalog_commit),
      memory_pool_(memory_pool),
      executor_(executor),
      specific_file_system_(specific_file_system),
      options_(options) {}

CommitContext::~CommitContext() = default;

class CommitContextBuilder::Impl {
 public:
    friend class CommitContextBuilder;

    void Reset() {
        ignore_empty_commit_ = true;
        use_rest_catalog_commit_ = false;
        memory_pool_ = GetDefaultPool();
        executor_ = CreateDefaultExecutor();
        specific_file_system_.reset();
        options_.clear();
    }

 private:
    std::string root_path_;
    std::string commit_user_;
    bool ignore_empty_commit_ = true;
    bool use_rest_catalog_commit_ = false;
    std::shared_ptr<MemoryPool> memory_pool_ = GetDefaultPool();
    std::shared_ptr<Executor> executor_ = CreateDefaultExecutor();
    std::shared_ptr<FileSystem> specific_file_system_;
    std::map<std::string, std::string> options_;
};

CommitContextBuilder::CommitContextBuilder(const std::string& root_path,
                                           const std::string& commit_user)
    : impl_(std::make_unique<Impl>()) {
    impl_->root_path_ = root_path;
    impl_->commit_user_ = commit_user;
}

CommitContextBuilder::~CommitContextBuilder() = default;

CommitContextBuilder& CommitContextBuilder::AddOption(const std::string& key,
                                                      const std::string& value) {
    impl_->options_[key] = value;
    return *this;
}

CommitContextBuilder& CommitContextBuilder::SetOptions(
    const std::map<std::string, std::string>& opts) {
    impl_->options_ = opts;
    return *this;
}

CommitContextBuilder& CommitContextBuilder::IgnoreEmptyCommit(bool ignore_empty_commit) {
    impl_->ignore_empty_commit_ = ignore_empty_commit;
    return *this;
}

CommitContextBuilder& CommitContextBuilder::UseRESTCatalogCommit(bool use_rest_catalog_commit) {
    impl_->use_rest_catalog_commit_ = use_rest_catalog_commit;
    return *this;
}

CommitContextBuilder& CommitContextBuilder::WithMemoryPool(
    const std::shared_ptr<MemoryPool>& memory_pool) {
    impl_->memory_pool_ = memory_pool;
    return *this;
}

CommitContextBuilder& CommitContextBuilder::WithExecutor(
    const std::shared_ptr<Executor>& executor) {
    impl_->executor_ = executor;
    return *this;
}

CommitContextBuilder& CommitContextBuilder::WithFileSystem(
    const std::shared_ptr<FileSystem>& file_system) {
    impl_->specific_file_system_ = file_system;
    return *this;
}

Result<std::unique_ptr<CommitContext>> CommitContextBuilder::Finish() {
    PAIMON_ASSIGN_OR_RAISE(impl_->root_path_, PathUtil::NormalizePath(impl_->root_path_));
    if (impl_->root_path_.empty()) {
        return Status::Invalid("root path is empty");
    }
    auto ctx = std::make_unique<CommitContext>(
        impl_->root_path_, impl_->commit_user_, impl_->ignore_empty_commit_,
        impl_->use_rest_catalog_commit_, impl_->memory_pool_, impl_->executor_,
        impl_->specific_file_system_, impl_->options_);
    impl_->Reset();
    return ctx;
}

}  // namespace paimon
