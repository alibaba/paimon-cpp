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

#include "paimon/scan_context.h"

#include <utility>

#include "paimon/common/utils/path_util.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"

namespace paimon {
class Predicate;

ScanContext::ScanContext(const std::string& path, bool is_streaming_mode,
                         std::optional<int32_t> limit,
                         const std::shared_ptr<ScanFilter>& scan_filter,
                         const std::shared_ptr<GlobalIndexResult>& global_index_result,
                         const std::shared_ptr<MemoryPool>& memory_pool,
                         const std::shared_ptr<Executor>& executor,
                         const std::shared_ptr<FileSystem>& specific_file_system,
                         const std::map<std::string, std::string>& options)
    : path_(path),
      is_streaming_mode_(is_streaming_mode),
      limit_(limit),
      scan_filters_(scan_filter),
      global_index_result_(global_index_result),
      memory_pool_(memory_pool),
      executor_(executor),
      specific_file_system_(specific_file_system),
      options_(options) {}

ScanContext::~ScanContext() = default;

class ScanContextBuilder::Impl {
 public:
    friend class ScanContextBuilder;

    void Reset() {
        is_streaming_mode_ = false;
        limit_ = std::nullopt;
        bucket_filter_ = std::nullopt;
        partition_filters_.clear();
        predicates_.reset();
        vector_search_.reset();
        global_index_result_.reset();
        memory_pool_ = GetDefaultPool();
        executor_ = CreateDefaultExecutor();
        specific_file_system_.reset();
        options_.clear();
    }

 private:
    std::string path_;
    bool is_streaming_mode_ = false;
    std::optional<int32_t> limit_;
    std::optional<int32_t> bucket_filter_;
    std::vector<std::map<std::string, std::string>> partition_filters_;
    std::shared_ptr<Predicate> predicates_;
    std::shared_ptr<VectorSearch> vector_search_;
    std::shared_ptr<GlobalIndexResult> global_index_result_;
    std::shared_ptr<MemoryPool> memory_pool_ = GetDefaultPool();
    std::shared_ptr<Executor> executor_ = CreateDefaultExecutor();
    std::shared_ptr<FileSystem> specific_file_system_;
    std::map<std::string, std::string> options_;
};

ScanContextBuilder::ScanContextBuilder(const std::string& path)
    : impl_(std::make_unique<ScanContextBuilder::Impl>()) {
    impl_->path_ = path;
}
ScanContextBuilder::~ScanContextBuilder() = default;
ScanContextBuilder& ScanContextBuilder::WithStreamingMode(bool is_streaming_mode) {
    impl_->is_streaming_mode_ = is_streaming_mode;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::SetLimit(int32_t limit) {
    impl_->limit_ = limit;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::SetBucketFilter(int32_t bucket_filter) {
    impl_->bucket_filter_ = bucket_filter;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::SetPartitionFilter(
    const std::vector<std::map<std::string, std::string>>& partition_filters) {
    impl_->partition_filters_ = partition_filters;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::SetPredicate(const std::shared_ptr<Predicate>& predicate) {
    impl_->predicates_ = predicate;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::SetVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search) {
    impl_->vector_search_ = vector_search;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::SetGlobalIndexResult(
    const std::shared_ptr<GlobalIndexResult>& global_index_result) {
    impl_->global_index_result_ = global_index_result;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::SetOptions(
    const std::map<std::string, std::string>& options) {
    impl_->options_ = options;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::AddOption(const std::string& key,
                                                  const std::string& value) {
    impl_->options_[key] = value;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::WithMemoryPool(
    const std::shared_ptr<MemoryPool>& memory_pool) {
    impl_->memory_pool_ = memory_pool;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::WithExecutor(const std::shared_ptr<Executor>& executor) {
    impl_->executor_ = executor;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::WithFileSystem(
    const std::shared_ptr<FileSystem>& file_system) {
    impl_->specific_file_system_ = file_system;
    return *this;
}

Result<std::unique_ptr<ScanContext>> ScanContextBuilder::Finish() {
    PAIMON_ASSIGN_OR_RAISE(impl_->path_, PathUtil::NormalizePath(impl_->path_));
    if (impl_->path_.empty()) {
        return Status::Invalid("cannot scan with empty table path");
    }
    auto ctx = std::make_unique<ScanContext>(
        impl_->path_, impl_->is_streaming_mode_, impl_->limit_,
        std::make_shared<ScanFilter>(impl_->predicates_, impl_->partition_filters_,
                                     impl_->bucket_filter_, impl_->vector_search_),
        impl_->global_index_result_, impl_->memory_pool_, impl_->executor_,
        impl_->specific_file_system_, impl_->options_);
    impl_->Reset();
    return ctx;
}

}  // namespace paimon
