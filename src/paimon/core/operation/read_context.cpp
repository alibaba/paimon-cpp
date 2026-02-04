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

#include "paimon/read_context.h"

#include <utility>

#include "paimon/common/utils/path_util.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"

namespace paimon {
class Predicate;

ReadContext::ReadContext(
    const std::string& path, const std::string& branch, const std::vector<std::string>& read_schema,
    const std::vector<int32_t>& read_field_ids, const std::shared_ptr<Predicate>& predicate,
    bool enable_predicate_filter, bool enable_prefetch, uint32_t prefetch_batch_count,
    uint32_t prefetch_max_parallel_num, bool enable_multi_thread_row_to_batch,
    uint32_t row_to_batch_thread_number, const std::optional<std::string>& table_schema,
    const std::shared_ptr<MemoryPool>& memory_pool, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<FileSystem>& specific_file_system,
    const std::map<std::string, std::string>& fs_scheme_to_identifier_map,
    const std::map<std::string, std::string>& options, PrefetchCacheMode prefetch_cache_mode,
    const CacheConfig& cache_config)
    : path_(path),
      branch_(branch),
      read_schema_(read_schema),
      read_field_ids_(read_field_ids),
      predicate_(predicate),
      enable_predicate_filter_(enable_predicate_filter),
      enable_prefetch_(enable_prefetch),
      prefetch_batch_count_(prefetch_batch_count),
      prefetch_max_parallel_num_(prefetch_max_parallel_num),
      enable_multi_thread_row_to_batch_(enable_multi_thread_row_to_batch),
      row_to_batch_thread_number_(row_to_batch_thread_number),
      table_schema_(table_schema),
      memory_pool_(memory_pool),
      executor_(executor),
      specific_file_system_(specific_file_system),
      fs_scheme_to_identifier_map_(fs_scheme_to_identifier_map),
      options_(options),
      prefetch_cache_mode_(prefetch_cache_mode),
      cache_config_(cache_config) {}

ReadContext::~ReadContext() = default;

class ReadContextBuilder::Impl {
 public:
    friend class ReadContextBuilder;
    void Reset() {
        branch_ = BranchManager::DEFAULT_MAIN_BRANCH;
        read_field_names_.clear();
        read_field_ids_.clear();
        fs_scheme_to_identifier_map_.clear();
        options_.clear();
        predicate_.reset();
        enable_predicate_filter_ = false;
        enable_prefetch_ = false;
        prefetch_cache_mode_ = PrefetchCacheMode::ALWAYS;
        prefetch_batch_count_ = 600;
        prefetch_max_parallel_num_ = 3;
        enable_multi_thread_row_to_batch_ = false;
        row_to_batch_thread_number_ = 1;
        table_schema_ = std::nullopt;
        memory_pool_ = GetDefaultPool();
        executor_.reset();
        specific_file_system_.reset();
        cache_config_ = CacheConfig();
    }

 private:
    std::string path_;
    std::string branch_ = BranchManager::DEFAULT_MAIN_BRANCH;
    std::vector<std::string> read_field_names_;
    std::vector<int32_t> read_field_ids_;
    std::map<std::string, std::string> fs_scheme_to_identifier_map_;
    std::map<std::string, std::string> options_;
    std::shared_ptr<Predicate> predicate_;
    bool enable_predicate_filter_ = false;
    bool enable_prefetch_ = false;
    uint32_t prefetch_batch_count_ = 600;
    uint32_t prefetch_max_parallel_num_ = 3;
    bool enable_multi_thread_row_to_batch_ = false;
    uint32_t row_to_batch_thread_number_ = 1;
    std::optional<std::string> table_schema_;
    std::shared_ptr<MemoryPool> memory_pool_ = GetDefaultPool();
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<FileSystem> specific_file_system_;
    PrefetchCacheMode prefetch_cache_mode_ = PrefetchCacheMode::ALWAYS;
    CacheConfig cache_config_;
};

ReadContextBuilder::ReadContextBuilder(const std::string& path)
    : impl_(std::make_unique<ReadContextBuilder::Impl>()) {
    impl_->path_ = path;
}

ReadContextBuilder::~ReadContextBuilder() = default;

ReadContextBuilder::ReadContextBuilder(ReadContextBuilder&&) noexcept = default;
ReadContextBuilder& ReadContextBuilder::operator=(ReadContextBuilder&&) noexcept = default;

ReadContextBuilder& ReadContextBuilder::AddOption(const std::string& key,
                                                  const std::string& value) {
    impl_->options_[key] = value;
    return *this;
}

ReadContextBuilder& ReadContextBuilder::SetOptions(const std::map<std::string, std::string>& opts) {
    impl_->options_ = opts;
    return *this;
}

ReadContextBuilder& ReadContextBuilder::SetReadSchema(
    const std::vector<std::string>& read_field_names) {
    impl_->read_field_names_ = read_field_names;
    return *this;
}

ReadContextBuilder& ReadContextBuilder::SetReadFieldIds(
    const std::vector<int32_t>& read_field_ids) {
    impl_->read_field_ids_ = read_field_ids;
    return *this;
}

ReadContextBuilder& ReadContextBuilder::SetPredicate(const std::shared_ptr<Predicate>& predicate) {
    impl_->predicate_ = predicate;
    return *this;
}

ReadContextBuilder& ReadContextBuilder::EnablePredicateFilter(bool enabled) {
    impl_->enable_predicate_filter_ = enabled;
    return *this;
}

ReadContextBuilder& ReadContextBuilder::EnablePrefetch(bool enabled) {
    impl_->enable_prefetch_ = enabled;
    return *this;
}

ReadContextBuilder& ReadContextBuilder::SetPrefetchBatchCount(uint32_t batch_count) {
    impl_->prefetch_batch_count_ = batch_count;
    return *this;
}

ReadContextBuilder& ReadContextBuilder::SetPrefetchMaxParallelNum(uint32_t max_parallel_num) {
    impl_->prefetch_max_parallel_num_ = max_parallel_num;
    return *this;
}

ReadContextBuilder& ReadContextBuilder::EnableMultiThreadRowToBatch(bool enabled) {
    impl_->enable_multi_thread_row_to_batch_ = enabled;
    return *this;
}

ReadContextBuilder& ReadContextBuilder::SetRowToBatchThreadNumber(uint32_t thread_number) {
    impl_->row_to_batch_thread_number_ = thread_number;
    return *this;
}

ReadContextBuilder& ReadContextBuilder::WithMemoryPool(
    const std::shared_ptr<MemoryPool>& memory_pool) {
    impl_->memory_pool_ = memory_pool;
    return *this;
}

ReadContextBuilder& ReadContextBuilder::WithExecutor(const std::shared_ptr<Executor>& executor) {
    impl_->executor_ = executor;
    return *this;
}

ReadContextBuilder& ReadContextBuilder::SetTableSchema(const std::string& table_schema) {
    impl_->table_schema_ = table_schema;
    return *this;
}

ReadContextBuilder& ReadContextBuilder::WithBranch(const std::string& branch) {
    impl_->branch_ = branch;
    return *this;
}

ReadContextBuilder& ReadContextBuilder::WithFileSystemSchemeToIdentifierMap(
    const std::map<std::string, std::string>& fs_scheme_to_identifier_map) {
    impl_->fs_scheme_to_identifier_map_ = fs_scheme_to_identifier_map;
    return *this;
}

ReadContextBuilder& ReadContextBuilder::WithFileSystem(
    const std::shared_ptr<FileSystem>& file_system) {
    impl_->specific_file_system_ = file_system;
    return *this;
}

ReadContextBuilder& ReadContextBuilder::SetPrefetchCacheMode(PrefetchCacheMode mode) {
    impl_->prefetch_cache_mode_ = mode;
    return *this;
}

ReadContextBuilder& ReadContextBuilder::WithCacheConfig(const CacheConfig& cache_config) {
    impl_->cache_config_ = cache_config;
    return *this;
}

Result<std::unique_ptr<ReadContext>> ReadContextBuilder::Finish() {
    PAIMON_ASSIGN_OR_RAISE(impl_->path_, PathUtil::NormalizePath(impl_->path_));
    if (impl_->path_.empty()) {
        return Status::Invalid("cannot read with empty table path");
    }
    if (impl_->enable_prefetch_ && impl_->prefetch_batch_count_ <= 0) {
        return Status::Invalid("prefetch batch count should be greater than 0");
    }
    if (impl_->enable_prefetch_ &&
        impl_->prefetch_batch_count_ < impl_->prefetch_max_parallel_num_) {
        return Status::Invalid(
            "prefetch batch count should be greater than or equal to prefetch max parallel num");
    }
    if (!impl_->executor_) {
        // If the user do not set executor, create default executor by prefetch batch count
        uint32_t thread_count = impl_->enable_prefetch_ ? impl_->prefetch_max_parallel_num_ : 1;
        impl_->executor_ = CreateDefaultExecutor(thread_count);
    }

    if (impl_->enable_multi_thread_row_to_batch_ && impl_->row_to_batch_thread_number_ <= 0) {
        return Status::Invalid("row to batch thread number should be greater than 0");
    }
    auto ctx = std::make_unique<ReadContext>(
        impl_->path_, impl_->branch_, impl_->read_field_names_, impl_->read_field_ids_,
        impl_->predicate_, impl_->enable_predicate_filter_, impl_->enable_prefetch_,
        impl_->prefetch_batch_count_, impl_->prefetch_max_parallel_num_,
        impl_->enable_multi_thread_row_to_batch_, impl_->row_to_batch_thread_number_,
        impl_->table_schema_, impl_->memory_pool_, impl_->executor_, impl_->specific_file_system_,
        impl_->fs_scheme_to_identifier_map_, impl_->options_, impl_->prefetch_cache_mode_,
        impl_->cache_config_);
    impl_->Reset();
    return ctx;
}

}  // namespace paimon
