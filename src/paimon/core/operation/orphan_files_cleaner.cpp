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

#include "paimon/orphan_files_cleaner.h"

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/core_options.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/orphan_files_cleaner_impl.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/executor.h"
#include "paimon/format/file_format.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

static constexpr int64_t FILE_MODIFICATION_THRESHOLD_MS = 86400 * 1000L;  // 1 day

CleanContext::CleanContext(const std::string& root_path,
                           const std::map<std::string, std::string>& options, int64_t older_than_ms,
                           const std::shared_ptr<MemoryPool>& pool,
                           const std::shared_ptr<Executor>& executor,
                           const std::shared_ptr<FileSystem>& specific_file_system,
                           std::function<bool(const std::string&)> should_be_retained)
    : root_path_(root_path),
      options_(options),
      older_than_ms_(older_than_ms),
      memory_pool_(pool),
      executor_(executor),
      specific_file_system_(specific_file_system),
      should_be_retained_(should_be_retained) {}

CleanContext::~CleanContext() = default;

class CleanContextBuilder::Impl {
 public:
    friend class CleanContextBuilder;

    void Reset() {
        options_.clear();
        older_than_ms_ =
            DateTimeUtils::GetCurrentUTCTimeUs() / 1000 - FILE_MODIFICATION_THRESHOLD_MS;
        memory_pool_ = GetDefaultPool();
        executor_ = CreateDefaultExecutor();
        specific_file_system_.reset();
        should_be_retained_ = nullptr;
    }

 private:
    std::string root_path_;
    std::map<std::string, std::string> options_;
    int64_t older_than_ms_ =
        DateTimeUtils::GetCurrentUTCTimeUs() / 1000 - FILE_MODIFICATION_THRESHOLD_MS;
    std::shared_ptr<MemoryPool> memory_pool_ = GetDefaultPool();
    std::shared_ptr<Executor> executor_ = CreateDefaultExecutor();
    std::shared_ptr<FileSystem> specific_file_system_ = nullptr;
    std::function<bool(const std::string&)> should_be_retained_ = nullptr;
};

CleanContextBuilder::CleanContextBuilder(const std::string& root_path)
    : impl_(std::make_unique<Impl>()) {
    impl_->root_path_ = root_path;
}

CleanContextBuilder::~CleanContextBuilder() = default;

CleanContextBuilder& CleanContextBuilder::AddOption(const std::string& key,
                                                    const std::string& value) {
    impl_->options_[key] = value;
    return *this;
}

CleanContextBuilder& CleanContextBuilder::SetOptions(
    const std::map<std::string, std::string>& opts) {
    impl_->options_ = opts;
    return *this;
}

CleanContextBuilder& CleanContextBuilder::WithOlderThanMs(int64_t older_than_ms) {
    impl_->older_than_ms_ = older_than_ms;
    return *this;
}

CleanContextBuilder& CleanContextBuilder::WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) {
    impl_->memory_pool_ = pool;
    return *this;
}

CleanContextBuilder& CleanContextBuilder::WithExecutor(const std::shared_ptr<Executor>& executor) {
    impl_->executor_ = executor;
    return *this;
}

CleanContextBuilder& CleanContextBuilder::WithFileSystem(
    const std::shared_ptr<FileSystem>& file_system) {
    impl_->specific_file_system_ = file_system;
    return *this;
}

CleanContextBuilder& CleanContextBuilder::WithFileRetainCondition(
    std::function<bool(const std::string&)> should_be_retained) {
    impl_->should_be_retained_ = should_be_retained;
    return *this;
}

Result<std::unique_ptr<CleanContext>> CleanContextBuilder::Finish() {
    PAIMON_ASSIGN_OR_RAISE(impl_->root_path_, PathUtil::NormalizePath(impl_->root_path_));
    if (impl_->root_path_.empty()) {
        return Status::Invalid("root path is empty");
    }
    auto ctx = std::make_unique<CleanContext>(
        impl_->root_path_, impl_->options_, impl_->older_than_ms_, impl_->memory_pool_,
        impl_->executor_, impl_->specific_file_system_, impl_->should_be_retained_);
    impl_->Reset();
    return ctx;
}

Result<std::unique_ptr<OrphanFilesCleaner>> OrphanFilesCleaner::Create(
    std::unique_ptr<CleanContext>&& ctx) {
    if (ctx == nullptr) {
        return Status::Invalid("clean context is null pointer");
    }
    if (ctx->GetMemoryPool() == nullptr) {
        return Status::Invalid("memory pool is null pointer");
    }
    if (ctx->GetExecutor() == nullptr) {
        return Status::Invalid("executor is null pointer");
    }
    if (ctx->GetOlderThanMs() < 0) {
        return Status::Invalid("older than time needs to be greater than or equal to 0.");
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options,
                           CoreOptions::FromMap(ctx->GetOptions(), ctx->GetSpecificFileSystem()));
    SchemaManager schema_manager(tmp_options.GetFileSystem(), ctx->GetRootPath());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> table_schema,
                           schema_manager.Latest());
    if (table_schema == std::nullopt) {
        return Status::Invalid("not found latest schema");
    }
    if (!table_schema.value()->PrimaryKeys().empty()) {
        return Status::NotImplemented("orphan files cleaner only support append table");
    }
    // merge options
    const auto& schema = table_schema.value();
    auto opts = schema->Options();
    for (const auto& [key, value] : ctx->GetOptions()) {
        opts[key] = value;
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options,
                           CoreOptions::FromMap(opts, ctx->GetSpecificFileSystem()));
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths, options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           options.CreateGlobalIndexExternalPath());

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            ctx->GetRootPath(), arrow_schema, schema->PartitionKeys(),
            options.GetPartitionDefaultName(), options.GetWriteFileFormat()->Identifier(),
            options.DataFilePrefix(), options.LegacyPartitionNameEnabled(), external_paths,
            global_index_external_path, options.IndexFileInDataFileDir(), ctx->GetMemoryPool()));
    auto snapshot_manager =
        std::make_shared<SnapshotManager>(options.GetFileSystem(), ctx->GetRootPath());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestList> manifest_list,
        ManifestList::Create(options.GetFileSystem(), options.GetManifestFormat(),
                             options.GetManifestCompression(), path_factory, ctx->GetMemoryPool()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, table_schema.value()->PartitionKeys()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestFile> manifest_file,
        ManifestFile::Create(options.GetFileSystem(), options.GetManifestFormat(),
                             options.GetManifestCompression(), path_factory,
                             options.GetManifestTargetFileSize(), ctx->GetMemoryPool(), options,
                             partition_schema));
    return std::make_unique<OrphanFilesCleanerImpl>(
        ctx->GetMemoryPool(), ctx->GetExecutor(), arrow_schema, ctx->GetRootPath(), options,
        snapshot_manager, schema->PartitionKeys(), manifest_file, manifest_list,
        ctx->GetOlderThanMs(), ctx->GetFileRetainCondition());
}

}  // namespace paimon
