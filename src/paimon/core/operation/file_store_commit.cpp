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

#include "paimon/file_store_commit.h"

#include <cassert>
#include <utility>

#include "paimon/commit_context.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/core/core_options.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/expire_snapshots.h"
#include "paimon/core/operation/file_store_commit_impl.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/format/file_format.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

Result<std::unique_ptr<FileStoreCommit>> FileStoreCommit::Create(
    std::unique_ptr<CommitContext> ctx) {
    if (ctx == nullptr) {
        return Status::Invalid("commit context is null pointer");
    }
    if (ctx->GetMemoryPool() == nullptr) {
        return Status::Invalid("memory pool is null pointer");
    }
    if (ctx->GetExecutor() == nullptr) {
        return Status::Invalid("executor is null pointer");
    }

    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options,
                           CoreOptions::FromMap(ctx->GetOptions(), ctx->GetSpecificFileSystem()));
    const std::string& root_path = ctx->GetRootPath();
    auto schema_manager = std::make_shared<SchemaManager>(tmp_options.GetFileSystem(), root_path);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> table_schema,
                           schema_manager->Latest());
    if (table_schema == std::nullopt) {
        return Status::Invalid("not found latest schema");
    }
    const auto& schema = table_schema.value();
    if (!schema->PrimaryKeys().empty() &&
        ctx->GetOptions().find("enable-pk-commit-in-inte-test") == ctx->GetOptions().end()) {
        return Status::NotImplemented("not support pk table commit yet");
    }
    auto opts = schema->Options();
    for (const auto& [key, value] : ctx->GetOptions()) {
        opts[key] = value;
    }
    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options,
                           CoreOptions::FromMap(opts, ctx->GetSpecificFileSystem()));
    assert(options.GetFileSystem());
    assert(options.GetWriteFileFormat());
    PAIMON_ASSIGN_OR_RAISE(bool is_object_store, FileSystem::IsObjectStore(root_path));
    if (is_object_store && opts.find("enable-object-store-commit-in-inte-test") == opts.end()) {
        return Status::NotImplemented(
            "commit operation does not support object store file system for now");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<BinaryRowPartitionComputer> partition_computer,
        BinaryRowPartitionComputer::Create(
            table_schema.value()->PartitionKeys(), arrow_schema, options.GetPartitionDefaultName(),
            options.LegacyPartitionNameEnabled(), ctx->GetMemoryPool()));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths, options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           options.CreateGlobalIndexExternalPath());

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            root_path, arrow_schema, table_schema.value()->PartitionKeys(),
            options.GetPartitionDefaultName(), options.GetWriteFileFormat()->Identifier(),
            options.DataFilePrefix(), options.LegacyPartitionNameEnabled(), external_paths,
            global_index_external_path, options.IndexFileInDataFileDir(), ctx->GetMemoryPool()));

    auto snapshot_manager = std::make_shared<SnapshotManager>(options.GetFileSystem(), root_path);
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
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<IndexManifestFile> index_manifest_file,
        IndexManifestFile::Create(options.GetFileSystem(), options.GetManifestFormat(),
                                  options.GetManifestCompression(), path_factory,
                                  ctx->GetMemoryPool(), options));

    auto expire_snapshots = std::make_shared<ExpireSnapshots>(
        snapshot_manager, path_factory, manifest_list, manifest_file, options.GetFileSystem(),
        options.GetExpireConfig(), ctx->GetExecutor());

    return std::make_unique<FileStoreCommitImpl>(
        ctx->GetMemoryPool(), ctx->GetExecutor(), arrow_schema, root_path, ctx->GetCommitUser(),
        options, path_factory, std::move(partition_computer), snapshot_manager,
        ctx->IgnoreEmptyCommit(), ctx->UseRESTCatalogCommit(), table_schema.value(), manifest_file,
        manifest_list, index_manifest_file, expire_snapshots, schema_manager);
}

}  // namespace paimon
