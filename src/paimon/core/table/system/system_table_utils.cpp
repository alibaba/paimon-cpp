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

#include "paimon/core/table/system/system_table_utils.h"

#include <utility>

#include "paimon/common/data/binary_string.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"

namespace paimon {

SystemTableContext SystemTableUtils::CreateContext(std::shared_ptr<FileSystem> fs,
                                                   std::string table_path, std::string branch) {
    return {
        std::move(fs), std::move(table_path), BranchManager::NormalizeBranch(branch), nullptr, {},
    };
}

SystemTableContext SystemTableUtils::CreateContext(std::shared_ptr<FileSystem> fs,
                                                   std::string table_path, std::string branch,
                                                   std::shared_ptr<TableSchema> table_schema,
                                                   std::map<std::string, std::string> options) {
    return {
        std::move(fs),           std::move(table_path), BranchManager::NormalizeBranch(branch),
        std::move(table_schema), std::move(options),
    };
}

VariantType SystemTableUtils::OptionalInt64Value(const std::optional<int64_t>& value) {
    if (!value) {
        return NullType();
    }
    return value.value();
}

VariantType SystemTableUtils::StringValue(const std::string& value) {
    return BinaryString::FromString(value, GetDefaultPool().get());
}

VariantType SystemTableUtils::OptionalStringValue(const std::optional<std::string>& value) {
    if (!value) {
        return NullType();
    }
    return StringValue(value.value());
}

namespace {

VariantType TimestampMillisValue(int64_t value) {
    return Timestamp::FromEpochMillis(value);
}

}  // namespace

Result<VariantType> SystemTableUtils::LocalTimestampMillisValue(int64_t epoch_millis) {
    PAIMON_ASSIGN_OR_RAISE(
        Timestamp local_timestamp,
        DateTimeUtils::ToLocalTimestamp(Timestamp::FromEpochMillis(epoch_millis)));
    return TimestampMillisValue(local_timestamp.GetMillisecond());
}

Result<VariantType> SystemTableUtils::LocalTimestampMillisValue(const Timestamp& local_timestamp) {
    PAIMON_ASSIGN_OR_RAISE(Timestamp utc_timestamp, DateTimeUtils::ToUTCTimestamp(local_timestamp));
    int64_t epoch_millis = utc_timestamp.GetMillisecond();
    return LocalTimestampMillisValue(epoch_millis);
}

Result<CoreOptions> SystemTableUtils::CreateCoreOptions(const SystemTableContext& context) {
    return CoreOptions::FromMap(context.options, context.fs);
}

Result<std::shared_ptr<FileStorePathFactory>> SystemTableUtils::CreatePathFactory(
    const SystemTableContext& context, const CoreOptions& core_options,
    const std::shared_ptr<MemoryPool>& pool) {
    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(context.table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                           core_options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           core_options.CreateGlobalIndexExternalPath());
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            context.table_path, arrow_schema, context.table_schema->PartitionKeys(),
            core_options.GetPartitionDefaultName(), core_options.GetFileFormat()->Identifier(),
            core_options.DataFilePrefix(), core_options.LegacyPartitionNameEnabled(),
            external_paths, global_index_external_path, core_options.IndexFileInDataFileDir(),
            pool));
    return std::shared_ptr<FileStorePathFactory>(std::move(path_factory));
}

Result<std::optional<Snapshot>> SystemTableUtils::LatestSnapshot(
    const SystemTableContext& context) {
    SnapshotManager snapshot_manager(context.fs, context.table_path, context.branch);
    return snapshot_manager.LatestSnapshot();
}

Result<std::vector<ManifestFileMeta>> SystemTableUtils::ReadDataManifests(
    const SystemTableContext& context, const Snapshot& snapshot,
    const std::shared_ptr<FileStorePathFactory>& path_factory, const CoreOptions& core_options,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<ManifestList> manifest_list,
        ManifestList::Create(context.fs, core_options.GetManifestFormat(),
                             core_options.GetManifestCompression(), path_factory, pool));
    std::vector<ManifestFileMeta> manifests;
    PAIMON_RETURN_NOT_OK(manifest_list->ReadDataManifests(snapshot, &manifests));
    return manifests;
}

}  // namespace paimon
