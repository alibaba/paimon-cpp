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

#include "paimon/table/source/table_scan.h"

#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/predicate/predicate_validator.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/core_options.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/append_only_file_store_scan.h"
#include "paimon/core/operation/data_evolution_file_store_scan.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/operation/key_value_file_store_scan.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/schema_validation.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/source/abstract_table_scan.h"
#include "paimon/core/table/source/append_only_split_generator.h"
#include "paimon/core/table/source/data_evolution_batch_scan.h"
#include "paimon/core/table/source/data_evolution_split_generator.h"
#include "paimon/core/table/source/data_table_batch_scan.h"
#include "paimon/core/table/source/data_table_stream_scan.h"
#include "paimon/core/table/source/merge_tree_split_generator.h"
#include "paimon/core/table/source/snapshot/snapshot_reader.h"
#include "paimon/core/table/source/split_generator.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/fields_comparator.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/index_file_path_factories.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/format/file_format.h"
#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class Executor;
class MemoryPool;

class TableScanImpl {
 public:
    static Result<std::unique_ptr<FileStoreScan>> CreateFileStoreScan(
        const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::shared_ptr<TableSchema>& table_schema, const CoreOptions& core_options,
        const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& memory_pool,
        const ScanContext* context) {
        auto fs = core_options.GetFileSystem();
        auto manifest_file_format = core_options.GetManifestFormat();
        auto snapshot_manager = std::make_shared<SnapshotManager>(fs, context->GetPath());
        // TODO(liancheng.lsz): support fallback branch in scan
        auto schema_manager = std::make_shared<SchemaManager>(fs, context->GetPath());
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<ManifestList> manifest_list,
            ManifestList::Create(fs, manifest_file_format, core_options.GetManifestCompression(),
                                 path_factory, memory_pool));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Schema> partition_schema,
            FieldMapping::GetPartitionSchema(arrow_schema, table_schema->PartitionKeys()));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<ManifestFile> manifest_file,
            ManifestFile::Create(fs, manifest_file_format, core_options.GetManifestCompression(),
                                 path_factory, core_options.GetManifestTargetFileSize(),
                                 memory_pool, core_options, partition_schema));
        if (table_schema->PrimaryKeys().empty()) {
            if (core_options.DataEvolutionEnabled()) {
                return DataEvolutionFileStoreScan::Create(
                    snapshot_manager, schema_manager, manifest_list, manifest_file, table_schema,
                    arrow_schema, context->GetScanFilters(), core_options, executor, memory_pool);
            }
            return AppendOnlyFileStoreScan::Create(
                snapshot_manager, schema_manager, manifest_list, manifest_file, table_schema,
                arrow_schema, context->GetScanFilters(), core_options, executor, memory_pool);
        }
        return KeyValueFileStoreScan::Create(
            snapshot_manager, schema_manager, manifest_list, manifest_file, table_schema,
            arrow_schema, context->GetScanFilters(), core_options, executor, memory_pool);
    }

    static Result<std::unique_ptr<SplitGenerator>> CreateSplitGenerator(
        const std::shared_ptr<TableSchema>& table_schema, const CoreOptions& core_options,
        const ScanContext* context) {
        auto source_split_target_size = core_options.GetSourceSplitTargetSize();
        auto source_split_open_file_cost = core_options.GetSourceSplitOpenFileCost();
        if (table_schema->PrimaryKeys().empty()) {
            if (core_options.DataEvolutionEnabled()) {
                return std::make_unique<DataEvolutionSplitGenerator>(source_split_target_size,
                                                                     source_split_open_file_cost);
            }
            BucketMode bucket_mode = (core_options.GetBucket() == -1 ? BucketMode::BUCKET_UNAWARE
                                                                     : BucketMode::HASH_FIXED);
            return std::make_unique<AppendOnlySplitGenerator>(
                source_split_target_size, source_split_open_file_cost, bucket_mode);
        } else {
            // TODO(liancheng.lsz): support evolution
            PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                                   table_schema->TrimmedPrimaryKeys());
            PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> trimmed_pk_fields,
                                   table_schema->GetFields(trimmed_primary_keys));
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<FieldsComparator> key_comparator,
                FieldsComparator::Create(trimmed_pk_fields, /*is_ascending_order=*/true,
                                         /*use_view=*/false));
            return std::make_unique<MergeTreeSplitGenerator>(
                source_split_target_size, source_split_open_file_cost,
                core_options.DeletionVectorsEnabled(), core_options.GetMergeEngine(),
                key_comparator);
        }
    }

    static Result<std::unique_ptr<IndexFileHandler>> CreateIndexFileHandler(
        const CoreOptions& core_options, const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<MemoryPool>& memory_pool) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<IndexManifestFile> index_manifest_file,
                               IndexManifestFile::Create(core_options.GetFileSystem(),
                                                         core_options.GetManifestFormat(),
                                                         core_options.GetManifestCompression(),
                                                         path_factory, memory_pool, core_options));
        return std::make_unique<IndexFileHandler>(
            std::move(index_manifest_file), std::make_shared<IndexFilePathFactories>(path_factory));
    }
};

Result<std::unique_ptr<TableScan>> TableScan::Create(std::unique_ptr<ScanContext> context) {
    if (context == nullptr) {
        return Status::Invalid("scan context is null pointer");
    }
    if (context->GetMemoryPool() == nullptr) {
        return Status::Invalid("memory pool is null pointer");
    }
    if (context->GetExecutor() == nullptr) {
        return Status::Invalid("executor is null pointer");
    }

    // load schema
    PAIMON_ASSIGN_OR_RAISE(
        CoreOptions tmp_options,
        CoreOptions::FromMap(context->GetOptions(), context->GetSpecificFileSystem()));
    SchemaManager schema_manager(tmp_options.GetFileSystem(), context->GetPath());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_table_schema,
                           schema_manager.Latest());
    if (latest_table_schema == std::nullopt) {
        return Status::Invalid("not found latest schema");
    }
    const auto& table_schema = latest_table_schema.value();
    if (table_schema->Id() != TableSchema::FIRST_SCHEMA_ID &&
        !table_schema->PrimaryKeys().empty()) {
        return Status::NotImplemented(
            "do not support schema evolution in pk table while scan process");
    }
    // merge options
    auto options = table_schema->Options();
    for (const auto& [key, value] : context->GetOptions()) {
        options[key] = value;
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(options, context->GetSpecificFileSystem()));
    // validate options
    if (core_options.GetBucket() == -1) {
        if (!table_schema->PrimaryKeys().empty()) {
            return Status::NotImplemented(fmt::format(
                "do not support pk table bucket={} in scan process", core_options.GetBucket()));
        }
    } else if (core_options.GetBucket() < 1 &&
               !SchemaValidation::IsPostponeBucketTable(*table_schema, core_options.GetBucket())) {
        return Status::Invalid(
            fmt::format("do not support bucket={} in scan process", core_options.GetBucket()));
    }

    // validate schema and scan filter
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    if (context->GetScanFilters() && context->GetScanFilters()->GetPredicate()) {
        PAIMON_RETURN_NOT_OK(PredicateValidator::ValidatePredicateWithSchema(
            *arrow_schema, context->GetScanFilters()->GetPredicate(),
            /*validate_field_idx=*/false));
        PAIMON_RETURN_NOT_OK(PredicateValidator::ValidatePredicateWithLiterals(
            context->GetScanFilters()->GetPredicate()));
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                           core_options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           core_options.CreateGlobalIndexExternalPath());

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            context->GetPath(), arrow_schema, table_schema->PartitionKeys(),
            core_options.GetPartitionDefaultName(), core_options.GetWriteFileFormat()->Identifier(),
            core_options.DataFilePrefix(), core_options.LegacyPartitionNameEnabled(),
            external_paths, global_index_external_path, core_options.IndexFileInDataFileDir(),
            context->GetMemoryPool()));

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStoreScan> file_store_scan,
                           TableScanImpl::CreateFileStoreScan(
                               path_factory, arrow_schema, table_schema, core_options,
                               context->GetExecutor(), context->GetMemoryPool(), context.get()));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<SplitGenerator> split_generator,
        TableScanImpl::CreateSplitGenerator(table_schema, core_options, context.get()));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<IndexFileHandler> index_file_handler,
                           TableScanImpl::CreateIndexFileHandler(core_options, path_factory,
                                                                 context->GetMemoryPool()));
    auto snapshot_reader = std::make_shared<SnapshotReader>(
        file_store_scan, path_factory, std::move(split_generator), std::move(index_file_handler));
    if (context->IsStreamingMode()) {
        return std::make_unique<DataTableStreamScan>(core_options, snapshot_reader);
    }
    auto batch_scan =
        std::make_unique<DataTableBatchScan>(/*pk_table=*/!table_schema->PrimaryKeys().empty(),
                                             core_options, snapshot_reader, context->GetLimit());
    if (!core_options.DataEvolutionEnabled()) {
        return batch_scan;
    }
    return std::make_unique<DataEvolutionBatchScan>(
        context->GetPath(), snapshot_reader, std::move(batch_scan), context->GetGlobalIndexResult(),
        context->GetScanFilters()->GetVectorSearch(), core_options, context->GetMemoryPool(),
        context->GetExecutor());
}

}  // namespace paimon
