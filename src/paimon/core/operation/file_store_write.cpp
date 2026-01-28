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

#include "paimon/file_store_write.h"

#include <map>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/core_options.h"
#include "paimon/core/mergetree/compact/lookup_merge_function.h"
#include "paimon/core/mergetree/compact/merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/core/operation/append_only_file_store_write.h"
#include "paimon/core/operation/key_value_file_store_write.h"
#include "paimon/core/options/merge_engine.h"
#include "paimon/core/postpone/postpone_bucket_file_store_write.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/fields_comparator.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/primary_key_table_utils.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/format/file_format.h"
#include "paimon/result.h"
#include "paimon/write_context.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
struct KeyValue;
template <typename T>
class MergeFunctionWrapper;

Result<std::unique_ptr<FileStoreWrite>> FileStoreWrite::Create(std::unique_ptr<WriteContext> ctx) {
    if (ctx == nullptr) {
        return Status::Invalid("write context is null pointer");
    }
    if (ctx->GetMemoryPool() == nullptr) {
        return Status::Invalid("memory pool is null pointer");
    }
    if (ctx->GetExecutor() == nullptr) {
        return Status::Invalid("executor is null pointer");
    }

    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options,
                           CoreOptions::FromMap(ctx->GetOptions(), ctx->GetSpecificFileSystem(),
                                                ctx->GetFileSystemSchemeToIdentifierMap()));
    std::string branch = ctx->GetBranch();
    auto schema_manager =
        std::make_shared<SchemaManager>(tmp_options.GetFileSystem(), ctx->GetRootPath(), branch);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> table_schema,
                           schema_manager->Latest());
    if (table_schema == std::nullopt) {
        return Status::Invalid(fmt::format("cannot found latest schema in branch {}", branch));
    }
    const auto& schema = table_schema.value();
    auto opts = schema->Options();
    for (const auto& [key, value] : ctx->GetOptions()) {
        opts[key] = value;
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options,
                           CoreOptions::FromMap(opts, ctx->GetSpecificFileSystem(),
                                                ctx->GetFileSystemSchemeToIdentifierMap()));
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> partition_schema,
                           FieldMapping::GetPartitionSchema(arrow_schema, schema->PartitionKeys()));

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths, options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           options.CreateGlobalIndexExternalPath());

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> file_store_path_factory,
        FileStorePathFactory::Create(
            ctx->GetRootPath(), arrow_schema, schema->PartitionKeys(),
            options.GetPartitionDefaultName(), options.GetWriteFileFormat()->Identifier(),
            options.DataFilePrefix(), options.LegacyPartitionNameEnabled(), external_paths,
            global_index_external_path, options.IndexFileInDataFileDir(), ctx->GetMemoryPool()));
    auto snapshot_manager =
        std::make_shared<SnapshotManager>(options.GetFileSystem(), ctx->GetRootPath(), branch);
    bool ignore_previous_files = ctx->IgnorePreviousFiles();
    if (schema->PrimaryKeys().empty()) {
        // append table
        if (options.GetBucket() == -1) {
            ignore_previous_files = true;
        } else if (options.GetBucket() <= 0) {
            return Status::Invalid(
                fmt::format("not support bucket {} in append table", options.GetBucket()));
        }
        std::shared_ptr<arrow::Schema> write_schema = arrow_schema;
        const auto& write_field_names = ctx->GetWriteSchema();
        if (!write_field_names.empty()) {
            arrow::FieldVector write_fields;
            write_fields.reserve(write_field_names.size());
            for (const auto& field_name : write_field_names) {
                auto field = arrow_schema->GetFieldByName(field_name);
                if (!field) {
                    // TODO(xinyu.lxy): support _ROW_ID and _SEQUENCE_NUMBER
                    return Status::Invalid(
                        fmt::format("write field {} does not exist in table schema", field_name));
                }
                write_fields.push_back(field);
            }
            write_schema = arrow::schema(write_fields);
        }
        return std::make_unique<AppendOnlyFileStoreWrite>(
            file_store_path_factory, snapshot_manager, schema_manager, ctx->GetCommitUser(),
            ctx->GetRootPath(), schema, arrow_schema, write_schema, partition_schema, options,
            ignore_previous_files, ctx->IsStreamingMode(), ctx->IgnoreNumBucketCheck(),
            ctx->GetExecutor(), ctx->GetMemoryPool());
    } else {
        // pk table
        if (options.GetBucket() == BucketModeDefine::POSTPONE_BUCKET) {
            return PostponeBucketFileStoreWrite::Create(
                snapshot_manager, schema_manager, ctx->GetCommitUser(), ctx->GetRootPath(), schema,
                arrow_schema, partition_schema, options, ctx->IsStreamingMode(),
                ctx->IgnoreNumBucketCheck(), ctx->GetWriteId(),
                ctx->GetFileSystemSchemeToIdentifierMap(), ctx->GetExecutor(), ctx->GetMemoryPool(),
                ctx->GetSpecificFileSystem());
        }
        if (options.GetBucket() <= 0) {
            return Status::Invalid(
                fmt::format("not support bucket {} in key value table", options.GetBucket()));
        }
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                               schema->TrimmedPrimaryKeys());
        PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> trimmed_primary_key_fields,
                               schema->GetFields(trimmed_primary_keys));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FieldsComparator> key_comparator,
                               FieldsComparator::Create(trimmed_primary_key_fields,
                                                        options.SequenceFieldSortOrderIsAscending(),
                                                        /*use_view=*/true));
        auto primary_keys = schema->PrimaryKeys();
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<MergeFunction> merge_function,
            PrimaryKeyTableUtils::CreateMergeFunction(arrow_schema, primary_keys, options));
        if (options.NeedLookup() && options.GetMergeEngine() != MergeEngine::FIRST_ROW) {
            // don't wrap first row, it is already OK
            merge_function = std::make_unique<LookupMergeFunction>(std::move(merge_function));
        }
        std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper =
            std::make_shared<ReducerMergeFunctionWrapper>(std::move(merge_function));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FieldsComparator> sequence_fields_comparator,
            PrimaryKeyTableUtils::CreateSequenceFieldsComparator(schema->Fields(), options));
        return std::make_unique<KeyValueFileStoreWrite>(
            file_store_path_factory, snapshot_manager, schema_manager, ctx->GetCommitUser(),
            ctx->GetRootPath(), schema, arrow_schema, partition_schema, key_comparator,
            sequence_fields_comparator, merge_function_wrapper, options, ignore_previous_files,
            ctx->IsStreamingMode(), ctx->IgnoreNumBucketCheck(), ctx->GetExecutor(),
            ctx->GetMemoryPool());
    }
}

}  // namespace paimon
