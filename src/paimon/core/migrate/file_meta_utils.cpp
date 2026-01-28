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

#include "paimon/migrate/file_meta_utils.h"

#include <cassert>
#include <cstdint>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/stats/simple_stats_converter.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/format/file_format.h"
#include "paimon/format/format_stats_extractor.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"

namespace paimon {
namespace {
Result<std::shared_ptr<TableSchema>> LoadTableSchema(const std::shared_ptr<FileSystem>& fs,
                                                     const std::string& table_path) {
    SchemaManager schema_manager(fs, table_path);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> table_schema,
                           schema_manager.Latest());
    if (table_schema == std::nullopt) {
        return Status::Invalid(fmt::format("load schema failed, no schema in {}", table_path));
    }
    if (table_schema.value()->Id() != TableSchema::FIRST_SCHEMA_ID) {
        return Status::NotImplemented("do not support schema evolution in migrate process");
    }
    return table_schema.value();
}

Result<std::shared_ptr<DataFileMeta>> ConstructFileMeta(
    const std::string& src_file_path, const std::string& format_identifier,
    const std::string& bucket_path, int64_t schema_id,
    const std::shared_ptr<FormatStatsExtractor>& stats_extractor,
    const std::shared_ptr<FileSystem>& fs, const std::shared_ptr<MemoryPool>& memory_pool) {
    std::string file_name = PathUtil::GetName(src_file_path);
    // rename
    std::string new_file_name = StringUtils::EndsWith(file_name, "." + format_identifier)
                                    ? file_name
                                    : (file_name + "." + format_identifier);
    std::string dst_file_path = PathUtil::JoinPath(bucket_path, new_file_name);
    PAIMON_ASSIGN_OR_RAISE(bool dst_exist, fs->Exists(dst_file_path));
    if (!dst_exist) {
        PAIMON_RETURN_NOT_OK(fs->Rename(/*src=*/src_file_path, /*dst=*/dst_file_path));
    }
    // extract stats
    PAIMON_ASSIGN_OR_RAISE(auto stats,
                           stats_extractor->ExtractWithFileInfo(fs, dst_file_path, memory_pool));
    PAIMON_ASSIGN_OR_RAISE(SimpleStats simple_stats,
                           SimpleStatsConverter::ToBinary(stats.first, memory_pool.get()));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStatus> file_status,
                           fs->GetFileStatus(dst_file_path));
    assert(file_status);
    return DataFileMeta::ForAppend(
        new_file_name, file_status->GetLen(), stats.second.GetRowCount(), simple_stats,
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, schema_id, /*extra_files=*/{},
        /*embedded_index=*/nullptr, FileSource::Append(), /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
}

Status ValidateNonObjectPath(const std::vector<std::string>& files) {
    for (const auto& file : files) {
        PAIMON_ASSIGN_OR_RAISE(bool is_object_store, FileSystem::IsObjectStore(file));
        if (is_object_store) {
            return Status::NotImplemented(
                "FileMetaUtils does not support object store file system for now");
        }
    }
    return Status::OK();
}
}  // namespace

Result<std::unique_ptr<CommitMessage>> FileMetaUtils::GenerateCommitMessage(
    const std::vector<std::string>& src_data_files, const std::string& dst_table_path,
    const std::map<std::string, std::string>& partition_values,
    const std::map<std::string, std::string>& options,
    const std::shared_ptr<FileSystem>& file_system) {
    auto memory_pool = GetDefaultPool();
    // load table schema
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options, CoreOptions::FromMap(options, file_system));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> table_schema,
                           LoadTableSchema(tmp_options.GetFileSystem(), dst_table_path));
    if (!table_schema->PrimaryKeys().empty() || table_schema->NumBuckets() != -1) {
        return Status::Invalid("migrate only support append table with unaware-bucket");
    }
    // merge options
    auto table_options = table_schema->Options();
    for (const auto& [key, value] : options) {
        table_options[key] = value;
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(table_options, file_system));

    std::shared_ptr<FileSystem> fs = core_options.GetFileSystem();
    std::shared_ptr<FileFormat> format = core_options.GetWriteFileFormat();
    assert(fs);
    assert(format);
    PAIMON_RETURN_NOT_OK(ValidateNonObjectPath(src_data_files));
    PAIMON_RETURN_NOT_OK(ValidateNonObjectPath({dst_table_path}));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                           core_options.CreateExternalPaths());
    if (!external_paths.empty() || core_options.IndexFileInDataFileDir()) {
        return Status::Invalid(
            "migrate only support schema without external paths and index not in data file dir");
    }

    // generate partition
    auto schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<BinaryRowPartitionComputer> partition_computer,
        BinaryRowPartitionComputer::Create(table_schema->PartitionKeys(), schema,
                                           core_options.GetPartitionDefaultName(),
                                           core_options.LegacyPartitionNameEnabled(), memory_pool));
    PAIMON_ASSIGN_OR_RAISE(BinaryRow partition_row,
                           partition_computer->ToBinaryRow(partition_values));

    // generate bucket path
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<FileStorePathFactory> file_store_path_factory,
        FileStorePathFactory::Create(dst_table_path, schema, table_schema->PartitionKeys(),
                                     core_options.GetPartitionDefaultName(), format->Identifier(),
                                     core_options.DataFilePrefix(),
                                     core_options.LegacyPartitionNameEnabled(),
                                     /*external_paths=*/std::vector<std::string>(),
                                     /*global_index_external_path=*/std::nullopt,
                                     /*index_file_in_data_file_dir=*/false, memory_pool));
    PAIMON_ASSIGN_OR_RAISE(std::string bucket_path,
                           file_store_path_factory->BucketPath(partition_row, /*bucket=*/0));

    // prepare stats extractor
    ::ArrowSchema arrow_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &arrow_schema));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatStatsExtractor> stats_extractor,
                           format->CreateStatsExtractor(&arrow_schema));

    // prepare data file meta
    std::vector<std::shared_ptr<DataFileMeta>> data_file_metas;
    data_file_metas.reserve(src_data_files.size());
    for (const auto& file : src_data_files) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<DataFileMeta> meta,
            ConstructFileMeta(file, format->Identifier(), bucket_path, table_schema->Id(),
                              stats_extractor, fs, memory_pool));
        data_file_metas.push_back(meta);
    }

    return std::make_unique<CommitMessageImpl>(
        partition_row, /*bucket=*/0, /*total_buckets=*/core_options.GetBucket(),
        DataIncrement(std::move(data_file_metas), /*deleted_files=*/{}, /*changelog_files=*/{}),
        CompactIncrement(/*compact_before=*/{}, /*compact_after=*/{}, /*changelog_files=*/{}));
}
}  // namespace paimon
