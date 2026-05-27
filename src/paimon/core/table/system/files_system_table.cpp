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

#include "paimon/core/table/system/files_system_table.h"

#include <utility>

#include "arrow/api.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/internal_row_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_entry.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats_evolutions.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {
namespace {

Result<std::vector<ManifestEntry>> ReadLatestManifestEntries(
    const SystemTableContext& context, const std::shared_ptr<FileStorePathFactory>& path_factory,
    const CoreOptions& core_options, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot,
                           SystemTableUtils::LatestSnapshot(context));
    if (!snapshot) {
        return std::vector<ManifestEntry>();
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestFileMeta> manifests,
                           SystemTableUtils::ReadDataManifests(context, snapshot.value(),
                                                               path_factory, core_options, pool));
    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(context.table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, context.table_schema->PartitionKeys()));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ManifestFile> manifest_file,
                           ManifestFile::Create(context.fs, core_options.GetManifestFormat(),
                                                core_options.GetManifestCompression(), path_factory,
                                                core_options.GetManifestTargetFileSize(), pool,
                                                core_options, partition_schema));
    std::vector<ManifestEntry> entries;
    for (const auto& manifest : manifests) {
        PAIMON_RETURN_NOT_OK(
            manifest_file->Read(manifest.FileName(), /*filter=*/nullptr, &entries));
    }
    return entries;
}

Result<std::vector<ManifestEntry>> ReadLatestDataFiles(
    const SystemTableContext& context, const std::shared_ptr<FileStorePathFactory>& path_factory,
    const CoreOptions& core_options, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestEntry> entries,
                           ReadLatestManifestEntries(context, path_factory, core_options, pool));
    std::vector<ManifestEntry> merged_entries;
    PAIMON_RETURN_NOT_OK(FileEntry::MergeEntries(entries, &merged_entries));
    return merged_entries;
}

std::optional<std::string> OptionalBinaryRowString(const BinaryRow& row) {
    if (row.GetFieldCount() <= 0) {
        return std::nullopt;
    }
    return row.ToString();
}

Result<std::string> PartitionString(const std::shared_ptr<FileStorePathFactory>& path_factory,
                                    const BinaryRow& partition) {
    PAIMON_ASSIGN_OR_RAISE(std::string value, path_factory->GetPartitionString(partition));
    return value;
}

Result<std::string> FilePath(const std::shared_ptr<FileStorePathFactory>& path_factory,
                             const ManifestEntry& entry, const DataFileMeta& file) {
    if (file.external_path) {
        return file.external_path.value();
    }
    PAIMON_ASSIGN_OR_RAISE(std::string bucket_path,
                           path_factory->BucketPath(entry.Partition(), entry.Bucket()));
    return PathUtil::JoinPath(bucket_path, file.file_name);
}

Result<std::string> FieldsValueMapString(const std::vector<DataField>& fields,
                                         const InternalRow& row) {
    std::shared_ptr<arrow::Schema> schema = DataField::ConvertDataFieldsToArrowSchema(fields);
    PAIMON_ASSIGN_OR_RAISE(std::vector<InternalRow::FieldGetterFunc> getters,
                           InternalRowUtils::CreateFieldGetters(schema, /*use_view=*/false));
    std::vector<std::string> values;
    values.reserve(fields.size());
    for (size_t i = 0; i < fields.size(); ++i) {
        std::string value = "null";
        if (!row.IsNullAt(i)) {
            VariantType field_value = getters[i](row);
            if (std::holds_alternative<std::string_view>(field_value)) {
                value = std::string(std::get<std::string_view>(field_value));
            } else {
                value = DataDefine::VariantValueToString(field_value);
            }
        }
        values.emplace_back(fmt::format("{}:{}", fields[i].Name(), value));
    }
    return fmt::format("{{{}}}", fmt::join(values, ", "));
}

Result<std::string> NullValueCountsString(const std::vector<DataField>& fields,
                                          const InternalArray& null_counts) {
    std::vector<std::string> values;
    values.reserve(fields.size());
    for (size_t i = 0; i < fields.size(); ++i) {
        std::string value =
            null_counts.IsNullAt(i) ? "null" : std::to_string(null_counts.GetLong(i));
        values.emplace_back(fmt::format("{}:{}", fields[i].Name(), value));
    }
    return fmt::format("{{{}}}", fmt::join(values, ", "));
}

Result<std::shared_ptr<TableSchema>> LoadDataSchema(const SystemTableContext& context,
                                                    int64_t schema_id) {
    if (schema_id == context.table_schema->Id()) {
        return context.table_schema;
    }
    SchemaManager schema_manager(context.fs, context.table_path, context.branch);
    return schema_manager.ReadSchema(schema_id);
}

Result<std::vector<DataField>> ValueStatsFields(const SystemTableContext& context,
                                                int64_t schema_id) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> data_schema,
                           LoadDataSchema(context, schema_id));
    return data_schema->Fields();
}

Result<std::shared_ptr<InternalArray>> WriteColsValue(
    const std::optional<std::vector<std::string>>& write_cols,
    const std::shared_ptr<MemoryPool>& pool) {
    if (!write_cols) {
        return std::shared_ptr<InternalArray>();
    }
    return std::make_shared<BinaryArray>(
        InternalRowUtils::ToNotNullStringArrayData(write_cols.value(), pool));
}

}  // namespace

FilesSystemTable::FilesSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                   std::string branch, std::shared_ptr<TableSchema> table_schema,
                                   std::map<std::string, std::string> options)
    : InMemorySystemTable(table_path),
      context_(SystemTableUtils::CreateContext(std::move(fs), std::move(table_path),
                                               std::move(branch), std::move(table_schema),
                                               std::move(options))) {}

std::string FilesSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> FilesSystemTable::ArrowSchema() const {
    return arrow::schema({
        arrow::field("partition", arrow::utf8(), /*nullable=*/true),
        arrow::field("bucket", arrow::int32(), /*nullable=*/false),
        arrow::field("file_path", arrow::utf8(), /*nullable=*/false),
        arrow::field("file_format", arrow::utf8(), /*nullable=*/false),
        arrow::field("schema_id", arrow::int64(), /*nullable=*/false),
        arrow::field("level", arrow::int32(), /*nullable=*/false),
        arrow::field("record_count", arrow::int64(), /*nullable=*/false),
        arrow::field("file_size_in_bytes", arrow::int64(), /*nullable=*/false),
        arrow::field("min_key", arrow::utf8(), /*nullable=*/true),
        arrow::field("max_key", arrow::utf8(), /*nullable=*/true),
        arrow::field("null_value_counts", arrow::utf8(), /*nullable=*/false),
        arrow::field("min_value_stats", arrow::utf8(), /*nullable=*/false),
        arrow::field("max_value_stats", arrow::utf8(), /*nullable=*/false),
        arrow::field("min_sequence_number", arrow::int64(), /*nullable=*/true),
        arrow::field("max_sequence_number", arrow::int64(), /*nullable=*/true),
        arrow::field("creation_time", arrow::timestamp(arrow::TimeUnit::MILLI),
                     /*nullable=*/true),
        arrow::field("deleteRowCount", arrow::int64(), /*nullable=*/true),
        arrow::field("file_source", arrow::utf8(), /*nullable=*/true),
        arrow::field("first_row_id", arrow::int64(), /*nullable=*/true),
        arrow::field("write_cols", arrow::list(arrow::utf8()), /*nullable=*/true),
    });
}

Result<std::vector<GenericRow>> FilesSystemTable::BuildRows() const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, SystemTableUtils::CreateCoreOptions(context_));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStorePathFactory> path_factory,
                           SystemTableUtils::CreatePathFactory(context_, core_options, pool));
    PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestEntry> entries,
                           ReadLatestDataFiles(context_, path_factory, core_options, pool));
    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(context_.table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, context_.table_schema->PartitionKeys()));

    SimpleStatsEvolutions stats_evolutions(context_.table_schema, pool);
    std::vector<GenericRow> rows;
    rows.reserve(entries.size());
    for (const auto& entry : entries) {
        if (!(entry.Kind() == FileKind::Add())) {
            continue;
        }

        const std::shared_ptr<DataFileMeta>& file = entry.File();
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> data_schema,
                               LoadDataSchema(context_, file->schema_id));
        PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> value_stats_fields,
                               ValueStatsFields(context_, file->schema_id));
        std::shared_ptr<SimpleStatsEvolution> stats_evolution =
            stats_evolutions.GetOrCreate(data_schema);
        PAIMON_ASSIGN_OR_RAISE(
            SimpleStatsEvolution::EvolutionStats stats,
            stats_evolution->Evolution(file->value_stats, file->row_count, file->value_stats_cols));

        GenericRow row(schema->num_fields());
        if (context_.table_schema->PartitionKeys().empty()) {
            row.SetField(0, NullType());
        } else {
            PAIMON_ASSIGN_OR_RAISE(std::string partition,
                                   PartitionString(path_factory, entry.Partition()));
            row.SetField(0, SystemTableUtils::StringValue(partition));
        }
        row.SetField(1, entry.Bucket());
        PAIMON_ASSIGN_OR_RAISE(std::string file_path, FilePath(path_factory, entry, *file));
        row.SetField(2, SystemTableUtils::StringValue(file_path));
        PAIMON_ASSIGN_OR_RAISE(std::string file_format, file->FileFormat());
        row.SetField(3, SystemTableUtils::StringValue(file_format));
        row.SetField(4, file->schema_id);
        row.SetField(5, file->level);
        row.SetField(6, file->row_count);
        row.SetField(7, file->file_size);
        row.SetField(8,
                     SystemTableUtils::OptionalStringValue(OptionalBinaryRowString(file->min_key)));
        row.SetField(9,
                     SystemTableUtils::OptionalStringValue(OptionalBinaryRowString(file->max_key)));
        PAIMON_ASSIGN_OR_RAISE(std::string null_value_counts,
                               NullValueCountsString(value_stats_fields, *stats.null_counts));
        row.SetField(10, SystemTableUtils::StringValue(null_value_counts));
        PAIMON_ASSIGN_OR_RAISE(std::string min_value_stats,
                               FieldsValueMapString(value_stats_fields, *stats.min_values));
        row.SetField(11, SystemTableUtils::StringValue(min_value_stats));
        PAIMON_ASSIGN_OR_RAISE(std::string max_value_stats,
                               FieldsValueMapString(value_stats_fields, *stats.max_values));
        row.SetField(12, SystemTableUtils::StringValue(max_value_stats));
        row.SetField(13, file->min_sequence_number);
        row.SetField(14, file->max_sequence_number);
        PAIMON_ASSIGN_OR_RAISE(VariantType creation_time,
                               SystemTableUtils::LocalTimestampMillisValue(file->creation_time));
        row.SetField(15, creation_time);
        row.SetField(16, SystemTableUtils::OptionalInt64Value(file->delete_row_count));
        row.SetField(17, file->file_source
                             ? SystemTableUtils::StringValue(file->file_source.value().ToString())
                             : VariantType(NullType()));
        row.SetField(18, SystemTableUtils::OptionalInt64Value(file->first_row_id));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InternalArray> write_cols,
                               WriteColsValue(file->write_cols, pool));
        row.SetField(19, write_cols ? VariantType(write_cols) : VariantType(NullType()));
        rows.push_back(std::move(row));
    }
    return rows;
}

}  // namespace paimon
