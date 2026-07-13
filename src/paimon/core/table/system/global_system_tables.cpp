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

#include "paimon/core/table/system/global_system_tables.h"

#include <functional>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_entry.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"

namespace paimon {
namespace {

// =============================================================================
// Registry
// =============================================================================

using GlobalSystemTableFactory =
    std::function<Result<std::shared_ptr<SystemTable>>(const GlobalSystemTableContext&)>;

struct GlobalSystemTableRegistryEntry {
    std::string name;
    bool requires_catalog;
    GlobalSystemTableFactory factory;
};

const std::vector<GlobalSystemTableRegistryEntry>& GlobalSystemTableRegistry() {
    static const std::vector<GlobalSystemTableRegistryEntry> registry = {
        {CatalogOptionsSystemTable::kName, false,
         [](const GlobalSystemTableContext& ctx) -> Result<std::shared_ptr<SystemTable>> {
             return std::make_shared<CatalogOptionsSystemTable>(ctx);
         }},
        {AllTableOptionsSystemTable::kName, true,
         [](const GlobalSystemTableContext& ctx) -> Result<std::shared_ptr<SystemTable>> {
             return std::make_shared<AllTableOptionsSystemTable>(ctx);
         }},
        {TablesSystemTable::kName, true,
         [](const GlobalSystemTableContext& ctx) -> Result<std::shared_ptr<SystemTable>> {
             return std::make_shared<TablesSystemTable>(ctx);
         }},
        {PartitionsSystemTable::kName, true,
         [](const GlobalSystemTableContext& ctx) -> Result<std::shared_ptr<SystemTable>> {
             return std::make_shared<PartitionsSystemTable>(ctx);
         }},
    };
    return registry;
}

// =============================================================================
// Helpers for sys.tables and sys.partitions
// =============================================================================

VariantType StringValue(const std::string& value) {
    return BinaryString::FromString(value, GetDefaultPool().get());
}

VariantType OptionalStringValue(const std::map<std::string, std::string>& options,
                                const std::string& key) {
    auto it = options.find(key);
    return it == options.end() ? VariantType(NullType()) : VariantType(StringValue(it->second));
}

Result<VariantType> OptionalLongValue(const std::map<std::string, std::string>& options,
                                      const std::string& key) {
    if (options.find(key) == options.end()) {
        return VariantType(NullType());
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t value, OptionsUtils::GetValueFromMap<int64_t>(options, key));
    return VariantType(value);
}

Result<bool> IsEnabled(const GlobalSystemTableRegistryEntry& entry,
                       const std::map<std::string, std::string>& catalog_options) {
    if (entry.name != CatalogOptionsSystemTable::kName) {
        return true;
    }
    return OptionsUtils::GetValueFromMap<bool>(catalog_options,
                                               CatalogOptionsSystemTable::kEnabledOption, false);
}

// Aggregated file-level statistics for a table or partition.
struct FileStats {
    int64_t record_count = 0;
    int64_t file_size_in_bytes = 0;
    int64_t file_count = 0;
    int64_t last_file_creation_time_millis = 0;
};

struct AggregatedFileStats {
    bool has_snapshot = false;
    std::map<std::string, FileStats> by_partition;
};

// Read the latest snapshot's data files and aggregate statistics.
Result<AggregatedFileStats> AggregateFileStats(const std::shared_ptr<FileSystem>& fs,
                                               const std::string& table_path,
                                               const std::map<std::string, std::string>& options) {
    AggregatedFileStats result;

    SnapshotManager snapshot_manager(fs, table_path, BranchManager::DEFAULT_MAIN_BRANCH);
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot, snapshot_manager.LatestSnapshot());
    if (!snapshot) {
        return result;
    }
    result.has_snapshot = true;

    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(options));

    // Use SchemaManager to load the latest schema for field/partition info
    SchemaManager schema_mgr(fs, table_path, BranchManager::DEFAULT_MAIN_BRANCH);
    auto latest_schema_result = schema_mgr.Latest();
    if (!latest_schema_result.ok() || !latest_schema_result.value()) {
        return result;
    }
    auto table_schema = *latest_schema_result.value();

    auto pool = GetDefaultPool();

    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                           core_options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           core_options.CreateGlobalIndexExternalPath());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            table_path, arrow_schema, table_schema->PartitionKeys(),
            core_options.GetPartitionDefaultName(), core_options.GetFileFormat()->Identifier(),
            core_options.DataFilePrefix(), core_options.LegacyPartitionNameEnabled(),
            external_paths, global_index_external_path, core_options.IndexFileInDataFileDir(),
            pool));

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ManifestList> manifest_list,
                           ManifestList::Create(fs, core_options.GetManifestFormat(),
                                                core_options.GetManifestCompression(), path_factory,
                                                core_options.GetCache(), pool));

    std::vector<ManifestFileMeta> manifests;
    PAIMON_RETURN_NOT_OK(manifest_list->ReadDataManifests(*snapshot, &manifests));

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, table_schema->PartitionKeys()));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ManifestFile> manifest_file,
                           ManifestFile::Create(fs, core_options.GetManifestFormat(),
                                                core_options.GetManifestCompression(), path_factory,
                                                core_options.GetManifestTargetFileSize(), pool,
                                                core_options, partition_schema));

    std::vector<ManifestEntry> entries;
    for (const auto& manifest : manifests) {
        PAIMON_RETURN_NOT_OK(
            manifest_file->Read(manifest.FileName(), /*filter=*/nullptr, &entries));
    }

    std::vector<ManifestEntry> merged_entries;
    PAIMON_RETURN_NOT_OK(FileEntry::MergeEntries(entries, &merged_entries));

    for (const auto& entry : merged_entries) {
        if (!(entry.Kind() == FileKind::Add())) {
            continue;
        }
        const auto& file = entry.File();

        // Convert partition BinaryRow to string representation
        std::string partition_key;
        if (entry.Partition().GetFieldCount() > 0) {
            PAIMON_ASSIGN_OR_RAISE(auto partition_values,
                                   path_factory->GeneratePartitionVector(entry.Partition()));
            for (const auto& [key, value] : partition_values) {
                if (!partition_key.empty()) {
                    partition_key += "/";
                }
                partition_key += key + "=" + value;
            }
        }

        auto& stats = result.by_partition[partition_key];
        stats.record_count += file->row_count;
        stats.file_size_in_bytes += file->file_size;
        stats.file_count++;
        int64_t creation_millis = file->creation_time.GetMillisecond();
        if (creation_millis > stats.last_file_creation_time_millis) {
            stats.last_file_creation_time_millis = creation_millis;
        }
    }

    return result;
}

}  // namespace

// =============================================================================
// GlobalSystemTableLoader
// =============================================================================

Result<bool> GlobalSystemTableLoader::IsSupported(
    const std::string& table_name, const std::map<std::string, std::string>& catalog_options) {
    std::string normalized = StringUtils::ToLowerCase(table_name);
    for (const auto& entry : GlobalSystemTableRegistry()) {
        if (entry.name == normalized) {
            return IsEnabled(entry, catalog_options);
        }
    }
    return false;
}

Result<std::shared_ptr<SystemTable>> GlobalSystemTableLoader::Load(
    const std::string& table_name, const GlobalSystemTableContext& context) {
    std::string normalized = StringUtils::ToLowerCase(table_name);
    for (const auto& entry : GlobalSystemTableRegistry()) {
        if (entry.name == normalized) {
            PAIMON_ASSIGN_OR_RAISE(bool enabled, IsEnabled(entry, context.catalog_options));
            if (!enabled) {
                return Status::NotExist("global system table is disabled: ", table_name);
            }
            if (entry.requires_catalog && context.catalog == nullptr) {
                return Status::NotImplemented("global system table requires catalog context: ",
                                              table_name);
            }
            return entry.factory(context);
        }
    }
    return Status::NotImplemented("unsupported global system table: ", table_name);
}

Result<std::vector<std::string>> GlobalSystemTableLoader::GetSupportedTableNames(
    const std::map<std::string, std::string>& catalog_options) {
    std::vector<std::string> names;
    names.reserve(GlobalSystemTableRegistry().size());
    for (const auto& entry : GlobalSystemTableRegistry()) {
        PAIMON_ASSIGN_OR_RAISE(bool enabled, IsEnabled(entry, catalog_options));
        if (enabled) {
            names.push_back(entry.name);
        }
    }
    return names;
}

// =============================================================================
// sys.catalog_options
// =============================================================================

CatalogOptionsSystemTable::CatalogOptionsSystemTable(GlobalSystemTableContext context)
    : InMemorySystemTable("sys/catalog_options"), context_(std::move(context)) {}

std::string CatalogOptionsSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> CatalogOptionsSystemTable::ArrowSchema() const {
    return arrow::schema({
        arrow::field("key", arrow::utf8(), /*nullable=*/false),
        arrow::field("value", arrow::utf8(), /*nullable=*/false),
    });
}

Result<std::vector<GenericRow>> CatalogOptionsSystemTable::BuildRows() const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    std::vector<GenericRow> rows;
    rows.reserve(context_.catalog_options.size());
    for (const auto& [key, value] : context_.catalog_options) {
        GenericRow row(schema->num_fields());
        row.SetField(0, StringValue(key));
        row.SetField(1, StringValue(value));
        rows.push_back(std::move(row));
    }
    return rows;
}

// =============================================================================
// sys.all_table_options
// =============================================================================

AllTableOptionsSystemTable::AllTableOptionsSystemTable(GlobalSystemTableContext context)
    : InMemorySystemTable("sys/all_table_options"), context_(std::move(context)) {}

std::string AllTableOptionsSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> AllTableOptionsSystemTable::ArrowSchema() const {
    return arrow::schema({
        arrow::field("database_name", arrow::utf8(), /*nullable=*/false),
        arrow::field("table_name", arrow::utf8(), /*nullable=*/false),
        arrow::field("key", arrow::utf8(), /*nullable=*/false),
        arrow::field("value", arrow::utf8(), /*nullable=*/false),
    });
}

Result<std::vector<GenericRow>> AllTableOptionsSystemTable::BuildRows() const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    std::vector<GenericRow> rows;

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> databases, context_.catalog->ListDatabases());
    for (const auto& db : databases) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> tables, context_.catalog->ListTables(db));
        for (const auto& table : tables) {
            Identifier id(db, table);
            auto schema_result = context_.catalog->LoadTableSchema(id);
            if (!schema_result.ok()) {
                continue;  // skip tables with errors (e.g. dropped concurrently)
            }
            auto schema_ptr = schema_result.value();
            auto data_schema = std::dynamic_pointer_cast<DataSchema>(schema_ptr);
            if (!data_schema) {
                continue;
            }
            for (const auto& [key, value] : data_schema->Options()) {
                GenericRow row(schema->num_fields());
                row.SetField(0, StringValue(db));
                row.SetField(1, StringValue(table));
                row.SetField(2, StringValue(key));
                row.SetField(3, StringValue(value));
                rows.push_back(std::move(row));
            }
        }
    }
    return rows;
}

// =============================================================================
// sys.tables
// =============================================================================

TablesSystemTable::TablesSystemTable(GlobalSystemTableContext context)
    : InMemorySystemTable("sys/tables"), context_(std::move(context)) {}

std::string TablesSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> TablesSystemTable::ArrowSchema() const {
    return arrow::schema({
        arrow::field("database_name", arrow::utf8(), /*nullable=*/false),
        arrow::field("table_name", arrow::utf8(), /*nullable=*/false),
        arrow::field("table_type", arrow::utf8(), /*nullable=*/false),
        arrow::field("partitioned", arrow::boolean(), /*nullable=*/false),
        arrow::field("primary_key", arrow::boolean(), /*nullable=*/false),
        arrow::field("owner", arrow::utf8(), /*nullable=*/true),
        arrow::field("created_at", arrow::int64(), /*nullable=*/true),
        arrow::field("created_by", arrow::utf8(), /*nullable=*/true),
        arrow::field("updated_at", arrow::int64(), /*nullable=*/true),
        arrow::field("updated_by", arrow::utf8(), /*nullable=*/true),
        arrow::field("record_count", arrow::int64(), /*nullable=*/true),
        arrow::field("file_size_in_bytes", arrow::int64(), /*nullable=*/true),
        arrow::field("file_count", arrow::int64(), /*nullable=*/true),
        arrow::field("last_file_creation_time", arrow::int64(), /*nullable=*/true),
    });
}

Result<std::vector<GenericRow>> TablesSystemTable::BuildRows() const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    std::vector<GenericRow> rows;

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> databases, context_.catalog->ListDatabases());
    for (const auto& db : databases) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> tables, context_.catalog->ListTables(db));
        for (const auto& table : tables) {
            Identifier id(db, table);
            auto schema_result = context_.catalog->LoadTableSchema(id);
            if (!schema_result.ok()) {
                continue;
            }
            auto schema_ptr = schema_result.value();
            auto data_schema = std::dynamic_pointer_cast<DataSchema>(schema_ptr);
            if (!data_schema) {
                continue;
            }

            const auto& opts = data_schema->Options();
            auto table_type = opts.find("type");
            const std::string table_type_str =
                table_type == opts.end() ? "TABLE" : table_type->second;

            bool partitioned = !data_schema->PartitionKeys().empty();

            GenericRow row(schema->num_fields());
            row.SetField(0, StringValue(db));
            row.SetField(1, StringValue(table));
            row.SetField(2, StringValue(table_type_str));
            row.SetField(3, partitioned);
            row.SetField(4, !data_schema->PrimaryKeys().empty());
            row.SetField(5, OptionalStringValue(opts, "owner"));
            PAIMON_ASSIGN_OR_RAISE(VariantType created_at, OptionalLongValue(opts, "createdAt"));
            row.SetField(6, std::move(created_at));
            row.SetField(7, OptionalStringValue(opts, "createdBy"));
            PAIMON_ASSIGN_OR_RAISE(VariantType updated_at, OptionalLongValue(opts, "updatedAt"));
            row.SetField(8, std::move(updated_at));
            row.SetField(9, OptionalStringValue(opts, "updatedBy"));

            // Get table path and aggregate file stats from manifest entries
            PAIMON_ASSIGN_OR_RAISE(std::string table_path, context_.catalog->GetTableLocation(id));

            auto file_stats_result =
                AggregateFileStats(context_.fs, table_path, data_schema->Options());
            if (file_stats_result.ok() && file_stats_result.value().has_snapshot) {
                auto& all_stats = file_stats_result.value().by_partition;
                int64_t total_record = 0, total_size = 0, total_files = 0, max_creation = 0;
                for (const auto& [key, stats] : all_stats) {
                    total_record += stats.record_count;
                    total_size += stats.file_size_in_bytes;
                    total_files += stats.file_count;
                    if (stats.last_file_creation_time_millis > max_creation) {
                        max_creation = stats.last_file_creation_time_millis;
                    }
                }
                row.SetField(10, VariantType(total_record));
                row.SetField(11, VariantType(total_size));
                row.SetField(12, VariantType(total_files));
                row.SetField(
                    13, max_creation > 0 ? VariantType(max_creation) : VariantType(NullType()));
            } else {
                row.SetField(10, NullType());
                row.SetField(11, NullType());
                row.SetField(12, NullType());
                row.SetField(13, NullType());
            }

            rows.push_back(std::move(row));
        }
    }
    return rows;
}

// =============================================================================
// sys.partitions
// =============================================================================

PartitionsSystemTable::PartitionsSystemTable(GlobalSystemTableContext context)
    : InMemorySystemTable("sys/partitions"), context_(std::move(context)) {}

std::string PartitionsSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> PartitionsSystemTable::ArrowSchema() const {
    return arrow::schema({
        arrow::field("database_name", arrow::utf8(), /*nullable=*/false),
        arrow::field("table_name", arrow::utf8(), /*nullable=*/false),
        arrow::field("partition_name", arrow::utf8(), /*nullable=*/true),
        arrow::field("record_count", arrow::int64(), /*nullable=*/true),
        arrow::field("file_size_in_bytes", arrow::int64(), /*nullable=*/true),
        arrow::field("file_count", arrow::int64(), /*nullable=*/true),
        arrow::field("last_file_creation_time", arrow::int64(), /*nullable=*/true),
        arrow::field("done", arrow::boolean(), /*nullable=*/false),
    });
}

Result<std::vector<GenericRow>> PartitionsSystemTable::BuildRows() const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    std::vector<GenericRow> rows;

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> databases, context_.catalog->ListDatabases());
    for (const auto& db : databases) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> tables, context_.catalog->ListTables(db));
        for (const auto& table : tables) {
            Identifier id(db, table);
            auto schema_result = context_.catalog->LoadTableSchema(id);
            if (!schema_result.ok()) {
                continue;
            }
            auto schema_ptr = schema_result.value();
            auto data_schema = std::dynamic_pointer_cast<DataSchema>(schema_ptr);
            if (!data_schema) {
                continue;
            }

            // Only emit rows for partitioned tables
            if (data_schema->PartitionKeys().empty()) {
                continue;
            }

            // Get table path and aggregate file stats by partition
            auto table_path_result = context_.catalog->GetTableLocation(id);
            if (!table_path_result.ok()) {
                continue;
            }
            std::string table_path = table_path_result.value();

            auto file_stats_result =
                AggregateFileStats(context_.fs, table_path, data_schema->Options());
            if (!file_stats_result.ok()) {
                continue;
            }

            auto& stats_map = file_stats_result.value().by_partition;
            for (const auto& [partition_key, stats] : stats_map) {
                if (stats.file_count == 0) {
                    continue;
                }
                GenericRow row(schema->num_fields());
                row.SetField(0, StringValue(db));
                row.SetField(1, StringValue(table));
                row.SetField(2, partition_key.empty() ? VariantType(NullType())
                                                      : VariantType(StringValue(partition_key)));
                row.SetField(3, VariantType(stats.record_count));
                row.SetField(4, VariantType(stats.file_size_in_bytes));
                row.SetField(5, VariantType(stats.file_count));
                row.SetField(6, stats.last_file_creation_time_millis > 0
                                    ? VariantType(stats.last_file_creation_time_millis)
                                    : VariantType(NullType()));
                // File-system catalog partitions are not explicitly marked as done.
                row.SetField(7, false);
                rows.push_back(std::move(row));
            }
        }
    }
    return rows;
}

}  // namespace paimon
