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

#include <ctime>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/core_options.h"
#include "paimon/defs.h"
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
#include "paimon/data/timestamp.h"
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
    GlobalSystemTableFactory factory;
};

const std::vector<GlobalSystemTableRegistryEntry>& GlobalSystemTableRegistry() {
    static const std::vector<GlobalSystemTableRegistryEntry> registry = {
        {CatalogOptionsSystemTable::kName,
         [](const GlobalSystemTableContext& ctx) -> Result<std::shared_ptr<SystemTable>> {
             return std::make_shared<CatalogOptionsSystemTable>(ctx);
         }},
        {AllTableOptionsSystemTable::kName,
         [](const GlobalSystemTableContext& ctx) -> Result<std::shared_ptr<SystemTable>> {
             return std::make_shared<AllTableOptionsSystemTable>(ctx);
         }},
        {TablesSystemTable::kName,
         [](const GlobalSystemTableContext& ctx) -> Result<std::shared_ptr<SystemTable>> {
             return std::make_shared<TablesSystemTable>(ctx);
         }},
        {PartitionsSystemTable::kName,
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

// Aggregated file-level statistics for a table or partition.
struct FileStats {
    int64_t record_count = 0;
    int64_t file_size_in_bytes = 0;
    int64_t file_count = 0;
    int64_t last_file_creation_time_millis = 0;
};

// Read the latest snapshot's data files and aggregate statistics.
// Returns an empty map if no snapshot or no data files exist.
Result<std::map<std::string, FileStats>> AggregateFileStats(
    const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
    const std::map<std::string, std::string>& options) {
    std::map<std::string, FileStats> result;

    SnapshotManager snapshot_manager(fs, table_path,
                                     BranchManager::DEFAULT_MAIN_BRANCH);
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot,
                           snapshot_manager.LatestSnapshot());
    if (!snapshot) {
        return result;
    }

    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(options));

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
            core_options.GetPartitionDefaultName(),
            core_options.GetFileFormat()->Identifier(),
            core_options.DataFilePrefix(),
            core_options.LegacyPartitionNameEnabled(), external_paths,
            global_index_external_path, core_options.IndexFileInDataFileDir(), pool));

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<ManifestList> manifest_list,
        ManifestList::Create(fs, core_options.GetManifestFormat(),
                             core_options.GetManifestCompression(), path_factory,
                             core_options.GetCache(), pool));

    std::vector<ManifestFileMeta> manifests;
    PAIMON_RETURN_NOT_OK(
        manifest_list->ReadDataManifests(*snapshot, &manifests));

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, table_schema->PartitionKeys()));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<ManifestFile> manifest_file,
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
            PAIMON_ASSIGN_OR_RAISE(
                partition_key,
                BinaryRowPartitionComputer::PartToSimpleString(
                    partition_schema, entry.Partition(), ",",
                    /*max_length=*/255,
                    /*legacy_partition_name_enabled=*/false));
            partition_key = "{" + partition_key + "}";
        }

        auto& stats = result[partition_key];
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

bool GlobalSystemTableLoader::IsSupported(const std::string& table_name) {
    std::string normalized = StringUtils::ToLowerCase(table_name);
    for (const auto& entry : GlobalSystemTableRegistry()) {
        if (entry.name == normalized) {
            return true;
        }
    }
    return false;
}

Result<std::shared_ptr<SystemTable>> GlobalSystemTableLoader::Load(
    const std::string& table_name, const GlobalSystemTableContext& context) {
    std::string normalized = StringUtils::ToLowerCase(table_name);
    for (const auto& entry : GlobalSystemTableRegistry()) {
        if (entry.name == normalized) {
            return entry.factory(context);
        }
    }
    return Status::NotImplemented("unsupported global system table: ", table_name);
}

std::vector<std::string> GlobalSystemTableLoader::GetSupportedTableNames() {
    std::vector<std::string> names;
    names.reserve(GlobalSystemTableRegistry().size());
    for (const auto& entry : GlobalSystemTableRegistry()) {
        names.push_back(entry.name);
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
        row.SetField(0, std::string_view(key));
        row.SetField(1, std::string_view(value));
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

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> databases,
                           context_.catalog->ListDatabases());
    for (const auto& db : databases) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> tables,
                               context_.catalog->ListTables(db));
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
                row.SetField(0, std::string_view(db));
                row.SetField(1, std::string_view(table));
                row.SetField(2, std::string_view(key));
                row.SetField(3, std::string_view(value));
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
        arrow::field("primary_key", arrow::utf8(), /*nullable=*/false),
        arrow::field("record_count", arrow::int64(), /*nullable=*/true),
        arrow::field("file_size_in_bytes", arrow::int64(), /*nullable=*/true),
        arrow::field("file_count", arrow::int64(), /*nullable=*/true),
        arrow::field("last_file_creation_time",
                     arrow::timestamp(arrow::TimeUnit::MILLI), /*nullable=*/true),
    });
}

Result<std::vector<GenericRow>> TablesSystemTable::BuildRows() const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    std::vector<GenericRow> rows;

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> databases,
                           context_.catalog->ListDatabases());
    for (const auto& db : databases) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> tables,
                               context_.catalog->ListTables(db));
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

            // Determine table type: EXTERNAL if data-file.external-paths is set
            std::string table_type_str = "MANAGED";
            const auto& opts = data_schema->Options();
            if (opts.find(Options::DATA_FILE_EXTERNAL_PATHS) != opts.end()) {
                table_type_str = "EXTERNAL";
            }

            bool partitioned = !data_schema->PartitionKeys().empty();
            std::string primary_keys_str;
            const auto& pks = data_schema->PrimaryKeys();
            for (size_t i = 0; i < pks.size(); ++i) {
                if (i > 0) primary_keys_str += ",";
                primary_keys_str += pks[i];
            }

            GenericRow row(schema->num_fields());
            row.SetField(0, std::string_view(db));
            row.SetField(1, std::string_view(table));
            row.SetField(2, StringValue(table_type_str));
            row.SetField(3, partitioned);
            row.SetField(4, primary_keys_str.empty()
                                ? VariantType(NullType())
                                : VariantType(StringValue(primary_keys_str)));

            // Get table path and aggregate file stats from manifest entries
            PAIMON_ASSIGN_OR_RAISE(std::string table_path,
                                   context_.catalog->GetTableLocation(id));

            auto file_stats_result =
                AggregateFileStats(context_.fs, table_path, data_schema->Options());
            if (file_stats_result.ok()) {
                auto& all_stats = file_stats_result.value();
                int64_t total_record = 0, total_size = 0, total_files = 0,
                        max_creation = 0;
                for (const auto& [key, stats] : all_stats) {
                    total_record += stats.record_count;
                    total_size += stats.file_size_in_bytes;
                    total_files += stats.file_count;
                    if (stats.last_file_creation_time_millis > max_creation) {
                        max_creation = stats.last_file_creation_time_millis;
                    }
                }
                row.SetField(5, VariantType(total_record));
                row.SetField(6, VariantType(total_size));
                row.SetField(7, VariantType(total_files));
                row.SetField(8, max_creation > 0
                                    ? VariantType(Timestamp::FromEpochMillis(max_creation))
                                    : VariantType(NullType()));
            } else {
                row.SetField(5, NullType());
                row.SetField(6, NullType());
                row.SetField(7, NullType());
                row.SetField(8, NullType());
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
        arrow::field("last_update_time",
                     arrow::timestamp(arrow::TimeUnit::MILLI), /*nullable=*/true),
    });
}

Result<std::vector<GenericRow>> PartitionsSystemTable::BuildRows() const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    std::vector<GenericRow> rows;

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> databases,
                           context_.catalog->ListDatabases());
    for (const auto& db : databases) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> tables,
                               context_.catalog->ListTables(db));
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

            auto& stats_map = file_stats_result.value();
            for (const auto& [partition_key, stats] : stats_map) {
                if (stats.file_count == 0) {
                    continue;
                }
                GenericRow row(schema->num_fields());
                row.SetField(0, std::string_view(db));
                row.SetField(1, std::string_view(table));
                row.SetField(2, partition_key.empty()
                                    ? VariantType(NullType())
                                    : VariantType(StringValue(partition_key)));
                row.SetField(3, VariantType(stats.record_count));
                row.SetField(4, VariantType(stats.file_size_in_bytes));
                row.SetField(5, VariantType(stats.file_count));
                row.SetField(6, stats.last_file_creation_time_millis > 0
                                    ? VariantType(Timestamp::FromEpochMillis(
                                          stats.last_file_creation_time_millis))
                                    : VariantType(NullType()));
                rows.push_back(std::move(row));
            }
        }
    }
    return rows;
}

}  // namespace paimon
