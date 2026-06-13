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
#include "paimon/common/utils/string_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/core_options.h"
#include "paimon/defs.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/snapshot_manager.h"
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

            // Try to get stats from latest snapshot
            PAIMON_ASSIGN_OR_RAISE(std::string table_path,
                                   context_.catalog->GetTableLocation(id));
            SnapshotManager snapshot_manager(context_.fs, table_path,
                                             BranchManager::DEFAULT_MAIN_BRANCH);
            auto snapshot_result = snapshot_manager.LatestSnapshot();
            if (snapshot_result.ok() && snapshot_result.value()) {
                const auto& snapshot = *snapshot_result.value();
                auto total_count = snapshot.TotalRecordCount();
                row.SetField(5, total_count ? VariantType(total_count.value())
                                            : VariantType(NullType()));
                // TODO(suxiaogang223): Populate file_size_in_bytes, file_count, and
                // last_file_creation_time by reading manifest entries. This requires
                // the manifest reading infrastructure from the files/manifests system
                // tables PR (codex/system-table-files-manifests-pr4).
                row.SetField(6, NullType());
                row.SetField(7, NullType());
                row.SetField(8, NullType());
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

    // TODO(suxiaogang223): Implement partition-level aggregation using
    // manifest entry reading (similar to FilesSystemTable::BuildRows()
    // but grouped by partition). For now, return empty result set.
    //
    // The implementation should:
    // 1. Enumerate all databases and tables
    // 2. For each partitioned table, read latest snapshot's manifest entries
    // 3. Group DataFileMeta entries by entry.Partition()
    // 4. Aggregate: sum(file_size), sum(record_count), count files,
    //    max(creation_time)

    return rows;
}

}  // namespace paimon
