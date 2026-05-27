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

#include "paimon/core/table/system/manifests_system_table.h"

#include <utility>

#include "arrow/api.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/core_options.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {

ManifestsSystemTable::ManifestsSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                           std::string branch,
                                           std::shared_ptr<TableSchema> table_schema,
                                           std::map<std::string, std::string> options)
    : InMemorySystemTable(table_path),
      context_(SystemTableUtils::CreateContext(std::move(fs), std::move(table_path),
                                               std::move(branch), std::move(table_schema),
                                               std::move(options))) {}

std::string ManifestsSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> ManifestsSystemTable::ArrowSchema() const {
    return arrow::schema({
        arrow::field("file_name", arrow::utf8(), /*nullable=*/false),
        arrow::field("file_size", arrow::int64(), /*nullable=*/false),
        arrow::field("num_added_files", arrow::int64(), /*nullable=*/false),
        arrow::field("num_deleted_files", arrow::int64(), /*nullable=*/false),
        arrow::field("schema_id", arrow::int64(), /*nullable=*/false),
        arrow::field("min_partition_stats", arrow::utf8(), /*nullable=*/true),
        arrow::field("max_partition_stats", arrow::utf8(), /*nullable=*/true),
        arrow::field("min_row_id", arrow::int64(), /*nullable=*/true),
        arrow::field("max_row_id", arrow::int64(), /*nullable=*/true),
    });
}

Result<std::vector<GenericRow>> ManifestsSystemTable::BuildRows() const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot,
                           SystemTableUtils::LatestSnapshot(context_));
    if (!snapshot) {
        return std::vector<GenericRow>();
    }

    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, SystemTableUtils::CreateCoreOptions(context_));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStorePathFactory> path_factory,
                           SystemTableUtils::CreatePathFactory(context_, core_options, pool));
    PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestFileMeta> manifests,
                           SystemTableUtils::ReadDataManifests(context_, snapshot.value(),
                                                               path_factory, core_options, pool));
    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(context_.table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, context_.table_schema->PartitionKeys()));

    std::vector<GenericRow> rows;
    rows.reserve(manifests.size());
    for (const auto& manifest : manifests) {
        GenericRow row(schema->num_fields());
        row.SetField(0, SystemTableUtils::StringValue(manifest.FileName()));
        row.SetField(1, manifest.FileSize());
        row.SetField(2, manifest.NumAddedFiles());
        row.SetField(3, manifest.NumDeletedFiles());
        row.SetField(4, manifest.SchemaId());
        PAIMON_ASSIGN_OR_RAISE(VariantType min_partition,
                               SystemTableUtils::OptionalPartitionStringValue(
                                   manifest.PartitionStats().MinValues(), partition_schema));
        PAIMON_ASSIGN_OR_RAISE(VariantType max_partition,
                               SystemTableUtils::OptionalPartitionStringValue(
                                   manifest.PartitionStats().MaxValues(), partition_schema));
        row.SetField(5, min_partition);
        row.SetField(6, max_partition);
        row.SetField(7, SystemTableUtils::OptionalInt64Value(manifest.MinRowId()));
        row.SetField(8, SystemTableUtils::OptionalInt64Value(manifest.MaxRowId()));
        rows.push_back(std::move(row));
    }
    return rows;
}

}  // namespace paimon
