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

#include <string_view>
#include <utility>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/internal_row_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_entry.h"
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
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"

namespace paimon {
namespace {

constexpr int32_t kMaxPartitionStatsLength = 255;

}  // namespace

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

std::map<std::string, std::string> SystemTableUtils::MergeOptions(
    const std::shared_ptr<TableSchema>& table_schema,
    const std::map<std::string, std::string>& dynamic_options) {
    auto options = table_schema->Options();
    for (const auto& [key, value] : dynamic_options) {
        options[key] = value;
    }
    return options;
}

std::string SystemTableUtils::DefaultBranch() {
    return BranchManager::DEFAULT_MAIN_BRANCH;
}

std::string SystemTableUtils::LoadBranch(const std::map<std::string, std::string>& options) {
    auto branch_iter = options.find(Options::BRANCH);
    return branch_iter == options.end() ? DefaultBranch() : branch_iter->second;
}

Result<int64_t> SystemTableUtils::LocalDateTimePartsToTimestampMillis(
    const std::vector<int64_t>& parts) {
    if (parts.size() < 6) {
        return Status::Invalid("tag create time requires at least 6 date-time fields");
    }

    int64_t year = parts[0];
    int64_t month = parts[1];
    int64_t day = parts[2];
    int64_t hour = parts[3];
    int64_t minute = parts[4];
    int64_t second = parts[5];
    int64_t nanos = parts.size() > 6 ? parts[6] : 0;
    auto is_leap_year = [](int64_t value) {
        return value % 4 == 0 && (value % 100 != 0 || value % 400 == 0);
    };
    int64_t days_in_month[] = {31, is_leap_year(year) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30,
                               31};
    if (month < 1 || month > 12 || day < 1 || day > days_in_month[month - 1] || hour < 0 ||
        hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59 || nanos < 0 ||
        nanos > 999999999) {
        return Status::Invalid("invalid tag create time fields");
    }

    year -= month <= 2 ? 1 : 0;
    int64_t era = (year >= 0 ? year : year - 399) / 400;
    auto year_of_era = static_cast<uint32_t>(year - era * 400);
    auto month_prime = static_cast<uint32_t>(month + (month > 2 ? -3 : 9));
    uint32_t day_of_year = (153 * month_prime + 2) / 5 + static_cast<uint32_t>(day) - 1;
    uint32_t day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    int64_t epoch_day = era * 146097 + static_cast<int64_t>(day_of_era) - 719468;
    return epoch_day * DateTimeUtils::MILLIS_PER_DAY + hour * 3600000 + minute * 60000 +
           second * 1000 + nanos / 1000000;
}

Result<std::optional<int64_t>> SystemTableUtils::OptionalLocalDateTimePartsToTimestampMillis(
    const std::optional<std::vector<int64_t>>& parts) {
    if (!parts) {
        return std::optional<int64_t>();
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t timestamp_millis,
                           LocalDateTimePartsToTimestampMillis(parts.value()));
    return std::optional<int64_t>(timestamp_millis);
}

std::optional<std::string> SystemTableUtils::OptionalDoubleToString(
    const std::optional<double_t>& value) {
    if (!value) {
        return std::optional<std::string>();
    }
    return std::to_string(value.value());
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

VariantType SystemTableUtils::TimestampMillisValue(int64_t value) {
    return Timestamp::FromEpochMillis(value);
}

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

VariantType SystemTableUtils::OptionalTimestampMillisValue(const std::optional<int64_t>& value) {
    if (!value) {
        return NullType();
    }
    return TimestampMillisValue(value.value());
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

Result<std::vector<ManifestEntry>> SystemTableUtils::ReadLatestManifestEntries(
    const SystemTableContext& context, const std::shared_ptr<FileStorePathFactory>& path_factory,
    const CoreOptions& core_options, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot, LatestSnapshot(context));
    if (!snapshot) {
        return std::vector<ManifestEntry>();
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<ManifestFileMeta> manifests,
        ReadDataManifests(context, snapshot.value(), path_factory, core_options, pool));
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

Result<std::vector<ManifestEntry>> SystemTableUtils::ReadLatestDataFiles(
    const SystemTableContext& context, const std::shared_ptr<FileStorePathFactory>& path_factory,
    const CoreOptions& core_options, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestEntry> entries,
                           ReadLatestManifestEntries(context, path_factory, core_options, pool));
    std::vector<ManifestEntry> merged_entries;
    PAIMON_RETURN_NOT_OK(FileEntry::MergeEntries(entries, &merged_entries));
    return merged_entries;
}

std::optional<std::string> SystemTableUtils::OptionalBinaryRowString(const BinaryRow& row) {
    if (row.GetFieldCount() <= 0) {
        return std::nullopt;
    }
    return row.ToString();
}

Result<std::optional<std::string>> SystemTableUtils::OptionalPartitionString(
    const BinaryRow& row, const std::shared_ptr<arrow::Schema>& partition_schema) {
    if (row.GetFieldCount() <= 0) {
        return std::optional<std::string>();
    }
    PAIMON_ASSIGN_OR_RAISE(std::string value,
                           BinaryRowPartitionComputer::PartToSimpleString(
                               partition_schema, row, ",", kMaxPartitionStatsLength));
    return std::optional<std::string>(value);
}

Result<VariantType> SystemTableUtils::OptionalPartitionStringValue(
    const BinaryRow& row, const std::shared_ptr<arrow::Schema>& partition_schema) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> value,
                           OptionalPartitionString(row, partition_schema));
    return OptionalStringValue(value);
}

Result<std::string> SystemTableUtils::PartitionString(
    const std::shared_ptr<FileStorePathFactory>& path_factory, const BinaryRow& partition) {
    PAIMON_ASSIGN_OR_RAISE(std::string value, path_factory->GetPartitionString(partition));
    return value;
}

Result<std::string> SystemTableUtils::FilePath(
    const std::shared_ptr<FileStorePathFactory>& path_factory, const ManifestEntry& entry,
    const DataFileMeta& file) {
    if (file.external_path) {
        return file.external_path.value();
    }
    PAIMON_ASSIGN_OR_RAISE(std::string bucket_path,
                           path_factory->BucketPath(entry.Partition(), entry.Bucket()));
    return PathUtil::JoinPath(bucket_path, file.file_name);
}

Result<std::string> SystemTableUtils::FieldsValueMapString(const std::vector<DataField>& fields,
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

Result<std::string> SystemTableUtils::NullValueCountsString(const std::vector<DataField>& fields,
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

Result<std::shared_ptr<TableSchema>> SystemTableUtils::LoadDataSchema(
    const SystemTableContext& context, int64_t schema_id) {
    if (schema_id == context.table_schema->Id()) {
        return context.table_schema;
    }
    SchemaManager schema_manager(context.fs, context.table_path, context.branch);
    return schema_manager.ReadSchema(schema_id);
}

Result<std::vector<DataField>> SystemTableUtils::ValueStatsFields(const SystemTableContext& context,
                                                                  int64_t schema_id) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> data_schema,
                           LoadDataSchema(context, schema_id));
    return data_schema->Fields();
}

Result<std::shared_ptr<InternalArray>> SystemTableUtils::WriteColsValue(
    const std::optional<std::vector<std::string>>& write_cols,
    const std::shared_ptr<MemoryPool>& pool) {
    if (!write_cols) {
        return std::shared_ptr<InternalArray>();
    }
    return std::make_shared<BinaryArray>(
        InternalRowUtils::ToNotNullStringArrayData(write_cols.value(), pool));
}

}  // namespace paimon
