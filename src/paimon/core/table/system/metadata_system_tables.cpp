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

#include "paimon/core/table/system/metadata_system_tables.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/common/utils/internal_row_utils.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/core/casting/cast_executor_factory.h"
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
#include "paimon/core/stats/simple_stats_evolution.h"
#include "paimon/core/tag/tag.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/consumer_manager.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/core/utils/tag_manager.h"
#include "paimon/data/timestamp.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace paimon {
namespace {

constexpr int32_t kMaxPartitionStatsLength = 255;

template <typename T>
Result<std::string> JsonString(const T& value) {
    rapidjson::Document document;
    auto json_value = RapidJsonUtil::SerializeValue(value, &document.GetAllocator());
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    if (!json_value.Accept(writer)) {
        return Status::Invalid("failed to serialize metadata system table value");
    }
    return std::string(buffer.GetString(), buffer.GetSize());
}

Result<int64_t> LocalDateTimePartsToTimestampMillis(const std::vector<int64_t>& parts) {
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

Result<std::optional<int64_t>> OptionalLocalDateTimePartsToTimestampMillis(
    const std::optional<std::vector<int64_t>>& parts) {
    if (!parts) {
        return std::optional<int64_t>();
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t timestamp_millis,
                           LocalDateTimePartsToTimestampMillis(parts.value()));
    return std::optional<int64_t>(timestamp_millis);
}

std::optional<std::string> OptionalDoubleToString(const std::optional<double_t>& value) {
    if (!value) {
        return std::optional<std::string>();
    }
    return std::to_string(value.value());
}

VariantType OptionalInt64Value(const std::optional<int64_t>& value) {
    if (!value) {
        return NullType();
    }
    return value.value();
}

VariantType StringValue(const std::string& value) {
    return BinaryString::FromString(value, GetDefaultPool().get());
}

VariantType OptionalStringValue(const std::optional<std::string>& value) {
    if (!value) {
        return NullType();
    }
    return StringValue(value.value());
}

VariantType TimestampMillisValue(int64_t value) {
    return Timestamp::FromEpochMillis(value);
}

Result<VariantType> LocalTimestampMillisValue(int64_t epoch_millis) {
    PAIMON_ASSIGN_OR_RAISE(
        Timestamp local_timestamp,
        DateTimeUtils::ToLocalTimestamp(Timestamp::FromEpochMillis(epoch_millis)));
    return TimestampMillisValue(local_timestamp.GetMillisecond());
}

VariantType OptionalTimestampMillisValue(const std::optional<int64_t>& value) {
    if (!value) {
        return NullType();
    }
    return TimestampMillisValue(value.value());
}

MetadataSystemTableContext CreateMetadataContext(std::shared_ptr<FileSystem> fs,
                                                 std::string table_path, std::string branch) {
    return {
        std::move(fs), std::move(table_path), BranchManager::NormalizeBranch(branch), nullptr, {},
    };
}

MetadataSystemTableContext CreateMetadataContext(std::shared_ptr<FileSystem> fs,
                                                 std::string table_path, std::string branch,
                                                 std::shared_ptr<TableSchema> table_schema,
                                                 std::map<std::string, std::string> options) {
    return {
        std::move(fs),           std::move(table_path), BranchManager::NormalizeBranch(branch),
        std::move(table_schema), std::move(options),
    };
}

Result<CoreOptions> CreateCoreOptions(const MetadataSystemTableContext& context) {
    return CoreOptions::FromMap(context.options, context.fs);
}

Result<std::shared_ptr<FileStorePathFactory>> CreatePathFactory(
    const MetadataSystemTableContext& context, const CoreOptions& core_options,
    const std::shared_ptr<MemoryPool>& pool) {
    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(context.table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                           core_options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           core_options.CreateGlobalIndexExternalPath());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            context.table_path, arrow_schema, context.table_schema->PartitionKeys(),
            core_options.GetPartitionDefaultName(), core_options.GetFileFormat()->Identifier(),
            core_options.DataFilePrefix(), core_options.LegacyPartitionNameEnabled(),
            external_paths, global_index_external_path, core_options.IndexFileInDataFileDir(),
            pool));
    return path_factory;
}

Result<std::optional<Snapshot>> LatestSnapshot(const MetadataSystemTableContext& context) {
    SnapshotManager snapshot_manager(context.fs, context.table_path, context.branch);
    return snapshot_manager.LatestSnapshot();
}

Result<std::vector<ManifestFileMeta>> ReadDataManifests(
    const MetadataSystemTableContext& context, const Snapshot& snapshot,
    const std::shared_ptr<FileStorePathFactory>& path_factory, const CoreOptions& core_options,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ManifestList> manifest_list,
                           ManifestList::Create(context.fs, core_options.GetManifestFormat(),
                                                core_options.GetManifestCompression(), path_factory,
                                                core_options.GetCache(), pool));
    std::vector<ManifestFileMeta> manifests;
    // TODO(suxiaogang223): Align Java ReadAllManifests semantics by including changelog
    // manifests. ReadAllManifests currently delegates to ReadChangelogManifests, which returns
    // NotImplemented when a snapshot has a changelog manifest list.
    PAIMON_RETURN_NOT_OK(manifest_list->ReadDataManifests(snapshot, &manifests));
    return manifests;
}

Result<std::unique_ptr<ManifestFile>> CreateManifestFile(
    const MetadataSystemTableContext& context,
    const std::shared_ptr<FileStorePathFactory>& path_factory, const CoreOptions& core_options,
    const std::shared_ptr<MemoryPool>& pool) {
    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(context.table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, context.table_schema->PartitionKeys()));
    return ManifestFile::Create(context.fs, core_options.GetManifestFormat(),
                                core_options.GetManifestCompression(), path_factory,
                                core_options.GetManifestTargetFileSize(), pool, core_options,
                                partition_schema);
}

Result<std::vector<ManifestEntry>> ReadLatestManifestEntries(
    const MetadataSystemTableContext& context,
    const std::shared_ptr<FileStorePathFactory>& path_factory, const CoreOptions& core_options,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot, LatestSnapshot(context));
    if (!snapshot) {
        return std::vector<ManifestEntry>();
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<ManifestFileMeta> manifests,
        ReadDataManifests(context, snapshot.value(), path_factory, core_options, pool));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ManifestFile> manifest_file,
                           CreateManifestFile(context, path_factory, core_options, pool));
    std::vector<ManifestEntry> entries;
    for (const auto& manifest : manifests) {
        PAIMON_RETURN_NOT_OK(
            manifest_file->Read(manifest.FileName(), /*filter=*/nullptr, &entries));
    }
    return entries;
}

Result<std::vector<ManifestEntry>> ReadLatestDataFiles(
    const MetadataSystemTableContext& context,
    const std::shared_ptr<FileStorePathFactory>& path_factory, const CoreOptions& core_options,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestEntry> entries,
                           ReadLatestManifestEntries(context, path_factory, core_options, pool));
    std::vector<ManifestEntry> merged_entries;
    PAIMON_RETURN_NOT_OK(FileEntry::MergeEntries(entries, &merged_entries));
    return merged_entries;
}

Result<std::optional<std::string>> OptionalPartitionString(
    const BinaryRow& row, const std::shared_ptr<arrow::Schema>& partition_schema) {
    if (row.GetFieldCount() <= 0) {
        return std::optional<std::string>();
    }
    PAIMON_ASSIGN_OR_RAISE(std::string value,
                           BinaryRowPartitionComputer::PartToSimpleString(
                               partition_schema, row, ",", kMaxPartitionStatsLength,
                               /*legacy_partition_name_enabled=*/false));
    return std::optional<std::string>(fmt::format("{{{}}}", value));
}

Result<VariantType> OptionalPartitionStringValue(
    const BinaryRow& row, const std::shared_ptr<arrow::Schema>& partition_schema) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> value,
                           OptionalPartitionString(row, partition_schema));
    return OptionalStringValue(value);
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

Result<std::string> FieldValueString(const DataField& field, const VariantType& value) {
    PAIMON_ASSIGN_OR_RAISE(FieldType field_type,
                           FieldTypeUtils::ConvertToFieldType(field.Type()->id()));
    std::shared_ptr<CastExecutor> cast_executor =
        CastExecutorFactory::GetCastExecutorFactory()->GetCastExecutor(field_type,
                                                                       FieldType::STRING);
    if (!cast_executor) {
        return DataDefine::VariantValueToString(value);
    }
    PAIMON_ASSIGN_OR_RAISE(Literal literal,
                           DataDefine::VariantValueToLiteral(value, field.Type()->id()));
    PAIMON_ASSIGN_OR_RAISE(Literal string_literal, cast_executor->Cast(literal, arrow::utf8()));
    return string_literal.GetValue<std::string>();
}

Result<std::vector<std::string>> RowValueStrings(const std::vector<DataField>& fields,
                                                 const InternalRow& row) {
    std::shared_ptr<arrow::Schema> schema = DataField::ConvertDataFieldsToArrowSchema(fields);
    PAIMON_ASSIGN_OR_RAISE(std::vector<InternalRow::FieldGetterFunc> getters,
                           InternalRowUtils::CreateFieldGetters(schema, /*use_view=*/false));
    std::vector<std::string> values;
    int32_t length = std::min<int32_t>(static_cast<int32_t>(fields.size()), row.GetFieldCount());
    values.reserve(length);
    for (int32_t i = 0; i < length; ++i) {
        std::string value = "null";
        if (!row.IsNullAt(i)) {
            VariantType field_value = getters[i](row);
            PAIMON_ASSIGN_OR_RAISE(value, FieldValueString(fields[i], field_value));
        }
        values.push_back(std::move(value));
    }
    return values;
}

Result<std::string> RowValuesString(const std::vector<DataField>& fields, const InternalRow& row,
                                    std::string_view left, std::string_view right) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> values, RowValueStrings(fields, row));
    return fmt::format("{}{}{}", left, fmt::join(values, ", "), right);
}

Result<std::optional<std::string>> OptionalRowValuesString(const std::vector<DataField>& fields,
                                                           const InternalRow& row,
                                                           std::string_view left,
                                                           std::string_view right) {
    if (row.GetFieldCount() <= 0) {
        return std::optional<std::string>();
    }
    PAIMON_ASSIGN_OR_RAISE(std::string value, RowValuesString(fields, row, left, right));
    return std::optional<std::string>(value);
}

Result<std::string> FieldsValueMapString(const std::vector<DataField>& fields,
                                         const InternalRow& row) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> values, RowValueStrings(fields, row));
    std::vector<std::pair<std::string, std::string>> field_values;
    size_t length = std::min(fields.size(), values.size());
    field_values.reserve(length);
    for (size_t i = 0; i < length; ++i) {
        field_values.emplace_back(fields[i].Name(), std::move(values[i]));
    }
    std::sort(field_values.begin(), field_values.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

    std::vector<std::string> entries;
    entries.reserve(field_values.size());
    for (const auto& [name, value] : field_values) {
        entries.emplace_back(fmt::format("{}={}", name, value));
    }
    return fmt::format("{{{}}}", fmt::join(entries, ", "));
}

Result<std::string> NullValueCountsString(const std::vector<DataField>& fields,
                                          const InternalArray& null_counts) {
    std::vector<std::pair<std::string, std::string>> field_values;
    int32_t length = std::min<int32_t>(static_cast<int32_t>(fields.size()), null_counts.Size());
    field_values.reserve(length);
    for (int32_t i = 0; i < length; ++i) {
        std::string value =
            null_counts.IsNullAt(i) ? "null" : std::to_string(null_counts.GetLong(i));
        field_values.emplace_back(fields[i].Name(), std::move(value));
    }
    std::sort(field_values.begin(), field_values.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

    std::vector<std::string> entries;
    entries.reserve(field_values.size());
    for (const auto& [name, value] : field_values) {
        entries.emplace_back(fmt::format("{}={}", name, value));
    }
    return fmt::format("{{{}}}", fmt::join(entries, ", "));
}

Result<std::shared_ptr<TableSchema>> LoadDataSchema(const MetadataSystemTableContext& context,
                                                    int64_t schema_id) {
    if (schema_id == context.table_schema->Id()) {
        return context.table_schema;
    }
    SchemaManager schema_manager(context.fs, context.table_path, context.branch);
    return schema_manager.ReadSchema(schema_id);
}

Result<std::vector<DataField>> ProjectWriteFields(const std::shared_ptr<TableSchema>& data_schema,
                                                  const DataFileMeta& file) {
    if (!file.write_cols) {
        return data_schema->Fields();
    }

    std::vector<DataField> fields;
    fields.reserve(file.write_cols->size() + data_schema->PartitionKeys().size());
    for (const auto& write_col : file.write_cols.value()) {
        if (SpecialFields::IsSpecialFieldName(write_col)) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(DataField field, data_schema->GetField(write_col));
        fields.push_back(std::move(field));
    }

    // Partial writes may omit partition columns from write_cols. Keep them in the stats source
    // fields so SimpleStatsEvolution can map partition stats consistently.
    for (const auto& partition_key : data_schema->PartitionKeys()) {
        if (!ObjectUtils::Contains(file.write_cols.value(), partition_key)) {
            PAIMON_ASSIGN_OR_RAISE(DataField field, data_schema->GetField(partition_key));
            fields.push_back(std::move(field));
        }
    }
    return fields;
}

Result<std::vector<DataField>> KeyFieldsForFilesTable(
    const std::shared_ptr<TableSchema>& data_schema) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> key_fields,
                           data_schema->TrimmedPrimaryKeyFields());
    // Java FilesTable falls back to logicalRowType when logicalTrimmedPrimaryKeysType is empty.
    if (key_fields.empty()) {
        return data_schema->Fields();
    }
    return key_fields;
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

OptionsSystemTable::OptionsSystemTable(std::string table_path,
                                       std::shared_ptr<TableSchema> table_schema)
    : InMemorySystemTable(std::move(table_path)), table_schema_(std::move(table_schema)) {}

std::string OptionsSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> OptionsSystemTable::ArrowSchema() const {
    return arrow::schema({arrow::field("key", arrow::utf8(), /*nullable=*/false),
                          arrow::field("value", arrow::utf8(), /*nullable=*/false)});
}

Result<std::vector<GenericRow>> OptionsSystemTable::BuildRows() const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    std::vector<GenericRow> rows;
    rows.reserve(table_schema_->Options().size());
    for (const auto& [key, value] : table_schema_->Options()) {
        GenericRow row(schema->num_fields());
        row.SetField(0, std::string_view(key));
        row.SetField(1, std::string_view(value));
        rows.push_back(std::move(row));
    }
    return rows;
}

SnapshotsSystemTable::SnapshotsSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                           std::string branch)
    : InMemorySystemTable(table_path),
      context_(CreateMetadataContext(std::move(fs), std::move(table_path), std::move(branch))) {}

std::string SnapshotsSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> SnapshotsSystemTable::ArrowSchema() const {
    return arrow::schema({
        arrow::field("snapshot_id", arrow::int64(), /*nullable=*/false),
        arrow::field("schema_id", arrow::int64(), /*nullable=*/false),
        arrow::field("commit_user", arrow::utf8(), /*nullable=*/false),
        arrow::field("commit_identifier", arrow::int64(), /*nullable=*/false),
        arrow::field("commit_kind", arrow::utf8(), /*nullable=*/false),
        arrow::field("commit_time", arrow::timestamp(arrow::TimeUnit::MILLI),
                     /*nullable=*/false),
        arrow::field("base_manifest_list", arrow::utf8(), /*nullable=*/false),
        arrow::field("delta_manifest_list", arrow::utf8(), /*nullable=*/false),
        arrow::field("changelog_manifest_list", arrow::utf8(), /*nullable=*/true),
        arrow::field("total_record_count", arrow::int64(), /*nullable=*/true),
        arrow::field("delta_record_count", arrow::int64(), /*nullable=*/true),
        arrow::field("changelog_record_count", arrow::int64(), /*nullable=*/true),
        arrow::field("watermark", arrow::int64(), /*nullable=*/true),
        arrow::field("next_row_id", arrow::int64(), /*nullable=*/true),
    });
}

Result<std::vector<GenericRow>> SnapshotsSystemTable::BuildRows() const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    SnapshotManager snapshot_manager(context_.fs, context_.table_path, context_.branch);
    PAIMON_ASSIGN_OR_RAISE(std::vector<Snapshot> snapshots, snapshot_manager.GetAllSnapshots());
    std::sort(snapshots.begin(), snapshots.end(),
              [](const Snapshot& lhs, const Snapshot& rhs) { return lhs.Id() < rhs.Id(); });
    std::vector<GenericRow> rows;
    rows.reserve(snapshots.size());

    for (const auto& snapshot : snapshots) {
        GenericRow row(schema->num_fields());
        row.SetField(0, snapshot.Id());
        row.SetField(1, snapshot.SchemaId());
        row.SetField(2, StringValue(snapshot.CommitUser()));
        row.SetField(3, snapshot.CommitIdentifier());
        row.SetField(4, StringValue(Snapshot::CommitKind::ToString(snapshot.GetCommitKind())));
        PAIMON_ASSIGN_OR_RAISE(VariantType commit_time,
                               LocalTimestampMillisValue(snapshot.TimeMillis()));
        row.SetField(5, commit_time);
        row.SetField(6, StringValue(snapshot.BaseManifestList()));
        row.SetField(7, StringValue(snapshot.DeltaManifestList()));
        row.SetField(8, OptionalStringValue(snapshot.ChangelogManifestList()));
        row.SetField(9, OptionalInt64Value(snapshot.TotalRecordCount()));
        row.SetField(10, OptionalInt64Value(snapshot.DeltaRecordCount()));
        row.SetField(11, OptionalInt64Value(snapshot.ChangelogRecordCount()));
        row.SetField(12, OptionalInt64Value(snapshot.Watermark()));
        row.SetField(13, OptionalInt64Value(snapshot.NextRowId()));
        rows.push_back(std::move(row));
    }

    return rows;
}

SchemasSystemTable::SchemasSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                       std::string branch)
    : InMemorySystemTable(table_path),
      context_(CreateMetadataContext(std::move(fs), std::move(table_path), std::move(branch))) {}

std::string SchemasSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> SchemasSystemTable::ArrowSchema() const {
    return arrow::schema({
        arrow::field("schema_id", arrow::int64(), /*nullable=*/false),
        arrow::field("fields", arrow::utf8(), /*nullable=*/false),
        arrow::field("partition_keys", arrow::utf8(), /*nullable=*/false),
        arrow::field("primary_keys", arrow::utf8(), /*nullable=*/false),
        arrow::field("options", arrow::utf8(), /*nullable=*/false),
        arrow::field("comment", arrow::utf8(), /*nullable=*/true),
        arrow::field("update_time", arrow::timestamp(arrow::TimeUnit::MILLI),
                     /*nullable=*/false),
    });
}

Result<std::vector<GenericRow>> SchemasSystemTable::BuildRows() const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    SchemaManager schema_manager(context_.fs, context_.table_path, context_.branch);
    PAIMON_ASSIGN_OR_RAISE(std::vector<int64_t> schema_ids, schema_manager.ListAllIds());
    std::sort(schema_ids.begin(), schema_ids.end());
    std::vector<GenericRow> rows;
    rows.reserve(schema_ids.size());

    for (int64_t id : schema_ids) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> table_schema,
                               schema_manager.ReadSchema(id));
        PAIMON_ASSIGN_OR_RAISE(std::string fields_json, JsonString(table_schema->Fields()));
        PAIMON_ASSIGN_OR_RAISE(std::string partition_keys_json,
                               JsonString(table_schema->PartitionKeys()));
        PAIMON_ASSIGN_OR_RAISE(std::string primary_keys_json,
                               JsonString(table_schema->PrimaryKeys()));
        PAIMON_ASSIGN_OR_RAISE(std::string options_json, JsonString(table_schema->Options()));

        GenericRow row(schema->num_fields());
        row.SetField(0, table_schema->Id());
        row.SetField(1, StringValue(fields_json));
        row.SetField(2, StringValue(partition_keys_json));
        row.SetField(3, StringValue(primary_keys_json));
        row.SetField(4, StringValue(options_json));
        row.SetField(5, OptionalStringValue(table_schema->Comment()));
        PAIMON_ASSIGN_OR_RAISE(VariantType update_time,
                               LocalTimestampMillisValue(table_schema->TimeMillis()));
        row.SetField(6, update_time);
        rows.push_back(std::move(row));
    }

    return rows;
}

TagsSystemTable::TagsSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                 std::string branch)
    : InMemorySystemTable(table_path),
      context_(CreateMetadataContext(std::move(fs), std::move(table_path), std::move(branch))) {}

std::string TagsSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> TagsSystemTable::ArrowSchema() const {
    return arrow::schema({
        arrow::field("tag_name", arrow::utf8(), /*nullable=*/false),
        arrow::field("snapshot_id", arrow::int64(), /*nullable=*/false),
        arrow::field("schema_id", arrow::int64(), /*nullable=*/false),
        arrow::field("commit_time", arrow::timestamp(arrow::TimeUnit::MILLI),
                     /*nullable=*/false),
        arrow::field("record_count", arrow::int64(), /*nullable=*/true),
        arrow::field("create_time", arrow::timestamp(arrow::TimeUnit::MILLI),
                     /*nullable=*/true),
        arrow::field("time_retained", arrow::utf8(), /*nullable=*/true),
    });
}

Result<std::vector<GenericRow>> TagsSystemTable::BuildRows() const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    TagManager tag_manager(context_.fs, context_.table_path, context_.branch);
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> tag_names, tag_manager.ListTagNames());
    std::vector<GenericRow> rows;
    rows.reserve(tag_names.size());

    for (const auto& name : tag_names) {
        PAIMON_ASSIGN_OR_RAISE(Tag tag, tag_manager.GetOrThrow(name));
        PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> tag_create_time,
                               OptionalLocalDateTimePartsToTimestampMillis(tag.TagCreateTime()));
        GenericRow row(schema->num_fields());
        row.SetField(0, StringValue(name));
        row.SetField(1, tag.Id());
        row.SetField(2, tag.SchemaId());
        PAIMON_ASSIGN_OR_RAISE(VariantType commit_time,
                               LocalTimestampMillisValue(tag.TimeMillis()));
        row.SetField(3, commit_time);
        row.SetField(4, OptionalInt64Value(tag.TotalRecordCount()));
        row.SetField(5, OptionalTimestampMillisValue(tag_create_time));
        row.SetField(6, OptionalStringValue(OptionalDoubleToString(tag.TagTimeRetained())));
        rows.push_back(std::move(row));
    }

    return rows;
}

BranchesSystemTable::BranchesSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                         std::string branch)
    : InMemorySystemTable(table_path),
      context_(CreateMetadataContext(std::move(fs), std::move(table_path), std::move(branch))) {}

std::string BranchesSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> BranchesSystemTable::ArrowSchema() const {
    return arrow::schema({
        arrow::field("branch_name", arrow::utf8(), /*nullable=*/false),
        arrow::field("create_time", arrow::timestamp(arrow::TimeUnit::MILLI),
                     /*nullable=*/false),
    });
}

Result<std::vector<GenericRow>> BranchesSystemTable::BuildRows() const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> branches,
                           BranchManager::ListBranches(context_.fs, context_.table_path));
    std::vector<GenericRow> rows;
    rows.reserve(branches.size());

    for (const auto& name : branches) {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<FileStatus> branch_status,
            context_.fs->GetFileStatus(BranchManager::BranchPath(context_.table_path, name)));
        GenericRow row(schema->num_fields());
        row.SetField(0, StringValue(name));
        PAIMON_ASSIGN_OR_RAISE(VariantType create_time,
                               LocalTimestampMillisValue(branch_status->GetModificationTime()));
        row.SetField(1, create_time);
        rows.push_back(std::move(row));
    }

    return rows;
}

ConsumersSystemTable::ConsumersSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                           std::string branch)
    : InMemorySystemTable(table_path),
      context_(CreateMetadataContext(std::move(fs), std::move(table_path), std::move(branch))) {}

std::string ConsumersSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> ConsumersSystemTable::ArrowSchema() const {
    return arrow::schema({
        arrow::field("consumer_id", arrow::utf8(), /*nullable=*/false),
        arrow::field("next_snapshot_id", arrow::int64(), /*nullable=*/false),
    });
}

Result<std::vector<GenericRow>> ConsumersSystemTable::BuildRows() const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    ConsumerManager consumer_manager(context_.fs, context_.table_path, context_.branch);
    PAIMON_ASSIGN_OR_RAISE(auto consumers, consumer_manager.Consumers());
    std::vector<GenericRow> rows;
    rows.reserve(consumers.size());

    for (const auto& [id, snapshot_id] : consumers) {
        GenericRow row(schema->num_fields());
        row.SetField(0, StringValue(id));
        row.SetField(1, snapshot_id);
        rows.push_back(std::move(row));
    }

    return rows;
}

ManifestsSystemTable::ManifestsSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                           std::string branch,
                                           std::shared_ptr<TableSchema> table_schema,
                                           std::map<std::string, std::string> options)
    : InMemorySystemTable(table_path),
      context_(CreateMetadataContext(std::move(fs), std::move(table_path), std::move(branch),
                                     std::move(table_schema), std::move(options))) {}

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
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot, LatestSnapshot(context_));
    if (!snapshot) {
        return std::vector<GenericRow>();
    }

    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CreateCoreOptions(context_));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStorePathFactory> path_factory,
                           CreatePathFactory(context_, core_options, pool));
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<ManifestFileMeta> manifests,
        ReadDataManifests(context_, snapshot.value(), path_factory, core_options, pool));
    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(context_.table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, context_.table_schema->PartitionKeys()));

    std::vector<GenericRow> rows;
    rows.reserve(manifests.size());
    for (const auto& manifest : manifests) {
        GenericRow row(schema->num_fields());
        row.SetField(0, StringValue(manifest.FileName()));
        row.SetField(1, manifest.FileSize());
        row.SetField(2, manifest.NumAddedFiles());
        row.SetField(3, manifest.NumDeletedFiles());
        row.SetField(4, manifest.SchemaId());
        PAIMON_ASSIGN_OR_RAISE(
            VariantType min_partition,
            OptionalPartitionStringValue(manifest.PartitionStats().MinValues(), partition_schema));
        PAIMON_ASSIGN_OR_RAISE(
            VariantType max_partition,
            OptionalPartitionStringValue(manifest.PartitionStats().MaxValues(), partition_schema));
        row.SetField(5, min_partition);
        row.SetField(6, max_partition);
        row.SetField(7, OptionalInt64Value(manifest.MinRowId()));
        row.SetField(8, OptionalInt64Value(manifest.MaxRowId()));
        rows.push_back(std::move(row));
    }
    return rows;
}

FilesSystemTable::FilesSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                   std::string branch, std::shared_ptr<TableSchema> table_schema,
                                   std::map<std::string, std::string> options)
    : InMemorySystemTable(table_path),
      context_(CreateMetadataContext(std::move(fs), std::move(table_path), std::move(branch),
                                     std::move(table_schema), std::move(options))) {}

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
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CreateCoreOptions(context_));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStorePathFactory> path_factory,
                           CreatePathFactory(context_, core_options, pool));
    PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestEntry> entries,
                           ReadLatestDataFiles(context_, path_factory, core_options, pool));
    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(context_.table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, context_.table_schema->PartitionKeys()));
    const std::vector<DataField>& value_stats_fields = context_.table_schema->Fields();

    std::vector<GenericRow> rows;
    rows.reserve(entries.size());
    for (const auto& entry : entries) {
        if (!(entry.Kind() == FileKind::Add())) {
            continue;
        }

        const std::shared_ptr<DataFileMeta>& file = entry.File();
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> data_schema,
                               LoadDataSchema(context_, file->schema_id));
        PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> data_stats_fields,
                               ProjectWriteFields(data_schema, *file));
        PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> key_fields,
                               KeyFieldsForFilesTable(data_schema));
        auto stats_evolution = std::make_shared<SimpleStatsEvolution>(
            data_stats_fields, value_stats_fields,
            data_schema->Id() != context_.table_schema->Id() || file->write_cols.has_value(), pool);
        PAIMON_ASSIGN_OR_RAISE(
            SimpleStatsEvolution::EvolutionStats stats,
            stats_evolution->Evolution(file->value_stats, file->row_count, file->value_stats_cols));

        GenericRow row(schema->num_fields());
        if (context_.table_schema->PartitionKeys().empty()) {
            row.SetField(0, NullType());
        } else {
            PAIMON_ASSIGN_OR_RAISE(VariantType partition, OptionalPartitionStringValue(
                                                              entry.Partition(), partition_schema));
            row.SetField(0, partition);
        }
        row.SetField(1, entry.Bucket());
        PAIMON_ASSIGN_OR_RAISE(std::string file_path, FilePath(path_factory, entry, *file));
        row.SetField(2, StringValue(file_path));
        PAIMON_ASSIGN_OR_RAISE(std::string file_format, file->FileFormat());
        row.SetField(3, StringValue(file_format));
        row.SetField(4, file->schema_id);
        row.SetField(5, file->level);
        row.SetField(6, file->row_count);
        row.SetField(7, file->file_size);
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> min_key,
                               OptionalRowValuesString(key_fields, file->min_key, "[", "]"));
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> max_key,
                               OptionalRowValuesString(key_fields, file->max_key, "[", "]"));
        row.SetField(8, OptionalStringValue(min_key));
        row.SetField(9, OptionalStringValue(max_key));
        PAIMON_ASSIGN_OR_RAISE(std::string null_value_counts,
                               NullValueCountsString(value_stats_fields, *stats.null_counts));
        row.SetField(10, StringValue(null_value_counts));
        PAIMON_ASSIGN_OR_RAISE(std::string min_value_stats,
                               FieldsValueMapString(value_stats_fields, *stats.min_values));
        row.SetField(11, StringValue(min_value_stats));
        PAIMON_ASSIGN_OR_RAISE(std::string max_value_stats,
                               FieldsValueMapString(value_stats_fields, *stats.max_values));
        row.SetField(12, StringValue(max_value_stats));
        row.SetField(13, file->min_sequence_number);
        row.SetField(14, file->max_sequence_number);
        row.SetField(15, TimestampMillisValue(file->creation_time.GetMillisecond()));
        row.SetField(16, OptionalInt64Value(file->delete_row_count));
        row.SetField(17, file->file_source ? StringValue(file->file_source.value().ToString())
                                           : VariantType(NullType()));
        row.SetField(18, OptionalInt64Value(file->first_row_id));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InternalArray> write_cols,
                               WriteColsValue(file->write_cols, pool));
        row.SetField(19, write_cols ? VariantType(write_cols) : VariantType(NullType()));
        rows.push_back(std::move(row));
    }
    return rows;
}

}  // namespace paimon
