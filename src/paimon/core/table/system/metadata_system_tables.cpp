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
#include <utility>
#include <vector>

#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/tag/tag.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/consumer_manager.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/core/utils/tag_manager.h"
#include "paimon/fs/file_system.h"
#include "paimon/status.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace paimon {
namespace {

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
        std::move(fs),
        std::move(table_path),
        BranchManager::NormalizeBranch(branch),
    };
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

}  // namespace paimon
