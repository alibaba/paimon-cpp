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

#include "arrow/api.h"
#include "paimon/common/utils/arrow/status_utils.h"
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

    std::tm time_info{};
    time_info.tm_year = static_cast<int>(parts[0] - 1900);
    time_info.tm_mon = static_cast<int>(parts[1] - 1);
    time_info.tm_mday = static_cast<int>(parts[2]);
    time_info.tm_hour = static_cast<int>(parts[3]);
    time_info.tm_min = static_cast<int>(parts[4]);
    time_info.tm_sec = static_cast<int>(parts[5]);
    time_info.tm_isdst = -1;

    std::time_t seconds = std::mktime(&time_info);
    if (seconds == -1) {
        return Status::Invalid("failed to convert tag create time to timestamp");
    }
    int64_t nanos = parts.size() > 6 ? parts[6] : 0;
    return static_cast<int64_t>(seconds) * 1000 + nanos / 1000000;
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

class MetadataRecordBatchBuilder {
 public:
    static paimon::Result<std::unique_ptr<MetadataRecordBatchBuilder>> Create(
        std::shared_ptr<arrow::Schema> schema, arrow::MemoryPool* pool) {
        auto rows = std::unique_ptr<MetadataRecordBatchBuilder>(
            new MetadataRecordBatchBuilder(std::move(schema)));
        rows->builders_.reserve(rows->schema_->num_fields());
        for (const auto& field : rows->schema_->fields()) {
            std::unique_ptr<arrow::ArrayBuilder> builder;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::MakeBuilder(pool, field->type(), &builder));
            rows->builders_.push_back(std::move(builder));
        }
        if (rows->builders_.size() != static_cast<size_t>(rows->schema_->num_fields())) {
            return Status::Invalid("failed to create metadata system table builders");
        }
        return std::move(rows);
    }

    Status AppendInt64(int32_t index, int64_t value) {
        return AppendValue<arrow::Int64Builder>(index, value, "int64");
    }

    Status AppendOptionalInt64(int32_t index, const std::optional<int64_t>& value) {
        return AppendOptionalValue<arrow::Int64Builder>(index, value, "int64");
    }

    Status AppendTimestampMillis(int32_t index, int64_t value) {
        return AppendValue<arrow::TimestampBuilder>(index, value, "timestamp");
    }

    Status AppendOptionalTimestampMillis(int32_t index, const std::optional<int64_t>& value) {
        return AppendOptionalValue<arrow::TimestampBuilder>(index, value, "timestamp");
    }

    Status AppendString(int32_t index, const std::string& value) {
        return AppendValue<arrow::StringBuilder>(index, value, "string");
    }

    Status AppendOptionalString(int32_t index, const std::optional<std::string>& value) {
        return AppendOptionalValue<arrow::StringBuilder>(index, value, "string");
    }

    paimon::Result<std::shared_ptr<arrow::RecordBatch>> Finish() {
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        arrays.reserve(builders_.size());
        int64_t num_rows = 0;
        for (size_t i = 0; i < builders_.size(); ++i) {
            std::shared_ptr<arrow::Array> array;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builders_[i]->Finish(&array));
            if (i == 0) {
                num_rows = array->length();
            } else if (array->length() != num_rows) {
                return Status::Invalid("metadata field ", schema_->field(i)->name(),
                                       " length does not match previous fields");
            }
            arrays.push_back(std::move(array));
        }
        return arrow::RecordBatch::Make(schema_, num_rows, std::move(arrays));
    }

 private:
    explicit MetadataRecordBatchBuilder(std::shared_ptr<arrow::Schema> schema)
        : schema_(std::move(schema)) {}

    template <typename Builder, typename Value>
    Status AppendValue(int32_t index, const Value& value, const std::string& type_name) {
        auto* builder = dynamic_cast<Builder*>(builders_.at(index).get());
        if (builder == nullptr) {
            return Status::Invalid("metadata field ", schema_->field(index)->name(), " is not ",
                                   type_name);
        }
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(value));
        return Status::OK();
    }

    template <typename Builder, typename Value>
    Status AppendOptionalValue(int32_t index, const std::optional<Value>& value,
                               const std::string& type_name) {
        auto* builder = dynamic_cast<Builder*>(builders_.at(index).get());
        if (builder == nullptr) {
            return Status::Invalid("metadata field ", schema_->field(index)->name(), " is not ",
                                   type_name);
        }
        if (value) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(value.value()));
        } else {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->AppendNull());
        }
        return Status::OK();
    }

    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders_;
};

}  // namespace

SnapshotsSystemTable::SnapshotsSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                           std::string branch)
    : MetadataSystemTable(std::move(fs), std::move(table_path), std::move(branch)) {}

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

Result<std::shared_ptr<arrow::RecordBatch>> SnapshotsSystemTable::BuildRecordBatch(
    arrow::MemoryPool* pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    SnapshotManager snapshot_manager(fs_, TablePath(), branch_);
    PAIMON_ASSIGN_OR_RAISE(std::vector<Snapshot> snapshots, snapshot_manager.GetAllSnapshots());
    std::sort(snapshots.begin(), snapshots.end(),
              [](const Snapshot& lhs, const Snapshot& rhs) { return lhs.Id() < rhs.Id(); });
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<MetadataRecordBatchBuilder> rows,
                           MetadataRecordBatchBuilder::Create(schema, pool));

    for (const auto& snapshot : snapshots) {
        PAIMON_RETURN_NOT_OK(rows->AppendInt64(0, snapshot.Id()));
        PAIMON_RETURN_NOT_OK(rows->AppendInt64(1, snapshot.SchemaId()));
        PAIMON_RETURN_NOT_OK(rows->AppendString(2, snapshot.CommitUser()));
        PAIMON_RETURN_NOT_OK(rows->AppendInt64(3, snapshot.CommitIdentifier()));
        PAIMON_RETURN_NOT_OK(
            rows->AppendString(4, Snapshot::CommitKind::ToString(snapshot.GetCommitKind())));
        PAIMON_RETURN_NOT_OK(rows->AppendTimestampMillis(5, snapshot.TimeMillis()));
        PAIMON_RETURN_NOT_OK(rows->AppendString(6, snapshot.BaseManifestList()));
        PAIMON_RETURN_NOT_OK(rows->AppendString(7, snapshot.DeltaManifestList()));
        PAIMON_RETURN_NOT_OK(rows->AppendOptionalString(8, snapshot.ChangelogManifestList()));
        PAIMON_RETURN_NOT_OK(rows->AppendOptionalInt64(9, snapshot.TotalRecordCount()));
        PAIMON_RETURN_NOT_OK(rows->AppendOptionalInt64(10, snapshot.DeltaRecordCount()));
        PAIMON_RETURN_NOT_OK(rows->AppendOptionalInt64(11, snapshot.ChangelogRecordCount()));
        PAIMON_RETURN_NOT_OK(rows->AppendOptionalInt64(12, snapshot.Watermark()));
        PAIMON_RETURN_NOT_OK(rows->AppendOptionalInt64(13, snapshot.NextRowId()));
    }

    return rows->Finish();
}

SchemasSystemTable::SchemasSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                       std::string branch)
    : MetadataSystemTable(std::move(fs), std::move(table_path), std::move(branch)) {}

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

Result<std::shared_ptr<arrow::RecordBatch>> SchemasSystemTable::BuildRecordBatch(
    arrow::MemoryPool* pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    SchemaManager schema_manager(fs_, TablePath(), branch_);
    PAIMON_ASSIGN_OR_RAISE(std::vector<int64_t> schema_ids, schema_manager.ListAllIds());
    std::sort(schema_ids.begin(), schema_ids.end());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<MetadataRecordBatchBuilder> rows,
                           MetadataRecordBatchBuilder::Create(schema, pool));

    for (int64_t id : schema_ids) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> table_schema,
                               schema_manager.ReadSchema(id));
        PAIMON_ASSIGN_OR_RAISE(std::string fields_json, JsonString(table_schema->Fields()));
        PAIMON_ASSIGN_OR_RAISE(std::string partition_keys_json,
                               JsonString(table_schema->PartitionKeys()));
        PAIMON_ASSIGN_OR_RAISE(std::string primary_keys_json,
                               JsonString(table_schema->PrimaryKeys()));
        PAIMON_ASSIGN_OR_RAISE(std::string options_json, JsonString(table_schema->Options()));

        PAIMON_RETURN_NOT_OK(rows->AppendInt64(0, table_schema->Id()));
        PAIMON_RETURN_NOT_OK(rows->AppendString(1, fields_json));
        PAIMON_RETURN_NOT_OK(rows->AppendString(2, partition_keys_json));
        PAIMON_RETURN_NOT_OK(rows->AppendString(3, primary_keys_json));
        PAIMON_RETURN_NOT_OK(rows->AppendString(4, options_json));
        PAIMON_RETURN_NOT_OK(rows->AppendOptionalString(5, table_schema->Comment()));
        PAIMON_RETURN_NOT_OK(rows->AppendTimestampMillis(6, table_schema->TimeMillis()));
    }

    return rows->Finish();
}

TagsSystemTable::TagsSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                 std::string branch)
    : MetadataSystemTable(std::move(fs), std::move(table_path), std::move(branch)) {}

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

Result<std::shared_ptr<arrow::RecordBatch>> TagsSystemTable::BuildRecordBatch(
    arrow::MemoryPool* pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    TagManager tag_manager(fs_, TablePath(), branch_);
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> tag_names, tag_manager.ListTagNames());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<MetadataRecordBatchBuilder> rows,
                           MetadataRecordBatchBuilder::Create(schema, pool));

    for (const auto& name : tag_names) {
        PAIMON_ASSIGN_OR_RAISE(Tag tag, tag_manager.GetOrThrow(name));
        PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> tag_create_time,
                               OptionalLocalDateTimePartsToTimestampMillis(tag.TagCreateTime()));
        PAIMON_RETURN_NOT_OK(rows->AppendString(0, name));
        PAIMON_RETURN_NOT_OK(rows->AppendInt64(1, tag.Id()));
        PAIMON_RETURN_NOT_OK(rows->AppendInt64(2, tag.SchemaId()));
        PAIMON_RETURN_NOT_OK(rows->AppendTimestampMillis(3, tag.TimeMillis()));
        PAIMON_RETURN_NOT_OK(rows->AppendOptionalInt64(4, tag.TotalRecordCount()));
        PAIMON_RETURN_NOT_OK(rows->AppendOptionalTimestampMillis(5, tag_create_time));
        PAIMON_RETURN_NOT_OK(
            rows->AppendOptionalString(6, OptionalDoubleToString(tag.TagTimeRetained())));
    }

    return rows->Finish();
}

BranchesSystemTable::BranchesSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                         std::string branch)
    : MetadataSystemTable(std::move(fs), std::move(table_path), std::move(branch)) {}

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

Result<std::shared_ptr<arrow::RecordBatch>> BranchesSystemTable::BuildRecordBatch(
    arrow::MemoryPool* pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> branches,
                           BranchManager::ListBranches(fs_, TablePath()));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<MetadataRecordBatchBuilder> rows,
                           MetadataRecordBatchBuilder::Create(schema, pool));

    for (const auto& name : branches) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStatus> branch_status,
                               fs_->GetFileStatus(BranchManager::BranchPath(TablePath(), name)));
        PAIMON_RETURN_NOT_OK(rows->AppendString(0, name));
        PAIMON_RETURN_NOT_OK(rows->AppendTimestampMillis(1, branch_status->GetModificationTime()));
    }

    return rows->Finish();
}

ConsumersSystemTable::ConsumersSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                           std::string branch)
    : MetadataSystemTable(std::move(fs), std::move(table_path), std::move(branch)) {}

std::string ConsumersSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> ConsumersSystemTable::ArrowSchema() const {
    return arrow::schema({
        arrow::field("consumer_id", arrow::utf8(), /*nullable=*/false),
        arrow::field("next_snapshot_id", arrow::int64(), /*nullable=*/false),
    });
}

Result<std::shared_ptr<arrow::RecordBatch>> ConsumersSystemTable::BuildRecordBatch(
    arrow::MemoryPool* pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    ConsumerManager consumer_manager(fs_, TablePath(), branch_);
    PAIMON_ASSIGN_OR_RAISE(auto consumers, consumer_manager.Consumers());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<MetadataRecordBatchBuilder> rows,
                           MetadataRecordBatchBuilder::Create(schema, pool));

    for (const auto& [id, snapshot_id] : consumers) {
        PAIMON_RETURN_NOT_OK(rows->AppendString(0, id));
        PAIMON_RETURN_NOT_OK(rows->AppendInt64(1, snapshot_id));
    }

    return rows->Finish();
}

}  // namespace paimon
