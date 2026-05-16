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
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/tag/tag.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/core/utils/tag_manager.h"
#include "paimon/fs/file_system.h"
#include "paimon/status.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace paimon {
namespace {

std::shared_ptr<arrow::Field> NotNullField(const std::string& name,
                                           const std::shared_ptr<arrow::DataType>& type) {
    return arrow::field(name, type, /*nullable=*/false);
}

std::shared_ptr<arrow::Field> NullableField(const std::string& name,
                                            const std::shared_ptr<arrow::DataType>& type) {
    return arrow::field(name, type, /*nullable=*/true);
}

arrow::Status AppendOptionalString(arrow::StringBuilder* builder,
                                   const std::optional<std::string>& value) {
    if (value) {
        return builder->Append(value.value());
    }
    return builder->AppendNull();
}

arrow::Status AppendOptionalInt64(arrow::Int64Builder* builder,
                                  const std::optional<int64_t>& value) {
    if (value) {
        return builder->Append(value.value());
    }
    return builder->AppendNull();
}

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

template <typename T>
Result<std::optional<std::string>> OptionalJson(const std::optional<T>& value) {
    if (!value) {
        return std::optional<std::string>();
    }
    PAIMON_ASSIGN_OR_RAISE(std::string json, JsonString(value.value()));
    return std::optional<std::string>(std::move(json));
}

arrow::Status FinishBuilder(arrow::ArrayBuilder* builder, std::shared_ptr<arrow::Array>* out) {
    return builder->Finish(out);
}

Result<std::shared_ptr<arrow::RecordBatch>> MakeRecordBatch(
    const std::shared_ptr<arrow::Schema>& schema, int64_t num_rows,
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders) {
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(builders.size());
    for (auto& builder : builders) {
        std::shared_ptr<arrow::Array> array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(FinishBuilder(builder.get(), &array));
        arrays.push_back(std::move(array));
    }
    return arrow::RecordBatch::Make(schema, num_rows, std::move(arrays));
}

std::string ConsumerDirectory(const std::string& table_path, const std::string& branch) {
    return PathUtil::JoinPath(BranchManager::BranchPath(table_path, branch), "/consumer");
}

Result<std::vector<std::string>> ListConsumers(const std::shared_ptr<FileSystem>& fs,
                                               const std::string& table_path,
                                               const std::string& branch) {
    std::vector<std::string> consumers;
    std::string consumer_dir = ConsumerDirectory(table_path, branch);
    PAIMON_ASSIGN_OR_RAISE(bool exists, fs->Exists(consumer_dir));
    if (!exists) {
        return consumers;
    }

    std::vector<std::unique_ptr<BasicFileStatus>> file_status_list;
    PAIMON_RETURN_NOT_OK(fs->ListDir(consumer_dir, &file_status_list));
    std::string prefix = "consumer-";
    for (const auto& file_status : file_status_list) {
        if (file_status->IsDir()) {
            continue;
        }
        std::string file_name = PathUtil::GetName(file_status->GetPath());
        if (StringUtils::StartsWith(file_name, prefix, /*start_pos=*/0)) {
            consumers.push_back(file_name.substr(prefix.length()));
        }
    }
    std::sort(consumers.begin(), consumers.end());
    return consumers;
}

}  // namespace

SnapshotsSystemTable::SnapshotsSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                           std::shared_ptr<TableSchema> table_schema,
                                           std::string branch)
    : MetadataSystemTable(std::move(fs), std::move(table_path), std::move(table_schema),
                          std::move(branch)) {}

std::string SnapshotsSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> SnapshotsSystemTable::ArrowSchema() const {
    return arrow::schema({
        NotNullField("snapshot_id", arrow::int64()),
        NotNullField("schema_id", arrow::int64()),
        NotNullField("commit_user", arrow::utf8()),
        NotNullField("commit_identifier", arrow::int64()),
        NotNullField("commit_kind", arrow::utf8()),
        NotNullField("commit_time", arrow::int64()),
        NullableField("total_record_count", arrow::int64()),
        NullableField("delta_record_count", arrow::int64()),
        NullableField("changelog_record_count", arrow::int64()),
        NullableField("watermark", arrow::int64()),
    });
}

Result<std::shared_ptr<arrow::RecordBatch>> SnapshotsSystemTable::BuildRecordBatch(
    arrow::MemoryPool* pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    SnapshotManager snapshot_manager(fs_, table_path_, branch_);
    PAIMON_ASSIGN_OR_RAISE(std::vector<Snapshot> snapshots, snapshot_manager.GetAllSnapshots());
    std::sort(snapshots.begin(), snapshots.end(),
              [](const Snapshot& lhs, const Snapshot& rhs) { return lhs.Id() < rhs.Id(); });

    auto snapshot_id = std::make_unique<arrow::Int64Builder>(pool);
    auto schema_id = std::make_unique<arrow::Int64Builder>(pool);
    auto commit_user = std::make_unique<arrow::StringBuilder>(pool);
    auto commit_identifier = std::make_unique<arrow::Int64Builder>(pool);
    auto commit_kind = std::make_unique<arrow::StringBuilder>(pool);
    auto commit_time = std::make_unique<arrow::Int64Builder>(pool);
    auto total_record_count = std::make_unique<arrow::Int64Builder>(pool);
    auto delta_record_count = std::make_unique<arrow::Int64Builder>(pool);
    auto changelog_record_count = std::make_unique<arrow::Int64Builder>(pool);
    auto watermark = std::make_unique<arrow::Int64Builder>(pool);

    for (const auto& snapshot : snapshots) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(snapshot_id->Append(snapshot.Id()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(schema_id->Append(snapshot.SchemaId()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(commit_user->Append(snapshot.CommitUser()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(commit_identifier->Append(snapshot.CommitIdentifier()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            commit_kind->Append(Snapshot::CommitKind::ToString(snapshot.GetCommitKind())));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(commit_time->Append(snapshot.TimeMillis()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            AppendOptionalInt64(total_record_count.get(), snapshot.TotalRecordCount()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            AppendOptionalInt64(delta_record_count.get(), snapshot.DeltaRecordCount()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            AppendOptionalInt64(changelog_record_count.get(), snapshot.ChangelogRecordCount()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(AppendOptionalInt64(watermark.get(), snapshot.Watermark()));
    }

    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    builders.push_back(std::move(snapshot_id));
    builders.push_back(std::move(schema_id));
    builders.push_back(std::move(commit_user));
    builders.push_back(std::move(commit_identifier));
    builders.push_back(std::move(commit_kind));
    builders.push_back(std::move(commit_time));
    builders.push_back(std::move(total_record_count));
    builders.push_back(std::move(delta_record_count));
    builders.push_back(std::move(changelog_record_count));
    builders.push_back(std::move(watermark));
    return MakeRecordBatch(schema, static_cast<int64_t>(snapshots.size()), std::move(builders));
}

SchemasSystemTable::SchemasSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                       std::shared_ptr<TableSchema> table_schema,
                                       std::string branch)
    : MetadataSystemTable(std::move(fs), std::move(table_path), std::move(table_schema),
                          std::move(branch)) {}

std::string SchemasSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> SchemasSystemTable::ArrowSchema() const {
    return arrow::schema({
        NotNullField("schema_id", arrow::int64()),
        NotNullField("fields", arrow::utf8()),
        NotNullField("partition_keys", arrow::utf8()),
        NotNullField("primary_keys", arrow::utf8()),
        NotNullField("options", arrow::utf8()),
        NullableField("comment", arrow::utf8()),
    });
}

Result<std::shared_ptr<arrow::RecordBatch>> SchemasSystemTable::BuildRecordBatch(
    arrow::MemoryPool* pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    SchemaManager schema_manager(fs_, table_path_, branch_);
    PAIMON_ASSIGN_OR_RAISE(std::vector<int64_t> schema_ids, schema_manager.ListAllIds());
    std::sort(schema_ids.begin(), schema_ids.end());

    auto schema_id = std::make_unique<arrow::Int64Builder>(pool);
    auto fields = std::make_unique<arrow::StringBuilder>(pool);
    auto partition_keys = std::make_unique<arrow::StringBuilder>(pool);
    auto primary_keys = std::make_unique<arrow::StringBuilder>(pool);
    auto options = std::make_unique<arrow::StringBuilder>(pool);
    auto comment = std::make_unique<arrow::StringBuilder>(pool);

    for (int64_t id : schema_ids) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> table_schema,
                               schema_manager.ReadSchema(id));
        PAIMON_ASSIGN_OR_RAISE(std::string json_schema, table_schema->GetJsonSchema());
        PAIMON_ASSIGN_OR_RAISE(std::string partition_keys_json,
                               JsonString(table_schema->PartitionKeys()));
        PAIMON_ASSIGN_OR_RAISE(std::string primary_keys_json,
                               JsonString(table_schema->PrimaryKeys()));
        PAIMON_ASSIGN_OR_RAISE(std::string options_json, JsonString(table_schema->Options()));

        PAIMON_RETURN_NOT_OK_FROM_ARROW(schema_id->Append(table_schema->Id()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(fields->Append(json_schema));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(partition_keys->Append(partition_keys_json));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(primary_keys->Append(primary_keys_json));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(options->Append(options_json));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            AppendOptionalString(comment.get(), table_schema->Comment()));
    }

    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    builders.push_back(std::move(schema_id));
    builders.push_back(std::move(fields));
    builders.push_back(std::move(partition_keys));
    builders.push_back(std::move(primary_keys));
    builders.push_back(std::move(options));
    builders.push_back(std::move(comment));
    return MakeRecordBatch(schema, static_cast<int64_t>(schema_ids.size()), std::move(builders));
}

TagsSystemTable::TagsSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                 std::shared_ptr<TableSchema> table_schema, std::string branch)
    : MetadataSystemTable(std::move(fs), std::move(table_path), std::move(table_schema),
                          std::move(branch)) {}

std::string TagsSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> TagsSystemTable::ArrowSchema() const {
    return arrow::schema({
        NotNullField("tag_name", arrow::utf8()),
        NotNullField("snapshot_id", arrow::int64()),
        NotNullField("schema_id", arrow::int64()),
        NullableField("tag_create_time", arrow::utf8()),
        NullableField("tag_time_retained", arrow::float64()),
    });
}

Result<std::shared_ptr<arrow::RecordBatch>> TagsSystemTable::BuildRecordBatch(
    arrow::MemoryPool* pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    TagManager tag_manager(fs_, table_path_, branch_);
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> tag_names, tag_manager.ListTagNames());

    auto tag_name = std::make_unique<arrow::StringBuilder>(pool);
    auto snapshot_id = std::make_unique<arrow::Int64Builder>(pool);
    auto schema_id = std::make_unique<arrow::Int64Builder>(pool);
    auto tag_create_time = std::make_unique<arrow::StringBuilder>(pool);
    auto tag_time_retained = std::make_unique<arrow::DoubleBuilder>(pool);

    for (const auto& name : tag_names) {
        PAIMON_ASSIGN_OR_RAISE(Tag tag, tag_manager.GetOrThrow(name));
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> tag_create_time_json,
                               OptionalJson(tag.TagCreateTime()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(tag_name->Append(name));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(snapshot_id->Append(tag.Id()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(schema_id->Append(tag.SchemaId()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            AppendOptionalString(tag_create_time.get(), tag_create_time_json));
        if (tag.TagTimeRetained()) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                tag_time_retained->Append(tag.TagTimeRetained().value()));
        } else {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(tag_time_retained->AppendNull());
        }
    }

    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    builders.push_back(std::move(tag_name));
    builders.push_back(std::move(snapshot_id));
    builders.push_back(std::move(schema_id));
    builders.push_back(std::move(tag_create_time));
    builders.push_back(std::move(tag_time_retained));
    return MakeRecordBatch(schema, static_cast<int64_t>(tag_names.size()), std::move(builders));
}

BranchesSystemTable::BranchesSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                         std::shared_ptr<TableSchema> table_schema,
                                         std::string branch)
    : MetadataSystemTable(std::move(fs), std::move(table_path), std::move(table_schema),
                          std::move(branch)) {}

std::string BranchesSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> BranchesSystemTable::ArrowSchema() const {
    return arrow::schema({
        NotNullField("branch_name", arrow::utf8()),
        NullableField("schema_id", arrow::int64()),
        NullableField("snapshot_id", arrow::int64()),
    });
}

Result<std::shared_ptr<arrow::RecordBatch>> BranchesSystemTable::BuildRecordBatch(
    arrow::MemoryPool* pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> branches,
                           BranchManager::ListBranches(fs_, table_path_));

    auto branch_name = std::make_unique<arrow::StringBuilder>(pool);
    auto schema_id = std::make_unique<arrow::Int64Builder>(pool);
    auto snapshot_id = std::make_unique<arrow::Int64Builder>(pool);

    for (const auto& name : branches) {
        SchemaManager schema_manager(fs_, table_path_, name);
        SnapshotManager snapshot_manager(fs_, table_path_, name);
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                               schema_manager.Latest());
        PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> latest_snapshot_id,
                               snapshot_manager.LatestSnapshotId());

        PAIMON_RETURN_NOT_OK_FROM_ARROW(branch_name->Append(name));
        if (latest_schema) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(schema_id->Append(latest_schema.value()->Id()));
        } else {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(schema_id->AppendNull());
        }
        PAIMON_RETURN_NOT_OK_FROM_ARROW(AppendOptionalInt64(snapshot_id.get(), latest_snapshot_id));
    }

    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    builders.push_back(std::move(branch_name));
    builders.push_back(std::move(schema_id));
    builders.push_back(std::move(snapshot_id));
    return MakeRecordBatch(schema, static_cast<int64_t>(branches.size()), std::move(builders));
}

ConsumersSystemTable::ConsumersSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                           std::shared_ptr<TableSchema> table_schema,
                                           std::string branch)
    : MetadataSystemTable(std::move(fs), std::move(table_path), std::move(table_schema),
                          std::move(branch)) {}

std::string ConsumersSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> ConsumersSystemTable::ArrowSchema() const {
    return arrow::schema({
        NotNullField("consumer_id", arrow::utf8()),
        NullableField("next_snapshot_id", arrow::int64()),
    });
}

Result<std::shared_ptr<arrow::RecordBatch>> ConsumersSystemTable::BuildRecordBatch(
    arrow::MemoryPool* pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> consumers,
                           ListConsumers(fs_, table_path_, branch_));

    auto consumer_id = std::make_unique<arrow::StringBuilder>(pool);
    auto next_snapshot_id = std::make_unique<arrow::Int64Builder>(pool);
    std::string consumer_dir = ConsumerDirectory(table_path_, branch_);

    for (const auto& id : consumers) {
        std::string content;
        std::string consumer_path = PathUtil::JoinPath(consumer_dir, "consumer-" + id);
        Status read_status = fs_->ReadFile(consumer_path, &content);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(consumer_id->Append(id));
        if (read_status.ok()) {
            std::optional<int64_t> snapshot_id = StringUtils::StringToValue<int64_t>(content);
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                AppendOptionalInt64(next_snapshot_id.get(), snapshot_id));
        } else {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(next_snapshot_id->AppendNull());
        }
    }

    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    builders.push_back(std::move(consumer_id));
    builders.push_back(std::move(next_snapshot_id));
    return MakeRecordBatch(schema, static_cast<int64_t>(consumers.size()), std::move(builders));
}

}  // namespace paimon
