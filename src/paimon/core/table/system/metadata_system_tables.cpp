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
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/tag/tag.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/consumer_manager.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/core/utils/tag_manager.h"
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

template <typename T>
Result<std::optional<std::string>> OptionalJson(const std::optional<T>& value) {
    if (!value) {
        return std::optional<std::string>();
    }
    PAIMON_ASSIGN_OR_RAISE(std::string json, JsonString(value.value()));
    return std::optional<std::string>(std::move(json));
}

class MetadataRecordBatchBuilder {
 public:
    MetadataRecordBatchBuilder(std::shared_ptr<arrow::Schema> schema, arrow::MemoryPool* pool)
        : schema_(std::move(schema)) {
        builders_.reserve(schema_->num_fields());
        for (const auto& field : schema_->fields()) {
            std::unique_ptr<arrow::ArrayBuilder> builder;
            init_status_ = ToPaimonStatus(arrow::MakeBuilder(pool, field->type(), &builder));
            if (!init_status_.ok()) {
                return;
            }
            builders_.push_back(std::move(builder));
        }
    }

    Status InitStatus() const {
        if (!init_status_.ok()) {
            return init_status_;
        }
        return builders_.size() == static_cast<size_t>(schema_->num_fields())
                   ? Status::OK()
                   : Status::Invalid("failed to create metadata system table builders");
    }

    Status AppendInt64(int32_t index, int64_t value) {
        auto* builder = dynamic_cast<arrow::Int64Builder*>(builders_.at(index).get());
        if (builder == nullptr) {
            return Status::Invalid("metadata field ", schema_->field(index)->name(),
                                   " is not int64");
        }
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(value));
        return Status::OK();
    }

    Status AppendOptionalInt64(int32_t index, const std::optional<int64_t>& value) {
        auto* builder = dynamic_cast<arrow::Int64Builder*>(builders_.at(index).get());
        if (builder == nullptr) {
            return Status::Invalid("metadata field ", schema_->field(index)->name(),
                                   " is not int64");
        }
        if (value) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(value.value()));
        } else {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->AppendNull());
        }
        return Status::OK();
    }

    Status AppendString(int32_t index, const std::string& value) {
        auto* builder = dynamic_cast<arrow::StringBuilder*>(builders_.at(index).get());
        if (builder == nullptr) {
            return Status::Invalid("metadata field ", schema_->field(index)->name(),
                                   " is not string");
        }
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(value));
        return Status::OK();
    }

    Status AppendOptionalString(int32_t index, const std::optional<std::string>& value) {
        auto* builder = dynamic_cast<arrow::StringBuilder*>(builders_.at(index).get());
        if (builder == nullptr) {
            return Status::Invalid("metadata field ", schema_->field(index)->name(),
                                   " is not string");
        }
        if (value) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(value.value()));
        } else {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->AppendNull());
        }
        return Status::OK();
    }

    Status AppendOptionalDouble(int32_t index, const std::optional<double>& value) {
        auto* builder = dynamic_cast<arrow::DoubleBuilder*>(builders_.at(index).get());
        if (builder == nullptr) {
            return Status::Invalid("metadata field ", schema_->field(index)->name(),
                                   " is not double");
        }
        if (value) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(value.value()));
        } else {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->AppendNull());
        }
        return Status::OK();
    }

    Result<std::shared_ptr<arrow::RecordBatch>> Finish() {
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
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders_;
    Status init_status_;
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
        arrow::field("commit_time", arrow::int64(), /*nullable=*/false),
        arrow::field("total_record_count", arrow::int64(), /*nullable=*/true),
        arrow::field("delta_record_count", arrow::int64(), /*nullable=*/true),
        arrow::field("changelog_record_count", arrow::int64(), /*nullable=*/true),
        arrow::field("watermark", arrow::int64(), /*nullable=*/true),
    });
}

Result<std::shared_ptr<arrow::RecordBatch>> SnapshotsSystemTable::BuildRecordBatch(
    arrow::MemoryPool* pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    SnapshotManager snapshot_manager(fs_, TablePath(), branch_);
    PAIMON_ASSIGN_OR_RAISE(std::vector<Snapshot> snapshots, snapshot_manager.GetAllSnapshots());
    std::sort(snapshots.begin(), snapshots.end(),
              [](const Snapshot& lhs, const Snapshot& rhs) { return lhs.Id() < rhs.Id(); });
    MetadataRecordBatchBuilder rows(schema, pool);
    PAIMON_RETURN_NOT_OK(rows.InitStatus());

    for (const auto& snapshot : snapshots) {
        PAIMON_RETURN_NOT_OK(rows.AppendInt64(0, snapshot.Id()));
        PAIMON_RETURN_NOT_OK(rows.AppendInt64(1, snapshot.SchemaId()));
        PAIMON_RETURN_NOT_OK(rows.AppendString(2, snapshot.CommitUser()));
        PAIMON_RETURN_NOT_OK(rows.AppendInt64(3, snapshot.CommitIdentifier()));
        PAIMON_RETURN_NOT_OK(
            rows.AppendString(4, Snapshot::CommitKind::ToString(snapshot.GetCommitKind())));
        PAIMON_RETURN_NOT_OK(rows.AppendInt64(5, snapshot.TimeMillis()));
        PAIMON_RETURN_NOT_OK(rows.AppendOptionalInt64(6, snapshot.TotalRecordCount()));
        PAIMON_RETURN_NOT_OK(rows.AppendOptionalInt64(7, snapshot.DeltaRecordCount()));
        PAIMON_RETURN_NOT_OK(rows.AppendOptionalInt64(8, snapshot.ChangelogRecordCount()));
        PAIMON_RETURN_NOT_OK(rows.AppendOptionalInt64(9, snapshot.Watermark()));
    }

    return rows.Finish();
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
    });
}

Result<std::shared_ptr<arrow::RecordBatch>> SchemasSystemTable::BuildRecordBatch(
    arrow::MemoryPool* pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    SchemaManager schema_manager(fs_, TablePath(), branch_);
    PAIMON_ASSIGN_OR_RAISE(std::vector<int64_t> schema_ids, schema_manager.ListAllIds());
    std::sort(schema_ids.begin(), schema_ids.end());
    MetadataRecordBatchBuilder rows(schema, pool);
    PAIMON_RETURN_NOT_OK(rows.InitStatus());

    for (int64_t id : schema_ids) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> table_schema,
                               schema_manager.ReadSchema(id));
        PAIMON_ASSIGN_OR_RAISE(std::string json_schema, table_schema->GetJsonSchema());
        PAIMON_ASSIGN_OR_RAISE(std::string partition_keys_json,
                               JsonString(table_schema->PartitionKeys()));
        PAIMON_ASSIGN_OR_RAISE(std::string primary_keys_json,
                               JsonString(table_schema->PrimaryKeys()));
        PAIMON_ASSIGN_OR_RAISE(std::string options_json, JsonString(table_schema->Options()));

        PAIMON_RETURN_NOT_OK(rows.AppendInt64(0, table_schema->Id()));
        PAIMON_RETURN_NOT_OK(rows.AppendString(1, json_schema));
        PAIMON_RETURN_NOT_OK(rows.AppendString(2, partition_keys_json));
        PAIMON_RETURN_NOT_OK(rows.AppendString(3, primary_keys_json));
        PAIMON_RETURN_NOT_OK(rows.AppendString(4, options_json));
        PAIMON_RETURN_NOT_OK(rows.AppendOptionalString(5, table_schema->Comment()));
    }

    return rows.Finish();
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
        arrow::field("tag_create_time", arrow::utf8(), /*nullable=*/true),
        arrow::field("tag_time_retained", arrow::float64(), /*nullable=*/true),
    });
}

Result<std::shared_ptr<arrow::RecordBatch>> TagsSystemTable::BuildRecordBatch(
    arrow::MemoryPool* pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    TagManager tag_manager(fs_, TablePath(), branch_);
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> tag_names, tag_manager.ListTagNames());
    MetadataRecordBatchBuilder rows(schema, pool);
    PAIMON_RETURN_NOT_OK(rows.InitStatus());

    for (const auto& name : tag_names) {
        PAIMON_ASSIGN_OR_RAISE(Tag tag, tag_manager.GetOrThrow(name));
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> tag_create_time_json,
                               OptionalJson(tag.TagCreateTime()));
        PAIMON_RETURN_NOT_OK(rows.AppendString(0, name));
        PAIMON_RETURN_NOT_OK(rows.AppendInt64(1, tag.Id()));
        PAIMON_RETURN_NOT_OK(rows.AppendInt64(2, tag.SchemaId()));
        PAIMON_RETURN_NOT_OK(rows.AppendOptionalString(3, tag_create_time_json));
        PAIMON_RETURN_NOT_OK(rows.AppendOptionalDouble(4, tag.TagTimeRetained()));
    }

    return rows.Finish();
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
        arrow::field("schema_id", arrow::int64(), /*nullable=*/true),
        arrow::field("snapshot_id", arrow::int64(), /*nullable=*/true),
    });
}

Result<std::shared_ptr<arrow::RecordBatch>> BranchesSystemTable::BuildRecordBatch(
    arrow::MemoryPool* pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> branches,
                           BranchManager::ListBranches(fs_, TablePath()));
    MetadataRecordBatchBuilder rows(schema, pool);
    PAIMON_RETURN_NOT_OK(rows.InitStatus());

    for (const auto& name : branches) {
        SchemaManager schema_manager(fs_, TablePath(), name);
        SnapshotManager snapshot_manager(fs_, TablePath(), name);
        PAIMON_ASSIGN_OR_RAISE(std::vector<int64_t> schema_ids, schema_manager.ListAllIds());
        std::optional<int64_t> latest_schema_id;
        if (!schema_ids.empty()) {
            latest_schema_id = *std::max_element(schema_ids.begin(), schema_ids.end());
        }
        PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> latest_snapshot_id,
                               snapshot_manager.LatestSnapshotId());

        PAIMON_RETURN_NOT_OK(rows.AppendString(0, name));
        PAIMON_RETURN_NOT_OK(rows.AppendOptionalInt64(1, latest_schema_id));
        PAIMON_RETURN_NOT_OK(rows.AppendOptionalInt64(2, latest_snapshot_id));
    }

    return rows.Finish();
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
        arrow::field("next_snapshot_id", arrow::int64(), /*nullable=*/true),
    });
}

Result<std::shared_ptr<arrow::RecordBatch>> ConsumersSystemTable::BuildRecordBatch(
    arrow::MemoryPool* pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    ConsumerManager consumer_manager(fs_, TablePath(), branch_);
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> consumers, consumer_manager.ListConsumers());
    MetadataRecordBatchBuilder rows(schema, pool);
    PAIMON_RETURN_NOT_OK(rows.InitStatus());

    for (const auto& id : consumers) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> snapshot_id,
                               consumer_manager.GetNextSnapshotId(id));
        PAIMON_RETURN_NOT_OK(rows.AppendString(0, id));
        PAIMON_RETURN_NOT_OK(rows.AppendOptionalInt64(1, snapshot_id));
    }

    return rows.Finish();
}

}  // namespace paimon
