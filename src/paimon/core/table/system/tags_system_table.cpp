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

#include "paimon/core/table/system/tags_system_table.h"

#include <utility>

#include "arrow/api.h"
#include "paimon/core/tag/tag.h"
#include "paimon/core/utils/tag_manager.h"

namespace paimon {

TagsSystemTable::TagsSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                 std::string branch)
    : InMemorySystemTable(table_path),
      context_(SystemTableUtils::CreateContext(std::move(fs), std::move(table_path),
                                               std::move(branch))) {}

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
        PAIMON_ASSIGN_OR_RAISE(
            std::optional<int64_t> tag_create_time,
            SystemTableUtils::OptionalLocalDateTimePartsToTimestampMillis(tag.TagCreateTime()));
        GenericRow row(schema->num_fields());
        row.SetField(0, SystemTableUtils::StringValue(name));
        row.SetField(1, tag.Id());
        row.SetField(2, tag.SchemaId());
        PAIMON_ASSIGN_OR_RAISE(VariantType commit_time,
                               SystemTableUtils::LocalTimestampMillisValue(tag.TimeMillis()));
        row.SetField(3, commit_time);
        row.SetField(4, SystemTableUtils::OptionalInt64Value(tag.TotalRecordCount()));
        row.SetField(5, SystemTableUtils::OptionalTimestampMillisValue(tag_create_time));
        row.SetField(6, SystemTableUtils::OptionalStringValue(
                            SystemTableUtils::OptionalDoubleToString(tag.TagTimeRetained())));
        rows.push_back(std::move(row));
    }

    return rows;
}

}  // namespace paimon
