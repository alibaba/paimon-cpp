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

#include "paimon/core/table/system/schemas_system_table.h"

#include <algorithm>
#include <utility>

#include "arrow/api.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
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
        return Status::Invalid("failed to serialize schemas system table value");
    }
    return std::string(buffer.GetString(), buffer.GetSize());
}

}  // namespace

SchemasSystemTable::SchemasSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                       std::string branch)
    : InMemorySystemTable(table_path),
      context_(SystemTableUtils::CreateContext(std::move(fs), std::move(table_path),
                                               std::move(branch))) {}

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
        row.SetField(1, SystemTableUtils::StringValue(fields_json));
        row.SetField(2, SystemTableUtils::StringValue(partition_keys_json));
        row.SetField(3, SystemTableUtils::StringValue(primary_keys_json));
        row.SetField(4, SystemTableUtils::StringValue(options_json));
        row.SetField(5, SystemTableUtils::OptionalStringValue(table_schema->Comment()));
        PAIMON_ASSIGN_OR_RAISE(VariantType update_time, SystemTableUtils::LocalTimestampMillisValue(
                                                            table_schema->TimeMillis()));
        row.SetField(6, update_time);
        rows.push_back(std::move(row));
    }

    return rows;
}

}  // namespace paimon
