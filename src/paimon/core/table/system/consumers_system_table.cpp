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

#include "paimon/core/table/system/consumers_system_table.h"

#include <utility>

#include "arrow/api.h"
#include "paimon/core/utils/consumer_manager.h"

namespace paimon {

ConsumersSystemTable::ConsumersSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                           std::string branch)
    : InMemorySystemTable(table_path),
      context_(SystemTableUtils::CreateContext(std::move(fs), std::move(table_path),
                                               std::move(branch))) {}

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
        row.SetField(0, SystemTableUtils::StringValue(id));
        row.SetField(1, snapshot_id);
        rows.push_back(std::move(row));
    }

    return rows;
}

}  // namespace paimon
