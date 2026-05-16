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

#include "paimon/core/table/system/options_system_table.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
namespace {

std::shared_ptr<arrow::Schema> OptionsSchema() {
    return arrow::schema({arrow::field("key", arrow::utf8(), /*nullable=*/false),
                          arrow::field("value", arrow::utf8(), /*nullable=*/false)});
}

}  // namespace

OptionsSystemTable::OptionsSystemTable(std::string table_path,
                                       std::shared_ptr<TableSchema> table_schema)
    : InMemorySystemTable(std::move(table_path)), table_schema_(std::move(table_schema)) {}

std::string OptionsSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> OptionsSystemTable::ArrowSchema() const {
    return OptionsSchema();
}

Result<std::shared_ptr<arrow::RecordBatch>> OptionsSystemTable::BuildRecordBatch(
    arrow::MemoryPool* pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::unique_ptr<arrow::ArrayBuilder> key_array_builder,
                                      arrow::MakeBuilder(arrow::utf8(), pool));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::unique_ptr<arrow::ArrayBuilder> value_array_builder,
                                      arrow::MakeBuilder(arrow::utf8(), pool));
    auto* key_builder = dynamic_cast<arrow::StringBuilder*>(key_array_builder.get());
    auto* value_builder = dynamic_cast<arrow::StringBuilder*>(value_array_builder.get());
    if (key_builder == nullptr || value_builder == nullptr) {
        return Status::Invalid("cannot create string builders for options system table");
    }
    for (const auto& [key, value] : table_schema_->Options()) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(key_builder->Append(key));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(value_builder->Append(value));
    }
    std::shared_ptr<arrow::Array> key_array;
    std::shared_ptr<arrow::Array> value_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(key_builder->Finish(&key_array));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(value_builder->Finish(&value_array));
    return arrow::RecordBatch::Make(OptionsSchema(), key_array->length(), {key_array, value_array});
}

}  // namespace paimon
