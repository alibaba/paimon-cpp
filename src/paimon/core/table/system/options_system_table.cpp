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
#include "arrow/util/checked_cast.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/core/io/generic_row_to_arrow_array_converter.h"
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
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    std::vector<GenericRow> rows;
    rows.reserve(table_schema_->Options().size());
    for (const auto& [key, value] : table_schema_->Options()) {
        GenericRow row(schema->num_fields());
        row.SetField(0, std::string_view(key));
        row.SetField(1, std::string_view(value));
        rows.push_back(std::move(row));
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GenericRowToArrowArrayConverter> converter,
                           GenericRowToArrowArrayConverter::Create(schema, pool));
    PAIMON_ASSIGN_OR_RAISE(auto array, converter->NextBatch(rows));
    auto struct_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(array);
    return arrow::RecordBatch::Make(schema, struct_array->length(), struct_array->fields());
}

}  // namespace paimon
