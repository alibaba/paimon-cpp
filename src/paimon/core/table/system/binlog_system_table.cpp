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

#include "paimon/core/table/system/binlog_system_table.h"

#include <memory>
#include <string>
#include <utility>

#include "arrow/api.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/core_options.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/defs.h"

namespace paimon {

BinlogSystemTable::BinlogSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                     std::shared_ptr<TableSchema> table_schema,
                                     std::map<std::string, std::string> options)
    : AuditLogSystemTable(std::move(fs), std::move(table_path), std::move(table_schema),
                          std::move(options)) {}

std::string BinlogSystemTable::Name() const {
    return kName;
}

std::shared_ptr<arrow::Schema> BinlogSystemTable::ArrowSchema() const {
    arrow::FieldVector fields = {arrow::field("rowkind", arrow::utf8(), /*nullable=*/false)};
    auto core_options = CoreOptions::FromMap(options_);
    bool include_sequence_number =
        core_options.ok() && core_options.value().TableReadSequenceNumberEnabled();
    if (include_sequence_number) {
        fields.push_back(DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber()));
    }
    for (const auto& field : table_schema_->Fields()) {
        auto arrow_field = field.ArrowField();
        fields.push_back(arrow::field(arrow_field->name(), arrow::list(arrow_field->type()),
                                      arrow_field->nullable()));
    }
    return arrow::schema(fields);
}

}  // namespace paimon
