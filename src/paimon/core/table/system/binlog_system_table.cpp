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
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/defs.h"
#include "paimon/read_context.h"
#include "paimon/table/source/table_read.h"

namespace paimon {
namespace {

class BinlogBatchConverter : public ChangelogBatchConverter {
 public:
    Result<std::shared_ptr<arrow::Array>> ConvertDataColumn(
        const std::shared_ptr<arrow::Array>& array, arrow::MemoryPool* pool) const override {
        arrow::Int32Builder offsets_builder(pool);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(offsets_builder.Reserve(array->length() + 1));
        for (int64_t i = 0; i <= array->length(); ++i) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(offsets_builder.Append(static_cast<int32_t>(i)));
        }
        std::shared_ptr<arrow::Array> offsets_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(offsets_builder.Finish(&offsets_array));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> list_array,
            arrow::ListArray::FromArrays(*offsets_array, *array, pool));
        return list_array;
    }
};

}  // namespace

BinlogSystemTable::BinlogSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                     std::shared_ptr<TableSchema> table_schema,
                                     std::map<std::string, std::string> options)
    : AuditLogSystemTable(std::move(fs), std::move(table_path), std::move(table_schema),
                          std::move(options)) {}

std::string BinlogSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> BinlogSystemTable::ArrowSchema() const {
    std::shared_ptr<arrow::Field> rowkind_field =
        DataField::ConvertDataFieldToArrowField(SpecialFields::RowKind());
    rowkind_field = rowkind_field->WithNullable(false);
    arrow::FieldVector fields = {rowkind_field};
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(options_));
    if (core_options.TableReadSequenceNumberEnabled()) {
        fields.push_back(DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber()));
    }
    for (const auto& field : table_schema_->Fields()) {
        auto arrow_field = field.ArrowField();
        fields.push_back(arrow::field(arrow_field->name(), arrow::list(arrow_field->type()),
                                      arrow_field->nullable()));
    }
    return arrow::schema(fields);
}

Result<std::unique_ptr<TableRead>> BinlogSystemTable::NewRead(
    const std::shared_ptr<ReadContext>& context) const {
    return NewChangelogRead(context, std::make_shared<BinlogBatchConverter>());
}

}  // namespace paimon
