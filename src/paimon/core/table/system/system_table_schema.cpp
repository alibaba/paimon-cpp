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

#include "paimon/core/table/system/system_table_schema.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/status.h"

namespace paimon {

SystemTableSchema::SystemTableSchema(std::shared_ptr<arrow::Schema> schema)
    : schema_(std::move(schema)) {
    field_names_ = schema_->field_names();
}

Result<std::unique_ptr<::ArrowSchema>> SystemTableSchema::GetArrowSchema() const {
    auto c_schema = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema_, c_schema.get()));
    return c_schema;
}

Result<std::string> SystemTableSchema::GetJsonSchema() const {
    return Status::NotImplemented("system table JSON schema is not supported");
}

std::vector<std::string> SystemTableSchema::FieldNames() const {
    return field_names_;
}

Result<FieldType> SystemTableSchema::GetFieldType(const std::string& field_name) const {
    auto field = schema_->GetFieldByName(field_name);
    if (!field) {
        return Status::NotExist("field ", field_name, " not exist in system table schema");
    }
    return FieldTypeUtils::ConvertToFieldType(field->type()->id());
}

std::optional<std::string> SystemTableSchema::Comment() const {
    return std::nullopt;
}

}  // namespace paimon
