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

#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/result.h"
#include "paimon/schema/schema.h"

namespace paimon {

class SystemTableSchema : public SystemSchema {
 public:
    explicit SystemTableSchema(std::shared_ptr<arrow::Schema> schema);

    Result<std::unique_ptr<::ArrowSchema>> GetArrowSchema() const override;
    Result<std::string> GetJsonSchema() const override;
    std::vector<std::string> FieldNames() const override;
    Result<FieldType> GetFieldType(const std::string& field_name) const override;
    std::optional<std::string> Comment() const override;

 private:
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::string> field_names_;
};

}  // namespace paimon
