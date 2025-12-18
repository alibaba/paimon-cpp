/*
 * Copyright 2025-present Alibaba Inc.
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
#include <string>
#include <vector>

#include "paimon/core/schema/table_schema.h"

namespace paimon {

class SchemaImpl : public Schema {
 public:
    explicit SchemaImpl(const std::shared_ptr<TableSchema>& table_schema)
        : table_schema_(table_schema) {}
    Result<std::unique_ptr<::ArrowSchema>> GetArrowSchema() const override {
        return table_schema_->GetArrowSchema();
    }
    std::vector<std::string> FieldNames() const override {
        return table_schema_->FieldNames();
    }
    int64_t Id() const override {
        return table_schema_->Id();
    }
    const std::vector<std::string>& PrimaryKeys() const override {
        return table_schema_->PrimaryKeys();
    }
    const std::vector<std::string>& PartitionKeys() const override {
        return table_schema_->PartitionKeys();
    }
    const std::vector<std::string>& BucketKeys() const override {
        return table_schema_->BucketKeys();
    }
    int32_t NumBuckets() const override {
        return table_schema_->NumBuckets();
    }
    int32_t HighestFieldId() const override {
        return table_schema_->HighestFieldId();
    }
    const std::map<std::string, std::string>& Options() const override {
        return table_schema_->Options();
    }
    std::optional<std::string> Comment() const override {
        return table_schema_->Comment();
    }

 private:
    std::shared_ptr<TableSchema> table_schema_;
};

}  // namespace paimon
