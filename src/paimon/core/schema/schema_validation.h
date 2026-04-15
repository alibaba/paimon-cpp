/*
 * Copyright 2024-present Alibaba Inc.
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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "paimon/core/core_options.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/status.h"

namespace arrow {
class DataType;
class Field;
}  // namespace arrow

namespace paimon {
class CoreOptions;
class DataField;
class TableSchema;

/// Validation utils for `TableSchema`.
class SchemaValidation {
 public:
    SchemaValidation() = delete;
    ~SchemaValidation() = delete;

    static Status ValidateTableSchema(const TableSchema& schema);

    static bool IsPostponeBucketTable(const TableSchema& schema, int32_t bucket);

 private:
    static Status ValidateNoDuplicateField(const std::vector<std::string>& field_names,
                                           const std::string& error_message_intro);
    static Status ValidateOnlyContainPrimitiveType(const std::vector<DataField>& fields,
                                                   const std::vector<std::string>& field_names,
                                                   const std::string& error_message_intro);
    static Status ValidateNotContainComplexType(const std::vector<DataField>& fields,
                                                const std::vector<std::string>& field_names);
    static Status ValidateBucket(const TableSchema& schema, const CoreOptions& options);
    static Status ValidateDefaultValues(const TableSchema& schema) {
        return Status::NotImplemented("validate default values not implemented");
    }
    static Status ValidateStartupMode(const CoreOptions& options) {
        return Status::NotImplemented("validate startup mode not implemented");
    }
    static Status ValidateFieldsPrefix(const TableSchema& schema, const CoreOptions& options);
    static Status ValidateSequenceField(const TableSchema& schema, const CoreOptions& options);
    static Status ValidateSequenceGroup(const TableSchema& schema, const CoreOptions& options);
    static Status ValidateChangelogProducer(const CoreOptions& options);
    static Status ValidateForDeletionVectors(const CoreOptions& options);

    static Status ValidateRowTracking(const TableSchema& table_schema, const CoreOptions& options);

    static bool IsComplexType(const std::shared_ptr<arrow::Field>& field);
};

}  // namespace paimon
