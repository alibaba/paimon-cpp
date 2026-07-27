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
#include <set>
#include <vector>

#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

namespace arrow {
class DataType;
class Field;
class Schema;
class KeyValueMetadata;
}  // namespace arrow

namespace paimon {
class PAIMON_EXPORT ArrowSchemaValidator {
 public:
    ArrowSchemaValidator() = delete;
    ~ArrowSchemaValidator() = delete;
    static Status ValidateSchema(const arrow::Schema& schema);

    static Status ValidateSchemaWithFieldId(const arrow::Schema& schema);

    static Status ValidateNoRedundantFields(const arrow::FieldVector& fields);

    static Status ValidateNoWhitespaceOnlyFields(const arrow::FieldVector& fields);

    static Status ValidateField(const std::shared_ptr<arrow::Field>& field);

    static bool ContainTimestampWithTimezone(const arrow::DataType& type);

    static bool IsNestedType(const std::shared_ptr<arrow::DataType>& data_type);

 private:
    static Status ValidateDataTypeWithFieldId(
        const std::shared_ptr<arrow::DataType>& type,
        const std::shared_ptr<const arrow::KeyValueMetadata>& key_value_metadata, bool allow_blob,
        std::set<int32_t>* field_id_set);
    static Status ValidateField(const std::shared_ptr<arrow::Field>& field, bool allow_blob);
};
}  // namespace paimon
