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

#include "paimon/common/data/blob_utils.h"

#include <cstddef>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_nested.h"
#include "arrow/type.h"
#include "paimon/common/data/blob_defs.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/string_utils.h"

namespace arrow {
class Array;
}

namespace paimon {

BlobUtils::SeparatedSchemas BlobUtils::SeparateBlobSchema(
    const std::shared_ptr<arrow::Schema>& schema) {
    std::vector<std::shared_ptr<arrow::Field>> remaining_fields;
    std::vector<std::shared_ptr<arrow::Field>> blob_fields;
    for (auto i = 0; i < schema->num_fields(); i++) {
        auto field = schema->field(i);
        if (IsBlobField(field)) {
            blob_fields.emplace_back(field);
        } else {
            remaining_fields.emplace_back(field);
        }
    }
    SeparatedSchemas result;
    result.main_schema = arrow::schema(remaining_fields);
    result.blob_schema = arrow::schema(blob_fields);
    return result;
}

Result<BlobUtils::SeparatedStructArrays> BlobUtils::SeparateBlobArray(
    const std::shared_ptr<arrow::StructArray>& struct_array) {
    std::shared_ptr<arrow::StructType> old_type =
        std::static_pointer_cast<arrow::StructType>(struct_array->type());
    const auto& old_fields = old_type->fields();
    const auto& old_arrays = struct_array->fields();

    std::vector<std::shared_ptr<arrow::Field>> remaining_fields;
    std::vector<std::shared_ptr<arrow::Array>> remaining_arrays;
    std::vector<std::shared_ptr<arrow::Field>> blob_fields;
    std::vector<std::shared_ptr<arrow::Array>> blob_arrays;

    for (size_t i = 0; i < old_fields.size(); i++) {
        if (IsBlobField(old_fields[i])) {
            blob_fields.push_back(old_fields[i]);
            blob_arrays.push_back(old_arrays[i]);
        } else {
            remaining_fields.push_back(old_fields[i]);
            remaining_arrays.push_back(old_arrays[i]);
        }
    }

    SeparatedStructArrays result;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(result.main_array,
                                      arrow::StructArray::Make(remaining_arrays, remaining_fields));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(result.blob_array,
                                      arrow::StructArray::Make(blob_arrays, blob_fields));
    return result;
}

bool BlobUtils::IsBlobField(const std::shared_ptr<arrow::Field>& field) {
    const auto& type = field->type();
    if (type->id() != arrow::Type::LARGE_BINARY) {
        return false;
    }
    if (!field->HasMetadata()) {
        return false;
    }
    return IsBlobMetadata(field->metadata());
}

bool BlobUtils::IsBlobMetadata(const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) {
    if (!metadata) {
        return false;
    }
    auto extension_name = metadata->Get(BLOB_EXTENSION_TYPE_KEY);
    if (!extension_name.ok()) {
        return false;
    }
    return extension_name.ValueUnsafe() == BLOB_EXTENSION_TYPE_VALUE;
}

bool BlobUtils::IsBlobFile(const std::string& file_name) {
    return StringUtils::EndsWith(file_name, ".blob");
}

std::shared_ptr<arrow::Field> BlobUtils::ToArrowField(
    const std::string& field_name, bool nullable,
    std::unordered_map<std::string, std::string> metadata) {
    metadata[BLOB_EXTENSION_TYPE_KEY] = BLOB_EXTENSION_TYPE_VALUE;
    return arrow::field(field_name, arrow::large_binary(), nullable,
                        std::make_shared<arrow::KeyValueMetadata>(metadata));
}
}  // namespace paimon
