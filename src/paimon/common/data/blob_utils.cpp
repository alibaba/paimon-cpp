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
#include <set>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_nested.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/data/blob_defs.h"
#include "paimon/common/data/blob_descriptor.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/string_utils.h"
namespace arrow {
class Array;
}

namespace paimon {
BlobUtils::SeparatedSchemas BlobUtils::SeparateBlobSchema(
    const std::shared_ptr<arrow::Schema>& schema, const std::set<std::string>& inline_fields) {
    std::vector<std::shared_ptr<arrow::Field>> main_fields;
    std::vector<std::shared_ptr<arrow::Field>> blob_fields;
    for (int32_t i = 0; i < schema->num_fields(); i++) {
        auto field = schema->field(i);
        if (IsBlobField(field) && inline_fields.count(field->name()) == 0) {
            // Non-inline BLOB -> goes to blob file
            blob_fields.emplace_back(field);
        } else {
            // Non-blob fields OR inline BLOB fields -> stay in main
            main_fields.emplace_back(field);
        }
    }
    SeparatedSchemas result;
    result.main_schema = arrow::schema(main_fields);
    result.blob_schema = arrow::schema(blob_fields);
    return result;
}

Result<BlobUtils::SeparatedStructArrays> BlobUtils::SeparateBlobArray(
    const std::shared_ptr<arrow::StructArray>& struct_array,
    const std::set<std::string>& inline_fields) {
    std::shared_ptr<arrow::StructType> old_type =
        std::static_pointer_cast<arrow::StructType>(struct_array->type());
    const auto& old_fields = old_type->fields();
    const auto& old_arrays = struct_array->fields();

    arrow::ArrayVector main_arrays;
    arrow::ArrayVector blob_arrays;
    arrow::FieldVector main_fields;
    arrow::FieldVector blob_fields;

    for (size_t i = 0; i < old_fields.size(); i++) {
        if (IsBlobField(old_fields[i]) && inline_fields.count(old_fields[i]->name()) == 0) {
            blob_fields.push_back(old_fields[i]);
            blob_arrays.push_back(old_arrays[i]);
        } else {
            main_fields.push_back(old_fields[i]);
            main_arrays.push_back(old_arrays[i]);
        }
    }

    if (blob_fields.empty()) {
        return Status::Invalid(
            "SeparateBlobArray expects at least one non-inline blob field, but got none.");
    }
    if (main_fields.empty()) {
        return Status::Invalid("SeparateBlobArray expects at least one main field, but got none.");
    }

    SeparatedStructArrays result;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(result.main_array,
                                      arrow::StructArray::Make(main_arrays, main_fields));
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
    auto extension_name = metadata->Get(BlobDefs::kExtensionTypeKey);
    if (!extension_name.ok()) {
        return false;
    }
    return extension_name.ValueUnsafe() == BlobDefs::kExtensionTypeValue;
}

bool BlobUtils::IsBlobFile(const std::string& file_name) {
    return StringUtils::EndsWith(file_name, ".blob");
}

std::shared_ptr<arrow::Field> BlobUtils::ToArrowField(
    const std::string& field_name, bool nullable,
    std::unordered_map<std::string, std::string> metadata) {
    metadata[BlobDefs::kExtensionTypeKey] = BlobDefs::kExtensionTypeValue;
    return arrow::field(field_name, arrow::large_binary(), nullable,
                        std::make_shared<arrow::KeyValueMetadata>(metadata));
}

Status BlobUtils::ValidateInlineBlobDescriptors(
    const std::shared_ptr<arrow::StructArray>& struct_array,
    const std::set<std::string>& inline_descriptor_fields) {
    if (inline_descriptor_fields.empty()) {
        return Status::OK();
    }
    if (!struct_array) {
        return Status::Invalid("array in ValidateInlineBlobDescriptors must be a struct_array");
    }
    for (const auto& field_name : inline_descriptor_fields) {
        auto field_array = struct_array->GetFieldByName(field_name);
        if (!field_array) {
            continue;
        }
        const auto* binary_array =
            arrow::internal::checked_cast<const arrow::LargeBinaryArray*>(field_array.get());
        if (!binary_array) {
            return Status::Invalid(
                fmt::format("cannot cast array for field {} to LargeBinaryArray", field_name));
        }
        for (int64_t row = 0; row < binary_array->length(); ++row) {
            if (binary_array->IsNull(row)) {
                continue;
            }
            auto value = binary_array->GetView(row);
            PAIMON_ASSIGN_OR_RAISE(bool is_descriptor,
                                   BlobDescriptor::IsBlobDescriptor(value.data(), value.size()));
            if (!is_descriptor) {
                return Status::Invalid(fmt::format(
                    "BLOB inline field {} configured by blob-descriptor-field or blob-view-field "
                    "require values to be a BlobDescriptor or BlobViewStruct.",
                    field_name));
            }
        }
    }
    return Status::OK();
}

std::vector<DataField> BlobUtils::ConvertBlobInlineDataFields(
    const std::vector<DataField>& data_fields, const std::vector<std::string>& blob_inline_fields) {
    if (blob_inline_fields.empty()) {
        return data_fields;
    }

    std::set<std::string> blob_inline_field_set(blob_inline_fields.begin(),
                                                blob_inline_fields.end());
    std::vector<DataField> converted_fields;
    converted_fields.reserve(data_fields.size());
    for (const auto& data_field : data_fields) {
        if (blob_inline_field_set.find(data_field.Name()) == blob_inline_field_set.end()) {
            converted_fields.push_back(data_field);
            continue;
        }

        auto binary_field = arrow::field(data_field.Name(), arrow::binary(), data_field.Nullable(),
                                         data_field.ArrowField()->metadata());
        converted_fields.emplace_back(data_field.Id(), binary_field, data_field.Description());
    }
    return converted_fields;
}

}  // namespace paimon
