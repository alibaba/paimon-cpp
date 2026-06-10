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

#include "paimon/core/operation/blob_file_context.h"

#include <utility>

#include "arrow/type.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/core/core_options.h"

namespace paimon {

BlobFileContext::BlobFileContext(std::set<std::string> descriptor_fields,
                                 std::set<std::string> view_fields,
                                 std::set<std::string> inline_fields,
                                 std::set<std::string> external_storage_fields,
                                 std::set<std::string> blob_file_fields,
                                 std::optional<std::string> external_storage_path)
    : descriptor_fields_(std::move(descriptor_fields)),
      view_fields_(std::move(view_fields)),
      inline_fields_(std::move(inline_fields)),
      external_storage_fields_(std::move(external_storage_fields)),
      blob_file_fields_(std::move(blob_file_fields)),
      external_storage_path_(std::move(external_storage_path)) {}

std::unique_ptr<BlobFileContext> BlobFileContext::Create(
    const std::shared_ptr<arrow::Schema>& schema, const CoreOptions& options) {
    // Collect the BLOB field names that are present in the given schema. The schema may
    // only contain a subset of the table columns (e.g. a projected read/write schema), so all
    // field categories below must be derived from this set rather than from the options
    // alone, which describe the full table.
    std::set<std::string> schema_blob_fields;
    for (int i = 0; i < schema->num_fields(); ++i) {
        const auto& field = schema->field(i);
        if (BlobUtils::IsBlobField(field)) {
            schema_blob_fields.insert(field->name());
        }
    }
    if (schema_blob_fields.empty()) {
        return nullptr;
    }

    // Populate descriptor fields
    std::set<std::string> descriptor_fields;
    for (const auto& name : options.GetBlobDescriptorFields()) {
        if (schema_blob_fields.count(name) > 0) {
            descriptor_fields.insert(name);
        }
    }

    // Populate view fields
    std::set<std::string> view_fields;
    for (const auto& name : options.GetBlobViewFields()) {
        if (schema_blob_fields.count(name) > 0) {
            view_fields.insert(name);
        }
    }

    // Populate inline fields from options (descriptor ∪ view)
    std::set<std::string> inline_fields;
    for (const auto& name : options.GetBlobInlineFields()) {
        if (schema_blob_fields.count(name) > 0) {
            inline_fields.insert(name);
        }
    }

    // Populate external storage fields
    std::set<std::string> external_storage_fields;
    for (const auto& name : options.GetBlobExternalStorageFields()) {
        if (schema_blob_fields.count(name) > 0) {
            external_storage_fields.insert(name);
        }
    }

    // Populate external storage path
    std::optional<std::string> external_storage_path = options.GetBlobExternalStoragePath();

    // Determine blob_file_fields: schema BLOB fields that are NOT inline
    std::set<std::string> blob_file_fields;
    for (const auto& name : schema_blob_fields) {
        if (inline_fields.count(name) == 0) {
            blob_file_fields.insert(name);
        }
    }

    return std::unique_ptr<BlobFileContext>(
        new BlobFileContext(std::move(descriptor_fields), std::move(view_fields),
                            std::move(inline_fields), std::move(external_storage_fields),
                            std::move(blob_file_fields), std::move(external_storage_path)));
}

bool BlobFileContext::RequireBlobFileWriter() const {
    return !blob_file_fields_.empty();
}

bool BlobFileContext::RequireExternalStorageWriter() const {
    return !external_storage_fields_.empty();
}

}  // namespace paimon
