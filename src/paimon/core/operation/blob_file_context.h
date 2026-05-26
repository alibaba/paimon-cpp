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

#include <memory>
#include <optional>
#include <set>
#include <string>

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class CoreOptions;

/// Context that classifies BLOB fields into different storage categories.
///
/// Categories:
///   - descriptor_fields: stored as BlobDescriptor bytes inline in the main data file.
///   - view_fields: stored as BlobViewStruct bytes inline in the main data file.
///   - inline_fields: descriptor_fields ∪ view_fields. These stay in the main data file.
///   - external_storage_fields: subset of descriptor_fields whose raw data is written to an
///     external storage path (the descriptor still goes into the main data file).
///   - blob_file_fields: BLOB fields that are NOT inline. These go into separate .blob files.
class BlobFileContext {
 public:
    /// Creates a BlobFileContext from schema and options.
    /// Returns nullptr if the schema has no BLOB fields at all.
    /// Otherwise always returns a valid context (even if all blobs are inline).
    static std::unique_ptr<BlobFileContext> Create(const std::shared_ptr<arrow::Schema>& schema,
                                                   const CoreOptions& options);

    /// Returns true if the given field should be stored inline in the main data file
    /// (either as descriptor bytes or view bytes).
    bool IsInlineField(const std::string& field_name) const;

    /// Returns true if the given field should be written to a separate .blob file.
    bool IsBlobFileField(const std::string& field_name) const;

    /// Returns true if the given field is a descriptor field.
    bool IsDescriptorField(const std::string& field_name) const;

    /// Returns true if the given field is a view field.
    bool IsViewField(const std::string& field_name) const;

    /// Returns true if the given field should be written to external storage.
    bool IsExternalStorageField(const std::string& field_name) const;

    /// Returns true if there are any BLOB fields that need a .blob file writer.
    bool RequireBlobFileWriter() const;

    /// Returns true if there are any external storage fields that need an external writer.
    bool RequireExternalStorageWriter() const;

    const std::set<std::string>& GetDescriptorFields() const {
        return descriptor_fields_;
    }

    const std::set<std::string>& GetViewFields() const {
        return view_fields_;
    }

    const std::set<std::string>& GetInlineFields() const {
        return inline_fields_;
    }

    const std::set<std::string>& GetExternalStorageFields() const {
        return external_storage_fields_;
    }

    const std::set<std::string>& GetBlobFileFields() const {
        return blob_file_fields_;
    }

    const std::optional<std::string>& GetExternalStoragePath() const {
        return external_storage_path_;
    }

 private:
    BlobFileContext(std::set<std::string> descriptor_fields, std::set<std::string> view_fields,
                    std::set<std::string> inline_fields,
                    std::set<std::string> external_storage_fields,
                    std::set<std::string> blob_file_fields,
                    std::optional<std::string> external_storage_path);

    std::set<std::string> descriptor_fields_;
    std::set<std::string> view_fields_;
    std::set<std::string> inline_fields_;
    std::set<std::string> external_storage_fields_;
    std::set<std::string> blob_file_fields_;
    std::optional<std::string> external_storage_path_;
};

}  // namespace paimon
