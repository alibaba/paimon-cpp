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

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "paimon/result.h"
#include "paimon/visibility.h"

namespace arrow {
class Field;
class KeyValueMetadata;
class Schema;
class StructArray;
}  // namespace arrow

namespace paimon {
class DataField;
}  // namespace paimon

namespace paimon {
/// Utils for blob type.
class PAIMON_EXPORT BlobUtils {
 public:
    BlobUtils() = delete;
    ~BlobUtils() = delete;

    struct SeparatedSchemas {
        /// Non-blob fields (includes inline blob fields when inline_fields is provided)
        std::shared_ptr<arrow::Schema> main_schema;
        /// Blob fields that go to separate .blob files
        std::shared_ptr<arrow::Schema> blob_schema;
    };

    struct SeparatedStructArrays {
        /// Non-blob fields (includes inline blob fields when inline_fields is provided)
        std::shared_ptr<arrow::StructArray> main_array;
        /// Blob fields that go to separate .blob files
        std::shared_ptr<arrow::StructArray> blob_array;
    };

    /// Separates schema with inline field awareness.
    /// BLOB fields in inline_fields stay in main_schema; others go to blob_schema.
    static SeparatedSchemas SeparateBlobSchema(const std::shared_ptr<arrow::Schema>& schema,
                                               const std::set<std::string>& inline_fields);

    /// Separates array with inline field awareness.
    /// BLOB fields in inline_fields stay in main_array; others go to blob_array.
    static Result<SeparatedStructArrays> SeparateBlobArray(
        const std::shared_ptr<arrow::StructArray>& struct_array,
        const std::set<std::string>& inline_fields);

    static bool IsBlobField(const std::shared_ptr<arrow::Field>& field);
    static bool IsBlobMetadata(const std::shared_ptr<const arrow::KeyValueMetadata>& metadata);
    static bool IsBlobFile(const std::string& file_name);

    static std::shared_ptr<arrow::Field> ToArrowField(
        const std::string& field_name, bool nullable = false,
        std::unordered_map<std::string, std::string> metadata = {});

    static Status ValidateInlineBlobDescriptors(
        const std::shared_ptr<arrow::StructArray>& struct_array,
        const std::set<std::string>& inline_descriptor_fields);

    /// Converts inline blob DataFields from large_binary to binary type.
    /// Inline blob fields use large_binary in the table schema (because they are BLOB type),
    /// but are stored as binary in data files. This conversion aligns the field type with
    /// the actual on-disk storage format for correct reading.
    static std::vector<DataField> ConvertBlobInlineDataFields(
        const std::vector<DataField>& data_fields,
        const std::vector<std::string>& blob_inline_fields);
};

}  // namespace paimon
