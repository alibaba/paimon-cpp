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

namespace paimon {

/// Blob file format constants shared between writer and reader.
///
/// A Blob field uses the 'large_binary' type as its underlying physical storage in Apache Arrow
/// Schema, and is marked as the Paimon Blob extension type by attaching specific
/// **KeyValueMetadata**. Multiple blob fields in one paimon table are supported.
class BlobDefs {
 public:
    BlobDefs() = delete;
    ~BlobDefs() = delete;

    /// To create a Blob field:
    /// @code
    ///   std::unordered_map<std::string, std::string> blob_metadata_map = {
    ///       {Blob::EXTENSION_TYPE_KEY, Blob::EXTENSION_TYPE_VALUE}
    ///   };
    ///   auto field = arrow::field("my_blob_field", arrow::large_binary(), false,
    ///       std::make_shared<arrow::KeyValueMetadata>(blob_metadata_map));
    /// @endcode
    /// Metadata key identifying a Paimon Blob extension type field.
    static constexpr char kExtensionTypeKey[] = "paimon.extension.type";
    /// Metadata value identifying a Paimon Blob extension type field.
    static constexpr char kExtensionTypeValue[] = "paimon.type.blob";

    /// A bin_length value of -1 in the index indicates a null blob entry.
    static constexpr int64_t kNullBinLength = -1;
    /// Blob file format version.
    static constexpr int8_t kFileVersion = 1;
    /// Magic number identifying the start of each blob bin.
    static constexpr int32_t kMagicNumber = 1481511375;
    /// Offset from the start of a bin to the actual blob content (magic number size).
    static constexpr int32_t kContentStartOffset = 4;
    /// Total metadata length per bin: magic(4) + bin_length(8) + crc32(4) = 16.
    static constexpr int32_t kTotalMetaLength = 16;
    /// Blob file header length: index_len(4) + version(1) = 5.
    static constexpr uint32_t kBlobFileHeaderLength = 5;
};

}  // namespace paimon
