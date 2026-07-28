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
#include <cstring>
#include <map>
#include <string>
#include <string_view>

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
    ///       {Blob::kExtensionTypeKey, Blob::kExtensionTypeValue}
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
    /// A bin_length value of -2 in the index indicates a placeholder blob entry, written by
    /// data-evolution partial updates for rows whose blob value is not updated. A placeholder
    /// entry occupies no file space; readers must fall back to an older blob file covering the
    /// same row to resolve the value. Aligned with Java's BlobFormatWriter.PLACE_HOLDER_LENGTH.
    static constexpr int64_t kPlaceholderBinLength = -2;
    /// Sentinel bytes standing for a placeholder blob value in two internal channels:
    ///
    /// - Write channel: a data-evolution partial update (a blob-only column write, see
    ///   kWritePlaceholderKey) marks a not-updated row with these bytes, and the blob format
    ///   writer persists it as a bin_length -2 entry. Use PlaceholderSentinelView() to build
    ///   such write arrays. Outside that mode the writer never interprets values, so arbitrary
    ///   user bytes can never be turned into a placeholder entry.
    /// - Read channel: a placeholder-aware reader (see kEmitPlaceholderSentinelKey) emits these
    ///   bytes for -2 entries so the fallback merge can identify placeholders after the batch
    ///   has passed through schema-mapping readers.
    ///
    /// Both channels identify a placeholder by exact byte equality with this internal reserved
    /// value (IsPlaceholderSentinel), and the fallback merge byte-compares every layer of a
    /// bunch — including files written outside the write channel. A user blob whose bytes
    /// exactly equal the marker therefore collides with it in two ways: written through the
    /// partial-update channel it is persisted as a placeholder entry, which a single-layer read
    /// rejects loudly (no older layer can resolve it); left untouched in an older layer under a
    /// later partial update it reads as a placeholder in every layer and silently degrades to a
    /// null blob. The marker is distinctive enough that these collisions are accepted as
    /// negligibly improbable. Sentinel bytes are never stored in blob files.
    static constexpr char kPlaceholderSentinel[] = "_PAIMON_BLOB_PLACEHOLDER";
    /// Byte length of kPlaceholderSentinel, excluding the literal's terminating NUL.
    static constexpr int32_t kPlaceholderSentinelLength = sizeof(kPlaceholderSentinel) - 1;
    /// Internal (non user-facing) format option, "false" by default: when "true", the blob
    /// reader emits kPlaceholderSentinel for placeholder entries instead of failing on them.
    /// Only the data-evolution blob fallback read path sets this.
    static constexpr char kEmitPlaceholderSentinelKey[] = "blob.internal.emit-placeholder-sentinel";
    /// Internal (non user-facing) format option, "false" by default: when "true", the blob
    /// format writer persists a value exactly equal to kPlaceholderSentinel as a bin_length -2
    /// entry. Only set for data-evolution partial updates, i.e. blob-only column writes of a
    /// table with data evolution enabled; all other writes store bytes verbatim.
    static constexpr char kWritePlaceholderKey[] = "blob.internal.write-placeholder";

    /// The sentinel bytes for building a data-evolution partial-update write array: a row equal
    /// to this view is persisted as a placeholder entry (see kWritePlaceholderKey).
    static std::string_view PlaceholderSentinelView() {
        return {kPlaceholderSentinel, static_cast<size_t>(kPlaceholderSentinelLength)};
    }

    /// True when the bytes are exactly the placeholder sentinel.
    static bool IsPlaceholderSentinel(const char* data, size_t size) {
        return size == static_cast<size_t>(kPlaceholderSentinelLength) &&
               memcmp(data, kPlaceholderSentinel, kPlaceholderSentinelLength) == 0;
    }

    /// Removes the internal placeholder option keys from a format options map. The placeholder
    /// channels must only ever be enabled by the internal data-evolution write and read paths,
    /// so every consumer building format options from user-supplied table options strips these
    /// keys before applying its own decision.
    static void EraseInternalPlaceholderOptions(std::map<std::string, std::string>* options) {
        options->erase(kEmitPlaceholderSentinelKey);
        options->erase(kWritePlaceholderKey);
    }
    /// Blob file format version.
    static constexpr int8_t kFileVersion = 1;
    /// Magic number identifying the start of each blob bin.
    static constexpr int32_t kMagicNumber = 1481511375;
    /// Offset from the start of a bin to the actual blob content (magic number size).
    static constexpr int32_t kContentStartOffset = 4;
    /// Total metadata length per bin: magic(4) + bin_length(8) + crc32(4) = 16.
    static constexpr int32_t kTotalMetaLength = 16;
    /// Blob file footer length: index_len(4) + version(1) = 5.
    static constexpr uint32_t kBlobFileFooterLength = 5;
};

}  // namespace paimon
