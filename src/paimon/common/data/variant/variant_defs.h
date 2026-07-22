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

#include <cstdint>

namespace paimon {

/// Constants of the Paimon Variant type and the Variant Binary Encoding, which follows the
/// parquet-format VariantEncoding.md specification (compatible with the Java / Spark
/// implementation).
///
/// A variant is made up of 2 binaries: value and metadata. A variant value consists of a one-byte
/// header and a number of content bytes (can be zero). The header byte is divided into upper 6
/// bits (called "type info") and lower 2 bits (called "basic type").
///
/// The variant metadata includes a version id and a dictionary of distinct strings
/// (case-sensitive). Its binary format is:
/// - Version: 1-byte unsigned integer. The only acceptable value is 1 currently.
/// - Dictionary size: `offset_size`-byte little-endian unsigned integer. The number of keys in the
///   dictionary.
/// - Offsets: (size + 1) * `offset_size`-byte little-endian unsigned integers. `offsets[i]`
///   represents the starting position of string i, counting starting from the address of
///   `offsets[0]`. Strings must be stored contiguously, so we don't need to store the string size,
///   instead, we compute it with `offset[i + 1] - offset[i]`.
/// - UTF-8 string data.
///
/// A Variant field uses `struct<value: binary not null, metadata: binary not null>` as its
/// underlying physical storage in Apache Arrow Schema, and is marked as the Paimon Variant
/// extension type by attaching specific **KeyValueMetadata** on the outer field.
class VariantDefs {
 public:
    VariantDefs() = delete;
    ~VariantDefs() = delete;

    /// Metadata key identifying a Paimon extension type field (shared with BLOB).
    static constexpr char kExtensionTypeKey[] = "paimon.extension.type";
    /// Metadata value identifying a Paimon Variant extension type field.
    static constexpr char kExtensionTypeValue[] = "paimon.type.variant";

    /// Name of the binary child field holding the variant value.
    static constexpr char kValueFieldName[] = "value";
    /// Name of the binary child field holding the variant metadata.
    static constexpr char kMetadataFieldName[] = "metadata";
    /// Name of the typed child field of a shredded variant (parquet VariantShredding.md).
    static constexpr char kTypedValueFieldName[] = "typed_value";
    /// Paimon field id of the `value` child field.
    static constexpr int32_t kValueFieldId = 0;
    /// Paimon field id of the `metadata` child field.
    static constexpr int32_t kMetadataFieldId = 1;

    static constexpr int32_t kBasicTypeBits = 2;
    static constexpr int32_t kBasicTypeMask = 0x3;
    static constexpr int32_t kTypeInfoMask = 0x3F;
    /// The inclusive maximum value of the type info value. It is the size limit of `kShortStr`.
    static constexpr int32_t kMaxShortStrSize = 0x3F;

    /// Primitive value. The type info value must be one of the primitive type values below.
    static constexpr int32_t kPrimitive = 0;
    /// Short string value. The type info value is the string size, which must be in
    /// `[0, kMaxShortStrSize]`. The string content bytes directly follow the header byte.
    static constexpr int32_t kShortStr = 1;
    /// Object value. The content contains a size, a list of field ids, a list of field offsets,
    /// and the actual field data. The length of the id list is `size`, while the length of the
    /// offset list is `size + 1`, where the last offset represents the total size of the field
    /// data. The fields in an object must be sorted by the field name in alphabetical order.
    /// Duplicate field names in one object are not allowed.
    /// The type info is 0_b4_b3b2_b1b0 (MSB is 0), where:
    /// - b4 specifies the type of size. When it is 0/1, `size` is a little-endian 1/4-byte
    ///   unsigned integer.
    /// - b3b2/b1b0 specifies the integer type of id and offset. When the 2 bits are 0/1/2, the
    ///   list contains 1/2/3-byte little-endian unsigned integers.
    static constexpr int32_t kObject = 2;
    /// Array value. The content contains a size, a list of field offsets, and the actual element
    /// data. It is similar to an object without the id list. The type info is 000_b2_b1b0:
    /// - b2 specifies the type of size.
    /// - b1b0 specifies the integer type of offset.
    static constexpr int32_t kArray = 3;

    /// JSON null value. Empty content.
    static constexpr int32_t kNull = 0;
    /// True value. Empty content.
    static constexpr int32_t kTrue = 1;
    /// False value. Empty content.
    static constexpr int32_t kFalse = 2;
    /// 1-byte little-endian signed integer.
    static constexpr int32_t kInt1 = 3;
    /// 2-byte little-endian signed integer.
    static constexpr int32_t kInt2 = 4;
    /// 4-byte little-endian signed integer.
    static constexpr int32_t kInt4 = 5;
    /// 8-byte little-endian signed integer.
    static constexpr int32_t kInt8 = 6;
    /// 8-byte IEEE double.
    static constexpr int32_t kDouble = 7;
    /// 4-byte decimal. Content is 1-byte scale + 4-byte little-endian signed integer.
    static constexpr int32_t kDecimal4 = 8;
    /// 8-byte decimal. Content is 1-byte scale + 8-byte little-endian signed integer.
    static constexpr int32_t kDecimal8 = 9;
    /// 16-byte decimal. Content is 1-byte scale + 16-byte little-endian signed integer.
    static constexpr int32_t kDecimal16 = 10;
    /// Date value. Content is 4-byte little-endian signed integer that represents the number of
    /// days from the Unix epoch.
    static constexpr int32_t kDate = 11;
    /// Timestamp value. Content is 8-byte little-endian signed integer that represents the number
    /// of microseconds elapsed since the Unix epoch, 1970-01-01 00:00:00 UTC. It is displayed to
    /// users in their local time zones and may be displayed differently depending on the
    /// execution environment.
    static constexpr int32_t kTimestamp = 12;
    /// Timestamp_ntz value. It has the same content as `kTimestamp` but should always be
    /// interpreted as if the local time zone is UTC.
    static constexpr int32_t kTimestampNtz = 13;
    /// 4-byte IEEE float.
    static constexpr int32_t kFloat = 14;
    /// Binary value. The content is (4-byte little-endian unsigned integer representing the
    /// binary size) + (size bytes of binary content).
    static constexpr int32_t kBinary = 15;
    /// Long string value. The content is (4-byte little-endian unsigned integer representing the
    /// string size) + (size bytes of string content).
    static constexpr int32_t kLongStr = 16;
    /// UUID, 16-byte big-endian.
    static constexpr int32_t kUuid = 20;

    /// The only acceptable variant version. It is stored in the lower 4 bits of the first
    /// metadata byte.
    static constexpr uint8_t kVersion = 1;
    static constexpr uint8_t kVersionMask = 0x0F;

    static constexpr int32_t kU8Max = 0xFF;
    static constexpr int32_t kU16Max = 0xFFFF;
    static constexpr int32_t kU24Max = 0xFFFFFF;
    static constexpr int32_t kU24Size = 3;
    static constexpr int32_t kU32Size = 4;

    /// Both variant value and variant metadata need to be no longer than 128MiB.
    static constexpr int32_t kSizeLimit = 128 * 1024 * 1024;

    static constexpr int32_t kMaxDecimal4Precision = 9;
    static constexpr int32_t kMaxDecimal8Precision = 18;
    static constexpr int32_t kMaxDecimal16Precision = 38;

    /// Object field lookup switches from linear search to binary search when the object size
    /// reaches this threshold.
    static constexpr int32_t kBinarySearchThreshold = 32;
};

}  // namespace paimon
