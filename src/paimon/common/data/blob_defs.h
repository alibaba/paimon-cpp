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

namespace paimon {

/// A Blob field uses the 'large_binary' type as its underlying physical storage in Apache Arrow
/// Schema, and is marked as the Paimon Blob extension type by attaching specific
/// **KeyValueMetadata**. Multiple blob fields in one paimon table are supported.
///
/// To create a Blob field:
/// @code
///   std::unordered_map<std::string, std::string> blob_metadata_map = {
///       {Blob::EXTENSION_TYPE_KEY, Blob::EXTENSION_TYPE_VALUE}
///   };
///   auto field = arrow::field("my_blob_field", arrow::large_binary(), false,
///       std::make_shared<arrow::KeyValueMetadata>(blob_metadata_map));
/// @endcode
constexpr char BLOB_EXTENSION_TYPE_KEY[] = "paimon.extension.type";
constexpr char BLOB_EXTENSION_TYPE_VALUE[] = "paimon.type.blob";

}  // namespace paimon
