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

#include <map>
#include <string>

#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon {

/// Utilities for the REST catalog; URL encoding and decoding live in `UrlUtils`,
/// credential redaction in `SensitiveConfigUtils`.
class RestUtil {
 public:
    RestUtil() = delete;
    ~RestUtil() = delete;

    /// Header under which the rest server reports the id of a request.
    static constexpr const char* kRequestIdHeader = "x-request-id";
    /// Placeholder `ExtractRequestId` returns when no header carries a request id.
    static constexpr const char* kUnknownRequestId = "unknown";

    /// Extract all options whose key starts with `prefix`, with the prefix stripped from
    /// the resulting keys. A key exactly equal to the prefix is dropped instead of
    /// yielding an empty key.
    static std::map<std::string, std::string> ExtractPrefixMap(
        const std::map<std::string, std::string>& options, const std::string& prefix);

    /// Returns the request id tying a log line or an error to the server side trace: the
    /// `kRequestIdHeader` value, falling back to any other header whose name contains
    /// "request-id" (a gateway may report it under its own name, e.g. "x-amz-request-id"),
    /// then to `kUnknownRequestId`. `headers` must have lower-cased names.
    static std::string ExtractRequestId(const std::map<std::string, std::string>& headers);

    /// Serialize a rapidjson value to a compact JSON string.
    static std::string JsonToString(const rapidjson::Value& value);

    /// Parse a JSON string into a rapidjson value owned by `allocator`.
    /// Throws `std::invalid_argument` on parse error, consistent with `Jsonizable`.
    static rapidjson::Value ParseToValue(const std::string& json,
                                         rapidjson::Document::AllocatorType* allocator);
};

}  // namespace paimon
