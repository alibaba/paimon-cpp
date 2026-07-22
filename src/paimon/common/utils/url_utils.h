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

#include <string>
#include <string_view>

#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {

/// URL encoding and decoding shared by the REST catalog and the object store file
/// systems.
class PAIMON_EXPORT UrlUtils {
 public:
    UrlUtils() = delete;
    ~UrlUtils() = delete;

    /// URL-encode a string in the `application/x-www-form-urlencoded` flavor (UTF-8):
    /// alphanumeric characters and ".", "-", "*", "_" are kept, a space becomes "+", all
    /// other bytes become percent-encoded "%XX". This is what the REST catalog server
    /// expects; unlike RFC 3986 it also escapes "~".
    static std::string EncodeString(const std::string& input);

    /// Decodes `EncodeString` output ('+' back to space, "%XX" to the byte); malformed
    /// escape sequences are kept as-is instead of failing.
    static std::string DecodeString(const std::string& input);

    /// Percent-encode a URL component with RFC 3986 rules: alphanumeric characters and
    /// "-", ".", "_", "~" are kept, every other byte (including a space) becomes "%XX".
    /// With `preserve_slash`, '/' is kept as-is so an object key keeps its path shape.
    static std::string PercentEncode(std::string_view value, bool preserve_slash = false);

    /// Strict inverse of `PercentEncode`: "%XX" becomes the byte, '+' is kept as-is, and
    /// a '%' not followed by two hex digits is an error.
    static Result<std::string> PercentDecode(std::string_view value);
};

}  // namespace paimon
