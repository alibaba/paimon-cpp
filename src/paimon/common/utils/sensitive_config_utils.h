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

#include "paimon/visibility.h"

namespace paimon {

/// Redacts credentials (passwords, secrets, tokens, access keys) before they reach a log
/// line, an error message or a user visible table.
class PAIMON_EXPORT SensitiveConfigUtils {
 public:
    SensitiveConfigUtils() = delete;
    ~SensitiveConfigUtils() = delete;

    /// Replaces a redacted option value, and the whole text of a redacted message.
    static constexpr const char* kRedacted = "******";

    /// Returns whether `key` names a credential. The key is matched lower-cased and
    /// stripped of separators, so "dlf.access-key-secret", "fs.s3a.access.key",
    /// "fs.azure.account-key.store" and "accessKeySecret" all hit a marker.
    static bool IsSensitiveKey(const std::string& key);

    /// Returns `value` masked when `key` names a credential, and `value` unchanged
    /// otherwise. A key naming a true secret (a token, a password, an account key, ...)
    /// is masked as a whole, while an identifier-like key ("dlf.access-key-id" hits only
    /// the "accesskey" marker) keeps the last four characters of a long enough value:
    /// enough to tell two credentials apart in a support case, not enough to use either.
    static std::string RedactValue(const std::string& key, const std::string& value);

    /// Returns a copy of `options` with the value of every sensitive key redacted by
    /// `RedactValue`. Every surface exposing catalog or table options to users (e.g. the
    /// `sys.catalog_options` table) must pass them through this filter.
    static std::map<std::string, std::string> RedactMap(
        const std::map<std::string, std::string>& options);

    /// Redacts free-form text such as a server error message: arbitrary text cannot be
    /// masked per-secret reliably, so the whole text becomes `kRedacted` as soon as any
    /// marker of a sensitive value ("password", "token", "sig=", ...) appears.
    static std::string RedactText(const std::string& text);
};

}  // namespace paimon
