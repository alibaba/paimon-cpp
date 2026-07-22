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

#include "paimon/common/utils/sensitive_config_utils.h"

#include "paimon/common/utils/string_utils.h"

namespace paimon {

namespace {

// Substring markers of a credential, matched against the normalized form so that the
// separators of a key or a message do not matter.
constexpr const char* kSensitiveMarkers[] = {
    "password",      "secret",        "token",      "credential", "accesskey", "accountkey",
    "encryptionkey", "authorization", "privatekey", "apikey",     "sas"};

// Keys whose value is masked as a whole rather than keeping a trailing hint. Every true
// secret is here; only an identifier-like key keeps a tail. "accessKeySecret" normalizes
// to contain "secret" and is masked as a whole, while "accessKeyId" hits only the
// "accesskey" marker of `kSensitiveMarkers` and keeps its tail. This mirrors AWS/Azure,
// where the access key id is loggable but the secret, the token and the SAS are not.
constexpr const char* kFullMaskMarkers[] = {"password",   "token",      "authorization", "secret",
                                            "credential", "privatekey", "encryptionkey", "apikey",
                                            "accountkey", "sas"};

// A value shorter than this reveals too much of itself through a four character tail, so
// it is masked as a whole.
constexpr size_t kMinLengthForTail = 12;
constexpr size_t kTailLength = 4;

// Free-form text is scanned for one more marker: in a message "signature" names a
// credential, while in an option key it names a request signature algorithm.
constexpr const char kTextOnlyMarker[] = "signature";

// The Azure SAS "sig" would be ambiguous once separators are removed ("sig" is a
// substring of many words), so it is matched literally instead.
constexpr const char* kLiteralTextMarkers[] = {"sig=", "\"sig\"", "'sig'"};

// Drops every character of an already lower-cased string that is not a letter or a
// digit, so "access-key", "access.key" and "accessKey" all normalize to "accesskey".
std::string StripSeparators(const std::string& lowered) {
    std::string normalized;
    normalized.reserve(lowered.size());
    for (char c : lowered) {
        if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
            normalized.push_back(c);
        }
    }
    return normalized;
}

template <size_t N>
bool ContainsMarker(const std::string& normalized, const char* const (&markers)[N]) {
    for (const char* marker : markers) {
        if (normalized.find(marker) != std::string::npos) {
            return true;
        }
    }
    return false;
}

std::string NormalizeKey(const std::string& key) {
    return StripSeparators(StringUtils::ToLowerCase(key));
}

}  // namespace

bool SensitiveConfigUtils::IsSensitiveKey(const std::string& key) {
    if (key.empty()) {
        return false;
    }
    return ContainsMarker(NormalizeKey(key), kSensitiveMarkers);
}

std::string SensitiveConfigUtils::RedactValue(const std::string& key, const std::string& value) {
    if (key.empty()) {
        return value;
    }
    std::string normalized = NormalizeKey(key);
    if (!ContainsMarker(normalized, kSensitiveMarkers)) {
        return value;
    }
    if (!ContainsMarker(normalized, kFullMaskMarkers) && value.size() >= kMinLengthForTail) {
        return "****" + value.substr(value.size() - kTailLength);
    }
    return kRedacted;
}

std::map<std::string, std::string> SensitiveConfigUtils::RedactMap(
    const std::map<std::string, std::string>& options) {
    std::map<std::string, std::string> redacted;
    for (const auto& [key, value] : options) {
        redacted.emplace(key, RedactValue(key, value));
    }
    return redacted;
}

std::string SensitiveConfigUtils::RedactText(const std::string& text) {
    if (text.empty()) {
        return text;
    }
    std::string lowered = StringUtils::ToLowerCase(text);
    for (const char* marker : kLiteralTextMarkers) {
        if (lowered.find(marker) != std::string::npos) {
            return kRedacted;
        }
    }
    std::string normalized = StripSeparators(lowered);
    if (ContainsMarker(normalized, kSensitiveMarkers) ||
        normalized.find(kTextOnlyMarker) != std::string::npos) {
        return kRedacted;
    }
    return text;
}

}  // namespace paimon
