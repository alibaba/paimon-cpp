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

#include "paimon/common/utils/url_utils.h"

#include <cstdio>

namespace paimon {

namespace {

bool IsAlphaNumeric(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
}

bool IsFormUnreservedChar(char c) {
    return IsAlphaNumeric(c) || c == '.' || c == '-' || c == '*' || c == '_';
}

bool IsRfc3986UnreservedChar(char c) {
    return IsAlphaNumeric(c) || c == '-' || c == '.' || c == '_' || c == '~';
}

void AppendPercentEncoded(char c, std::string* out) {
    char buf[4];
    std::snprintf(buf, sizeof(buf), "%%%02X", static_cast<unsigned char>(c));
    out->append(buf);
}

int32_t HexValue(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

}  // namespace

std::string UrlUtils::EncodeString(const std::string& input) {
    std::string encoded;
    encoded.reserve(input.size());
    for (char c : input) {
        if (IsFormUnreservedChar(c)) {
            encoded.push_back(c);
        } else if (c == ' ') {
            encoded.push_back('+');
        } else {
            AppendPercentEncoded(c, &encoded);
        }
    }
    return encoded;
}

std::string UrlUtils::DecodeString(const std::string& input) {
    std::string decoded;
    decoded.reserve(input.size());
    for (size_t i = 0; i < input.size(); i++) {
        char c = input[i];
        if (c == '+') {
            decoded.push_back(' ');
        } else if (c == '%' && i + 2 < input.size()) {
            int32_t high = HexValue(input[i + 1]);
            int32_t low = HexValue(input[i + 2]);
            if (high >= 0 && low >= 0) {
                decoded.push_back(static_cast<char>((high << 4) | low));
                i += 2;
            } else {
                decoded.push_back(c);
            }
        } else {
            decoded.push_back(c);
        }
    }
    return decoded;
}

std::string UrlUtils::PercentEncode(std::string_view value, bool preserve_slash) {
    std::string encoded;
    encoded.reserve(value.size());
    for (char c : value) {
        if (IsRfc3986UnreservedChar(c) || (preserve_slash && c == '/')) {
            encoded.push_back(c);
        } else {
            AppendPercentEncoded(c, &encoded);
        }
    }
    return encoded;
}

Result<std::string> UrlUtils::PercentDecode(std::string_view value) {
    std::string decoded;
    decoded.reserve(value.size());
    for (size_t i = 0; i < value.size(); i++) {
        char c = value[i];
        if (c == '%') {
            if (i + 2 >= value.size()) {
                return Status::IOError("invalid percent encoding in URL component");
            }
            int32_t high = HexValue(value[i + 1]);
            int32_t low = HexValue(value[i + 2]);
            if (high < 0 || low < 0) {
                return Status::IOError("invalid percent encoding in URL component");
            }
            decoded.push_back(static_cast<char>((high << 4) | low));
            i += 2;
        } else {
            decoded.push_back(c);
        }
    }
    return decoded;
}

}  // namespace paimon
