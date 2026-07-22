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

#include "gtest/gtest.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(UrlUtilsTest, EncodeString) {
    ASSERT_EQ("abcDEF012.-*_", UrlUtils::EncodeString("abcDEF012.-*_"));
    ASSERT_EQ("a+b", UrlUtils::EncodeString("a b"));
    ASSERT_EQ("a%2Fb", UrlUtils::EncodeString("a/b"));
    ASSERT_EQ("a%3Db%26c", UrlUtils::EncodeString("a=b&c"));
    // '~' is the character distinguishing the two encoding flavors: EncodeString
    // (form-urlencoded) escapes it while PercentEncode (RFC 3986) keeps it.
    ASSERT_EQ("%7E", UrlUtils::EncodeString("~"));
    ASSERT_EQ("a%2Bb", UrlUtils::EncodeString("a+b"));
    ASSERT_EQ("%E4%B8%AD", UrlUtils::EncodeString("中"));
}

TEST(UrlUtilsTest, DecodeString) {
    ASSERT_EQ("a/b", UrlUtils::DecodeString("a%2Fb"));
    ASSERT_EQ("a b", UrlUtils::DecodeString("a+b"));
    ASSERT_EQ("中", UrlUtils::DecodeString("%E4%B8%AD"));
    ASSERT_EQ("a/", UrlUtils::DecodeString("a%2F"));
    // Malformed escapes are kept as-is.
    ASSERT_EQ("a%", UrlUtils::DecodeString("a%"));
    ASSERT_EQ("%2", UrlUtils::DecodeString("%2"));
    ASSERT_EQ("%ZZ", UrlUtils::DecodeString("%ZZ"));
    ASSERT_EQ("100%", UrlUtils::DecodeString("100%"));
}

TEST(UrlUtilsTest, PercentEncode) {
    ASSERT_EQ("abcDEF012-._~", UrlUtils::PercentEncode("abcDEF012-._~"));
    ASSERT_EQ("a%20b", UrlUtils::PercentEncode("a b"));
    ASSERT_EQ("a%2Ab", UrlUtils::PercentEncode("a*b"));
    ASSERT_EQ("a%2Fb", UrlUtils::PercentEncode("a/b"));
    ASSERT_EQ("a/b%3Dc", UrlUtils::PercentEncode("a/b=c", /*preserve_slash=*/true));
    // a literal '%' is encoded whether or not '/' is kept, so an input already reading
    // "%2F" cannot come out as a slash
    ASSERT_EQ("a%252Fb", UrlUtils::PercentEncode("a%2Fb", /*preserve_slash=*/true));
    ASSERT_EQ("%E4%B8%AD", UrlUtils::PercentEncode("中"));
}

TEST(UrlUtilsTest, PercentDecode) {
    ASSERT_OK_AND_ASSIGN(std::string decoded, UrlUtils::PercentDecode("a%2Fb%20c"));
    ASSERT_EQ("a/b c", decoded);
    // '+' is form encoding only; percent decoding keeps it.
    ASSERT_OK_AND_ASSIGN(decoded, UrlUtils::PercentDecode("a+b"));
    ASSERT_EQ("a+b", decoded);
    ASSERT_OK_AND_ASSIGN(decoded, UrlUtils::PercentDecode("%e4%b8%ad"));
    ASSERT_EQ("中", decoded);
    // Malformed escapes are an error.
    ASSERT_NOK(UrlUtils::PercentDecode("a%").status());
    ASSERT_NOK(UrlUtils::PercentDecode("%2").status());
    ASSERT_NOK(UrlUtils::PercentDecode("%ZZ").status());
    ASSERT_NOK(UrlUtils::PercentDecode("100%").status());
}

TEST(UrlUtilsTest, RoundTrip) {
    const std::string input = "db 1/table$branch_b1=中%";
    ASSERT_EQ(input, UrlUtils::DecodeString(UrlUtils::EncodeString(input)));
    ASSERT_OK_AND_ASSIGN(std::string decoded,
                         UrlUtils::PercentDecode(UrlUtils::PercentEncode(input)));
    ASSERT_EQ(input, decoded);
}

}  // namespace paimon::test
