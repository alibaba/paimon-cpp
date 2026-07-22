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

#include <map>
#include <string>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(SensitiveConfigUtilsTest, IsSensitiveKey) {
    // the key is matched lower-cased and stripped of separators, so the same marker hits
    // whichever separator style a key uses
    ASSERT_TRUE(SensitiveConfigUtils::IsSensitiveKey("token"));
    ASSERT_TRUE(SensitiveConfigUtils::IsSensitiveKey("dlf.access-key-secret"));
    ASSERT_TRUE(SensitiveConfigUtils::IsSensitiveKey("fs.s3a.access.key"));
    ASSERT_TRUE(SensitiveConfigUtils::IsSensitiveKey("fs.azure.account-key.store1"));
    ASSERT_TRUE(SensitiveConfigUtils::IsSensitiveKey("accessKeySecret"));
    ASSERT_TRUE(SensitiveConfigUtils::IsSensitiveKey("client.credential"));
    ASSERT_TRUE(SensitiveConfigUtils::IsSensitiveKey("fs.azure.sas.container"));
    ASSERT_TRUE(SensitiveConfigUtils::IsSensitiveKey("HTTP.Authorization"));

    // the match is a substring one, so a key merely naming a credential is masked too
    ASSERT_TRUE(SensitiveConfigUtils::IsSensitiveKey("token.provider"));

    ASSERT_FALSE(SensitiveConfigUtils::IsSensitiveKey(""));
    ASSERT_FALSE(SensitiveConfigUtils::IsSensitiveKey("uri"));
    ASSERT_FALSE(SensitiveConfigUtils::IsSensitiveKey("file.format"));
    // "signature" marks free-form text, not an option key: in a key it names the signing
    // algorithm rather than a stored credential
    ASSERT_FALSE(SensitiveConfigUtils::IsSensitiveKey("dlf.signing-algorithm"));
}

TEST(SensitiveConfigUtilsTest, RedactValue) {
    // a key naming no credential keeps its value
    ASSERT_EQ("orc", SensitiveConfigUtils::RedactValue("file.format", "orc"));
    ASSERT_EQ("v", SensitiveConfigUtils::RedactValue("", "v"));

    // a key naming a true secret is masked as a whole, however long the value is
    ASSERT_EQ(SensitiveConfigUtils::kRedacted,
              SensitiveConfigUtils::RedactValue("token", "bearer-credential-1"));
    ASSERT_EQ(SensitiveConfigUtils::kRedacted,
              SensitiveConfigUtils::RedactValue("dlf.access-key-secret", "secret-value-1"));

    // an identifier-like key keeps the last four characters of a long enough value, so
    // two credentials can be told apart without disclosing either
    ASSERT_EQ("****k-id", SensitiveConfigUtils::RedactValue("dlf.access-key-id", "an-access-k-id"));
    // a short value would reveal too much of itself through the tail
    ASSERT_EQ(SensitiveConfigUtils::kRedacted,
              SensitiveConfigUtils::RedactValue("dlf.access-key-id", "short-id"));
}

TEST(SensitiveConfigUtilsTest, RedactMap) {
    const std::map<std::string, std::string> options = {
        {"uri", "http://127.0.0.1:8080"},
        {"token", "bearer-credential"},
        {"dlf.access-key-secret", "ak-secret"},
        {"file.format", "orc"},
    };
    std::map<std::string, std::string> redacted = SensitiveConfigUtils::RedactMap(options);
    // every key stays listed, only the credential-carrying values are replaced
    ASSERT_EQ(options.size(), redacted.size());
    ASSERT_EQ("http://127.0.0.1:8080", redacted.at("uri"));
    ASSERT_EQ("orc", redacted.at("file.format"));
    ASSERT_EQ(SensitiveConfigUtils::kRedacted, redacted.at("token"));
    ASSERT_EQ(SensitiveConfigUtils::kRedacted, redacted.at("dlf.access-key-secret"));

    ASSERT_TRUE(SensitiveConfigUtils::RedactMap({}).empty());
}

TEST(SensitiveConfigUtilsTest, RedactText) {
    // a marker anywhere in the text redacts all of it: arbitrary text cannot be masked
    // per-secret reliably
    ASSERT_EQ(SensitiveConfigUtils::kRedacted,
              SensitiveConfigUtils::RedactText("invalid password=abc123"));
    ASSERT_EQ(SensitiveConfigUtils::kRedacted,
              SensitiveConfigUtils::RedactText("bad ACCESS-KEY provided"));
    ASSERT_EQ(SensitiveConfigUtils::kRedacted,
              SensitiveConfigUtils::RedactText("url?X-Amz-Signature=deadbeef"));
    // the Azure SAS "sig" is matched literally, since it is ambiguous once the separators
    // are stripped
    ASSERT_EQ(SensitiveConfigUtils::kRedacted,
              SensitiveConfigUtils::RedactText("url?sig=deadbeef"));

    ASSERT_EQ("table t1 not found", SensitiveConfigUtils::RedactText("table t1 not found"));
    // a word merely containing "sig" is not a marker
    ASSERT_EQ("design is invalid", SensitiveConfigUtils::RedactText("design is invalid"));
    ASSERT_EQ("", SensitiveConfigUtils::RedactText(""));
}

}  // namespace paimon::test
