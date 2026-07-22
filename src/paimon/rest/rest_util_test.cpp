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

#include "paimon/rest/rest_util.h"

#include <map>
#include <string>

#include "gtest/gtest.h"

namespace paimon::test {

TEST(RestUtilTest, ExtractPrefixMap) {
    std::map<std::string, std::string> options = {
        {"header.k1", "v1"}, {"header.k2", "v2"}, {"other", "v3"}, {"header.", "v4"}};
    std::map<std::string, std::string> expected = {{"k1", "v1"}, {"k2", "v2"}};
    ASSERT_EQ(expected, RestUtil::ExtractPrefixMap(options, "header."));
}

TEST(RestUtilTest, ExtractRequestId) {
    ASSERT_EQ("req-1", RestUtil::ExtractRequestId({{"x-request-id", "req-1"}}));
    // a gateway may report the request id under a name of its own
    ASSERT_EQ("amz-1", RestUtil::ExtractRequestId({{"x-amz-request-id", "amz-1"}}));
    // the dedicated header wins over a gateway one
    ASSERT_EQ("req-1", RestUtil::ExtractRequestId(
                           {{"x-request-id", "req-1"}, {"x-amz-request-id", "amz-1"}}));
    // an empty value carries no id and falls through like an absent header
    ASSERT_EQ("amz-1",
              RestUtil::ExtractRequestId({{"x-request-id", ""}, {"x-amz-request-id", "amz-1"}}));
    ASSERT_EQ(RestUtil::kUnknownRequestId, RestUtil::ExtractRequestId({}));
    ASSERT_EQ(RestUtil::kUnknownRequestId, RestUtil::ExtractRequestId({{"content-type", "json"}}));
}

}  // namespace paimon::test
