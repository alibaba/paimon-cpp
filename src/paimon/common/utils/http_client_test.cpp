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

#include "paimon/common/utils/http_client.h"

#include <string>

#include "gtest/gtest.h"

namespace paimon::test {

TEST(HttpClientUtilTest, ParseHttpHeaderLine) {
    HttpHeaders headers;
    const std::string line = "Content-Type: application/json\r\n";
    ParseHttpHeaderLine(line.data(), line.size(), &headers);
    ASSERT_EQ((HttpHeaders{{"content-type", "application/json"}}), headers);

    const std::string overwrite = "content-type:  text/PLAIN \r\n";
    ParseHttpHeaderLine(overwrite.data(), overwrite.size(), &headers);
    ASSERT_EQ((HttpHeaders{{"content-type", "text/PLAIN"}}), headers);

    // Lines without a colon (like the status line) and empty names are ignored.
    const std::string status_line = "HTTP/1.1 200 OK\r\n";
    ParseHttpHeaderLine(status_line.data(), status_line.size(), &headers);
    const std::string empty_name = ": value\r\n";
    ParseHttpHeaderLine(empty_name.data(), empty_name.size(), &headers);
    ASSERT_EQ(1, headers.size());

    const std::string empty_value = "x-empty:\r\n";
    ParseHttpHeaderLine(empty_value.data(), empty_value.size(), &headers);
    ASSERT_EQ("", headers.at("x-empty"));
}

}  // namespace paimon::test
