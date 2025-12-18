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

#include "xxhash.h"  // NOLINT(build/include_subdir)

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(XXHashTest, TestCompatibleWithJava) {
    auto file_system = std::make_unique<LocalFileSystem>();
    auto file_name = paimon::test::GetDataDir() + "/xxhash.data";
    std::string bytes;
    ASSERT_OK(file_system->ReadFile(file_name, &bytes));
    auto lines = StringUtils::Split(bytes, "\n");
    // 1000 random str and empty str
    ASSERT_EQ(1001, lines.size());
    for (const auto& line : lines) {
        auto data_and_hash = StringUtils::Split(line, ",", /*ignore_empty=*/false);
        ASSERT_EQ(2, data_and_hash.size());
        const auto& str = data_and_hash[0];
        int64_t expected_hash = std::stoull(data_and_hash[1], nullptr, 16);
        ASSERT_EQ(expected_hash, XXH64(str.data(), str.size(), /*seed=*/0));
    }
}

}  // namespace paimon::test
