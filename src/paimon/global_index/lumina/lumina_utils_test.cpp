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
#include "paimon/global_index/lumina/lumina_utils.h"

#include <future>

#include "paimon/testing/utils/testharness.h"
namespace paimon::lumina::test {
TEST(LuminaUtilsTest, TestSimple) {
    std::map<std::string, std::string> options = {{"key1", "value1"}, {"lumina.key2", "value2"}};
    auto lumina_options = LuminaUtils::FetchLuminaOptions(options);
    std::map<std::string, std::string> expected = {{"key2", "value2"}};
    ASSERT_EQ(expected, lumina_options);
}
}  // namespace paimon::lumina::test
