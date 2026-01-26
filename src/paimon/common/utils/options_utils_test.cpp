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

#include "paimon/common/utils/options_utils.h"

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(OptionsUtilsTest, TestGetValueFromMap) {
    std::map<std::string, std::string> key_value_map;
    key_value_map["key_int"] = "10";
    key_value_map["key_bool"] = "true";
    key_value_map["key_int16"] = "100";
    key_value_map["key_double"] = "4.5E10";
    // invalid
    key_value_map["key_bool2"] = "true1";
    key_value_map["key_int8"] = "500";
    key_value_map["key_int64"] = "ab";

    ASSERT_OK_AND_ASSIGN(auto int32_value,
                         OptionsUtils::GetValueFromMap<int32_t>(key_value_map, "key_int"));
    ASSERT_EQ(10, int32_value);
    ASSERT_OK_AND_ASSIGN(auto bool_value,
                         OptionsUtils::GetValueFromMap<bool>(key_value_map, "key_bool"));
    ASSERT_TRUE(bool_value);
    ASSERT_OK_AND_ASSIGN(auto int16_value,
                         OptionsUtils::GetValueFromMap<int16_t>(key_value_map, "key_int16"));
    ASSERT_EQ(100, int16_value);
    ASSERT_OK_AND_ASSIGN(auto double_value,
                         OptionsUtils::GetValueFromMap<double>(key_value_map, "key_double"));
    ASSERT_NEAR(4.5E10, double_value, 0.00001);
    ASSERT_NOK_WITH_MSG(OptionsUtils::GetValueFromMap<bool>(key_value_map, "key_bool2"),
                        "convert key key_bool2, value true1 to bool failed");
    ASSERT_NOK_WITH_MSG(OptionsUtils::GetValueFromMap<int8_t>(key_value_map, "key_int8"),
                        "convert key key_int8, value 500 to signed char failed");
    ASSERT_NOK_WITH_MSG(OptionsUtils::GetValueFromMap<int64_t>(key_value_map, "key_int64"),
                        "convert key key_int64, value ab to long failed");
    ASSERT_NOK_WITH_MSG(OptionsUtils::GetValueFromMap<int64_t>(key_value_map, "key_int64", 10),
                        "convert key key_int64, value ab to long failed");

    ASSERT_OK_AND_ASSIGN(
        auto nonexist, OptionsUtils::GetValueFromMap<int32_t>(key_value_map, "key_nonexist", 233));
    ASSERT_EQ(233, nonexist);
    ASSERT_OK_AND_ASSIGN(auto empty,
                         OptionsUtils::GetValueFromMap<int32_t>(key_value_map, "", 999));
    ASSERT_EQ(999, empty);
}

TEST(OptionsUtilsTest, TestFetchOptionsWithPrefix) {
    std::map<std::string, std::string> options = {{"key1", "value1"}, {"test.key2", "value2"}};
    auto new_options = OptionsUtils::FetchOptionsWithPrefix("test.", options);
    std::map<std::string, std::string> expected = {{"key2", "value2"}};
    ASSERT_EQ(expected, new_options);
}
}  // namespace paimon::test
