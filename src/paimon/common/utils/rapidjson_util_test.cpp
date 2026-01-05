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

#include "paimon/common/utils/rapidjson_util.h"

#include <algorithm>
#include <map>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon::test {

TEST(RapidJsonUtilTest, TestSerializeAndDeserialize) {
    // serialize
    rapidjson::Document doc;
    doc.SetObject();
    rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();
    // string
    std::string str_value = "John";
    doc.AddMember("name", RapidJsonUtil::SerializeValue(str_value, &allocator).Move(), allocator);
    // int
    int32_t int_value = 30;
    doc.AddMember("age", RapidJsonUtil::SerializeValue(int_value, &allocator).Move(), allocator);
    // vector
    std::vector<int> vector_value = {7, 12};
    doc.AddMember("vector_value", RapidJsonUtil::SerializeValue(vector_value, &allocator).Move(),
                  allocator);
    // map
    std::map<std::string, double> map_value = {{"a", 0.2}, {"b", 1.2}};
    doc.AddMember("map_value", RapidJsonUtil::SerializeValue(map_value, &allocator).Move(),
                  allocator);

    // vector of vector
    std::vector<std::vector<int>> vector_of_vector = {{7, 12}, {27, 45}};
    doc.AddMember("vector_of_vector",
                  RapidJsonUtil::SerializeValue(vector_of_vector, &allocator).Move(), allocator);

    // vector of map
    std::vector<std::map<std::string, double>> vector_of_map = {{{{"a", 0.2}, {"b", 1.2}}},
                                                                {{"c", 2.2}, {"d", 3.2}}};
    doc.AddMember("vector_of_map", RapidJsonUtil::SerializeValue(vector_of_map, &allocator).Move(),
                  allocator);

    // map of vector
    std::map<std::string, std::vector<int>> map_of_vector = {{"aa", {7, 12}}, {"bb", {27, 45}}};
    doc.AddMember("map_of_vector", RapidJsonUtil::SerializeValue(map_of_vector, &allocator).Move(),
                  allocator);

    std::optional<int64_t> null_value;
    doc.AddMember("null_value", RapidJsonUtil::SerializeValue(null_value, &allocator).Move(),
                  allocator);

    std::optional<std::string> optional_value("abcd");
    doc.AddMember("optional_value",
                  RapidJsonUtil::SerializeValue(optional_value, &allocator).Move(), allocator);

    // map with int key (not string key, will convert in util)
    std::map<int32_t, int64_t> map_with_int_key = {{100, 1000}, {200, 2000}};
    doc.AddMember("map_with_int_key",
                  RapidJsonUtil::SerializeValue(map_with_int_key, &allocator).Move(), allocator);

    std::string jsonStr;
    ASSERT_TRUE(RapidJsonUtil::ToJson(doc, &jsonStr));

    // deserialize
    rapidjson::Document doc2;
    ASSERT_TRUE(RapidJsonUtil::FromJson(jsonStr, &doc2));

    ASSERT_EQ(str_value, RapidJsonUtil::DeserializeKeyValue<std::string>(doc2, "name", ""));
    ASSERT_EQ(int_value, RapidJsonUtil::DeserializeKeyValue<int32_t>(doc2, "age", -1));
    ASSERT_EQ(vector_value,
              RapidJsonUtil::DeserializeKeyValue<std::vector<int>>(doc2, "vector_value", {}));

    auto de_map_value =
        RapidJsonUtil::DeserializeKeyValue<std::map<std::string, double>>(doc2, "map_value", {});
    ASSERT_EQ(map_value, de_map_value);

    auto de_vector_of_vector = RapidJsonUtil::DeserializeKeyValue<std::vector<std::vector<int>>>(
        doc2, "vector_of_vector", {});
    ASSERT_EQ(vector_of_vector, de_vector_of_vector);

    auto de_vector_of_map =
        RapidJsonUtil::DeserializeKeyValue<std::vector<std::map<std::string, double>>>(
            doc2, "vector_of_map", {});
    ASSERT_EQ(vector_of_map, de_vector_of_map);

    auto de_map_of_vector =
        RapidJsonUtil::DeserializeKeyValue<std::map<std::string, std::vector<int>>>(
            doc2, "map_of_vector", {});
    ASSERT_EQ(map_of_vector, de_map_of_vector);

    auto de_null_value =
        RapidJsonUtil::DeserializeKeyValue<std::optional<int64_t>>(doc2, "null_value");
    ASSERT_EQ(null_value, de_null_value);

    auto de_null_value_with_default = RapidJsonUtil::DeserializeKeyValue<std::optional<int64_t>>(
        doc2, "null_value", /*default_value=*/std::optional<int64_t>(2333));
    ASSERT_EQ(2333, de_null_value_with_default.value());

    auto de_optional_value =
        RapidJsonUtil::DeserializeKeyValue<std::optional<std::string>>(doc2, "optional_value");
    ASSERT_EQ(optional_value, de_optional_value);

    auto de_map_with_int_key = RapidJsonUtil::DeserializeKeyValue<std::map<int32_t, int64_t>>(
        doc2, "map_with_int_key", {});
    ASSERT_EQ(map_with_int_key, de_map_with_int_key);

    // test non exist key, will use default value
    double non_exist_value = 0.0;
    non_exist_value = RapidJsonUtil::DeserializeKeyValue<double>(doc2, "non_exist_key", 2.333);
    ASSERT_EQ(2.333, non_exist_value);
}

TEST(RapidJsonUtilTest, TestMapJsonString) {
    std::map<std::string, std::string> m1 = {{"key1", "value1"}, {"key2", "value2"}};
    std::string result;
    ASSERT_OK(RapidJsonUtil::ToJsonString(m1, &result));
    ASSERT_EQ(result, "{\"key1\":\"value1\",\"key2\":\"value2\"}");

    std::map<std::string, std::string> m2;
    ASSERT_OK(RapidJsonUtil::FromJsonString(result, &m2));
    ASSERT_EQ(m1, m2);
}

}  // namespace paimon::test
