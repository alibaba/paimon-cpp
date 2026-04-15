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

#include "paimon/common/utils/object_utils.h"

#include <string>

#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"

namespace paimon::test {

TEST(ObjectUtilsTest, TestContainsAll) {
    std::vector<int32_t> all = {0, 1, 2, 3};
    std::vector<int32_t> contains1 = {1, 2};
    std::vector<int32_t> contains2 = {1, 10};
    ASSERT_TRUE(ObjectUtils::ContainsAll(all, contains1));
    ASSERT_FALSE(ObjectUtils::ContainsAll(all, contains2));
}

TEST(ObjectUtilsTest, TestContains) {
    std::vector<int32_t> all = {0, 2, 4, 6};
    ASSERT_TRUE(ObjectUtils::Contains(all, 4));
    ASSERT_FALSE(ObjectUtils::Contains(all, 8));
}

TEST(ObjectUtilsTest, TestDuplicateItems) {
    std::vector<std::string> entries = {"apple",  "banana", "cherry", "apple",
                                        "banana", "date",   "fig",    "cherry"};
    std::unordered_set<std::string> expected = {"banana", "apple", "cherry"};
    ASSERT_EQ(ObjectUtils::DuplicateItems(entries), expected);
}

TEST(ObjectUtilsTest, TestCreateIdentifierToIndexMap) {
    std::vector<DataField> fields = {
        DataField(0, arrow::field("f11", arrow::boolean())),
        DataField(1, arrow::field("f10", arrow::int8())),
        DataField(2, arrow::field("f9", arrow::int16())),
        DataField(3, arrow::field("f8", arrow::int32())),
        DataField(4, arrow::field("f7", arrow::int64())),
        DataField(5, arrow::field("f6", arrow::float32())),
        DataField(6, arrow::field("f5", arrow::float64())),
        DataField(7, arrow::field("f4", arrow::utf8())),
        DataField(8, arrow::field("f3", arrow::binary())),
        DataField(9, arrow::field("f2", arrow::timestamp(arrow::TimeUnit::NANO))),
        DataField(10, arrow::field("f1", arrow::date32())),
        DataField(11, arrow::field("f0", arrow::decimal128(2, 2))),
    };

    {
        auto result_map = ObjectUtils::CreateIdentifierToIndexMap(
            fields, [](const DataField& field) { return field.Name(); });
        ASSERT_EQ(result_map.size(), 12);
        std::map<std::string, int32_t> expected_map = {
            {"f0", 11}, {"f1", 10}, {"f2", 9}, {"f3", 8}, {"f4", 7},  {"f5", 6},
            {"f6", 5},  {"f7", 4},  {"f8", 3}, {"f9", 2}, {"f10", 1}, {"f11", 0},
        };
        ASSERT_EQ(expected_map, result_map);
    }
    {
        auto result_map = ObjectUtils::CreateIdentifierToIndexMap(
            std::vector<DataField>({}), [](const DataField& field) { return field.Name(); });
        ASSERT_TRUE(result_map.empty());
    }
    {
        std::vector<std::string> vec = {"f0", "f1", "f2"};
        std::map<std::string, int32_t> result_map = ObjectUtils::CreateIdentifierToIndexMap(vec);
        std::map<std::string, int32_t> expected_map = {
            {"f0", 0},
            {"f1", 1},
            {"f2", 2},
        };
        ASSERT_EQ(expected_map, result_map);
    }
}
TEST(ObjectUtilsTest, TestMoveVector) {
    struct Base {
        virtual ~Base() = default;
        virtual int32_t Value() const = 0;
    };

    struct Derived : Base {
        explicit Derived(int32_t v) : val(v) {}
        int32_t Value() const override {
            return val;
        }
        int32_t val;
    };
    std::vector<std::unique_ptr<Derived>> derived_vec;
    derived_vec.push_back(std::make_unique<Derived>(10));
    derived_vec.push_back(std::make_unique<Derived>(20));
    derived_vec.push_back(std::make_unique<Derived>(30));

    auto base_vec = paimon::ObjectUtils::MoveVector<std::unique_ptr<Base>>(std::move(derived_vec));

    ASSERT_EQ(base_vec[0]->Value(), 10);
    ASSERT_EQ(base_vec[1]->Value(), 20);
    ASSERT_EQ(base_vec[2]->Value(), 30);
}
}  // namespace paimon::test
