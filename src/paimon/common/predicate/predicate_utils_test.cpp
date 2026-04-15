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

#include "paimon/predicate/predicate_utils.h"

#include <utility>

#include "gtest/gtest.h"
#include "paimon/defs.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(PredicateUtilsTest, TestSplitAnd) {
    ASSERT_OK_AND_ASSIGN(
        auto child1,
        PredicateBuilder::Or(
            {PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT),
             PredicateBuilder::IsNull(/*field_index=*/1, /*field_name=*/"f1", FieldType::BIGINT),
             PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::BIGINT)}));

    auto sub_child1 =
        PredicateBuilder::IsNull(/*field_index=*/3, /*field_name=*/"f3", FieldType::BIGINT);
    auto sub_child2 =
        PredicateBuilder::IsNull(/*field_index=*/4, /*field_name=*/"f4", FieldType::BIGINT);
    auto sub_child3 =
        PredicateBuilder::IsNull(/*field_index=*/5, /*field_name=*/"f5", FieldType::BIGINT);
    ASSERT_OK_AND_ASSIGN(auto child2, PredicateBuilder::And({sub_child1, sub_child2, sub_child3}));

    auto child3 =
        PredicateBuilder::IsNull(/*field_index=*/6, /*field_name=*/"f6", FieldType::BIGINT);

    auto res = PredicateUtils::SplitAnd(PredicateBuilder::And({child1, child2, child3}).value());
    ASSERT_EQ(res.size(), 5u);
    ASSERT_EQ(*res[0], *child1);
    ASSERT_EQ(*res[1], *sub_child1);
    ASSERT_EQ(*res[2], *sub_child2);
    ASSERT_EQ(*res[3], *sub_child3);
    ASSERT_EQ(*res[4], *child3);
}

TEST(PredicateUtilsTest, TestContainAnyField) {
    ASSERT_OK_AND_ASSIGN(
        auto child1,
        PredicateBuilder::Or(
            {PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT),
             PredicateBuilder::IsNull(/*field_index=*/1, /*field_name=*/"f1", FieldType::BIGINT),
             PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::BIGINT)}));

    auto sub_child1 =
        PredicateBuilder::IsNull(/*field_index=*/3, /*field_name=*/"f3", FieldType::BIGINT);
    auto sub_child2 =
        PredicateBuilder::IsNull(/*field_index=*/4, /*field_name=*/"f4", FieldType::BIGINT);
    auto sub_child3 =
        PredicateBuilder::IsNull(/*field_index=*/5, /*field_name=*/"f5", FieldType::BIGINT);
    ASSERT_OK_AND_ASSIGN(auto child2, PredicateBuilder::And({sub_child1, sub_child2, sub_child3}));

    auto child3 =
        PredicateBuilder::IsNull(/*field_index=*/6, /*field_name=*/"f6", FieldType::BIGINT);
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({child1, child2, child3}));
    ASSERT_TRUE(PredicateUtils::ContainAnyField(predicate, {"f0"}).value());
    ASSERT_FALSE(PredicateUtils::ContainAnyField(predicate, {"non-exist"}).value());
}

TEST(PredicateUtilsTest, TestExcludePredicateWithFields) {
    ASSERT_OK_AND_ASSIGN(
        auto child1,
        PredicateBuilder::Or(
            {PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT),
             PredicateBuilder::IsNull(/*field_index=*/1, /*field_name=*/"f1", FieldType::BIGINT),
             PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::BIGINT)}));

    auto sub_child1 =
        PredicateBuilder::IsNull(/*field_index=*/3, /*field_name=*/"f3", FieldType::BIGINT);
    auto sub_child2 =
        PredicateBuilder::IsNull(/*field_index=*/4, /*field_name=*/"f4", FieldType::BIGINT);
    auto sub_child3 =
        PredicateBuilder::IsNull(/*field_index=*/5, /*field_name=*/"f5", FieldType::BIGINT);
    ASSERT_OK_AND_ASSIGN(auto child2, PredicateBuilder::And({sub_child1, sub_child2, sub_child3}));

    auto child3 =
        PredicateBuilder::IsNull(/*field_index=*/6, /*field_name=*/"f6", FieldType::BIGINT);

    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({child1, child2, child3}));
    std::vector<std::shared_ptr<Predicate>> predicates = {child1, child2, child3};
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             PredicateUtils::ExcludePredicateWithFields(predicates, {"non-exist"}));
        ASSERT_EQ(3, result.size());
        ASSERT_EQ(*child1, *result[0]);
        ASSERT_EQ(*child2, *result[1]);
        ASSERT_EQ(*child3, *result[2]);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             PredicateUtils::ExcludePredicateWithFields(predicate, {"non-exist"}));
        ASSERT_OK_AND_ASSIGN(auto expected, PredicateBuilder::And({child1, sub_child1, sub_child2,
                                                                   sub_child3, child3}));
        ASSERT_EQ(*expected, *result)
            << "result=" << result->ToString() << ", expected=" << predicate->ToString();
    }

    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             PredicateUtils::ExcludePredicateWithFields(predicates, {"f0", "f5"}));
        ASSERT_EQ(1, result.size());
        ASSERT_EQ(*child3, *result[0]);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             PredicateUtils::ExcludePredicateWithFields(predicate, {"f0", "f5"}));
        ASSERT_OK_AND_ASSIGN(auto expected,
                             PredicateBuilder::And({sub_child1, sub_child2, child3}));
        ASSERT_EQ(*expected, *result)
            << "result=" << result->ToString() << ", expected=" << predicate->ToString();
    }

    {
        ASSERT_OK_AND_ASSIGN(auto result, PredicateUtils::ExcludePredicateWithFields(
                                              predicates, {"f0", "f5", "f6"}));
        ASSERT_TRUE(result.empty());
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, PredicateUtils::ExcludePredicateWithFields(
                                              predicate, {"f0", "f3", "f4", "f5", "f6"}));
        ASSERT_FALSE(result);
    }
}

TEST(PredicateUtilsTest, TestCreatePickedFieldFilter) {
    std::map<std::string, int32_t> picked_field_name_to_idx = {
        {"f0", 10}, {"f1", 20}, {"f2", 30}, {"f3", 40}};
    auto equal = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::INT,
                                         Literal(10));
    auto not_equal = PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                FieldType::INT, Literal(20));
    ASSERT_OK_AND_ASSIGN(auto and_predicate, PredicateBuilder::And({equal, not_equal}));

    auto greater_than = PredicateBuilder::GreaterThan(/*field_index=*/2, /*field_name=*/"v0",
                                                      FieldType::INT, Literal(30));
    auto less_than = PredicateBuilder::LessThan(/*field_index=*/3, /*field_name=*/"f3",
                                                FieldType::INT, Literal(20));
    ASSERT_OK_AND_ASSIGN(auto or_predicate, PredicateBuilder::Or({greater_than, less_than}));

    auto less_or_equal = PredicateBuilder::LessOrEqual(/*field_index=*/4, /*field_name=*/"f2",
                                                       FieldType::INT, Literal(50));
    auto in = PredicateBuilder::In(/*field_index=*/5, /*field_name=*/"f0", FieldType::INT,
                                   {Literal(20), Literal(60)});
    ASSERT_OK_AND_ASSIGN(auto or_predicate2, PredicateBuilder::Or({less_or_equal, in}));

    auto greater_or_equal = PredicateBuilder::GreaterOrEqual(/*field_index=*/4, /*field_name=*/"v1",
                                                             FieldType::INT, Literal(120));

    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({and_predicate, or_predicate,
                                                                or_predicate2, greater_or_equal}));

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> result,
        PredicateUtils::CreatePickedFieldFilter(predicate, picked_field_name_to_idx));

    auto new_equal = PredicateBuilder::Equal(/*field_index=*/10, /*field_name=*/"f0",
                                             FieldType::INT, Literal(10));
    auto new_not_equal = PredicateBuilder::NotEqual(/*field_index=*/20, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(20));
    auto new_less_or_equal = PredicateBuilder::LessOrEqual(/*field_index=*/30, /*field_name=*/"f2",
                                                           FieldType::INT, Literal(50));
    auto new_in = PredicateBuilder::In(/*field_index=*/10, /*field_name=*/"f0", FieldType::INT,
                                       {Literal(20), Literal(60)});
    ASSERT_OK_AND_ASSIGN(auto new_or_predicate2, PredicateBuilder::Or({new_less_or_equal, new_in}));

    ASSERT_OK_AND_ASSIGN(auto expected,
                         PredicateBuilder::And({new_equal, new_not_equal, new_or_predicate2}));
    ASSERT_EQ(*result, *expected);
}

TEST(PredicateUtilsTest, TestGetAllNames) {
    {
        ASSERT_OK_AND_ASSIGN(
            auto child1,
            PredicateBuilder::Or({PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f0",
                                                           FieldType::BIGINT),
                                  PredicateBuilder::IsNull(/*field_index=*/1, /*field_name=*/"f1",
                                                           FieldType::BIGINT),
                                  PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"f2",
                                                           FieldType::BIGINT)}));

        auto sub_child1 =
            PredicateBuilder::IsNull(/*field_index=*/3, /*field_name=*/"f3", FieldType::BIGINT);
        auto sub_child2 =
            PredicateBuilder::IsNull(/*field_index=*/4, /*field_name=*/"f4", FieldType::BIGINT);
        auto sub_child3 =
            PredicateBuilder::IsNull(/*field_index=*/5, /*field_name=*/"f5", FieldType::BIGINT);
        ASSERT_OK_AND_ASSIGN(auto child2,
                             PredicateBuilder::And({sub_child1, sub_child2, sub_child3}));

        auto child3 =
            PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT);

        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({child1, child2, child3}));
        std::set<std::string> field_names;
        ASSERT_OK(PredicateUtils::GetAllNames(predicate, &field_names));
        ASSERT_EQ(field_names, std::set<std::string>({"f0", "f1", "f2", "f3", "f4", "f5"}));
    }
    {
        auto equal =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "apple", 5));
        auto not_equal = PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(20));
        ASSERT_OK_AND_ASSIGN(auto and_predicate, PredicateBuilder::And({equal, not_equal}));
        auto greater_than = PredicateBuilder::GreaterThan(/*field_index=*/2, /*field_name=*/"f2",
                                                          FieldType::INT, Literal(30));
        auto less_than = PredicateBuilder::LessThan(/*field_index=*/3, /*field_name=*/"f3",
                                                    FieldType::DOUBLE, Literal(20.1));
        ASSERT_OK_AND_ASSIGN(auto or_predicate, PredicateBuilder::Or({greater_than, less_than}));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({and_predicate, or_predicate}));
        std::set<std::string> field_names;
        ASSERT_OK(PredicateUtils::GetAllNames(predicate, &field_names));
        ASSERT_EQ(field_names, std::set<std::string>({"f0", "f1", "f2", "f3"}));
    }
}

}  // namespace paimon::test
