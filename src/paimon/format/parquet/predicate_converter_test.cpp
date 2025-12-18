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

#include "paimon/format/parquet/predicate_converter.h"

#include <cstdint>
#include <utility>

#include "arrow/compute/expression.h"
#include "gtest/gtest.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::parquet::test {

TEST(PredicateConverterTest, TestSimple) {
    // "struct<f0:bigint,f1:double,f2:string,f3:int,f4:tinyint,f5:decimal(6,2),f6:date,f7:timestamp>";
    {
        auto predicate =
            PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT);
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ("is_null(f0, {nan_is_null=false})", expression.ToString());
    }
    {
        auto predicate =
            PredicateBuilder::IsNotNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT);
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ("invert(is_null(f0, {nan_is_null=false}))", expression.ToString());
    }
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                 FieldType::BIGINT, Literal(5l));
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ("(f0 == 5)", expression.ToString());
    }
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                 FieldType::INT, Literal(10));
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ("(f3 == 10)", expression.ToString());
    }
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/6, /*field_name=*/"f6",
                                                 FieldType::DATE, Literal(FieldType::DATE, 10));
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ("(f6 == 1970-01-11)", expression.ToString());
    }
    {
        auto predicate = PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                    FieldType::BIGINT, Literal(5l));
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ("(f0 != 5)", expression.ToString());
    }
    {
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f0",
                                                       FieldType::BIGINT, Literal(5l));
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ("(f0 > 5)", expression.ToString());
    }
    {
        auto predicate = PredicateBuilder::GreaterOrEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                          FieldType::BIGINT, Literal(5l));
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ("(f0 >= 5)", expression.ToString());
    }
    {
        auto predicate = PredicateBuilder::GreaterOrEqual(
            /*field_index=*/4, /*field_name=*/"f4", FieldType::TINYINT,
            Literal(static_cast<int8_t>(16)));
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ("(f4 >= 16)", expression.ToString());
    }
    {
        auto predicate = PredicateBuilder::LessThan(/*field_index=*/0, /*field_name=*/"f0",
                                                    FieldType::BIGINT, Literal(5l));
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ("(f0 < 5)", expression.ToString());
    }
    {
        auto predicate = PredicateBuilder::LessOrEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                       FieldType::BIGINT, Literal(5l));
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ("(f0 <= 5)", expression.ToString());
    }
    {
        auto predicate =
            PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                 {Literal(1l), Literal(3l), Literal(5l)});
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ("(((f0 == 1) or (f0 == 3)) or (f0 == 5))", expression.ToString());
    }
    {
        auto predicate =
            PredicateBuilder::NotIn(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                    {Literal(1l), Literal(3l), Literal(5l)});
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ("(((f0 != 1) and (f0 != 3)) and (f0 != 5))", expression.ToString());
    }
    {
        // support decimal precision and scale mismatches between literal and data
        auto predicate = PredicateBuilder::In(/*field_index=*/7, /*field_name=*/"f7",
                                              FieldType::DECIMAL, {Literal(Decimal(5, 1, 12345))});
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ("(f7 == 1234.5)", expression.ToString());
    }
    {
        // do not support pushdown predicate with timestamp literal, will always return true
        auto predicate =
            PredicateBuilder::In(/*field_index=*/6, /*field_name=*/"f6", FieldType::TIMESTAMP,
                                 {Literal(Timestamp(1000, 12345))});
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ("true", expression.ToString());
    }
    {
        auto predicate = PredicateBuilder::LessOrEqual(
            /*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT, Literal(FieldType::BIGINT));
        ASSERT_NOK_WITH_MSG(
            PredicateConverter::Convert(predicate, /*predicate_node_count_limit=*/100),
            "literal cannot be null in predicate");
    }
}

TEST(PredicateConverterTest, TestCompound) {
    // "struct<f0:bigint,f1:float,f2:string,f3:boolean,f4:date,f5:timestamp,f6:decimal(6,2),f7:binary>";
    {
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And({
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                        Literal(3l)),
                PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                        Literal(static_cast<float>(5.0))),
                PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2", FieldType::STRING,
                                        Literal(FieldType::STRING, "apple", 5)),
                PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3", FieldType::BOOLEAN,
                                        Literal(true)),
                PredicateBuilder::Equal(/*field_index=*/4, /*field_name=*/"f4", FieldType::DATE,
                                        Literal(FieldType::DATE, 3)),
                PredicateBuilder::Equal(/*field_index=*/5, /*field_name=*/"f5",
                                        FieldType::TIMESTAMP,
                                        Literal(Timestamp(1725875365442l, 12000))),
                PredicateBuilder::Equal(/*field_index=*/6, /*field_name=*/"f6", FieldType::DECIMAL,
                                        Literal(Decimal(6, 2, 123456))),
                PredicateBuilder::Equal(/*field_index=*/7, /*field_name=*/"f7", FieldType::BINARY,
                                        Literal(FieldType::BINARY, "add", 3)),
            }));
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ(
            "((((((((f0 == 3) and (f1 == 5)) and (f2 == \"apple\")) and (f3 == true)) and (f4 == "
            "1970-01-04)) and true) and (f6 == 1234.56)) and true)",
            expression.ToString());
    }
    {
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::Or({
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                        Literal(3l)),
                PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                        Literal(static_cast<float>(5.0))),
                PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2", FieldType::STRING,
                                        Literal(FieldType::STRING, "apple", 5)),
                PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3", FieldType::BOOLEAN,
                                        Literal(true)),
            }));
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ("((((f0 == 3) or (f1 == 5)) or (f2 == \"apple\")) or (f3 == true))",
                  expression.ToString());
    }
    {
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::Or(
                {PredicateBuilder::And(
                     {PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                              FieldType::BOOLEAN, Literal(true)),
                      PredicateBuilder::LessThan(/*field_index=*/0, /*field_name=*/"f0",
                                                 FieldType::BIGINT, Literal(3l))})
                     .value(),
                 PredicateBuilder::And(
                     {PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                              FieldType::BOOLEAN, Literal(false)),
                      PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                 FieldType::FLOAT,
                                                 Literal(static_cast<float>(3.1)))})
                     .value()}));
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ("(((f3 == true) and (f0 < 3)) or ((f3 == false) and (f1 < 3.1)))",
                  expression.ToString());
    }
    {
        // predicate nodes containing binary type will not be pushed down
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And(
                {PredicateBuilder::Or(
                     {PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                              FieldType::BOOLEAN, Literal(true)),
                      PredicateBuilder::LessThan(
                          /*field_index=*/7, /*field_name=*/"f7", FieldType::BINARY,
                          Literal(FieldType::BINARY, "add", 3))})
                     .value(),
                 PredicateBuilder::Or(
                     {PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                              FieldType::BOOLEAN, Literal(false)),
                      PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                 FieldType::FLOAT,
                                                 Literal(static_cast<float>(3.1)))})
                     .value()}));
        ASSERT_OK_AND_ASSIGN(auto expression, PredicateConverter::Convert(
                                                  predicate, /*predicate_node_count_limit=*/100));
        ASSERT_EQ("(((f3 == true) or true) and ((f3 == false) or (f1 < 3.1)))",
                  expression.ToString());
    }
}

TEST(PredicateConverterTest, TestCollectNodeCount) {
    // And([And([Equal(f0, 10), NotEqual(f1, 20)]), Or([GreaterThan(v0, 30), LessThan(f3, 20)]),
    // Or([LessOrEqual(f2, 50), In(f0, [20, 60]), In(f0, [120, 160])]), GreaterOrEqual(v1, 120)])
    auto equal = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::INT,
                                         Literal(10));
    auto not_equal = PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                FieldType::INT, Literal(20));
    // and_predicate has 3 nodes
    ASSERT_OK_AND_ASSIGN(auto and_predicate, PredicateBuilder::And({equal, not_equal}));
    uint32_t node_count = 0;
    PredicateConverter::CollectNodeCount(and_predicate, &node_count);
    ASSERT_EQ(node_count, 3);

    auto greater_than = PredicateBuilder::GreaterThan(/*field_index=*/2, /*field_name=*/"v0",
                                                      FieldType::INT, Literal(30));
    auto less_than = PredicateBuilder::LessThan(/*field_index=*/3, /*field_name=*/"f3",
                                                FieldType::INT, Literal(20));
    // or_predicate has 3 nodes
    ASSERT_OK_AND_ASSIGN(auto or_predicate, PredicateBuilder::Or({greater_than, less_than}));
    node_count = 0;
    PredicateConverter::CollectNodeCount(or_predicate, &node_count);
    ASSERT_EQ(node_count, 3);

    auto less_or_equal = PredicateBuilder::LessOrEqual(/*field_index=*/4, /*field_name=*/"f2",
                                                       FieldType::INT, Literal(50));
    auto in = PredicateBuilder::In(/*field_index=*/5, /*field_name=*/"f0", FieldType::INT,
                                   {Literal(20), Literal(60)});
    auto not_in = PredicateBuilder::NotIn(/*field_index=*/5, /*field_name=*/"f0", FieldType::INT,
                                          {Literal(120), Literal(160)});
    // or_predicate2 has 8 nodes
    ASSERT_OK_AND_ASSIGN(auto or_predicate2, PredicateBuilder::Or({less_or_equal, in, not_in}));
    node_count = 0;
    PredicateConverter::CollectNodeCount(or_predicate2, &node_count);
    ASSERT_EQ(node_count, 8);

    auto greater_or_equal = PredicateBuilder::GreaterOrEqual(/*field_index=*/4, /*field_name=*/"v1",
                                                             FieldType::INT, Literal(120));
    node_count = 0;
    PredicateConverter::CollectNodeCount(greater_or_equal, &node_count);
    ASSERT_EQ(node_count, 1);

    // predicate has (1+3+3+8+1) nodes
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({and_predicate, or_predicate,
                                                                or_predicate2, greater_or_equal}));
    node_count = 0;
    PredicateConverter::CollectNodeCount(predicate, &node_count);
    ASSERT_EQ(node_count, 16);
}

TEST(PredicateConverterTest, TestExceedNodeCountLimit) {
    // And([And([Equal(f0, 10), NotEqual(f1, 20)]), Or([GreaterThan(v0, 30), LessThan(f3, 20)]),
    // Or([LessOrEqual(f2, 50), In(f0, [20, 60]), In(f0, [120, 160])]), GreaterOrEqual(v1, 120)])
    auto equal = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::INT,
                                         Literal(10));
    auto not_equal = PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                FieldType::INT, Literal(20));
    // and_predicate has 3 nodes
    ASSERT_OK_AND_ASSIGN(auto and_predicate, PredicateBuilder::And({equal, not_equal}));

    auto greater_than = PredicateBuilder::GreaterThan(/*field_index=*/2, /*field_name=*/"v0",
                                                      FieldType::INT, Literal(30));
    auto less_than = PredicateBuilder::LessThan(/*field_index=*/3, /*field_name=*/"f3",
                                                FieldType::INT, Literal(20));
    // or_predicate has 3 nodes
    ASSERT_OK_AND_ASSIGN(auto or_predicate, PredicateBuilder::Or({greater_than, less_than}));

    auto less_or_equal = PredicateBuilder::LessOrEqual(/*field_index=*/4, /*field_name=*/"f2",
                                                       FieldType::INT, Literal(50));
    auto in = PredicateBuilder::In(/*field_index=*/5, /*field_name=*/"f0", FieldType::INT,
                                   {Literal(20), Literal(60)});
    auto not_in = PredicateBuilder::NotIn(/*field_index=*/5, /*field_name=*/"f0", FieldType::INT,
                                          {Literal(120), Literal(160)});
    // or_predicate2 has 8 nodes
    ASSERT_OK_AND_ASSIGN(auto or_predicate2, PredicateBuilder::Or({less_or_equal, in, not_in}));

    auto greater_or_equal = PredicateBuilder::GreaterOrEqual(/*field_index=*/4, /*field_name=*/"v1",
                                                             FieldType::INT, Literal(120));

    // predicate has (1+3+3+8+1) nodes
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({and_predicate, or_predicate,
                                                                or_predicate2, greater_or_equal}));

    ASSERT_OK_AND_ASSIGN(auto expression,
                         PredicateConverter::Convert(predicate, /*predicate_node_count_limit=*/20));
    ASSERT_EQ(expression.ToString(),
              "(((((f0 == 10) and (f1 != 20)) and ((v0 > 30) or (f3 < 20))) and (((f2 <= 50) or "
              "((f0 == 20) or (f0 == 60))) or ((f0 != 120) and (f0 != 160)))) and (v1 >= 120))");
    ASSERT_OK_AND_ASSIGN(auto expression_always_true,
                         PredicateConverter::Convert(predicate, /*predicate_node_count_limit=*/10));
    ASSERT_EQ(expression_always_true.ToString(), "true");
}

TEST(PredicateConverterTest, TestWithoutPredicate) {
    ASSERT_OK_AND_ASSIGN(auto expression,
                         PredicateConverter::Convert(nullptr, /*predicate_node_count_limit=*/100));
    ASSERT_EQ("true", expression.ToString());
}

TEST(PredicateConverterTest, TestInvalidCase) {
    auto predicate =
        PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f0", FieldType::INT, {});
    ASSERT_NOK_WITH_MSG(PredicateConverter::Convert(predicate, /*predicate_node_count_limit=*/100),
                        "predicate [In] need literal on field f0");
}

}  // namespace paimon::parquet::test
