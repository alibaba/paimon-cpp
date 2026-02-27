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

#include "paimon/format/orc/predicate_converter.h"

#include <utility>

#include "gtest/gtest.h"
#include "orc/Type.hh"
#include "orc/sargs/SearchArgument.hh"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::orc::test {

TEST(PredicateConverterTest, TestSimple) {
    std::string orc_schema =
        "struct<f0:bigint,f1:double,f2:string,f3:int,f4:tinyint,f5:decimal(6,2)>";
    std::unique_ptr<::orc::Type> orc_type = ::orc::Type::buildTypeFromString(orc_schema);
    {
        auto predicate =
            PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT);
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("leaf-0 = (column(id=1) is null), expr = leaf-0", search_arg->toString());
    }
    {
        auto predicate =
            PredicateBuilder::IsNotNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT);
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("leaf-0 = (column(id=1) is null), expr = (not leaf-0)", search_arg->toString());
    }
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                 FieldType::BIGINT, Literal(5l));
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("leaf-0 = (column(id=1) = 5), expr = leaf-0", search_arg->toString());
    }
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                 FieldType::INT, Literal(10));
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("leaf-0 = (column(id=4) = 10), expr = leaf-0", search_arg->toString());
    }
    {
        // Noted: equal null convert to is null in orc, which is different from java paimon,
        // where any field value equal/not equal null return false
        // therefore, we disable literal in predicate to be null (see PredicateValidator)
        auto predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                 FieldType::BIGINT, Literal(FieldType::BIGINT));
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("leaf-0 = (column(id=1) is null), expr = leaf-0", search_arg->toString());
    }
    {
        auto predicate = PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                    FieldType::BIGINT, Literal(5l));
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("leaf-0 = (column(id=1) = 5), expr = (not leaf-0)", search_arg->toString());
    }
    {
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f0",
                                                       FieldType::BIGINT, Literal(5l));
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("leaf-0 = (column(id=1) <= 5), expr = (not leaf-0)", search_arg->toString());
    }
    {
        auto predicate = PredicateBuilder::GreaterOrEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                          FieldType::BIGINT, Literal(5l));
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("leaf-0 = (column(id=1) < 5), expr = (not leaf-0)", search_arg->toString());
    }
    {
        auto predicate = PredicateBuilder::GreaterOrEqual(
            /*field_index=*/4, /*field_name=*/"f4", FieldType::TINYINT,
            Literal(static_cast<int8_t>(16)));
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("leaf-0 = (column(id=5) < 16), expr = (not leaf-0)", search_arg->toString());
    }
    {
        auto predicate = PredicateBuilder::LessThan(/*field_index=*/0, /*field_name=*/"f0",
                                                    FieldType::BIGINT, Literal(5l));
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("leaf-0 = (column(id=1) < 5), expr = leaf-0", search_arg->toString());
    }
    {
        auto predicate = PredicateBuilder::LessOrEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                       FieldType::BIGINT, Literal(5l));
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("leaf-0 = (column(id=1) <= 5), expr = leaf-0", search_arg->toString());
    }
    {
        auto predicate =
            PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                 {Literal(1l), Literal(3l), Literal(5l)});
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("leaf-0 = (column(id=1) in [1, 3, 5]), expr = leaf-0", search_arg->toString());
    }
    {
        ASSERT_OK_AND_ASSIGN(
            const auto predicate,
            PredicateBuilder::StartsWith(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                         Literal(FieldType::STRING, "aab", 3)));
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("expr = YES", search_arg->toString());
    }
    {
        ASSERT_OK_AND_ASSIGN(
            const auto predicate,
            PredicateBuilder::EndsWith(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                       Literal(FieldType::STRING, "bcc", 3)));
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("expr = YES", search_arg->toString());
    }
    {
        ASSERT_OK_AND_ASSIGN(
            const auto predicate,
            PredicateBuilder::Contains(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                       Literal(FieldType::STRING, "abc", 3)));
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("expr = YES", search_arg->toString());
    }
    {
        ASSERT_OK_AND_ASSIGN(
            const auto predicate,
            PredicateBuilder::Like(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                   Literal(FieldType::STRING, "abc", 3)));
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("expr = YES", search_arg->toString());
    }
    {
        auto predicate =
            PredicateBuilder::NotIn(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                    {Literal(1l), Literal(3l), Literal(5l)});
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("leaf-0 = (column(id=1) in [1, 3, 5]), expr = (not leaf-0)",
                  search_arg->toString());
    }
    {
        // orc format do not support only one decimal for In Predicate
        auto predicate = PredicateBuilder::In(/*field_index=*/5, /*field_name=*/"f5",
                                              FieldType::DECIMAL, {Literal(Decimal(6, 2, 12345))});
        ASSERT_NOK_WITH_MSG(PredicateConverter::Convert(*orc_type, predicate),
                            "At least two literals are required!");
    }
    {
        // test idx mismatches in orc type
        auto predicate =
            PredicateBuilder::IsNull(/*field_index=*/3, /*field_name=*/"f0", FieldType::BIGINT);
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("leaf-0 = (column(id=1) is null), expr = leaf-0", search_arg->toString());
    }
    {
        // support decimal precision and scale mismatches between literal and data
        auto predicate =
            PredicateBuilder::In(/*field_index=*/5, /*field_name=*/"f5", FieldType::DECIMAL,
                                 {Literal(Decimal(5, 1, 12345)), Literal(Decimal(5, 1, 54321))});
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ("leaf-0 = (column(id=6) in [1234.5, 5432.1]), expr = leaf-0",
                  search_arg->toString());
    }
}

TEST(PredicateConverterTest, TestCompound) {
    std::string orc_schema =
        "struct<f0:bigint,f1:float,f2:string,f3:boolean,f4:date,f5:timestamp,f6:decimal(6,2),f7:"
        "binary>";
    std::unique_ptr<::orc::Type> orc_type = ::orc::Type::buildTypeFromString(orc_schema);
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
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ(
            "leaf-0 = (column(id=1) = 3), "
            "leaf-1 = (column(id=2) = 5), "
            "leaf-2 = (column(id=3) = apple), "
            "leaf-3 = (column(id=4) = true), "
            "leaf-4 = (column(id=5) = 3), "
            "leaf-5 = (column(id=6) = 1725875365.442012000), "
            "leaf-6 = (column(id=7) = 1234.56), "
            "expr = (and leaf-0 leaf-1 leaf-2 leaf-3 leaf-4 leaf-5 leaf-6)",
            search_arg->toString());
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
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ(
            "leaf-0 = (column(id=1) = 3), "
            "leaf-1 = (column(id=2) = 5), "
            "leaf-2 = (column(id=3) = apple), "
            "leaf-3 = (column(id=4) = true), "
            "expr = (or leaf-0 leaf-1 leaf-2 leaf-3)",
            search_arg->toString());
    }
    {
        auto predicate =
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
                     .value()})
                .value();
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ(
            "leaf-0 = (column(id=4) = true), "
            "leaf-1 = (column(id=4) = false), "
            "leaf-2 = (column(id=1) < 3), "
            "leaf-3 = (column(id=2) < 3.1), "
            "expr = (and (or leaf-0 leaf-1) (or leaf-2 leaf-1) "
            "(or leaf-0 leaf-3) (or leaf-2 leaf-3))",
            search_arg->toString());
    }
    {
        // predicate nodes containing binary type will not be pushed down
        auto predicate = PredicateBuilder::And(
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
                                   PredicateBuilder::LessThan(/*field_index=*/1,
                                                              /*field_name=*/"f1", FieldType::FLOAT,
                                                              Literal(static_cast<float>(3.1)))})
                                  .value()})
                             .value();
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_EQ(
            "leaf-0 = (column(id=4) = false), "
            "leaf-1 = (column(id=2) < 3.1), "
            "expr = (or leaf-0 leaf-1)",
            search_arg->toString());
    }
    {
        // predicate nodes containing binary type will not be pushed down with OR Predicate
        auto predicate =
            PredicateBuilder::Or(
                {PredicateBuilder::Or(
                     {PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                              FieldType::BOOLEAN, Literal(true)),
                      PredicateBuilder::LessThan(
                          /*field_index=*/7, /*field_name=*/"f7", FieldType::BINARY,
                          Literal(FieldType::BINARY, "add", 3))})
                     .value(),
                 PredicateBuilder::Or(
                     {PredicateBuilder::Equal(/*field_index=*/4, /*field_name=*/"f4",
                                              FieldType::DATE, Literal(FieldType::DATE, 3)),
                      PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                 FieldType::FLOAT,
                                                 Literal(static_cast<float>(3.1)))})
                     .value()})
                .value();
        ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, predicate));
        ASSERT_FALSE(search_arg);
    }
}

TEST(PredicateConverterTest, TestWithoutPredicate) {
    std::string orc_schema = "struct<f0:int,f1:double,f2:string>";
    std::unique_ptr<::orc::Type> orc_type = ::orc::Type::buildTypeFromString(orc_schema);
    ASSERT_OK_AND_ASSIGN(auto search_arg, PredicateConverter::Convert(*orc_type, nullptr));
    ASSERT_FALSE(search_arg);
}

TEST(PredicateConverterTest, TestInvalidCase) {
    std::string orc_schema = "struct<f0:int,f1:double,f2:string>";
    std::unique_ptr<::orc::Type> orc_type = ::orc::Type::buildTypeFromString(orc_schema);
    {
        auto predicate =
            PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f0", FieldType::INT, {});
        ASSERT_NOK_WITH_MSG(PredicateConverter::Convert(*orc_type, predicate),
                            "predicate [In] need literal on field f0");
    }
    {
        auto predicate = PredicateBuilder::In(/*field_index=*/4, /*field_name=*/"f3",
                                              FieldType::INT, {Literal(3)});
        ASSERT_NOK_WITH_MSG(PredicateConverter::Convert(*orc_type, predicate),
                            "field f3 is not exist in data file");
    }
}

}  // namespace paimon::orc::test
