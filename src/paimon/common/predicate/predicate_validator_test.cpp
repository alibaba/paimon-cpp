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

#include "paimon/common/predicate/predicate_validator.h"

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/result.h"
#include "paimon/testing/utils/testharness.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon::test {
TEST(PredicateValidatorTest, TestValidateLiterals) {
    std::string str("apple");
    {
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And({
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                        Literal(3l)),
                PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                        Literal(static_cast<float>(5.5))),
                PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2", FieldType::STRING,
                                        Literal(FieldType::STRING, str.data(), str.size())),
                PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3", FieldType::BOOLEAN,
                                        Literal(true)),
                PredicateBuilder::Equal(/*field_index=*/4, /*field_name=*/"f4", FieldType::DOUBLE,
                                        Literal(static_cast<double>(6.5))),
                PredicateBuilder::Equal(/*field_index=*/5, /*field_name=*/"f5", FieldType::TINYINT,
                                        Literal(static_cast<int8_t>(20))),
                PredicateBuilder::Equal(/*field_index=*/6, /*field_name=*/"f6", FieldType::DATE,
                                        Literal(FieldType::DATE, 3)),
                PredicateBuilder::Equal(/*field_index=*/7, /*field_name=*/"f7",
                                        FieldType::TIMESTAMP,
                                        Literal(Timestamp(1230422400000l, 123460))),
                PredicateBuilder::Equal(/*field_index=*/8, /*field_name=*/"f8", FieldType::DECIMAL,
                                        Literal(Decimal(23, 5, 123456))),
                PredicateBuilder::Equal(/*field_index=*/9, /*field_name=*/"f9", FieldType::BINARY,
                                        Literal(FieldType::BINARY, str.data(), str.size())),
            }));
        ASSERT_OK(PredicateValidator::ValidatePredicateWithLiterals(predicate));
    }
    {
        // f1 field type is FLOAT, literal type is BIGINT
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And({
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                        Literal(3l)),
                PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                        Literal(5l)),
                PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2", FieldType::STRING,
                                        Literal(FieldType::STRING, str.data(), str.size())),
                PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3", FieldType::BOOLEAN,
                                        Literal(true)),
            }));
        ASSERT_NOK_WITH_MSG(PredicateValidator::ValidatePredicateWithLiterals(predicate),
                            "field f1 has field type BIGINT in literal, mismatch "
                            "field type FLOAT in predicate");
    }
    {
        // f2 field type is STRING, literal type is BINARY
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And({
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                        Literal(3l)),
                PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                        Literal(static_cast<float>(5.5))),
                PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2", FieldType::STRING,
                                        Literal(FieldType::BINARY, str.data(), str.size())),
                PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3", FieldType::BOOLEAN,
                                        Literal(true)),
            }));
        ASSERT_NOK_WITH_MSG(PredicateValidator::ValidatePredicateWithLiterals(predicate),
                            "field f2 has field type BINARY in literal, mismatch "
                            "field type STRING in predicate");
    }
    {
        // f2 literal is null
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And({
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                        Literal(3l)),
                PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                        Literal(static_cast<float>(5.5))),
                PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2", FieldType::STRING,
                                        Literal(FieldType::STRING)),
                PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3", FieldType::BOOLEAN,
                                        Literal(true)),
            }));
        ASSERT_NOK_WITH_MSG(PredicateValidator::ValidatePredicateWithLiterals(predicate),
                            "literal cannot be null in predicate, field name f2");
    }
}

TEST(PredicateValidatorTest, TestValidateSchema) {
    std::string str("apple");
    {
        std::shared_ptr<arrow::Schema> schema = arrow::schema(arrow::FieldVector({
            arrow::field("f0", arrow::int64()),
            arrow::field("f1", arrow::float32()),
            arrow::field("f2", arrow::utf8()),
            arrow::field("f3", arrow::boolean()),
            arrow::field("f4", arrow::date32()),
            arrow::field("f5", arrow::timestamp(arrow::TimeUnit::NANO)),
            arrow::field("f6", arrow::decimal128(23, 5)),
            arrow::field("f7", arrow::binary()),
            arrow::field("f8", arrow::int8()),
        }));

        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And({
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                        Literal(3l)),
                PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                        Literal(static_cast<float>(5.5))),
                PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2", FieldType::STRING,
                                        Literal(FieldType::STRING, str.data(), str.size())),
                PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3", FieldType::BOOLEAN,
                                        Literal(true)),
                PredicateBuilder::Equal(/*field_index=*/4, /*field_name=*/"f4", FieldType::DATE,
                                        Literal(FieldType::DATE, 3)),
                PredicateBuilder::Equal(/*field_index=*/5, /*field_name=*/"f5",
                                        FieldType::TIMESTAMP,
                                        Literal(Timestamp(1230422400000l, 123460))),
                PredicateBuilder::Equal(/*field_index=*/6, /*field_name=*/"f6", FieldType::DECIMAL,
                                        Literal(Decimal(23, 5, 123456))),
                PredicateBuilder::Equal(/*field_index=*/7, /*field_name=*/"f7", FieldType::BINARY,
                                        Literal(FieldType::BINARY, str.data(), str.size())),
                PredicateBuilder::Equal(/*field_index=*/8, /*field_name=*/"f8", FieldType::TINYINT,
                                        Literal(static_cast<int8_t>(20))),
            }));
        ASSERT_OK(PredicateValidator::ValidatePredicateWithSchema(*schema, predicate,
                                                                  /*validate_field_idx=*/true));
    }
    {
        // f2 schema type is DECIMAL(23,5), predicate type can be different precision and scale,
        // such as DECIMAL(22,4)
        std::shared_ptr<arrow::Schema> schema = arrow::schema(arrow::FieldVector({
            arrow::field("f0", arrow::int16()),
            arrow::field("f1", arrow::float32()),
            arrow::field("f2", arrow::decimal128(23, 5)),
            arrow::field("f3", arrow::boolean()),
        }));

        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And({
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::SMALLINT,
                                        Literal(static_cast<int16_t>(3))),
                PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                        Literal(static_cast<float>(5.5))),
                PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2", FieldType::DECIMAL,
                                        Literal(Decimal(22, 4, 123456))),
                PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3", FieldType::BOOLEAN,
                                        Literal(true)),
            }));
        ASSERT_OK(PredicateValidator::ValidatePredicateWithLiterals(predicate));
        ASSERT_OK(PredicateValidator::ValidatePredicateWithSchema(*schema, predicate,
                                                                  /*validate_field_idx=*/true));
    }
    {
        // predicate field idx mismatch
        std::shared_ptr<arrow::Schema> schema = arrow::schema(arrow::FieldVector({
            arrow::field("f0", arrow::int64()),
            arrow::field("f1", arrow::float32()),
            arrow::field("f2", arrow::utf8()),
            arrow::field("f3", arrow::boolean()),
        }));

        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And({
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                        Literal(3l)),
                PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f1", FieldType::FLOAT,
                                        Literal(static_cast<float>(5.5))),
                PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2", FieldType::STRING,
                                        Literal(FieldType::STRING, str.data(), str.size())),
                PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3", FieldType::BOOLEAN,
                                        Literal(true)),
            }));

        ASSERT_NOK_WITH_MSG(
            PredicateValidator::ValidatePredicateWithSchema(*schema, predicate,
                                                            /*validate_field_idx=*/true),
            "field f1 has field idx 1 in input schema, mismatch "
            "field idx 2 in predicate");
    }
    {
        // predicate field idx mismatch, but not validate
        std::shared_ptr<arrow::Schema> schema = arrow::schema(arrow::FieldVector({
            arrow::field("f0", arrow::int64()),
            arrow::field("f1", arrow::float32()),
            arrow::field("f2", arrow::utf8()),
            arrow::field("f3", arrow::boolean()),
        }));

        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And({
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                        Literal(3l)),
                PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f1", FieldType::FLOAT,
                                        Literal(static_cast<float>(5.5))),
                PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2", FieldType::STRING,
                                        Literal(FieldType::STRING, str.data(), str.size())),
                PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3", FieldType::BOOLEAN,
                                        Literal(true)),
            }));

        ASSERT_OK(PredicateValidator::ValidatePredicateWithSchema(*schema, predicate,
                                                                  /*validate_field_idx=*/false));
    }
    {
        // f0 is uint16
        std::shared_ptr<arrow::Schema> schema = arrow::schema(arrow::FieldVector({
            arrow::field("f0", arrow::uint16()),
            arrow::field("f1", arrow::float32()),
            arrow::field("f2", arrow::utf8()),
            arrow::field("f3", arrow::boolean()),
        }));

        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And({
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::SMALLINT,
                                        Literal(static_cast<int16_t>(3))),
                PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                        Literal(static_cast<float>(5.5))),
                PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2", FieldType::STRING,
                                        Literal(FieldType::STRING, str.data(), str.size())),
                PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3", FieldType::BOOLEAN,
                                        Literal(true)),
            }));
        ASSERT_NOK_WITH_MSG(
            PredicateValidator::ValidatePredicateWithSchema(*schema, predicate,
                                                            /*validate_field_idx=*/true),
            "Invalid type uint16 for predicate");
    }
    {
        // f2 schema type is DOUBLE, predicate type is STRING
        std::shared_ptr<arrow::Schema> schema = arrow::schema(arrow::FieldVector({
            arrow::field("f0", arrow::int64()),
            arrow::field("f1", arrow::float32()),
            arrow::field("f2", arrow::float64()),
            arrow::field("f3", arrow::boolean()),
        }));

        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And({
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                        Literal(3l)),
                PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                        Literal(static_cast<float>(5.5))),
                PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2", FieldType::STRING,
                                        Literal(FieldType::STRING, str.data(), str.size())),
                PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3", FieldType::BOOLEAN,
                                        Literal(true)),
            }));
        ASSERT_NOK_WITH_MSG(
            PredicateValidator::ValidatePredicateWithSchema(*schema, predicate,
                                                            /*validate_field_idx=*/true),
            "schema type double mismatches predicate field type STRING");
    }
    {
        // f2 schema type is BINARY, predicate type is STRING
        std::shared_ptr<arrow::Schema> schema = arrow::schema(arrow::FieldVector({
            arrow::field("f0", arrow::int64()),
            arrow::field("f1", arrow::float32()),
            arrow::field("f2", arrow::binary()),
            arrow::field("f3", arrow::boolean()),
        }));

        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And({
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                        Literal(3l)),
                PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                        Literal(static_cast<float>(5.5))),
                PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2", FieldType::STRING,
                                        Literal(FieldType::STRING, str.data(), str.size())),
                PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3", FieldType::BOOLEAN,
                                        Literal(true)),
            }));
        ASSERT_NOK_WITH_MSG(
            PredicateValidator::ValidatePredicateWithSchema(*schema, predicate,
                                                            /*validate_field_idx=*/true),
            "schema type binary mismatches predicate field type STRING");
    }
    {
        // f2 in predicate does not exist in schema
        std::shared_ptr<arrow::Schema> schema = arrow::schema(arrow::FieldVector({
            arrow::field("f0", arrow::int16()),
            arrow::field("f1", arrow::float32()),
            arrow::field("f3", arrow::boolean()),
        }));

        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And({
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::SMALLINT,
                                        Literal(static_cast<int16_t>(3))),
                PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                        Literal(static_cast<float>(5.5))),
                PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2", FieldType::STRING,
                                        Literal(FieldType::STRING, str.data(), str.size())),
                PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3", FieldType::BOOLEAN,
                                        Literal(true)),
            }));

        ASSERT_NOK_WITH_MSG(
            PredicateValidator::ValidatePredicateWithSchema(*schema, predicate,
                                                            /*validate_field_idx=*/true),
            "field f2 does not exist in schema");
    }
}
}  // namespace paimon::test
