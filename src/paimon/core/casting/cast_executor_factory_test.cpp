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

#include "paimon/core/casting/cast_executor_factory.h"

#include "gtest/gtest.h"
#include "paimon/core/casting/binary_to_blob_cast_executor.h"
#include "paimon/core/casting/binary_to_string_cast_executor.h"
#include "paimon/core/casting/boolean_to_decimal_cast_executor.h"
#include "paimon/core/casting/boolean_to_numeric_cast_executor.h"
#include "paimon/core/casting/boolean_to_string_cast_executor.h"
#include "paimon/core/casting/date_to_string_cast_executor.h"
#include "paimon/core/casting/date_to_timestamp_cast_executor.h"
#include "paimon/core/casting/decimal_to_decimal_cast_executor.h"
#include "paimon/core/casting/decimal_to_numeric_primitive_cast_executor.h"
#include "paimon/core/casting/numeric_primitive_cast_executor.h"
#include "paimon/core/casting/numeric_primitive_to_decimal_cast_executor.h"
#include "paimon/core/casting/numeric_primitive_to_timestamp_cast_executor.h"
#include "paimon/core/casting/numeric_to_boolean_cast_executor.h"
#include "paimon/core/casting/numeric_to_string_cast_executor.h"
#include "paimon/core/casting/string_to_binary_cast_executor.h"
#include "paimon/core/casting/string_to_boolean_cast_executor.h"
#include "paimon/core/casting/string_to_date_cast_executor.h"
#include "paimon/core/casting/string_to_decimal_cast_executor.h"
#include "paimon/core/casting/string_to_numeric_primitive_cast_executor.h"
#include "paimon/core/casting/string_to_timestamp_cast_executor.h"
#include "paimon/core/casting/timestamp_to_date_cast_executor.h"
#include "paimon/core/casting/timestamp_to_numeric_primitive_cast_executor.h"
#include "paimon/core/casting/timestamp_to_string_cast_executor.h"
#include "paimon/core/casting/timestamp_to_timestamp_cast_executor.h"
#include "paimon/defs.h"

namespace paimon::test {
TEST(CastExecutorFactoryTest, TestRegister) {
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::TINYINT, FieldType::INT);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<NumericPrimitiveCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::INT, FieldType::BIGINT);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<NumericPrimitiveCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::DECIMAL, FieldType::TIMESTAMP);
        ASSERT_FALSE(cast_executor);
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::BOOLEAN, FieldType::TINYINT);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<BooleanToNumericCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::BOOLEAN, FieldType::STRING);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<BooleanToStringCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::INT, FieldType::BOOLEAN);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<NumericToBooleanCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::STRING, FieldType::BOOLEAN);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<StringToBooleanCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::STRING, FieldType::BIGINT);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<StringToNumericPrimitiveCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::FLOAT, FieldType::STRING);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<NumericToStringCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::STRING, FieldType::BINARY);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<StringToBinaryCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::BINARY, FieldType::STRING);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<BinaryToStringCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::BINARY, FieldType::BLOB);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<BinaryToBlobCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::DATE, FieldType::STRING);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<DateToStringCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::DATE, FieldType::TIMESTAMP);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<DateToTimestampCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::INT, FieldType::TIMESTAMP);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(
            std::dynamic_pointer_cast<NumericPrimitiveToTimestampCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::STRING, FieldType::DATE);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<StringToDateCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::TIMESTAMP, FieldType::STRING);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<TimestampToStringCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::TIMESTAMP, FieldType::DATE);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<TimestampToDateCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::TIMESTAMP, FieldType::BIGINT);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(
            std::dynamic_pointer_cast<TimestampToNumericPrimitiveCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::STRING, FieldType::TIMESTAMP);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<StringToTimestampCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::DECIMAL, FieldType::TINYINT);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(
            std::dynamic_pointer_cast<DecimalToNumericPrimitiveCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::BIGINT, FieldType::DECIMAL);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(
            std::dynamic_pointer_cast<NumericPrimitiveToDecimalCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::STRING, FieldType::DECIMAL);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<StringToDecimalCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::DECIMAL, FieldType::DECIMAL);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<DecimalToDecimalCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::TIMESTAMP, FieldType::TIMESTAMP);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<TimestampToTimestampCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::BOOLEAN, FieldType::DECIMAL);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<BooleanToDecimalCastExecutor>(cast_executor));
    }
    {
        // test non-exist cast executor
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::ARRAY, FieldType::MAP);
        ASSERT_FALSE(cast_executor);
    }
}
}  // namespace paimon::test
