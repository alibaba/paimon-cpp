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

#include "paimon/core/casting/cast_executor.h"

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/builder_dict.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/core/casting/binary_to_string_cast_executor.h"
#include "paimon/core/casting/boolean_to_decimal_cast_executor.h"
#include "paimon/core/casting/boolean_to_numeric_cast_executor.h"
#include "paimon/core/casting/boolean_to_string_cast_executor.h"
#include "paimon/core/casting/casting_utils.h"
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
#include "paimon/data/decimal.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"

namespace paimon::test {

class CastExecutorTest : public ::testing::Test {
 public:
    // for numeric
    template <typename DataType, typename BuilderType>
    std::shared_ptr<arrow::Array> MakeArrowArray(const std::shared_ptr<arrow::DataType>& type,
                                                 const std::vector<DataType>& data) const {
        std::shared_ptr<arrow::Array> array;
        auto builder = arrow::MakeBuilder(type).ValueOrDie();
        auto typed_builder = dynamic_cast<BuilderType*>(builder.get());
        for (size_t i = 0; i < data.size(); ++i) {
            EXPECT_TRUE(typed_builder->Append(data[i]).ok());
        }
        EXPECT_TRUE(typed_builder->AppendNull().ok());
        EXPECT_TRUE(typed_builder->Finish(&array).ok());
        return array;
    }

    void CheckArrayResult(const std::shared_ptr<CastExecutor>& cast_executor,
                          const std::shared_ptr<arrow::DataType>& target_type,
                          const std::shared_ptr<arrow::Array>& src_array,
                          const std::shared_ptr<arrow::Array>& expected_array) const {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> target_array,
                             cast_executor->Cast(src_array, target_type, arrow_pool_.get()));
        ASSERT_TRUE(
            target_array->Equals(expected_array, arrow::EqualOptions::Defaults().nans_equal(true)))
            << "target:" << target_array->ToString() << "expected:" << expected_array->ToString();
    }

    void CheckArrayApproxResult(const std::shared_ptr<CastExecutor>& cast_executor,
                                const std::shared_ptr<arrow::DataType>& target_type,
                                const std::shared_ptr<arrow::Array>& src_array,
                                const std::shared_ptr<arrow::Array>& expected_array) const {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> target_array,
                             cast_executor->Cast(src_array, target_type, arrow_pool_.get()));
        ASSERT_TRUE(target_array->ApproxEquals(expected_array,
                                               arrow::EqualOptions::Defaults().nans_equal(true)))
            << "target:" << target_array->ToString() << "expected:" << expected_array->ToString();
    }
    void CheckArrayResult(const std::shared_ptr<CastExecutor>& cast_executor,
                          const std::shared_ptr<arrow::DataType>& src_type,
                          const std::shared_ptr<arrow::DataType>& target_type,
                          const std::string& src_array_str,
                          const std::string& target_array_str) const {
        auto src_array =
            arrow::ipc::internal::json::ArrayFromJSON(src_type, src_array_str).ValueOrDie();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> target_array,
                             cast_executor->Cast(src_array, target_type, arrow_pool_.get()));
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(target_type, target_array_str).ValueOrDie();
        ASSERT_TRUE(target_array->Equals(expected_array))
            << "target:" << target_array->ToString() << "expected:" << expected_array->ToString();
    }
    std::string CheckArrayInvalidResult(const std::shared_ptr<CastExecutor>& cast_executor,
                                        const std::shared_ptr<arrow::DataType>& src_type,
                                        const std::shared_ptr<arrow::DataType>& target_type,
                                        const std::string& src_array_str) const {
        auto src_array =
            arrow::ipc::internal::json::ArrayFromJSON(src_type, src_array_str).ValueOrDie();
        auto target_array = cast_executor->Cast(src_array, target_type, arrow_pool_.get());
        EXPECT_FALSE(target_array.ok()) << target_array.value()->ToString();
        return target_array.status().ToString();
    }

    template <typename Type>
    Literal CreateLiteral(const FieldType& type, const Type& data) const {
        if constexpr (std::is_same_v<Type, std::string>) {
            return Literal(type, data.data(), data.size());
        } else if constexpr (std::is_same_v<Type, int32_t>) {
            if (type == FieldType::DATE) {
                return Literal(type, data);
            } else {
                return Literal(data);
            }
        } else {
            return Literal(data);
        }
    }

    template <typename SrcType, typename TargetType>
    void CheckLiteralResult(const std::shared_ptr<CastExecutor>& cast_executor,
                            const FieldType& src_type, const std::vector<SrcType>& src_data,
                            const std::shared_ptr<arrow::DataType>& target_type,
                            const std::vector<TargetType>& target_data) const {
        FieldType target_field_type = FieldTypeUtils::ConvertToFieldType(target_type->id()).value();
        std::vector<Literal> src_literals;
        std::vector<Literal> target_literals;
        for (size_t i = 0; i < src_data.size(); i++) {
            src_literals.push_back(CreateLiteral<SrcType>(src_type, src_data[i]));
            target_literals.push_back(CreateLiteral<TargetType>(target_field_type, target_data[i]));
        }

        std::vector<Literal> result_literals;
        for (size_t i = 0; i < src_data.size(); i++) {
            ASSERT_OK_AND_ASSIGN(Literal result, cast_executor->Cast(src_literals[i], target_type));
            ASSERT_FALSE(result.IsNull());
            ASSERT_EQ(target_literals[i], result)
                << target_literals[i].ToString() << "->" << result.ToString();
        }
        // test null
        Literal null_literal(src_type);
        ASSERT_OK_AND_ASSIGN(Literal result, cast_executor->Cast(null_literal, target_type));
        ASSERT_EQ(Literal(target_field_type), result);
    }

    template <typename SrcType>
    void CheckNullLiteralResult(const std::shared_ptr<CastExecutor>& cast_executor,
                                const FieldType& src_type, const std::vector<SrcType>& src_data,
                                const std::shared_ptr<arrow::DataType>& target_type) const {
        for (size_t i = 0; i < src_data.size(); i++) {
            Literal src_literal = CreateLiteral<SrcType>(src_type, src_data[i]);
            ASSERT_OK_AND_ASSIGN(Literal result, cast_executor->Cast(src_literal, target_type));
            ASSERT_TRUE(result.IsNull());
        }
    }

    template <typename SrcType>
    std::string CheckLiteralInvalidResult(
        const std::shared_ptr<CastExecutor>& cast_executor, const FieldType& src_type,
        const SrcType& src_data, const std::shared_ptr<arrow::DataType>& target_type) const {
        Literal src_literal = CreateLiteral<SrcType>(src_type, src_data);
        auto result = cast_executor->Cast(src_literal, target_type);
        EXPECT_FALSE(result.ok());
        return result.status().ToString();
    }

 public:
    static constexpr int64_t NANOS_PER_SECOND = 1000000000l;
    static constexpr int64_t MILLIS_PER_SECOND = 1000l;
    static constexpr int64_t NANOS_PER_MILLIS = 1000000l;
    static constexpr int64_t MAX_INT64 = std::numeric_limits<int64_t>::max();
    static constexpr int64_t MIN_INT64 = std::numeric_limits<int64_t>::min();
    static constexpr float MAX_FLOAT = std::numeric_limits<float>::max();
    static constexpr float MIN_FLOAT = std::numeric_limits<float>::lowest();
    static constexpr double MAX_DOUBLE = std::numeric_limits<double>::max();
    static constexpr double MIN_DOUBLE = std::numeric_limits<double>::lowest();

 private:
    std::shared_ptr<arrow::MemoryPool> arrow_pool_ = GetArrowPool(GetDefaultPool());
};

TEST_F(CastExecutorTest, TestNumericPrimitiveCastExecutorCastLiteral) {
    auto cast_executor = std::make_shared<NumericPrimitiveCastExecutor>();
    // test src type TINYINT
    {
        std::vector<int8_t> src_data = {1, 2, 3, -10, 0, 127, -128};
        std::vector<int8_t> target_data = {1, 2, 3, -10, 0, 127, -128};
        CheckLiteralResult<int8_t, int8_t>(cast_executor, FieldType::TINYINT, src_data,
                                           arrow::int8(), target_data);
    }
    {
        std::vector<int8_t> src_data = {1, 2, 3, -10, 0, 127, -128};
        std::vector<int16_t> target_data = {1, 2, 3, -10, 0, 127, -128};
        CheckLiteralResult<int8_t, int16_t>(cast_executor, FieldType::TINYINT, src_data,
                                            arrow::int16(), target_data);
    }
    {
        std::vector<int8_t> src_data = {1, 2, 3, -10, 0, 127, -128};
        std::vector<int32_t> target_data = {1, 2, 3, -10, 0, 127, -128};
        CheckLiteralResult<int8_t, int32_t>(cast_executor, FieldType::TINYINT, src_data,
                                            arrow::int32(), target_data);
    }
    {
        std::vector<int8_t> src_data = {1, 2, 3, -10, 0, 127, -128};
        std::vector<int64_t> target_data = {1, 2, 3, -10, 0, 127, -128};
        CheckLiteralResult<int8_t, int64_t>(cast_executor, FieldType::TINYINT, src_data,
                                            arrow::int64(), target_data);
    }
    {
        std::vector<int8_t> src_data = {1, 2, 3, -10, 0, 127, -128};
        std::vector<float> target_data = {1, 2, 3, -10, 0, 127, -128};
        CheckLiteralResult<int8_t, float>(cast_executor, FieldType::TINYINT, src_data,
                                          arrow::float32(), target_data);
    }
    {
        std::vector<int8_t> src_data = {1, 2, 3, -10, 0, 127, -128};
        std::vector<double> target_data = {1, 2, 3, -10, 0, 127, -128};
        CheckLiteralResult<int8_t, double>(cast_executor, FieldType::TINYINT, src_data,
                                           arrow::float64(), target_data);
    }
    // test src type SMALLINT
    {
        std::vector<int16_t> src_data = {1, 2, 3, -10, 0, 32767, -32768};
        std::vector<int8_t> target_data = {1, 2, 3, -10, 0, -1, 0};
        CheckLiteralResult<int16_t, int8_t>(cast_executor, FieldType::SMALLINT, src_data,
                                            arrow::int8(), target_data);
    }
    {
        std::vector<int16_t> src_data = {1, 2, 3, -10, 0, 32767, -32768};
        std::vector<int16_t> target_data = {1, 2, 3, -10, 0, 32767, -32768};
        CheckLiteralResult<int16_t, int16_t>(cast_executor, FieldType::SMALLINT, src_data,
                                             arrow::int16(), target_data);
    }
    {
        std::vector<int16_t> src_data = {1, 2, 3, -10, 0, 32767, -32768};
        std::vector<int32_t> target_data = {1, 2, 3, -10, 0, 32767, -32768};
        CheckLiteralResult<int16_t, int32_t>(cast_executor, FieldType::SMALLINT, src_data,
                                             arrow::int32(), target_data);
    }
    {
        std::vector<int16_t> src_data = {1, 2, 3, -10, 0, 32767, -32768};
        std::vector<int64_t> target_data = {1, 2, 3, -10, 0, 32767, -32768};
        CheckLiteralResult<int16_t, int64_t>(cast_executor, FieldType::SMALLINT, src_data,
                                             arrow::int64(), target_data);
    }
    {
        std::vector<int16_t> src_data = {1, 2, 3, -10, 0, 32767, -32768};
        std::vector<float> target_data = {1, 2, 3, -10, 0, 32767, -32768};
        CheckLiteralResult<int16_t, float>(cast_executor, FieldType::SMALLINT, src_data,
                                           arrow::float32(), target_data);
    }
    {
        std::vector<int16_t> src_data = {1, 2, 3, -10, 0, 32767, -32768};
        std::vector<double> target_data = {1, 2, 3, -10, 0, 32767, -32768};
        CheckLiteralResult<int16_t, double>(cast_executor, FieldType::SMALLINT, src_data,
                                            arrow::float64(), target_data);
    }
    // test src type INT
    {
        std::vector<int32_t> src_data = {1, 2, 3, -10, 0, 2147483647, -2147483648};
        std::vector<int8_t> target_data = {1, 2, 3, -10, 0, -1, 0};
        CheckLiteralResult<int32_t, int8_t>(cast_executor, FieldType::INT, src_data, arrow::int8(),
                                            target_data);
    }
    {
        std::vector<int32_t> src_data = {1, 2, 3, -10, 0, 2147483647, -2147483648};
        std::vector<int16_t> target_data = {1, 2, 3, -10, 0, -1, 0};
        CheckLiteralResult<int32_t, int16_t>(cast_executor, FieldType::INT, src_data,
                                             arrow::int16(), target_data);
    }
    {
        std::vector<int32_t> src_data = {1, 2, 3, -10, 0, 2147483647, -2147483648};
        std::vector<int32_t> target_data = {1, 2, 3, -10, 0, 2147483647, -2147483648};
        CheckLiteralResult<int32_t, int32_t>(cast_executor, FieldType::INT, src_data,
                                             arrow::int32(), target_data);
    }
    {
        std::vector<int32_t> src_data = {1, 2, 3, -10, 0, 2147483647, -2147483648};
        std::vector<int64_t> target_data = {1, 2, 3, -10, 0, 2147483647, -2147483648};
        CheckLiteralResult<int32_t, int64_t>(cast_executor, FieldType::INT, src_data,
                                             arrow::int64(), target_data);
    }
    {
        std::vector<int32_t> src_data = {1, 2, 3, -10, 0, 2147483647, -2147483648};
        std::vector<float> target_data = {1, 2, 3, -10, 0, 2147483647.0f, -2147483648.0f};
        CheckLiteralResult<int32_t, float>(cast_executor, FieldType::INT, src_data,
                                           arrow::float32(), target_data);
    }
    {
        std::vector<int32_t> src_data = {1, 2, 3, -10, 0, 2147483647, -2147483648};
        std::vector<double> target_data = {1, 2, 3, -10, 0, 2147483647, -2147483648};
        CheckLiteralResult<int32_t, double>(cast_executor, FieldType::INT, src_data,
                                            arrow::float64(), target_data);
    }
    // test src type BIGINT
    {
        std::vector<int64_t> src_data = {1, 2, 3, -10, 0, MAX_INT64, MIN_INT64};
        std::vector<int8_t> target_data = {1, 2, 3, -10, 0, -1, 0};
        CheckLiteralResult<int64_t, int8_t>(cast_executor, FieldType::BIGINT, src_data,
                                            arrow::int8(), target_data);
    }
    {
        std::vector<int64_t> src_data = {1, 2, 3, -10, 0, MAX_INT64, MIN_INT64};
        std::vector<int16_t> target_data = {1, 2, 3, -10, 0, -1, 0};
        CheckLiteralResult<int64_t, int16_t>(cast_executor, FieldType::BIGINT, src_data,
                                             arrow::int16(), target_data);
    }
    {
        std::vector<int64_t> src_data = {1, 2, 3, -10, 0, MAX_INT64, MIN_INT64};
        std::vector<int32_t> target_data = {1, 2, 3, -10, 0, -1, 0};
        CheckLiteralResult<int64_t, int32_t>(cast_executor, FieldType::BIGINT, src_data,
                                             arrow::int32(), target_data);
    }
    {
        std::vector<int64_t> src_data = {1, 2, 3, -10, 0, MAX_INT64, MIN_INT64};
        std::vector<int64_t> target_data = {1, 2, 3, -10, 0, MAX_INT64, MIN_INT64};
        CheckLiteralResult<int64_t, int64_t>(cast_executor, FieldType::BIGINT, src_data,
                                             arrow::int64(), target_data);
    }
    {
        std::vector<int64_t> src_data = {1, 2, 3, -10, 0, MAX_INT64, MIN_INT64};
        std::vector<float> target_data = {
            1, 2, 3, -10, 0, static_cast<float>(MAX_INT64), static_cast<float>(MIN_INT64)};
        CheckLiteralResult<int64_t, float>(cast_executor, FieldType::BIGINT, src_data,
                                           arrow::float32(), target_data);
    }
    {
        std::vector<int64_t> src_data = {1, 2, 3, -10, 0, MAX_INT64, MIN_INT64};
        std::vector<double> target_data = {
            1, 2, 3, -10, 0, static_cast<double>(MAX_INT64), static_cast<double>(MIN_INT64)};
        CheckLiteralResult<int64_t, double>(cast_executor, FieldType::BIGINT, src_data,
                                            arrow::float64(), target_data);
    }
    // test src type FLOAT
    {
        // Java Paimon cast MAX_FLOAT to -1 while C++ Paimon cast to 0
        // Java Paimon cast INFINITY to -1 while C++ Paimon cast to 0
        std::vector<float> src_data = {1.1,       2.2,       3.3,      -10.01,    0,
                                       MAX_FLOAT, MIN_FLOAT, INFINITY, -INFINITY, NAN};
        std::vector<int8_t> target_data = {1, 2, 3, -10, 0, 0, 0, 0, 0, 0};
        CheckLiteralResult<float, int8_t>(cast_executor, FieldType::FLOAT, src_data, arrow::int8(),
                                          target_data);
    }
    {
        // Java Paimon cast MAX_FLOAT to -1 while C++ Paimon cast to 0
        // Java Paimon cast INFINITY to -1 while C++ Paimon cast to 0
        std::vector<float> src_data = {1.1,       2.2,       3.3,      -10.01,    0,
                                       MAX_FLOAT, MIN_FLOAT, INFINITY, -INFINITY, NAN};
        std::vector<int16_t> target_data = {1, 2, 3, -10, 0, 0, 0, 0, 0, 0};
        CheckLiteralResult<float, int16_t>(cast_executor, FieldType::FLOAT, src_data,
                                           arrow::int16(), target_data);
    }
    {
        // Java Paimon cast MAX_FLOAT to 2147483647 while C++ Paimon cast to -2147483648
        // Java Paimon cast INFINITY to 2147483647 while C++ Paimon cast to -2147483648
        // Java Paimon cast NAN to 0 while C++ Paimon cast to -2147483648
        std::vector<float> src_data = {1.1,       2.2,       3.3,      -10.01,    0,
                                       MAX_FLOAT, MIN_FLOAT, INFINITY, -INFINITY, NAN};
        std::vector<int32_t> target_data = {
            1, 2, 3, -10, 0, -2147483648, -2147483648, -2147483648, -2147483648, -2147483648};
        CheckLiteralResult<float, int32_t>(cast_executor, FieldType::FLOAT, src_data,
                                           arrow::int32(), target_data);
    }
    {
        // Java Paimon cast MAX_FLOAT to MAX_INT64 while C++ Paimon cast to MIN_INT64
        // Java Paimon cast INFINITY to MAX_INT64 while C++ Paimon cast to MIN_INT64
        // Java Paimon cast NAN to 0 while C++ Paimon cast to MIN_INT64
        std::vector<float> src_data = {1.1,       2.2,       3.3,      -10.01,    0,
                                       MAX_FLOAT, MIN_FLOAT, INFINITY, -INFINITY, NAN};
        std::vector<int64_t> target_data = {1,         2,         3,         -10,       0,
                                            MIN_INT64, MIN_INT64, MIN_INT64, MIN_INT64, MIN_INT64};
        CheckLiteralResult<float, int64_t>(cast_executor, FieldType::FLOAT, src_data,
                                           arrow::int64(), target_data);
    }
    {
        std::vector<float> src_data = {1.1,       2.2,       3.3,      -10.01,    0,
                                       MAX_FLOAT, MIN_FLOAT, INFINITY, -INFINITY, NAN};
        std::vector<float> target_data = {1.1,       2.2,       3.3,      -10.01,    0,
                                          MAX_FLOAT, MIN_FLOAT, INFINITY, -INFINITY, NAN};
        CheckLiteralResult<float, float>(cast_executor, FieldType::FLOAT, src_data,
                                         arrow::float32(), target_data);
    }
    {
        std::vector<float> src_data = {1.1,       2.2,       3.3,      -10.01,    0,
                                       MAX_FLOAT, MIN_FLOAT, INFINITY, -INFINITY, NAN};
        std::vector<double> target_data = {1.1,
                                           2.2,
                                           3.3,
                                           -10.01,
                                           0,
                                           static_cast<double>(MAX_FLOAT),
                                           static_cast<double>(MIN_FLOAT),
                                           INFINITY,
                                           -INFINITY,
                                           NAN};
        CheckLiteralResult<float, double>(cast_executor, FieldType::FLOAT, src_data,
                                          arrow::float64(), target_data);
    }
    // test src type DOUBLE
    {
        // Java Paimon cast MAX_DOUBLE to -1 while C++ Paimon cast to 0
        // Java Paimon cast INFINITY to -1 while C++ Paimon cast to 0
        std::vector<double> src_data = {1.1,        2.2,        3.3,      -10.01,    0,
                                        MAX_DOUBLE, MIN_DOUBLE, INFINITY, -INFINITY, NAN};
        std::vector<int8_t> target_data = {1, 2, 3, -10, 0, 0, 0, 0, 0, 0};
        CheckLiteralResult<double, int8_t>(cast_executor, FieldType::DOUBLE, src_data,
                                           arrow::int8(), target_data);
    }
    {
        // Java Paimon cast MAX_DOUBLE to -1 while C++ Paimon cast to 0
        // Java Paimon cast INFINITY to -1 while C++ Paimon cast to 0
        std::vector<double> src_data = {1.1,        2.2,        3.3,      -10.01,    0,
                                        MAX_DOUBLE, MIN_DOUBLE, INFINITY, -INFINITY, NAN};
        std::vector<int16_t> target_data = {1, 2, 3, -10, 0, 0, 0, 0, 0, 0};
        CheckLiteralResult<double, int16_t>(cast_executor, FieldType::DOUBLE, src_data,
                                            arrow::int16(), target_data);
    }
    {
        // Java Paimon cast MAX_DOUBLE to 2147483647 while C++ Paimon cast to -2147483648
        // Java Paimon cast INFINITY to 2147483647 while C++ Paimon cast to -2147483648
        // Java Paimon cast NAN to 0 while C++ Paimon cast to -2147483648
        std::vector<double> src_data = {1.1,        2.2,        3.3,      -10.01,    0,
                                        MAX_DOUBLE, MIN_DOUBLE, INFINITY, -INFINITY, NAN};
        std::vector<int32_t> target_data = {
            1, 2, 3, -10, 0, -2147483648, -2147483648, -2147483648, -2147483648, -2147483648};
        CheckLiteralResult<double, int32_t>(cast_executor, FieldType::DOUBLE, src_data,
                                            arrow::int32(), target_data);
    }
    {
        // Java Paimon cast MAX_DOUBLE to MAX_INT64 while C++ Paimon cast to MIN_INT64
        // Java Paimon cast INFINITY to MAX_INT64 while C++ Paimon cast to MIN_INT64
        // Java Paimon cast NAN to 0 while C++ Paimon cast to MIN_INT64
        std::vector<double> src_data = {1.1,        2.2,        3.3,      -10.01,    0,
                                        MAX_DOUBLE, MIN_DOUBLE, INFINITY, -INFINITY, NAN};
        std::vector<int64_t> target_data = {1,         2,         3,         -10,       0,
                                            MIN_INT64, MIN_INT64, MIN_INT64, MIN_INT64, MIN_INT64};
        CheckLiteralResult<double, int64_t>(cast_executor, FieldType::DOUBLE, src_data,
                                            arrow::int64(), target_data);
    }
    {
        std::vector<double> src_data = {1.1,        2.2,        3.3,      -10.01,    0,
                                        MAX_DOUBLE, MIN_DOUBLE, INFINITY, -INFINITY, NAN};
        std::vector<float> target_data = {1.1,      2.2,       3.3,      -10.01,    0,
                                          INFINITY, -INFINITY, INFINITY, -INFINITY, NAN};
        CheckLiteralResult<double, float>(cast_executor, FieldType::DOUBLE, src_data,
                                          arrow::float32(), target_data);
    }
    {
        std::vector<double> src_data = {1.1,        2.2,        3.3,      -10.01,    0,
                                        MAX_DOUBLE, MIN_DOUBLE, INFINITY, -INFINITY, NAN};
        std::vector<double> target_data = {1.1,        2.2,        3.3,      -10.01,    0,
                                           MAX_DOUBLE, MIN_DOUBLE, INFINITY, -INFINITY, NAN};
        CheckLiteralResult<double, double>(cast_executor, FieldType::DOUBLE, src_data,
                                           arrow::float64(), target_data);
    }
}

TEST_F(CastExecutorTest, TestNumericPrimitiveCastExecutorCastArray) {
    auto cast_executor = std::make_shared<NumericPrimitiveCastExecutor>();
    // test src type TINYINT
    {
        CheckArrayResult(cast_executor, arrow::int8(), arrow::int8(),
                         "[1, 2, 3, null, -10, 0, 127, -128]",
                         "[1, 2, 3, null, -10, 0, 127, -128]");
        CheckArrayResult(cast_executor, arrow::int8(), arrow::int16(),
                         "[1, 2, 3, null, -10, 0, 127, -128]",
                         "[1, 2, 3, null, -10, 0, 127, -128]");
        CheckArrayResult(cast_executor, arrow::int8(), arrow::int32(),
                         "[1, 2, 3, null, -10, 0, 127, -128]",
                         "[1, 2, 3, null, -10, 0, 127, -128]");
        CheckArrayResult(cast_executor, arrow::int8(), arrow::int64(),
                         "[1, 2, 3, null, -10, 0, 127, -128]",
                         "[1, 2, 3, null, -10, 0, 127, -128]");
        CheckArrayResult(cast_executor, arrow::int8(), arrow::float32(),
                         "[1, 2, 3, null, -10, 0, 127, -128]",
                         "[1, 2, 3, null, -10, 0, 127, -128]");
        CheckArrayResult(cast_executor, arrow::int8(), arrow::float64(),
                         "[1, 2, 3, null, -10, 0, 127, -128]",
                         "[1, 2, 3, null, -10, 0, 127, -128]");
    }
    // test src type SMALLINT
    {
        CheckArrayResult(cast_executor, arrow::int16(), arrow::int8(),
                         "[1, 2, 3, null, -10, 0, 32767, -32768]",
                         "[1, 2, 3, null, -10, 0, -1, 0]");
        CheckArrayResult(cast_executor, arrow::int16(), arrow::int16(),
                         "[1, 2, 3, null, -10, 0, 32767, -32768]",
                         "[1, 2, 3, null, -10, 0, 32767, -32768]");
        CheckArrayResult(cast_executor, arrow::int16(), arrow::int32(),
                         "[1, 2, 3, null, -10, 0, 32767, -32768]",
                         "[1, 2, 3, null, -10, 0, 32767, -32768]");
        CheckArrayResult(cast_executor, arrow::int16(), arrow::int64(),
                         "[1, 2, 3, null, -10, 0, 32767, -32768]",
                         "[1, 2, 3, null, -10, 0, 32767, -32768]");
        CheckArrayResult(cast_executor, arrow::int16(), arrow::float32(),
                         "[1, 2, 3, null, -10, 0, 32767, -32768]",
                         "[1, 2, 3, null, -10, 0, 32767, -32768]");
        CheckArrayResult(cast_executor, arrow::int16(), arrow::float64(),
                         "[1, 2, 3, null, -10, 0, 32767, -32768]",
                         "[1, 2, 3, null, -10, 0, 32767, -32768]");
    }
    // test src type INT
    {
        CheckArrayResult(cast_executor, arrow::int32(), arrow::int8(),
                         "[1, 2, 3, null, -10, 0, 2147483647, -2147483648]",
                         "[1, 2, 3, null, -10, 0, -1, 0]");
        CheckArrayResult(cast_executor, arrow::int32(), arrow::int16(),
                         "[1, 2, 3, null, -10, 0, 2147483647, -2147483648]",
                         "[1, 2, 3, null, -10, 0, -1, 0]");
        CheckArrayResult(cast_executor, arrow::int32(), arrow::int32(),
                         "[1, 2, 3, null, -10, 0, 2147483647, -2147483648]",
                         "[1, 2, 3, null, -10, 0, 2147483647, -2147483648]");
        CheckArrayResult(cast_executor, arrow::int32(), arrow::int64(),
                         "[1, 2, 3, null, -10, 0, 2147483647, -2147483648]",
                         "[1, 2, 3, null, -10, 0, 2147483647, -2147483648]");
        CheckArrayResult(cast_executor, arrow::int32(), arrow::float32(),
                         "[1, 2, 3, null, -10, 0, 2147483647, -2147483648]",
                         "[1, 2, 3, null, -10, 0, 2147483647, -2147483648]");
        CheckArrayResult(cast_executor, arrow::int32(), arrow::float64(),
                         "[1, 2, 3, null, -10, 0, 2147483647, -2147483648]",
                         "[1, 2, 3, null, -10, 0, 2147483647, -2147483648]");
    }
    // test src type BIGINT
    {
        CheckArrayResult(cast_executor, arrow::int64(), arrow::int8(),
                         "[1, 2, 3, null, -10, 0, 9223372036854775807, -9223372036854775808]",
                         "[1, 2, 3, null, -10, 0, -1, 0]");
        CheckArrayResult(cast_executor, arrow::int64(), arrow::int16(),
                         "[1, 2, 3, null, -10, 0, 9223372036854775807, -9223372036854775808]",
                         "[1, 2, 3, null, -10, 0, -1, 0]");
        CheckArrayResult(cast_executor, arrow::int64(), arrow::int32(),
                         "[1, 2, 3, null, -10, 0, 9223372036854775807, -9223372036854775808]",
                         "[1, 2, 3, null, -10, 0, -1, 0]");
        CheckArrayResult(cast_executor, arrow::int64(), arrow::int64(),
                         "[1, 2, 3, null, -10, 0, 9223372036854775807, -9223372036854775808]",
                         "[1, 2, 3, null, -10, 0, 9223372036854775807, -9223372036854775808]");
        CheckArrayResult(cast_executor, arrow::int64(), arrow::float32(),
                         "[1, 2, 3, null, -10, 0, 9223372036854775807, -9223372036854775808]",
                         "[1, 2, 3, null, -10, 0, 9223372036854775807, -9223372036854775808]");
        CheckArrayResult(cast_executor, arrow::int64(), arrow::float64(),
                         "[1, 2, 3, null, -10, 0, 9223372036854775807, -9223372036854775808]",
                         "[1, 2, 3, null, -10, 0, 9223372036854775807, -9223372036854775808]");
    }
    // test src type float
    {
        // Java Paimon cast MAX_FLOAT to -1 while C++ Paimon cast to 0
        // Java Paimon cast INFINITY to -1 while C++ Paimon cast to 0
        std::vector<float> src_data = {1.1,       2.2,       3.3,      -10.01,    0,
                                       MAX_FLOAT, MIN_FLOAT, INFINITY, -INFINITY, NAN};
        auto src_array = MakeArrowArray<float, arrow::FloatBuilder>(arrow::float32(), src_data);
        std::vector<int8_t> expected_data = {1, 2, 3, -10, 0, 0, 0, 0, 0, 0};
        auto expected_array =
            MakeArrowArray<int8_t, arrow::Int8Builder>(arrow::int8(), expected_data);
        CheckArrayResult(cast_executor, arrow::int8(), src_array, expected_array);
    }
    {
#ifndef NDEBUG
        // Java Paimon cast MAX_FLOAT to -1 while C++ Paimon cast to 0
        // Java Paimon cast INFINITY to -1 while C++ Paimon cast to 0
        std::vector<float> src_data = {1.1,       2.2,       3.3,      -10.01,    0,
                                       MAX_FLOAT, MIN_FLOAT, INFINITY, -INFINITY, NAN};
        auto src_array = MakeArrowArray<float, arrow::FloatBuilder>(arrow::float32(), src_data);
        std::vector<int16_t> expected_data = {1, 2, 3, -10, 0, 0, 0, 0, 0, 0};
        auto expected_array =
            MakeArrowArray<int16_t, arrow::Int16Builder>(arrow::int16(), expected_data);
        CheckArrayResult(cast_executor, arrow::int16(), src_array, expected_array);
#endif
    }
    {
        // Java Paimon cast MAX_FLOAT to 2147483647 while C++ Paimon cast to -2147483648
        // Java Paimon cast INFINITY to 2147483647 while C++ Paimon cast to -2147483648
        // Java Paimon cast NAN to 0 while C++ Paimon cast to -2147483648
        std::vector<float> src_data = {1.1,       2.2,       3.3,      -10.01,    0,
                                       MAX_FLOAT, MIN_FLOAT, INFINITY, -INFINITY, NAN};
        auto src_array = MakeArrowArray<float, arrow::FloatBuilder>(arrow::float32(), src_data);
        std::vector<int32_t> expected_data = {
            1, 2, 3, -10, 0, -2147483648, -2147483648, -2147483648, -2147483648, -2147483648};
        auto expected_array =
            MakeArrowArray<int32_t, arrow::Int32Builder>(arrow::int32(), expected_data);
        CheckArrayResult(cast_executor, arrow::int32(), src_array, expected_array);
    }
    {
        // Java Paimon cast MAX_FLOAT to MAX_INT64 while C++ Paimon cast to MIN_INT64
        // Java Paimon cast INFINITY to MAX_INT64 while C++ Paimon cast to MIN_INT64
        // Java Paimon cast NAN to 0 while C++ Paimon cast to MIN_INT64
        std::vector<float> src_data = {1.1,       2.2,       3.3,      -10.01,    0,
                                       MAX_FLOAT, MIN_FLOAT, INFINITY, -INFINITY, NAN};
        auto src_array = MakeArrowArray<float, arrow::FloatBuilder>(arrow::float32(), src_data);
        std::vector<int64_t> expected_data = {
            1, 2, 3, -10, 0, MIN_INT64, MIN_INT64, MIN_INT64, MIN_INT64, MIN_INT64};
        auto expected_array =
            MakeArrowArray<int64_t, arrow::Int64Builder>(arrow::int64(), expected_data);
        CheckArrayResult(cast_executor, arrow::int64(), src_array, expected_array);
    }
    {
        std::vector<float> src_data = {1.1,       2.2,       3.3,      -10.01,    0,
                                       MAX_FLOAT, MIN_FLOAT, INFINITY, -INFINITY, NAN};
        auto src_array = MakeArrowArray<float, arrow::FloatBuilder>(arrow::float32(), src_data);
        std::vector<float> expected_data = {1.1,       2.2,       3.3,      -10.01,    0,
                                            MAX_FLOAT, MIN_FLOAT, INFINITY, -INFINITY, NAN};
        auto expected_array =
            MakeArrowArray<float, arrow::FloatBuilder>(arrow::float32(), expected_data);
        CheckArrayResult(cast_executor, arrow::float32(), src_array, expected_array);
    }
    {
        std::vector<float> src_data = {1.1,       2.2,       3.3,      -10.01,    0,
                                       MAX_FLOAT, MIN_FLOAT, INFINITY, -INFINITY, NAN};
        auto src_array = MakeArrowArray<float, arrow::FloatBuilder>(arrow::float32(), src_data);
        std::vector<double> expected_data = {1.1,       2.2,       3.3,      -10.01,    0,
                                             MAX_FLOAT, MIN_FLOAT, INFINITY, -INFINITY, NAN};
        auto expected_array =
            MakeArrowArray<double, arrow::DoubleBuilder>(arrow::float64(), expected_data);
        CheckArrayApproxResult(cast_executor, arrow::float64(), src_array, expected_array);
    }

    // test src type double
    {
        // Java Paimon cast MAX_DOUBLE to -1 while C++ Paimon cast to 0
        // Java Paimon cast INFINITY to -1 while C++ Paimon cast to 0
        std::vector<double> src_data = {1.1,        2.2,        3.3,      -10.01,    0,
                                        MAX_DOUBLE, MIN_DOUBLE, INFINITY, -INFINITY, NAN};
        auto src_array = MakeArrowArray<double, arrow::DoubleBuilder>(arrow::float64(), src_data);
        std::vector<int8_t> expected_data = {1, 2, 3, -10, 0, 0, 0, 0, 0, 0};
        auto expected_array =
            MakeArrowArray<int8_t, arrow::Int8Builder>(arrow::int8(), expected_data);
        CheckArrayResult(cast_executor, arrow::int8(), src_array, expected_array);
    }
    {
        // Java Paimon cast MAX_DOUBLE to -1 while C++ Paimon cast to 0
        // Java Paimon cast INFINITY to -1 while C++ Paimon cast to 0
        std::vector<double> src_data = {1.1,        2.2,        3.3,      -10.01,    0,
                                        MAX_DOUBLE, MIN_DOUBLE, INFINITY, -INFINITY, NAN};
        auto src_array = MakeArrowArray<double, arrow::DoubleBuilder>(arrow::float64(), src_data);
        std::vector<int16_t> expected_data = {1, 2, 3, -10, 0, 0, 0, 0, 0, 0};
        auto expected_array =
            MakeArrowArray<int16_t, arrow::Int16Builder>(arrow::int16(), expected_data);
        CheckArrayResult(cast_executor, arrow::int16(), src_array, expected_array);
    }
    {
        // Java Paimon cast MAX_DOUBLE to 2147483647 while C++ Paimon cast to -2147483648
        // Java Paimon cast INFINITY to 2147483647 while C++ Paimon cast to -2147483648
        // Java Paimon cast NAN to 0 while C++ Paimon cast to -2147483648
        std::vector<double> src_data = {1.1,        2.2,        3.3,      -10.01,    0,
                                        MAX_DOUBLE, MIN_DOUBLE, INFINITY, -INFINITY, NAN};
        auto src_array = MakeArrowArray<double, arrow::DoubleBuilder>(arrow::float64(), src_data);
        std::vector<int32_t> expected_data = {
            1, 2, 3, -10, 0, -2147483648, -2147483648, -2147483648, -2147483648, -2147483648};
        auto expected_array =
            MakeArrowArray<int32_t, arrow::Int32Builder>(arrow::int32(), expected_data);
        CheckArrayResult(cast_executor, arrow::int32(), src_array, expected_array);
    }
    {
        // Java Paimon cast MAX_DOUBLE to MAX_INT64 while C++ Paimon cast to MIN_INT64
        // Java Paimon cast INFINITY to MAX_INT64 while C++ Paimon cast to MIN_INT64
        // Java Paimon cast NAN to 0 while C++ Paimon cast to MIN_INT64
        std::vector<double> src_data = {1.1,        2.2,        3.3,      -10.01,    0,
                                        MAX_DOUBLE, MIN_DOUBLE, INFINITY, -INFINITY, NAN};
        auto src_array = MakeArrowArray<double, arrow::DoubleBuilder>(arrow::float64(), src_data);
        std::vector<int64_t> expected_data = {
            1, 2, 3, -10, 0, MIN_INT64, MIN_INT64, MIN_INT64, MIN_INT64, MIN_INT64};
        auto expected_array =
            MakeArrowArray<int64_t, arrow::Int64Builder>(arrow::int64(), expected_data);
        CheckArrayResult(cast_executor, arrow::int64(), src_array, expected_array);
    }
    {
        std::vector<double> src_data = {1.1,        2.2,        3.3,      -10.01,    0,
                                        MAX_DOUBLE, MIN_DOUBLE, INFINITY, -INFINITY, NAN};
        auto src_array = MakeArrowArray<double, arrow::DoubleBuilder>(arrow::float64(), src_data);
        std::vector<float> expected_data = {1.1,      2.2,       3.3,      -10.01,    0,
                                            INFINITY, -INFINITY, INFINITY, -INFINITY, NAN};
        auto expected_array =
            MakeArrowArray<float, arrow::FloatBuilder>(arrow::float32(), expected_data);
        CheckArrayResult(cast_executor, arrow::float32(), src_array, expected_array);
    }
    {
        std::vector<double> src_data = {1.1,        2.2,        3.3,      -10.01,    0,
                                        MAX_DOUBLE, MIN_DOUBLE, INFINITY, -INFINITY, NAN};
        auto src_array = MakeArrowArray<double, arrow::DoubleBuilder>(arrow::float64(), src_data);
        std::vector<double> expected_data = {1.1,        2.2,        3.3,      -10.01,    0,
                                             MAX_DOUBLE, MIN_DOUBLE, INFINITY, -INFINITY, NAN};
        auto expected_array =
            MakeArrowArray<double, arrow::DoubleBuilder>(arrow::float64(), expected_data);
        CheckArrayApproxResult(cast_executor, arrow::float64(), src_array, expected_array);
    }
}

TEST_F(CastExecutorTest, TestBooleanToNumericCastExecutorCastLiteral) {
    auto cast_executor = std::make_shared<BooleanToNumericCastExecutor>();
    std::vector<bool> src_data = {true, false};
    {
        std::vector<int8_t> target_data = {1, 0};
        CheckLiteralResult<bool, int8_t>(cast_executor, FieldType::BOOLEAN, src_data, arrow::int8(),
                                         target_data);
    }
    {
        std::vector<int16_t> target_data = {1, 0};
        CheckLiteralResult<bool, int16_t>(cast_executor, FieldType::BOOLEAN, src_data,
                                          arrow::int16(), target_data);
    }
    {
        std::vector<int32_t> target_data = {1, 0};
        CheckLiteralResult<bool, int32_t>(cast_executor, FieldType::BOOLEAN, src_data,
                                          arrow::int32(), target_data);
    }
    {
        std::vector<int64_t> target_data = {1, 0};
        CheckLiteralResult<bool, int64_t>(cast_executor, FieldType::BOOLEAN, src_data,
                                          arrow::int64(), target_data);
    }
    {
        std::vector<float> target_data = {1, 0};
        CheckLiteralResult<bool, float>(cast_executor, FieldType::BOOLEAN, src_data,
                                        arrow::float32(), target_data);
    }
    {
        std::vector<double> target_data = {1, 0};
        CheckLiteralResult<bool, double>(cast_executor, FieldType::BOOLEAN, src_data,
                                         arrow::float64(), target_data);
    }
}

TEST_F(CastExecutorTest, TestBooleanToNumericCastExecutorCastArray) {
    auto cast_executor = std::make_shared<BooleanToNumericCastExecutor>();
    CheckArrayResult(cast_executor, arrow::boolean(), arrow::int8(), "[true, false, null]",
                     "[1, 0, null]");
    CheckArrayResult(cast_executor, arrow::boolean(), arrow::int16(), "[true, false, null]",
                     "[1, 0, null]");
    CheckArrayResult(cast_executor, arrow::boolean(), arrow::int32(), "[true, false, null]",
                     "[1, 0, null]");
    CheckArrayResult(cast_executor, arrow::boolean(), arrow::int64(), "[true, false, null]",
                     "[1, 0, null]");
    CheckArrayResult(cast_executor, arrow::boolean(), arrow::float32(), "[true, false, null]",
                     "[1, 0, null]");
    CheckArrayResult(cast_executor, arrow::boolean(), arrow::float64(), "[true, false, null]",
                     "[1, 0, null]");
}

TEST_F(CastExecutorTest, TestBooleanToStringCastExecutorCastLiteral) {
    auto cast_executor = std::make_shared<BooleanToStringCastExecutor>();
    std::vector<bool> src_data = {true, false};
    std::vector<std::string> target_data = {"true", "false"};
    CheckLiteralResult<bool, std::string>(cast_executor, FieldType::BOOLEAN, src_data,
                                          arrow::utf8(), target_data);
}

TEST_F(CastExecutorTest, TestBooleanToStringCastExecutorCastArray) {
    auto cast_executor = std::make_shared<BooleanToStringCastExecutor>();
    CheckArrayResult(cast_executor, arrow::boolean(), arrow::utf8(), "[true, false, null]",
                     R"(["true", "false", null])");
}

TEST_F(CastExecutorTest, TestNumericToBooleanCastExecutorCastLiteral) {
    auto cast_executor = std::make_shared<NumericToBooleanCastExecutor>();
    std::vector<bool> target_data = {true, false, true, true, true, true};
    {
        std::vector<int8_t> src_data = {1, 0, -1, 10, 127, -128};
        CheckLiteralResult<int8_t, bool>(cast_executor, FieldType::TINYINT, src_data,
                                         arrow::boolean(), target_data);
    }
    {
        std::vector<int16_t> src_data = {1, 0, -1, 10, 32767, -32768};
        CheckLiteralResult<int16_t, bool>(cast_executor, FieldType::SMALLINT, src_data,
                                          arrow::boolean(), target_data);
    }
    {
        std::vector<int32_t> src_data = {1, 0, -1, 10, 2147483647, -2147483648};
        CheckLiteralResult<int32_t, bool>(cast_executor, FieldType::INT, src_data, arrow::boolean(),
                                          target_data);
    }
    {
        // TODO(liancheng.lsz): Java Paimon now cast MIN_INT64 to false, they will fix this.
        std::vector<int64_t> src_data = {1, 0, -1, 10, MAX_INT64, MIN_INT64};
        std::vector<bool> target_data_int64 = {true, false, true, true, true, true};
        CheckLiteralResult<int64_t, bool>(cast_executor, FieldType::BIGINT, src_data,
                                          arrow::boolean(), target_data_int64);
    }
}

TEST_F(CastExecutorTest, TestNumericToBooleanCastExecutorCastArray) {
    auto cast_executor = std::make_shared<NumericToBooleanCastExecutor>();
    CheckArrayResult(cast_executor, arrow::int8(), arrow::boolean(),
                     "[1, 2, 3, null, -10, 0, 127, -128]",
                     "[true, true, true, null, true, false, true, true]");
    CheckArrayResult(cast_executor, arrow::int16(), arrow::boolean(),
                     "[1, 2, 3, null, -10, 0, 32767, -32768]",
                     "[true, true, true, null, true, false, true, true]");
    CheckArrayResult(cast_executor, arrow::int32(), arrow::boolean(),
                     "[1, 2, 3, null, -10, 0, 2147483647, -2147483648]",
                     "[true, true, true, null, true, false, true, true]");
    // TODO(liancheng.lsz): Java Paimon now cast MIN_INT64 to false, they will fix this.
    CheckArrayResult(cast_executor, arrow::int64(), arrow::boolean(),
                     "[1, 2, 3, null, -10, 0, 9223372036854775807, -9223372036854775808]",
                     "[true, true, true, null, true, false, true, true]");
}

TEST_F(CastExecutorTest, TestStringToBooleanCastExecutorCastLiteral) {
    auto cast_executor = std::make_shared<StringToBooleanCastExecutor>();
    {
        std::vector<std::string> src_data = {"t", "true",  "y", "yes", "1",
                                             "f", "false", "n", "no",  "0"};
        std::vector<bool> target_data = {true,  true,  true,  true,  true,
                                         false, false, false, false, false};
        CheckLiteralResult<std::string, bool>(cast_executor, FieldType::STRING, src_data,
                                              arrow::boolean(), target_data);
    }
    {
        // invalid src string
        std::string src_data = "";
        auto msg = CheckLiteralInvalidResult<std::string>(cast_executor, FieldType::STRING,
                                                          src_data, arrow::boolean());
        ASSERT_TRUE(
            msg.find("StringToBooleanCastExecutor cast failed: STRING '' cannot cast to BOOLEAN") !=
            std::string::npos);
    }
    {
        // invalid src string
        std::string src_data = "ttrue";
        auto msg = CheckLiteralInvalidResult<std::string>(cast_executor, FieldType::STRING,
                                                          src_data, arrow::boolean());
        ASSERT_TRUE(
            msg.find(
                "StringToBooleanCastExecutor cast failed: STRING 'ttrue' cannot cast to BOOLEAN") !=
            std::string::npos);
    }
}

TEST_F(CastExecutorTest, TestStringToBooleanCastExecutorCastArray) {
    auto cast_executor = std::make_shared<StringToBooleanCastExecutor>();
    {
        CheckArrayResult(cast_executor, arrow::utf8(), arrow::boolean(),
                         R"(["t", "true", "y", "yes", "1", "f", "false", "n", "no", "0", null])",
                         "[true, true, true, true, true, false, false, false, false, false, null]");
    }
    {
        // invalid src string
        auto msg =
            CheckArrayInvalidResult(cast_executor, arrow::utf8(), arrow::boolean(), R"([""])");
        ASSERT_TRUE(
            msg.find("StringToBooleanCastExecutor cast failed: STRING '' cannot cast to BOOLEAN") !=
            std::string::npos);
    }
    {
        // invalid src string
        auto msg = CheckArrayInvalidResult(cast_executor, arrow::utf8(), arrow::boolean(),
                                           R"(["true", "ttrue"])");
        ASSERT_TRUE(
            msg.find(
                "StringToBooleanCastExecutor cast failed: STRING 'ttrue' cannot cast to BOOLEAN") !=
            std::string::npos);
    }
}

TEST_F(CastExecutorTest, TestStringToNumericPrimitiveCastExecutorCastLiteral) {
    auto cast_executor = std::make_shared<StringToNumericPrimitiveCastExecutor>();
    {
        std::vector<std::string> src_data = {"1", "0", "-1", "10", "127", "-128"};
        std::vector<int8_t> target_data = {1, 0, -1, 10, 127, -128};
        CheckLiteralResult<std::string, int8_t>(cast_executor, FieldType::STRING, src_data,
                                                arrow::int8(), target_data);
    }
    {
        std::vector<std::string> src_data = {"1", "0", "-1", "10", "32767", "-32768"};
        std::vector<int16_t> target_data = {1, 0, -1, 10, 32767, -32768};
        CheckLiteralResult<std::string, int16_t>(cast_executor, FieldType::STRING, src_data,
                                                 arrow::int16(), target_data);
    }
    {
        std::vector<std::string> src_data = {"1", "0", "-1", "10", "2147483647", "-2147483648"};
        std::vector<int32_t> target_data = {1, 0, -1, 10, 2147483647, -2147483648};
        CheckLiteralResult<std::string, int32_t>(cast_executor, FieldType::STRING, src_data,
                                                 arrow::int32(), target_data);
    }
    {
        std::vector<std::string> src_data = {
            "1", "0", "-1", "10", "9223372036854775807", "-9223372036854775808"};
        std::vector<int64_t> target_data = {1, 0, -1, 10, MAX_INT64, MIN_INT64};
        CheckLiteralResult<std::string, int64_t>(cast_executor, FieldType::STRING, src_data,
                                                 arrow::int64(), target_data);
    }
    {
        // Keyword in Java Paimon is "Infinity", "-Infinity", "NaN", while in C++ Paimon is "inf",
        // "-inf", "nan"
        std::vector<std::string> src_data = {
            "1.0", "2.2",  "3.3", "-10.01", "0", "3.4028235e+38", "-3.4028235e+38",
            "inf", "-inf", "nan"};
        std::vector<float> target_data = {1.0,       2.2,       3.3,      -10.01,    0,
                                          MAX_FLOAT, MIN_FLOAT, INFINITY, -INFINITY, NAN};
        CheckLiteralResult<std::string, float>(cast_executor, FieldType::STRING, src_data,
                                               arrow::float32(), target_data);
    }
    {
        // Keyword in Java Paimon is "Infinity", "-Infinity", "NaN", while in C++ Paimon is "inf",
        // "-inf", "nan"
        std::vector<std::string> src_data = {"1.0",
                                             "2.2",
                                             "3.3",
                                             "-10.01",
                                             "0",
                                             "1.7976931348623157e+308",
                                             "-1.7976931348623157e+308",
                                             "inf",
                                             "-inf",
                                             "nan"};
        std::vector<double> target_data = {1.0,        2.2,        3.3,      -10.01,    0,
                                           MAX_DOUBLE, MIN_DOUBLE, INFINITY, -INFINITY, NAN};
        CheckLiteralResult<std::string, double>(cast_executor, FieldType::STRING, src_data,
                                                arrow::float64(), target_data);
    }
    // test overflow
    {
        std::string src_data = "128";
        auto msg = CheckLiteralInvalidResult<std::string>(cast_executor, FieldType::STRING,
                                                          src_data, arrow::int8());
        ASSERT_TRUE(msg.find("cast literal in StringToNumericPrimitiveCastExecutor failed: cannot "
                             "cast '128' from STRING to TINYINT") != std::string::npos);
    }
    {
        std::string src_data = "-129";
        auto msg = CheckLiteralInvalidResult<std::string>(cast_executor, FieldType::STRING,
                                                          src_data, arrow::int8());
        ASSERT_TRUE(msg.find("cast literal in StringToNumericPrimitiveCastExecutor failed: cannot "
                             "cast '-129' from STRING to TINYINT") != std::string::npos);
    }
    {
        std::string src_data = "32768";
        auto msg = CheckLiteralInvalidResult<std::string>(cast_executor, FieldType::STRING,
                                                          src_data, arrow::int16());
        ASSERT_TRUE(msg.find("cast literal in StringToNumericPrimitiveCastExecutor failed: cannot "
                             "cast '32768' from STRING to SMALLINT") != std::string::npos);
    }
    {
        std::string src_data = "-32769";
        auto msg = CheckLiteralInvalidResult<std::string>(cast_executor, FieldType::STRING,
                                                          src_data, arrow::int16());
        ASSERT_TRUE(msg.find("cast literal in StringToNumericPrimitiveCastExecutor failed: cannot "
                             "cast '-32769' from STRING to SMALLINT") != std::string::npos);
    }
    {
        std::string src_data = "2147483648";
        auto msg = CheckLiteralInvalidResult<std::string>(cast_executor, FieldType::STRING,
                                                          src_data, arrow::int32());
        ASSERT_TRUE(msg.find("cast literal in StringToNumericPrimitiveCastExecutor failed: cannot "
                             "cast '2147483648' from STRING to INT") != std::string::npos);
    }
    {
        std::string src_data = "-2147483649";
        auto msg = CheckLiteralInvalidResult<std::string>(cast_executor, FieldType::STRING,
                                                          src_data, arrow::int32());
        ASSERT_TRUE(msg.find("cast literal in StringToNumericPrimitiveCastExecutor failed: cannot "
                             "cast '-2147483649' from STRING to INT") != std::string::npos);
    }
    {
        std::string src_data = "9223372036854775808";
        auto msg = CheckLiteralInvalidResult<std::string>(cast_executor, FieldType::STRING,
                                                          src_data, arrow::int64());
        ASSERT_TRUE(msg.find("cast literal in StringToNumericPrimitiveCastExecutor failed: cannot "
                             "cast '9223372036854775808' from STRING to BIGINT") !=
                    std::string::npos);
    }
    {
        std::string src_data = "-9223372036854775809";
        auto msg = CheckLiteralInvalidResult<std::string>(cast_executor, FieldType::STRING,
                                                          src_data, arrow::int64());
        ASSERT_TRUE(msg.find("cast literal in StringToNumericPrimitiveCastExecutor failed: cannot "
                             "cast '-9223372036854775809' from STRING to BIGINT") !=
                    std::string::npos);
    }
    {
        std::vector<std::string> src_data = {"3.4028235e+39", "-3.4028235e+39"};
        std::vector<float> target_data = {INFINITY, -INFINITY};
        CheckLiteralResult<std::string, float>(cast_executor, FieldType::STRING, src_data,
                                               arrow::float32(), target_data);
    }
    {
        std::vector<std::string> src_data = {"1.7976931348623157e+309", "-1.7976931348623157e+309"};
        std::vector<double> target_data = {INFINITY, -INFINITY};
        CheckLiteralResult<std::string, double>(cast_executor, FieldType::STRING, src_data,
                                                arrow::float64(), target_data);
    }
    // test invalid str
    {
        std::string src_data = "";
        auto msg = CheckLiteralInvalidResult<std::string>(cast_executor, FieldType::STRING,
                                                          src_data, arrow::int16());
        ASSERT_TRUE(msg.find("cast literal in StringToNumericPrimitiveCastExecutor failed: cannot "
                             "cast '' from STRING to SMALLINT") != std::string::npos);
    }
    {
        std::string src_data = "abc";
        auto msg = CheckLiteralInvalidResult<std::string>(cast_executor, FieldType::STRING,
                                                          src_data, arrow::int32());
        ASSERT_TRUE(msg.find("cast literal in StringToNumericPrimitiveCastExecutor failed: cannot "
                             "cast 'abc' from STRING to INT") != std::string::npos);
    }
}

TEST_F(CastExecutorTest, TestStringToNumericPrimitiveCastExecutorCastArray) {
    auto cast_executor = std::make_shared<StringToNumericPrimitiveCastExecutor>();
    CheckArrayResult(cast_executor, arrow::utf8(), arrow::int8(),
                     R"(["1", "0", "-1", "10", "127", "-128", null])",
                     "[1, 0, -1, 10, 127, -128, null]");
    CheckArrayResult(cast_executor, arrow::utf8(), arrow::int16(),
                     R"(["1", "0", "-1", "10", "32767", "-32768", null])",
                     "[1, 0, -1, 10, 32767, -32768, null]");
    CheckArrayResult(cast_executor, arrow::utf8(), arrow::int32(),
                     R"(["1", "0", "-1", "10", "2147483647", "-2147483648", null])",
                     "[1, 0, -1, 10, 2147483647, -2147483648, null]");
    CheckArrayResult(
        cast_executor, arrow::utf8(), arrow::int64(),
        R"(["1", "0", "-1", "10", "9223372036854775807", "-9223372036854775808", null])",
        "[1, 0, -1, 10, 9223372036854775807, -9223372036854775808, null]");
    {
        // Keyword in Java Paimon is "Infinity", "-Infinity", "NaN", while in C++ Paimon is "inf",
        // "-inf", "nan"
        auto src_array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::utf8(),
                R"(["1.0","2.2","3.3","-10.01","0","3.4028235e+38","-3.4028235e+38", "inf", "-inf", "nan", null])")
                .ValueOrDie();
        std::vector<float> expected_data = {
            1.0, 2.2, 3.3, -10.01, 0, 3.4028235e+38, -3.4028235e+38, INFINITY, -INFINITY, NAN};
        auto expected_array =
            MakeArrowArray<float, arrow::FloatBuilder>(arrow::float32(), expected_data);
        CheckArrayResult(cast_executor, arrow::float32(), src_array, expected_array);
    }
    {
        // Keyword in Java Paimon is "Infinity", "-Infinity", "NaN", while in C++ Paimon is "inf",
        // "-inf", "nan"
        auto src_array = arrow::ipc::internal::json::ArrayFromJSON(
                             arrow::utf8(),
                             R"(["1.0","2.2","3.3","-10.01","0","1.7976931348623157e+308",
                             "-1.7976931348623157e+308", "inf", "-inf", "nan", null])")
                             .ValueOrDie();
        std::vector<double> expected_data = {
            1.0,      2.2,       3.3, -10.01, 0, 1.7976931348623157e+308, -1.7976931348623157e+308,
            INFINITY, -INFINITY, NAN};
        auto expected_array =
            MakeArrowArray<double, arrow::DoubleBuilder>(arrow::float64(), expected_data);
        CheckArrayResult(cast_executor, arrow::float64(), src_array, expected_array);
    }
    // test overflow
    CheckArrayResult(cast_executor, arrow::utf8(), arrow::float32(), R"(["3.4028235e+39"])",
                     "[Inf]");
    CheckArrayResult(cast_executor, arrow::utf8(), arrow::float32(), R"(["-3.4028235e+39"])",
                     "[-Inf]");
    CheckArrayResult(cast_executor, arrow::utf8(), arrow::float64(),
                     R"(["1.7976931348623157e+309"])", "[Inf]");
    CheckArrayResult(cast_executor, arrow::utf8(), arrow::float64(),
                     R"(["-1.7976931348623157e+309"])", "[-Inf]");

    CheckArrayInvalidResult(cast_executor, arrow::utf8(), arrow::int8(), R"(["128"])");
    CheckArrayInvalidResult(cast_executor, arrow::utf8(), arrow::int8(), R"(["-129"])");
    CheckArrayInvalidResult(cast_executor, arrow::utf8(), arrow::int16(), R"(["32768"])");
    CheckArrayInvalidResult(cast_executor, arrow::utf8(), arrow::int16(), R"(["-32769"])");
    CheckArrayInvalidResult(cast_executor, arrow::utf8(), arrow::int32(), R"(["2147483648"])");
    CheckArrayInvalidResult(cast_executor, arrow::utf8(), arrow::int32(), R"(["-2147483649"])");
    CheckArrayInvalidResult(cast_executor, arrow::utf8(), arrow::int64(),
                            R"(["9223372036854775808"])");
    CheckArrayInvalidResult(cast_executor, arrow::utf8(), arrow::int64(),
                            R"(["-9223372036854775809"])");
}

TEST_F(CastExecutorTest, TestNumericToStringCastExecutorCastLiteral) {
    auto cast_executor = std::make_shared<NumericToStringCastExecutor>();
    {
        std::vector<int8_t> src_data = {1, 0, -1, 10, 127, -128};
        std::vector<std::string> target_data = {"1", "0", "-1", "10", "127", "-128"};
        CheckLiteralResult<int8_t, std::string>(cast_executor, FieldType::TINYINT, src_data,
                                                arrow::utf8(), target_data);
    }
    {
        std::vector<int16_t> src_data = {1, 0, -1, 10, 32767, -32768};
        std::vector<std::string> target_data = {"1", "0", "-1", "10", "32767", "-32768"};
        CheckLiteralResult<int16_t, std::string>(cast_executor, FieldType::SMALLINT, src_data,
                                                 arrow::utf8(), target_data);
    }
    {
        std::vector<int32_t> src_data = {1, 0, -1, 10, 2147483647, -2147483648};
        std::vector<std::string> target_data = {"1", "0", "-1", "10", "2147483647", "-2147483648"};
        CheckLiteralResult<int32_t, std::string>(cast_executor, FieldType::INT, src_data,
                                                 arrow::utf8(), target_data);
    }
    {
        std::vector<int64_t> src_data = {1, 0, -1, 10, MAX_INT64, MIN_INT64};
        std::vector<std::string> target_data = {
            "1", "0", "-1", "10", "9223372036854775807", "-9223372036854775808"};
        CheckLiteralResult<int64_t, std::string>(cast_executor, FieldType::BIGINT, src_data,
                                                 arrow::utf8(), target_data);
    }
    {
        std::vector<float> src_data = {1,         2.2,       3.3,      -10.01,    0,
                                       MAX_FLOAT, MIN_FLOAT, INFINITY, -INFINITY, NAN};
        std::vector<std::string> target_data = {
            "1",   "2.2",  "3.3", "-10.01", "0", "3.4028235e+38", "-3.4028235e+38",
            "inf", "-inf", "nan"};
        CheckLiteralResult<float, std::string>(cast_executor, FieldType::FLOAT, src_data,
                                               arrow::utf8(), target_data);
    }
    {
        // Java Paimon cast 1.0 to 1.0, while C++ cast to 1
        std::vector<double> src_data = {1.0,        2.2,        3.3,      -10.01,    0,
                                        MAX_DOUBLE, MIN_DOUBLE, INFINITY, -INFINITY, NAN};
        std::vector<std::string> target_data = {"1",
                                                "2.2",
                                                "3.3",
                                                "-10.01",
                                                "0",
                                                "1.7976931348623157e+308",
                                                "-1.7976931348623157e+308",
                                                "inf",
                                                "-inf",
                                                "nan"};

        CheckLiteralResult<double, std::string>(cast_executor, FieldType::DOUBLE, src_data,
                                                arrow::utf8(), target_data);
    }
    {
        std::vector<Decimal> src_data = {
            Decimal(5, 2, DecimalUtils::StrToInt128("000").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("110").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("12345").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("12346").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("99999").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-12345").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-12346").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-99999").value())};
        std::vector<std::string> target_data = {"0.00",   "1.10",    "123.45",  "123.46",
                                                "999.99", "-123.45", "-123.46", "-999.99"};
        CheckLiteralResult<Decimal, std::string>(cast_executor, FieldType::DECIMAL, src_data,
                                                 arrow::utf8(), target_data);
    }
}

TEST_F(CastExecutorTest, TestNumericToStringCastExecutorCastArray) {
    auto cast_executor = std::make_shared<NumericToStringCastExecutor>();
    CheckArrayResult(cast_executor, arrow::int8(), arrow::utf8(), "[1, 0, -1, 10, 127, -128, null]",
                     R"(["1", "0", "-1", "10", "127", "-128", null])");
    CheckArrayResult(cast_executor, arrow::int16(), arrow::utf8(),
                     "[1, 0, -1, 10, 32767, -32768, null]",
                     R"(["1", "0", "-1", "10", "32767", "-32768", null])");
    CheckArrayResult(cast_executor, arrow::int32(), arrow::utf8(),
                     "[1, 0, -1, 10, 2147483647, -2147483648, null]",
                     R"(["1", "0", "-1", "10", "2147483647", "-2147483648", null])");
    CheckArrayResult(
        cast_executor, arrow::int64(), arrow::utf8(),
        "[1, 0, -1, 10, 9223372036854775807, -9223372036854775808, null]",
        R"(["1", "0", "-1", "10", "9223372036854775807", "-9223372036854775808", null])");
    {
        std::vector<float> src_data = {1.0,       2.2,       3.3,      -10.01,    0,
                                       MAX_FLOAT, MIN_FLOAT, INFINITY, -INFINITY, NAN};
        auto src_array = MakeArrowArray<float, arrow::FloatBuilder>(arrow::float32(), src_data);
        auto expected_array = arrow::ipc::internal::json::ArrayFromJSON(
                                  arrow::utf8(),
                                  R"(["1","2.2","3.3","-10.01","0","3.4028235e+38",
                                             "-3.4028235e+38", "inf", "-inf", "nan", null])")
                                  .ValueOrDie();
        CheckArrayResult(cast_executor, arrow::utf8(), src_array, expected_array);
    }
    {
        // Java Paimon cast 1.0 to 1.0, while C++ cast to 1
        std::vector<double> src_data = {1.0,        2.2,        3.3,      -10.01,    0,
                                        MAX_DOUBLE, MIN_DOUBLE, INFINITY, -INFINITY, NAN};
        auto src_array = MakeArrowArray<double, arrow::DoubleBuilder>(arrow::float64(), src_data);
        auto expected_array = arrow::ipc::internal::json::ArrayFromJSON(
                                  arrow::utf8(),
                                  R"(["1","2.2","3.3","-10.01","0","1.7976931348623157e+308",
                                             "-1.7976931348623157e+308", "inf", "-inf", "nan", null])")
                                  .ValueOrDie();
        CheckArrayResult(cast_executor, arrow::utf8(), src_array, expected_array);
    }
    CheckArrayResult(
        cast_executor, arrow::decimal128(5, 2), arrow::utf8(),
        R"(["0.00", "1.10", "123.45", "123.46", "999.99", "-123.45", "-123.46", "-999.99", null])",
        R"(["0.00", "1.10", "123.45", "123.46", "999.99", "-123.45", "-123.46", "-999.99", null])");
}

TEST_F(CastExecutorTest, TestStringToBinaryCastExecutorCastLiteral) {
    auto cast_executor = std::make_shared<StringToBinaryCastExecutor>();
    std::string invalid_utf8_str =
        "["
        R"(
                       "Hi",
                       "olá mundo",
                       "你好世界",
                       "",
                       )"
        "\"\xa0\xa1\""
        "]";

    std::vector<std::string> src_data = {"foo", "bar", "", "这是中文字符", invalid_utf8_str};
    CheckLiteralResult<std::string, std::string>(cast_executor, FieldType::STRING, src_data,
                                                 arrow::binary(), src_data);
}

TEST_F(CastExecutorTest, TestStringToBinaryCastExecutorCastArray) {
    auto cast_executor = std::make_shared<StringToBinaryCastExecutor>();
    CheckArrayResult(cast_executor, arrow::utf8(), arrow::binary(),
                     R"(["foo", "bar", "", "这首歌送给你", null])",
                     R"(["foo", "bar", "", "这首歌送给你", null])");

    {
        // invalid utf-8 is not an error for binary
        std::string invalid_utf8_str =
            "["
            R"(
                       "Hi",
                       "olá mundo",
                       "你好世界",
                       "",
                       )"
            "\"\xa0\xa1\""
            "]";
        CheckArrayResult(cast_executor, arrow::utf8(), arrow::binary(), invalid_utf8_str,
                         invalid_utf8_str);
    }
}

TEST_F(CastExecutorTest, TestBinaryToStringCastExecutorCastLiteral) {
    auto cast_executor = std::make_shared<BinaryToStringCastExecutor>();
    std::string invalid_utf8_str =
        "["
        R"(
                       "Hi",
                       "olá mundo",
                       "你好世界",
                       "",
                       )"
        "\"\xa0\xa1\""
        "]";

    std::vector<std::string> src_data = {"foo", "bar", "", "这是中文字符", invalid_utf8_str};
    CheckLiteralResult<std::string, std::string>(cast_executor, FieldType::BINARY, src_data,
                                                 arrow::utf8(), src_data);
}

TEST_F(CastExecutorTest, TestBinaryToStringCastExecutorCastArray) {
    auto cast_executor = std::make_shared<BinaryToStringCastExecutor>();
    CheckArrayResult(cast_executor, arrow::binary(), arrow::utf8(),
                     R"(["foo", "bar", "", "这首歌送给你", null])",
                     R"(["foo", "bar", "", "这首歌送给你", null])");

    {
        // invalid utf-8 is not an error for string
        std::string invalid_utf8_str =
            "["
            R"(
                       "Hi",
                       "olá mundo",
                       "你好世界",
                       "",
                       )"
            "\"\xa0\xa1\""
            "]";
        CheckArrayResult(cast_executor, arrow::binary(), arrow::utf8(), invalid_utf8_str,
                         invalid_utf8_str);
    }
}

TEST_F(CastExecutorTest, TestDateToStringCastExecutorCastLiteral) {
    auto cast_executor = std::make_shared<DateToStringCastExecutor>();
    // date values ranging from 0000-01-01 to 9999-12-31
    std::vector<int32_t> src_data = {1, 0, -1, 19516, 2932896, -719528};
    std::vector<std::string> target_data = {"1970-01-02", "1970-01-01", "1969-12-31",
                                            "2023-06-08", "9999-12-31", "0000-01-01"};
    CheckLiteralResult<int32_t, std::string>(cast_executor, FieldType::DATE, src_data,
                                             arrow::utf8(), target_data);
}

TEST_F(CastExecutorTest, TestDateToStringCastExecutorCastArray) {
    auto cast_executor = std::make_shared<DateToStringCastExecutor>();
    CheckArrayResult(cast_executor, arrow::date32(), arrow::utf8(),
                     R"([1, 0, -1, 19516, 2932896, -719528, null])",
                     R"(["1970-01-02", "1970-01-01", "1969-12-31",
                         "2023-06-08", "9999-12-31", "0000-01-01", null])");
}

TEST_F(CastExecutorTest, TestDateToTimestampCastExecutorCastArray) {
    auto cast_executor = std::make_shared<DateToTimestampCastExecutor>();
    {
        auto target_type = arrow::timestamp(arrow::TimeUnit::NANO);
        auto src_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::date32(), R"([1, 0, -1, 19516, null])")
                .ValueOrDie();
        std::vector<int64_t> target_data = {
            1l * DateTimeUtils::MILLIS_PER_DAY * NANOS_PER_MILLIS,
            0l * DateTimeUtils::MILLIS_PER_DAY * NANOS_PER_MILLIS,
            -1l * DateTimeUtils::MILLIS_PER_DAY * NANOS_PER_MILLIS,
            19516l * DateTimeUtils::MILLIS_PER_DAY * NANOS_PER_MILLIS,
        };
        std::shared_ptr<arrow::Array> target_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(target_type, target_data);
        // Java Paimon supports date values ranging from 0000-01-01 to 9999-12-31, while C++ Paimon
        // only supports from (MIN_INT64/NANOS_PER_DAY) to (MAX_INT64/NANOS_PER_DAY)
        // TODO(liancheng.lsz): support date values ranging from 0000-01-01 to 9999-12-31
        CheckArrayResult(cast_executor, target_type, src_array, target_array);
    }
    {
        auto target_type = arrow::timestamp(arrow::TimeUnit::MILLI, "Asia/Shanghai");
        auto src_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::date32(), R"([1, 0, -1, 19516, null])")
                .ValueOrDie();
        std::vector<int64_t> target_data = {
            57600000,
            -28800000,
            -115200000,
            1686153600000,
        };
        std::shared_ptr<arrow::Array> target_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(target_type, target_data);
        CheckArrayResult(cast_executor, target_type, src_array, target_array);
    }
    {
        auto target_type = arrow::timestamp(arrow::TimeUnit::NANO);
        // now not support date time at 0000-01-01 and 9999-12-31
        auto msg = CheckArrayInvalidResult(cast_executor, arrow::date32(), target_type,
                                           R"([2932896, -719528])");
        ASSERT_TRUE(msg.find("Casting from date32[day] to timestamp[ns] would result in out of "
                             "bounds timestamp: 2932896") != std::string::npos);
    }
}

TEST_F(CastExecutorTest, TestNumericPrimitiveToTimestampCastExecutorCastArray) {
    TimezoneGuard tz_guard("Asia/Shanghai");
    auto cast_executor = std::make_shared<NumericPrimitiveToTimestampCastExecutor>();
    {
        auto target_type = arrow::timestamp(arrow::TimeUnit::NANO);
        {
            auto src_array = arrow::ipc::internal::json::ArrayFromJSON(
                                 arrow::int32(), R"([1, 0, -1, 2147483647, -2147483648, null])")
                                 .ValueOrDie();
            std::vector<int64_t> target_data = {
                28801l * NANOS_PER_SECOND,       28800l * NANOS_PER_SECOND,
                28799l * NANOS_PER_SECOND,       2147512447l * NANOS_PER_SECOND,
                -2147454848l * NANOS_PER_SECOND,
            };
            std::shared_ptr<arrow::Array> target_array =
                MakeArrowArray<int64_t, arrow::TimestampBuilder>(target_type, target_data);
            CheckArrayResult(cast_executor, target_type, src_array, target_array);
        }
        {
            auto src_array =
                arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), R"([1, 0, -1, null])")
                    .ValueOrDie();
            std::vector<int64_t> target_data = {
                28801l * NANOS_PER_SECOND, 28800l * NANOS_PER_SECOND, 28799l * NANOS_PER_SECOND};
            std::shared_ptr<arrow::Array> target_array =
                MakeArrowArray<int64_t, arrow::TimestampBuilder>(target_type, target_data);
            CheckArrayResult(cast_executor, target_type, src_array, target_array);
        }
    }
    {
        auto target_type = arrow::timestamp(arrow::TimeUnit::NANO, "Asia/Shanghai");
        {
            auto src_array = arrow::ipc::internal::json::ArrayFromJSON(
                                 arrow::int32(), R"([1, 0, -1, 2147483647, -2147483648, null])")
                                 .ValueOrDie();
            std::vector<int64_t> target_data = {
                1l * NANOS_PER_SECOND,           0l * NANOS_PER_SECOND,
                -1l * NANOS_PER_SECOND,          2147483647l * NANOS_PER_SECOND,
                -2147483648l * NANOS_PER_SECOND,
            };
            std::shared_ptr<arrow::Array> target_array =
                MakeArrowArray<int64_t, arrow::TimestampBuilder>(target_type, target_data);
            CheckArrayResult(cast_executor, target_type, src_array, target_array);
        }
        {
            auto src_array =
                arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), R"([1, 0, -1, null])")
                    .ValueOrDie();
            std::vector<int64_t> target_data = {1l * NANOS_PER_SECOND, 0l * NANOS_PER_SECOND,
                                                -1l * NANOS_PER_SECOND};
            std::shared_ptr<arrow::Array> target_array =
                MakeArrowArray<int64_t, arrow::TimestampBuilder>(target_type, target_data);
            CheckArrayResult(cast_executor, target_type, src_array, target_array);
        }
    }
}

TEST_F(CastExecutorTest, TestStringToDateCastExecutorCastLiteral) {
    auto cast_executor = std::make_shared<StringToDateCastExecutor>();
    // date values ranging from 0000-01-01 to 9999-12-31
    {
        std::vector<std::string> src_data = {"1",          "2147483647", "-2147483648",
                                             "1970-01-02", "1970-01-01", "1969-12-31",
                                             "2023-06-08", "9999-12-31", "0000-01-01"};
        std::vector<int32_t> target_data = {1,  2147483647, -2147483648, 1,      0,
                                            -1, 19516,      2932896,     -719528};
        CheckLiteralResult<std::string, int32_t>(cast_executor, FieldType::STRING, src_data,
                                                 arrow::date32(), target_data);
    }
    {
        // invalid value, overflow int32
        std::string src_data = "9223372036854775807";
        auto msg = CheckLiteralInvalidResult<std::string>(cast_executor, FieldType::STRING,
                                                          src_data, arrow::date32());
        ASSERT_TRUE(msg.find("failed to convert string '9223372036854775807' to date") !=
                    std::string::npos);
    }
    {
        // invalid date str
        std::string src_data = "11970-01-02";
        auto msg = CheckLiteralInvalidResult<std::string>(cast_executor, FieldType::STRING,
                                                          src_data, arrow::date32());
        ASSERT_TRUE(msg.find("failed to convert string '11970-01-02' to date") !=
                    std::string::npos);
    }
    {
        // invalid date str
        std::string src_data = "-1970-01-02";
        auto msg = CheckLiteralInvalidResult<std::string>(cast_executor, FieldType::STRING,
                                                          src_data, arrow::date32());
        ASSERT_TRUE(msg.find("failed to convert string '-1970-01-02' to date") !=
                    std::string::npos);
    }
    {
        // invalid date str
        std::string src_data = "";
        auto msg = CheckLiteralInvalidResult<std::string>(cast_executor, FieldType::STRING,
                                                          src_data, arrow::date32());
        ASSERT_TRUE(msg.find("failed to convert string '' to date") != std::string::npos);
    }
    {
        // invalid date str
        std::string src_data = "0x1";
        auto msg = CheckLiteralInvalidResult<std::string>(cast_executor, FieldType::STRING,
                                                          src_data, arrow::date32());
        ASSERT_TRUE(msg.find("failed to convert string '0x1' to date") != std::string::npos);
    }
}

TEST_F(CastExecutorTest, TestStringToDateCastExecutorCastArray) {
    auto cast_executor = std::make_shared<StringToDateCastExecutor>();
    CheckArrayResult(cast_executor, arrow::utf8(), arrow::date32(),
                     R"(["1", "2147483647", "-2147483648", "1970-01-02", "1970-01-01", "1969-12-31",
                         "2023-06-08", "9999-12-31", "0000-01-01", null])",
                     R"([1, 2147483647, -2147483648, 1, 0, -1, 19516, 2932896, -719528, null])");
    // invalid string refer to TestStringToDateCastExecutorCastLiteral
}

TEST_F(CastExecutorTest, TestTimestampToStringCastExecutorCastArray) {
    auto cast_executor = std::make_shared<TimestampToStringCastExecutor>();
    {
        // timestamp ranges from MIN_INT64 ns to MAX_INT64 ns in arrow timestamp array
        auto src_type = arrow::timestamp(arrow::TimeUnit::NANO);
        std::vector<int64_t> src_data = {
            1l * DateTimeUtils::MILLIS_PER_DAY * NANOS_PER_MILLIS + 1,
            0l * DateTimeUtils::MILLIS_PER_DAY * NANOS_PER_MILLIS + 500,
            -1l * DateTimeUtils::MILLIS_PER_DAY * NANOS_PER_MILLIS + 500,
            19516l * DateTimeUtils::MILLIS_PER_DAY * NANOS_PER_MILLIS,
            MIN_INT64,
            MAX_INT64};
        std::shared_ptr<arrow::Array> src_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(src_type, src_data);

        auto target_array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::utf8(),
                R"(["1970-01-02 00:00:00.000000001", "1970-01-01 00:00:00.000000500",
                    "1969-12-31 00:00:00.000000500", "2023-06-08 00:00:00.000000000",
                    "1677-09-21 00:12:43.145224192", "2262-04-11 23:47:16.854775807", null])")
                .ValueOrDie();
        CheckArrayResult(cast_executor, arrow::utf8(), src_array, target_array);
    }
    {
        auto src_type = arrow::timestamp(arrow::TimeUnit::MICRO);
        std::vector<int64_t> src_data = {0, -1, 1};
        std::shared_ptr<arrow::Array> src_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(src_type, src_data);

        auto target_array = arrow::ipc::internal::json::ArrayFromJSON(
                                arrow::utf8(),
                                R"(["1970-01-01 00:00:00.000000", "1969-12-31 23:59:59.999999",
                    "1970-01-01 00:00:00.000001", null])")
                                .ValueOrDie();
        CheckArrayResult(cast_executor, arrow::utf8(), src_array, target_array);
    }
    {
        auto src_type = arrow::timestamp(arrow::TimeUnit::MILLI);
        std::vector<int64_t> src_data = {0, -1, 1};
        std::shared_ptr<arrow::Array> src_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(src_type, src_data);

        auto target_array = arrow::ipc::internal::json::ArrayFromJSON(
                                arrow::utf8(),
                                R"(["1970-01-01 00:00:00.000", "1969-12-31 23:59:59.999",
                    "1970-01-01 00:00:00.001", null])")
                                .ValueOrDie();
        CheckArrayResult(cast_executor, arrow::utf8(), src_array, target_array);
    }
    {
        auto src_type = arrow::timestamp(arrow::TimeUnit::SECOND);
        std::vector<int64_t> src_data = {0, -1, 1};
        std::shared_ptr<arrow::Array> src_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(src_type, src_data);

        auto target_array = arrow::ipc::internal::json::ArrayFromJSON(
                                arrow::utf8(),
                                R"(["1970-01-01 00:00:00", "1969-12-31 23:59:59",
                    "1970-01-01 00:00:01", null])")
                                .ValueOrDie();
        CheckArrayResult(cast_executor, arrow::utf8(), src_array, target_array);
    }
    {
        auto src_type = arrow::timestamp(arrow::TimeUnit::SECOND, "Asia/Shanghai");
        std::vector<int64_t> src_data = {0, -1, 1};
        std::shared_ptr<arrow::Array> src_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(src_type, src_data);

        auto target_array = arrow::ipc::internal::json::ArrayFromJSON(
                                arrow::utf8(),
                                R"(["1970-01-01 08:00:00", "1970-01-01 07:59:59",
                    "1970-01-01 08:00:01", null])")
                                .ValueOrDie();
        CheckArrayResult(cast_executor, arrow::utf8(), src_array, target_array);
    }
    {
        auto src_type = arrow::timestamp(arrow::TimeUnit::NANO, "Asia/Shanghai");
        std::vector<int64_t> src_data = {0, -1, 1};
        std::shared_ptr<arrow::Array> src_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(src_type, src_data);

        auto target_array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::utf8(),
                R"(["1970-01-01 08:00:00.000000000", "1970-01-01 07:59:59.999999999",
                    "1970-01-01 08:00:00.000000001", null])")
                .ValueOrDie();
        CheckArrayResult(cast_executor, arrow::utf8(), src_array, target_array);
    }
}

TEST_F(CastExecutorTest, TestTimestampToDateCastExecutorCastArray) {
    auto cast_executor = std::make_shared<TimestampToDateCastExecutor>();
    {
        // timestamp ranges from MIN_INT64 ns to MAX_INT64 ns in arrow timestamp array
        auto src_type = arrow::timestamp(arrow::TimeUnit::NANO);
        std::vector<int64_t> src_data = {
            1l * DateTimeUtils::MILLIS_PER_DAY * NANOS_PER_MILLIS + 1,
            0l * DateTimeUtils::MILLIS_PER_DAY * NANOS_PER_MILLIS + 57600000 * NANOS_PER_MILLIS,
            -1l * DateTimeUtils::MILLIS_PER_DAY * NANOS_PER_MILLIS + 500,
            19516l * DateTimeUtils::MILLIS_PER_DAY * NANOS_PER_MILLIS,
            MIN_INT64,
            MAX_INT64};
        std::shared_ptr<arrow::Array> src_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(src_type, src_data);

        auto target_array = arrow::ipc::internal::json::ArrayFromJSON(
                                arrow::date32(), R"([1, 0, -1, 19516, -106752, 106751, null])")
                                .ValueOrDie();
        CheckArrayResult(cast_executor, arrow::date32(), src_array, target_array);
    }
    {
        auto src_type = arrow::timestamp(arrow::TimeUnit::MILLI, "Asia/Shanghai");
        std::vector<int64_t> src_data = {
            1l * DateTimeUtils::MILLIS_PER_DAY,
            0l * DateTimeUtils::MILLIS_PER_DAY + 57600000,
            -1l * DateTimeUtils::MILLIS_PER_DAY,
        };
        std::shared_ptr<arrow::Array> src_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(src_type, src_data);

        auto target_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::date32(), R"([1, 1, -1, null])")
                .ValueOrDie();
        CheckArrayResult(cast_executor, arrow::date32(), src_array, target_array);
    }
}

TEST_F(CastExecutorTest, TestTimestampToNumericPrimitiveCastExecutorCastArray) {
    TimezoneGuard tz_guard("Asia/Shanghai");
    auto cast_executor = std::make_shared<TimestampToNumericPrimitiveCastExecutor>();
    {
        auto src_type = arrow::timestamp(arrow::TimeUnit::MILLI);
        std::vector<int64_t> src_data = {
            1l * DateTimeUtils::MILLIS_PER_DAY, 0l * DateTimeUtils::MILLIS_PER_DAY,
            -1l * DateTimeUtils::MILLIS_PER_DAY, 19516l * DateTimeUtils::MILLIS_PER_DAY};
        std::shared_ptr<arrow::Array> src_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(src_type, src_data);
        {
            auto target_array = arrow::ipc::internal::json::ArrayFromJSON(
                                    arrow::int32(), R"([57600, -28800, -115200, 1686153600, null])")
                                    .ValueOrDie();
            CheckArrayResult(cast_executor, arrow::int32(), src_array, target_array);
        }
        {
            auto target_array = arrow::ipc::internal::json::ArrayFromJSON(
                                    arrow::int64(), R"([57600, -28800, -115200, 1686153600, null])")
                                    .ValueOrDie();
            CheckArrayResult(cast_executor, arrow::int64(), src_array, target_array);
        }
    }
    {
        auto src_type = arrow::timestamp(arrow::TimeUnit::MILLI, "Asia/Shanghai");
        std::vector<int64_t> src_data = {
            1l * DateTimeUtils::MILLIS_PER_DAY, 0l * DateTimeUtils::MILLIS_PER_DAY,
            -1l * DateTimeUtils::MILLIS_PER_DAY, 19516l * DateTimeUtils::MILLIS_PER_DAY};
        std::shared_ptr<arrow::Array> src_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(src_type, src_data);
        {
            auto target_array = arrow::ipc::internal::json::ArrayFromJSON(
                                    arrow::int32(), R"([86400, 0, -86400, 1686182400, null])")
                                    .ValueOrDie();
            CheckArrayResult(cast_executor, arrow::int32(), src_array, target_array);
        }
        {
            auto target_array = arrow::ipc::internal::json::ArrayFromJSON(
                                    arrow::int64(), R"([86400, 0, -86400, 1686182400, null])")
                                    .ValueOrDie();
            CheckArrayResult(cast_executor, arrow::int64(), src_array, target_array);
        }
    }
}

TEST_F(CastExecutorTest, TestStringToTimestampCastExecutorCastArray) {
    auto cast_executor = std::make_shared<StringToTimestampCastExecutor>();
    {
        // timestamp ranges from MIN_INT64 ns to MAX_INT64 ns in arrow timestamp array
        auto target_type = arrow::timestamp(arrow::TimeUnit::NANO);
        auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(),
                                                                   R"([
                    "2024-11-21", "2024-11-21 09:41:56", "2024-11-21 09:41:56.80160",
                    "1970-01-02 00:00:00.000000001", "1970-01-01 00:00:00.000000500",
                    "1969-12-31 00:00:00.000000500", "2023-06-08 00:00:00.000000000",
                    "1677-09-21 00:12:43.145224192", "2262-04-11 23:47:16.854775807", null])")
                             .ValueOrDie();

        std::vector<int64_t> target_data = {
            1732147200000000000l,
            1732182116000000000l,
            1732182116801600000l,
            1l * DateTimeUtils::MILLIS_PER_DAY * NANOS_PER_MILLIS + 1,
            0l * DateTimeUtils::MILLIS_PER_DAY * NANOS_PER_MILLIS + 500,
            -1l * DateTimeUtils::MILLIS_PER_DAY * NANOS_PER_MILLIS + 500,
            19516l * DateTimeUtils::MILLIS_PER_DAY * NANOS_PER_MILLIS,
            MIN_INT64,
            MAX_INT64};
        std::shared_ptr<arrow::Array> target_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(target_type, target_data);

        CheckArrayResult(cast_executor, target_type, src_array, target_array);
    }
    {
        auto target_type = arrow::timestamp(arrow::TimeUnit::MICRO, "Asia/Shanghai");
        auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(),
                                                                   R"([
        "1970-01-01 00:00:00.000000", "1969-12-31 23:59:59.999999",
        "1970-01-01 00:00:00.000001", null])")
                             .ValueOrDie();

        std::vector<int64_t> target_data = {-28800000000l, -28800000001l, -28799999999l};
        std::shared_ptr<arrow::Array> target_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(target_type, target_data);

        CheckArrayResult(cast_executor, target_type, src_array, target_array);
    }
    {
        auto target_type = arrow::timestamp(arrow::TimeUnit::MILLI);
        auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(),
                                                                   R"([
        "1970-01-01 00:00:00.000", "1969-12-31 23:59:59.999",
        "1970-01-01 00:00:00.001", null])")
                             .ValueOrDie();

        std::vector<int64_t> target_data = {0, -1, 1};
        std::shared_ptr<arrow::Array> target_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(target_type, target_data);

        CheckArrayResult(cast_executor, target_type, src_array, target_array);
    }
    {
        auto target_type = arrow::timestamp(arrow::TimeUnit::SECOND, "Asia/Shanghai");
        auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(),
                                                                   R"([
        "1970-01-01T00:00:00", "1969-12-31T23:59:59",
        "1970-01-01T00:00:01", null])")
                             .ValueOrDie();

        std::vector<int64_t> target_data = {-28800, -28801, -28799};
        std::shared_ptr<arrow::Array> target_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(target_type, target_data);

        CheckArrayResult(cast_executor, target_type, src_array, target_array);
    }
}

TEST_F(CastExecutorTest, TestDecimalToNumericPrimitiveCastExecutorCastLiteral) {
    auto cast_executor = std::make_shared<DecimalToNumericPrimitiveCastExecutor>();
    {
        // decimal to int8
        std::vector<Decimal> src_data = {
            Decimal(38, 5, DecimalUtils::StrToInt128("210000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-1110000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("2260000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-12150000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("000000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-12800000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("12700000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("12800000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-12900000").value()),
        };

        std::vector<int8_t> target_data = {2, -11, 22, -121, 0, -128, 127, -128, 127};
        CheckLiteralResult<Decimal, int8_t>(cast_executor, FieldType::DECIMAL, src_data,
                                            arrow::int8(), target_data);
    }
    {
        // decimal to int16
        std::vector<Decimal> src_data = {
            Decimal(38, 5, DecimalUtils::StrToInt128("0210000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-1110000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("2260000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-12150000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-000000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-3276800000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("3276700000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("3276800000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-3276900000").value())};

        std::vector<int16_t> target_data = {2, -11, 22, -121, 0, -32768, 32767, -32768, 32767};
        CheckLiteralResult<Decimal, int16_t>(cast_executor, FieldType::DECIMAL, src_data,
                                             arrow::int16(), target_data);
    }
    {
        // decimal to int32
        std::vector<Decimal> src_data = {
            Decimal(38, 5, DecimalUtils::StrToInt128("0210000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-1110000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("2260000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-12150000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-000000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-214748364800000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("214748364700000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("214748364800000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-214748364900000").value())};
        std::vector<int32_t> target_data = {2,           -11,        22,          -121,      0,
                                            -2147483648, 2147483647, -2147483648, 2147483647};
        CheckLiteralResult<Decimal, int32_t>(cast_executor, FieldType::DECIMAL, src_data,
                                             arrow::int32(), target_data);
    }
    {
        // decimal to int64
        std::vector<Decimal> src_data = {
            Decimal(38, 5, DecimalUtils::StrToInt128("0210000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-1110000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("2260000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-12150000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-000000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-922337203685477580800000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("922337203685477580700000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("922337203685477580800000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-922337203685477580900000").value())};
        std::vector<int64_t> target_data = {2,         -11,       22,        -121,     0,
                                            MIN_INT64, MAX_INT64, MIN_INT64, MAX_INT64};
        CheckLiteralResult<Decimal, int64_t>(cast_executor, FieldType::DECIMAL, src_data,
                                             arrow::int64(), target_data);
    }
    {
        // decimal to float
        std::vector<Decimal> src_data = {
            Decimal(38, 5, DecimalUtils::StrToInt128("0210000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-1110000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("2260000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-12150000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("000000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("123456").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-123456").value()),
            Decimal(38, 5,
                    DecimalUtils::StrToInt128("11111111111111111111111111111111111111").value()),
            Decimal(38, 5,
                    DecimalUtils::StrToInt128("-11111111111111111111111111111111111111").value()),
            Decimal(38, 5,
                    DecimalUtils::StrToInt128("99999999999999999999999999999999999999").value()),
            Decimal(38, 5,
                    DecimalUtils::StrToInt128("-99999999999999999999999999999999999999").value())};
        std::vector<float> target_data = {2.1,           -11.1f,  22.6,     -121.5,
                                          0.0,           1.23456, -1.23456, 1.1111111E32,
                                          -1.1111111E32, 1.0E33,  -1.0E33};
        CheckLiteralResult<Decimal, float>(cast_executor, FieldType::DECIMAL, src_data,
                                           arrow::float32(), target_data);
    }
    {
        // decimal to double
        std::vector<Decimal> src_data = {
            Decimal(38, 5, DecimalUtils::StrToInt128("0210000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-1110000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("2260000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-12150000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("000000").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("123456").value()),
            Decimal(38, 5, DecimalUtils::StrToInt128("-123456").value()),
            Decimal(38, 5,
                    DecimalUtils::StrToInt128("11111111111111111111111111111111111111").value()),
            Decimal(38, 5,
                    DecimalUtils::StrToInt128("-11111111111111111111111111111111111111").value()),
            Decimal(38, 5,
                    DecimalUtils::StrToInt128("99999999999999999999999999999999999999").value()),
            Decimal(38, 5,
                    DecimalUtils::StrToInt128("-99999999999999999999999999999999999999").value())};
        std::vector<double> target_data = {2.1,
                                           -11.1,
                                           22.6,
                                           -121.5,
                                           0.0,
                                           1.23456,
                                           -1.23456,
                                           1.1111111111111112E32,
                                           -1.1111111111111112E32,
                                           1.0E33,
                                           -1.0E33};
        CheckLiteralResult<Decimal, double>(cast_executor, FieldType::DECIMAL, src_data,
                                            arrow::float64(), target_data);
    }
}

TEST_F(CastExecutorTest, TestDecimalToNumericPrimitiveCastExecutorCastArray) {
    auto cast_executor = std::make_shared<DecimalToNumericPrimitiveCastExecutor>();
    // to integer
    CheckArrayResult(
        cast_executor, arrow::decimal128(38, 5), arrow::int8(),
        R"(["02.10000","-11.10000", "22.60000", "-121.50000", "0.00000", "-128.00000", "127.00000", "128.00000", "-129.00000", null])",
        "[2, -11, 22, -121, 0, -128, 127, -128, 127, null]");
    CheckArrayResult(
        cast_executor, arrow::decimal128(38, 5), arrow::int16(),
        R"(["02.10000","-11.10000", "22.60000", "-121.50000", "-0.00000", "-32768.00000", "32767.00000", "32768.00000", "-32769.00000", null])",
        "[2, -11, 22, -121, 0, -32768, 32767, -32768, 32767, null]");
    CheckArrayResult(
        cast_executor, arrow::decimal128(38, 5), arrow::int32(),
        R"(["02.10000","-11.10000", "22.60000", "-121.50000", "-0.00000", "-2147483648.00000", "2147483647.00000", "2147483648.00000", "-2147483649.00000", null])",
        "[2, -11, 22, -121, 0, -2147483648, 2147483647, -2147483648, 2147483647, null]");
    CheckArrayResult(
        cast_executor, arrow::decimal128(38, 5), arrow::int32(),
        R"(["02.10000","-11.10000", "22.60000", "-121.50000", "-0.00000", "-9223372036854775808.00000", "9223372036854775807.00000", "9223372036854775808.00000", "-9223372036854775809.00000", null])",
        "[2, -11, 22, -121, 0, 0, -1, 0, -1, null]");
    CheckArrayResult(
        cast_executor, arrow::decimal128(38, 5), arrow::int64(),
        R"(["02.10000","-11.10000", "22.60000", "-121.50000", "-0.00000", "-9223372036854775808.00000", "9223372036854775807.00000", "9223372036854775808.00000", "-9223372036854775809.00000", null])",
        "[2, -11, 22, -121, 0, -9223372036854775808, 9223372036854775807, -9223372036854775808, "
        "9223372036854775807, null]");

    // to floating
    {
        auto src_array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::decimal128(38, 5),
                R"(["02.10000","-11.10000", "22.60000", "-121.50000", "0.00000", "1.23456", "-1.23456",
                "111111111111111111111111111111111.11111", "-111111111111111111111111111111111.11111",
                "999999999999999999999999999999999.99999", "-999999999999999999999999999999999.99999", null])")
                .ValueOrDie();

        std::vector<float> target_data = {2.1,           -11.1,   22.6,     -121.5,
                                          0.0,           1.23456, -1.23456, 1.1111111E32,
                                          -1.1111111E32, 1.0E33,  -1.0E33};
        auto target_array =
            MakeArrowArray<float, arrow::FloatBuilder>(arrow::float32(), target_data);
        CheckArrayApproxResult(cast_executor, arrow::float32(), src_array, target_array);
    }
    {
        auto src_array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::decimal128(38, 5),
                R"(["02.10000","-11.10000", "22.60000", "-121.50000", "0.00000", "1.23456", "-1.23456",
                "111111111111111111111111111111111.11111", "-111111111111111111111111111111111.11111",
                "999999999999999999999999999999999.99999", "-999999999999999999999999999999999.99999", null])")
                .ValueOrDie();

        std::vector<double> target_data = {2.1,
                                           -11.1,
                                           22.6,
                                           -121.5,
                                           0.0,
                                           1.23456,
                                           -1.23456,
                                           1.1111111111111112E32,
                                           -1.1111111111111112E32,
                                           1.0E33,
                                           -1.0E33};
        auto target_array =
            MakeArrowArray<double, arrow::DoubleBuilder>(arrow::float64(), target_data);
        CheckArrayApproxResult(cast_executor, arrow::float64(), src_array, target_array);
    }
}

TEST_F(CastExecutorTest, TestNumericPrimitiveToDecimalCastExecutorCastLiteral) {
    auto cast_executor = std::make_shared<NumericPrimitiveToDecimalCastExecutor>();
    {
        // int8 to decimal
        std::vector<int8_t> src_data = {0, 2, -11, 22, -121, -128, 127};
        std::vector<Decimal> target_data = {
            Decimal(5, 2, DecimalUtils::StrToInt128("000").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("200").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-1100").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("2200").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-12100").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-12800").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("12700").value())};
        CheckLiteralResult<int8_t, Decimal>(cast_executor, FieldType::TINYINT, src_data,
                                            arrow::decimal128(5, 2), target_data);
    }
    {
        // int8 to decimal
        std::vector<int8_t> src_data = {0, 2, -11, 22};
        std::vector<Decimal> target_data = {
            Decimal(3, 1, DecimalUtils::StrToInt128("00").value()),
            Decimal(3, 1, DecimalUtils::StrToInt128("20").value()),
            Decimal(3, 1, DecimalUtils::StrToInt128("-110").value()),
            Decimal(3, 1, DecimalUtils::StrToInt128("220").value())};
        CheckLiteralResult<int8_t, Decimal>(cast_executor, FieldType::TINYINT, src_data,
                                            arrow::decimal128(3, 1), target_data);
    }
    {
        // int8 to decimal, overflow
        std::vector<int8_t> src_data = {-121, -128, 127};
        CheckNullLiteralResult<int8_t>(cast_executor, FieldType::TINYINT, src_data,
                                       arrow::decimal128(3, 1));
    }
    {
        // int16 to decimal
        std::vector<int16_t> src_data = {2, -11, 22, -121, 0, -32768, 32767};
        std::vector<Decimal> target_data = {
            Decimal(10, 5, DecimalUtils::StrToInt128("200000").value()),
            Decimal(10, 5, DecimalUtils::StrToInt128("-1100000").value()),
            Decimal(10, 5, DecimalUtils::StrToInt128("2200000").value()),
            Decimal(10, 5, DecimalUtils::StrToInt128("-12100000").value()),
            Decimal(10, 5, DecimalUtils::StrToInt128("000000").value()),
            Decimal(10, 5, DecimalUtils::StrToInt128("-3276800000").value()),
            Decimal(10, 5, DecimalUtils::StrToInt128("3276700000").value())};
        CheckLiteralResult<int16_t, Decimal>(cast_executor, FieldType::SMALLINT, src_data,
                                             arrow::decimal128(10, 5), target_data);
    }
    {
        // int16 to decimal
        std::vector<int16_t> src_data = {2, -11, 22, -121, 0};
        std::vector<Decimal> target_data = {
            Decimal(8, 5, DecimalUtils::StrToInt128("200000").value()),
            Decimal(8, 5, DecimalUtils::StrToInt128("-1100000").value()),
            Decimal(8, 5, DecimalUtils::StrToInt128("2200000").value()),
            Decimal(8, 5, DecimalUtils::StrToInt128("-12100000").value()),
            Decimal(8, 5, DecimalUtils::StrToInt128("000000").value())};
        CheckLiteralResult<int16_t, Decimal>(cast_executor, FieldType::SMALLINT, src_data,
                                             arrow::decimal128(8, 5), target_data);
    }
    {
        // int16 to decimal, overflow
        std::vector<int16_t> src_data = {-32768, 32767};
        CheckNullLiteralResult<int16_t>(cast_executor, FieldType::SMALLINT, src_data,
                                        arrow::decimal128(8, 5));
    }
    {
        // int32 to decimal
        std::vector<int32_t> src_data = {2, -11, 22, -121, 0, -2147483648, 2147483647};
        std::vector<Decimal> target_data = {
            Decimal(13, 3, DecimalUtils::StrToInt128("2000").value()),
            Decimal(13, 3, DecimalUtils::StrToInt128("-11000").value()),
            Decimal(13, 3, DecimalUtils::StrToInt128("22000").value()),
            Decimal(13, 3, DecimalUtils::StrToInt128("-121000").value()),
            Decimal(13, 3, DecimalUtils::StrToInt128("0000").value()),
            Decimal(13, 3, DecimalUtils::StrToInt128("-2147483648000").value()),
            Decimal(13, 3, DecimalUtils::StrToInt128("2147483647000").value())};
        CheckLiteralResult<int32_t, Decimal>(cast_executor, FieldType::INT, src_data,
                                             arrow::decimal128(13, 3), target_data);
    }
    {
        // int32 to decimal
        std::vector<int32_t> src_data = {2, -11, 22, -121, 0};
        std::vector<Decimal> target_data = {
            Decimal(10, 3, DecimalUtils::StrToInt128("2000").value()),
            Decimal(10, 3, DecimalUtils::StrToInt128("-11000").value()),
            Decimal(10, 3, DecimalUtils::StrToInt128("22000").value()),
            Decimal(10, 3, DecimalUtils::StrToInt128("-121000").value()),
            Decimal(10, 3, DecimalUtils::StrToInt128("0000").value())};
        CheckLiteralResult<int32_t, Decimal>(cast_executor, FieldType::INT, src_data,
                                             arrow::decimal128(10, 3), target_data);
    }
    {
        // int32 to decimal, overflow
        std::vector<int32_t> src_data = {-2147483648, 2147483647};
        CheckNullLiteralResult<int32_t>(cast_executor, FieldType::INT, src_data,
                                        arrow::decimal128(10, 3));
    }
    {
        // int64 to decimal
        std::vector<int64_t> src_data = {2, -11, 22, -121, 0, MIN_INT64, MAX_INT64};
        std::vector<Decimal> target_data = {
            Decimal(20, 1, DecimalUtils::StrToInt128("20").value()),
            Decimal(20, 1, DecimalUtils::StrToInt128("-110").value()),
            Decimal(20, 1, DecimalUtils::StrToInt128("220").value()),
            Decimal(20, 1, DecimalUtils::StrToInt128("-1210").value()),
            Decimal(20, 1, DecimalUtils::StrToInt128("00").value()),
            Decimal(20, 1, DecimalUtils::StrToInt128("-92233720368547758080").value()),
            Decimal(20, 1, DecimalUtils::StrToInt128("92233720368547758070").value())};
        CheckLiteralResult<int64_t, Decimal>(cast_executor, FieldType::BIGINT, src_data,
                                             arrow::decimal128(20, 1), target_data);
    }
    {
        // int64 to decimal
        std::vector<int64_t> src_data = {0};
        std::vector<Decimal> target_data = {
            Decimal(38, 38, DecimalUtils::StrToInt128("0").value())};
        CheckLiteralResult<int64_t, Decimal>(cast_executor, FieldType::BIGINT, src_data,
                                             arrow::decimal128(38, 38), target_data);
    }
    {
        // int64 to decimal, overflow
        std::vector<int64_t> src_data = {1, -1, 2, -11, 22, -121, MIN_INT64, MAX_INT64};
        CheckNullLiteralResult<int64_t>(cast_executor, FieldType::BIGINT, src_data,
                                        arrow::decimal128(38, 38));
    }

    {
        // float to decimal
        std::vector<float> src_data = {0.0,     1.1,     123.45,   123.456,
                                       999.994, -123.45, -123.456, -999.994};
        std::vector<Decimal> target_data = {
            Decimal(5, 2, DecimalUtils::StrToInt128("000").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("110").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("12345").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("12346").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("99999").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-12345").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-12346").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-99999").value())};
        CheckLiteralResult<float, Decimal>(cast_executor, FieldType::FLOAT, src_data,
                                           arrow::decimal128(5, 2), target_data);
    }
    {
        // float to decimal, overflow
        std::vector<float> src_data = {999.996, MAX_FLOAT, MIN_FLOAT};
        CheckNullLiteralResult<float>(cast_executor, FieldType::FLOAT, src_data,
                                      arrow::decimal128(5, 2));
    }
    {
        // Java Cannot cast INFINITY, -INFINITY, NAN, will throw exception
        float src_data = INFINITY;
        auto msg = CheckLiteralInvalidResult<float>(cast_executor, FieldType::FLOAT, src_data,
                                                    arrow::decimal128(38, 0));
        ASSERT_TRUE(msg.find("Invalid: Cannot cast inf to decimal") != std::string::npos);
    }
    {
        // double to decimal
        std::vector<double> src_data = {0.0,     1.1,      123.45,  123.456, 999.994,
                                        -123.45, -123.456, -999.99, -999.994};
        std::vector<Decimal> target_data = {
            Decimal(5, 2, DecimalUtils::StrToInt128("000").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("110").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("12345").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("12346").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("99999").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-12345").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-12346").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-99999").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-99999").value())};
        CheckLiteralResult<double, Decimal>(cast_executor, FieldType::DOUBLE, src_data,
                                            arrow::decimal128(5, 2), target_data);
    }
    {
        // double to decimal, overflow
        std::vector<double> src_data = {999.996, MAX_DOUBLE, MIN_DOUBLE};
        CheckNullLiteralResult<double>(cast_executor, FieldType::DOUBLE, src_data,
                                       arrow::decimal128(5, 2));
    }
    {
        // Java Cannot cast INFINITY, -INFINITY, NAN, will throw exception
        double src_data = NAN;
        auto msg = CheckLiteralInvalidResult<double>(cast_executor, FieldType::DOUBLE, src_data,
                                                     arrow::decimal128(38, 0));
        ASSERT_TRUE(msg.find("Invalid: Cannot cast nan to decimal") != std::string::npos);
    }
}

TEST_F(CastExecutorTest, TestNumericPrimitiveToDecimalCastExecutorCastArray) {
    auto cast_executor = std::make_shared<NumericPrimitiveToDecimalCastExecutor>();
    {
        // test int8
        CheckArrayResult(
            cast_executor, arrow::int8(), arrow::decimal128(5, 2),
            "[0, 2, -11, 22, -121, -128, 127, null]",
            R"(["0.00", "2.00", "-11.00", "22.00", "-121.00", "-128.00", "127.00", null])");

        CheckArrayResult(cast_executor, arrow::int8(), arrow::decimal128(3, 1),
                         "[0, 2, -11, 22, -121, -128, 127]",
                         R"(["0.0", "2.0", "-11.0", "22.0", null, null, null])");
    }
    {
        // test int16
        CheckArrayResult(
            cast_executor, arrow::int16(), arrow::decimal128(10, 5),
            "[2, -11, 22, -121, 0, -32768, 32767, null]",
            R"(["2.00000","-11.00000", "22.00000", "-121.00000", "0.00000", "-32768.00000", "32767.00000", null])");
        CheckArrayResult(
            cast_executor, arrow::int16(), arrow::decimal128(8, 5),
            "[2, -11, 22, -121, 0, -32768, 32767]",
            R"(["2.00000","-11.00000", "22.00000", "-121.00000", "0.00000", null, null])");
    }
    {
        // test int32
        CheckArrayResult(
            cast_executor, arrow::int32(), arrow::decimal128(13, 3),
            "[2, -11, 22, -121, 0, -2147483648, 2147483647, null]",
            R"(["2.000","-11.000", "22.000", "-121.000", "0.000", "-2147483648.000", "2147483647.000", null])");
        CheckArrayResult(cast_executor, arrow::int32(), arrow::decimal128(10, 3),
                         "[2, -11, 22, -121, 0, -2147483648, 2147483647]",
                         R"(["2.000","-11.000", "22.000", "-121.000", "0.000", null, null])");
    }
    {
        // test int64
        CheckArrayResult(
            cast_executor, arrow::int64(), arrow::decimal128(20, 1),
            "[2, -11, 22, -121, 0, -9223372036854775808, 9223372036854775807, null]",
            R"(["2.0","-11.0", "22.0", "-121.0", "0.0", "-9223372036854775808.0", "9223372036854775807.0", null])");
        CheckArrayResult(
            cast_executor, arrow::int64(), arrow::decimal128(38, 38),
            "[1, -1, 2, -11, 22, -121, 0, -9223372036854775808, 9223372036854775807]",
            R"([null, null, null, null, null, null, "0.00000000000000000000000000000000000000", null, null])");
    }
    // test float
    {
        std::vector<float> src_data = {0.0,      1.1,      123.45,  123.456,   999.994,  -123.45,
                                       -123.456, -999.994, 999.996, MAX_FLOAT, MIN_FLOAT};
        auto src_array = MakeArrowArray<float, arrow::FloatBuilder>(arrow::float32(), src_data);
        auto target_array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::decimal128(5, 2),
                R"(["0.00", "1.10", "123.45", "123.46", "999.99", "-123.45", "-123.46", "-999.99", null, null, null, null])")
                .ValueOrDie();
        CheckArrayResult(cast_executor, arrow::decimal128(5, 2), src_array, target_array);
    }
    // test double
    {
        std::vector<double> src_data = {0.0,      1.1,     123.45,     123.456,
                                        999.994,  -123.45, -123.456,   -999.99,
                                        -999.994, 999.996, MAX_DOUBLE, MIN_DOUBLE};
        auto src_array = MakeArrowArray<double, arrow::DoubleBuilder>(arrow::float64(), src_data);
        auto target_array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::decimal128(5, 2),
                R"(["0.00", "1.10", "123.45", "123.46", "999.99", "-123.45", "-123.46", "-999.99", "-999.99", null, null, null, null])")
                .ValueOrDie();
        CheckArrayResult(cast_executor, arrow::decimal128(5, 2), src_array, target_array);
    }
    {
        // invalid numeric
        auto msg = CheckArrayInvalidResult(cast_executor, arrow::float32(),
                                           arrow::decimal128(38, 0), R"([NaN])");
        ASSERT_TRUE(msg.find("Invalid: Cannot cast nan to decimal") != std::string::npos);
    }
    {
        // invalid numeric
        auto msg = CheckArrayInvalidResult(cast_executor, arrow::float64(),
                                           arrow::decimal128(38, 0), R"([-Inf])");
        ASSERT_TRUE(msg.find("Invalid: Cannot cast -inf to decimal") != std::string::npos);
    }
}

TEST_F(CastExecutorTest, TestStringToDecimalCastExecutorCastLiteral) {
    auto cast_executor = std::make_shared<StringToDecimalCastExecutor>();
    {
        std::vector<std::string> src_data = {
            "0.01",    "127.3",      "15.789",   "20.404", "0.56",    "-127.3",
            "-15.789", "-20.404",    "-0.56",    "0",      "1.237E2", "1.2E-1",
            "-1.2E-2", "1.23789E+2", "1.237e+2", "999.99", "-999.99"};
        std::vector<Decimal> target_data = {
            Decimal(5, 2, DecimalUtils::StrToInt128("001").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("12730").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("1579").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("2040").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("056").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-12730").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-1579").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-2040").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-056").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("000").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("12370").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("012").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-001").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("12379").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("12370").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("99999").value()),
            Decimal(5, 2, DecimalUtils::StrToInt128("-99999").value()),
        };
        CheckLiteralResult<std::string, Decimal>(cast_executor, FieldType::STRING, src_data,
                                                 arrow::decimal128(5, 2), target_data);
    }
    {
        // scaled data overflow precision
        std::vector<std::string> src_data = {"-999.996", "9999.1"};
        CheckNullLiteralResult<std::string>(cast_executor, FieldType::STRING, src_data,
                                            arrow::decimal128(5, 2));
    }
    {
        std::vector<std::string> src_data = {"1111111111111111111111111111111111111.1"};
        std::vector<Decimal> target_data = {
            Decimal(38, 1,
                    DecimalUtils::StrToInt128("11111111111111111111111111111111111111").value()),
        };
        CheckLiteralResult<std::string, Decimal>(cast_executor, FieldType::STRING, src_data,
                                                 arrow::decimal128(38, 1), target_data);
    }
    {
        // For str "1111111111111111111111111111111111111.15", which is overflow max precision (38),
        // Java return "1111111111111111111111111111111111111.2",
        // while C++ return null
        std::vector<std::string> src_data = {"1111111111111111111111111111111111111.15",
                                             "111111111111111111111111111111111111111.1",
                                             "3.4028235e+38"};
        CheckNullLiteralResult<std::string>(cast_executor, FieldType::STRING, src_data,
                                            arrow::decimal128(38, 1));
    }
    {
        std::vector<std::string> src_data = {"0"};
        std::vector<Decimal> target_data = {
            Decimal(38, 38, DecimalUtils::StrToInt128("0").value()),
        };
        CheckLiteralResult<std::string, Decimal>(cast_executor, FieldType::STRING, src_data,
                                                 arrow::decimal128(38, 38), target_data);
    }
    {
        // rescale will overflow 128bit
        std::vector<std::string> src_data = {"1", "-1", "9", "-9"};
        CheckNullLiteralResult<std::string>(cast_executor, FieldType::STRING, src_data,
                                            arrow::decimal128(38, 38));
    }
    {
        // rescale will overflow 128bit
        std::vector<std::string> src_data = {"2e+28"};
        CheckNullLiteralResult<std::string>(cast_executor, FieldType::STRING, src_data,
                                            arrow::decimal128(38, 10));
    }

    {
        // invalid str
        std::string src_data = "inf";
        auto msg = CheckLiteralInvalidResult<std::string>(cast_executor, FieldType::STRING,
                                                          src_data, arrow::decimal128(5, 2));
        ASSERT_TRUE(msg.find("The string \'inf' is not a valid decimal128 number") !=
                    std::string::npos);
    }
}

TEST_F(CastExecutorTest, TestStringToDecimalCastExecutorCastArray) {
    auto cast_executor = std::make_shared<StringToDecimalCastExecutor>();
    CheckArrayResult(
        cast_executor, arrow::utf8(), arrow::decimal128(5, 2),
        R"(["0.01", "127.3", "15.789", "20.404", "0.56", "-127.3", "-15.789", "-20.404", "-0.56", "0", "1.237E2", "1.2E-1", "-1.2E-2", "1.23789E+2", "1.237e+2", "999.99", "-999.99", "-999.996", "9999.1", null])",
        R"(["0.01", "127.30", "15.79", "20.40", "0.56", "-127.30", "-15.79", "-20.40", "-0.56", "0.00", "123.70", "0.12", "-0.01", "123.79", "123.70", "999.99", "-999.99", null, null, null])");

    // For str "1111111111111111111111111111111111111.15", which is overflow max precision (38),
    // Java return "1111111111111111111111111111111111111.2",
    // while C++ return null
    CheckArrayResult(
        cast_executor, arrow::utf8(), arrow::decimal128(38, 1),
        R"(["1111111111111111111111111111111111111.1", "1111111111111111111111111111111111111.15", "111111111111111111111111111111111111111.1", "3.4028235e+38"])",
        R"(["1111111111111111111111111111111111111.1", null, null, null])");
    CheckArrayResult(cast_executor, arrow::utf8(), arrow::decimal128(38, 38),
                     R"(["1", "0", "-1", "9", "-9"])",
                     R"([null, "0.00000000000000000000000000000000000000", null, null, null])");
    CheckArrayResult(cast_executor, arrow::utf8(), arrow::decimal128(38, 10), R"(["2e+28"])",
                     R"([null])");
    {
        // invalid str
        auto msg = CheckArrayInvalidResult(cast_executor, arrow::utf8(), arrow::decimal128(5, 2),
                                           R"(["1+2"])");
        ASSERT_TRUE(msg.find("The string \'1+2\' is not a valid decimal128 number") !=
                    std::string::npos);
    }
    {
        // invalid str
        auto msg = CheckArrayInvalidResult(cast_executor, arrow::utf8(), arrow::decimal128(5, 2),
                                           R"(["nan"])");
        ASSERT_TRUE(msg.find("The string \'nan' is not a valid decimal128 number") !=
                    std::string::npos);
    }
}

TEST_F(CastExecutorTest, TestDecimalToDecimalCastExecutorCastLiteral) {
    auto cast_executor = std::make_shared<DecimalToDecimalCastExecutor>();
    {
        std::vector<Decimal> src_data = {
            Decimal(38, 10, DecimalUtils::StrToInt128("020000000000").value()),
            Decimal(38, 10, DecimalUtils::StrToInt128("300000000000").value()),
            Decimal(38, 10, DecimalUtils::StrToInt128("220000000000").value()),
            Decimal(38, 10, DecimalUtils::StrToInt128("-1210000000000").value()),
            Decimal(38, 10, DecimalUtils::StrToInt128("1000000000000000000000000000000").value()),
        };
        std::vector<Decimal> target_data = {
            Decimal(28, 0, DecimalUtils::StrToInt128("2").value()),
            Decimal(28, 0, DecimalUtils::StrToInt128("30").value()),
            Decimal(28, 0, DecimalUtils::StrToInt128("22").value()),
            Decimal(28, 0, DecimalUtils::StrToInt128("-121").value()),
            Decimal(28, 0, DecimalUtils::StrToInt128("100000000000000000000").value()),
        };
        CheckLiteralResult<Decimal, Decimal>(cast_executor, FieldType::DECIMAL, src_data,
                                             arrow::decimal128(28, 0), target_data);
        CheckLiteralResult<Decimal, Decimal>(cast_executor, FieldType::DECIMAL, target_data,
                                             arrow::decimal128(38, 10), src_data);
    }
    {
        // reduce scale and precision
        std::vector<Decimal> src_data = {
            Decimal(5, 3, DecimalUtils::StrToInt128("0010").value()),
            Decimal(5, 3, DecimalUtils::StrToInt128("27324").value()),
            Decimal(5, 3, DecimalUtils::StrToInt128("15789").value()),
            Decimal(5, 3, DecimalUtils::StrToInt128("-27320").value()),
            Decimal(5, 3, DecimalUtils::StrToInt128("-1234").value()),
            Decimal(5, 3, DecimalUtils::StrToInt128("-15789").value()),
            Decimal(5, 3, DecimalUtils::StrToInt128("0000").value()),
            Decimal(5, 3, DecimalUtils::StrToInt128("99994").value()),
        };
        std::vector<Decimal> target_data = {
            Decimal(4, 2, DecimalUtils::StrToInt128("001").value()),
            Decimal(4, 2, DecimalUtils::StrToInt128("2732").value()),
            Decimal(4, 2, DecimalUtils::StrToInt128("1579").value()),
            Decimal(4, 2, DecimalUtils::StrToInt128("-2732").value()),
            Decimal(4, 2, DecimalUtils::StrToInt128("-123").value()),
            Decimal(4, 2, DecimalUtils::StrToInt128("-1579").value()),
            Decimal(4, 2, DecimalUtils::StrToInt128("000").value()),
            Decimal(4, 2, DecimalUtils::StrToInt128("9999").value()),
        };
        CheckLiteralResult<Decimal, Decimal>(cast_executor, FieldType::DECIMAL, src_data,
                                             arrow::decimal128(4, 2), target_data);
    }
    {
        // reduce scale and precision with precision overflow
        std::vector<Decimal> src_data = {
            Decimal(5, 3, DecimalUtils::StrToInt128("-99996").value())};
        CheckNullLiteralResult<Decimal>(cast_executor, FieldType::DECIMAL, src_data,
                                        arrow::decimal128(4, 2));
    }
    {
        // increase scale
        std::vector<Decimal> src_data = {Decimal(4, 1, DecimalUtils::StrToInt128("12").value())};
        std::vector<Decimal> target_data = {
            Decimal(3, 2, DecimalUtils::StrToInt128("120").value())};
        CheckLiteralResult<Decimal, Decimal>(cast_executor, FieldType::DECIMAL, src_data,
                                             arrow::decimal128(3, 2), target_data);
    }
    {
        // increase scale, with precision overflow
        std::vector<Decimal> src_data = {Decimal(4, 1, DecimalUtils::StrToInt128("123").value())};
        CheckNullLiteralResult<Decimal>(cast_executor, FieldType::DECIMAL, src_data,
                                        arrow::decimal128(3, 2));
    }
    {
        // change precision
        std::vector<Decimal> src_data = {Decimal(5, 3, DecimalUtils::StrToInt128("10").value()),
                                         Decimal(5, 3, DecimalUtils::StrToInt128("7326").value())};
        std::vector<Decimal> target_data = {
            Decimal(4, 3, DecimalUtils::StrToInt128("10").value()),
            Decimal(4, 3, DecimalUtils::StrToInt128("7326").value())};
        CheckLiteralResult<Decimal, Decimal>(cast_executor, FieldType::DECIMAL, src_data,
                                             arrow::decimal128(4, 3), target_data);
    }
    {
        // change precision, with precision overflow
        std::vector<Decimal> src_data = {Decimal(5, 3, DecimalUtils::StrToInt128("32456").value()),
                                         Decimal(5, 3, DecimalUtils::StrToInt128("99999").value())};
        CheckNullLiteralResult<Decimal>(cast_executor, FieldType::DECIMAL, src_data,
                                        arrow::decimal128(4, 3));
    }
}

TEST_F(CastExecutorTest, TestDecimalToDecimalCastExecutorCastArray) {
    auto cast_executor = std::make_shared<DecimalToDecimalCastExecutor>();
    CheckArrayResult(cast_executor, arrow::decimal128(38, 10), arrow::decimal128(28, 0),
                     R"([
          "02.0000000000",
          "30.0000000000",
          "22.0000000000",
          "-121.0000000000",
          "100000000000000000000.0000000000",
           null
        ])",
                     R"([
          "02.",
          "30.",
          "22.",
          "-121.",
          "100000000000000000000",
          null
        ])");
    CheckArrayResult(cast_executor, arrow::decimal128(28, 0), arrow::decimal128(38, 10),
                     R"([
          "02.",
          "30.",
          "22.",
          "-121.",
           null
        ])",
                     R"([
          "02.0000000000",
          "30.0000000000",
          "22.0000000000",
          "-121.0000000000",
          null
        ])");

    // reduce scale
    CheckArrayResult(
        cast_executor, arrow::decimal128(5, 3), arrow::decimal128(4, 2),
        R"(["0.010", "27.324", "15.789", "-27.320", "-1.234", "-15.789", "0.000", "99.994", "-99.996"])",
        R"(["0.01", "27.32", "15.79", "-27.32", "-1.23", "-15.79", "0.00", "99.99", null])");

    // increase scale
    CheckArrayResult(cast_executor, arrow::decimal128(4, 1), arrow::decimal128(3, 2),
                     R"(["1.2", "12.3"])", R"(["1.20", null])");

    // change precision
    CheckArrayResult(cast_executor, arrow::decimal128(5, 3), arrow::decimal128(4, 3),
                     R"(["0.010", "7.326", "32.456", "99.999"])",
                     R"(["0.010", "7.326", null, null])");
}

TEST_F(CastExecutorTest, TestBooleanToDecimalCastExecutorCastLiteral) {
    auto cast_executor = std::make_shared<BooleanToDecimalCastExecutor>();
    std::vector<bool> src_data = {true, false};
    {
        std::vector<Decimal> target_data = {
            Decimal(5, 3, DecimalUtils::StrToInt128("1000").value()),
            Decimal(5, 3, DecimalUtils::StrToInt128("0000").value())};
        CheckLiteralResult<bool, Decimal>(cast_executor, FieldType::BOOLEAN, src_data,
                                          arrow::decimal128(5, 3), target_data);
    }
    {
        std::vector<Decimal> target_data = {Decimal(1, 0, DecimalUtils::StrToInt128("1").value()),
                                            Decimal(1, 0, DecimalUtils::StrToInt128("0").value())};
        CheckLiteralResult<bool, Decimal>(cast_executor, FieldType::BOOLEAN, src_data,
                                          arrow::decimal128(1, 0), target_data);
    }
    {
        std::vector<Decimal> target_data = {
            Decimal(38, 37,
                    DecimalUtils::StrToInt128("10000000000000000000000000000000000000").value()),
            Decimal(38, 37,
                    DecimalUtils::StrToInt128("00000000000000000000000000000000000000").value())};
        CheckLiteralResult<bool, Decimal>(cast_executor, FieldType::BOOLEAN, src_data,
                                          arrow::decimal128(38, 37), target_data);
    }
    {
        ASSERT_OK_AND_ASSIGN(Literal invalid_literal,
                             cast_executor->Cast(Literal(true), arrow::decimal128(3, 3)));
        ASSERT_TRUE(invalid_literal.IsNull());
    }
    {
        ASSERT_OK_AND_ASSIGN(Literal valid_literal,
                             cast_executor->Cast(Literal(false), arrow::decimal128(3, 3)))
        ASSERT_EQ(valid_literal, Literal(Decimal(3, 3, 0)));
    }
}

TEST_F(CastExecutorTest, TestBooleanToDecimalCastExecutorCastArray) {
    auto cast_executor = std::make_shared<BooleanToDecimalCastExecutor>();
    CheckArrayResult(cast_executor, arrow::boolean(), arrow::decimal128(5, 3),
                     R"([true, false, null])", R"(["1.000", "0.000", null])");
    CheckArrayResult(
        cast_executor, arrow::boolean(), arrow::decimal128(38, 37), R"([true, false, null])",
        R"(["1.0000000000000000000000000000000000000", "0.0000000000000000000000000000000000000", null])");
    CheckArrayResult(cast_executor, arrow::boolean(), arrow::decimal128(37, 37),
                     R"([true, false, null])",
                     R"([null, "0.0000000000000000000000000000000000000", null])");
    CheckArrayResult(cast_executor, arrow::boolean(), arrow::decimal128(1, 0),
                     R"([true, false, null])", R"(["1", "0", null])");
}

TEST_F(CastExecutorTest, TestCastLiteralOverflow) {
    auto cast_executor = std::make_shared<NumericPrimitiveCastExecutor>();
    {
        Literal src(static_cast<int16_t>(10));
        ASSERT_OK_AND_ASSIGN(Literal casted, cast_executor->Cast(src, arrow::int8()));
        ASSERT_FALSE(CastingUtils::IsIntegerLiteralCastedOverflow(src, casted));
    }
    {
        Literal src(static_cast<int16_t>(300));
        ASSERT_OK_AND_ASSIGN(Literal casted, cast_executor->Cast(src, arrow::int8()));
        ASSERT_TRUE(CastingUtils::IsIntegerLiteralCastedOverflow(src, casted));
    }
}

TEST_F(CastExecutorTest, TestTimestampToTimestampCastExecutorCastArray) {
    auto cast_executor = std::make_shared<TimestampToTimestampCastExecutor>();
    // test only convert precision
    {
        // nano -> second
        auto src_type = arrow::timestamp(arrow::TimeUnit::NANO);
        std::vector<int64_t> src_data = {
            1001001001,
            10101001000,
        };
        std::shared_ptr<arrow::Array> src_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(src_type, src_data);

        auto target_type = arrow::timestamp(arrow::TimeUnit::SECOND);
        std::vector<int64_t> target_data = {
            1,
            10,
        };
        std::shared_ptr<arrow::Array> target_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(target_type, target_data);
        CheckArrayResult(cast_executor, target_type, src_array, target_array);
    }
    {
        // second -> nano
        auto src_type = arrow::timestamp(arrow::TimeUnit::SECOND);
        std::vector<int64_t> src_data = {
            1,
            10,
        };
        std::shared_ptr<arrow::Array> src_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(src_type, src_data);

        auto target_type = arrow::timestamp(arrow::TimeUnit::NANO);
        std::vector<int64_t> target_data = {
            1000000000,
            10000000000,
        };
        std::shared_ptr<arrow::Array> target_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(target_type, target_data);
        CheckArrayResult(cast_executor, target_type, src_array, target_array);
    }
    {
        // milli -> milli
        auto src_type = arrow::timestamp(arrow::TimeUnit::MILLI);
        std::vector<int64_t> src_data = {
            1001001001,
            10101001000,
        };
        std::shared_ptr<arrow::Array> src_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(src_type, src_data);

        CheckArrayResult(cast_executor, src_type, src_array, src_array);
    }
    // test only convert tz
    {
        // ts -> ts with tz
        auto src_type = arrow::timestamp(arrow::TimeUnit::SECOND);
        std::vector<int64_t> src_data = {
            28800,
        };
        std::shared_ptr<arrow::Array> src_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(src_type, src_data);

        auto target_type = arrow::timestamp(arrow::TimeUnit::SECOND, "Asia/Shanghai");
        std::vector<int64_t> target_data = {
            0,
        };
        std::shared_ptr<arrow::Array> target_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(target_type, target_data);
        CheckArrayResult(cast_executor, target_type, src_array, target_array);
    }
    {
        // ts with tz -> ts
        auto src_type = arrow::timestamp(arrow::TimeUnit::SECOND, "Asia/Shanghai");
        std::vector<int64_t> src_data = {
            0,
        };
        std::shared_ptr<arrow::Array> src_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(src_type, src_data);

        auto target_type = arrow::timestamp(arrow::TimeUnit::SECOND);
        std::vector<int64_t> target_data = {
            28800,
        };
        std::shared_ptr<arrow::Array> target_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(target_type, target_data);
        CheckArrayResult(cast_executor, target_type, src_array, target_array);
    }
    // test convert tz and precision
    {
        // ts -> ts with tz
        auto src_type = arrow::timestamp(arrow::TimeUnit::NANO);
        std::vector<int64_t> src_data = {
            28800000000000l,
            -28800000000000l,
            0l,
            1758529078741000001l,
        };
        std::shared_ptr<arrow::Array> src_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(src_type, src_data);

        auto target_type = arrow::timestamp(arrow::TimeUnit::SECOND, "Asia/Shanghai");
        std::vector<int64_t> target_data = {
            0,
            -57600l,
            -28800l,
            1758500278l,
        };
        std::shared_ptr<arrow::Array> target_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(target_type, target_data);
        CheckArrayResult(cast_executor, target_type, src_array, target_array);
    }
    {
        // ts with tz -> ts
        auto src_type = arrow::timestamp(arrow::TimeUnit::SECOND, "Asia/Shanghai");
        std::vector<int64_t> src_data = {
            0l,
            -28800l,
            28800l,
            1758529078l,
        };
        std::shared_ptr<arrow::Array> src_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(src_type, src_data);

        auto target_type = arrow::timestamp(arrow::TimeUnit::MILLI);
        std::vector<int64_t> target_data = {
            28800000l,
            0l,
            57600000l,
            1758557878000l,
        };
        std::shared_ptr<arrow::Array> target_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(target_type, target_data);
        CheckArrayResult(cast_executor, target_type, src_array, target_array);
    }
    {
        // special case:ts -> ts with tz
        // 500ns in ts equals -28799999999500ns (-28800000000000 + 500) in ts with tz
        // -28799999999500ns cast to second will result in -28799s, which is different from Java
        // Paimon (-28800s)
        auto src_type = arrow::timestamp(arrow::TimeUnit::NANO);
        std::vector<int64_t> src_data = {
            0l,
            500l,
        };
        std::shared_ptr<arrow::Array> src_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(src_type, src_data);

        auto target_type = arrow::timestamp(arrow::TimeUnit::SECOND, "Asia/Shanghai");
        std::vector<int64_t> target_data = {
            -28800,
            -28799l,
        };
        std::shared_ptr<arrow::Array> target_array =
            MakeArrowArray<int64_t, arrow::TimestampBuilder>(target_type, target_data);
        CheckArrayResult(cast_executor, target_type, src_array, target_array);
    }
}

}  // namespace paimon::test
