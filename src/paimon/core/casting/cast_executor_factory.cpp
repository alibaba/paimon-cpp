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

#include <utility>

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

namespace paimon {
#define REGISTER_CAST_EXECUTOR(TARGET, SRC, EXECUTOR) \
    executor_map_[FieldType::TARGET][FieldType::SRC] = std::make_shared<EXECUTOR>();

CastExecutorFactory* CastExecutorFactory::GetCastExecutorFactory() {
    static std::unique_ptr<CastExecutorFactory> executor_factory =
        std::unique_ptr<CastExecutorFactory>(new CastExecutorFactory());
    return executor_factory.get();
}

std::shared_ptr<CastExecutor> CastExecutorFactory::GetCastExecutor(const FieldType& src,
                                                                   const FieldType& target) const {
    auto target_iter = executor_map_.find(target);
    if (target_iter == executor_map_.end()) {
        return nullptr;
    }
    auto src_iter = target_iter->second.find(src);
    if (src_iter == target_iter->second.end()) {
        return nullptr;
    }
    return src_iter->second;
}

CastExecutorFactory::CastExecutorFactory() {
    REGISTER_CAST_EXECUTOR(TINYINT, BOOLEAN, BooleanToNumericCastExecutor);
    REGISTER_CAST_EXECUTOR(SMALLINT, BOOLEAN, BooleanToNumericCastExecutor);
    REGISTER_CAST_EXECUTOR(INT, BOOLEAN, BooleanToNumericCastExecutor);
    REGISTER_CAST_EXECUTOR(BIGINT, BOOLEAN, BooleanToNumericCastExecutor);
    REGISTER_CAST_EXECUTOR(FLOAT, BOOLEAN, BooleanToNumericCastExecutor);
    REGISTER_CAST_EXECUTOR(DOUBLE, BOOLEAN, BooleanToNumericCastExecutor);

    REGISTER_CAST_EXECUTOR(DECIMAL, BOOLEAN, BooleanToDecimalCastExecutor);

    REGISTER_CAST_EXECUTOR(STRING, BOOLEAN, BooleanToStringCastExecutor);

    REGISTER_CAST_EXECUTOR(TINYINT, TINYINT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(TINYINT, SMALLINT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(TINYINT, INT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(TINYINT, BIGINT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(TINYINT, FLOAT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(TINYINT, DOUBLE, NumericPrimitiveCastExecutor);

    REGISTER_CAST_EXECUTOR(SMALLINT, TINYINT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(SMALLINT, SMALLINT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(SMALLINT, INT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(SMALLINT, BIGINT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(SMALLINT, FLOAT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(SMALLINT, DOUBLE, NumericPrimitiveCastExecutor);

    REGISTER_CAST_EXECUTOR(INT, TINYINT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(INT, SMALLINT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(INT, INT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(INT, BIGINT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(INT, FLOAT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(INT, DOUBLE, NumericPrimitiveCastExecutor);

    REGISTER_CAST_EXECUTOR(BIGINT, TINYINT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(BIGINT, SMALLINT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(BIGINT, INT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(BIGINT, BIGINT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(BIGINT, FLOAT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(BIGINT, DOUBLE, NumericPrimitiveCastExecutor);

    REGISTER_CAST_EXECUTOR(FLOAT, TINYINT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(FLOAT, SMALLINT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(FLOAT, INT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(FLOAT, BIGINT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(FLOAT, FLOAT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(FLOAT, DOUBLE, NumericPrimitiveCastExecutor);

    REGISTER_CAST_EXECUTOR(DOUBLE, TINYINT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(DOUBLE, SMALLINT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(DOUBLE, INT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(DOUBLE, BIGINT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(DOUBLE, FLOAT, NumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(DOUBLE, DOUBLE, NumericPrimitiveCastExecutor);

    REGISTER_CAST_EXECUTOR(BOOLEAN, TINYINT, NumericToBooleanCastExecutor);
    REGISTER_CAST_EXECUTOR(BOOLEAN, SMALLINT, NumericToBooleanCastExecutor);
    REGISTER_CAST_EXECUTOR(BOOLEAN, INT, NumericToBooleanCastExecutor);
    REGISTER_CAST_EXECUTOR(BOOLEAN, BIGINT, NumericToBooleanCastExecutor);

    REGISTER_CAST_EXECUTOR(BOOLEAN, STRING, StringToBooleanCastExecutor);

    REGISTER_CAST_EXECUTOR(TINYINT, STRING, StringToNumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(SMALLINT, STRING, StringToNumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(INT, STRING, StringToNumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(BIGINT, STRING, StringToNumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(FLOAT, STRING, StringToNumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(DOUBLE, STRING, StringToNumericPrimitiveCastExecutor);

    REGISTER_CAST_EXECUTOR(STRING, TINYINT, NumericToStringCastExecutor);
    REGISTER_CAST_EXECUTOR(STRING, SMALLINT, NumericToStringCastExecutor);
    REGISTER_CAST_EXECUTOR(STRING, INT, NumericToStringCastExecutor);
    REGISTER_CAST_EXECUTOR(STRING, BIGINT, NumericToStringCastExecutor);
    REGISTER_CAST_EXECUTOR(STRING, FLOAT, NumericToStringCastExecutor);
    REGISTER_CAST_EXECUTOR(STRING, DOUBLE, NumericToStringCastExecutor);
    REGISTER_CAST_EXECUTOR(STRING, DECIMAL, NumericToStringCastExecutor);

    REGISTER_CAST_EXECUTOR(BINARY, STRING, StringToBinaryCastExecutor);

    REGISTER_CAST_EXECUTOR(STRING, BINARY, BinaryToStringCastExecutor);

    REGISTER_CAST_EXECUTOR(BLOB, BINARY, BinaryToBlobCastExecutor);

    REGISTER_CAST_EXECUTOR(STRING, DATE, DateToStringCastExecutor);

    REGISTER_CAST_EXECUTOR(TIMESTAMP, DATE, DateToTimestampCastExecutor);

    REGISTER_CAST_EXECUTOR(TIMESTAMP, INT, NumericPrimitiveToTimestampCastExecutor);
    REGISTER_CAST_EXECUTOR(TIMESTAMP, BIGINT, NumericPrimitiveToTimestampCastExecutor);

    REGISTER_CAST_EXECUTOR(DATE, STRING, StringToDateCastExecutor);

    REGISTER_CAST_EXECUTOR(STRING, TIMESTAMP, TimestampToStringCastExecutor);

    REGISTER_CAST_EXECUTOR(DATE, TIMESTAMP, TimestampToDateCastExecutor);

    REGISTER_CAST_EXECUTOR(INT, TIMESTAMP, TimestampToNumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(BIGINT, TIMESTAMP, TimestampToNumericPrimitiveCastExecutor);

    REGISTER_CAST_EXECUTOR(TIMESTAMP, STRING, StringToTimestampCastExecutor);

    REGISTER_CAST_EXECUTOR(TINYINT, DECIMAL, DecimalToNumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(SMALLINT, DECIMAL, DecimalToNumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(INT, DECIMAL, DecimalToNumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(BIGINT, DECIMAL, DecimalToNumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(FLOAT, DECIMAL, DecimalToNumericPrimitiveCastExecutor);
    REGISTER_CAST_EXECUTOR(DOUBLE, DECIMAL, DecimalToNumericPrimitiveCastExecutor);

    REGISTER_CAST_EXECUTOR(DECIMAL, TINYINT, NumericPrimitiveToDecimalCastExecutor);
    REGISTER_CAST_EXECUTOR(DECIMAL, SMALLINT, NumericPrimitiveToDecimalCastExecutor);
    REGISTER_CAST_EXECUTOR(DECIMAL, INT, NumericPrimitiveToDecimalCastExecutor);
    REGISTER_CAST_EXECUTOR(DECIMAL, BIGINT, NumericPrimitiveToDecimalCastExecutor);
    REGISTER_CAST_EXECUTOR(DECIMAL, FLOAT, NumericPrimitiveToDecimalCastExecutor);
    REGISTER_CAST_EXECUTOR(DECIMAL, DOUBLE, NumericPrimitiveToDecimalCastExecutor);

    REGISTER_CAST_EXECUTOR(DECIMAL, STRING, StringToDecimalCastExecutor);

    REGISTER_CAST_EXECUTOR(DECIMAL, DECIMAL, DecimalToDecimalCastExecutor);

    REGISTER_CAST_EXECUTOR(TIMESTAMP, TIMESTAMP, TimestampToTimestampCastExecutor);
}

}  // namespace paimon
