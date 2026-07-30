/*
 * Copyright 2026-present Alibaba Inc.
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

#pragma once

#include <cmath>

#include "paimon/defs.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/literal.h"

namespace paimon::parquet {

class FloatingPointPredicateUtils {
 public:
    struct LiteralInfo {
        bool is_nan;
        bool is_zero;
        bool is_negative_zero;
    };

    FloatingPointPredicateUtils() = delete;
    ~FloatingPointPredicateUtils() = delete;

    static bool IsType(FieldType field_type) {
        return field_type == FieldType::FLOAT || field_type == FieldType::DOUBLE;
    }

    static bool IsComparison(Function::Type function_type) {
        switch (function_type) {
            case Function::Type::EQUAL:
            case Function::Type::NOT_EQUAL:
            case Function::Type::GREATER_THAN:
            case Function::Type::GREATER_OR_EQUAL:
            case Function::Type::LESS_THAN:
            case Function::Type::LESS_OR_EQUAL:
            case Function::Type::IN:
            case Function::Type::NOT_IN:
                return true;
            default:
                return false;
        }
    }

    static LiteralInfo GetLiteralInfo(const Literal& literal) {
        if (literal.GetType() == FieldType::FLOAT) {
            float value = literal.GetValue<float>();
            return {/*is_nan=*/std::isnan(value),
                    /*is_zero=*/value == 0.0f,
                    /*is_negative_zero=*/value == 0.0f && std::signbit(value)};
        }
        double value = literal.GetValue<double>();
        return {/*is_nan=*/std::isnan(value),
                /*is_zero=*/value == 0.0,
                /*is_negative_zero=*/value == 0.0 && std::signbit(value)};
    }
};

}  // namespace paimon::parquet
