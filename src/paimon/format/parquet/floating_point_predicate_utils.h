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
    FloatingPointPredicateUtils() = delete;
    ~FloatingPointPredicateUtils() = delete;

    static bool IsType(FieldType field_type) {
        return field_type == FieldType::FLOAT || field_type == FieldType::DOUBLE;
    }

    static bool IsZero(const Literal& literal) {
        if (literal.IsNull()) {
            return false;
        }
        if (literal.GetType() == FieldType::FLOAT) {
            return literal.GetValue<float>() == 0.0f;
        }
        if (literal.GetType() == FieldType::DOUBLE) {
            return literal.GetValue<double>() == 0.0;
        }
        return false;
    }

    static bool IsNegativeZero(const Literal& literal) {
        if (!IsZero(literal)) {
            return false;
        }
        if (literal.GetType() == FieldType::FLOAT) {
            return std::signbit(literal.GetValue<float>());
        }
        return std::signbit(literal.GetValue<double>());
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
};

}  // namespace paimon::parquet
