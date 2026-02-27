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

#pragma once

#include <string>

#include "paimon/visibility.h"

namespace paimon {
/// `Function` represents a predicate function used in query expressions and filtering operations.
/// It serves as the base class for all predicate functions in Paimon.
class PAIMON_EXPORT Function {
 public:
    enum class PAIMON_EXPORT Type {
        IS_NULL = 1,
        IS_NOT_NULL = 2,
        EQUAL = 3,
        NOT_EQUAL = 4,
        GREATER_THAN = 5,
        GREATER_OR_EQUAL = 6,
        LESS_THAN = 7,
        LESS_OR_EQUAL = 8,
        IN = 9,
        NOT_IN = 10,
        AND = 11,
        OR = 12,
        STARTS_WITH = 13,
        ENDS_WITH = 14,
        CONTAINS = 15,
        LIKE = 16
    };
    virtual ~Function() = default;
    virtual Type GetType() const = 0;
    virtual std::string ToString() const = 0;
};
}  // namespace paimon
