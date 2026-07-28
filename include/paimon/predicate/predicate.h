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
#include <memory>
#include <string>

#include "paimon/predicate/function.h"
#include "paimon/visibility.h"

struct ArrowArray;
struct ArrowSchema;

namespace paimon {
class Function;

/// Predicate interface. To create a predicate, please use `PredicateBuilder`.
/// @see PredicateBuilder
class PAIMON_EXPORT Predicate {
 public:
    virtual ~Predicate();
    virtual bool operator==(const Predicate& other) const = 0;

    virtual const Function& GetFunction() const = 0;
    /// @return The negation predicate of this predicate if possible.
    virtual std::shared_ptr<Predicate> Negate() const = 0;
    virtual std::string ToString() const = 0;
};
}  // namespace paimon
