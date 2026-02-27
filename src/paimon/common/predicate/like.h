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

#include <string>

#include "paimon/common/predicate/string_leaf_binary_function.h"
#include "paimon/result.h"

namespace paimon {
/// A `StringLeafBinaryFunction` to eval filter like.
class Like : public StringLeafBinaryFunction {
 public:
    static const Like& Instance() {
        static const Like instance = Like();
        return instance;
    }

    Type GetType() const override {
        return Type::LIKE;
    }

    std::string ToString() const override {
        return "Like";
    }

    Result<bool> TestString(const std::string& field, const std::string& pattern) const override;

 private:
    Like() = default;
};
}  // namespace paimon
