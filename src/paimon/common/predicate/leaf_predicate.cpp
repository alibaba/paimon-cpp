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

#include "paimon/predicate/leaf_predicate.h"

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/predicate/leaf_function.h"
#include "paimon/common/predicate/leaf_predicate_impl.h"
#include "paimon/predicate/function.h"

namespace paimon {
enum class FieldType;

LeafPredicate::LeafPredicate(const LeafFunction& leaf_function, int32_t field_index,
                             const std::string& field_name, const FieldType& field_type,
                             const std::vector<Literal>& literals)
    : leaf_function_(leaf_function),
      field_index_(field_index),
      field_name_(field_name),
      field_type_(field_type),
      literals_(literals) {}

const Function& LeafPredicate::GetFunction() const {
    return leaf_function_;
}

std::shared_ptr<Predicate> LeafPredicate::Negate() const {
    const auto* negate_func = leaf_function_.Negate();
    if (!negate_func) {
        return nullptr;
    }
    return std::make_shared<LeafPredicateImpl>(*negate_func, field_index_, field_name_, field_type_,
                                               literals_);
}

bool LeafPredicate::operator==(const Predicate& other) const {
    if (this == &other) {
        return true;
    }
    auto leaf_predicate = dynamic_cast<LeafPredicate*>(const_cast<Predicate*>(&other));
    if (!leaf_predicate) {
        return false;
    }
    return GetFunction().GetType() == leaf_predicate->GetFunction().GetType() &&
           FieldIndex() == leaf_predicate->FieldIndex() &&
           FieldName() == leaf_predicate->FieldName() &&
           GetFieldType() == leaf_predicate->GetFieldType() &&
           Literals() == leaf_predicate->Literals();
}

std::string LeafPredicate::ToString() const {
    std::string literals_str;
    if (literals_.empty()) {
        literals_str = "";
    } else if (literals_.size() == 1) {
        literals_str = literals_[0].ToString();
    } else {
        std::vector<std::string> tmp_literals_strs;
        tmp_literals_strs.reserve(literals_.size());
        for (const auto& literal : literals_) {
            tmp_literals_strs.emplace_back(literal.ToString());
        }
        literals_str = fmt::format("[{}]", fmt::join(tmp_literals_strs, ", "));
    }
    return literals_str.empty()
               ? fmt::format("{}({})", leaf_function_.ToString(), field_name_)
               : fmt::format("{}({}, {})", leaf_function_.ToString(), field_name_, literals_str);
}

}  // namespace paimon
