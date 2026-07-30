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

#include "paimon/format/parquet/predicate_converter.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <utility>

#include "arrow/compute/api.h"
#include "arrow/compute/expression.h"
#include "arrow/scalar.h"
#include "arrow/type_fwd.h"
#include "arrow/util/decimal.h"
#include "fmt/format.h"
#include "paimon/data/decimal.h"
#include "paimon/defs.h"
#include "paimon/format/parquet/floating_point_predicate_utils.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate.h"

namespace paimon::parquet {
namespace {

arrow::compute::Expression PositiveInfinity(const Literal& literal) {
    if (literal.GetType() == FieldType::FLOAT) {
        return arrow::compute::literal(
            std::make_shared<arrow::FloatScalar>(std::numeric_limits<float>::infinity()));
    }
    return arrow::compute::literal(
        std::make_shared<arrow::DoubleScalar>(std::numeric_limits<double>::infinity()));
}

arrow::compute::Expression ConvertFloatingPointComparison(
    const std::string& field_name, Function::Type function_type, const Literal& literal,
    const arrow::compute::Expression& arrow_literal) {
    const FloatingPointPredicateUtils::LiteralInfo info =
        FloatingPointPredicateUtils::GetLiteralInfo(literal);
    arrow::compute::Expression field = arrow::compute::field_ref(field_name);
    switch (function_type) {
        case Function::Type::EQUAL:
            if (info.is_nan) {
                // parquet-mr omits FLOAT/DOUBLE min/max statistics when they contain NaN.
                // Therefore a row group with NaN cannot be eliminated, while a row group with
                // valid finite statistics can be pruned using max <= +infinity.
                return arrow::compute::greater(std::move(field), PositiveInfinity(literal));
            }
            // Arrow considers -0.0 and +0.0 equal, which is a safe pruning superset.
            return arrow::compute::equal(std::move(field), arrow_literal);
        case Function::Type::NOT_EQUAL:
            if (info.is_nan || info.is_zero) {
                // NaN invalidates parquet-mr statistics, and Arrow cannot distinguish the two
                // zero signs. Neither case can be safely eliminated through Arrow expressions.
                return arrow::compute::is_valid(std::move(field));
            }
            return arrow::compute::not_equal(std::move(field), arrow_literal);
        case Function::Type::GREATER_THAN:
            if (info.is_nan) {
                return arrow::compute::literal(false);
            }
            if (info.is_negative_zero) {
                // parquet-mr normalizes a zero upper bound to +0.0. Arrow treats both zero signs
                // as equal, so >= is the safe pruning equivalent of Java's > -0.0.
                return arrow::compute::greater_equal(std::move(field), arrow_literal);
            }
            return arrow::compute::greater(std::move(field), arrow_literal);
        case Function::Type::GREATER_OR_EQUAL:
            if (info.is_nan) {
                return arrow::compute::greater(std::move(field), PositiveInfinity(literal));
            }
            return arrow::compute::greater_equal(std::move(field), arrow_literal);
        case Function::Type::LESS_THAN:
            if (info.is_nan) {
                // Every non-null value may satisfy Java's value < NaN. If NaN is present,
                // parquet-mr omits the statistics, so the row group is also retained.
                return arrow::compute::is_valid(std::move(field));
            }
            if (info.is_zero && !info.is_negative_zero) {
                // parquet-mr normalizes a zero lower bound to -0.0. Arrow's <= 0.0 retains the
                // page for Java's -0.0 < +0.0.
                return arrow::compute::less_equal(std::move(field), arrow_literal);
            }
            return arrow::compute::less(std::move(field), arrow_literal);
        case Function::Type::LESS_OR_EQUAL:
            if (info.is_nan) {
                return arrow::compute::is_valid(std::move(field));
            }
            return arrow::compute::less_equal(std::move(field), arrow_literal);
        default:
            return arrow::compute::literal(true);
    }
}

}  // namespace

arrow::compute::Expression PredicateConverter::AlwaysTrue() {
    static const arrow::compute::Expression expr = arrow::compute::literal(true);
    return expr;
}

Result<arrow::compute::Expression> PredicateConverter::Convert(
    const std::shared_ptr<Predicate>& predicate, uint32_t node_count_limit) {
    if (!predicate) {
        return AlwaysTrue();
    }
    uint32_t node_count = 0;
    CollectNodeCount(predicate, &node_count);
    if (node_count > node_count_limit) {
        return AlwaysTrue();
    }
    return InnerConvert(predicate);
}

void PredicateConverter::CollectNodeCount(const std::shared_ptr<Predicate>& predicate,
                                          uint32_t* node_count) {
    const auto& function_type = predicate->GetFunction().GetType();
    if (auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
        if (function_type == Function::Type::IN || function_type == Function::Type::NOT_IN) {
            // IN and NOT_IN will be converted to Or(Equals) and And(NotEqual)
            *node_count += leaf_predicate->Literals().size();
        }
        *node_count += 1;
        return;
    }
    if (auto compound_predicate = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
        *node_count += 1;
        for (const auto& child : compound_predicate->Children()) {
            CollectNodeCount(child, node_count);
        }
    }
}

Result<arrow::compute::Expression> PredicateConverter::InnerConvert(
    const std::shared_ptr<Predicate>& predicate) {
    if (!predicate) {
        return AlwaysTrue();
    }
    if (auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
        return ConvertLeaf(leaf_predicate);
    }
    if (auto compound_predicate = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
        return ConvertCompound(compound_predicate);
    }
    return Status::Invalid("invalid predicate, must be leaf or compound");
}

Result<arrow::compute::Expression> PredicateConverter::ConvertCompound(
    const std::shared_ptr<CompoundPredicate>& compound_predicate) {
    const auto& children = compound_predicate->Children();
    const auto& function = compound_predicate->GetFunction();
    auto function_type = function.GetType();
    switch (function_type) {
        case Function::Type::AND: {
            std::vector<arrow::compute::Expression> sub_exprs;
            sub_exprs.reserve(children.size());
            for (const auto& child : children) {
                PAIMON_ASSIGN_OR_RAISE(arrow::compute::Expression sub_expr, InnerConvert(child));
                sub_exprs.push_back(std::move(sub_expr));
            }
            return arrow::compute::and_(sub_exprs);
        }
        case Function::Type::OR: {
            std::vector<arrow::compute::Expression> sub_exprs;
            sub_exprs.reserve(children.size());
            for (const auto& child : children) {
                PAIMON_ASSIGN_OR_RAISE(arrow::compute::Expression sub_expr, InnerConvert(child));
                sub_exprs.push_back(std::move(sub_expr));
            }
            return arrow::compute::or_(sub_exprs);
        }
        default:
            return Status::Invalid(
                fmt::format("invalid predicate type {}", static_cast<int32_t>(function_type)));
    }
}

Status PredicateConverter::CheckLiteralNotEmpty(const std::vector<Literal>& literals,
                                                const Function& function,
                                                const std::string& field_name) {
    if (literals.empty()) {
        return Status::Invalid(fmt::format("predicate [{}] need literal on field {}",
                                           function.ToString(), field_name));
    }
    return Status::OK();
}

#define CONVERT_TO_ARROW_LITERAL(LITERAL)                                                 \
    auto arrow_literal_result = ConvertToArrowLiteral(LITERAL);                           \
    if (!arrow_literal_result.ok() && arrow_literal_result.status().IsNotImplemented()) { \
        return AlwaysTrue();                                                              \
    }                                                                                     \
    if (!arrow_literal_result.ok()) {                                                     \
        return arrow_literal_result.status();                                             \
    }                                                                                     \
    auto arrow_literal = std::move(arrow_literal_result).value();

Result<arrow::compute::Expression> PredicateConverter::ConvertLeaf(
    const std::shared_ptr<LeafPredicate>& leaf_predicate) {
    const auto& field_name = leaf_predicate->FieldName();
    const auto& literals = leaf_predicate->Literals();
    const auto& function = leaf_predicate->GetFunction();
    auto function_type = function.GetType();
    if (FloatingPointPredicateUtils::IsType(leaf_predicate->GetFieldType()) &&
        FloatingPointPredicateUtils::IsComparison(function_type)) {
        return ConvertFloatingPointLeaf(leaf_predicate);
    }
    switch (function_type) {
        case Function::Type::IS_NULL: {
            return arrow::compute::is_null(arrow::compute::field_ref(field_name),
                                           /*nan_is_null=*/false);
        }
        case Function::Type::IS_NOT_NULL: {
            return arrow::compute::not_(
                arrow::compute::is_null(arrow::compute::field_ref(field_name),
                                        /*nan_is_null=*/false));
        }
        case Function::Type::EQUAL: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            CONVERT_TO_ARROW_LITERAL(literals[0]);
            return arrow::compute::equal(arrow::compute::field_ref(field_name), arrow_literal);
        }
        case Function::Type::NOT_EQUAL: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            CONVERT_TO_ARROW_LITERAL(literals[0]);
            return arrow::compute::not_equal(arrow::compute::field_ref(field_name), arrow_literal);
        }
        case Function::Type::GREATER_THAN: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            CONVERT_TO_ARROW_LITERAL(literals[0]);
            return arrow::compute::greater(arrow::compute::field_ref(field_name), arrow_literal);
        }
        case Function::Type::GREATER_OR_EQUAL: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            CONVERT_TO_ARROW_LITERAL(literals[0]);
            return arrow::compute::greater_equal(arrow::compute::field_ref(field_name),
                                                 arrow_literal);
        }
        case Function::Type::LESS_THAN: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            CONVERT_TO_ARROW_LITERAL(literals[0]);
            return arrow::compute::less(arrow::compute::field_ref(field_name), arrow_literal);
        }
        case Function::Type::LESS_OR_EQUAL: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            CONVERT_TO_ARROW_LITERAL(literals[0]);
            return arrow::compute::less_equal(arrow::compute::field_ref(field_name), arrow_literal);
        }
        // Noted that: java paimon don't support pushdown IN and NOT_IN to parquet
        case Function::Type::IN: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            // in convert to Or(Equals)
            std::vector<arrow::compute::Expression> sub_exprs;
            sub_exprs.reserve(literals.size());
            for (const auto& literal : literals) {
                CONVERT_TO_ARROW_LITERAL(literal);
                sub_exprs.push_back(
                    arrow::compute::equal(arrow::compute::field_ref(field_name), arrow_literal));
            }
            return arrow::compute::or_(sub_exprs);
        }
        case Function::Type::NOT_IN: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            // not in convert to And(NotEqual)
            std::vector<arrow::compute::Expression> sub_exprs;
            sub_exprs.reserve(literals.size());
            for (const auto& literal : literals) {
                CONVERT_TO_ARROW_LITERAL(literal);
                sub_exprs.push_back(arrow::compute::not_equal(arrow::compute::field_ref(field_name),
                                                              arrow_literal));
            }
            return arrow::compute::and_(sub_exprs);
        }
        case Function::Type::STARTS_WITH: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            auto options = std::make_shared<arrow::compute::MatchSubstringOptions>(
                literals[0].GetValue<std::string>());
            return arrow::compute::call("starts_with", {arrow::compute::field_ref(field_name)},
                                        options);
        }
        case Function::Type::ENDS_WITH: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            auto options = std::make_shared<arrow::compute::MatchSubstringOptions>(
                literals[0].GetValue<std::string>());
            return arrow::compute::call("ends_with", {arrow::compute::field_ref(field_name)},
                                        options);
        }
        case Function::Type::CONTAINS: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            auto options = std::make_shared<arrow::compute::MatchSubstringOptions>(
                literals[0].GetValue<std::string>());
            return arrow::compute::call("match_substring", {arrow::compute::field_ref(field_name)},
                                        options);
        }
        case Function::Type::LIKE: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            auto options = std::make_shared<arrow::compute::MatchSubstringOptions>(
                literals[0].GetValue<std::string>());
            return arrow::compute::call("match_like", {arrow::compute::field_ref(field_name)},
                                        options);
        }
        default:
            return Status::Invalid(
                fmt::format("invalid predicate type {}", static_cast<int32_t>(function_type)));
    }
    return Status::OK();
}

Result<arrow::compute::Expression> PredicateConverter::ConvertFloatingPointLeaf(
    const std::shared_ptr<LeafPredicate>& leaf_predicate) {
    const auto& field_name = leaf_predicate->FieldName();
    const auto& literals = leaf_predicate->Literals();
    const auto& function = leaf_predicate->GetFunction();
    Function::Type function_type = function.GetType();
    PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));

    if (function_type == Function::Type::IN || function_type == Function::Type::NOT_IN) {
        if (std::any_of(literals.begin(), literals.end(),
                        [](const Literal& literal) { return literal.IsNull(); })) {
            return Status::Invalid("literal cannot be null in predicate");
        }
        std::vector<arrow::compute::Expression> sub_exprs;
        sub_exprs.reserve(literals.size());
        for (const auto& literal : literals) {
            CONVERT_TO_ARROW_LITERAL(literal);
            sub_exprs.push_back(ConvertFloatingPointComparison(field_name,
                                                               function_type == Function::Type::IN
                                                                   ? Function::Type::EQUAL
                                                                   : Function::Type::NOT_EQUAL,
                                                               literal, arrow_literal));
        }
        return function_type == Function::Type::IN ? arrow::compute::or_(sub_exprs)
                                                   : arrow::compute::and_(sub_exprs);
    }

    CONVERT_TO_ARROW_LITERAL(literals[0]);
    return ConvertFloatingPointComparison(field_name, function_type, literals[0], arrow_literal);
}

Result<arrow::compute::Expression> PredicateConverter::ConvertToArrowLiteral(
    const Literal& literal) {
    auto literal_type = literal.GetType();
    if (literal.IsNull()) {
        return Status::Invalid("literal cannot be null in predicate");
    }
    switch (literal_type) {
        case FieldType::BOOLEAN:
            return arrow::compute::literal(std::make_shared<arrow::BooleanScalar>(
                static_cast<bool>(literal.GetValue<bool>())));
        case FieldType::TINYINT:
            return arrow::compute::literal(std::make_shared<arrow::Int8Scalar>(
                static_cast<int8_t>(literal.GetValue<int8_t>())));
        case FieldType::SMALLINT:
            return arrow::compute::literal(std::make_shared<arrow::Int16Scalar>(
                static_cast<int16_t>(literal.GetValue<int16_t>())));
        case FieldType::INT:
            return arrow::compute::literal(std::make_shared<arrow::Int32Scalar>(
                static_cast<int32_t>(literal.GetValue<int32_t>())));
        case FieldType::DATE:
            return arrow::compute::literal(std::make_shared<arrow::Date32Scalar>(
                static_cast<int32_t>(literal.GetValue<int32_t>())));
        case FieldType::BIGINT:
            return arrow::compute::literal(std::make_shared<arrow::Int64Scalar>(
                static_cast<int64_t>(literal.GetValue<int64_t>())));
        case FieldType::FLOAT:
            return arrow::compute::literal(std::make_shared<arrow::FloatScalar>(
                static_cast<float>(literal.GetValue<float>())));
        case FieldType::DOUBLE:
            return arrow::compute::literal(std::make_shared<arrow::DoubleScalar>(
                static_cast<double>(literal.GetValue<double>())));
        case FieldType::STRING: {
            auto str = literal.GetValue<std::string>();
            return arrow::compute::literal(std::make_shared<arrow::StringScalar>(str));
        }
        case FieldType::DECIMAL: {
            auto decimal = literal.GetValue<Decimal>();
            return arrow::compute::literal(std::make_shared<arrow::Decimal128Scalar>(
                arrow::Decimal128(decimal.HighBits(), decimal.LowBits()),
                arrow::decimal128(decimal.Precision(), decimal.Scale())));
        }
        // TODO(lisizhuo.lsz): java paimon does not support BINARY, TIMESTAMP and DECIMAL
        case FieldType::TIMESTAMP:
        case FieldType::BINARY:
            return Status::NotImplemented(
                "Not support Binary and Timestamp predicate push down in parquet file "
                "format");
        default:
            return Status::Invalid(
                fmt::format("invalid literal type {}", static_cast<int32_t>(literal_type)));
    }
}

}  // namespace paimon::parquet
