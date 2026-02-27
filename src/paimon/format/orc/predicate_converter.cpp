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

#include "paimon/format/orc/predicate_converter.h"

#include <exception>
#include <utility>

#include "fmt/format.h"
#include "orc/Int128.hh"
#include "orc/Type.hh"
#include "orc/sargs/Literal.hh"
#include "orc/sargs/SearchArgument.hh"
#include "paimon/common/predicate/predicate_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate.h"

namespace paimon::orc {

Result<std::unique_ptr<::orc::SearchArgument>> PredicateConverter::Convert(
    const ::orc::Type& orc_type, const std::shared_ptr<Predicate>& predicate) {
    if (!predicate) {
        return std::unique_ptr<::orc::SearchArgument>();
    }
    std::unordered_map<std::string, uint64_t> name_id_mapping;
    for (uint64_t i = 0; i < orc_type.getSubtypeCount(); i++) {
        name_id_mapping[orc_type.getFieldName(i)] = orc_type.getSubtype(i)->getColumnId();
    }
    auto split_predicates = PredicateUtils::SplitAnd(predicate);
    // same as java, if check failed, do not push down to ::orc instead of return invalid
    // status
    std::vector<std::shared_ptr<Predicate>> pushable_predicates;
    pushable_predicates.reserve(split_predicates.size());
    for (const auto& split_predicate : split_predicates) {
        PAIMON_ASSIGN_OR_RAISE(bool pushable,
                               IsPredicatePushable(split_predicate, name_id_mapping));
        if (pushable) {
            pushable_predicates.push_back(split_predicate);
        }
    }
    if (!pushable_predicates.empty()) {
        try {
            auto builder = ::orc::SearchArgumentFactory::newBuilder();
            builder->startAnd();
            for (auto predicate : pushable_predicates) {
                PAIMON_RETURN_NOT_OK(Convert(predicate, name_id_mapping, builder.get()));
            }
            builder->end();
            return builder->build();
        } catch (const std::exception& e) {
            return Status::Invalid(fmt::format(
                "convert paimon predicate to orc SearchArgument failed, with error: {}", e.what()));
        } catch (...) {
            return Status::Invalid(
                "convert paimon predicate to orc SearchArgument failed, with unknown error");
        }
    }
    return std::unique_ptr<::orc::SearchArgument>();
}

Status PredicateConverter::ConvertCompound(
    const std::shared_ptr<CompoundPredicate>& compound_predicate,
    const std::unordered_map<std::string, uint64_t>& name_id_mapping,
    ::orc::SearchArgumentBuilder* builder) {
    const auto& children = compound_predicate->Children();
    const auto& function = compound_predicate->GetFunction();
    auto function_type = function.GetType();
    switch (function_type) {
        case Function::Type::AND: {
            builder->startAnd();
            for (const auto& child : children) {
                PAIMON_RETURN_NOT_OK(Convert(child, name_id_mapping, builder));
            }
            builder->end();
            break;
        }
        case Function::Type::OR: {
            builder->startOr();
            for (const auto& child : children) {
                PAIMON_RETURN_NOT_OK(Convert(child, name_id_mapping, builder));
            }
            builder->end();
            break;
        }
        default:
            return Status::Invalid(
                fmt::format("invalid predicate type {}", static_cast<int32_t>(function_type)));
    }
    return Status::OK();
}

Status PredicateConverter::ConvertLeaf(
    const std::shared_ptr<LeafPredicate>& leaf_predicate,
    const std::unordered_map<std::string, uint64_t>& name_id_mapping,
    ::orc::SearchArgumentBuilder* builder) {
    const auto& field_name = leaf_predicate->FieldName();
    auto iter = name_id_mapping.find(field_name);
    if (iter == name_id_mapping.end()) {
        return Status::Invalid(
            fmt::format("ConvertLeafPredicate failed in PredicateConverter: field {} in predicate "
                        "not in file schema",
                        field_name));
    }
    auto field_index = iter->second;
    const auto& literals = leaf_predicate->Literals();
    const auto& function = leaf_predicate->GetFunction();
    auto function_type = function.GetType();
    PAIMON_ASSIGN_OR_RAISE(::orc::PredicateDataType orc_type,
                           ConvertToOrcPredicateType(leaf_predicate->GetFieldType()));
    switch (function_type) {
        case Function::Type::IS_NULL: {
            builder->isNull(field_index, orc_type);
            break;
        }
        case Function::Type::IS_NOT_NULL: {
            builder->startNot();
            builder->isNull(field_index, orc_type);
            builder->end();
            break;
        }
        case Function::Type::EQUAL: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            PAIMON_ASSIGN_OR_RAISE(std::vector<::orc::Literal> orc_literals,
                                   ConvertToOrcLiterals(literals));
            builder->equals(field_index, orc_type, orc_literals[0]);
            break;
        }
        case Function::Type::NOT_EQUAL: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            PAIMON_ASSIGN_OR_RAISE(std::vector<::orc::Literal> orc_literals,
                                   ConvertToOrcLiterals(literals));
            builder->startNot();
            builder->equals(field_index, orc_type, orc_literals[0]);
            builder->end();
            break;
        }
        case Function::Type::GREATER_THAN: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            PAIMON_ASSIGN_OR_RAISE(std::vector<::orc::Literal> orc_literals,
                                   ConvertToOrcLiterals(literals));
            builder->startNot();
            builder->lessThanEquals(field_index, orc_type, orc_literals[0]);
            builder->end();
            break;
        }
        case Function::Type::GREATER_OR_EQUAL: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            PAIMON_ASSIGN_OR_RAISE(std::vector<::orc::Literal> orc_literals,
                                   ConvertToOrcLiterals(literals));
            builder->startNot();
            builder->lessThan(field_index, orc_type, orc_literals[0]);
            builder->end();
            break;
        }
        case Function::Type::LESS_THAN: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            PAIMON_ASSIGN_OR_RAISE(std::vector<::orc::Literal> orc_literals,
                                   ConvertToOrcLiterals(literals));
            builder->lessThan(field_index, orc_type, orc_literals[0]);
            break;
        }
        case Function::Type::LESS_OR_EQUAL: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            PAIMON_ASSIGN_OR_RAISE(std::vector<::orc::Literal> orc_literals,
                                   ConvertToOrcLiterals(literals));
            builder->lessThanEquals(field_index, orc_type, orc_literals[0]);
            break;
        }
        // Noted that: java paimon don't support pushdown IN and NOT_IN to orc
        case Function::Type::IN: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            PAIMON_ASSIGN_OR_RAISE(std::vector<::orc::Literal> orc_literals,
                                   ConvertToOrcLiterals(literals));
            builder->in(field_index, orc_type, orc_literals);
            break;
        }
        case Function::Type::NOT_IN: {
            PAIMON_RETURN_NOT_OK(CheckLiteralNotEmpty(literals, function, field_name));
            PAIMON_ASSIGN_OR_RAISE(std::vector<::orc::Literal> orc_literals,
                                   ConvertToOrcLiterals(literals));
            builder->startNot();
            builder->in(field_index, orc_type, orc_literals);
            builder->end();
            break;
        }
        case Function::Type::STARTS_WITH:
        case Function::Type::ENDS_WITH:
        case Function::Type::CONTAINS:
        case Function::Type::LIKE:
            // SearchArgument does not support predicates including StartsWith, EndsWith, Contains
            // and Like that should skip.
            builder->literal(::orc::TruthValue::YES);
            break;
        default:
            return Status::Invalid(
                fmt::format("invalid predicate type {}", static_cast<int32_t>(function_type)));
    }
    return Status::OK();
}

Result<::orc::PredicateDataType> PredicateConverter::ConvertToOrcPredicateType(
    const FieldType& field_type) {
    switch (field_type) {
        case FieldType::BOOLEAN:
            return ::orc::PredicateDataType::BOOLEAN;
        case FieldType::TINYINT:
        case FieldType::SMALLINT:
        case FieldType::INT:
        case FieldType::BIGINT:
            return ::orc::PredicateDataType::LONG;
        case FieldType::FLOAT:
        case FieldType::DOUBLE:
            return ::orc::PredicateDataType::FLOAT;
        case FieldType::STRING:
            return ::orc::PredicateDataType::STRING;
        case FieldType::TIMESTAMP:
            return ::orc::PredicateDataType::TIMESTAMP;
        case FieldType::DECIMAL:
            return ::orc::PredicateDataType::DECIMAL;
        case FieldType::DATE:
            return ::orc::PredicateDataType::DATE;
        default:
            return Status::Invalid(fmt::format("invalid field type {} in PredicateConverter",
                                               FieldTypeUtils::FieldTypeToString(field_type)));
    }
}

Result<std::vector<::orc::Literal>> PredicateConverter::ConvertToOrcLiterals(
    const std::vector<Literal>& literals) {
    std::vector<::orc::Literal> orc_literals;
    orc_literals.reserve(literals.size());
    for (const auto& literal : literals) {
        auto literal_type = literal.GetType();
        PAIMON_ASSIGN_OR_RAISE(::orc::PredicateDataType orc_type,
                               ConvertToOrcPredicateType(literal_type));
        if (literal.IsNull()) {
            orc_literals.emplace_back(orc_type);
            continue;
        }
        switch (literal_type) {
            case FieldType::BOOLEAN:
                orc_literals.emplace_back(literal.GetValue<bool>());
                break;
            case FieldType::TINYINT:
                orc_literals.emplace_back(static_cast<int64_t>(literal.GetValue<int8_t>()));
                break;
            case FieldType::SMALLINT:
                orc_literals.emplace_back(static_cast<int64_t>(literal.GetValue<int16_t>()));
                break;
            case FieldType::INT:
                orc_literals.emplace_back(static_cast<int64_t>(literal.GetValue<int32_t>()));
                break;
            case FieldType::BIGINT:
                orc_literals.emplace_back(literal.GetValue<int64_t>());
                break;
            case FieldType::FLOAT:
                orc_literals.emplace_back(static_cast<double>(literal.GetValue<float>()));
                break;
            case FieldType::DOUBLE:
                orc_literals.emplace_back(literal.GetValue<double>());
                break;
            case FieldType::STRING: {
                auto str = literal.GetValue<std::string>();
                orc_literals.emplace_back(str.data(), str.length());
                break;
            }
            case FieldType::TIMESTAMP: {
                auto timestamp = literal.GetValue<Timestamp>();
                // orc literal is different with paimon literal,
                // if timestamp < 1970-01-01 00:00:00, nanosecond in orc literal is negative
                const int64_t conversion_factor_from_second = 1000;
                const int64_t conversion_factor_from_nanosecond = 1000000;
                orc_literals.emplace_back(
                    timestamp.GetMillisecond() / conversion_factor_from_second,
                    (timestamp.GetMillisecond() % conversion_factor_from_second) *
                            conversion_factor_from_nanosecond +
                        timestamp.GetNanoOfMillisecond());
                break;
            }
            case FieldType::DECIMAL: {
                auto decimal = literal.GetValue<Decimal>();
                orc_literals.emplace_back(::orc::Int128(decimal.HighBits(), decimal.LowBits()),
                                          decimal.Precision(), decimal.Scale());
                break;
            }
            case FieldType::DATE:
                orc_literals.emplace_back(orc_type,
                                          static_cast<int64_t>(literal.GetValue<int32_t>()));
                break;
            default:
                return Status::Invalid(
                    fmt::format("invalid literal type {}", static_cast<int32_t>(literal_type)));
        }
    }
    return orc_literals;
}
Status PredicateConverter::Convert(const std::shared_ptr<Predicate>& predicate,
                                   const std::unordered_map<std::string, uint64_t>& name_id_mapping,
                                   ::orc::SearchArgumentBuilder* builder) {
    if (auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
        return ConvertLeaf(leaf_predicate, name_id_mapping, builder);
    }
    if (auto compound_predicate = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
        return ConvertCompound(compound_predicate, name_id_mapping, builder);
    }
    return Status::Invalid("invalid predicate, must be leaf or compound");
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

Result<bool> PredicateConverter::IsPredicatePushable(
    const std::shared_ptr<Predicate>& predicate,
    const std::unordered_map<std::string, uint64_t>& name_id_mapping) {
    if (auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
        return IsLeafPredicatePushable(leaf_predicate, name_id_mapping);
    }
    if (auto compound_predicate = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
        return IsCompoundPredicatePushable(compound_predicate, name_id_mapping);
    }
    return Status::Invalid("invalid predicate, must be leaf or compound");
}

Result<bool> PredicateConverter::IsCompoundPredicatePushable(
    const std::shared_ptr<CompoundPredicate>& compound_predicate,
    const std::unordered_map<std::string, uint64_t>& name_id_mapping) {
    const auto& children = compound_predicate->Children();
    const auto& function = compound_predicate->GetFunction();
    auto function_type = function.GetType();
    switch (function_type) {
        case Function::Type::AND:
        case Function::Type::OR: {
            for (const auto& child : children) {
                PAIMON_ASSIGN_OR_RAISE(bool pushable, IsPredicatePushable(child, name_id_mapping));
                if (!pushable) {
                    return false;
                }
            }
            return true;
        }
        default:
            return Status::Invalid(
                fmt::format("invalid predicate type {}", static_cast<int32_t>(function_type)));
    }
}

Result<bool> PredicateConverter::IsLeafPredicatePushable(
    const std::shared_ptr<LeafPredicate>& leaf_predicate,
    const std::unordered_map<std::string, uint64_t>& name_id_mapping) {
    const auto& field_name = leaf_predicate->FieldName();
    auto iter = name_id_mapping.find(field_name);
    if (iter == name_id_mapping.end()) {
        return Status::Invalid(fmt::format("field {} is not exist in data file", field_name));
    }
    if (leaf_predicate->GetFieldType() == FieldType::BINARY) {
        return false;
    }
    return true;
}

}  // namespace paimon::orc
