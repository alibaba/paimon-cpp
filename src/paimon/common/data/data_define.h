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

#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/memory/bytes.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class InternalArray;
class InternalRow;
class InternalMap;

/// std::monostate indicates null
using NullType = std::monostate;
using VariantType =
    std::variant<NullType, bool, char, int16_t, int32_t, int64_t, float, double, BinaryString,
                 std::shared_ptr<Bytes>, Timestamp, Decimal, std::shared_ptr<InternalRow>,
                 std::shared_ptr<InternalArray>, std::shared_ptr<InternalMap>, std::string_view>;
class DataDefine {
 public:
    DataDefine() = delete;
    ~DataDefine() = delete;
    inline static bool IsVariantNull(const VariantType& value) {
        return std::holds_alternative<NullType>(value);
    }

    /// always make sure value is T type
    template <typename T>
    inline static T GetVariantValue(const VariantType& value) {
        const T* ptr = std::get_if<T>(&value);
        assert(ptr);
        return *ptr;
    }

    /// if value type mismatch T, return nullptr
    template <typename T>
    inline static const T* GetVariantPtr(const VariantType& value) {
        return std::get_if<T>(&value);
    }

    static std::string VariantValueToString(const VariantType& value) {
        return std::visit(
            [](const auto& arg) -> std::string {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, NullType>) {
                    return "null";
                } else if constexpr (std::is_same_v<T, bool>) {
                    return arg ? "true" : "false";
                } else if constexpr (std::is_same_v<T, std::shared_ptr<Bytes>>) {
                    return std::string(arg->data(), arg->size());
                } else if constexpr (std::is_same_v<T,  // NOLINT(readability/braces)
                                                    BinaryString> ||
                                     std::is_same_v<T, Timestamp> || std::is_same_v<T, Decimal>) {
                    return arg.ToString();
                } else if constexpr (std::is_same_v<T, std::shared_ptr<InternalRow>>) {
                    return "row";
                } else if constexpr (std::is_same_v<T, std::shared_ptr<InternalArray>>) {
                    return "array";
                } else if constexpr (std::is_same_v<T, std::shared_ptr<InternalMap>>) {
                    return "map";
                } else if constexpr (std::is_same_v<T, std::string_view>) {
                    return std::string(arg);
                } else {
                    return std::to_string(arg);
                }
            },
            value);
    }

    static Result<Literal> VariantValueToLiteral(const VariantType& value,
                                                 const arrow::Type::type& arrow_type) {
        switch (arrow_type) {
            case arrow::Type::type::BOOL:
                return Literal(GetVariantValue<bool>(value));
            case arrow::Type::type::INT8:
                return Literal(static_cast<int8_t>(GetVariantValue<char>(value)));
            case arrow::Type::type::INT16:
                return Literal(GetVariantValue<int16_t>(value));
            case arrow::Type::type::INT32:
                return Literal(GetVariantValue<int32_t>(value));
            case arrow::Type::type::INT64:
                return Literal(GetVariantValue<int64_t>(value));
            case arrow::Type::type::FLOAT:
                return Literal(GetVariantValue<float>(value));
            case arrow::Type::type::DOUBLE:
                return Literal(GetVariantValue<double>(value));
            case arrow::Type::type::STRING: {
                auto binary_string_ptr = GetVariantPtr<BinaryString>(value);
                if (binary_string_ptr == nullptr) {
                    return Status::Invalid(
                        "VariantValueToLiteral failed, cannot get BinaryString from VariantType, "
                        "input value maybe string view");
                }
                auto str = binary_string_ptr->ToString();
                return Literal(FieldType::STRING, str.data(), str.size());
            }
            case arrow::Type::type::BINARY: {
                auto bytes = GetVariantValue<std::shared_ptr<Bytes>>(value);
                return Literal(FieldType::BINARY, bytes->data(), bytes->size());
            }
            case arrow::Type::type::TIMESTAMP:
                return Literal(GetVariantValue<Timestamp>(value));
            case arrow::Type::type::DECIMAL128:
                return Literal(GetVariantValue<Decimal>(value));
            case arrow::Type::type::DATE32:
                return Literal(FieldType::DATE, GetVariantValue<int32_t>(value));
            default:
                return Status::Invalid(
                    fmt::format("Not support arrow type {} in VariantValueToLiteral",
                                static_cast<int32_t>(arrow_type)));
        }
    }
};
}  // namespace paimon
