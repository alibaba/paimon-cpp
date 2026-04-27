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

#include "paimon/predicate/literal.h"

#include <cmath>
#include <cstring>
#include <functional>
#include <sstream>
#include <string_view>
#include <type_traits>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/status.h"

namespace paimon {
class Literal::Impl {
 public:
    void Release() {
        if ((type_ == FieldType::STRING || type_ == FieldType::BINARY ||
             type_ == FieldType::BLOB) &&
            value_.Buffer) {
            if (own_data_) {
                delete[] value_.Buffer;
            }
            value_.Buffer = nullptr;
        }
    }

    size_t CalculateHashCode() const {
        if (is_null_) {
            return 0;
        }
        switch (type_) {
            case FieldType::BOOLEAN:
                return std::hash<bool>{}(value_.BooleanVal);
            case FieldType::TINYINT:
                return std::hash<int8_t>{}(value_.TinyIntVal);
            case FieldType::SMALLINT:
                return std::hash<int16_t>{}(value_.SmallIntVal);
            case FieldType::INT:
                return std::hash<int32_t>{}(value_.IntVal);
            case FieldType::BIGINT:
                return std::hash<int64_t>{}(value_.BigIntVal);
            case FieldType::FLOAT:
                return std::hash<float>{}(value_.FloatVal);
            case FieldType::DOUBLE:
                return std::hash<double>{}(value_.DoubleVal);
            case FieldType::STRING:
            case FieldType::BINARY:
                return std::hash<std::string_view>{}(std::string_view(value_.Buffer, size_));
            case FieldType::TIMESTAMP:
                return std::hash<int64_t>{}(value_.TimestampVal.GetMillisecond()) * 17 +
                       std::hash<int64_t>{}(value_.TimestampVal.GetNanoOfMillisecond());
            case FieldType::DECIMAL:
                return std::hash<int64_t>{}(value_.DecimalVal.HighBits()) * 31 +
                       std::hash<int64_t>{}(value_.DecimalVal.LowBits()) * 17 +
                       std::hash<int32_t>{}(value_.DecimalVal.Scale());
            case FieldType::DATE:
                return std::hash<int32_t>{}(value_.IntVal);
            default:
                return 0;
        }
    }

    union LiteralVal {
        int64_t BigIntVal;
        bool BooleanVal;
        int8_t TinyIntVal;
        int16_t SmallIntVal;
        int32_t IntVal;
        float FloatVal;
        double DoubleVal;
        char* Buffer;
        Decimal DecimalVal;
        Timestamp TimestampVal;
    } value_ = {};

    FieldType type_ = FieldType::UNKNOWN;
    // size of value_ if it is Buffer
    size_t size_ = 0;
    // indicate if this literal is null
    bool is_null_ = false;
    size_t hash_code_ = 0;
    bool own_data_ = true;
};

Literal::Literal(FieldType type) : impl_(std::make_unique<Impl>()) {
    impl_->type_ = type;
    impl_->size_ = 0;
    impl_->is_null_ = true;
}

template <typename T>
Literal::Literal(const T& val) : impl_(std::make_unique<Impl>()) {
    if constexpr (std::is_same_v<T, bool>) {
        impl_->type_ = FieldType::BOOLEAN;
        impl_->value_.BooleanVal = val;
    } else if constexpr (std::is_same_v<T, int8_t>) {
        impl_->type_ = FieldType::TINYINT;
        impl_->value_.TinyIntVal = val;
    } else if constexpr (std::is_same_v<T, int16_t>) {
        impl_->type_ = FieldType::SMALLINT;
        impl_->value_.SmallIntVal = val;
    } else if constexpr (std::is_same_v<T, int32_t>) {
        impl_->type_ = FieldType::INT;
        impl_->value_.IntVal = val;
    } else if constexpr (std::is_same_v<T, int64_t>) {
        impl_->type_ = FieldType::BIGINT;
        impl_->value_.BigIntVal = val;
    } else if constexpr (std::is_same_v<T, float>) {
        impl_->type_ = FieldType::FLOAT;
        impl_->value_.FloatVal = val;
    } else if constexpr (std::is_same_v<T, double>) {
        impl_->type_ = FieldType::DOUBLE;
        impl_->value_.DoubleVal = val;
    } else if constexpr (std::is_same_v<T, Timestamp>) {
        impl_->type_ = FieldType::TIMESTAMP;
        impl_->value_.TimestampVal = val;
    } else if constexpr (std::is_same_v<T, Decimal>) {
        impl_->type_ = FieldType::DECIMAL;
        impl_->value_.DecimalVal = val;
    } else {
        impl_->type_ = FieldType::UNKNOWN;
    }
    impl_->is_null_ = false;
    impl_->hash_code_ = impl_->CalculateHashCode();
}

Literal::Literal(FieldType binary_type, const char* str, size_t size)
    : Literal(binary_type, str, size, /*own_data=*/true) {}

Literal::Literal(FieldType binary_type, const char* str, size_t size, bool own_data)
    : impl_(std::make_unique<Impl>()) {
    impl_->type_ = binary_type;
    impl_->size_ = size;
    impl_->is_null_ = false;
    impl_->own_data_ = own_data;
    if (own_data) {
        impl_->value_.Buffer = new char[size];
        memcpy(impl_->value_.Buffer, str, size);
        impl_->hash_code_ = impl_->CalculateHashCode();
    } else {
        impl_->value_.Buffer = const_cast<char*>(str);
        impl_->hash_code_ = impl_->CalculateHashCode();
    }
}

Literal::Literal(FieldType date_type, int32_t date_value) : impl_(std::make_unique<Impl>()) {
    impl_->type_ = date_type;
    impl_->is_null_ = false;
    impl_->value_.IntVal = date_value;
    impl_->hash_code_ = impl_->CalculateHashCode();
}

Literal::Literal(Literal&& other) {
    *this = std::move(other);
}

Literal::Literal(const Literal& other) {
    *this = other;
}

Literal& Literal::operator=(Literal&& other) {
    if (&other == this) {
        return *this;
    }
    if (this->impl_) {
        this->impl_->Release();
    } else {
        impl_ = std::make_unique<Impl>();
    }
    impl_->type_ = other.impl_->type_;
    impl_->size_ = other.impl_->size_;
    impl_->is_null_ = other.impl_->is_null_;
    impl_->hash_code_ = other.impl_->hash_code_;
    impl_->own_data_ = other.impl_->own_data_;
    impl_->value_ = other.impl_->value_;
    if (impl_->type_ == FieldType::STRING || impl_->type_ == FieldType::BINARY ||
        impl_->type_ == FieldType::BLOB) {
        other.impl_->value_.Buffer = nullptr;
    }
    return *this;
}

Literal& Literal::operator=(const Literal& other) {
    if (&other == this) {
        return *this;
    }
    if (this->impl_) {
        this->impl_->Release();
    } else {
        impl_ = std::make_unique<Impl>();
    }
    impl_->type_ = other.impl_->type_;
    impl_->size_ = other.impl_->size_;
    impl_->is_null_ = other.impl_->is_null_;
    impl_->hash_code_ = other.impl_->hash_code_;
    impl_->own_data_ = other.impl_->own_data_;
    if ((impl_->type_ == FieldType::STRING || impl_->type_ == FieldType::BINARY ||
         impl_->type_ == FieldType::BLOB) &&
        impl_->own_data_) {
        impl_->value_.Buffer = new char[other.impl_->size_];
        memcpy(impl_->value_.Buffer, other.impl_->value_.Buffer, other.impl_->size_);
    } else {
        impl_->value_ = other.impl_->value_;
    }
    return *this;
}

Literal::~Literal() {
    impl_->Release();
}

bool Literal::IsNull() const {
    return impl_->is_null_;
}

FieldType Literal::GetType() const {
    return impl_->type_;
}

size_t Literal::HashCode() const {
    return impl_->hash_code_;
}

std::string Literal::ToString() const {
    if (impl_->is_null_) {
        return "null";
    }

    std::ostringstream sstream;
    std::string str;
    switch (impl_->type_) {
        case FieldType::BOOLEAN:
            sstream << (impl_->value_.BooleanVal ? "true" : "false");
            break;
        case FieldType::TINYINT:
            sstream << impl_->value_.TinyIntVal;
            break;
        case FieldType::SMALLINT:
            sstream << impl_->value_.SmallIntVal;
            break;
        case FieldType::INT:
            sstream << impl_->value_.IntVal;
            break;
        case FieldType::BIGINT:
            sstream << impl_->value_.BigIntVal;
            break;
        case FieldType::FLOAT:
            sstream << impl_->value_.FloatVal;
            break;
        case FieldType::DOUBLE:
            sstream << impl_->value_.DoubleVal;
            break;
        case FieldType::STRING:
        case FieldType::BINARY:
            str.assign(impl_->value_.Buffer, impl_->size_);
            sstream << str;
            break;
        case FieldType::TIMESTAMP:
            sstream << impl_->value_.TimestampVal.ToString();
            break;
        case FieldType::DECIMAL:
            sstream << impl_->value_.DecimalVal.ToString();
            break;
        case FieldType::DATE:
            sstream << impl_->value_.IntVal;
            break;
        default:
            sstream << "unknown type id:" << FieldTypeUtils::FieldTypeToString(impl_->type_);
    }
    return sstream.str();
}

Result<int32_t> Literal::CompareTo(const Literal& other) const {
    if (this == &other) {
        return 0;
    }
    // TODO(xinyu.lxy): compare with BIGINT and FLOAT/ INT and BIGINT
    if (impl_->type_ != other.impl_->type_) {
        return Status::Invalid(
            fmt::format("cannot compare with different type [{}: {}], [{}: {}]", ToString(),
                        FieldTypeUtils::FieldTypeToString(impl_->type_), other.ToString(),
                        FieldTypeUtils::FieldTypeToString(other.impl_->type_)));
    }
    if (impl_->is_null_ && other.impl_->is_null_) {
        return 0;
    }
    if (impl_->is_null_ || other.impl_->is_null_) {
        return Status::Invalid("cannot compare with null");
    }
    switch (impl_->type_) {
        case FieldType::BOOLEAN:
            return impl_->value_.BooleanVal == other.impl_->value_.BooleanVal
                       ? 0
                       : ((impl_->value_.BooleanVal < other.impl_->value_.BooleanVal) ? -1 : 1);
        case FieldType::TINYINT:
            return impl_->value_.TinyIntVal == other.impl_->value_.TinyIntVal
                       ? 0
                       : ((impl_->value_.TinyIntVal < other.impl_->value_.TinyIntVal) ? -1 : 1);
        case FieldType::SMALLINT:
            return impl_->value_.SmallIntVal == other.impl_->value_.SmallIntVal
                       ? 0
                       : ((impl_->value_.SmallIntVal < other.impl_->value_.SmallIntVal) ? -1 : 1);
        case FieldType::INT:
            return impl_->value_.IntVal == other.impl_->value_.IntVal
                       ? 0
                       : ((impl_->value_.IntVal < other.impl_->value_.IntVal) ? -1 : 1);
        case FieldType::BIGINT:
            return impl_->value_.BigIntVal == other.impl_->value_.BigIntVal
                       ? 0
                       : ((impl_->value_.BigIntVal < other.impl_->value_.BigIntVal) ? -1 : 1);
        case FieldType::FLOAT:
            return FieldsComparator::CompareFloatingPoint(impl_->value_.FloatVal,
                                                          other.impl_->value_.FloatVal);
        case FieldType::DOUBLE:
            return FieldsComparator::CompareFloatingPoint(impl_->value_.DoubleVal,
                                                          other.impl_->value_.DoubleVal);
        case FieldType::STRING:
        case FieldType::BINARY: {
            std::string_view v1(impl_->value_.Buffer, impl_->size_);
            std::string_view v2(other.impl_->value_.Buffer, other.impl_->size_);
            return (*this == other) ? 0 : (v1 < v2 ? -1 : 1);
        }
        case FieldType::TIMESTAMP:
            return impl_->value_.TimestampVal == other.impl_->value_.TimestampVal
                       ? 0
                       : (impl_->value_.TimestampVal < other.impl_->value_.TimestampVal ? -1 : 1);
        case FieldType::DECIMAL:
            return impl_->value_.DecimalVal.CompareTo(other.impl_->value_.DecimalVal);
        case FieldType::DATE:
            return impl_->value_.IntVal == other.impl_->value_.IntVal
                       ? 0
                       : ((impl_->value_.IntVal < other.impl_->value_.IntVal) ? -1 : 1);
        default:
            return Status::Invalid(fmt::format("unsupported type {}",
                                               FieldTypeUtils::FieldTypeToString(impl_->type_)));
    }
}

bool Literal::operator==(const Literal& other) const {
    if (this == &other) {
        return true;
    }
    if (GetType() != other.GetType() || IsNull() != other.IsNull()) {
        return false;
    }
    if (IsNull()) {
        return true;
    }
    if (GetType() != FieldType::FLOAT && GetType() != FieldType::DOUBLE &&
        HashCode() != other.HashCode()) {
        return false;
    }
    switch (GetType()) {
        case FieldType::BOOLEAN:
            return impl_->value_.BooleanVal == other.impl_->value_.BooleanVal;
        case FieldType::TINYINT:
            return impl_->value_.TinyIntVal == other.impl_->value_.TinyIntVal;
        case FieldType::SMALLINT:
            return impl_->value_.SmallIntVal == other.impl_->value_.SmallIntVal;
        case FieldType::INT:
            return impl_->value_.IntVal == other.impl_->value_.IntVal;
        case FieldType::BIGINT:
            return impl_->value_.BigIntVal == other.impl_->value_.BigIntVal;
        case FieldType::FLOAT: {
            if (std::isnan(impl_->value_.FloatVal) && std::isnan(other.impl_->value_.FloatVal)) {
                return true;
            }
            if (impl_->value_.FloatVal == INFINITY && other.impl_->value_.FloatVal == INFINITY) {
                return true;
            }
            if (impl_->value_.FloatVal == -INFINITY && other.impl_->value_.FloatVal == -INFINITY) {
                return true;
            }
            return std::fabs(impl_->value_.FloatVal - other.impl_->value_.FloatVal) < 1E-5;
        }
        case FieldType::DOUBLE: {
            if (std::isnan(impl_->value_.DoubleVal) && std::isnan(other.impl_->value_.DoubleVal)) {
                return true;
            }
            if (impl_->value_.DoubleVal == INFINITY && other.impl_->value_.DoubleVal == INFINITY) {
                return true;
            }
            if (impl_->value_.DoubleVal == -INFINITY &&
                other.impl_->value_.DoubleVal == -INFINITY) {
                return true;
            }
            return std::fabs(impl_->value_.DoubleVal - other.impl_->value_.DoubleVal) < 1E-5;
        }
        case FieldType::STRING:
        case FieldType::BINARY:
            return impl_->size_ == other.impl_->size_ &&
                   memcmp(impl_->value_.Buffer, other.impl_->value_.Buffer, impl_->size_) == 0;
        case FieldType::TIMESTAMP:
            return impl_->value_.TimestampVal == other.impl_->value_.TimestampVal;
        case FieldType::DECIMAL:
            return impl_->value_.DecimalVal == other.impl_->value_.DecimalVal;
        case FieldType::DATE:
            return impl_->value_.IntVal == other.impl_->value_.IntVal;
        default:
            return false;
    }
}

bool Literal::operator!=(const Literal& r) const {
    return !(*this == r);
}

template <typename T>
T Literal::GetValue() const {
    if constexpr (std::is_same_v<T, bool>) {
        return impl_->value_.BooleanVal;
    } else if constexpr (std::is_same_v<T, int8_t>) {
        return impl_->value_.TinyIntVal;
    } else if constexpr (std::is_same_v<T, int16_t>) {
        return impl_->value_.SmallIntVal;
    } else if constexpr (std::is_same_v<T, int32_t>) {
        return impl_->value_.IntVal;
    } else if constexpr (std::is_same_v<T, int64_t>) {
        return impl_->value_.BigIntVal;
    } else if constexpr (std::is_same_v<T, float>) {
        return impl_->value_.FloatVal;
    } else if constexpr (std::is_same_v<T, double>) {
        return impl_->value_.DoubleVal;
    } else if constexpr (std::is_same_v<T, std::string>) {
        return std::string(impl_->value_.Buffer, impl_->size_);
    } else if constexpr (std::is_same_v<T, Timestamp>) {
        return impl_->value_.TimestampVal;
    } else if constexpr (std::is_same_v<T, Decimal>) {
        return impl_->value_.DecimalVal;
    } else {
        return T();
    }
}

template Literal::Literal(const bool&);
template Literal::Literal(const int8_t&);
template Literal::Literal(const int16_t&);
template Literal::Literal(const int32_t&);
template Literal::Literal(const int64_t&);
template Literal::Literal(const float&);
template Literal::Literal(const double&);
template Literal::Literal(const Timestamp&);
template Literal::Literal(const Decimal&);

template bool Literal::GetValue() const;
template int8_t Literal::GetValue() const;
template int16_t Literal::GetValue() const;
template int32_t Literal::GetValue() const;
template int64_t Literal::GetValue() const;
template float Literal::GetValue() const;
template double Literal::GetValue() const;
template std::string Literal::GetValue() const;
template Timestamp Literal::GetValue() const;
template Decimal Literal::GetValue() const;
}  // namespace paimon
