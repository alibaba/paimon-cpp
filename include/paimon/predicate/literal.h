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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "paimon/defs.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
/// Literal represents a constant value used in predicate expressions.
///
/// Literal support BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, STRING, BINARY,
/// TIMESTAMP, DECIMAL, DATE.
class PAIMON_EXPORT Literal {
 public:
    /// Creates a null literal of the specified type.
    /// @param type The field type for this null literal.
    explicit Literal(FieldType type);

    /// Creates a literal from a typed value.
    /// The template parameter T must be compatible with one of the supported field types
    /// (must match a supported FieldType).
    /// T can be bool, int8_t, int16_t, int32_t, int64_t, float, double, Timestamp and Decimal.
    /// @param val The value to store in the literal.
    template <typename T>
    explicit Literal(const T& val);

    /// Creates a literal from binary data (string or binary type).
    /// The data is copied into the literal's internal storage.
    /// @param binary_type Must be either `STRING` or `BINARY` field type.
    /// @param str Pointer to the binary data.
    /// @param size Size of the binary data in bytes.
    /// @note `BLOB` type is not supported by literal
    Literal(FieldType binary_type, const char* str, size_t size);

    /// Creates a literal from binary data with optional data ownership.
    /// @param binary_type Must be either `STRING` or `BINARY` field type.
    /// @param str Pointer to the binary data.
    /// @param size Size of the binary data in bytes.
    /// @param own_data If true, the literal takes ownership and will free the data;
    ///                 if false, the caller must ensure the data remains valid.
    Literal(FieldType binary_type, const char* str, size_t size, bool own_data);

    /// Creates a date literal from an integer value.
    /// @param date_type Must be `DATE` field type.
    /// @param date_value Date value as days since epoch (1970-01-01).
    Literal(FieldType date_type, int32_t date_value);

    Literal(const Literal& other);
    ~Literal();
    Literal(Literal&& other);
    Literal& operator=(Literal&& other);
    Literal& operator=(const Literal& other);
    bool operator==(const Literal& other) const;
    bool operator!=(const Literal& other) const;

    /// Checks if this literal represents a null value.
    bool IsNull() const;

    /// Gets the typed value stored in this literal.
    /// @tparam T The expected C++ type of the value.
    /// @return The value of type `T`.
    /// @warning This method is unsafe - caller must verify the type and null status first.
    template <typename T>
    T GetValue() const;

    /// Gets the field type of this literal.
    FieldType GetType() const;

    std::string ToString() const;

    /// Gets the hash code for this literal.
    size_t HashCode() const;

    /// Compares this literal with another literal. The comparison follows SQL semantics for the
    /// respective data types.
    /// @param other The literal to compare with.
    /// @return Result containing -1 (this < other), 0 (this == other), or 1 (this > other),
    ///         or an error if the literals are not comparable.
    Result<int32_t> CompareTo(const Literal& other) const;

 private:
    class Impl;

    std::unique_ptr<Impl> impl_;
};
}  // namespace paimon

namespace std {
template <>
struct hash<::paimon::Literal> {
    size_t operator()(const ::paimon::Literal& literal) const {
        return literal.HashCode();
    }
};
}  // namespace std
