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
#include <type_traits>
#include <utility>

#include "paimon/status.h"
#include "paimon/traits.h"

namespace paimon {
/// A `Result<T>` object holds either a value of type T or a `Status` object explaining
/// why such a value is not present.
template <typename T>
class PAIMON_MUST_USE_TYPE PAIMON_EXPORT Result {
 public:
    /// Destructor that properly cleans up the stored value or status.
    ~Result() {
        if (ok()) {
            data_.~T();
        }
        status_.~Status();
    }

    /// Construct a successful result with a copy of the given value.
    /// @param data The value to store in result.
    Result(const T& data) : status_(), data_(data) {}  // NOLINT(runtime/explicit)

    /// Construct a successful result by moving the given value.
    /// @param data The value to move into result.
    Result(T&& data) : status_(), data_(std::move(data)) {}  // NOLINT(runtime/explicit)

    /// Template constructor for converting compatible pointer types.
    /// Support T = std::unique_ptr<B> and U = std::unique_ptr<D> convert, where D is derived class
    /// of B also supports T = std::unique_ptr<B, AllocatorDelete>
    /// @param data The value to move into result.
    template <typename U,
              std::enable_if_t<
                  is_pointer<U>::value && is_pointer<T>::value &&
                  std::is_convertible_v<value_type_traits_t<U>, value_type_traits_t<T>>>* = nullptr>
    Result(U&& data) : status_(), data_(std::move(data)) {}  // NOLINT(runtime/explicit)

    /// Construct a failed result with the given status.
    /// @param status The status object describing the error.
    Result(const Status& status) : status_(status) {}  // NOLINT(runtime/explicit)

    /// Copy constructor.
    /// @param other The result to copy from.
    Result(const Result& other) {
        if (other.ok()) {
            MakeValue(other.data_);
        }
        MakeStatus(other.status_);
    }

    /// Move constructor.
    /// @param other The result to move from.
    Result(Result&& other) noexcept {
        if (other.ok()) {
            MakeValue(std::move(other.data_));
        }
        // If we moved the status, the other status may become ok but the other
        // value hasn't been constructed => crash on other destructor.
        MakeStatus(other.status_);
    }

    /// Template move constructor for converting compatible `Result` types.
    /// @param other The result to move from.
    template <typename U,
              std::enable_if_t<
                  is_pointer<U>::value && is_pointer<T>::value &&
                  std::is_convertible_v<value_type_traits_t<U>, value_type_traits_t<T>>>* = nullptr>
    Result(Result<U>&& other) noexcept {  // NOLINT(runtime/explicit)
        if (other.ok()) {
            MakeValue(std::move(other).value());
        }
        // If we moved the status, the other status may become ok but the other
        // value hasn't been constructed => crash on other destructor.
        MakeStatus(other.status());
    }

    /// Copy assignment operator.
    /// @param other The result to copy from.
    /// @return Reference to this result.
    Result& operator=(const Result& other) {
        if (this == &other) {
            return *this;
        }
        if (other.ok()) {
            if (ok()) {
                data_ = other.data_;
            } else {
                MakeValue(other.value());
            }
        } else {
            if (ok()) {
                data_.~T();
            }
        }
        status_ = other.status_;
        return *this;
    }

    /// Move assignment operator.
    /// @param other The result to move from.
    /// @return Reference to this result.
    Result& operator=(Result&& other) {
        if (this == &other) {
            return *this;
        }
        if (other.ok()) {
            if (ok()) {
                data_ = std::move(other.data_);
            } else {
                MakeValue(std::move(other).value());
            }
        } else {
            if (ok()) {
                data_.~T();
            }
        }
        status_ = other.status_;
        return *this;
    }

    /// Template move assignment operator for converting compatible `Result` types.
    /// @param other The result to move from.
    /// @return Reference to this result.
    template <typename U,
              std::enable_if_t<
                  is_pointer<U>::value && is_pointer<T>::value &&
                  std::is_convertible_v<value_type_traits_t<U>, value_type_traits_t<T>>>* = nullptr>
    Result& operator=(Result<U>&& other) {
        if (other.ok()) {
            if (ok()) {
                data_ = std::move(other.data_);
            } else {
                MakeValue(std::move(other).value());
            }
        } else {
            if (ok()) {
                data_.~T();
            }
        }
        status_ = other.status();
        return *this;
    }

    /// Check if this result contains a valid value (i.e., the operation succeeded).
    /// @return `true` if the result contains a valid value, `false` if it contains an error status.
    inline bool ok() const {
        return status_.ok();
    }

    /// Return the value stored in result.
    /// @return Const reference to the stored value.
    /// @note Calling this method when `ok()` is false results in undefined behavior.
    inline const T& value() const& {
        return data_;
    }

    /// Return the value stored in result.
    /// @return Rvalue reference to the stored value.
    /// @note Calling this method when `ok()` is false results in undefined behavior.
    inline T&& value() && {
        return std::move(data_);
    }

    /// Get the status of the result.
    /// @return Const reference to the status object.
    /// @note `status()` is always active, regardless of whether `ok()` is `true` or `false`.
    inline const Status& status() const {
        return status_;
    }

    /// Return the stored value if `ok()`, otherwise return the default value.
    /// @param default_value The value to return if this result contains an error.
    /// @return The stored value if `ok()`, otherwise the default_value.
    inline T value_or(T&& default_value) const& {
        if (ok()) {
            return data_;
        } else {
            return std::forward<T>(default_value);
        }
    }

    /// Return the stored value if `ok()`, otherwise return the default value (rvalue version).
    /// @param default_value The value to return if this result contains an error.
    /// @return The stored value if `ok()`, otherwise the default_value.
    inline T value_or(T&& default_value) && {
        if (ok()) {
            return std::move(data_);
        } else {
            return std::forward<T>(default_value);
        }
    }

 private:
    /// Construct the value(data_) through placement new with the passed arguments.
    /// @param args Arguments to forward to T's constructor.
    template <typename... Arg>
    void MakeValue(Arg&&... args) {
        new (&dummy_) T(std::forward<Arg>(args)...);
    }

    /// Construct the status (status_) through placement new with the passed arguments.
    /// @param args Arguments to forward to status constructor.
    template <typename... Arg>
    void MakeStatus(Arg&&... args) {
        new (&status_) Status(std::forward<Arg>(args)...);
    }

 private:
    // status_ will always be active after the constructor.
    // We make it a union to be able to initialize exactly how we need without
    // waste.
    // Eg. in the copy constructor we use the default constructor of `Status` in
    // the ok() path to avoid an extra Ref call.
    union {
        Status status_;
    };

    union {
        // When T is const, we need some non-const object we can cast to void* for
        // the placement new. dummy_ is that object.
        std::aligned_storage_t<sizeof(T), alignof(T)> dummy_;
        T data_;
    };
};

namespace internal {

/// Extract `Status` from `Result` (const lvalue reference version).
/// @param res The result to extract the status from.
/// @return Const reference to the status.
template <typename T>
inline const Status& GenericToStatus(const Result<T>& res) {
    return res.status();
}

/// Extract `Status` from `Result` (rvalue reference version).
/// @param res The result to extract the status from.
/// @return status object (moved)
template <typename T>
inline Status GenericToStatus(Result<T>&& res) {
    return std::move(res).status();
}

}  // namespace internal

#define PAIMON_ASSIGN_OR_RAISE_IMPL(result_name, lhs, rexpr)                                 \
    auto&& result_name = (rexpr);                                                            \
    PAIMON_RETURN_IF_(!(result_name).ok(), (result_name).status(), PAIMON_STRINGIFY(rexpr)); \
    lhs = std::move(result_name).value();

#define PAIMON_ASSIGN_OR_RAISE_NAME(x, y) PAIMON_CONCAT(x, y)

#define PAIMON_ASSIGN_OR_RAISE(lhs, rexpr)                                                      \
    PAIMON_ASSIGN_OR_RAISE_IMPL(PAIMON_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, \
                                (rexpr));
}  // namespace paimon
