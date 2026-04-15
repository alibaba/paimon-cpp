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

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Adapted from Apache Arrow
// https://github.com/apache/arrow/blob/main/cpp/src/arrow/status.h

// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

#pragma once

#include <cstring>
#include <iosfwd>
#include <memory>
#include <string>
#include <utility>

#include "paimon/compare.h"
#include "paimon/macros.h"
#include "paimon/string_builder.h"
#include "paimon/visibility.h"

namespace paimon {

enum class StatusCode : char {
    OK = 0,
    OutOfMemory = 1,
    KeyError = 2,
    TypeError = 3,
    Invalid = 4,
    IOError = 5,
    CapacityError = 6,
    IndexError = 7,
    Cancelled = 8,
    UnknownError = 9,
    NotImplemented = 10,
    SerializationError = 11,
    NotExist = 12,
    Exist = 13,
};

/// \brief An opaque class that allows subsystems to retain
/// additional information inside the Status.
class PAIMON_EXPORT StatusDetail {
 public:
    virtual ~StatusDetail() = default;
    /// \brief Return a unique id for the type of the StatusDetail
    /// (effectively a poor man's substitute for RTTI).
    virtual const char* type_id() const = 0;
    /// \brief Produce a human-readable description of this status.
    virtual std::string ToString() const = 0;

    bool operator==(const StatusDetail& other) const noexcept {
        return std::string(type_id()) == other.type_id() && ToString() == other.ToString();
    }
};

/// \brief Status outcome object (success or error)
///
/// The Status object is an object holding the outcome of an operation.
/// The outcome is represented as a StatusCode, either success
/// (StatusCode::OK) or an error (any other of the StatusCode enumeration values).
///
/// Additionally, if an error occurred, a specific error message is generally
/// attached.
class PAIMON_MUST_USE_TYPE PAIMON_EXPORT Status : public util::EqualityComparable<Status>,
                                                  public util::ToStringOstreamable<Status> {
 public:
    // Create a success status.
    Status() noexcept = default;
    ~Status() noexcept {
        // PAIMON-2400: On certain compilers, splitting off the slow path improves
        // performance significantly.
        if (PAIMON_PREDICT_FALSE(state_ != nullptr)) {
            DeleteState();
        }
    }

    Status(StatusCode code, const std::string& msg);
    /// \brief Pluggable constructor for use by sub-systems.  detail cannot be null.
    Status(StatusCode code, std::string msg, std::shared_ptr<StatusDetail> detail);

    // Copy the specified status.
    inline Status(const Status& s);
    inline Status& operator=(const Status& s);

    // Move the specified status.
    inline Status(Status&& s) noexcept;
    inline Status& operator=(Status&& s) noexcept;

    inline bool Equals(const Status& s) const;

    // AND the statuses.
    inline Status operator&(const Status& s) const noexcept;
    inline Status operator&(Status&& s) const noexcept;
    inline Status& operator&=(const Status& s) noexcept;
    inline Status& operator&=(Status&& s) noexcept;

    /// Return a success status
    static Status OK() {
        return {};
    }

    template <typename... Args>
    static Status FromArgs(StatusCode code, Args&&... args) {
        return Status(code, util::StringBuilder(std::forward<Args>(args)...));
    }

    template <typename... Args>
    static Status FromDetailAndArgs(StatusCode code, std::shared_ptr<StatusDetail> detail,
                                    Args&&... args) {
        return Status(code, util::StringBuilder(std::forward<Args>(args)...), std::move(detail));
    }

    /// Return an error status for out-of-memory conditions
    template <typename... Args>
    static Status OutOfMemory(Args&&... args) {
        return Status::FromArgs(StatusCode::OutOfMemory, std::forward<Args>(args)...);
    }

    /// Return an error status for failed key lookups (e.g. column name in a table)
    template <typename... Args>
    static Status KeyError(Args&&... args) {
        return Status::FromArgs(StatusCode::KeyError, std::forward<Args>(args)...);
    }

    /// Return an error status for type errors (such as mismatching data types)
    template <typename... Args>
    static Status TypeError(Args&&... args) {
        return Status::FromArgs(StatusCode::TypeError, std::forward<Args>(args)...);
    }

    /// Return an error status for unknown errors
    template <typename... Args>
    static Status UnknownError(Args&&... args) {
        return Status::FromArgs(StatusCode::UnknownError, std::forward<Args>(args)...);
    }

    /// Return an error status when an operation or a combination of operation and
    /// data types is unimplemented
    template <typename... Args>
    static Status NotImplemented(Args&&... args) {
        return Status::FromArgs(StatusCode::NotImplemented, std::forward<Args>(args)...);
    }

    /// Return an error status for invalid data (for example a string that fails parsing)
    template <typename... Args>
    static Status Invalid(Args&&... args) {
        return Status::FromArgs(StatusCode::Invalid, std::forward<Args>(args)...);
    }

    /// Return an error status when an index is out of bounds
    template <typename... Args>
    static Status IndexError(Args&&... args) {
        return Status::FromArgs(StatusCode::IndexError, std::forward<Args>(args)...);
    }

    /// Return an error status for cancelled operation
    template <typename... Args>
    static Status Cancelled(Args&&... args) {
        return Status::FromArgs(StatusCode::Cancelled, std::forward<Args>(args)...);
    }

    /// Return an error status when a container's capacity would exceed its limits
    template <typename... Args>
    static Status CapacityError(Args&&... args) {
        return Status::FromArgs(StatusCode::CapacityError, std::forward<Args>(args)...);
    }

    /// Return an error status when some IO-related operation failed
    template <typename... Args>
    static Status IOError(Args&&... args) {
        return Status::FromArgs(StatusCode::IOError, std::forward<Args>(args)...);
    }

    /// Return an error status when some (de)serialization operation failed
    template <typename... Args>
    static Status SerializationError(Args&&... args) {
        return Status::FromArgs(StatusCode::SerializationError, std::forward<Args>(args)...);
    }

    /// Return an error status when something is not existed.
    template <typename... Args>
    static Status NotExist(Args&&... args) {
        return Status::FromArgs(StatusCode::NotExist, std::forward<Args>(args)...);
    }

    /// Return an error status when something is already existed.
    template <typename... Args>
    static Status Exist(Args&&... args) {
        return Status::FromArgs(StatusCode::Exist, std::forward<Args>(args)...);
    }

    /// Return true iff the status indicates success.
    bool ok() const {
        return (state_ == nullptr);
    }

    /// Return true iff the status indicates an out-of-memory error.
    bool IsOutOfMemory() const {
        return code() == StatusCode::OutOfMemory;
    }
    /// Return true iff the status indicates a key lookup error.
    bool IsKeyError() const {
        return code() == StatusCode::KeyError;
    }
    /// Return true iff the status indicates invalid data.
    bool IsInvalid() const {
        return code() == StatusCode::Invalid;
    }
    /// Return true iff the status indicates a cancelled operation.
    bool IsCancelled() const {
        return code() == StatusCode::Cancelled;
    }
    /// Return true iff the status indicates an IO-related failure.
    bool IsIOError() const {
        return code() == StatusCode::IOError;
    }
    /// Return true iff the status indicates a container reaching capacity limits.
    bool IsCapacityError() const {
        return code() == StatusCode::CapacityError;
    }
    /// Return true iff the status indicates an out of bounds index.
    bool IsIndexError() const {
        return code() == StatusCode::IndexError;
    }
    /// Return true iff the status indicates a type error.
    bool IsTypeError() const {
        return code() == StatusCode::TypeError;
    }
    /// Return true iff the status indicates an unknown error.
    bool IsUnknownError() const {
        return code() == StatusCode::UnknownError;
    }
    /// Return true iff the status indicates an unimplemented operation.
    bool IsNotImplemented() const {
        return code() == StatusCode::NotImplemented;
    }
    /// Return true iff the status indicates a (de)serialization failure
    bool IsSerializationError() const {
        return code() == StatusCode::SerializationError;
    }
    /// Return true iff the status indicates a not exist error.
    bool IsNotExist() const {
        return code() == StatusCode::NotExist;
    }
    /// Return true iff the status indicates an exist error.
    bool IsExist() const {
        return code() == StatusCode::Exist;
    }

    /// \brief Return a string representation of this status suitable for printing.
    ///
    /// The string "OK" is returned for success.
    std::string ToString() const;

    /// \brief Return a string representation of the status code, without the message
    /// text or POSIX code information.
    std::string CodeAsString() const;
    static std::string CodeAsString(StatusCode);

    /// \brief Return the StatusCode value attached to this status.
    StatusCode code() const {
        return ok() ? StatusCode::OK : state_->code;
    }

    /// \brief Return the specific error message attached to this status.
    std::string message() const {
        return ok() ? "" : state_->msg;
    }

    /// \brief Return the status detail attached to this message.
    const std::shared_ptr<StatusDetail>& detail() const {
        static std::shared_ptr<StatusDetail> no_detail = nullptr;
        return state_ ? state_->detail : no_detail;
    }

    /// \brief Return a new Status copying the existing status, but
    /// updating with the existing detail.
    Status WithDetail(std::shared_ptr<StatusDetail> new_detail) const {
        return {code(), message(), std::move(new_detail)};
    }

    /// \brief Return a new Status with changed message, copying the
    /// existing status code and detail.
    template <typename... Args>
    Status WithMessage(Args&&... args) const {
        return FromArgs(code(), std::forward<Args>(args)...).WithDetail(detail());
    }

    [[noreturn]] void Abort() const;
    [[noreturn]] void Abort(const std::string& message) const;

#ifdef PAIMON_EXTRA_ERROR_CONTEXT
    void AddContextLine(const char* filename, int line, const char* function_name,
                        const char* expr);
#endif

 private:
    struct State {
        StatusCode code;
        std::string msg;
        std::shared_ptr<StatusDetail> detail;
    };
    // OK status has a `NULL` state_.  Otherwise, `state_` points to
    // a `State` structure containing the error code and message(s)
    State* state_{nullptr};

    void DeleteState() {
        delete state_;
        state_ = nullptr;
    }
    void CopyFrom(const Status& s);
    inline void MoveFrom(Status& s);
};

void Status::MoveFrom(Status& s) {
    delete state_;
    state_ = s.state_;
    s.state_ = nullptr;
}

Status::Status(const Status& s) : state_((s.state_ == nullptr) ? nullptr : new State(*s.state_)) {}

Status& Status::operator=(const Status& s) {
    // The following condition catches both aliasing (when this == &s),
    // and the common case where both s and *this are ok.
    if (state_ != s.state_) {
        CopyFrom(s);
    }
    return *this;
}

Status::Status(Status&& s) noexcept : state_(s.state_) {
    s.state_ = nullptr;
}

Status& Status::operator=(Status&& s) noexcept {
    MoveFrom(s);
    return *this;
}

bool Status::Equals(const Status& s) const {
    if (state_ == s.state_) {
        return true;
    }

    if (ok() || s.ok()) {
        return false;
    }

    if (detail() != s.detail()) {
        if ((detail() && !s.detail()) || (!detail() && s.detail())) {
            return false;
        }
        return *detail() == *s.detail();
    }

    return code() == s.code() && message() == s.message();
}

/// \cond FALSE
// (note: emits warnings on Doxygen < 1.8.15,
//  see https://github.com/doxygen/doxygen/issues/6295)
Status Status::operator&(const Status& s) const noexcept {
    if (ok()) {
        return s;
    } else {
        return *this;
    }
}

Status Status::operator&(Status&& s) const noexcept {
    if (ok()) {
        return std::move(s);
    } else {
        return *this;
    }
}

Status& Status::operator&=(const Status& s) noexcept {
    if (ok() && !s.ok()) {
        CopyFrom(s);
    }
    return *this;
}

Status& Status::operator&=(Status&& s) noexcept {
    if (ok() && !s.ok()) {
        MoveFrom(s);
    }
    return *this;
}
/// \endcond

namespace internal {

// Extract Status from Status or Result<T>
// Useful for the status check macros such as RETURN_NOT_OK.
inline Status GenericToStatus(const Status& st) {
    return st;
}
inline Status GenericToStatus(Status&& st) {
    return std::move(st);
}

}  // namespace internal

#ifdef PAIMON_EXTRA_ERROR_CONTEXT

/// \brief Return with given status if condition is met.
#define PAIMON_RETURN_IF_(condition, status, expr)                      \
    do {                                                                \
        if (PAIMON_PREDICT_FALSE(condition)) {                          \
            ::paimon::Status _st = (status);                            \
            _st.AddContextLine(__FILE__, __LINE__, __FUNCTION__, expr); \
            return _st;                                                 \
        }                                                               \
    } while (0)
#else

#define PAIMON_RETURN_IF_(condition, status, _) \
    do {                                        \
        if (PAIMON_PREDICT_FALSE(condition)) {  \
            return (status);                    \
        }                                       \
    } while (0)

#endif  // PAIMON_EXTRA_ERROR_CONTEXT

#define PAIMON_RETURN_IF(condition, status) \
    PAIMON_RETURN_IF_(condition, status, PAIMON_STRINGIFY(status))

/// \brief Propagate any non-successful Status to the caller
#define PAIMON_RETURN_NOT_OK(status)                                        \
    do {                                                                    \
        ::paimon::Status __s = ::paimon::internal::GenericToStatus(status); \
        PAIMON_RETURN_IF_(!__s.ok(), __s, PAIMON_STRINGIFY(status));        \
    } while (false)

#define PAIMON_RETURN_NOT_OK_ELSE(s, else_)                           \
    do {                                                              \
        ::paimon::Status _s = ::paimon::internal::GenericToStatus(s); \
        if (!_s.ok()) {                                               \
            else_;                                                    \
            return _s;                                                \
        }                                                             \
    } while (false)

}  // namespace paimon
