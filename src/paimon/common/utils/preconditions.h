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

#include <string>
#include <utility>

#include "fmt/base.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/status.h"

namespace paimon {
/// A collection of static utility methods to validate input.
///
/// This class is modelled after Google Guava's Preconditions class and partly
/// takes code from that class to reduce external dependencies.
class Preconditions {
 public:
    Preconditions() = delete;

 public:
    /// Checks the given boolean condition, and return Status::Invalid() if
    /// the condition is not met (evaluates to `false`).
    ///
    /// @param condition The condition to check
    /// @param format_str The message template for the status
    /// if the check fails. The template substitutes its `%%s`
    /// placeholders with the error message arguments.
    /// @param args The arguments for the error message, to be
    /// inserted into the message template for the `%%s` placeholders.
    /// @return Status::Invalid(), if the condition is violated.
    template <typename... Args>
    static Status CheckState(bool condition, const std::string& format_str, Args&&... args) {
        if (!condition) {
            return Status::Invalid(
                fmt::format(fmt::runtime(format_str), std::forward<Args>(args)...));
        }
        return Status::OK();
    }

    /// Ensures that the given object reference is not null. Upon violation,
    /// Status::Invalid() with no message is returned.
    ///
    /// @param reference The object reference
    /// @return Status::Invalid(), if the passed reference was null.
    template <typename T>
    static Status CheckNotNull(const T& reference) {
        if (!reference) {
            return Status::Invalid("check null failed");
        }
        return Status::OK();
    }
    /// Ensures that the given object reference is not null. Upon violation,
    /// Status::Invalid() with the given message is returned.
    ///
    /// The error message is constructed from a template and an arguments
    /// array, after a similar fashion as fmt::format(String,
    /// Object...)}, but supporting only `%%s` as a placeholder.
    ///
    /// @param format_str The message template for the
    /// Status::Invalid() that is thrown if the check fails. The template
    /// substitutes its `%%s` placeholders with the error message arguments.
    /// @param args The arguments for the error message, to be
    /// inserted into the message template for the `%%s` placeholders.
    /// @return Status::Invalid() returned, if the passed reference was null.
    template <typename T, typename... Args>
    static Status CheckNotNull(const T& reference, const std::string& format_str, Args&&... args) {
        if (!reference) {
            return Status::Invalid(
                fmt::format(fmt::runtime(format_str), std::forward<Args>(args)...));
        }
        return Status::OK();
    }

    static Status CheckArgument(bool condition) {
        if (!condition) {
            return Status::Invalid("invalid argument");
        }
        return Status::OK();
    }
};
}  // namespace paimon
