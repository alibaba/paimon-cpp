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

#include "arrow/status.h"
#include "paimon/macros.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

inline arrow::Status ToArrowStatus(const Status& status) {
    switch (status.code()) {
        case StatusCode::OK:
            return arrow::Status::OK();
        case StatusCode::OutOfMemory:
            return arrow::Status::OutOfMemory(status.message());
        case StatusCode::KeyError:
            return arrow::Status::KeyError(status.message());
        case StatusCode::TypeError:
            return arrow::Status::TypeError(status.message());
        case StatusCode::Invalid:
            return arrow::Status::Invalid(status.message());
        case StatusCode::IOError:
            return arrow::Status::IOError(status.message());
        case StatusCode::CapacityError:
            return arrow::Status::CapacityError(status.message());
        case StatusCode::IndexError:
            return arrow::Status::IndexError(status.message());
        case StatusCode::UnknownError:
            return arrow::Status::UnknownError(status.message());
        case StatusCode::NotImplemented:
            return arrow::Status::NotImplemented(status.message());
        case StatusCode::SerializationError:
            return arrow::Status::SerializationError(status.message());
        default:
            return arrow::Status::Invalid(status.message());
    }
}

inline Status ToPaimonStatus(const arrow::Status& status) {
    switch (status.code()) {
        case arrow::StatusCode::OK:
            return Status::OK();
        case arrow::StatusCode::OutOfMemory:
            return Status::OutOfMemory(status.message());
        case arrow::StatusCode::KeyError:
            return Status::KeyError(status.message());
        case arrow::StatusCode::TypeError:
            return Status::TypeError(status.message());
        case arrow::StatusCode::Invalid:
            return Status::Invalid(status.message());
        case arrow::StatusCode::IOError:
            return Status::IOError(status.message());
        case arrow::StatusCode::CapacityError:
            return Status::CapacityError(status.message());
        case arrow::StatusCode::IndexError:
            return Status::IndexError(status.message());
        case arrow::StatusCode::UnknownError:
            return Status::UnknownError(status.message());
        case arrow::StatusCode::NotImplemented:
            return Status::NotImplemented(status.message());
        case arrow::StatusCode::SerializationError:
            return Status::SerializationError(status.message());
        default:
            return Status::Invalid(status.message());
    }
}

#define PAIMON_RETURN_NOT_OK_FROM_ARROW(ARROW_STATUS) \
    do {                                              \
        arrow::Status __s = (ARROW_STATUS);           \
        if (PAIMON_UNLIKELY(!(__s).ok())) {           \
            return ToPaimonStatus(__s);               \
        }                                             \
    } while (false)

#define PAIMON_ASSIGN_OR_RAISE_IMPL_FROM_ARROW(result_name, lhs, rexpr)            \
    auto&& result_name = (rexpr);                                                  \
    PAIMON_RETURN_IF_(!(result_name).ok(), ToPaimonStatus((result_name).status()), \
                      PAIMON_STRINGIFY(rexpr));                                    \
    lhs = std::move(result_name).ValueUnsafe();

#define PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(lhs, rexpr) \
    PAIMON_ASSIGN_OR_RAISE_IMPL_FROM_ARROW(           \
        PAIMON_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, (rexpr));

}  // namespace paimon
