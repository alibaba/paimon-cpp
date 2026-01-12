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

#include <utility>

#include "lumina/core/Status.h"
#include "paimon/status.h"

namespace paimon::lumina {
#define PAIMON_RETURN_NOT_OK_FROM_LUMINA(LUMINA_STATUS) \
    do {                                                \
        ::lumina::core::Status __s = (LUMINA_STATUS);   \
        if (PAIMON_UNLIKELY(!(__s).IsOk())) {           \
            return LuminaToPaimonStatus(__s);           \
        }                                               \
    } while (false)

#define PAIMON_ASSIGN_OR_RAISE_IMPL_FROM_LUMINA(result_name, lhs, rexpr)                      \
    auto&& result_name = (rexpr);                                                             \
    PAIMON_RETURN_IF_(!(result_name).IsOk(), LuminaToPaimonStatus((result_name).GetStatus()), \
                      PAIMON_STRINGIFY(rexpr));                                               \
    lhs = std::move(result_name).TakeValue();

#define PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(lhs, rexpr) \
    PAIMON_ASSIGN_OR_RAISE_IMPL_FROM_LUMINA(           \
        PAIMON_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, (rexpr));

inline ::lumina::core::Status PaimonToLuminaStatus(const Status& status) {
    switch (status.code()) {
        case StatusCode::OK:
            return ::lumina::core::Status::Ok();
        case StatusCode::OutOfMemory:
            return ::lumina::core::Status(::lumina::core::ErrorCode::OutOfMemory, status.message());
        case StatusCode::IOError:
            return ::lumina::core::Status(::lumina::core::ErrorCode::IoError, status.message());
        case StatusCode::NotImplemented:
            return ::lumina::core::Status(::lumina::core::ErrorCode::NotSupported,
                                          status.message());
        case StatusCode::NotExist:
            return ::lumina::core::Status(::lumina::core::ErrorCode::NotFound, status.message());
        case StatusCode::Exist:
            return ::lumina::core::Status(::lumina::core::ErrorCode::AlreadyExists,
                                          status.message());
        default:
            return ::lumina::core::Status(::lumina::core::ErrorCode::InvalidArgument,
                                          status.message());
    }
}

inline Status LuminaToPaimonStatus(const ::lumina::core::Status& status) {
    switch (status.Code()) {
        case ::lumina::core::ErrorCode::Ok:
            return Status::OK();
        case ::lumina::core::ErrorCode::NotFound:
            return Status::NotExist(status.Message());
        case ::lumina::core::ErrorCode::AlreadyExists:
            return Status::Exist(status.Message());
        case ::lumina::core::ErrorCode::NotSupported:
            return Status::NotImplemented(status.Message());
        case ::lumina::core::ErrorCode::IoError:
            return Status::IOError(status.Message());
        case ::lumina::core::ErrorCode::OutOfMemory:
            return Status::OutOfMemory(status.Message());
        default:
            return Status::Invalid(status.Message());
    }
}
}  // namespace paimon::lumina
