/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0.
 *
 * Translation layer: paimon_tantivy_status_t -> paimon::Status.
 * See docs/dev/tantivy_ffi_design.md §2.
 */
#pragma once

#include "fmt/format.h"
#include "paimon/status.h"

extern "C" {
#include "paimon_tantivy_ffi.h"
}

namespace paimon::tantivy {

/// Translate an FFI status code to a paimon::Status. OK returns Status::OK().
/// On error, the returned Status carries the thread-local last_error() text
/// prefixed with the status code name for easier grep.
///
/// Note: cbindgen emits `PaimonTantivyStatus` in the **global** namespace as
/// a C-style enum, so we accept it via its global type here. C++ ADL still
/// lets call sites write the unqualified enumerator names.
inline Status FfiStatusToStatus(::PaimonTantivyStatus code) {
    if (code == PAIMON_TANTIVY_STATUS_OK) {
        return Status::OK();
    }
    const char* err = paimon_tantivy_last_error();
    const char* name = [code]() -> const char* {
        switch (code) {
            case PAIMON_TANTIVY_STATUS_INVALID_ARGUMENT:
                return "InvalidArgument";
            case PAIMON_TANTIVY_STATUS_NOT_FOUND:
                return "NotFound";
            case PAIMON_TANTIVY_STATUS_IO_ERROR:
                return "IoError";
            case PAIMON_TANTIVY_STATUS_UNSUPPORTED:
                return "Unsupported";
            case PAIMON_TANTIVY_STATUS_TOKENIZER_ERROR:
                return "TokenizerError";
            case PAIMON_TANTIVY_STATUS_QUERY_PARSE_ERROR:
                return "QueryParseError";
            case PAIMON_TANTIVY_STATUS_INDEX_FORMAT_ERROR:
                return "IndexFormatError";
            case PAIMON_TANTIVY_STATUS_INTERNAL_ERROR:
                return "InternalError";
            default:
                return "UnknownFfiStatus";
        }
    }();
    std::string msg = fmt::format("tantivy-ffi[{}({})]: {}", name, static_cast<int>(code),
                                  err ? err : "(null)");
    switch (code) {
        case PAIMON_TANTIVY_STATUS_NOT_FOUND:
            return Status::NotExist(msg);
        case PAIMON_TANTIVY_STATUS_IO_ERROR:
            return Status::IOError(msg);
        case PAIMON_TANTIVY_STATUS_UNSUPPORTED:
            return Status::NotImplemented(msg);
        case PAIMON_TANTIVY_STATUS_INVALID_ARGUMENT:
        case PAIMON_TANTIVY_STATUS_TOKENIZER_ERROR:
        case PAIMON_TANTIVY_STATUS_QUERY_PARSE_ERROR:
        case PAIMON_TANTIVY_STATUS_INDEX_FORMAT_ERROR:
            return Status::Invalid(msg);
        default:
            return Status::UnknownError(msg);
    }
}

/// Like PAIMON_RETURN_NOT_OK but for FFI calls returning PaimonTantivyStatus.
#define PAIMON_TANTIVY_RETURN_NOT_OK(expr)                                                  \
    do {                                                                                    \
        ::PaimonTantivyStatus _paimon_tantivy_status_ = (expr);                             \
        if (_paimon_tantivy_status_ != PAIMON_TANTIVY_STATUS_OK) {                          \
            return ::paimon::tantivy::FfiStatusToStatus(_paimon_tantivy_status_);           \
        }                                                                                   \
    } while (0)

}  // namespace paimon::tantivy
