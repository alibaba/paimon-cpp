/*
 * Copyright 2026-present Alibaba Inc.
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
 *
 * RAII wrappers for opaque FFI handles returned by paimon_tantivy_ffi.
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

extern "C" {
#include "paimon_tantivy_ffi.h"  // NOLINT(build/include_subdir)
}

namespace paimon::tantivy {

/// Deleter template; specialize per handle type with the matching free function.
/// Usage:
///   template <> struct FfiDeleter<paimon_tantivy_writer_t> {
///       void operator()(paimon_tantivy_writer_t* p) const noexcept {
///           paimon_tantivy_writer_free(p);
///       }
///   };
///   using WriterPtr = FfiUniquePtr<paimon_tantivy_writer_t>;
template <typename Handle>
struct FfiDeleter {
    // Default unsupported so missing specializations fail at compile time
    void operator()(Handle*) const noexcept {
        static_assert(sizeof(Handle) == 0, "FfiDeleter must be specialized for this handle type");
    }
};

/// Generic RAII owning pointer for an FFI handle.
template <typename Handle>
using FfiUniquePtr = std::unique_ptr<Handle, FfiDeleter<Handle>>;

/// Tokenizer handle.
template <>
struct FfiDeleter<PaimonJiebaTokenizer> {
    void operator()(PaimonJiebaTokenizer* p) const noexcept {
        paimon_tantivy_tokenizer_free(p);
    }
};
using JiebaTokenizerPtr = FfiUniquePtr<PaimonJiebaTokenizer>;

/// Writer handle.
template <>
struct FfiDeleter<PaimonTantivyWriter> {
    void operator()(PaimonTantivyWriter* p) const noexcept {
        paimon_tantivy_writer_free(p);
    }
};
using WriterPtr = FfiUniquePtr<PaimonTantivyWriter>;

/// Reader handle.
template <>
struct FfiDeleter<PaimonTantivyReader> {
    void operator()(PaimonTantivyReader* p) const noexcept {
        paimon_tantivy_reader_free(p);
    }
};
using ReaderPtr = FfiUniquePtr<PaimonTantivyReader>;

/// Specialization: buffer_t is special - not an opaque handle but a value
/// struct owned on the stack. The contained `data` pointer is the Rust-owned
/// allocation; we call `paimon_tantivy_buffer_free` on the struct pointer.
/// Use BufferGuard to ensure free-on-scope-exit even on early return.
class BufferGuard {
 public:
    BufferGuard() noexcept {
        buf_.data = nullptr;
        buf_.len = 0;
        buf_.capacity = 0;
    }
    BufferGuard(const BufferGuard&) = delete;
    BufferGuard& operator=(const BufferGuard&) = delete;
    BufferGuard(BufferGuard&&) = delete;
    BufferGuard& operator=(BufferGuard&&) = delete;

    ~BufferGuard() noexcept {
        paimon_tantivy_buffer_free(&buf_);
    }

    PaimonTantivyBuffer* out() noexcept {
        return &buf_;
    }

    const uint8_t* data() const noexcept {
        return buf_.data;
    }
    std::size_t size() const noexcept {
        return buf_.len;
    }

 private:
    PaimonTantivyBuffer buf_{};
};

}  // namespace paimon::tantivy
