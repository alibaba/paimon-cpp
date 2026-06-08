/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>

#include "paimon/status.h"

namespace paimon {
class InputStream;
class OutputStream;
}  // namespace paimon

namespace paimon::tantivy {

/// C++ side wrapper around a seekable InputStream, used as the `ctx` of
/// `PaimonStreamCallbacks` (V3). Lifetime is transferred to Rust via
/// `paimon_tantivy_reader_new_streaming`; Rust invokes `paimon_cpp_stream_release`
/// when the reader handle is freed, which `delete`s this struct.
///
/// `pread_mu` is a defensive per-ctx lock: the underlying `InputStream::Read(
/// buffer, size, offset)` is declared pread-style (thread-safe, no position
/// mutation) but a few subclasses (notably `JindoInputStream`) have member-
/// variable races in practice. Rust also has its own `stream_mutex` that
/// serializes reads at the Directory level; `pread_mu` is belt-and-suspenders.
struct StreamCtx {
    std::shared_ptr<InputStream> stream;
    std::mutex pread_mu;
};

/// `ctx` of `PaimonWriteCallbacks` (W1). Holds a raw (non-owning) pointer to
/// a paimon `OutputStream` plus a sticky error for conveying write failures
/// back to the C++ caller of `TantivyGlobalIndexWriter::Finish`.
struct WriteCtx {
    OutputStream* out = nullptr;
    Status last_error = Status::OK();
};

/// Rust -> C++ read callback. Reads `len` bytes starting at archive-absolute
/// `offset` into `out_buf`. Returns 0 on success, 1 on IO error. Thread-safe
/// (serialized via `StreamCtx::pread_mu`; Rust also holds its own mutex).
extern "C" int32_t paimon_cpp_stream_read_at(void* ctx_ptr, uint64_t offset, std::size_t len,
                                             uint8_t* out_buf);

/// Rust -> C++ release callback. Called exactly once when the Rust reader is
/// dropped. Deletes the ctx (which closes the underlying stream via ~shared_ptr).
extern "C" void paimon_cpp_stream_release(void* ctx_ptr);

/// Rust -> C++ write push callback. Writes `len` bytes from `data` to the
/// underlying OutputStream. Returns 0 on success, 1 on IO error (with the
/// detailed Status stashed in `WriteCtx::last_error` for the caller to pick up).
extern "C" int32_t paimon_cpp_writer_push(void* ctx_ptr, const uint8_t* data, std::size_t len);

}  // namespace paimon::tantivy
