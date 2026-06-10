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
 */

#include "paimon/global_index/tantivy/tantivy_stream_ctx.h"

#include <cstring>

#include "fmt/format.h"
#include "paimon/fs/file_system.h"

namespace paimon::tantivy {

extern "C" int32_t paimon_cpp_stream_read_at(void* ctx_ptr, uint64_t offset, std::size_t len,
                                             uint8_t* out_buf) {
    if (ctx_ptr == nullptr || out_buf == nullptr) {
        return 1;
    }
    auto* ctx = static_cast<StreamCtx*>(ctx_ptr);
    std::lock_guard<std::mutex> lock(ctx->pread_mu);

    std::size_t total = 0;
    while (total < len) {
        auto r = ctx->stream->Read(reinterpret_cast<char*>(out_buf + total),
                                   static_cast<uint32_t>(len - total), offset + total);
        if (!r.ok()) {
            return 1;
        }
        int32_t got = r.value();
        if (got <= 0) {
            return 1;  // unexpected EOF / 0-byte read
        }
        total += static_cast<std::size_t>(got);
    }
    return 0;
}

extern "C" void paimon_cpp_stream_release(void* ctx_ptr) {
    if (ctx_ptr == nullptr) {
        return;
    }
    auto* ctx = static_cast<StreamCtx*>(ctx_ptr);
    // ~shared_ptr closes the underlying stream.
    delete ctx;
}

extern "C" int32_t paimon_cpp_writer_push(void* ctx_ptr, const uint8_t* data, std::size_t len) {
    if (ctx_ptr == nullptr) {
        return 1;
    }
    auto* ctx = static_cast<WriteCtx*>(ctx_ptr);
    if (ctx->out == nullptr) {
        ctx->last_error = Status::Invalid("writer_push: null OutputStream");
        return 1;
    }
    std::size_t total = 0;
    while (total < len) {
        auto r = ctx->out->Write(reinterpret_cast<const char*>(data + total),
                                 static_cast<uint32_t>(len - total));
        if (!r.ok()) {
            ctx->last_error = r.status();
            return 1;
        }
        int32_t written = r.value();
        if (written <= 0) {
            ctx->last_error = Status::IOError(fmt::format(
                "writer_push: short write (wrote {} of {} bytes)", written, len - total));
            return 1;
        }
        total += static_cast<std::size_t>(written);
    }
    return 0;
}

}  // namespace paimon::tantivy
