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

#include "paimon/format/avro/avro_output_stream_impl.h"

#include <stdexcept>
#include <string>

#include "fmt/format.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon::avro {

AvroOutputStreamImpl::AvroOutputStreamImpl(const std::shared_ptr<paimon::OutputStream>& out,
                                           size_t buffer_size,
                                           const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      buffer_size_(buffer_size),
      buffer_(reinterpret_cast<uint8_t*>(pool_->Malloc(buffer_size))),
      out_(out),
      next_(buffer_),
      available_(buffer_size_),
      byte_count_(0) {}

AvroOutputStreamImpl::~AvroOutputStreamImpl() {
    pool_->Free(buffer_, buffer_size_);
}

bool AvroOutputStreamImpl::next(uint8_t** data, size_t* len) {
    if (available_ == 0) {
        FlushBuffer();
    }
    *data = next_;
    *len = available_;
    next_ += available_;
    byte_count_ += available_;
    available_ = 0;
    return true;
}

void AvroOutputStreamImpl::backup(size_t len) {
    available_ += len;
    next_ -= len;
    byte_count_ -= len;
}

void AvroOutputStreamImpl::FlushBuffer() {
    size_t length = buffer_size_ - available_;
    Result<int32_t> write_len = out_->Write(reinterpret_cast<const char*>(buffer_), length);
    if (!write_len.ok()) {
        throw std::runtime_error("write failed, status: " + write_len.status().ToString());
    }
    if (static_cast<size_t>(write_len.value()) != length) {
        throw std::runtime_error(
            fmt::format("write failed, expected length: {}, actual write length: {}", length,
                        write_len.value()));
    }
    Status status = out_->Flush();
    if (!status.ok()) {
        throw std::runtime_error("flush failed, status: " + status.ToString());
    }
    next_ = buffer_;
    available_ = buffer_size_;
}

void AvroOutputStreamImpl::flush() {
    // In avro-java impl, there is an option to control flush frequency.
    // See: https://github.com/apache/avro/commit/35750393891c40f0ceb925a852162ec764bcae6c
    //
    // However, in the avro-cpp impl, there is no such option. Calling flush() too frequently
    // generates many small I/O operations, affecting write performance, so we make
    // ::avro::OutputStream's flush() do nothing
}

}  // namespace paimon::avro
