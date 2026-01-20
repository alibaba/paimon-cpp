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

#include "paimon/format/avro/avro_input_stream_impl.h"

#include <algorithm>
#include <string>
#include <utility>

#include "avro/Exception.hh"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"

namespace paimon::avro {

Result<std::unique_ptr<AvroInputStreamImpl>> AvroInputStreamImpl::Create(
    const std::shared_ptr<paimon::InputStream>& input_stream, size_t buffer_size,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(uint64_t length, input_stream->Length());
    return std::unique_ptr<AvroInputStreamImpl>(
        new AvroInputStreamImpl(input_stream, buffer_size, length, pool));
}

AvroInputStreamImpl::AvroInputStreamImpl(const std::shared_ptr<paimon::InputStream>& input_stream,
                                         size_t buffer_size, const uint64_t total_length,
                                         const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      buffer_size_(buffer_size),
      total_length_(total_length),
      buffer_(reinterpret_cast<uint8_t*>(pool_->Malloc(buffer_size))),
      in_(input_stream),
      byte_count_(0),
      next_(buffer_),
      available_(0) {}

AvroInputStreamImpl::~AvroInputStreamImpl() {
    pool_->Free(buffer_, buffer_size_);
}

bool AvroInputStreamImpl::next(const uint8_t** data, size_t* size) {
    if (available_ == 0 && !fill()) {
        return false;
    }
    *data = next_;
    *size = available_;
    next_ += available_;
    byte_count_ += available_;
    available_ = 0;
    return true;
}

void AvroInputStreamImpl::backup(size_t len) {
    next_ -= len;
    available_ += len;
    byte_count_ -= len;
}

void AvroInputStreamImpl::skip(size_t len) {
    while (len > 0) {
        if (available_ == 0) {
            auto s = in_->Seek(len, paimon::FS_SEEK_CUR);
            if (!s.ok()) {
                throw ::avro::Exception(s.ToString());
            }
            byte_count_ += len;
            total_read_len_ += len;
            return;
        }
        size_t n = std::min(available_, len);
        available_ -= n;
        next_ += n;
        len -= n;
        byte_count_ += n;
    }
}

void AvroInputStreamImpl::seek(int64_t position) {
    auto s = in_->Seek(position - byte_count_ - available_, paimon::FS_SEEK_CUR);
    if (!s.ok()) {
        throw ::avro::Exception(s.ToString());
    }
    byte_count_ = position;
    total_read_len_ = position;
    available_ = 0;
}

bool AvroInputStreamImpl::fill() {
    if (static_cast<uint64_t>(total_read_len_) >= total_length_) {
        // eof
        return false;
    }
    Result<int32_t> actual_len = in_->Read(reinterpret_cast<char*>(buffer_),
                                           std::min(buffer_size_, total_length_ - total_read_len_));
    if (!actual_len.ok()) {
        throw ::avro::Exception(actual_len.status().ToString());
    }
    total_read_len_ += actual_len.value();
    if (actual_len.value() != 0) {
        next_ = buffer_;
        available_ = actual_len.value();
        return true;
    }
    return false;
}

}  // namespace paimon::avro
