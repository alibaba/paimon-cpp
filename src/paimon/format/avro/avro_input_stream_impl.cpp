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

// Adapted from Apache Iceberg C++
// https://github.com/apache/iceberg-cpp/blob/main/src/iceberg/avro/avro_stream_internal.cc

#include "paimon/format/avro/avro_input_stream_impl.h"

#include <algorithm>
#include <memory>
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
      in_(input_stream),
      buffer_size_(buffer_size),
      total_length_(total_length),
      buffer_(reinterpret_cast<uint8_t*>(pool_->Malloc(buffer_size))) {}

AvroInputStreamImpl::~AvroInputStreamImpl() {
    pool_->Free(buffer_, buffer_size_);
}

bool AvroInputStreamImpl::next(const uint8_t** data, size_t* len) {
    // Return all unconsumed data in the buffer
    if (buffer_pos_ < available_bytes_) {
        *data = buffer_ + buffer_pos_;
        *len = available_bytes_ - buffer_pos_;
        byte_count_ += available_bytes_ - buffer_pos_;
        buffer_pos_ = available_bytes_;
        return true;
    }

    // Read from the input stream when the buffer is empty
    uint64_t remaining = total_length_ - stream_pos_;
    if (remaining == 0) {
        return false;  // eof
    }
    auto read_length =
        in_->Read(reinterpret_cast<char*>(buffer_), std::min(buffer_size_, remaining));
    if (!read_length.ok()) {
        throw ::avro::Exception("Read failed: {}", read_length.status().ToString());
    }
    available_bytes_ = read_length.value();
    stream_pos_ += available_bytes_;
    buffer_pos_ = 0;

    // Return the whole buffer
    *data = buffer_;
    *len = available_bytes_;
    byte_count_ += available_bytes_;
    buffer_pos_ = available_bytes_;

    return true;
}

void AvroInputStreamImpl::backup(size_t len) {
    if (len > buffer_pos_) {
        throw ::avro::Exception("Cannot backup {} bytes, only {} bytes available", len,
                                buffer_pos_);
    }

    buffer_pos_ -= len;
    byte_count_ -= len;
}

void AvroInputStreamImpl::skip(size_t len) {
    // The range to skip is within the buffer
    if (buffer_pos_ + len <= available_bytes_) {
        buffer_pos_ += len;
        byte_count_ += len;
        return;
    }
    seek(byte_count_ + len);
}

size_t AvroInputStreamImpl::byteCount() const {
    return byte_count_;
}

void AvroInputStreamImpl::seek(int64_t position) {
    if (static_cast<uint64_t>(position) > total_length_) {
        throw ::avro::Exception("Cannot seek to {}, total length is {}", position, total_length_);
    }
    auto status = in_->Seek(position, SeekOrigin::FS_SEEK_SET);
    if (!status.ok()) {
        throw ::avro::Exception("Failed to seek to {}, got {}", position, status.ToString());
    }

    stream_pos_ = position;
    buffer_pos_ = 0;
    available_bytes_ = 0;
    byte_count_ = position;
}

}  // namespace paimon::avro
