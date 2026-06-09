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

#include "paimon/io/byte_array_input_stream.h"

#include <cassert>
#include <cstring>
#include <utility>

#include "fmt/format.h"

namespace paimon {
ByteArrayInputStream::ByteArrayInputStream(const char* buffer, int64_t length)
    : buffer_(buffer), length_(length), position_(0) {
    assert(buffer_);
    assert(length >= 0);
}

const char* ByteArrayInputStream::GetRawData() const {
    return buffer_ + position_;
}

Status ByteArrayInputStream::Seek(int64_t offset, SeekOrigin origin) {
    int64_t new_position = 0;
    switch (origin) {
        case SeekOrigin::FS_SEEK_SET: {
            new_position = offset;
            break;
        }
        case SeekOrigin::FS_SEEK_CUR: {
            new_position = position_ + offset;
            break;
        }
        case SeekOrigin::FS_SEEK_END: {
            new_position = length_ + offset;
            break;
        }
        default:
            return Status::Invalid(
                "invalid SeekOrigin, only support FS_SEEK_SET, FS_SEEK_CUR, and FS_SEEK_END");
    }
    if (new_position < 0 || new_position > length_) {
        return Status::Invalid(fmt::format("invalid seek, after seek, current pos {}, length {}",
                                           new_position, length_));
    }
    position_ = new_position;
    return Status::OK();
}

Result<int64_t> ByteArrayInputStream::Read(char* buffer, int64_t size) {
    if (size < 0 || size > length_ - position_) {
        return Status::Invalid(
            fmt::format("ByteArrayInputStream assert boundary failed: need length {}, current "
                        "position {}, exceed length {}",
                        size, position_, length_));
    }
    memcpy(buffer, buffer_ + position_, static_cast<size_t>(size));
    position_ += size;
    return size;
}

Result<int64_t> ByteArrayInputStream::Read(char* buffer, int64_t size, int64_t offset) {
    if (size < 0 || offset < 0 || offset > length_ || size > length_ - offset) {
        return Status::Invalid(
            fmt::format("ByteArrayInputStream boundary check failed: read size {}, offset {}, "
                        "stream length {}",
                        size, offset, length_));
    }
    memcpy(buffer, buffer_ + offset, static_cast<size_t>(size));
    return size;
}

void ByteArrayInputStream::ReadAsync(char* buffer, int64_t size, int64_t offset,
                                     std::function<void(Status)>&& callback) {
    Result<int64_t> read_size = Read(buffer, size, offset);
    Status status = Status::OK();
    if (read_size.ok() && read_size.value() != size) {
        status = Status::Invalid(fmt::format(
            "ByteArrayInputStream async read size {} != expected {}", read_size.value(), size));
    } else if (!read_size.ok()) {
        status = read_size.status();
    }
    callback(status);
}

Status ByteArrayInputStream::Close() {
    return Status::OK();
}

Result<std::string> ByteArrayInputStream::GetUri() const {
    return std::string();
}
}  // namespace paimon
