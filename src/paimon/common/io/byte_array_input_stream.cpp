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
ByteArrayInputStream::ByteArrayInputStream(const char* buffer, uint64_t length)
    : buffer_(buffer), length_(length), position_(0) {
    assert(buffer_);
}

const char* ByteArrayInputStream::GetRawData() const {
    return buffer_ + position_;
}

Status ByteArrayInputStream::Seek(int64_t offset, SeekOrigin origin) {
    switch (origin) {
        case SeekOrigin::FS_SEEK_SET: {
            position_ = offset;
            break;
        }
        case SeekOrigin::FS_SEEK_CUR: {
            position_ += offset;
            break;
        }
        case SeekOrigin::FS_SEEK_END: {
            PAIMON_ASSIGN_OR_RAISE(uint64_t length, Length());
            position_ = static_cast<int64_t>(length) + offset;
            break;
        }
        default:
            return Status::Invalid(
                "invalid SeekOrigin, only support FS_SEEK_SET, FS_SEEK_CUR, and FS_SEEK_END");
    }
    if (position_ < 0 || position_ > static_cast<int64_t>(length_)) {
        return Status::Invalid(
            fmt::format("invalid seek, after seek, current pos {}, length {}", position_, length_));
    }
    return Status::OK();
}

Result<int32_t> ByteArrayInputStream::Read(char* buffer, uint32_t size) {
    if (position_ + static_cast<int64_t>(size) > static_cast<int64_t>(length_)) {
        return Status::Invalid(
            fmt::format("ByteArrayInputStream assert boundary failed: need length {}, current "
                        "position {}, exceed length {}",
                        size, position_, length_));
    }
    memcpy(buffer, buffer_ + position_, size);
    position_ += size;
    return size;
}

Result<int32_t> ByteArrayInputStream::Read(char* buffer, uint32_t size, uint64_t offset) {
    if (offset + static_cast<uint64_t>(size) > length_) {
        return Status::Invalid(
            fmt::format("ByteArrayInputStream assert boundary failed: need length {}, read offset "
                        "{}, exceed length {}",
                        size, offset, length_));
    }
    memcpy(buffer, buffer_ + offset, size);
    return size;
}

void ByteArrayInputStream::ReadAsync(char* buffer, uint32_t size, uint64_t offset,
                                     std::function<void(Status)>&& callback) {
    Result<int32_t> read_size = Read(buffer, size, offset);
    Status status = Status::OK();
    if (read_size.ok() && static_cast<uint32_t>(read_size.value()) != size) {
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
