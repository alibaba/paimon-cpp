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

#include "paimon/io/buffered_input_stream.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <utility>

#include "fmt/format.h"
#include "paimon/memory/bytes.h"

namespace paimon {
class MemoryPool;

BufferedInputStream::BufferedInputStream(const std::shared_ptr<InputStream>& in,
                                         int32_t buffer_size, MemoryPool* pool)
    : buffer_size_(buffer_size), in_(in) {
    assert(buffer_size > 0);
    buffer_ = std::make_unique<Bytes>(buffer_size, pool);
}

BufferedInputStream::~BufferedInputStream() noexcept = default;

Status BufferedInputStream::Seek(int64_t offset, SeekOrigin origin) {
    // Convert all seek origins to an absolute offset so the buffer-hit fast
    // path below can work uniformly on absolute positions.
    int64_t target_abs_offset = offset;
    if (origin == SeekOrigin::FS_SEEK_CUR) {
        PAIMON_ASSIGN_OR_RAISE(int64_t cur_pos, GetPos());
        target_abs_offset = cur_pos + offset;
    } else if (origin == SeekOrigin::FS_SEEK_END) {
        PAIMON_ASSIGN_OR_RAISE(int64_t length, in_->Length());
        target_abs_offset = length + offset;
    }
    // else: FS_SEEK_SET — target_abs_offset is already absolute.

    // Fast path: if the new absolute offset still falls into the bytes already
    // cached in buffer_ (i.e. the window from buf_start_abs to buf_end_abs), just
    // adjust pos_ without touching the underlying stream.
    if (count_ > 0) {
        PAIMON_ASSIGN_OR_RAISE(int64_t in_pos, in_->GetPos());
        const int64_t buf_start_abs = in_pos - count_;
        const int64_t buf_end_abs = in_pos;
        if (target_abs_offset >= buf_start_abs && target_abs_offset <= buf_end_abs) {
            pos_ = static_cast<int32_t>(target_abs_offset - buf_start_abs);
            return Status::OK();
        }
    }

    // Slow path: the target is outside the current buffer window, fall back to
    // a real seek on the underlying stream and invalidate the buffer.
    PAIMON_RETURN_NOT_OK(in_->Seek(target_abs_offset, FS_SEEK_SET));
    pos_ = 0;
    count_ = 0;
    return Status::OK();
}

Result<int64_t> BufferedInputStream::GetPos() const {
    PAIMON_ASSIGN_OR_RAISE(int64_t in_pos, in_->GetPos());
    return in_pos - count_ + pos_;
}

Result<int32_t> BufferedInputStream::Read(char* buffer, uint32_t size) {
    uint32_t actual_read_len = 0;
    while (actual_read_len < size) {
        PAIMON_ASSIGN_OR_RAISE(int32_t nread,
                               InnerRead(buffer + actual_read_len, size - actual_read_len));
        assert(nread > 0);
        actual_read_len += nread;
    }
    PAIMON_RETURN_NOT_OK(AssertReadLength(size, actual_read_len));
    return actual_read_len;
}

Result<int32_t> BufferedInputStream::Read(char* buffer, uint32_t size, uint64_t offset) {
    return Status::Invalid("BufferedInputStream does not support Read from offset");
}

void BufferedInputStream::ReadAsync(char* buffer, uint32_t size, uint64_t offset,
                                    std::function<void(Status)>&& callback) {
    callback(Status::NotImplemented("BufferedInputStream do not support ReadAsync"));
}

Result<uint64_t> BufferedInputStream::Length() const {
    return in_->Length();
}

Status BufferedInputStream::Close() {
    pos_ = 0;
    count_ = 0;
    buffer_.reset();
    return Status::OK();
}

Result<std::string> BufferedInputStream::GetUri() const {
    return in_->GetUri();
}

Status BufferedInputStream::Fill() {
    pos_ = 0;
    count_ = 0;
    PAIMON_ASSIGN_OR_RAISE(int64_t in_pos, in_->GetPos());
    PAIMON_ASSIGN_OR_RAISE(int64_t length, in_->Length());
    int64_t left_to_read = std::min((length - in_pos), static_cast<int64_t>(buffer_size_));
    PAIMON_ASSIGN_OR_RAISE(int32_t actual_read_len, in_->Read(buffer_->data(), left_to_read));
    PAIMON_RETURN_NOT_OK(AssertReadLength(left_to_read, actual_read_len));
    count_ = actual_read_len;
    return Status::OK();
}

Result<int32_t> BufferedInputStream::InnerRead(char* buffer, int32_t size) {
    assert(size > 0);
    int32_t avail = count_ - pos_;
    if (avail <= 0) {
        assert(avail == 0);
        /* If the requested length is at least as large as the buffer, and
           if there is no mark/reset activity, do not bother to copy the
           bytes into the local buffer.  In this way buffered streams will
           cascade harmlessly. */
        if (size >= buffer_size_) {
            return in_->Read(buffer, size);
        }
        PAIMON_RETURN_NOT_OK(Fill());
        avail = count_ - pos_;
        if (avail <= 0) {
            return Status::Invalid(fmt::format(
                "InnerRead failed, after Fill(), still no bytes available (may read eof), but "
                "expect read {} bytes",
                size));
        }
    }
    int32_t copy_length = std::min(avail, size);
    memcpy(buffer, buffer_->data() + pos_, copy_length);
    pos_ += copy_length;
    return copy_length;
}

Status BufferedInputStream::AssertReadLength(int32_t read_length,
                                             int32_t actual_read_length) const {
    if (read_length != actual_read_length) {
        return Status::Invalid(
            fmt::format("assert read length failed: read length not match, read length {}, actual "
                        "read length {}",
                        read_length, actual_read_length));
    }
    return Status::OK();
}

}  // namespace paimon
