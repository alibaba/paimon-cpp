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

#include "paimon/common/io/offset_input_stream.h"

#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/math.h"
#include "paimon/macros.h"

namespace paimon {
Result<std::unique_ptr<OffsetInputStream>> OffsetInputStream::Create(
    const std::shared_ptr<InputStream>& wrapped, int64_t length, int64_t offset) {
    if (PAIMON_UNLIKELY(wrapped == nullptr)) {
        return Status::Invalid("input stream is null pointer");
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t total_length, wrapped->Length());
    return Create(wrapped, length, offset, total_length);
}

Result<std::unique_ptr<OffsetInputStream>> OffsetInputStream::Create(
    const std::shared_ptr<InputStream>& wrapped, int64_t length, int64_t offset,
    int64_t total_length) {
    if (PAIMON_UNLIKELY(wrapped == nullptr)) {
        return Status::Invalid("input stream is null pointer");
    }
    PAIMON_RETURN_NOT_OK(ValidateValueNonNegative(offset, "offset"));
    PAIMON_RETURN_NOT_OK(ValidateValueNonNegative(length, "length"));
    PAIMON_RETURN_NOT_OK(ValidateValueNonNegative(total_length, "total length"));
    if (PAIMON_UNLIKELY(offset > total_length)) {
        return Status::Invalid(
            fmt::format("offset {} exceed total length {}", offset, total_length));
    }
    if (PAIMON_UNLIKELY(length > total_length - offset)) {
        return Status::Invalid(fmt::format("offset {} + length {} exceed total length {}", offset,
                                           length, total_length));
    }
    PAIMON_RETURN_NOT_OK(wrapped->Seek(offset, SeekOrigin::FS_SEEK_SET));
    return std::unique_ptr<OffsetInputStream>(new OffsetInputStream(wrapped, length, offset));
}

OffsetInputStream::OffsetInputStream(const std::shared_ptr<InputStream>& wrapped, int64_t length,
                                     int64_t offset)
    : wrapped_(wrapped), length_(length), offset_(offset) {}

Status OffsetInputStream::Seek(int64_t offset, SeekOrigin origin) {
    int64_t new_position = 0;
    switch (origin) {
        case SeekOrigin::FS_SEEK_SET: {
            new_position = offset;
            PAIMON_RETURN_NOT_OK(AssertBoundary(new_position));
            PAIMON_RETURN_NOT_OK(wrapped_->Seek(offset_ + new_position, SeekOrigin::FS_SEEK_SET));
            break;
        }
        case SeekOrigin::FS_SEEK_CUR: {
            new_position = inner_position_ + offset;
            PAIMON_RETURN_NOT_OK(AssertBoundary(new_position));
            PAIMON_RETURN_NOT_OK(wrapped_->Seek(offset, SeekOrigin::FS_SEEK_CUR));
            break;
        }
        case SeekOrigin::FS_SEEK_END: {
            new_position = length_ + offset;
            PAIMON_RETURN_NOT_OK(AssertBoundary(new_position));
            PAIMON_RETURN_NOT_OK(wrapped_->Seek(offset_ + new_position, SeekOrigin::FS_SEEK_SET));
            break;
        }
        default:
            return Status::Invalid(
                "invalid SeekOrigin, only support FS_SEEK_SET, FS_SEEK_CUR, and FS_SEEK_END");
    }
    inner_position_ = new_position;
    return Status::OK();
}

Result<int64_t> OffsetInputStream::Read(char* buffer, int64_t size) {
    PAIMON_RETURN_NOT_OK(AssertBoundary(inner_position_ + size));
    PAIMON_ASSIGN_OR_RAISE(int64_t actual_read_len, wrapped_->Read(buffer, size));
    inner_position_ += actual_read_len;
    return actual_read_len;
}

Result<int64_t> OffsetInputStream::Read(char* buffer, int64_t size, int64_t offset) {
    PAIMON_RETURN_NOT_OK(AssertBoundary(offset));
    PAIMON_RETURN_NOT_OK(AssertBoundary(offset + size));
    return wrapped_->Read(buffer, size, offset_ + offset);
}

void OffsetInputStream::ReadAsync(char* buffer, int64_t size, int64_t offset,
                                  std::function<void(Status)>&& callback) {
    auto status = AssertBoundary(offset);
    if (!status.ok()) {
        callback(status);
        return;
    }
    status = AssertBoundary(offset + size);
    if (!status.ok()) {
        callback(status);
        return;
    }
    wrapped_->ReadAsync(buffer, size, offset_ + offset, std::move(callback));
}

Status OffsetInputStream::Close() {
    return wrapped_->Close();
}

Result<std::string> OffsetInputStream::GetUri() const {
    return wrapped_->GetUri();
}

Result<int64_t> OffsetInputStream::GetPos() const {
    return inner_position_;
}

Result<int64_t> OffsetInputStream::Length() const {
    return length_;
}

Status OffsetInputStream::AssertBoundary(int64_t inner_pos) const {
    if (inner_pos < 0 || inner_pos > length_) {
        return Status::Invalid(
            fmt::format("OffsetInputStream assert boundary failed: inner pos {} exceed length {}",
                        inner_pos, length_));
    }
    return Status::OK();
}

}  // namespace paimon
