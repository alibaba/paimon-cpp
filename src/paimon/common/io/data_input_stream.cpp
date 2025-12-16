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

#include "paimon/io/data_input_stream.h"

#include <cassert>
#include <type_traits>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/math.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"

namespace paimon {

DataInputStream::DataInputStream(const std::shared_ptr<InputStream>& input_stream)
    : input_stream_(input_stream) {
    assert(input_stream_);
}

Status DataInputStream::Seek(int64_t offset) const {
    return input_stream_->Seek(offset, SeekOrigin::FS_SEEK_SET);
}

template <typename T>
Result<T> DataInputStream::ReadValue() const {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
    int32_t read_length = sizeof(T);
    PAIMON_RETURN_NOT_OK(AssertBoundary(read_length));
    T value;
    PAIMON_ASSIGN_OR_RAISE(int32_t actual_read_length,
                           input_stream_->Read(reinterpret_cast<char*>(&value), read_length));
    PAIMON_RETURN_NOT_OK(AssertReadLength(read_length, actual_read_length));
    if (NeedSwap()) {
        value = EndianSwapValue(value);
    }
    return value;
}

Status DataInputStream::ReadBytes(Bytes* bytes) const {
    int32_t read_length = bytes->size();
    PAIMON_RETURN_NOT_OK(AssertBoundary(read_length));
    PAIMON_ASSIGN_OR_RAISE(int32_t actual_read_length,
                           input_stream_->Read(bytes->data(), read_length));
    PAIMON_RETURN_NOT_OK(AssertReadLength(read_length, actual_read_length));
    return Status::OK();
}

Status DataInputStream::Read(char* data, uint32_t size) const {
    PAIMON_RETURN_NOT_OK(AssertBoundary(size));
    PAIMON_ASSIGN_OR_RAISE(int32_t actual_read_length, input_stream_->Read(data, size));
    PAIMON_RETURN_NOT_OK(AssertReadLength(size, actual_read_length));
    return Status::OK();
}

Result<std::string> DataInputStream::ReadString() const {
    uint16_t read_length = 0;
    PAIMON_ASSIGN_OR_RAISE(read_length, ReadValue<uint16_t>());
    PAIMON_RETURN_NOT_OK(AssertBoundary(read_length));
    std::string value(read_length, '\0');
    PAIMON_ASSIGN_OR_RAISE(int32_t actual_read_length,
                           input_stream_->Read(value.data(), read_length));
    PAIMON_RETURN_NOT_OK(AssertReadLength(read_length, actual_read_length));
    return value;
}

Result<int64_t> DataInputStream::GetPos() const {
    return input_stream_->GetPos();
}

Result<uint64_t> DataInputStream::Length() const {
    return input_stream_->Length();
}

Status DataInputStream::AssertReadLength(int32_t read_length, int32_t actual_read_length) const {
    if (read_length != actual_read_length) {
        return Status::Invalid(
            fmt::format("assert read length failed: read length not match, read length {}, actual "
                        "read length {}",
                        read_length, actual_read_length));
    }
    return Status::OK();
}

Status DataInputStream::AssertBoundary(int32_t need_length) const {
    // TODO(jinli.zjw): Store current_pos and file_length as member variables to reduce the overhead
    // of I/O calls.
    PAIMON_ASSIGN_OR_RAISE(int64_t pos, input_stream_->GetPos());
    PAIMON_ASSIGN_OR_RAISE(uint64_t length, input_stream_->Length());
    if (pos + need_length > static_cast<int64_t>(length)) {
        return Status::Invalid(
            fmt::format("DataInputStream assert boundary failed: need length {}, current position "
                        "{}, exceed length {}",
                        need_length, pos, length));
    }
    return Status::OK();
}

bool DataInputStream::NeedSwap() const {
    return SystemByteOrder() != byte_order_;
}

template Result<bool> DataInputStream::ReadValue() const;
template Result<char> DataInputStream::ReadValue() const;
template Result<int8_t> DataInputStream::ReadValue() const;
template Result<int16_t> DataInputStream::ReadValue() const;
template Result<uint16_t> DataInputStream::ReadValue() const;
template Result<int32_t> DataInputStream::ReadValue() const;
template Result<int64_t> DataInputStream::ReadValue() const;
template Result<float> DataInputStream::ReadValue() const;
}  // namespace paimon
