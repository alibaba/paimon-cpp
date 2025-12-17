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

#include "paimon/common/io/data_output_stream.h"

#include <cassert>
#include <type_traits>

#include "fmt/format.h"
#include "paimon/common/utils/math.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/result.h"

namespace paimon {
DataOutputStream::DataOutputStream(const std::shared_ptr<OutputStream>& output_stream)
    : output_stream_(output_stream) {
    assert(output_stream_);
}
template <typename T>
Status DataOutputStream::WriteValue(const T& value) {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
    T write_value = value;
    if (NeedSwap()) {
        write_value = EndianSwapValue(value);
    }
    int32_t write_length = sizeof(T);
    PAIMON_ASSIGN_OR_RAISE(
        int32_t actual_write_length,
        output_stream_->Write(reinterpret_cast<char*>(&write_value), write_length));
    PAIMON_RETURN_NOT_OK(AssertWriteLength(write_length, actual_write_length));
    return Status::OK();
}

Status DataOutputStream::WriteBytes(const std::shared_ptr<Bytes>& bytes) {
    int32_t write_length = bytes->size();
    PAIMON_ASSIGN_OR_RAISE(int32_t actual_write_length,
                           output_stream_->Write(bytes->data(), write_length));
    PAIMON_RETURN_NOT_OK(AssertWriteLength(write_length, actual_write_length));
    return Status::OK();
}

Status DataOutputStream::WriteString(const std::string& value) {
    uint16_t write_length = value.size();
    PAIMON_RETURN_NOT_OK(WriteValue<uint16_t>(write_length));
    PAIMON_ASSIGN_OR_RAISE(int32_t actual_write_length,
                           output_stream_->Write(value.data(), write_length));
    PAIMON_RETURN_NOT_OK(AssertWriteLength(write_length, actual_write_length));
    return Status::OK();
}

Status DataOutputStream::AssertWriteLength(int32_t write_length,
                                           int32_t actual_write_length) const {
    if (write_length != actual_write_length) {
        return Status::Invalid(fmt::format(
            "assert write length failed: write length not match, write length {}, actual "
            "write length {}",
            write_length, actual_write_length));
    }
    return Status::OK();
}

bool DataOutputStream::NeedSwap() const {
    return SystemByteOrder() != byte_order_;
}

template Status DataOutputStream::WriteValue(const bool&);
template Status DataOutputStream::WriteValue(const char&);
template Status DataOutputStream::WriteValue(const int16_t&);
template Status DataOutputStream::WriteValue(const uint16_t&);
template Status DataOutputStream::WriteValue(const int32_t&);
template Status DataOutputStream::WriteValue(const int64_t&);
}  // namespace paimon
