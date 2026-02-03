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

#pragma once
#include <cstdint>
#include <memory>
#include <string>

#include "paimon/io/byte_order.h"
#include "paimon/status.h"

namespace paimon {
class Bytes;
class OutputStream;

// data output stream, support WriteValue() and WriteString() from OutputStream, also do big-endian
// conversion to ensure cross-language compatibility
class PAIMON_EXPORT DataOutputStream {
 public:
    explicit DataOutputStream(const std::shared_ptr<OutputStream>& output_stream);

    template <typename T>
    Status WriteValue(const T& value);

    Status WriteBytes(const std::shared_ptr<Bytes>& bytes);

    /// First write length (int16), then write string bytes.
    Status WriteString(const std::string& value);

    void SetOrder(ByteOrder order) {
        byte_order_ = order;
    }

 private:
    Status AssertWriteLength(int32_t write_length, int32_t actual_write_length) const;

    bool NeedSwap() const;

 private:
    std::shared_ptr<OutputStream> output_stream_;

    ByteOrder byte_order_ = ByteOrder::PAIMON_BIG_ENDIAN;
};

}  // namespace paimon
