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
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

namespace paimon {
class Bytes;
class InputStream;

/// `DataInputStream` provides a convenient wrapper around `InputStream` for reading typed data.
/// @note The default byte order is big-endian to maintain compatibility with the Java
/// implementation.
class PAIMON_EXPORT DataInputStream {
 public:
    /// Constructs a `DataInputStream` wrapping the given `InputStream`.
    /// @param input_stream The underlying input stream to read from.
    explicit DataInputStream(const std::shared_ptr<InputStream>& input_stream);

    /// Seek to a specific position in the underlying input stream.
    /// @param offset The absolute byte offset to seek to.
    Status Seek(int64_t offset) const;

    /// Read a typed value from the stream.
    /// @return Result containing the read value or an error status.
    template <typename T>
    Result<T> ReadValue() const;

    /// Read some bytes to a `Bytes` object from the stream. The length of bytes is the number of
    /// bytes read from the stream.
    /// @param bytes Buffer to store the read bytes.
    Status ReadBytes(Bytes* bytes) const;

    /// Read raw data of specified size from the stream.
    /// @param data Buffer to store the read data.
    /// @param size Number of bytes to read.
    Status Read(char* data, int64_t size) const;

    /// Read string from the stream.
    /// @note First read length (int16), then read string bytes.
    Result<std::string> ReadString() const;

    /// Get the current position in the underlying input stream.
    Result<int64_t> GetPos() const;

    /// Get the total length of the underlying input stream.
    Result<int64_t> Length() const;

    /// Set the byte order for endianness conversion.
    /// @param order The byte order to use `PAIMON_BIG_ENDIAN` or `PAIMON_LITTLE_ENDIAN`.
    void SetOrder(ByteOrder order) {
        byte_order_ = order;
    }

 private:
    /// Validate that the expected number of bytes were read.
    /// @param read_length Expected number of bytes to read.
    /// @param actual_read_length Actual number of bytes read.
    Status AssertReadLength(int64_t read_length, int64_t actual_read_length) const;

    /// Check if there are enough bytes available to read.
    /// @param need_length Number of bytes needed.
    Status AssertBoundary(int64_t need_length) const;

    /// Determine if byte swapping is needed based on current byte order and system endianness.
    /// @return `true` if byte swapping is required, `false` otherwise.
    bool NeedSwap() const;

 private:
    std::shared_ptr<InputStream> input_stream_;
    ByteOrder byte_order_ = ByteOrder::PAIMON_BIG_ENDIAN;
};
}  // namespace paimon
