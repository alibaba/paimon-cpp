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
#include <functional>
#include <memory>
#include <string>

#include "paimon/fs/file_system.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

namespace paimon {
class Bytes;
class MemoryPool;

/// A buffered input stream that wraps another `InputStream` to provide buffering capabilities.
///
/// `BufferedInputStream` improves I/O performance by reducing the number of system calls
/// through internal buffering. It reads data from the underlying stream in larger chunks
/// and serves subsequent read requests from the internal buffer when possible.
class PAIMON_EXPORT BufferedInputStream : public InputStream {
 public:
    /// Creates a new buffered input stream that wraps the provided input stream.
    /// The buffer is allocated from the specified memory pool.
    ///
    /// @param in The underlying input stream to wrap.
    /// @param buffer_size Size of the internal buffer in bytes.
    /// @param pool Memory pool for buffer allocation.
    BufferedInputStream(const std::shared_ptr<InputStream>& in, int64_t buffer_size,
                        MemoryPool* pool);

    ~BufferedInputStream() noexcept override;

    Status Seek(int64_t offset, SeekOrigin origin) override;

    Result<int64_t> GetPos() const override;

    Result<int64_t> Read(char* buffer, int64_t size) override;

    Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override;

    void ReadAsync(char* buffer, int64_t size, int64_t offset,
                   std::function<void(Status)>&& callback) override;

    Result<int64_t> Length() const override;

    Status Close() override;

    Result<std::string> GetUri() const override;

    static constexpr int64_t DEFAULT_BUFFER_SIZE = 8192;

 private:
    /// Fill the internal buffer from the underlying stream.
    Status Fill();

    /// Internal read implementation.
    /// @pre size > 0
    Result<int64_t> InnerRead(char* buffer, int64_t size);

    /// Validate that the expected number of bytes were read.
    Status AssertReadLength(int64_t read_length, int64_t actual_read_length) const;

 private:
    int64_t buffer_size_;
    int64_t pos_ = 0;
    int64_t count_ = 0;
    std::unique_ptr<Bytes> buffer_;
    std::shared_ptr<InputStream> in_;
};

}  // namespace paimon
