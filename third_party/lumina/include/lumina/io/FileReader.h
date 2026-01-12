/*
 * Copyright 2025-present Alibaba Inc.
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

#include <lumina/api/Options.h>
#include <lumina/core/NoCopyable.h>
#include <lumina/core/Result.h>
#include <lumina/core/Status.h>

namespace lumina::io {

class FileReader;

core::Result<std::unique_ptr<FileReader>> CreateFileReader(const api::IOOptions& ioOptions) noexcept;

class FileReader : public core::NoCopyable
{
public:
    virtual ~FileReader() = default;
    virtual core::Status Read(char* data, uint64_t size) = 0;
    virtual core::Status Close() = 0;

    template <typename T>
    core::Status ReadObj(T& val) noexcept
    {
        return this->Read(reinterpret_cast<char*>(&val), sizeof(val));
    }
    virtual core::Result<uint64_t> GetLength() const noexcept = 0;
    virtual core::Result<uint64_t> GetPosition() const noexcept = 0;
    virtual core::Status Seek(uint64_t position) noexcept = 0;
    // Ownership is transferred when a reader is passed to LuminaSearcher::Open.
    // Implementations must support Close being called before destruction.
    // Thread-safe. When using async reads, do not call any sync APIs, and Position is not guaranteed after async calls.
    virtual void ReadAsync(char* data, uint64_t size, uint64_t offset,
                           std::function<void(core::Status)> callBack) noexcept
    {
        callBack(core::Status(core::ErrorCode::InvalidArgument, "Unimplemented."));
        return;
    }

    // Zero-copy read-only view (does not affect Position; thread-safe).
    // The view is valid until the next IO operation or explicit Unpin.
    struct PeekResult {
        const void* data = nullptr;
        uint64_t size = 0;
    };
    virtual core::Result<PeekResult> Peek(uint64_t offset, uint64_t length) noexcept
    {
        return core::Result<PeekResult>::Err(core::Status(core::ErrorCode::NotSupported, "Peek not implemented"));
    }
};
} // namespace lumina::io
