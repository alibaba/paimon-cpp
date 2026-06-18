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
#include <memory>
#include <type_traits>

#include <lumina/api/Options.h>
#include <lumina/core/NoCopyable.h>
#include <lumina/core/Result.h>
#include <lumina/core/Status.h>

namespace lumina::io {

class FileWriter;

core::Result<std::unique_ptr<FileWriter>> CreateFileWriter(const lumina::api::IOOptions& ioOptions) noexcept;

// Contract:
//   Write/Close share state and are NOT thread-safe; caller must synchronize externally.
//   Close() invalidates all state; no other method may be called after Close().
class FileWriter : public core::NoCopyable
{
public:
    virtual ~FileWriter() noexcept = default;
    virtual core::Status Write(const char* data, uint64_t size) noexcept = 0;
    virtual core::Status Close() noexcept = 0;

    template <typename T>
    core::Status WriteObj(const T& val) noexcept
    {
        static_assert(std::is_trivially_copyable_v<T>, "WriteObj requires a trivially copyable type");
        return this->Write(reinterpret_cast<const char*>(&val), sizeof(val));
    }

    virtual core::Result<uint64_t> GetLength() const noexcept = 0;
};
} // namespace lumina::io
