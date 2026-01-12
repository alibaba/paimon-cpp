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

#include <lumina/api/Options.h>
#include <lumina/core/NoCopyable.h>
#include <lumina/core/Result.h>
#include <lumina/core/Status.h>

namespace lumina::io {

class FileWriter;
// TODO don't wrape with Result?
core::Result<std::unique_ptr<FileWriter>> CreateFileWriter(const lumina::api::IOOptions& ioOptions) noexcept;

class FileWriter : public core::NoCopyable
{
public:
    virtual ~FileWriter() = default;
    virtual core::Status Write(const char* data, uint64_t size) = 0;
    virtual core::Status Close() = 0;
    // Ownership is transferred when a writer is passed to LuminaBuilder::Dump.
    // Implementations must support Close being called before destruction.

    template <typename T>
    core::Status WriteObj(const T& val)
    {
        return this->Write(reinterpret_cast<const char*>(&val), sizeof(val));
    }

    virtual core::Result<uint64_t> GetLength() const noexcept = 0;
};
} // namespace lumina::io
