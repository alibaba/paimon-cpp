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
/// A `InputStream` wrapping another `InputStream` with offset and length.
class PAIMON_EXPORT OffsetInputStream : public InputStream {
 public:
    static Result<std::unique_ptr<OffsetInputStream>> Create(
        const std::shared_ptr<InputStream>& wrapped, int64_t length, int64_t offset);
    static Result<std::unique_ptr<OffsetInputStream>> Create(
        const std::shared_ptr<InputStream>& wrapped, int64_t length, int64_t offset,
        int64_t total_length);
    ~OffsetInputStream() override = default;

    Status Seek(int64_t offset, SeekOrigin origin) override;

    Result<int64_t> GetPos() const override;

    Result<int64_t> Read(char* buffer, int64_t size) override;

    Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override;

    void ReadAsync(char* buffer, int64_t size, int64_t offset,
                   std::function<void(Status)>&& callback) override;

    Result<int64_t> Length() const override;

    Status Close() override;

    Result<std::string> GetUri() const override;

 private:
    OffsetInputStream(const std::shared_ptr<InputStream>& wrapped, int64_t length, int64_t offset);
    Status AssertBoundary(int64_t inner_pos) const;

 private:
    std::shared_ptr<InputStream> wrapped_;
    const int64_t length_;
    const int64_t offset_;
    int64_t inner_position_ = 0;
};
}  // namespace paimon
