/*
 * Copyright 2026-present Alibaba Inc.
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

#include <cstring>
#include <memory>
#include <string>

#include "paimon/fs/file_system.h"
#include "paimon/utils/read_ahead_cache.h"

namespace paimon {

class CacheInputStream : public InputStream {
 public:
    CacheInputStream(std::unique_ptr<InputStream> input_stream,
                     const std::shared_ptr<ReadAheadCache>& cache)
        : cache_(cache), input_stream_(std::move(input_stream)) {}

    Status Seek(int64_t offset, SeekOrigin origin) override {
        return input_stream_->Seek(offset, origin);
    }
    Result<int64_t> GetPos() const override {
        return input_stream_->GetPos();
    }
    Result<int32_t> Read(char* buffer, uint32_t size) override {
        return input_stream_->Read(buffer, size);
    }
    Result<int32_t> Read(char* buffer, uint32_t size, uint64_t offset) override {
        if (cache_) {
            ByteRange range{offset, static_cast<uint64_t>(size)};
            PAIMON_ASSIGN_OR_RAISE(ByteSlice slice, cache_->Read(range));
            if (slice.buffer) {
                std::memcpy(buffer, slice.buffer->data() + slice.offset, slice.length);
                return slice.length;
            }
        }
        return input_stream_->Read(buffer, size, offset);
    }
    void ReadAsync(char* buffer, uint32_t size, uint64_t offset,
                   std::function<void(Status)>&& callback) override {
        if (cache_) {
            ByteRange range{offset, static_cast<uint64_t>(size)};
            Result<ByteSlice> slice = cache_->Read(range);
            if (!slice.ok()) {
                callback(slice.status());
                return;
            }
            if (slice.value().buffer) {
                std::memcpy(buffer, slice.value().buffer->data() + slice.value().offset,
                            slice.value().length);
                callback(Status::OK());
                return;
            }
        }
        return input_stream_->ReadAsync(buffer, size, offset, std::move(callback));
    }

    Status Close() override {
        return input_stream_->Close();
    }

    Result<std::string> GetUri() const override {
        return input_stream_->GetUri();
    }

    Result<uint64_t> Length() const override {
        return input_stream_->Length();
    }

 private:
    std::shared_ptr<ReadAheadCache> cache_;
    std::unique_ptr<InputStream> input_stream_;
};

}  // namespace paimon
