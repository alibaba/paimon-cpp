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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/executor/future.h"
#include "paimon/fs/file_system.h"
#include "paimon/macros.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class StreamUtils {
 public:
    StreamUtils() = default;
    ~StreamUtils() = default;

    static Result<PAIMON_UNIQUE_PTR<Bytes>> ReadFully(std::unique_ptr<InputStream> input_stream,
                                                      const std::shared_ptr<MemoryPool>& pool) {
        PAIMON_RETURN_NOT_OK(input_stream->Seek(0, FS_SEEK_SET));
        PAIMON_ASSIGN_OR_RAISE(int64_t file_length, input_stream->Length());
        PAIMON_UNIQUE_PTR<Bytes> content = Bytes::AllocateBytes(file_length, pool.get());
        PAIMON_ASSIGN_OR_RAISE(int64_t actual_read_len,
                               input_stream->Read(content->data(), file_length));
        if (actual_read_len != file_length) {
            return Status::Invalid("actual read length {}, not match with expect length {}",
                                   actual_read_len, file_length);
        }
        return content;
    }

    static Result<PAIMON_UNIQUE_PTR<Bytes>> ReadAsyncFully(
        std::unique_ptr<InputStream> input_stream, const std::shared_ptr<MemoryPool>& pool) {
        PAIMON_ASSIGN_OR_RAISE(int64_t file_length, input_stream->Length());
        PAIMON_UNIQUE_PTR<Bytes> content = Bytes::AllocateBytes(file_length, pool.get());
        PAIMON_RETURN_NOT_OK(ReadAsyncFully(std::move(input_stream), content->data()));
        return content;
    }

    static Status ReadAsyncFully(std::unique_ptr<InputStream> input_stream, char* content) {
        PAIMON_RETURN_NOT_OK(input_stream->Seek(0, FS_SEEK_SET));
        PAIMON_ASSIGN_OR_RAISE(int64_t file_length, input_stream->Length());

        int64_t read_offset = 0;
        int64_t read_len = std::min(file_length, kDefaultReadChunkSize);
        std::vector<std::future<Status>> futures;
        futures.reserve(file_length / kDefaultReadChunkSize + 1);
        while (read_len > 0) {
            auto promise = std::make_shared<std::promise<Status>>();
            futures.push_back(promise->get_future());
            input_stream->ReadAsync(content, read_len, read_offset,
                                    [promise](Status status) { promise->set_value(status); });
            read_offset += read_len;
            content += read_len;
            read_len = std::min(file_length - read_offset, kDefaultReadChunkSize);
        }
        for (const auto& status : CollectAll(futures)) {
            if (!status.ok()) {
                return status;
            }
        }
        if (PAIMON_UNLIKELY(read_offset != file_length)) {
            return Status::IOError(
                fmt::format("Total read length {} does not match expected length {}.", read_offset,
                            file_length));
        }
        return Status::OK();
    }

 private:
    static constexpr int64_t kDefaultReadChunkSize = 1024 * 1024;
};

}  // namespace paimon
