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

#include <cstdint>
#include <memory>

#include "arrow/api.h"
#include "arrow/io/interfaces.h"
#include "arrow/util/future.h"
#include "paimon/visibility.h"

namespace paimon {
class InputStream;

class PAIMON_EXPORT ArrowInputStreamAdapter : public arrow::io::RandomAccessFile {
 public:
    ArrowInputStreamAdapter(const std::shared_ptr<paimon::InputStream>& input_stream,
                            const std::shared_ptr<arrow::MemoryPool>& pool, uint64_t file_size);
    ~ArrowInputStreamAdapter() override;

    // NOTE: In paimon file system definition, position + nbytes should not exceed file_size_.
    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;
    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override;
    arrow::Future<std::shared_ptr<arrow::Buffer>> ReadAsync(const arrow::io::IOContext& io_context,
                                                            int64_t position,
                                                            int64_t nbytes) override;
    arrow::Status Seek(int64_t position) override;
    arrow::Result<int64_t> Tell() const override;
    arrow::Result<int64_t> GetSize() override;
    arrow::Status Close() override {
        return DoClose();
    }
    bool closed() const override;

 private:
    arrow::Status DoClose();

    std::shared_ptr<paimon::InputStream> input_stream_;
    std::shared_ptr<arrow::MemoryPool> pool_;
    uint64_t file_size_;
    bool closed_ = false;
};

}  // namespace paimon
