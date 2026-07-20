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
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "arrow/api.h"
#include "arrow/util/crc32.h"
#include "paimon/format/format_writer.h"
#include "paimon/logging.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class DataType;
}  // namespace arrow
struct ArrowArray;

namespace paimon {
class Blob;
class FileSystem;
class InputStream;
class Metrics;
class OutputStream;
}  // namespace paimon

namespace paimon::blob {

// Blob format:
// https://cwiki.apache.org/confluence/display/PAIMON/PIP-35%3A+Introduce+Blob+to+store+multimodal+data
class BlobFormatWriter : public FormatWriter {
 public:
    /// When opening a descriptor input fails, `write_null_on_missing_file` converts a
    /// missing file (Status::NotExist) to a NULL element and `write_null_on_fetch_failure`
    /// converts any other open failure; failures during the streaming copy always fail the
    /// write. See Options::BLOB_WRITE_NULL_ON_MISSING_FILE / BLOB_WRITE_NULL_ON_FETCH_FAILURE.
    static Result<std::unique_ptr<BlobFormatWriter>> Create(
        const std::shared_ptr<OutputStream>& out, const std::shared_ptr<arrow::DataType>& data_type,
        bool write_null_on_missing_file, bool write_null_on_fetch_failure,
        const std::shared_ptr<FileSystem>& fs, const std::shared_ptr<MemoryPool>& pool);

    Status AddBatch(ArrowArray* batch) override;

    Status Flush() override;

    Status Finish() override;

    Result<bool> ReachTargetSize(bool suggested_check, int64_t target_size) const override;

    std::shared_ptr<Metrics> GetWriterMetrics() const override {
        return metrics_;
    }

    Status AddMetadata(const std::map<std::string, std::string>& metadata) override;

 private:
    BlobFormatWriter(const std::shared_ptr<OutputStream>& out, const std::string& uri,
                     const std::shared_ptr<arrow::DataType>& data_type,
                     bool write_null_on_missing_file, bool write_null_on_fetch_failure,
                     const std::shared_ptr<FileSystem>& fs,
                     const std::shared_ptr<MemoryPool>& pool);

    Status WriteBlob(std::string_view blob_data);

    /// Deserialize the descriptor and open an input stream on the referenced data.
    Result<std::unique_ptr<InputStream>> OpenDescriptorInputStream(
        std::string_view blob_data) const;

    Status WriteBytes(const char* data, int64_t length);
    Status WriteWithCrc32(const char* data, int64_t length);

    template <typename T>
    static PAIMON_UNIQUE_PTR<Bytes> IntegerToLittleEndian(T value,
                                                          const std::shared_ptr<MemoryPool>& pool);

 private:
    static constexpr uint32_t kTmpBufferSize = 1024 * 1024;

    uint32_t crc32_ = 0;
    std::vector<int64_t> bin_lengths_;
    std::shared_ptr<OutputStream> out_;
    std::string uri_;
    PAIMON_UNIQUE_PTR<Bytes> tmp_buffer_;
    PAIMON_UNIQUE_PTR<Bytes> magic_number_bytes_;
    std::shared_ptr<arrow::DataType> data_type_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<Metrics> metrics_;
    bool write_null_on_missing_file_ = false;
    bool write_null_on_fetch_failure_ = false;
    std::unique_ptr<Logger> logger_;
};

}  // namespace paimon::blob
