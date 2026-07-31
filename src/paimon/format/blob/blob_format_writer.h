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

class BlobMetrics {
 public:
    /// Number of rows written as NULL because their referenced file did not exist.
    static inline const char WRITE_NULL_ON_MISSING_FILE_COUNT[] =
        "blob.write.null-on-missing-file.count";
    /// Number of rows written as NULL because their referenced data could not be reached.
    static inline const char WRITE_NULL_ON_FETCH_FAILURE_COUNT[] =
        "blob.write.null-on-fetch-failure.count";
};

// Blob format:
// https://cwiki.apache.org/confluence/display/PAIMON/PIP-35%3A+Introduce+Blob+to+store+multimodal+data
class BlobFormatWriter : public FormatWriter {
 public:
    /// `write_null_on_missing_file` converts a descriptor whose referenced file does not exist
    /// (as reported by FileSystem::Exists) to a NULL element, and `write_null_on_fetch_failure`
    /// converts any other failure to access the referenced data; failures during the streaming
    /// copy always fail the write. The existence check runs only when
    /// `write_null_on_missing_file` is enabled; otherwise a missing file follows
    /// `write_null_on_fetch_failure` like any other failed open.
    /// See Options::BLOB_WRITE_NULL_ON_MISSING_FILE / BLOB_WRITE_NULL_ON_FETCH_FAILURE.
    ///
    /// `write_placeholder` (see BlobDefs::kWritePlaceholderKey, false unless the write is a
    /// data-evolution partial update) persists a value exactly equal to
    /// BlobDefs::kPlaceholderSentinel as a placeholder entry (bin_length -2, no data bytes);
    /// any other value is stored verbatim. When disabled, values are never interpreted and can
    /// never be turned into placeholder entries.
    static Result<std::unique_ptr<BlobFormatWriter>> Create(
        const std::shared_ptr<OutputStream>& out, const std::shared_ptr<arrow::DataType>& data_type,
        bool write_null_on_missing_file, bool write_null_on_fetch_failure, bool write_placeholder,
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
                     bool write_placeholder, const std::shared_ptr<FileSystem>& fs,
                     const std::shared_ptr<MemoryPool>& pool);

    Status WriteBlob(std::string_view blob_data);

    /// Deserialize the descriptor and open an input stream on the referenced data.
    /// Returns a null stream when the failure is converted to a NULL element by
    /// `write_null_on_missing_file_` or `write_null_on_fetch_failure_`.
    Result<std::unique_ptr<InputStream>> OpenDescriptorInputStream(std::string_view blob_data);

    /// Convert a file that FileSystem::Exists reported as absent to a NULL element: count it and
    /// return a null stream. Only reached under `write_null_on_missing_file_`, which callers check.
    std::unique_ptr<InputStream> HandleMissingFile(const std::string& blob_uri);

    /// Apply `write_null_on_fetch_failure_` to a failure to reach the referenced data: returns
    /// `status` when the option is disabled, and a null stream when it converts the failure to a
    /// NULL element. `blob_uri` is only used for logging.
    Result<std::unique_ptr<InputStream>> HandleFetchFailure(const std::string& blob_uri,
                                                            const Status& status);

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
    /// Path of the blob file being written, not of any referenced blob.
    std::string uri_;
    PAIMON_UNIQUE_PTR<Bytes> tmp_buffer_;
    PAIMON_UNIQUE_PTR<Bytes> magic_number_bytes_;
    std::shared_ptr<arrow::DataType> data_type_;
    std::string blob_field_name_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<Metrics> metrics_;
    bool write_null_on_missing_file_ = false;
    bool write_null_on_fetch_failure_ = false;
    bool write_placeholder_ = false;
    uint64_t null_on_missing_file_count_ = 0;
    uint64_t null_on_fetch_failure_count_ = 0;
    std::unique_ptr<Logger> logger_;
};

}  // namespace paimon::blob
