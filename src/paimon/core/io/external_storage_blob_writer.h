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
#include <set>
#include <string>
#include <vector>

#include "paimon/common/data/blob_descriptor.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/rolling_file_writer.h"
#include "paimon/core/io/single_file_writer.h"
#include "paimon/logging.h"
#include "paimon/result.h"
#include "paimon/status.h"
namespace arrow {
class Schema;
class StructArray;
}  // namespace arrow

namespace paimon {

class FileSystem;
class LongCounter;
class MemoryPool;
class DataFilePathFactory;

/// Batch-oriented writer for descriptor BLOB fields that writes raw data to external storage.
///
/// For each configured external_storage field, this writer:
///   1. Uses RollingFileWriter (same infra as MultipleBlobFileWriter) with BlobFormatWriter
///   2. Injects a WriteConsumer into BlobFormatWriter to capture each row's BlobDescriptor
///   3. After writing a batch, constructs a descriptor column from captured descriptors
///
/// After TransformBatch(), the returned StructArray has descriptor columns replaced with
/// serialized BlobDescriptor bytes (large_binary), ready to be written into the main data file.
class ExternalStorageBlobWriter {
 public:
    using BlobRollingWriter = RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>;

    ExternalStorageBlobWriter(const std::shared_ptr<arrow::Schema>& write_schema,
                              const std::set<std::string>& external_storage_fields,
                              const std::string& external_storage_path, int64_t schema_id,
                              const std::shared_ptr<LongCounter>& seq_num_counter,
                              const std::shared_ptr<DataFilePathFactory>& path_factory,
                              const CoreOptions& options,
                              const std::shared_ptr<MemoryPool>& memory_pool);

    /// Transforms a batch by writing external storage fields to .blob files and replacing
    /// the BLOB values with serialized BlobDescriptor bytes.
    Result<std::shared_ptr<arrow::StructArray>> TransformBatch(
        const std::shared_ptr<arrow::StructArray>& batch);

    /// Closes all internal blob writers and flushes pending data.
    Status Close();

    /// Aborts all internal blob writers.
    void Abort();

 private:
    /// Per-field writer state for one external storage blob field.
    struct FieldWriter {
        std::string field_name;
        int32_t field_index;
        std::unique_ptr<BlobRollingWriter> rolling_writer;
        /// Descriptors captured by the WriteConsumer callback during writes.
        std::vector<std::unique_ptr<BlobDescriptor>> captured_descriptors;
    };

    /// Lazily initializes per-field writers on first call to TransformBatch.
    Status InitializeFieldWritersIfNeeded();

    /// Writes all rows of a single external blob field via RollingFileWriter and returns
    /// a descriptor column (LargeBinary) built from captured BlobDescriptors.
    Result<std::shared_ptr<arrow::Array>> TransformField(
        const std::shared_ptr<arrow::Array>& column, FieldWriter* field_writer);

    /// Creates a RollingFileWriter for one external storage blob field with consumer injected.
    Result<std::unique_ptr<BlobRollingWriter>> CreateFieldRollingWriter(FieldWriter* field_writer);

    std::shared_ptr<arrow::Schema> write_schema_;
    std::set<std::string> external_storage_fields_;
    std::string external_storage_path_;
    int64_t schema_id_;
    std::shared_ptr<LongCounter> seq_num_counter_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
    std::shared_ptr<MemoryPool> memory_pool_;
    CoreOptions options_;

    std::vector<FieldWriter> field_writers_;
    bool initialized_ = false;
};

}  // namespace paimon
