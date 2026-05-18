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
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/rolling_file_writer.h"
#include "paimon/core/io/single_file_writer.h"
#include "paimon/logging.h"
#include "paimon/result.h"
#include "paimon/status.h"

struct ArrowArray;

namespace arrow {
class Schema;
class StructArray;
}  // namespace arrow

namespace paimon {

/// A blob file writer that manages multiple blob fields, each written to separate rolling files.
///
/// For each blob field in the schema, a dedicated RollingFileWriter is created. When writing a
/// row, the writer projects out each blob field and writes it to the corresponding blob file
/// independently.
///
/// This design supports multiple blob columns in a single table, where each blob column produces
/// its own set of blob files that are rolled independently based on target file size.
class MultipleBlobFileWriter {
 public:
    using BlobRollingWriter = RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>;
    using BlobWriterCreator = std::function<Result<std::unique_ptr<BlobRollingWriter>>(
        const std::string& blob_field_name)>;

    /// Constructs a MultipleBlobFileWriter.
    /// @param blob_schema The schema containing only blob fields.
    /// @param blob_writer_creator Factory function to create a RollingFileWriter for each blob
    /// field.
    MultipleBlobFileWriter(const std::shared_ptr<arrow::Schema>& blob_schema,
                           BlobWriterCreator blob_writer_creator);

    ~MultipleBlobFileWriter() = default;

    /// Writes a batch of blob data. The input ArrowArray should contain all blob fields as a
    /// StructArray. Each blob field is extracted and written to its dedicated rolling file writer
    /// row by row.
    Status Write(::ArrowArray* record);

    /// Aborts all blob writers and releases resources.
    void Abort();

    /// Closes all blob writers.
    Status Close();

    /// Returns the results (DataFileMeta) from all blob writers.
    Result<std::vector<std::shared_ptr<DataFileMeta>>> GetResult();

 private:
    /// Internal per-field blob writer.
    struct BlobFieldWriter {
        std::string field_name;
        int32_t field_index;
        std::unique_ptr<BlobRollingWriter> writer;
    };

    std::shared_ptr<arrow::Schema> blob_schema_;
    BlobWriterCreator blob_writer_creator_;
    std::vector<BlobFieldWriter> blob_field_writers_;
    bool closed_ = false;

    std::unique_ptr<Logger> logger_;
};

}  // namespace paimon
