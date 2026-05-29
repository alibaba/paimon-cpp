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
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "arrow/array/array_nested.h"
#include "arrow/c/bridge.h"
#include "arrow/result.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/core/io/multiple_blob_file_writer.h"
#include "paimon/core/io/rolling_file_writer.h"
#include "paimon/metrics.h"
#include "paimon/record_batch.h"

namespace paimon {

/// A rolling file writer that handles both normal data and blob data. This writer creates separate
/// files for normal columns and blob columns, managing their lifecycle and ensuring consistency
/// between them.
///
/// Multiple blob fields are supported. Each blob field is written to its own set of blob files
/// independently via MultipleBlobFileWriter.
///
/// <pre>
/// For example,
/// given a table schema with normal columns (id INT, name STRING) and blob columns (data1 BLOB,
/// data2 BLOB), this writer will create separate files for (id, name), (data1), and (data2).
/// It will roll files based on the specified target file size, ensuring that both normal and blob
/// files are rolled simultaneously.
///
/// Every time a file is rolled, the writer will close the current normal data file and blob data
/// files, so one normal data file may correspond to multiple blob data files.
///
/// Normal file1: f1.parquet may include (blob1_1.blob, blob1_2.blob, blob2_1.blob)
/// Normal file2: f2.parquet may include (blob1_3.blob, blob2_2.blob)
///
/// </pre>
class RollingBlobFileWriter
    : public RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>> {
 public:
    using MainWriter = SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>;

    RollingBlobFileWriter(int64_t target_file_size,
                          std::function<Result<std::unique_ptr<MainWriter>>()> create_file_writer,
                          const std::shared_ptr<arrow::Schema>& blob_schema,
                          MultipleBlobFileWriter::BlobWriterCreator blob_writer_creator,
                          const std::shared_ptr<arrow::DataType>& data_type,
                          const std::set<std::string>& inline_fields);
    ~RollingBlobFileWriter() override = default;

    Status Write(::ArrowArray* record) override;
    void Abort() override;
    Status Close() override;
    Result<std::vector<std::shared_ptr<DataFileMeta>>> GetResult() override;

 private:
    static Status ValidateFileConsistency(
        const std::shared_ptr<DataFileMeta>& main_data_file_meta,
        const std::vector<std::shared_ptr<DataFileMeta>>& blob_tagged_metas,
        int32_t blob_field_count);

    Status CloseCurrentWriter();

    Result<std::shared_ptr<DataFileMeta>> CloseMainWriter();
    Result<std::vector<std::shared_ptr<DataFileMeta>>> CloseBlobWriter();

    std::shared_ptr<arrow::Schema> blob_schema_;
    MultipleBlobFileWriter::BlobWriterCreator blob_writer_creator_;
    std::unique_ptr<MultipleBlobFileWriter> blob_writer_;
    std::shared_ptr<arrow::DataType> data_type_;
    std::set<std::string> inline_fields_;

    std::unique_ptr<Logger> logger_;
};

}  // namespace paimon
