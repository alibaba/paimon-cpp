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

#include "paimon/core/io/multiple_blob_file_writer.h"

#include <memory>
#include <utility>
#include <vector>

#include "arrow/array/array_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/type.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/macros.h"

namespace paimon {

MultipleBlobFileWriter::MultipleBlobFileWriter(const std::shared_ptr<arrow::Schema>& blob_schema,
                                               BlobWriterCreator blob_writer_creator)
    : blob_schema_(blob_schema),
      blob_writer_creator_(std::move(blob_writer_creator)),
      logger_(Logger::GetLogger("MultipleBlobFileWriter")) {}

Status MultipleBlobFileWriter::Write(::ArrowArray* record) {
    // Lazily initialize per-field blob writers on first write
    if (blob_field_writers_.empty()) {
        for (int32_t i = 0; i < blob_schema_->num_fields(); ++i) {
            const std::string& field_name = blob_schema_->field(i)->name();
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BlobRollingWriter> writer,
                                   blob_writer_creator_(field_name));
            blob_field_writers_.push_back(BlobFieldWriter{field_name, i, std::move(writer)});
        }
    }

    // Import the ArrowArray as a StructArray containing all blob fields
    std::shared_ptr<arrow::DataType> data_type = arrow::struct_(blob_schema_->fields());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(record, data_type));
    std::shared_ptr<arrow::StructArray> struct_array =
        std::dynamic_pointer_cast<arrow::StructArray>(arrow_array);
    if (!struct_array) {
        return Status::Invalid("MultipleBlobFileWriter: input is not a StructArray");
    }

    // TODO(xinyu.lxy): support write parallel
    // For each blob field, extract the column and write row by row to its dedicated writer
    for (BlobFieldWriter& field_writer : blob_field_writers_) {
        std::shared_ptr<arrow::Array> field_array = struct_array->field(field_writer.field_index);
        // Create a single-field StructArray for each row and write to the blob writer
        for (int64_t row = 0; row < field_array->length(); ++row) {
            std::shared_ptr<arrow::Array> slice = field_array->Slice(row, 1);
            // Wrap single field into a StructArray with the same field name
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::StructArray> single_field_struct,
                arrow::StructArray::Make({slice}, {field_writer.field_name}));
            ::ArrowArray c_blob_array;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                arrow::ExportArray(*single_field_struct, &c_blob_array));
            ScopeGuard guard([&c_blob_array]() { ArrowArrayRelease(&c_blob_array); });
            PAIMON_RETURN_NOT_OK(field_writer.writer->Write(&c_blob_array));
        }
    }

    return Status::OK();
}

void MultipleBlobFileWriter::Abort() {
    for (auto& field_writer : blob_field_writers_) {
        if (field_writer.writer) {
            field_writer.writer->Abort();
        }
    }
    blob_field_writers_.clear();
}

Status MultipleBlobFileWriter::Close() {
    if (closed_) {
        return Status::OK();
    }
    for (auto& field_writer : blob_field_writers_) {
        if (field_writer.writer) {
            PAIMON_RETURN_NOT_OK(field_writer.writer->Close());
        }
    }
    closed_ = true;
    return Status::OK();
}

Result<std::vector<std::shared_ptr<DataFileMeta>>> MultipleBlobFileWriter::GetResult() {
    std::vector<std::shared_ptr<DataFileMeta>> all_results;
    for (BlobFieldWriter& field_writer : blob_field_writers_) {
        if (field_writer.writer) {
            PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> results,
                                   field_writer.writer->GetResult());
            all_results.insert(all_results.end(), results.begin(), results.end());
        }
    }
    blob_field_writers_.clear();
    return all_results;
}

}  // namespace paimon
