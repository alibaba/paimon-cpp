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

#include "paimon/core/io/rolling_blob_file_writer.h"

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/macros.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {

RollingBlobFileWriter::RollingBlobFileWriter(
    int64_t target_file_size, const std::shared_ptr<MainWriterFactory>& writer_factory,
    const std::shared_ptr<arrow::Schema>& blob_schema,
    MultipleBlobFileWriter::BlobWriterCreator blob_writer_creator,
    const std::shared_ptr<arrow::DataType>& data_type, const std::set<std::string>& inline_fields)
    : RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>(target_file_size,
                                                                      writer_factory),
      blob_schema_(blob_schema),
      blob_writer_creator_(std::move(blob_writer_creator)),
      data_type_(data_type),
      inline_fields_(inline_fields),
      logger_(Logger::GetLogger("RollingBlobFileWriter")) {}

Status RollingBlobFileWriter::Write(::ArrowArray* record) {
    ScopeGuard guard([this]() -> void { this->Abort(); });
    // Open the current writer if write the first record or roll over happen before.
    if (writer_factory_ != nullptr && PAIMON_UNLIKELY(current_writer_ == nullptr)) {
        PAIMON_RETURN_NOT_OK(OpenCurrentWriter());
    }
    if (PAIMON_UNLIKELY(blob_writer_ == nullptr)) {
        blob_writer_ = std::make_unique<MultipleBlobFileWriter>(blob_schema_, blob_writer_creator_);
    }
    int64_t record_count = record->length;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(record, data_type_));
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(arrow_array);

    PAIMON_ASSIGN_OR_RAISE(BlobUtils::SeparatedStructArrays separated_arrays,
                           BlobUtils::SeparateBlobArray(struct_array, inline_fields_));
    // Write main (non-blob) data
    if (current_writer_ != nullptr) {
        ::ArrowArray c_main_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            arrow::ExportArray(*separated_arrays.main_array, &c_main_array));
        ScopeGuard array_lifecycle_guard(
            [&c_main_array]() -> void { ArrowArrayRelease(&c_main_array); });
        PAIMON_RETURN_NOT_OK(current_writer_->Write(&c_main_array));
    }

    // Write blob data via MultipleBlobFileWriter (each blob field independently)
    ::ArrowArray c_blob_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*separated_arrays.blob_array, &c_blob_array));
    ScopeGuard blob_array_guard([&c_blob_array]() -> void { ArrowArrayRelease(&c_blob_array); });
    PAIMON_RETURN_NOT_OK(blob_writer_->Write(&c_blob_array));

    record_count_ += record_count;
    if (current_writer_ != nullptr) {
        PAIMON_ASSIGN_OR_RAISE(bool need_rolling_file, NeedRollingFile());
        if (need_rolling_file) {
            PAIMON_RETURN_NOT_OK(CloseCurrentWriter());
        }
    }
    guard.Release();
    return Status::OK();
}

Status RollingBlobFileWriter::CloseCurrentWriter() {
    if (blob_writer_ == nullptr) {
        return Status::OK();
    }
    std::shared_ptr<DataFileMeta> main_data_file_meta;
    if (current_writer_ != nullptr) {
        PAIMON_ASSIGN_OR_RAISE(main_data_file_meta, CloseMainWriter());
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> blob_metas,
                           CloseBlobWriter());

    if (main_data_file_meta != nullptr) {
        PAIMON_RETURN_NOT_OK(ValidateFileConsistency(main_data_file_meta, blob_metas));
        results_.push_back(main_data_file_meta);
    }
    results_.insert(results_.end(), blob_metas.begin(), blob_metas.end());

    current_writer_.reset();
    return Status::OK();
}

Result<std::vector<std::shared_ptr<DataFileMeta>>> RollingBlobFileWriter::GetResult() {
    if (!closed_) {
        return Status::Invalid("Cannot access the results unless close all writers.");
    }
    return results_;
}

Result<std::shared_ptr<DataFileMeta>> RollingBlobFileWriter::CloseMainWriter() {
    PAIMON_RETURN_NOT_OK(current_writer_->Close());
    PAIMON_ASSIGN_OR_RAISE(MainWriter::AbortExecutor abort_executor,
                           current_writer_->GetAbortExecutor());
    closed_writers_.push_back(abort_executor);
    return current_writer_->GetResult();
}

Result<std::vector<std::shared_ptr<DataFileMeta>>> RollingBlobFileWriter::CloseBlobWriter() {
    PAIMON_RETURN_NOT_OK(blob_writer_->Close());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> results,
                           blob_writer_->GetResult());
    blob_writer_.reset();
    return results;
}

Status RollingBlobFileWriter::ValidateFileConsistency(
    const std::shared_ptr<DataFileMeta>& main_data_file_meta,
    const std::vector<std::shared_ptr<DataFileMeta>>& blob_tagged_metas) {
    std::map<std::string, int64_t> blob_field_row_counts;
    for (const auto& blob_tagged_meta : blob_tagged_metas) {
        if (!blob_tagged_meta->write_cols || blob_tagged_meta->write_cols->empty()) {
            return Status::Invalid(
                fmt::format("This is a bug: Blob file {} must contain a write column.",
                            blob_tagged_meta->file_name));
        }
        blob_field_row_counts[blob_tagged_meta->write_cols->at(0)] += blob_tagged_meta->row_count;
    }

    int64_t main_row_count = main_data_file_meta->row_count;
    for (const auto& [field_name, row_count] : blob_field_row_counts) {
        if (row_count != main_row_count) {
            return Status::Invalid(fmt::format(
                "This is a bug: The row count of main file and blob file does not match. Main "
                "file: {} (row count: {}), blob field name: {} (row count: {})",
                main_data_file_meta->file_name, main_row_count, field_name, row_count));
        }
    }
    return Status::OK();
}

Status RollingBlobFileWriter::Close() {
    if (closed_) {
        return Status::OK();
    }
    auto s = CloseCurrentWriter();
    if (!s.ok()) {
        if (current_writer_) {
            PAIMON_LOG_WARN(logger_, "Exception occurs when writing file %s. Cleaning up: %s",
                            current_writer_->GetPath().c_str(), s.ToString().c_str());
        }
        Abort();
    }
    closed_ = true;
    return s;
}

void RollingBlobFileWriter::Abort() {
    if (current_writer_ != nullptr) {
        current_writer_->Abort();
        current_writer_.reset();
    }
    for (auto& abort_executor : closed_writers_) {
        abort_executor.Abort();
    }
    if (blob_writer_ != nullptr) {
        blob_writer_->Abort();
        blob_writer_.reset();
    }
}

}  // namespace paimon
