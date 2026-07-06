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

#include "paimon/format/orc/orc_reader_wrapper.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "orc/OrcFile.hh"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"

namespace paimon::orc {

Status OrcReaderWrapper::SeekToRow(uint64_t row_number) {
    try {
        row_reader_->seekToRow(row_number);
        next_row_ = row_number;
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("orc file batch reader seek to row {} failed for file {}, with {} error",
                        row_number, file_name_, e.what()));
    } catch (...) {
        return Status::UnknownError(fmt::format(
            "orc file batch reader seek to row {} failed for file {}, with unknown error",
            row_number, file_name_));
    }
    return Status::OK();
}

Status OrcReaderWrapper::SetReadSchema(const std::shared_ptr<arrow::DataType>& target_type,
                                       const ::orc::RowReaderOptions& row_reader_options) {
    try {
        row_reader_ = reader_->createRowReader(row_reader_options);
        target_type_ = target_type;
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("orc file batch reader create row reader failed for file {}, with {} error",
                        file_name_, e.what()));
    } catch (...) {
        return Status::UnknownError(fmt::format(
            "orc file batch reader create row reader failed for file {}, with unknown error",
            file_name_));
    }
    return Status::OK();
}

Result<BatchReader::ReadBatch> OrcReaderWrapper::Next() {
    if (has_error_) {
        return Status::Invalid(fmt::format(
            "Since an error has occurred, next batch has been prohibited. file '{}'", file_name_));
    }
    std::unique_ptr<ArrowArray> c_array = std::make_unique<ArrowArray>();
    std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
    try {
        auto orc_batch = row_reader_->createRowBatch(batch_size_);
        bool eof = !row_reader_->next(*orc_batch);
        if (eof) {
            return BatchReader::MakeEofBatch();
        }
        ScopeGuard guard([this]() { has_error_ = true; });
        assert(orc_batch->numElements > 0);
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Array> array,
            OrcAdapter::AppendBatch(target_type_, orc_batch.get(), arrow_pool_.get()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, c_array.get(), c_schema.get()));
        next_row_ = GetRowNumber() + orc_batch->numElements;
        guard.Release();
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("orc file batch reader get next batch failed for file {}, with {} error",
                        file_name_, e.what()));
    } catch (...) {
        return Status::UnknownError(fmt::format(
            "orc file batch reader get next batch failed for file {}, with unknown error",
            file_name_));
    }
    return make_pair(std::move(c_array), std::move(c_schema));
}

}  // namespace paimon::orc
