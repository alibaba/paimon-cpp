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
#include <cassert>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/abi.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/file_index/file_index_writer.h"
#include "paimon/global_index/global_index_writer.h"
#include "paimon/global_index/io/global_index_file_writer.h"

namespace paimon {
/// A `GlobalIndexWriter` wrapper for `FileIndexWriter`.
class FileIndexWriterWrapper : public GlobalIndexWriter {
 public:
    FileIndexWriterWrapper(const std::string& index_type,
                           const std::shared_ptr<GlobalIndexFileWriter>& file_manager,
                           const std::shared_ptr<FileIndexWriter>& writer)
        : index_type_(index_type), file_manager_(file_manager), writer_(writer) {}

    Status AddBatch(::ArrowArray* c_arrow_array) override {
        int64_t length = c_arrow_array->length;
        PAIMON_RETURN_NOT_OK(writer_->AddBatch(c_arrow_array));
        count_ += length;
        return Status::OK();
    }

    Result<std::vector<GlobalIndexIOMeta>> Finish() override {
        if (count_ == 0) {
            return std::vector<GlobalIndexIOMeta>();
        }
        PAIMON_ASSIGN_OR_RAISE(std::string file_name, file_manager_->NewFileName(index_type_));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> out,
                               file_manager_->NewOutputStream(file_name));
        PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> bytes, writer_->SerializedBytes());

        uint64_t total_write_size = 0;
        while (total_write_size < bytes->size()) {
            uint64_t current_write_size =
                std::min(bytes->size() - total_write_size, max_write_size_);
            PAIMON_ASSIGN_OR_RAISE(int32_t actual_size,
                                   out->Write(bytes->data() + total_write_size,
                                              static_cast<uint32_t>(current_write_size)));
            if (static_cast<uint64_t>(actual_size) != current_write_size) {
                return Status::IOError(
                    fmt::format("expect write len {} mismatch actual write len {}",
                                current_write_size, actual_size));
            }
            total_write_size += current_write_size;
        }
        PAIMON_RETURN_NOT_OK(out->Flush());
        PAIMON_RETURN_NOT_OK(out->Close());
        GlobalIndexIOMeta meta(file_manager_->ToPath(file_name), /*file_size=*/bytes->size(),
                               /*range_end=*/count_ - 1,
                               /*metadata=*/nullptr);
        return std::vector<GlobalIndexIOMeta>({meta});
    }

 private:
    static constexpr uint64_t kMaxWriteSize = std::numeric_limits<int32_t>::max();

    std::string index_type_;
    int64_t count_ = 0;
    uint64_t max_write_size_ = kMaxWriteSize;
    std::shared_ptr<GlobalIndexFileWriter> file_manager_;
    std::shared_ptr<FileIndexWriter> writer_;
};
}  // namespace paimon
