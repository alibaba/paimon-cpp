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

#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"

#include <cstdint>
#include <utility>

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/math.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/macros.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

namespace {

template <typename To, typename From>
arrow::Status ValidateArrowIoRange(From value, const char* name) {
    if (!InRange<To>(value)) {
        return arrow::Status::Invalid(fmt::format("{} value {} is out of bound of type {}", name,
                                                  value, OptionsUtils::GetTypeName<To>()));
    }
    return arrow::Status::OK();
}

}  // namespace

ArrowInputStreamAdapter::ArrowInputStreamAdapter(
    const std::shared_ptr<paimon::InputStream>& input_stream,
    const std::shared_ptr<arrow::MemoryPool>& pool, uint64_t file_size)
    : input_stream_(input_stream), pool_(pool), file_size_(file_size) {}

ArrowInputStreamAdapter::~ArrowInputStreamAdapter() {
    [[maybe_unused]] auto status = DoClose();
}

arrow::Status ArrowInputStreamAdapter::Seek(int64_t position) {
    return ToArrowStatus(input_stream_->Seek(position, SeekOrigin::FS_SEEK_SET));
}

arrow::Result<int64_t> ArrowInputStreamAdapter::Read(int64_t nbytes, void* out) {
    ARROW_RETURN_NOT_OK(ValidateArrowIoRange<uint32_t>(nbytes, "nbytes"));
    Result<int32_t> read_bytes =
        input_stream_->Read(static_cast<char*>(out), static_cast<uint32_t>(nbytes));
    if (!read_bytes.ok()) {
        return ToArrowStatus(read_bytes.status());
    }
    return read_bytes.value();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ArrowInputStreamAdapter::Read(int64_t nbytes) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ResizableBuffer> buffer,
                          arrow::AllocateResizableBuffer(nbytes, pool_.get()));
    ARROW_ASSIGN_OR_RAISE(int64_t read_bytes, Read(nbytes, buffer->mutable_data()));
    if (read_bytes < nbytes) {
        ARROW_RETURN_NOT_OK(buffer->Resize(read_bytes));
    }
    return std::shared_ptr<arrow::Buffer>(std::move(buffer));
}

arrow::Result<int64_t> ArrowInputStreamAdapter::ReadAt(int64_t position, int64_t nbytes,
                                                       void* out) {
    ARROW_RETURN_NOT_OK(ValidateArrowIoRange<uint64_t>(position, "position"));
    ARROW_RETURN_NOT_OK(ValidateArrowIoRange<uint32_t>(nbytes, "nbytes"));
    Result<int32_t> read_bytes = input_stream_->Read(
        static_cast<char*>(out), static_cast<uint32_t>(nbytes), static_cast<uint64_t>(position));
    if (!read_bytes.ok()) {
        return ToArrowStatus(read_bytes.status());
    }
    return read_bytes.value();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ArrowInputStreamAdapter::ReadAt(int64_t position,
                                                                              int64_t nbytes) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ResizableBuffer> buffer,
                          arrow::AllocateResizableBuffer(nbytes, pool_.get()));
    ARROW_ASSIGN_OR_RAISE(int64_t read_bytes, ReadAt(position, nbytes, buffer->mutable_data()));
    if (read_bytes < nbytes) {
        ARROW_RETURN_NOT_OK(buffer->Resize(read_bytes));
    }
    return std::shared_ptr<arrow::Buffer>(std::move(buffer));
}

arrow::Future<std::shared_ptr<arrow::Buffer>> ArrowInputStreamAdapter::ReadAsync(
    const arrow::io::IOContext& io_context, int64_t position, int64_t nbytes) {
    auto fut = arrow::Future<std::shared_ptr<arrow::Buffer>>::Make();
    auto range_status = ValidateArrowIoRange<uint64_t>(position, "position");
    if (!range_status.ok()) {
        fut.MarkFinished(range_status);
        return fut;
    }
    range_status = ValidateArrowIoRange<uint32_t>(nbytes, "nbytes");
    if (!range_status.ok()) {
        fut.MarkFinished(range_status);
        return fut;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> buffer_result =
        arrow::AllocateResizableBuffer(nbytes, pool_.get());
    if (PAIMON_UNLIKELY(!buffer_result.ok())) {
        fut.MarkFinished(buffer_result.status());
        return fut;
    }
    std::shared_ptr<arrow::Buffer> buffer = std::move(buffer_result).ValueUnsafe();
    input_stream_->ReadAsync(reinterpret_cast<char*>(buffer->mutable_data()),
                             static_cast<uint32_t>(nbytes), static_cast<uint64_t>(position),
                             [fut, buffer](Status callback_status) mutable {
                                 if (callback_status.ok()) {
                                     fut.MarkFinished(std::move(buffer));
                                 } else {
                                     fut.MarkFinished(ToArrowStatus(callback_status));
                                 }
                             });
    return fut;
}

arrow::Result<int64_t> ArrowInputStreamAdapter::Tell() const {
    Result<int64_t> position = input_stream_->GetPos();
    if (!position.ok()) {
        return ToArrowStatus(position.status());
    }
    return position.value();
}

arrow::Result<int64_t> ArrowInputStreamAdapter::GetSize() {
    return static_cast<int64_t>(file_size_);
}

bool ArrowInputStreamAdapter::closed() const {
    return closed_;
}

arrow::Status ArrowInputStreamAdapter::DoClose() {
    if (!closed_) {
        Status status = input_stream_->Close();
        if (!status.ok()) {
            return ToArrowStatus(status);
        }
        closed_ = true;
    }
    return arrow::Status::OK();
}

}  // namespace paimon
