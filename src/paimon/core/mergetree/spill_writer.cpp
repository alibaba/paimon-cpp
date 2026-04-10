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

#include "paimon/core/mergetree/spill_writer.h"

#include "paimon/common/utils/arrow/arrow_output_stream_adapter.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/mergetree/spill_channel_manager.h"

namespace paimon {

SpillWriter::SpillWriter(const std::shared_ptr<FileSystem>& fs,
                         const std::shared_ptr<arrow::Schema>& schema,
                         const std::shared_ptr<FileIOChannel::Enumerator>& channel_enumerator,
                         const std::shared_ptr<SpillChannelManager>& spill_channel_manager,
                         const std::string& compression, int32_t compression_level)
    : fs_(fs),
      schema_(schema),
      channel_enumerator_(channel_enumerator),
      spill_channel_manager_(spill_channel_manager),
      compression_(compression),
      compression_level_(compression_level) {}

Result<std::unique_ptr<SpillWriter>> SpillWriter::Create(
    const std::shared_ptr<FileSystem>& fs, const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<FileIOChannel::Enumerator>& channel_enumerator,
    const std::shared_ptr<SpillChannelManager>& spill_channel_manager,
    const std::string& compression, int32_t compression_level) {
    std::unique_ptr<SpillWriter> writer(new SpillWriter(
        fs, schema, channel_enumerator, spill_channel_manager, compression, compression_level));
    PAIMON_RETURN_NOT_OK(writer->Open());
    return writer;
}

Status SpillWriter::Open() {
    channel_id_ = channel_enumerator_->Next();
    auto ipc_write_options = arrow::ipc::IpcWriteOptions::Defaults();
    auto cleanup_guard = ScopeGuard([&]() {
        arrow_writer_.reset();
        arrow_output_stream_adapter_.reset();
        if (out_stream_) {
            [[maybe_unused]] auto status = out_stream_->Close();
            out_stream_.reset();
        }
        if (!channel_id_.GetPath().empty()) {
            [[maybe_unused]] auto status = fs_->Delete(channel_id_.GetPath());
        }
    });
    PAIMON_ASSIGN_OR_RAISE(auto arrow_compression, ArrowUtils::GetCompressionType(compression_));
    if (!arrow::util::Codec::SupportsCompressionLevel(arrow_compression)) {
        compression_level_ = arrow::util::Codec::UseDefaultCompressionLevel();
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        ipc_write_options.codec, arrow::util::Codec::Create(arrow_compression, compression_level_));
    PAIMON_ASSIGN_OR_RAISE(out_stream_, fs_->Create(channel_id_.GetPath(), /*overwrite=*/false));
    arrow_output_stream_adapter_ = std::make_shared<ArrowOutputStreamAdapter>(out_stream_);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        arrow_writer_,
        arrow::ipc::MakeFileWriter(arrow_output_stream_adapter_, schema_, ipc_write_options));
    spill_channel_manager_->AddChannel(channel_id_);
    cleanup_guard.Release();
    return Status::OK();
}

Status SpillWriter::WriteBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow_writer_->WriteRecordBatch(*batch));
    return Status::OK();
}

Status SpillWriter::Close() {
    if (closed_) {
        return Status::OK();
    }
    if (arrow_writer_) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow_writer_->Close());
    }
    if (out_stream_) {
        PAIMON_RETURN_NOT_OK(out_stream_->Close());
    }
    closed_ = true;
    return Status::OK();
}

Result<int64_t> SpillWriter::GetFileSize() const {
    if (channel_id_.GetPath().empty()) {
        return Status::Invalid("spill writer has no channel id");
    }
    if (!closed_ && arrow_output_stream_adapter_ != nullptr) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(int64_t file_size, arrow_output_stream_adapter_->Tell());
        return file_size;
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStatus> file_status,
                           fs_->GetFileStatus(channel_id_.GetPath()));
    return static_cast<int64_t>(file_status->GetLen());
}

const FileIOChannel::ID& SpillWriter::GetChannelId() const {
    return channel_id_;
}

}  // namespace paimon
