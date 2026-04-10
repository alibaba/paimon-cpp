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

#include <memory>
#include <string>

#include "arrow/ipc/api.h"
#include "paimon/core/disk/file_io_channel.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class RecordBatch;
class Schema;
}  // namespace arrow

namespace paimon {

class ArrowOutputStreamAdapter;
class SpillChannelManager;

class SpillWriter {
 public:
    static Result<std::unique_ptr<SpillWriter>> Create(
        const std::shared_ptr<FileSystem>& fs, const std::shared_ptr<arrow::Schema>& schema,
        const std::shared_ptr<FileIOChannel::Enumerator>& channel_enumerator,
        const std::shared_ptr<SpillChannelManager>& spill_channel_manager,
        const std::string& compression, int32_t compression_level);

    SpillWriter(const SpillWriter&) = delete;
    SpillWriter& operator=(const SpillWriter&) = delete;

    Status WriteBatch(const std::shared_ptr<arrow::RecordBatch>& batch);
    Status Close();
    Result<int64_t> GetFileSize() const;
    const FileIOChannel::ID& GetChannelId() const;

 private:
    SpillWriter(const std::shared_ptr<FileSystem>& fs, const std::shared_ptr<arrow::Schema>& schema,
                const std::shared_ptr<FileIOChannel::Enumerator>& channel_enumerator,
                const std::shared_ptr<SpillChannelManager>& spill_channel_manager,
                const std::string& compression, int32_t compression_level);

    Status Open();

    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<FileIOChannel::Enumerator> channel_enumerator_;
    std::shared_ptr<SpillChannelManager> spill_channel_manager_;
    std::string compression_;
    int32_t compression_level_;
    std::shared_ptr<OutputStream> out_stream_;
    std::shared_ptr<ArrowOutputStreamAdapter> arrow_output_stream_adapter_;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> arrow_writer_;
    FileIOChannel::ID channel_id_;
    bool closed_ = false;
};

}  // namespace paimon
