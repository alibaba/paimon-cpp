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
#include "paimon/core/mergetree/lookup/remote_lookup_file_manager.h"

#include "fmt/format.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/mergetree/lookup/file_position.h"
#include "paimon/core/mergetree/lookup/positioned_key_value.h"
namespace paimon {

RemoteLookupFileManager::RemoteLookupFileManager(
    int32_t level_threshold, const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool)
    : level_threshold_(level_threshold),
      pool_(pool),
      path_factory_(path_factory),
      file_system_(file_system) {}

template <typename T>
Result<std::shared_ptr<DataFileMeta>> RemoteLookupFileManager::GenRemoteLookupFile(
    const std::shared_ptr<DataFileMeta>& file, LookupLevels<T>* lookup_levels) const {
    if (!lookup_levels) {
        return Status::Invalid("lookup_levels must not be null in GenRemoteLookupFile");
    }
    if (file->level < level_threshold_) {
        return file;
    }

    if (lookup_levels->RemoteSst(file).has_value()) {
        // ignore existed
        return file;
    }

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<LookupFile> lookup_file,
                           lookup_levels->CreateLookupFile(file));
    std::string local_file_path = lookup_file->LocalFile();

    // Get the file size from the local file system
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStatus> local_file_status,
                           file_system_->GetFileStatus(local_file_path));
    auto length = static_cast<int64_t>(local_file_status->GetLen());

    std::string remote_sst_name = lookup_levels->NewRemoteSst(file, length);
    std::string remote_sst_path = RemoteSstPath(file, remote_sst_name);

    // Upload local lookup file to remote storage
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> input_stream,
                           file_system_->Open(local_file_path));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> output_stream,
                           file_system_->Create(remote_sst_path, /*overwrite=*/true));

    PAIMON_RETURN_NOT_OK(CopyFromInputToOutput(std::move(input_stream), std::move(output_stream)));

    lookup_levels->AddLocalFile(file, lookup_file);

    std::vector<std::optional<std::string>> new_extra_files(file->extra_files);
    new_extra_files.push_back(remote_sst_name);
    return file->CopyWithExtraFiles(new_extra_files);
}

bool RemoteLookupFileManager::TryToDownload(const std::shared_ptr<DataFileMeta>& data_file,
                                            const std::string& remote_sst_file,
                                            const std::string& local_file) const {
    std::string remote_path = RemoteSstPath(data_file, remote_sst_file);

    auto status = CopyRemoteToLocal(remote_path, local_file);
    if (!status.ok()) {
        // Failed to download remote lookup file, skipping.
        return false;
    }
    return true;
}

std::string RemoteLookupFileManager::RemoteSstPath(const std::shared_ptr<DataFileMeta>& file,
                                                   const std::string& remote_sst_name) const {
    std::string data_file_path = path_factory_->ToPath(file);
    std::string parent_dir = PathUtil::GetParentDirPath(data_file_path);
    return PathUtil::JoinPath(parent_dir, remote_sst_name);
}

Status RemoteLookupFileManager::CopyRemoteToLocal(const std::string& remote_path,
                                                  const std::string& local_path) const {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> input_stream,
                           file_system_->Open(remote_path));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> output_stream,
                           file_system_->Create(local_path, /*overwrite=*/true));
    return CopyFromInputToOutput(std::move(input_stream), std::move(output_stream));
}

Status RemoteLookupFileManager::CopyFromInputToOutput(
    std::unique_ptr<InputStream>&& input_stream,
    std::unique_ptr<OutputStream>&& output_stream) const {
    auto buffer = std::make_shared<Bytes>(kBufferSize, pool_.get());
    PAIMON_ASSIGN_OR_RAISE(uint64_t total_length, input_stream->Length());
    uint64_t write_size = 0;
    while (write_size < total_length) {
        uint64_t current_read_size = std::min(total_length - write_size, kBufferSize);
        PAIMON_ASSIGN_OR_RAISE(int32_t bytes_read,
                               input_stream->Read(buffer->data(), current_read_size));
        if (static_cast<uint64_t>(bytes_read) != current_read_size) {
            return Status::Invalid(
                fmt::format("CopyFromInputToOutput failed: expected read {} bytes, while "
                            "actual read {} bytes",
                            current_read_size, bytes_read));
        }
        PAIMON_ASSIGN_OR_RAISE(int32_t bytes_written,
                               output_stream->Write(buffer->data(), bytes_read));
        if (bytes_written != bytes_read) {
            return Status::Invalid(
                fmt::format("CopyFromInputToOutput failed: expected write {} bytes, while "
                            "actual write {} bytes",
                            bytes_read, bytes_written));
        }
        write_size += current_read_size;
    }
    PAIMON_RETURN_NOT_OK(output_stream->Flush());
    PAIMON_RETURN_NOT_OK(output_stream->Close());
    PAIMON_RETURN_NOT_OK(input_stream->Close());
    return Status::OK();
}
template Result<std::shared_ptr<DataFileMeta>>
RemoteLookupFileManager::GenRemoteLookupFile<KeyValue>(const std::shared_ptr<DataFileMeta>&,
                                                       LookupLevels<KeyValue>*) const;
template Result<std::shared_ptr<DataFileMeta>>
RemoteLookupFileManager::GenRemoteLookupFile<FilePosition>(const std::shared_ptr<DataFileMeta>&,
                                                           LookupLevels<FilePosition>*) const;
template Result<std::shared_ptr<DataFileMeta>>
RemoteLookupFileManager::GenRemoteLookupFile<PositionedKeyValue>(
    const std::shared_ptr<DataFileMeta>&, LookupLevels<PositionedKeyValue>*) const;
template Result<std::shared_ptr<DataFileMeta>> RemoteLookupFileManager::GenRemoteLookupFile<bool>(
    const std::shared_ptr<DataFileMeta>&, LookupLevels<bool>*) const;

}  // namespace paimon
