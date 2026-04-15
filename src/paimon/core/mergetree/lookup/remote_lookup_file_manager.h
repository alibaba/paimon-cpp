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

#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/mergetree/lookup_levels.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"

namespace paimon {

/// Manager to manage remote files for lookup.
class RemoteLookupFileManager {
 public:
    RemoteLookupFileManager(int32_t level_threshold,
                            const std::shared_ptr<DataFilePathFactory>& path_factory,
                            const std::shared_ptr<FileSystem>& file_system,
                            const std::shared_ptr<MemoryPool>& pool);

    template <typename T>
    Result<std::shared_ptr<DataFileMeta>> GenRemoteLookupFile(
        const std::shared_ptr<DataFileMeta>& file, LookupLevels<T>* lookup_levels) const;

    bool TryToDownload(const std::shared_ptr<DataFileMeta>& data_file,
                       const std::string& remote_sst_file, const std::string& local_file) const;

 private:
    std::string RemoteSstPath(const std::shared_ptr<DataFileMeta>& file,
                              const std::string& remote_sst_name) const;

    Status CopyRemoteToLocal(const std::string& remote_path, const std::string& local_path) const;

    Status CopyFromInputToOutput(std::unique_ptr<InputStream>&& input_stream,
                                 std::unique_ptr<OutputStream>&& output_stream) const;

    static constexpr uint64_t kBufferSize = 1024 * 1024;

 private:
    int32_t level_threshold_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
    std::shared_ptr<FileSystem> file_system_;
};

}  // namespace paimon
