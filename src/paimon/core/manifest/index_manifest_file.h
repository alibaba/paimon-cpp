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
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/core/core_options.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/objects_file.h"
#include "paimon/format/file_format.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

namespace paimon {
class CoreOptions;
class FileFormat;
class FileStorePathFactory;
class FileSystem;
class MemoryPool;
class PathFactory;
class ReaderBuilder;
class WriterBuilder;
struct IndexManifestEntry;

/// Index manifest file.
class IndexManifestFile : public ObjectsFile<IndexManifestEntry> {
 public:
    static Result<std::unique_ptr<IndexManifestFile>> Create(
        const std::shared_ptr<FileSystem>& file_system,
        const std::shared_ptr<FileFormat>& file_format, const std::string& compression,
        const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<MemoryPool>& pool, const CoreOptions& options);

    /// Write new index files to index manifest.
    Result<std::optional<std::string>> WriteIndexFiles(
        const std::optional<std::string>& previous_index_manifest,
        const std::vector<IndexManifestEntry>& new_index_files);

 private:
    IndexManifestFile(const std::shared_ptr<FileSystem>& file_system,
                      const std::shared_ptr<ReaderBuilder>& reader_builder,
                      const std::shared_ptr<WriterBuilder>& writer_builder,
                      const std::string& compression,
                      const std::shared_ptr<PathFactory>& path_factory,
                      const std::shared_ptr<MemoryPool>& pool);
};
}  // namespace paimon
