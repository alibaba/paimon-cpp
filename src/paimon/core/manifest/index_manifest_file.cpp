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

#include "paimon/core/manifest/index_manifest_file.h"

#include <utility>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/manifest/index_manifest_entry_serializer.h"
#include "paimon/core/manifest/index_manifest_file_handler.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/object_serializer.h"
#include "paimon/core/utils/path_factory.h"
#include "paimon/core/utils/versioned_object_serializer.h"
#include "paimon/format/file_format.h"
#include "paimon/format/reader_builder.h"
#include "paimon/format/writer_builder.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {
class FileSystem;
class MemoryPool;

Result<std::unique_ptr<IndexManifestFile>> IndexManifestFile::Create(
    const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<FileFormat>& file_format,
    const std::string& compression, const std::shared_ptr<FileStorePathFactory>& path_factory,
    int32_t bucket_mode, const std::shared_ptr<MemoryPool>& pool, const CoreOptions& options) {
    std::shared_ptr<arrow::DataType> data_type =
        VersionedObjectSerializer<IndexManifestEntry>::VersionType(IndexManifestEntry::DataType());

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReaderBuilder> reader_builder,
                           file_format->CreateReaderBuilder(options.GetReadBatchSize()));
    reader_builder->WithMemoryPool(pool);
    ArrowSchema schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportType(*data_type, &schema));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<WriterBuilder> writer_builder,
                           file_format->CreateWriterBuilder(&schema, options.GetWriteBatchSize()));
    writer_builder->WithMemoryPool(pool);

    std::shared_ptr<PathFactory> index_manifest_file_factory =
        path_factory->CreateIndexManifestFileFactory();
    return std::unique_ptr<IndexManifestFile>(
        new IndexManifestFile(file_system, reader_builder, writer_builder, compression,
                              index_manifest_file_factory, bucket_mode, pool));
}

IndexManifestFile::IndexManifestFile(const std::shared_ptr<FileSystem>& file_system,
                                     const std::shared_ptr<ReaderBuilder>& reader_builder,
                                     const std::shared_ptr<WriterBuilder>& writer_builder,
                                     const std::string& compression,
                                     const std::shared_ptr<PathFactory>& path_factory,
                                     int32_t bucket_mode, const std::shared_ptr<MemoryPool>& pool)
    : ObjectsFile<IndexManifestEntry>(file_system, reader_builder, writer_builder,
                                      std::make_unique<IndexManifestEntrySerializer>(pool),
                                      compression, path_factory, pool),
      bucket_mode_(bucket_mode) {}

Result<std::optional<std::string>> IndexManifestFile::WriteIndexFiles(
    const std::optional<std::string>& previous_index_manifest,
    const std::vector<IndexManifestEntry>& new_index_files) {
    if (new_index_files.empty()) {
        return previous_index_manifest;
    }
    PAIMON_ASSIGN_OR_RAISE(std::string file,
                           IndexManifestFileHandler::Write(previous_index_manifest, new_index_files,
                                                           bucket_mode_, this));
    return std::optional<std::string>(file);
}

}  // namespace paimon
