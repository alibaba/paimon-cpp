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

#include "paimon/core/manifest/manifest_list.h"

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/manifest/manifest_file_meta_serializer.h"
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
class MemoryPool;

ManifestList::ManifestList(const std::shared_ptr<FileSystem>& file_system,
                           const std::shared_ptr<ReaderBuilder>& reader_builder,
                           const std::shared_ptr<WriterBuilder>& writer_builder,
                           const std::string& compression,
                           const std::shared_ptr<PathFactory>& path_factory,
                           const std::shared_ptr<Cache>& cache,
                           const std::shared_ptr<MemoryPool>& pool)
    : ObjectsFile<ManifestFileMeta>(file_system, reader_builder, writer_builder,
                                    std::make_unique<ManifestFileMetaSerializer>(pool), compression,
                                    std::move(path_factory), cache, pool) {}

Result<std::unique_ptr<ManifestList>> ManifestList::Create(
    const std::shared_ptr<FileSystem>& fs, const std::shared_ptr<FileFormat>& file_format,
    const std::string& compression, const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<Cache>& cache, const std::shared_ptr<MemoryPool>& pool) {
    std::shared_ptr<arrow::DataType> data_type =
        VersionedObjectSerializer<ManifestFileMeta>::VersionType(ManifestFileMeta::DataType());
    // prepare format reader builder
    // TODO(jinli.zjw) pass batch size
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReaderBuilder> reader_builder,
                           file_format->CreateReaderBuilder(/*batch_size=*/1024));
    reader_builder->WithMemoryPool(pool);

    // prepare format writer builder
    ArrowSchema schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportType(*data_type, &schema));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<WriterBuilder> writer_builder,
                           file_format->CreateWriterBuilder(&schema, /*batch_size=*/1024));
    writer_builder->WithMemoryPool(pool);

    // create manifest list
    std::shared_ptr<PathFactory> manifest_list_path_factory =
        path_factory->CreateManifestListFactory();
    return std::unique_ptr<ManifestList>(new ManifestList(
        fs, reader_builder, writer_builder, compression, manifest_list_path_factory, cache, pool));
}

Result<std::pair<std::string, int64_t>> ManifestList::Write(
    const std::vector<ManifestFileMeta>& metas) {
    return WriteWithoutRolling(metas);
}

}  // namespace paimon
