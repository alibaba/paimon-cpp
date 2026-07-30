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

#include "paimon/core/manifest/manifest_file.h"

#include <cassert>
#include <limits>
#include <utility>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/io/rolling_file_writer.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_entry_serializer.h"
#include "paimon/core/manifest/manifest_entry_writer_factory.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/object_serializer.h"
#include "paimon/core/utils/path_factory.h"
#include "paimon/core/utils/versioned_object_serializer.h"
#include "paimon/format/file_format.h"
#include "paimon/format/reader_builder.h"
#include "paimon/format/writer_builder.h"
#include "paimon/status.h"

namespace arrow {
class DataType;
class Schema;
}  // namespace arrow

namespace paimon {
class MemoryPool;

ManifestFile::ManifestFile(const std::shared_ptr<FileSystem>& file_system,
                           const std::shared_ptr<ReaderBuilder>& reader_builder,
                           const std::shared_ptr<WriterBuilder>& writer_builder,
                           const std::string& compression,
                           const std::shared_ptr<PathFactory>& path_factory,
                           int64_t target_file_size, const std::shared_ptr<MemoryPool>& pool,
                           const CoreOptions& options,
                           const std::shared_ptr<arrow::Schema>& partition_type)
    : ObjectsFile<ManifestEntry>(file_system, reader_builder, writer_builder,
                                 std::make_unique<ManifestEntrySerializer>(pool), compression,
                                 path_factory, options.GetCache(), pool),
      target_file_size_(target_file_size),
      options_(options),
      partition_type_(partition_type) {}

Result<std::unique_ptr<ManifestFile>> ManifestFile::Create(
    const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<FileFormat>& file_format,
    const std::string& compression, const std::shared_ptr<FileStorePathFactory>& path_factory,
    int64_t target_file_size, const std::shared_ptr<MemoryPool>& pool, const CoreOptions& options,
    const std::shared_ptr<arrow::Schema>& partition_type) {
    assert(partition_type);
    std::shared_ptr<arrow::DataType> data_type =
        VersionedObjectSerializer<ManifestEntry>::VersionType(ManifestEntry::DataType());

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReaderBuilder> reader_builder,
                           file_format->CreateReaderBuilder(options.GetReadBatchSize()));
    reader_builder->WithMemoryPool(pool);
    ArrowSchema schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportType(*data_type, &schema));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<WriterBuilder> writer_builder,
                           file_format->CreateWriterBuilder(&schema, options.GetWriteBatchSize()));
    writer_builder->WithMemoryPool(pool);

    std::shared_ptr<PathFactory> manifest_file_factory = path_factory->CreateManifestFileFactory();
    return std::unique_ptr<ManifestFile>(
        new ManifestFile(file_system, reader_builder, writer_builder, compression,
                         manifest_file_factory, target_file_size, pool, options, partition_type));
}

Result<std::vector<ManifestFileMeta>> ManifestFile::Write(
    const std::vector<ManifestEntry>& entries) {
    if (entries.empty()) {
        return std::vector<ManifestFileMeta>();
    }
    auto converter = [this](ManifestEntry entry, ::ArrowArray* dest) -> Status {
        if (!to_array_converter_) {
            PAIMON_ASSIGN_OR_RAISE(to_array_converter_, MetaToArrowArrayConverter::Create(
                                                            serializer_->GetDataType(), pool_));
        }
        PAIMON_ASSIGN_OR_RAISE(BinaryRow entry_row, serializer_->ToRow(entry));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> array,
                               to_array_converter_->NextBatch({entry_row}));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, dest));
        return Status::OK();
    };

    auto writer_factory = std::make_shared<ManifestEntryWriterFactory>(
        options_.GetManifestCompression(), converter, pool_, partition_type_,
        options_.GetFileSystem(), path_factory_, writer_builder_);
    std::unique_ptr<RollingFileWriter<const ManifestEntry&, ManifestFileMeta>> writer =
        std::make_unique<RollingFileWriter<const ManifestEntry&, ManifestFileMeta>>(
            target_file_size_, /*target_file_row_num=*/std::numeric_limits<int64_t>::max(),
            writer_factory);
    for (const auto& entry : entries) {
        auto s = writer->Write(entry);
        if (!s.ok()) {
            writer->Abort();
            return s;
        }
    }
    PAIMON_RETURN_NOT_OK(writer->Close());
    return writer->GetResult();
}

}  // namespace paimon
