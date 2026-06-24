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

#include "paimon/core/io/shredding_key_value_data_file_writer_factory.h"

#include <functional>
#include <utility>

#include "arrow/c/helpers.h"
#include "paimon/common/data/shredding/map_shared_shredding_batch_converter.h"
#include "paimon/common/data/shredding/map_shared_shredding_context.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/common/data/shredding/map_shredding_defs.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/key_value_data_file_writer.h"
#include "paimon/format/file_format.h"
#include "paimon/fs/file_system.h"

namespace paimon {

ShreddingKeyValueDataFileWriterFactory::ShreddingKeyValueDataFileWriterFactory(
    const CoreOptions& options, int64_t schema_id,
    const std::shared_ptr<arrow::Schema>& write_schema, int32_t level, FileSource file_source,
    const std::vector<std::string>& primary_keys,
    const std::shared_ptr<DataFilePathFactory>& path_factory, bool create_stats_extractor,
    const std::shared_ptr<MapSharedShreddingContext>& shredding_context,
    const std::shared_ptr<MemoryPool>& pool)
    : KeyValueDataFileWriterFactory(options, schema_id, write_schema, level, file_source,
                                    primary_keys, path_factory, create_stats_extractor, pool),
      shredding_context_(shredding_context) {}

Result<std::unique_ptr<SingleFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>>
ShreddingKeyValueDataFileWriterFactory::CreateWriter() const {
    PAIMON_ASSIGN_OR_RAISE(MapSharedShreddingBatchConverter::ConverterBundle bundle,
                           MapSharedShreddingBatchConverter::CreateConverter(
                               write_schema_, shredding_context_, pool_));
    if (!bundle.converter || !bundle.physical_schema) {
        return Status::Invalid(
            "Shared-shredding key-value writer requires a converter and physical schema.");
    }
    std::shared_ptr<arrow::Schema> file_schema = bundle.physical_schema;
    auto converter = bundle.converter;
    std::function<Status(KeyValueBatch&&, ::ArrowArray*)> batch_converter =
        [converter](KeyValueBatch key_value_batch, ::ArrowArray* array) -> Status {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowArray> physical,
                               converter->Convert(key_value_batch.batch.get()));
        ArrowArrayMove(physical.get(), array);
        return Status::OK();
    };

    auto format = options_.GetWriteFileFormat(level_);
    PAIMON_ASSIGN_OR_RAISE(WriterResources resources,
                           CreateWriterResources(*format, file_schema, create_stats_extractor_));
    auto writer = std::make_unique<KeyValueDataFileWriter>(
        options_.GetWriteFileCompression(level_), std::move(batch_converter), schema_id_, level_,
        file_source_, primary_keys_, resources.stats_extractor, file_schema,
        path_factory_->IsExternalPath(), pool_);
    PAIMON_RETURN_NOT_OK(
        writer->Init(options_.GetFileSystem(), path_factory_->NewPath(), resources.writer_builder));
    writer->SetMetadataFinalizer(MapSharedShreddingUtils::BuildMetadataFinalizer(
        bundle.converter, MapSharedShreddingDefine::kDefaultDictCompression, shredding_context_,
        file_schema));
    return std::unique_ptr<SingleFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>(
        std::move(writer));
}

}  // namespace paimon
