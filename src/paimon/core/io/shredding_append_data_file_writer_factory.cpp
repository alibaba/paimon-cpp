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

#include "paimon/core/io/shredding_append_data_file_writer_factory.h"

#include <utility>

#include "arrow/c/helpers.h"
#include "paimon/common/data/shredding/map_shared_shredding_batch_converter.h"
#include "paimon/common/data/shredding/map_shared_shredding_context.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/common/data/shredding/map_shredding_defs.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_file_writer.h"
#include "paimon/fs/file_system.h"

namespace paimon {

ShreddingAppendDataFileWriterFactory::ShreddingAppendDataFileWriterFactory(
    const CoreOptions& options, int64_t schema_id,
    const std::shared_ptr<arrow::Schema>& write_schema,
    const std::optional<std::vector<std::string>>& write_cols,
    const std::shared_ptr<LongCounter>& seq_num_counter, FileSource file_source,
    const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::shared_ptr<MapSharedShreddingContext>& shredding_context,
    const std::shared_ptr<MemoryPool>& pool)
    : AppendDataFileWriterFactory(options, schema_id, write_schema, write_cols, seq_num_counter,
                                  file_source, path_factory, pool),
      shredding_context_(shredding_context) {}

Result<std::unique_ptr<SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>>
ShreddingAppendDataFileWriterFactory::CreateWriter() const {
    if (!shredding_context_) {
        return Status::Invalid("Shared-shredding append writer requires a shredding context.");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MapSharedShreddingBatchConverter> converter,
                           MapSharedShreddingBatchConverter::Create(
                               write_schema_, shredding_context_, options_, pool_));
    std::shared_ptr<arrow::Schema> file_schema = converter->GetPhysicalSchema();
    std::function<Status(::ArrowArray*, ::ArrowArray*)> batch_converter =
        [converter](::ArrowArray* input, ::ArrowArray* output) -> Status {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowArray> physical, converter->Convert(input));
        ArrowArrayMove(physical.get(), output);
        return Status::OK();
    };
    PAIMON_ASSIGN_OR_RAISE(WriterResources resources,
                           CreateWriterResources(*options_.GetFileFormat(), file_schema,
                                                 /*create_stats_extractor=*/true));
    auto writer = std::make_unique<DataFileWriter>(
        options_.GetFileCompression(), std::move(batch_converter), schema_id_, seq_num_counter_,
        file_source_, resources.stats_extractor, path_factory_->IsExternalPath(), write_cols_,
        pool_);
    PAIMON_RETURN_NOT_OK(
        writer->Init(options_.GetFileSystem(), path_factory_->NewPath(), resources.writer_builder));
    writer->SetMetadataFinalizer(MapSharedShreddingUtils::BuildMetadataFinalizer(
        converter, MapSharedShreddingDefine::kDefaultDictCompression, shredding_context_,
        file_schema));
    return std::unique_ptr<SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>(
        std::move(writer));
}

}  // namespace paimon
