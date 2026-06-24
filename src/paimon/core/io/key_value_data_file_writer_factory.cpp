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

#include "paimon/core/io/key_value_data_file_writer_factory.h"

#include <functional>
#include <utility>

#include "arrow/c/helpers.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/key_value_data_file_writer.h"
#include "paimon/format/file_format.h"
#include "paimon/fs/file_system.h"

namespace paimon {

KeyValueDataFileWriterFactory::KeyValueDataFileWriterFactory(
    const CoreOptions& options, int64_t schema_id,
    const std::shared_ptr<arrow::Schema>& write_schema, int32_t level, FileSource file_source,
    const std::vector<std::string>& primary_keys,
    const std::shared_ptr<DataFilePathFactory>& path_factory, bool create_stats_extractor,
    const std::shared_ptr<MemoryPool>& pool)
    : DataFileWriterFactory(options, schema_id, pool),
      write_schema_(write_schema),
      level_(level),
      file_source_(file_source),
      primary_keys_(primary_keys),
      path_factory_(path_factory),
      create_stats_extractor_(create_stats_extractor) {}

Result<std::unique_ptr<SingleFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>>
KeyValueDataFileWriterFactory::CreateWriter() const {
    std::function<Status(KeyValueBatch&&, ::ArrowArray*)> converter =
        [](KeyValueBatch key_value_batch, ::ArrowArray* array) -> Status {
        ArrowArrayMove(key_value_batch.batch.get(), array);
        return Status::OK();
    };

    auto format = options_.GetWriteFileFormat(level_);
    PAIMON_ASSIGN_OR_RAISE(WriterResources resources,
                           CreateWriterResources(*format, write_schema_, create_stats_extractor_));
    auto writer = std::make_unique<KeyValueDataFileWriter>(
        options_.GetWriteFileCompression(level_), std::move(converter), schema_id_, level_,
        file_source_, primary_keys_, resources.stats_extractor, write_schema_,
        path_factory_->IsExternalPath(), pool_);
    PAIMON_RETURN_NOT_OK(
        writer->Init(options_.GetFileSystem(), path_factory_->NewPath(), resources.writer_builder));
    return std::unique_ptr<SingleFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>(
        std::move(writer));
}

}  // namespace paimon
