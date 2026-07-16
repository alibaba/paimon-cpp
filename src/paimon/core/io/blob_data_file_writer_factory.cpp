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

#include "paimon/core/io/blob_data_file_writer_factory.h"

#include <functional>
#include <utility>

#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/file_system.h"

namespace paimon {

BlobDataFileWriterFactory::BlobDataFileWriterFactory(
    const CoreOptions& options, int64_t schema_id,
    const std::shared_ptr<arrow::Schema>& file_schema,
    const std::optional<std::vector<std::string>>& write_cols,
    const std::shared_ptr<LongCounter>& seq_num_counter,
    const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::shared_ptr<MemoryPool>& pool)
    : DataFileWriterFactory(options, schema_id, pool),
      file_schema_(file_schema),
      write_cols_(write_cols),
      seq_num_counter_(seq_num_counter),
      path_factory_(path_factory) {}

Result<std::unique_ptr<SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>>
BlobDataFileWriterFactory::CreateWriter() const {
    std::shared_ptr<LongCounter> seq_num_counter =
        options_.DataEvolutionEnabled() ? std::make_shared<LongCounter>(0) : seq_num_counter_;
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileFormat> format,
                           FileFormatFactory::Get("blob", options_.ToMap()));
    PAIMON_ASSIGN_OR_RAISE(WriterResources resources,
                           CreateWriterResources(*format, file_schema_,
                                                 /*create_stats_extractor=*/true));
    auto writer = std::make_unique<DataFileWriter>(
        /*compression=*/"none", std::function<Status(::ArrowArray*, ::ArrowArray*)>(), schema_id_,
        seq_num_counter, FileSource::Append(), resources.stats_extractor,
        path_factory_->IsExternalPath(), write_cols_, pool_);
    PAIMON_RETURN_NOT_OK(writer->Init(options_.GetFileSystem(), path_factory_->NewBlobPath(),
                                      resources.writer_builder));
    return std::unique_ptr<SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>(
        std::move(writer));
}

}  // namespace paimon
