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

#include "paimon/core/io/append_data_file_writer_factory.h"

#include <utility>

#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/fs/file_system.h"

namespace paimon {

AppendDataFileWriterFactory::AppendDataFileWriterFactory(
    const CoreOptions& options, int64_t schema_id,
    const std::shared_ptr<arrow::Schema>& write_schema,
    const std::optional<std::vector<std::string>>& write_cols,
    const std::shared_ptr<LongCounter>& seq_num_counter, FileSource file_source,
    const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::shared_ptr<MemoryPool>& pool)
    : DataFileWriterFactory(options, schema_id, pool),
      write_schema_(write_schema),
      write_cols_(write_cols),
      seq_num_counter_(seq_num_counter),
      file_source_(file_source),
      path_factory_(path_factory) {}

std::shared_ptr<LongCounter> AppendDataFileWriterFactory::ResolveSeqNumCounter() const {
    return options_.DataEvolutionEnabled() ? std::make_shared<LongCounter>(0) : seq_num_counter_;
}

Result<std::unique_ptr<SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>>
AppendDataFileWriterFactory::CreateWriter() const {
    std::shared_ptr<LongCounter> seq_num_counter = ResolveSeqNumCounter();
    PAIMON_ASSIGN_OR_RAISE(WriterResources resources,
                           CreateWriterResources(*options_.GetFileFormat(), write_schema_,
                                                 /*create_stats_extractor=*/true));
    auto writer = std::make_unique<DataFileWriter>(
        options_.GetFileCompression(), std::function<Status(::ArrowArray*, ::ArrowArray*)>(),
        schema_id_, seq_num_counter, file_source_, resources.stats_extractor,
        path_factory_->IsExternalPath(), write_cols_, pool_);
    PAIMON_RETURN_NOT_OK(
        writer->Init(options_.GetFileSystem(), path_factory_->NewPath(), resources.writer_builder));
    return std::unique_ptr<SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>(
        std::move(writer));
}

}  // namespace paimon
