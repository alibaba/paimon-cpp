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

#include <memory>
#include <string>
#include <vector>

#include "paimon/core/io/key_value_data_file_writer_factory.h"

namespace paimon {

class DataFilePathFactory;
class MapSharedShreddingContext;
class MemoryPool;

class ShreddingKeyValueDataFileWriterFactory : public KeyValueDataFileWriterFactory {
 public:
    ShreddingKeyValueDataFileWriterFactory(
        const CoreOptions& options, int64_t schema_id,
        const std::shared_ptr<arrow::Schema>& write_schema, int32_t level, FileSource file_source,
        const std::vector<std::string>& primary_keys,
        const std::shared_ptr<DataFilePathFactory>& path_factory, bool create_stats_extractor,
        const std::shared_ptr<MapSharedShreddingContext>& shredding_context,
        const std::shared_ptr<MemoryPool>& pool);

    Result<std::unique_ptr<SingleFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>>
    CreateWriter() const override;

 private:
    std::shared_ptr<MapSharedShreddingContext> shredding_context_;
};

}  // namespace paimon
