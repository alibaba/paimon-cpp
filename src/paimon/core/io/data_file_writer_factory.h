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

#include <cstdint>
#include <memory>

#include "paimon/core/core_options.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class FileFormat;
class FormatStatsExtractor;
class MemoryPool;
class WriterBuilder;

class DataFileWriterFactory {
 public:
    DataFileWriterFactory(const CoreOptions& options, int64_t schema_id,
                          const std::shared_ptr<MemoryPool>& pool);
    virtual ~DataFileWriterFactory() = default;

 protected:
    struct WriterResources {
        std::shared_ptr<WriterBuilder> writer_builder;
        std::shared_ptr<FormatStatsExtractor> stats_extractor;
    };

    Result<WriterResources> CreateWriterResources(const FileFormat& format,
                                                  const std::shared_ptr<arrow::Schema>& file_schema,
                                                  bool create_stats_extractor) const;

    CoreOptions options_;
    int64_t schema_id_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon
