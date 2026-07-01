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
#include <optional>
#include <string>
#include <vector>

#include "arrow/c/abi.h"
#include "paimon/core/io/data_file_writer.h"
#include "paimon/core/io/data_file_writer_factory.h"
#include "paimon/core/io/single_file_writer_factory.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class CoreOptions;
class DataFilePathFactory;
class LongCounter;
class MemoryPool;

class BlobDataFileWriterFactory
    : public DataFileWriterFactory,
      public SingleFileWriterFactory<::ArrowArray*, std::shared_ptr<DataFileMeta>> {
 public:
    BlobDataFileWriterFactory(const CoreOptions& options, int64_t schema_id,
                              const std::shared_ptr<arrow::Schema>& file_schema,
                              const std::optional<std::vector<std::string>>& write_cols,
                              const std::shared_ptr<LongCounter>& seq_num_counter,
                              const std::shared_ptr<DataFilePathFactory>& path_factory,
                              const std::shared_ptr<MemoryPool>& pool);

    Result<std::unique_ptr<SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>>
    CreateWriter() const override;

 private:
    std::shared_ptr<arrow::Schema> file_schema_;
    std::optional<std::vector<std::string>> write_cols_;
    std::shared_ptr<LongCounter> seq_num_counter_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
};

}  // namespace paimon
