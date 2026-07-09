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
#include <optional>
#include <string>
#include <vector>

#include "paimon/common/data/shredding/shredding_write_plan_factory.h"
#include "paimon/core/io/append_data_file_writer_factory.h"

namespace paimon {

class DataFilePathFactory;
class LongCounter;
class MemoryPool;

/// Creates append data file writers that rewrite logical batches into a physical (shredded)
/// layout as planned by a `ShreddingWritePlanFactory` (MAP shared-shredding or VARIANT
/// shredding, configured or inferred).
class ShreddingAppendDataFileWriterFactory : public AppendDataFileWriterFactory {
 public:
    ShreddingAppendDataFileWriterFactory(
        const CoreOptions& options, int64_t schema_id,
        const std::shared_ptr<arrow::Schema>& write_schema,
        const std::optional<std::vector<std::string>>& write_cols,
        const std::shared_ptr<LongCounter>& seq_num_counter, FileSource file_source,
        const std::shared_ptr<DataFilePathFactory>& path_factory,
        const std::shared_ptr<ShreddingWritePlanFactory>& plan_factory,
        const std::shared_ptr<MemoryPool>& pool);

    Result<std::unique_ptr<SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>>
    CreateWriter() const override;

 private:
    Result<std::unique_ptr<SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>>
    CreateShreddedWriter(const std::shared_ptr<ShreddingBatchConverter>& converter) const;

    std::shared_ptr<ShreddingWritePlanFactory> plan_factory_;
};

}  // namespace paimon
