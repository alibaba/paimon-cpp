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

#include "paimon/core/io/data_file_writer_factory.h"

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/core_options.h"
#include "paimon/format/file_format.h"
#include "paimon/format/writer_builder.h"

namespace paimon {

DataFileWriterFactory::DataFileWriterFactory(const CoreOptions& options, int64_t schema_id,
                                             const std::shared_ptr<MemoryPool>& pool)
    : options_(options), schema_id_(schema_id), pool_(pool) {}

Result<DataFileWriterFactory::WriterResources> DataFileWriterFactory::CreateWriterResources(
    const FileFormat& format, const std::shared_ptr<arrow::Schema>& file_schema,
    bool create_stats_extractor) const {
    WriterResources resources;
    {
        ::ArrowSchema arrow_schema;
        ArrowSchemaMarkReleased(&arrow_schema);
        ScopeGuard guard([&arrow_schema]() { ArrowSchemaRelease(&arrow_schema); });
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*file_schema, &arrow_schema));
        PAIMON_ASSIGN_OR_RAISE(
            resources.writer_builder,
            format.CreateWriterBuilder(&arrow_schema, options_.GetWriteBatchSize()));
        resources.writer_builder->WithMemoryPool(pool_);
    }
    if (create_stats_extractor) {
        ::ArrowSchema arrow_schema;
        ArrowSchemaMarkReleased(&arrow_schema);
        ScopeGuard guard([&arrow_schema]() { ArrowSchemaRelease(&arrow_schema); });
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*file_schema, &arrow_schema));
        PAIMON_ASSIGN_OR_RAISE(resources.stats_extractor,
                               format.CreateStatsExtractor(&arrow_schema));
    }
    return resources;
}

}  // namespace paimon
