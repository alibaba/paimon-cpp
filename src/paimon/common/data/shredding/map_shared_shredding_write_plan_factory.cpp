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

#include "paimon/common/data/shredding/map_shared_shredding_write_plan_factory.h"

#include <utility>

#include "paimon/common/data/shredding/map_shared_shredding_batch_converter.h"
#include "paimon/common/data/shredding/map_shared_shredding_context.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/common/data/shredding/map_shredding_defs.h"

namespace paimon {

Result<std::shared_ptr<MapSharedShreddingWritePlanFactory>>
MapSharedShreddingWritePlanFactory::Create(const CoreOptions& options,
                                           const std::shared_ptr<arrow::Schema>& write_schema,
                                           const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> shredding_fields,
                           MapSharedShreddingUtils::DetectShreddingColumns(write_schema, options));
    std::map<std::string, int32_t> field_to_max_columns;
    PAIMON_ASSIGN_OR_RAISE(field_to_max_columns, MapSharedShreddingUtils::BuildColumnToNumColumns(
                                                     shredding_fields, options));
    return std::shared_ptr<MapSharedShreddingWritePlanFactory>(
        new MapSharedShreddingWritePlanFactory(options, write_schema, field_to_max_columns, pool));
}

MapSharedShreddingWritePlanFactory::MapSharedShreddingWritePlanFactory(
    const CoreOptions& options, const std::shared_ptr<arrow::Schema>& write_schema,
    const std::map<std::string, int32_t>& field_to_max_columns,
    const std::shared_ptr<MemoryPool>& pool)
    : options_(options),
      write_schema_(write_schema),
      field_to_max_columns_(field_to_max_columns),
      context_(std::make_shared<MapSharedShreddingContext>(field_to_max_columns)),
      pool_(pool) {}

bool MapSharedShreddingWritePlanFactory::ShouldCreateWritePlan() const {
    return !field_to_max_columns_.empty();
}

bool MapSharedShreddingWritePlanFactory::ShouldInferWritePlan() const {
    return false;
}

int32_t MapSharedShreddingWritePlanFactory::InferBufferRowCount() const {
    return 0;
}

Result<std::shared_ptr<ShreddingBatchConverter>>
MapSharedShreddingWritePlanFactory::CreateConverter(
    const std::string& file_format_identifier,
    const std::vector<std::shared_ptr<arrow::Array>>& sample_batches) {
    if (!ShouldCreateWritePlan()) {
        return Status::Invalid("MAP shared-shredding write plan is not active.");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<MapSharedShreddingBatchConverter> converter,
        MapSharedShreddingBatchConverter::Create(write_schema_, context_, options_, pool_));
    return std::shared_ptr<ShreddingBatchConverter>(std::move(converter));
}

ShreddingWritePlanFactory::MetadataFinalizer
MapSharedShreddingWritePlanFactory::CreateMetadataFinalizer(
    const std::shared_ptr<ShreddingBatchConverter>& converter,
    const std::string& compression) const {
    // The converter is created by CreateConverter above; the concrete type is guaranteed.
    auto map_converter = std::static_pointer_cast<MapSharedShreddingBatchConverter>(converter);
    return MapSharedShreddingUtils::BuildMetadataFinalizer(map_converter, compression,
                                                           map_converter->GetPhysicalSchema());
}

Status MapSharedShreddingWritePlanFactory::OnFileCompleted(
    const std::shared_ptr<ShreddingBatchConverter>& converter) {
    auto map_converter = std::dynamic_pointer_cast<MapSharedShreddingBatchConverter>(converter);
    if (map_converter == nullptr) {
        return Status::Invalid("Unexpected converter for MAP shared-shredding.");
    }
    std::vector<std::pair<std::string, int32_t>> completed_stats;
    for (const std::string& field_name : map_converter->GetShreddingColumnNames()) {
        PAIMON_ASSIGN_OR_RAISE(int32_t max_row_width, map_converter->GetMaxRowWidth(field_name));
        completed_stats.emplace_back(field_name, max_row_width);
    }
    for (const auto& [field_name, max_row_width] : completed_stats) {
        context_->ReportFileStats(field_name, max_row_width);
    }
    return Status::OK();
}

}  // namespace paimon
