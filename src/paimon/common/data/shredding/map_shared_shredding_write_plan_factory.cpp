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

MapSharedShreddingWritePlanFactory::MapSharedShreddingWritePlanFactory(
    const CoreOptions& options, const std::shared_ptr<arrow::Schema>& write_schema,
    const std::shared_ptr<MapSharedShreddingContext>& context,
    const std::shared_ptr<MemoryPool>& pool)
    : options_(options), write_schema_(write_schema), context_(context), pool_(pool) {}

bool MapSharedShreddingWritePlanFactory::ShouldCreateWritePlan() const {
    return context_ != nullptr;
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
    const std::vector<std::shared_ptr<arrow::Array>>& sample_batches) const {
    if (context_ == nullptr) {
        return Status::Invalid("Shared-shredding write plan requires a shredding context.");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<MapSharedShreddingBatchConverter> converter,
        MapSharedShreddingBatchConverter::Create(write_schema_, context_, options_, pool_));
    return std::shared_ptr<ShreddingBatchConverter>(std::move(converter));
}

ShreddingWritePlanFactory::MetadataFinalizer
MapSharedShreddingWritePlanFactory::CreateMetadataFinalizer(
    const std::shared_ptr<ShreddingBatchConverter>& converter) const {
    // The converter is created by CreateConverter above; the concrete type is guaranteed.
    auto map_converter = std::static_pointer_cast<MapSharedShreddingBatchConverter>(converter);
    return MapSharedShreddingUtils::BuildMetadataFinalizer(
        map_converter, MapSharedShreddingDefine::kDefaultDictCompression, context_,
        map_converter->GetPhysicalSchema());
}

}  // namespace paimon
