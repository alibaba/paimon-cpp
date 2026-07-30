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

#include "paimon/common/data/shredding/shredding_write_plan_factories.h"

#include "paimon/common/data/shredding/map_shared_shredding_write_plan_factory.h"
#include "paimon/common/data/variant/variant_shredding_write_plan_factory.h"
#include "paimon/core/core_options.h"

namespace paimon {

Result<std::shared_ptr<ShreddingWritePlanFactory>> ShreddingWritePlanFactories::SelectActive(
    const CoreOptions& options, const std::shared_ptr<arrow::Schema>& write_schema,
    const std::shared_ptr<MemoryPool>& pool) {
    std::shared_ptr<ShreddingWritePlanFactory> active_factory;

    auto variant_factory = VariantShreddingWritePlanFactory::Create(options, write_schema, pool);
    if (variant_factory->ShouldCreateWritePlan()) {
        active_factory = std::move(variant_factory);
    }

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MapSharedShreddingWritePlanFactory> map_factory,
                           MapSharedShreddingWritePlanFactory::Create(options, write_schema, pool));
    if (map_factory->ShouldCreateWritePlan()) {
        if (active_factory != nullptr) {
            return Status::NotImplemented(
                "Composing multiple active shredding write plans is not supported.");
        }
        active_factory = std::move(map_factory);
    }

    return active_factory;
}

}  // namespace paimon
