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

#include "paimon/common/data/shredding/shredding_write_plan_factory.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class CoreOptions;
/// Composes the known shredding write-plan factories (MAP shared-shredding and VARIANT
/// shredding) and selects the one active for a write schema.
class ShreddingWritePlanFactories {
 public:
    /// Returns the single active write-plan factory for the write, or nullptr when no shredding
    /// applies. Each concrete factory detects whether it is active and owns its internal state.
    static Result<std::shared_ptr<ShreddingWritePlanFactory>> SelectActive(
        const CoreOptions& options, const std::shared_ptr<arrow::Schema>& write_schema,
        const std::shared_ptr<MemoryPool>& pool);
};

}  // namespace paimon
