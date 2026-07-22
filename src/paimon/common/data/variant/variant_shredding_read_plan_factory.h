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

#include <map>
#include <memory>
#include <string>

#include "paimon/common/data/shredding/shredding_read_plan.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

/// Builds per-column read plans for VARIANT columns:
/// - a plain VARIANT read of a shredded file reassembles the full variant;
/// - a variant-access projection (struct whose children carry `__VARIANT_METADATA`
///   descriptions) extracts the described paths, reading only the required shredded
///   sub-columns from a shredded file (or the binary from an unshredded file).
class VariantShreddingReadPlanFactory {
 public:
    VariantShreddingReadPlanFactory() = delete;
    ~VariantShreddingReadPlanFactory() = delete;

    /// Creates the per-column read plans for the variant columns of `read_schema` against
    /// `file_schema`; the map is empty when no plan applies (no shredded file column and no
    /// variant-access projection).
    static Result<std::map<std::string, std::shared_ptr<ShreddingColumnReadPlan>>> CreateReadPlans(
        const std::shared_ptr<arrow::Schema>& read_schema,
        const std::shared_ptr<arrow::Schema>& file_schema, const std::shared_ptr<MemoryPool>& pool);
};

}  // namespace paimon
