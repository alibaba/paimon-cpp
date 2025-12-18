/*
 * Copyright 2024-present Alibaba Inc.
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

#include "paimon/common/utils/concurrent_hash_map.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats_evolution.h"

namespace paimon {
class SimpleStatsEvolutions {
 public:
    SimpleStatsEvolutions(const std::shared_ptr<TableSchema>& table_schema,
                          const std::shared_ptr<MemoryPool>& pool)
        : pool_(pool), table_schema_(table_schema) {}

    std::shared_ptr<SimpleStatsEvolution> GetOrCreate(
        const std::shared_ptr<TableSchema>& data_schema) {
        auto data_schema_id = data_schema->Id();
        auto cached_evolution = evolutions_.Find(data_schema_id);
        if (cached_evolution != std::nullopt) {
            return cached_evolution.value();
        }
        bool need_mapping = data_schema_id != table_schema_->Id();
        auto evolution = std::make_shared<SimpleStatsEvolution>(
            data_schema->Fields(), table_schema_->Fields(), need_mapping, pool_);
        evolutions_.Insert(data_schema_id, evolution);
        return evolution;
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<TableSchema> table_schema_;
    // scheme_id -> evolution
    ConcurrentHashMap<int64_t, std::shared_ptr<SimpleStatsEvolution>> evolutions_;
};
}  // namespace paimon
