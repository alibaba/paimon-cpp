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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/core/core_options.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/read_context.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class Executor;
class MemoryPool;
class Predicate;

// internal read context, contains ReadContext, CoreOptions and TableSchema
class InternalReadContext {
 public:
    static Result<std::unique_ptr<InternalReadContext>> Create(
        const std::shared_ptr<ReadContext>& read_context,
        const std::shared_ptr<TableSchema>& table_schema,
        const std::map<std::string, std::string>& options);

    const CoreOptions& GetCoreOptions() const {
        return options_;
    }
    std::shared_ptr<arrow::Schema> GetReadSchema() const {
        return read_schema_;
    }
    const std::shared_ptr<TableSchema>& GetTableSchema() const {
        return table_schema_;
    }
    const std::string& GetPath() const {
        return read_context_->GetPath();
    }
    const std::vector<std::string>& GetPartitionKeys() const {
        return table_schema_->PartitionKeys();
    }
    const std::vector<std::string>& GetPrimaryKeys() const {
        return table_schema_->PrimaryKeys();
    }
    const std::shared_ptr<Predicate>& GetPredicate() const {
        return read_context_->GetPredicate();
    }
    bool EnablePredicateFilter() const {
        return read_context_->EnablePredicateFilter();
    }
    bool EnablePrefetch() const {
        return read_context_->EnablePrefetch();
    }
    uint32_t GetPrefetchBatchCount() const {
        return read_context_->GetPrefetchBatchCount();
    }
    uint32_t GetPrefetchMaxParallelNum() const {
        return read_context_->GetPrefetchMaxParallelNum();
    }
    bool EnableMultiThreadRowToBatch() const {
        return read_context_->EnableMultiThreadRowToBatch();
    }
    uint32_t GetRowToBatchThreadNumber() const {
        return read_context_->GetRowToBatchThreadNumber();
    }
    std::shared_ptr<MemoryPool> GetMemoryPool() const {
        return read_context_->GetMemoryPool();
    }
    std::shared_ptr<Executor> GetExecutor() const {
        return read_context_->GetExecutor();
    }

    PrefetchCacheMode GetPrefetchCacheMode() const {
        return read_context_->GetPrefetchCacheMode();
    }

    const CacheConfig& GetCacheConfig() const {
        return read_context_->GetCacheConfig();
    }

 private:
    InternalReadContext(const std::shared_ptr<ReadContext>& read_context,
                        const std::shared_ptr<TableSchema>& table_schema,
                        const std::shared_ptr<arrow::Schema>& read_schema,
                        const CoreOptions& options);

    std::shared_ptr<ReadContext> read_context_;
    std::shared_ptr<TableSchema> table_schema_;
    std::shared_ptr<arrow::Schema> read_schema_;
    CoreOptions options_;
};

}  // namespace paimon
