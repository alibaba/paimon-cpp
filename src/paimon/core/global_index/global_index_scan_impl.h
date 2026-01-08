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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/core/core_options.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/global_index/global_index_scan.h"

namespace paimon {
class GlobalIndexScanImpl : public GlobalIndexScan {
 public:
    GlobalIndexScanImpl(const std::string& root_path,
                        const std::shared_ptr<TableSchema>& table_schema, const Snapshot& snapshot,
                        const std::shared_ptr<PredicateFilter>& partitions,
                        const CoreOptions& options, const std::shared_ptr<MemoryPool>& pool);

    Result<std::shared_ptr<RowRangeGlobalIndexScanner>> CreateRangeScan(
        const Range& range) override;

    Result<std::vector<Range>> GetRowRangeList() override;

    const Snapshot& GetSnapshot() const {
        return snapshot_;
    }

    Result<std::optional<std::shared_ptr<GlobalIndexResult>>> ParallelScan(
        const std::vector<Range>& ranges, const std::shared_ptr<Predicate>& predicate,
        const std::shared_ptr<VectorSearch>& vector_search,
        const std::shared_ptr<Executor>& executor);

 private:
    Status Scan();

 private:
    bool initialized_ = false;
    std::shared_ptr<MemoryPool> pool_;
    std::string root_path_;
    std::shared_ptr<TableSchema> table_schema_;
    Snapshot snapshot_;
    std::shared_ptr<PredicateFilter> partitions_;
    CoreOptions options_;
    std::shared_ptr<FileStorePathFactory> path_factory_;
    std::vector<IndexManifestEntry> entries_;
};

}  // namespace paimon
