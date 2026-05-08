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
#include "paimon/core/global_index/global_index_evaluator.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/global_index/global_index_scan.h"

namespace paimon {
class GlobalIndexScanImpl : public GlobalIndexScan {
 public:
    static Result<std::unique_ptr<GlobalIndexScanImpl>> Create(
        const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
        const Snapshot& snapshot, const std::shared_ptr<PredicateFilter>& partitions,
        const CoreOptions& options, const std::shared_ptr<Executor>& executor,
        const std::shared_ptr<MemoryPool>& pool);

    Result<std::shared_ptr<GlobalIndexResult>> Scan(const std::shared_ptr<Predicate>& predicate);

    Result<std::vector<std::shared_ptr<GlobalIndexReader>>> CreateReaders(
        const std::string& field_name,
        const std::optional<RowRangeIndex>& row_range_index) const override;

    Result<std::vector<std::shared_ptr<GlobalIndexReader>>> CreateReaders(
        int32_t field_id, const std::optional<RowRangeIndex>& row_range_index) const override;

 private:
    /// (id->index_type->row_range) -> index meta list
    using IndexMetaMap =
        std::map<int32_t, std::map<std::string,
                                   std::map<Range, std::vector<std::shared_ptr<IndexFileMeta>>>>>;

    GlobalIndexScanImpl(const std::shared_ptr<TableSchema>& table_schema,
                        const CoreOptions& options,
                        const std::shared_ptr<IndexPathFactory>& path_factory,
                        IndexMetaMap&& index_metas, const std::shared_ptr<Executor>& executor,
                        const std::shared_ptr<MemoryPool>& pool);

    Result<std::shared_ptr<GlobalIndexEvaluator>> GetOrCreateIndexEvaluator();

    Result<std::vector<std::shared_ptr<GlobalIndexReader>>> CreateReaders(
        const DataField& field, const std::optional<RowRangeIndex>& row_range_index) const;

    std::vector<GlobalIndexIOMeta> ToGlobalIndexIOMetas(
        const std::vector<std::shared_ptr<IndexFileMeta>>& metas) const;

    GlobalIndexIOMeta ToGlobalIndexIOMeta(const std::shared_ptr<IndexFileMeta>& index_meta) const;

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::string root_path_;
    std::shared_ptr<TableSchema> table_schema_;
    CoreOptions options_;
    std::shared_ptr<GlobalIndexFileManager> index_file_manager_;
    IndexMetaMap index_metas_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<GlobalIndexEvaluator> evaluator_;
};

}  // namespace paimon
