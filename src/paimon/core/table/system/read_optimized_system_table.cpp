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

#include "paimon/core/table/system/read_optimized_system_table.h"

#include <memory>
#include <string>
#include <utility>

#include "paimon/common/types/data_field.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/source/read_optimized_scan_options.h"
#include "paimon/defs.h"
#include "paimon/read_context.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"

namespace paimon {

ReadOptimizedSystemTable::ReadOptimizedSystemTable(std::string table_path,
                                                   std::shared_ptr<TableSchema> table_schema,
                                                   std::map<std::string, std::string> options)
    : table_path_(std::move(table_path)),
      table_schema_(std::move(table_schema)),
      options_(std::move(options)) {}

std::string ReadOptimizedSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> ReadOptimizedSystemTable::ArrowSchema() const {
    return DataField::ConvertDataFieldsToArrowSchema(table_schema_->Fields());
}

std::map<std::string, std::string> ReadOptimizedSystemTable::ReadOptimizedOptions() const {
    auto options = options_;
    options[kReadOptimizedScanOption] = "true";
    return options;
}

Result<std::unique_ptr<TableScan>> ReadOptimizedSystemTable::NewScan(
    const std::shared_ptr<ScanContext>& context) const {
    auto options = ReadOptimizedOptions();
    ScanContextBuilder builder(table_path_);
    builder.SetOptions(options)
        .WithStreamingMode(context->IsStreamingMode())
        .WithMemoryPool(context->GetMemoryPool())
        .WithExecutor(context->GetExecutor())
        .WithFileSystem(context->GetSpecificFileSystem())
        .WithCache(context->GetCache());
    if (context->GetLimit().has_value()) {
        builder.SetLimit(context->GetLimit().value());
    }
    if (context->GetScanFilters()) {
        if (context->GetScanFilters()->GetBucketFilter().has_value()) {
            builder.SetBucketFilter(context->GetScanFilters()->GetBucketFilter().value());
        }
        builder.SetPartitionFilter(context->GetScanFilters()->GetPartitionFilters());
        builder.SetPredicate(context->GetScanFilters()->GetPredicate());
    }
    if (context->GetGlobalIndexResult()) {
        builder.SetGlobalIndexResult(context->GetGlobalIndexResult());
    }
    if (context->GetSpecificTableSchema().has_value()) {
        builder.SetTableSchema(context->GetSpecificTableSchema().value());
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> base_context, builder.Finish());
    return TableScan::Create(std::move(base_context));
}

Result<std::unique_ptr<TableRead>> ReadOptimizedSystemTable::NewRead(
    const std::shared_ptr<ReadContext>& context) const {
    auto options = options_;
    std::string branch = context->GetBranch();
    // SystemTableLoader injects Options::BRANCH when parsing paths such as `T$branch_dev$ro`.
    auto branch_iter = options.find(Options::BRANCH);
    if (branch_iter != options.end()) {
        branch = branch_iter->second;
    }
    ReadContextBuilder builder(table_path_);
    builder.SetOptions(options)
        .WithBranch(branch)
        .SetPredicate(context->GetPredicate())
        .EnablePredicateFilter(context->EnablePredicateFilter())
        .EnablePrefetch(context->EnablePrefetch())
        .SetPrefetchBatchCount(context->GetPrefetchBatchCount())
        .SetPrefetchMaxParallelNum(context->GetPrefetchMaxParallelNum())
        .EnableMultiThreadRowToBatch(context->EnableMultiThreadRowToBatch())
        .SetRowToBatchThreadNumber(context->GetRowToBatchThreadNumber())
        .WithMemoryPool(context->GetMemoryPool())
        .WithExecutor(context->GetExecutor())
        .WithFileSystem(context->GetSpecificFileSystem())
        .WithFileSystemSchemeToIdentifierMap(context->GetFileSystemSchemeToIdentifierMap())
        .SetPrefetchCacheMode(context->GetPrefetchCacheMode())
        .WithCacheConfig(context->GetCacheConfig())
        .WithCache(context->GetCache())
        .SetReadFieldNames(context->GetReadFieldNames())
        .SetReadFieldIds(context->GetReadFieldIds());
    if (context->GetSpecificTableSchema().has_value()) {
        builder.SetTableSchema(context->GetSpecificTableSchema().value());
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> base_context, builder.Finish());
    return TableRead::Create(std::move(base_context));
}

}  // namespace paimon
