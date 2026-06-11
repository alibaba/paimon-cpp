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
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "paimon/catalog/identifier.h"
#include "paimon/common/catalog/catalog_context.h"
#include "paimon/common/data/blob_view_struct.h"
#include "paimon/memory/bytes.h"
#include "paimon/result.h"
#include "paimon/utils/range.h"

namespace paimon {

class BatchReader;

/// Provide a function for converting {@link BlobViewStruct}s to {@link BlobDescriptor}s by scanning
/// the upstream tables in row-range chunks.
class BlobViewLookup {
 public:
    using DescriptorMapping = std::unordered_map<BlobViewStruct, std::shared_ptr<Bytes>>;
    /// The minimum number of rows handled by a single parallel task.
    static constexpr int64_t MIN_ROW_PER_TASK = 100;

    BlobViewLookup() = delete;
    ~BlobViewLookup() = delete;

    static Result<BlobViewResolver> CreateResolver(
        const std::unordered_set<BlobViewStruct>& view_structs,
        const std::shared_ptr<CatalogContext>& catalog_context,
        const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Executor>& executor);

 private:
    class TableReadPlan {
     public:
        explicit TableReadPlan(const BlobViewStruct& view_struct);

        void Add(const BlobViewStruct& view_struct);
        const Identifier& GetIdentifier() const;
        std::vector<int32_t> GetFieldIds() const;
        std::vector<Range> GetSortedDistinctRanges() const;

     private:
        Identifier identifier_;
        std::set<int32_t> references_by_field_id_;
        std::vector<int64_t> row_ranges_;
    };

    static Result<DescriptorMapping> PreloadDescriptors(
        const std::unordered_set<BlobViewStruct>& view_structs,
        const std::shared_ptr<CatalogContext>& catalog_context,
        const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Executor>& executor);

    static Result<DescriptorMapping> LoadTableDescriptorChunk(
        const std::shared_ptr<CatalogContext>& catalog_context, const Identifier& identifier,
        const std::vector<int32_t>& field_ids, const std::vector<Range>& row_ranges,
        const std::shared_ptr<MemoryPool>& pool);

    static Status ExtractBlobDescriptors(const Identifier& identifier,
                                         const std::vector<int32_t>& field_ids,
                                         const std::shared_ptr<MemoryPool>& pool,
                                         BatchReader* reader, DescriptorMapping* mapping);

    static std::unordered_map<Identifier, TableReadPlan> GroupByIdentifier(
        const std::unordered_set<BlobViewStruct>& view_structs);

    static int64_t TargetRowsPerTask(
        const std::unordered_map<Identifier, TableReadPlan>& plan_by_identifier,
        uint32_t thread_num);

    static std::vector<std::vector<Range>> SplitRowRanges(const std::vector<Range>& row_ranges,
                                                          int64_t target_rows_per_task);

    static Result<std::string> GetTableLocation(
        const std::shared_ptr<CatalogContext>& catalog_context, const Identifier& identifier);
};

}  // namespace paimon
