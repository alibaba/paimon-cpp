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

#include "paimon/core/core_options.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/global_index/row_range_global_index_scanner.h"
namespace paimon {
class RowRangeGlobalIndexScannerImpl
    : public RowRangeGlobalIndexScanner,
      public std::enable_shared_from_this<RowRangeGlobalIndexScannerImpl> {
 public:
    using IndexManifestEntryGroup =
        std::map<int32_t, std::map<std::string, std::vector<IndexManifestEntry>>>;

    RowRangeGlobalIndexScannerImpl(const std::shared_ptr<TableSchema>& table_schema,
                                   const std::shared_ptr<IndexPathFactory>& path_factory,
                                   const IndexManifestEntryGroup& grouped_entries,
                                   const CoreOptions& options,
                                   const std::shared_ptr<MemoryPool>& pool);

    Result<std::shared_ptr<GlobalIndexEvaluator>> CreateIndexEvaluator() const override;

    /// @return nullptr if global index reader not exist or plugin mismatch
    Result<std::shared_ptr<GlobalIndexReader>> CreateReader(
        const std::string& field_name, const std::string& index_type) const override;

    Result<std::vector<std::shared_ptr<GlobalIndexReader>>> CreateReaders(
        const std::string& field_name) const override;

 private:
    Result<std::vector<std::shared_ptr<GlobalIndexReader>>> CreateReaders(int32_t field_id) const;
    Result<std::vector<std::shared_ptr<GlobalIndexReader>>> CreateReaders(
        const DataField& field) const;

    Result<std::shared_ptr<GlobalIndexReader>> CreateReader(
        const DataField& field, const std::string& index_type,
        const std::vector<IndexManifestEntry>& entries) const;

    static std::vector<GlobalIndexIOMeta> ToGlobalIndexIOMetas(
        const std::vector<IndexManifestEntry>& entries);

    static GlobalIndexIOMeta ToGlobalIndexIOMeta(const IndexManifestEntry& entry);

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<TableSchema> table_schema_;
    CoreOptions options_;
    IndexManifestEntryGroup grouped_entries_;
    std::shared_ptr<GlobalIndexFileManager> index_file_manager_;
};

}  // namespace paimon
