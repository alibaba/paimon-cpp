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
#include <string>
#include <vector>

#include "paimon/core/core_options.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
namespace paimon {
class FileStorePathFactoryCache {
 public:
    FileStorePathFactoryCache(const std::string& root,
                              const std::shared_ptr<TableSchema>& table_schema,
                              const CoreOptions& options, const std::shared_ptr<MemoryPool>& pool)
        : root_(root), table_schema_(table_schema), options_(options), pool_(pool) {}

    Result<std::shared_ptr<FileStorePathFactory>> GetOrCreatePathFactory(
        const std::string& format) {
        auto iter = format_to_path_factory_.find(format);
        if (iter != format_to_path_factory_.end()) {
            return iter->second;
        }
        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema_->Fields());
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                               options_.CreateExternalPaths());
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                               options_.CreateGlobalIndexExternalPath());

        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FileStorePathFactory> path_factory,
            FileStorePathFactory::Create(
                root_, arrow_schema, table_schema_->PartitionKeys(),
                options_.GetPartitionDefaultName(), format, options_.DataFilePrefix(),
                options_.LegacyPartitionNameEnabled(), external_paths, global_index_external_path,
                options_.IndexFileInDataFileDir(), pool_));
        format_to_path_factory_[format] = path_factory;
        return path_factory;
    }

    const std::string& RootPath() const {
        return root_;
    }

 private:
    std::string root_;
    std::shared_ptr<TableSchema> table_schema_;
    CoreOptions options_;
    std::shared_ptr<MemoryPool> pool_;
    std::map<std::string, std::shared_ptr<FileStorePathFactory>> format_to_path_factory_;
};
}  // namespace paimon
