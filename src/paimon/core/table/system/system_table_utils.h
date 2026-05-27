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

#include <cmath>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/core/core_options.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/snapshot.h"
#include "paimon/data/timestamp.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class BinaryRow;
class FileStorePathFactory;
class FileSystem;
class InternalArray;
class InternalRow;
class MemoryPool;
class TableSchema;

/// Shared base table metadata used by table-scoped system tables.
struct SystemTableContext {
    std::shared_ptr<FileSystem> fs;
    std::string table_path;
    std::string branch;
    std::shared_ptr<TableSchema> table_schema;
    std::map<std::string, std::string> options;
};

/// Utility methods shared by system table implementations.
class SystemTableUtils {
 public:
    SystemTableUtils() = delete;
    ~SystemTableUtils() = delete;

    static SystemTableContext CreateContext(std::shared_ptr<FileSystem> fs, std::string table_path,
                                            std::string branch);
    static SystemTableContext CreateContext(std::shared_ptr<FileSystem> fs, std::string table_path,
                                            std::string branch,
                                            std::shared_ptr<TableSchema> table_schema,
                                            std::map<std::string, std::string> options);
    static VariantType OptionalInt64Value(const std::optional<int64_t>& value);
    static VariantType StringValue(const std::string& value);
    static VariantType OptionalStringValue(const std::optional<std::string>& value);
    static Result<VariantType> LocalTimestampMillisValue(int64_t epoch_millis);
    static Result<VariantType> LocalTimestampMillisValue(const Timestamp& local_timestamp);

    static Result<CoreOptions> CreateCoreOptions(const SystemTableContext& context);
    static Result<std::shared_ptr<FileStorePathFactory>> CreatePathFactory(
        const SystemTableContext& context, const CoreOptions& core_options,
        const std::shared_ptr<MemoryPool>& pool);
    static Result<std::optional<Snapshot>> LatestSnapshot(const SystemTableContext& context);
    static Result<std::vector<ManifestFileMeta>> ReadDataManifests(
        const SystemTableContext& context, const Snapshot& snapshot,
        const std::shared_ptr<FileStorePathFactory>& path_factory, const CoreOptions& core_options,
        const std::shared_ptr<MemoryPool>& pool);
};

}  // namespace paimon
