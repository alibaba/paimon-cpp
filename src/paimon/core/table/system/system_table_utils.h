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

#include "paimon/common/data/data_define.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/snapshot.h"
#include "paimon/data/timestamp.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

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
    static std::map<std::string, std::string> MergeOptions(
        const std::shared_ptr<TableSchema>& table_schema,
        const std::map<std::string, std::string>& dynamic_options);
    static std::string DefaultBranch();
    static std::string LoadBranch(const std::map<std::string, std::string>& options);

    template <typename T>
    static Result<std::string> JsonString(const T& value) {
        rapidjson::Document document;
        auto json_value = RapidJsonUtil::SerializeValue(value, &document.GetAllocator());
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        if (!json_value.Accept(writer)) {
            return Status::Invalid("failed to serialize metadata system table value");
        }
        return std::string(buffer.GetString(), buffer.GetSize());
    }

    static Result<std::optional<int64_t>> OptionalLocalDateTimePartsToTimestampMillis(
        const std::optional<std::vector<int64_t>>& parts);
    static std::optional<std::string> OptionalDoubleToString(const std::optional<double_t>& value);
    static VariantType OptionalInt64Value(const std::optional<int64_t>& value);
    static VariantType StringValue(const std::string& value);
    static VariantType OptionalStringValue(const std::optional<std::string>& value);
    static VariantType TimestampMillisValue(int64_t value);
    static Result<VariantType> LocalTimestampMillisValue(int64_t epoch_millis);
    static Result<VariantType> LocalTimestampMillisValue(const Timestamp& local_timestamp);
    static VariantType OptionalTimestampMillisValue(const std::optional<int64_t>& value);

    static Result<CoreOptions> CreateCoreOptions(const SystemTableContext& context);
    static Result<std::shared_ptr<FileStorePathFactory>> CreatePathFactory(
        const SystemTableContext& context, const CoreOptions& core_options,
        const std::shared_ptr<MemoryPool>& pool);
    static Result<std::optional<Snapshot>> LatestSnapshot(const SystemTableContext& context);
    static Result<std::vector<ManifestFileMeta>> ReadDataManifests(
        const SystemTableContext& context, const Snapshot& snapshot,
        const std::shared_ptr<FileStorePathFactory>& path_factory, const CoreOptions& core_options,
        const std::shared_ptr<MemoryPool>& pool);
    static Result<std::vector<ManifestEntry>> ReadLatestDataFiles(
        const SystemTableContext& context,
        const std::shared_ptr<FileStorePathFactory>& path_factory, const CoreOptions& core_options,
        const std::shared_ptr<MemoryPool>& pool);

    static std::optional<std::string> OptionalBinaryRowString(const BinaryRow& row);
    static Result<VariantType> OptionalPartitionStringValue(
        const BinaryRow& row, const std::shared_ptr<arrow::Schema>& partition_schema);
    static Result<std::string> PartitionString(
        const std::shared_ptr<FileStorePathFactory>& path_factory, const BinaryRow& partition);
    static Result<std::string> FilePath(const std::shared_ptr<FileStorePathFactory>& path_factory,
                                        const ManifestEntry& entry, const DataFileMeta& file);
    static Result<std::string> FieldsValueMapString(const std::vector<DataField>& fields,
                                                    const InternalRow& row);
    static Result<std::string> NullValueCountsString(const std::vector<DataField>& fields,
                                                     const InternalArray& null_counts);
    static Result<std::shared_ptr<TableSchema>> LoadDataSchema(const SystemTableContext& context,
                                                               int64_t schema_id);
    static Result<std::vector<DataField>> ValueStatsFields(const SystemTableContext& context,
                                                           int64_t schema_id);
    static Result<std::shared_ptr<InternalArray>> WriteColsValue(
        const std::optional<std::vector<std::string>>& write_cols,
        const std::shared_ptr<MemoryPool>& pool);

 private:
    static Result<int64_t> LocalDateTimePartsToTimestampMillis(const std::vector<int64_t>& parts);
    static Result<std::optional<std::string>> OptionalPartitionString(
        const BinaryRow& row, const std::shared_ptr<arrow::Schema>& partition_schema);
    static Result<std::vector<ManifestEntry>> ReadLatestManifestEntries(
        const SystemTableContext& context,
        const std::shared_ptr<FileStorePathFactory>& path_factory, const CoreOptions& core_options,
        const std::shared_ptr<MemoryPool>& pool);
};

}  // namespace paimon
