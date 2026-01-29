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
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/core/options/changelog_producer.h"
#include "paimon/core/options/external_path_strategy.h"
#include "paimon/core/options/merge_engine.h"
#include "paimon/core/options/sort_engine.h"
#include "paimon/format/file_format.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"
#include "paimon/table/source/startup_mode.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {

class ExpireConfig;

class PAIMON_EXPORT CoreOptions {
 public:
    static Result<CoreOptions> FromMap(
        const std::map<std::string, std::string>& options_map,
        const std::shared_ptr<FileSystem>& specified_file_system = nullptr,
        const std::map<std::string, std::string>& fs_scheme_to_identifier_map = {});

    CoreOptions();
    CoreOptions(const CoreOptions&);
    CoreOptions& operator=(const CoreOptions&);
    ~CoreOptions();

    int32_t GetBucket() const;
    std::shared_ptr<FileFormat> GetWriteFileFormat() const;
    std::shared_ptr<FileSystem> GetFileSystem() const;
    const std::string& GetFileCompression() const;
    int32_t GetFileCompressionZstdLevel() const;
    int64_t GetPageSize() const;
    int64_t GetTargetFileSize() const;
    int64_t GetBlobTargetFileSize() const;
    std::string GetPartitionDefaultName() const;

    std::shared_ptr<FileFormat> GetManifestFormat() const;
    const std::string& GetManifestCompression() const;
    int32_t GetManifestMergeMinCount() const;
    int64_t GetManifestFullCompactionThresholdSize() const;
    int64_t GetSourceSplitTargetSize() const;
    int64_t GetSourceSplitOpenFileCost() const;
    std::optional<int64_t> GetScanSnapshotId() const;

    int64_t GetManifestTargetFileSize() const;
    StartupMode GetStartupMode() const;

    int32_t GetReadBatchSize() const;
    int32_t GetWriteBatchSize() const;
    int64_t GetWriteBufferSize() const;

    const ExpireConfig& GetExpireConfig() const;

    int64_t GetCommitTimeout() const;
    int32_t GetCommitMaxRetries() const;

    const std::vector<std::string>& GetSequenceField() const;
    bool SequenceFieldSortOrderIsAscending() const;
    MergeEngine GetMergeEngine() const;
    SortEngine GetSortEngine() const;
    bool IgnoreDelete() const;

    std::optional<std::string> GetFieldsDefaultFunc() const;
    Result<std::optional<std::string>> GetFieldAggFunc(const std::string& field_name) const;
    Result<bool> FieldAggIgnoreRetract(const std::string& field_name) const;
    bool DeletionVectorsEnabled() const;
    ChangelogProducer GetChangelogProducer() const;
    bool NeedLookup() const;
    bool FileIndexReadEnabled() const;

    std::map<std::string, std::string> GetFieldsSequenceGroups() const;
    bool PartialUpdateRemoveRecordOnDelete() const;
    std::vector<std::string> GetPartialUpdateRemoveRecordOnSequenceGroup() const;

    std::optional<std::string> GetScanFallbackBranch() const;
    std::string GetBranch() const;

    ExternalPathStrategy GetExternalPathStrategy() const;
    Result<std::vector<std::string>> CreateExternalPaths() const;
    bool EnableAdaptivePrefetchStrategy() const;

    std::string DataFilePrefix() const;

    bool IndexFileInDataFileDir() const;

    bool RowTrackingEnabled() const;
    bool DataEvolutionEnabled() const;

    bool LegacyPartitionNameEnabled() const;

    bool GlobalIndexEnabled() const;
    Result<std::optional<std::string>> CreateGlobalIndexExternalPath() const;

    const std::map<std::string, std::string>& ToMap() const;

 private:
    std::optional<std::string> GetDataFileExternalPaths() const;
    std::optional<std::string> GetGlobalIndexExternalPath() const;

 private:
    struct Impl;

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
