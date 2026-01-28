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

#include "paimon/core/core_options.h"

#include <limits>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/fs/resolving_file_system.h"
#include "paimon/core/options/expire_config.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {

TEST(CoreOptionsTest, TestDefaultValue) {
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap({}));
    ASSERT_EQ(core_options.GetManifestFormat()->Identifier(), "orc");
    ASSERT_EQ(core_options.GetWriteFileFormat()->Identifier(), "parquet");
    ASSERT_TRUE(core_options.GetFileSystem());
    ASSERT_EQ(-1, core_options.GetBucket());
    ASSERT_EQ(64 * 1024L, core_options.GetPageSize());
    ASSERT_EQ(256 * 1024 * 1024L, core_options.GetTargetFileSize());
    ASSERT_EQ(256 * 1024 * 1024L, core_options.GetBlobTargetFileSize());
    ASSERT_EQ("__DEFAULT_PARTITION__", core_options.GetPartitionDefaultName());
    ASSERT_EQ(std::nullopt, core_options.GetScanSnapshotId());
    ASSERT_EQ("zstd", core_options.GetFileCompression());
    ASSERT_EQ("zstd", core_options.GetManifestCompression());
    ASSERT_EQ(1, core_options.GetFileCompressionZstdLevel());
    ASSERT_EQ(StartupMode::LatestFull(), core_options.GetStartupMode());
    ASSERT_EQ(8 * 1024 * 1024L, core_options.GetManifestTargetFileSize());
    ASSERT_EQ(16 * 1024 * 1024L, core_options.GetManifestFullCompactionThresholdSize());
    ASSERT_EQ(30, core_options.GetManifestMergeMinCount());
    ASSERT_EQ(128 * 1024 * 1024L, core_options.GetSourceSplitTargetSize());
    ASSERT_EQ(4 * 1024 * 1024L, core_options.GetSourceSplitOpenFileCost());
    ASSERT_EQ(1024, core_options.GetReadBatchSize());
    ASSERT_EQ(1024, core_options.GetWriteBatchSize());
    ASSERT_EQ(256 * 1024 * 1024, core_options.GetWriteBufferSize());
    ASSERT_EQ(std::numeric_limits<int64_t>::max(), core_options.GetCommitTimeout());
    ASSERT_EQ(10, core_options.GetCommitMaxRetries());
    ExpireConfig expire_config = core_options.GetExpireConfig();
    ASSERT_EQ(10, expire_config.GetSnapshotRetainMin());
    ASSERT_EQ(std::numeric_limits<int32_t>::max(), expire_config.GetSnapshotRetainMax());
    ASSERT_EQ(10, expire_config.GetSnapshotMaxDeletes());
    ASSERT_FALSE(expire_config.CleanEmptyDirectories());
    ASSERT_EQ(1 * 3600 * 1000L, expire_config.GetSnapshotTimeRetainMs());
    ASSERT_EQ(std::vector<std::string>(), core_options.GetSequenceField());
    ASSERT_TRUE(core_options.SequenceFieldSortOrderIsAscending());
    ASSERT_EQ(MergeEngine::DEDUPLICATE, core_options.GetMergeEngine());
    ASSERT_EQ(SortEngine::LOSER_TREE, core_options.GetSortEngine());
    ASSERT_FALSE(core_options.IgnoreDelete());
    ASSERT_EQ(std::nullopt, core_options.GetFieldsDefaultFunc());
    ASSERT_EQ(std::nullopt, core_options.GetFieldAggFunc("f0").value());
    ASSERT_FALSE(core_options.FieldAggIgnoreRetract("f1").value());
    ASSERT_FALSE(core_options.DeletionVectorsEnabled());
    ASSERT_EQ(ChangelogProducer::NONE, core_options.GetChangelogProducer());
    ASSERT_FALSE(core_options.NeedLookup());
    ASSERT_TRUE(core_options.GetFieldsSequenceGroups().empty());
    ASSERT_FALSE(core_options.PartialUpdateRemoveRecordOnDelete());
    ASSERT_TRUE(core_options.GetPartialUpdateRemoveRecordOnSequenceGroup().empty());
    ASSERT_EQ(std::nullopt, core_options.GetScanFallbackBranch());
    ASSERT_EQ("main", core_options.GetBranch());
    ASSERT_TRUE(core_options.FileIndexReadEnabled());
    ASSERT_EQ(std::nullopt, core_options.GetDataFileExternalPaths());
    ASSERT_EQ(ExternalPathStrategy::NONE, core_options.GetExternalPathStrategy());
    ASSERT_TRUE(core_options.EnableAdaptivePrefetchStrategy());
    ASSERT_EQ(core_options.DataFilePrefix(), "data-");
    ASSERT_FALSE(core_options.IndexFileInDataFileDir());
    ASSERT_FALSE(core_options.RowTrackingEnabled());
    ASSERT_FALSE(core_options.DataEvolutionEnabled());
    ASSERT_TRUE(core_options.LegacyPartitionNameEnabled());
    ASSERT_TRUE(core_options.GlobalIndexEnabled());
    ASSERT_FALSE(core_options.GetGlobalIndexExternalPath());
}

TEST(CoreOptionsTest, TestFromMap) {
    std::map<std::string, std::string> options = {
        {Options::FILE_SYSTEM, "Local"},
        {Options::FILE_FORMAT, "ORC"},
        {Options::MANIFEST_FORMAT, "avRo"},
        {Options::BUCKET, "3"},
        {Options::PAGE_SIZE, "128 kb"},
        {Options::TARGET_FILE_SIZE, "512MB"},
        {Options::BLOB_TARGET_FILE_SIZE, "1G"},
        {Options::PARTITION_DEFAULT_NAME, "foo"},
        {Options::MANIFEST_TARGET_FILE_SIZE, "16MB"},
        {Options::MANIFEST_FULL_COMPACTION_FILE_SIZE, "32MB"},
        {Options::MANIFEST_MERGE_MIN_COUNT, "2"},
        {Options::SOURCE_SPLIT_TARGET_SIZE, "24MB"},
        {Options::SOURCE_SPLIT_OPEN_FILE_COST, "32MB"},
        {Options::READ_BATCH_SIZE, "2048"},
        {Options::WRITE_BUFFER_SIZE, "16MB"},
        {Options::WRITE_BATCH_SIZE, "1234"},
        {Options::COMMIT_TIMEOUT, "120s"},
        {Options::COMMIT_MAX_RETRIES, "20"},
        {Options::SCAN_SNAPSHOT_ID, "5"},
        {Options::SNAPSHOT_NUM_RETAINED_MIN, "15"},
        {Options::SNAPSHOT_NUM_RETAINED_MAX, "30"},
        {Options::SNAPSHOT_EXPIRE_LIMIT, "20"},
        {Options::SNAPSHOT_TIME_RETAINED, "2h"},
        {Options::SNAPSHOT_CLEAN_EMPTY_DIRECTORIES, "true"},
        {Options::SEQUENCE_FIELD, "f1,f2,f3"},
        {Options::SEQUENCE_FIELD_SORT_ORDER, "descending"},
        {Options::MERGE_ENGINE, "partial-update"},
        {Options::SORT_ENGINE, "min-heap"},
        {Options::IGNORE_DELETE, "true"},
        {Options::FIELDS_DEFAULT_AGG_FUNC, "sum"},
        {"fields.f0.aggregate-function", "min"},
        {"fields.f1.ignore-retract", "true"},
        {Options::DELETION_VECTORS_ENABLED, "true"},
        {Options::CHANGELOG_PRODUCER, "full-compaction"},
        {Options::FORCE_LOOKUP, "true"},
        {"fields.g_1,g_3.sequence-group", "c,d"},
        {Options::PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE, "true"},
        {Options::PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP, "a,b"},
        {Options::SCAN_FALLBACK_BRANCH, "fallback"},
        {Options::BRANCH, "rt"},
        {Options::FILE_INDEX_READ_ENABLED, "false"},
        {Options::DATA_FILE_EXTERNAL_PATHS, "FILE:///tmp/index"},
        {Options::DATA_FILE_EXTERNAL_PATHS_STRATEGY, "round-robin"},
        {Options::FILE_COMPRESSION, "snappy"},
        {Options::MANIFEST_COMPRESSION, "zlib"},
        {Options::FILE_COMPRESSION_ZSTD_LEVEL, "2"},
        {"test.enable-adaptive-prefetch-strategy", "false"},
        {Options::DATA_FILE_PREFIX, "test-data-"},
        {Options::INDEX_FILE_IN_DATA_FILE_DIR, "true"},
        {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::PARTITION_GENERATE_LEGACY_NAME, "false"},
        {Options::GLOBAL_INDEX_ENABLED, "false"},
        {Options::GLOBAL_INDEX_EXTERNAL_PATH, "FILE:///tmp/global_index/"},
    };

    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    auto fs = core_options.GetFileSystem();
    ASSERT_TRUE(fs);

    auto format = core_options.GetWriteFileFormat();
    ASSERT_EQ(format->Identifier(), "orc");

    auto manifest_format = core_options.GetManifestFormat();
    ASSERT_EQ(manifest_format->Identifier(), "avro");

    ASSERT_EQ(3, core_options.GetBucket());
    ASSERT_EQ(128 * 1024L, core_options.GetPageSize());
    ASSERT_EQ(512 * 1024 * 1024L, core_options.GetTargetFileSize());
    ASSERT_EQ(1024 * 1024 * 1024L, core_options.GetBlobTargetFileSize());
    ASSERT_EQ("foo", core_options.GetPartitionDefaultName());
    ASSERT_EQ(16 * 1024 * 1024L, core_options.GetManifestTargetFileSize());
    ASSERT_EQ(32 * 1024 * 1024L, core_options.GetManifestFullCompactionThresholdSize());
    ASSERT_EQ(2, core_options.GetManifestMergeMinCount());
    ASSERT_EQ(24 * 1024 * 1024L, core_options.GetSourceSplitTargetSize());
    ASSERT_EQ(32 * 1024 * 1024L, core_options.GetSourceSplitOpenFileCost());
    ASSERT_EQ(2048, core_options.GetReadBatchSize());
    ASSERT_EQ(1234, core_options.GetWriteBatchSize());
    ASSERT_EQ(16 * 1024 * 1024, core_options.GetWriteBufferSize());
    ASSERT_EQ(120 * 1000, core_options.GetCommitTimeout());
    ASSERT_EQ(20, core_options.GetCommitMaxRetries());
    ASSERT_EQ(5, core_options.GetScanSnapshotId().value_or(-1));
    ExpireConfig expire_config = core_options.GetExpireConfig();
    ASSERT_EQ(15, expire_config.GetSnapshotRetainMin());
    ASSERT_EQ(30, expire_config.GetSnapshotRetainMax());
    ASSERT_EQ(20, expire_config.GetSnapshotMaxDeletes());
    ASSERT_EQ(2 * 3600 * 1000L, expire_config.GetSnapshotTimeRetainMs());
    ASSERT_TRUE(expire_config.CleanEmptyDirectories());
    ASSERT_EQ(std::vector<std::string>({"f1", "f2", "f3"}), core_options.GetSequenceField());
    ASSERT_FALSE(core_options.SequenceFieldSortOrderIsAscending());
    ASSERT_EQ(MergeEngine::PARTIAL_UPDATE, core_options.GetMergeEngine());
    ASSERT_EQ(SortEngine::MIN_HEAP, core_options.GetSortEngine());
    ASSERT_TRUE(core_options.IgnoreDelete());
    ASSERT_EQ("sum", core_options.GetFieldsDefaultFunc().value());
    ASSERT_EQ("min", core_options.GetFieldAggFunc("f0").value().value());
    ASSERT_TRUE(core_options.FieldAggIgnoreRetract("f1").value());
    ASSERT_TRUE(core_options.FieldAggIgnoreRetract("f1").value());
    ASSERT_TRUE(core_options.DeletionVectorsEnabled());
    ASSERT_EQ(ChangelogProducer::FULL_COMPACTION, core_options.GetChangelogProducer());
    ASSERT_TRUE(core_options.NeedLookup());
    std::map<std::string, std::string> seq_grp;
    seq_grp["g_1,g_3"] = "c,d";
    ASSERT_EQ(core_options.GetFieldsSequenceGroups(), seq_grp);
    ASSERT_TRUE(core_options.PartialUpdateRemoveRecordOnDelete());
    ASSERT_EQ(core_options.GetPartialUpdateRemoveRecordOnSequenceGroup(),
              std::vector<std::string>({"a", "b"}));
    ASSERT_EQ(core_options.GetScanFallbackBranch(), std::optional<std::string>("fallback"));
    ASSERT_EQ(core_options.GetBranch(), "rt");
    ASSERT_FALSE(core_options.FileIndexReadEnabled());
    ASSERT_EQ(core_options.GetDataFileExternalPaths(),
              std::optional<std::string>("FILE:///tmp/index"));
    ASSERT_EQ(core_options.GetExternalPathStrategy(), ExternalPathStrategy::ROUND_ROBIN);
    ASSERT_EQ("snappy", core_options.GetFileCompression());
    ASSERT_EQ("zlib", core_options.GetManifestCompression());
    ASSERT_EQ(2, core_options.GetFileCompressionZstdLevel());
    ASSERT_FALSE(core_options.EnableAdaptivePrefetchStrategy());
    ASSERT_EQ(core_options.DataFilePrefix(), "test-data-");
    ASSERT_TRUE(core_options.IndexFileInDataFileDir());
    ASSERT_TRUE(core_options.RowTrackingEnabled());
    ASSERT_TRUE(core_options.DataEvolutionEnabled());
    ASSERT_FALSE(core_options.LegacyPartitionNameEnabled());
    ASSERT_FALSE(core_options.GlobalIndexEnabled());
    ASSERT_TRUE(core_options.GetGlobalIndexExternalPath());
    ASSERT_EQ(core_options.GetGlobalIndexExternalPath().value(), "FILE:///tmp/global_index/");
}

TEST(CoreOptionsTest, TestInvalidCase) {
    ASSERT_NOK_WITH_MSG(CoreOptions::FromMap({{Options::BUCKET, "3.5"}}),
                        "Invalid Config [bucket: 3.5]");
    ASSERT_NOK_WITH_MSG(CoreOptions::FromMap({{Options::SCAN_SNAPSHOT_ID, "3.5"}}),
                        "Invalid Config [scan.snapshot-id: 3.5]");
    ASSERT_NOK_WITH_MSG(CoreOptions::FromMap({{Options::SEQUENCE_FIELD_SORT_ORDER, "invalid"}}),
                        "invalid sort order: invalid");
    ASSERT_NOK_WITH_MSG(CoreOptions::FromMap({{Options::SORT_ENGINE, "invalid"}}),
                        "invalid sort engine: invalid");
    ASSERT_NOK_WITH_MSG(CoreOptions::FromMap({{Options::MERGE_ENGINE, "invalid"}}),
                        "invalid merge engine: invalid");
    ASSERT_NOK_WITH_MSG(CoreOptions::FromMap({{Options::CHANGELOG_PRODUCER, "invalid"}}),
                        "invalid changelog producer: invalid");
}

TEST(CoreOptionsTest, TestCreateExternalPath) {
    std::map<std::string, std::string> options = {
        {Options::DATA_FILE_EXTERNAL_PATHS,
         " FILE:///tmp/index1 ,FILE:///tmp/index2,FILE:///tmp/index3,,"},
        {Options::DATA_FILE_EXTERNAL_PATHS_STRATEGY, "round-robin"},
    };
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> external_paths,
                         core_options.CreateExternalPaths());
    ASSERT_EQ("FILE:/tmp/index1", external_paths[0]);
    ASSERT_EQ("FILE:/tmp/index2", external_paths[1]);
    ASSERT_EQ("FILE:/tmp/index3", external_paths[2]);
}

TEST(CoreOptionsTest, TestInvalidCreateExternalPath) {
    {
        std::map<std::string, std::string> options = {
            {Options::DATA_FILE_EXTERNAL_PATHS,
             "/tmp/index1,FILE:///tmp/index2,FILE:///tmp/index3, "},
            {Options::DATA_FILE_EXTERNAL_PATHS_STRATEGY, "round-robin"},
        };
        ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
        ASSERT_NOK_WITH_MSG(core_options.CreateExternalPaths(),
                            "scheme is null, path is /tmp/index1");
    }
    {
        std::map<std::string, std::string> options = {
            {Options::DATA_FILE_EXTERNAL_PATHS, "FILE:///tmp/index"},
            {Options::DATA_FILE_EXTERNAL_PATHS_STRATEGY, "specific-fs"},
        };
        ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
        ASSERT_NOK_WITH_MSG(core_options.CreateExternalPaths(),
                            "do not support specific-fs external path strategy for now");
    }
    {
        std::map<std::string, std::string> options = {
            {Options::DATA_FILE_EXTERNAL_PATHS, ","},
            {Options::DATA_FILE_EXTERNAL_PATHS_STRATEGY, "round-robin"},
        };
        ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
        ASSERT_NOK_WITH_MSG(core_options.CreateExternalPaths(), "external paths is empty");
    }
}

TEST(CoreOptionsTest, TestCreateGlobalIndexExternalPath) {
    std::map<std::string, std::string> options = {
        {Options::GLOBAL_INDEX_EXTERNAL_PATH, " FILE:///tmp/index1"},
    };
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(std::optional<std::string> external_path,
                         core_options.CreateGlobalIndexExternalPath());
    ASSERT_EQ("FILE:/tmp/index1", external_path.value());
}

TEST(CoreOptionsTest, TestInvalidCreateGlobalIndexExternalPath) {
    std::map<std::string, std::string> options = {
        {Options::GLOBAL_INDEX_EXTERNAL_PATH, "/tmp/index1"},
    };
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_NOK_WITH_MSG(core_options.CreateGlobalIndexExternalPath(),
                        "scheme is null, path is /tmp/index1");
}

TEST(CoreOptionsTest, TestFileSystem) {
    {
        auto mock_fs = std::make_shared<MockFileSystem>();
        ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                             CoreOptions::FromMap({},
                                                  /*specified_file_system=*/mock_fs));
        auto fs = core_options.GetFileSystem();
        ASSERT_TRUE(std::dynamic_pointer_cast<MockFileSystem>(fs));
    }
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                             CoreOptions::FromMap({},
                                                  /*specified_file_system=*/nullptr));
        auto fs = core_options.GetFileSystem();
        auto typed_fs = std::dynamic_pointer_cast<ResolvingFileSystem>(fs);
        ASSERT_TRUE(typed_fs);
        ASSERT_TRUE(std::dynamic_pointer_cast<LocalFileSystem>(
            typed_fs->GetRealFileSystem("/tmp").value_or(nullptr)));
    }
    {
        ASSERT_OK_AND_ASSIGN(
            CoreOptions core_options,
            CoreOptions::FromMap(
                {}, /*specified_file_system=*/nullptr,
                /*fs_scheme_to_identifier_map=*/{{"hdfs", "mock_fs"}, {"oss", "local"}}));
        auto fs = core_options.GetFileSystem();
        auto typed_fs = std::dynamic_pointer_cast<ResolvingFileSystem>(fs);
        ASSERT_TRUE(typed_fs);
        ASSERT_TRUE(std::dynamic_pointer_cast<LocalFileSystem>(
            typed_fs->GetRealFileSystem("/tmp").value_or(nullptr)));
        ASSERT_TRUE(std::dynamic_pointer_cast<MockFileSystem>(
            typed_fs->GetRealFileSystem("hdfs:///tmp/").value_or(nullptr)));
        ASSERT_TRUE(std::dynamic_pointer_cast<LocalFileSystem>(
            typed_fs->GetRealFileSystem("oss:///tmp/").value_or(nullptr)));
    }
}
}  // namespace paimon::test
