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

#include "paimon/core/table/source/snapshot/snapshot_reader.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/index_file_path_factories.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class SnapshotReaderTest : public testing::Test {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
        dir_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_ != nullptr);
    }

    std::shared_ptr<DataFileMeta> CreateDataFileMeta(const std::string& file_name) const {
        return std::make_shared<DataFileMeta>(
            file_name, /*file_size=*/100, /*row_count=*/10, DataFileMeta::EmptyMinKey(),
            DataFileMeta::EmptyMaxKey(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
            /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
            DataFileMeta::DUMMY_LEVEL, std::vector<std::optional<std::string>>{}, Timestamp(0, 0),
            std::nullopt, nullptr, FileSource::Append(), std::nullopt, std::nullopt, std::nullopt,
            std::nullopt);
    }

    std::shared_ptr<IndexFileMeta> CreateIndexFileMeta(const std::string& index_file_name,
                                                       const std::string& data_file_name,
                                                       int64_t offset, int64_t length,
                                                       std::optional<int64_t> cardinality) const {
        LinkedHashMap<std::string, DeletionVectorMeta> dv_ranges;
        dv_ranges.insert(data_file_name,
                         DeletionVectorMeta(data_file_name, offset, length, cardinality));
        return std::make_shared<IndexFileMeta>(DeletionVectorsIndexFile::DELETION_VECTORS_INDEX,
                                               index_file_name, /*file_size=*/100, /*row_count=*/10,
                                               dv_ranges, /*external_path=*/std::nullopt);
    }

    Result<std::unique_ptr<IndexFileHandler>> CreateIndexFileHandler() const {
        auto schema = arrow::schema({arrow::field("f0", arrow::int32())});
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FileStorePathFactory> path_factory,
            FileStorePathFactory::Create(
                dir_->Str(), schema, /*partition_keys=*/{}, /*default_part_value=*/"", "orc",
                /*data_file_prefix=*/"data-", /*legacy_partition_name_enabled=*/true,
                /*external_paths=*/{}, /*global_index_external_path=*/std::nullopt,
                /*index_file_in_data_file_dir=*/false, pool_));
        auto path_factories = std::make_shared<IndexFilePathFactories>(path_factory);
        return std::make_unique<IndexFileHandler>(std::make_shared<LocalFileSystem>(),
                                                  std::unique_ptr<IndexManifestFile>(),
                                                  path_factories, /*dv_bitmap64=*/false, pool_);
    }

    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<UniqueTestDirectory> dir_;
};

TEST_F(SnapshotReaderTest, GetDeletionFilesOverwritesDuplicateDataFileName) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<IndexFileHandler> index_file_handler,
                         CreateIndexFileHandler());
    SnapshotReader snapshot_reader(/*scan=*/nullptr, /*path_factory=*/nullptr,
                                   /*split_generator=*/nullptr, std::move(index_file_handler));

    const std::string data_file_name = "data-0.orc";
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {CreateDataFileMeta(data_file_name)};
    std::vector<std::shared_ptr<IndexFileMeta>> index_file_metas = {
        CreateIndexFileMeta("index-first", data_file_name, /*offset=*/1, /*length=*/11,
                            /*cardinality=*/3),
        CreateIndexFileMeta("index-second", data_file_name, /*offset=*/2, /*length=*/22,
                            /*cardinality=*/4)};

    ASSERT_OK_AND_ASSIGN(std::vector<std::optional<DeletionFile>> deletion_files,
                         snapshot_reader.GetDeletionFiles(BinaryRow::EmptyRow(), /*bucket=*/0,
                                                          data_files, index_file_metas));

    ASSERT_EQ(deletion_files.size(), 1);
    ASSERT_TRUE(deletion_files[0].has_value());
    EXPECT_EQ(deletion_files[0]->path, dir_->Str() + "/index/index-second");
    EXPECT_EQ(deletion_files[0]->offset, 2);
    EXPECT_EQ(deletion_files[0]->length, 22);
    EXPECT_EQ(deletion_files[0]->cardinality, std::optional<int64_t>(4));
}

}  // namespace paimon::test
