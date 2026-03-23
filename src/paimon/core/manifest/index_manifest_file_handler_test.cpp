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

#include "paimon/core/manifest/index_manifest_file_handler.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/core/core_options.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class IndexManifestFileHandlerTest : public testing::Test {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
        dir_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_ != nullptr);
    }

    Result<std::unique_ptr<IndexManifestFile>> CreateManifestFile(int32_t bucket_mode) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileFormat> file_format,
                               FileFormatFactory::Get("orc", {}));
        auto schema = arrow::schema({arrow::field("f0", arrow::int32())});
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FileStorePathFactory> path_factory,
            FileStorePathFactory::Create(
                dir_->Str(), schema, /*partition_keys=*/{}, /*default_part_value=*/"",
                file_format->Identifier(), /*data_file_prefix=*/"data-",
                /*legacy_partition_name_enabled=*/true, /*external_paths=*/{},
                /*global_index_external_path=*/std::nullopt,
                /*index_file_in_data_file_dir=*/false, pool_));
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options,
                               CoreOptions::FromMap({{Options::MANIFEST_FORMAT, "orc"}}));
        return IndexManifestFile::Create(dir_->GetFileSystem(), file_format, "zstd", path_factory,
                                         bucket_mode, pool_, options);
    }

    static IndexManifestEntry MakeEntry(const FileKind& kind, const BinaryRow& partition,
                                        int32_t bucket, const std::string& index_type,
                                        const std::string& file_name, int64_t row_count) {
        return IndexManifestEntry(
            kind, partition, bucket,
            std::make_shared<IndexFileMeta>(index_type, file_name, /*file_size=*/row_count * 10,
                                            row_count, /*dv_ranges=*/std::nullopt,
                                            /*external_path=*/std::nullopt));
    }

    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<UniqueTestDirectory> dir_;
};

TEST_F(IndexManifestFileHandlerTest, GlobalCombinerDeletesThenAddsByFileName) {
    ASSERT_OK_AND_ASSIGN(auto index_manifest_file, CreateManifestFile(/*bucket_mode=*/4));

    auto partition = BinaryRow::EmptyRow();
    std::vector<IndexManifestEntry> previous_entries = {
        MakeEntry(FileKind::Add(), partition, /*bucket=*/0, /*index_type=*/"BTREE", "global-0", 1)};

    ASSERT_OK_AND_ASSIGN(std::string previous_manifest,
                         IndexManifestFileHandler::Write(
                             /*previous_index_manifest=*/std::nullopt, previous_entries,
                             /*bucket_mode=*/4, index_manifest_file.get()));

    std::vector<IndexManifestEntry> new_entries = {
        MakeEntry(FileKind::Delete(), partition, /*bucket=*/999, /*index_type=*/"BTREE", "global-0",
                  1),
        MakeEntry(FileKind::Add(), partition, /*bucket=*/1, /*index_type=*/"BTREE", "global-0", 2)};

    ASSERT_OK_AND_ASSIGN(
        std::string current_manifest,
        IndexManifestFileHandler::Write(previous_manifest, new_entries,
                                        /*bucket_mode=*/4, index_manifest_file.get()));

    std::vector<IndexManifestEntry> written_entries;
    ASSERT_OK(index_manifest_file->Read(current_manifest, /*filter=*/nullptr, &written_entries));
    ASSERT_EQ(written_entries.size(), 1);
    ASSERT_EQ(written_entries[0].index_file->FileName(), "global-0");
    ASSERT_EQ(written_entries[0].index_file->RowCount(), 2);
}

TEST_F(IndexManifestFileHandlerTest, BucketedCombinerUsesPartitionBucketAndIndexType) {
    ASSERT_OK_AND_ASSIGN(auto index_manifest_file, CreateManifestFile(/*bucket_mode=*/2));

    auto partition_10 = BinaryRowGenerator::GenerateRow({10}, pool_.get());
    std::vector<IndexManifestEntry> previous_entries = {
        MakeEntry(FileKind::Add(), partition_10, /*bucket=*/0,
                  DeletionVectorsIndexFile::DELETION_VECTORS_INDEX, "dv-0-old", 10),
        MakeEntry(FileKind::Add(), partition_10, /*bucket=*/1,
                  DeletionVectorsIndexFile::DELETION_VECTORS_INDEX, "dv-1-keep", 11)};

    ASSERT_OK_AND_ASSIGN(std::string previous_manifest,
                         IndexManifestFileHandler::Write(
                             /*previous_index_manifest=*/std::nullopt, previous_entries,
                             /*bucket_mode=*/2, index_manifest_file.get()));

    std::vector<IndexManifestEntry> new_entries = {
        MakeEntry(FileKind::Delete(), partition_10, /*bucket=*/0,
                  DeletionVectorsIndexFile::DELETION_VECTORS_INDEX, "ignored", 10),
        MakeEntry(FileKind::Add(), partition_10, /*bucket=*/0,
                  DeletionVectorsIndexFile::DELETION_VECTORS_INDEX, "dv-0-new", 12)};

    ASSERT_OK_AND_ASSIGN(
        std::string current_manifest,
        IndexManifestFileHandler::Write(previous_manifest, new_entries,
                                        /*bucket_mode=*/2, index_manifest_file.get()));

    std::vector<IndexManifestEntry> written_entries;
    ASSERT_OK(index_manifest_file->Read(current_manifest, /*filter=*/nullptr, &written_entries));
    ASSERT_EQ(written_entries.size(), 2);

    bool found_bucket0 = false;
    bool found_bucket1 = false;
    for (const auto& entry : written_entries) {
        if (entry.bucket == 0) {
            found_bucket0 = true;
            ASSERT_EQ(entry.index_file->FileName(), "dv-0-new");
            ASSERT_EQ(entry.index_file->RowCount(), 12);
        } else if (entry.bucket == 1) {
            found_bucket1 = true;
            ASSERT_EQ(entry.index_file->FileName(), "dv-1-keep");
            ASSERT_EQ(entry.index_file->RowCount(), 11);
        }
    }
    ASSERT_TRUE(found_bucket0);
    ASSERT_TRUE(found_bucket1);
}

TEST_F(IndexManifestFileHandlerTest, DvWithBucketUnawareModeReturnsNotImplemented) {
    ASSERT_OK_AND_ASSIGN(auto index_manifest_file, CreateManifestFile(/*bucket_mode=*/-1));

    auto partition = BinaryRow::EmptyRow();
    std::vector<IndexManifestEntry> new_entries = {
        MakeEntry(FileKind::Add(), partition, /*bucket=*/0,
                  DeletionVectorsIndexFile::DELETION_VECTORS_INDEX, "dv-0", 1)};

    ASSERT_NOK_WITH_MSG(IndexManifestFileHandler::Write(
                            /*previous_index_manifest=*/std::nullopt, new_entries,
                            /*bucket_mode=*/-1, index_manifest_file.get()),
                        "not yet support dv with BUCKET_UNAWARE mode");
}

}  // namespace paimon::test
