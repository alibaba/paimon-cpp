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

#include "paimon/core/index/index_file_handler.h"

#include <map>
#include <optional>
#include <variant>

#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/core_options.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/index/deletion_vector_meta.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class IndexFileHandlerTest : public testing::Test {
 public:
    void SetUp() override {
        memory_pool_ = GetDefaultPool();
    }

    Result<std::unique_ptr<IndexFileHandler>> CreateIndexFileHandler(
        const std::string& table_path, const CoreOptions& core_options) const {
        SchemaManager schema_manager(core_options.GetFileSystem(), table_path);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> table_schema,
                               schema_manager.ReadSchema(/*schema_id=*/0));
        auto schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                               core_options.CreateExternalPaths());
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                               core_options.CreateGlobalIndexExternalPath());

        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FileStorePathFactory> path_factory,
            FileStorePathFactory::Create(
                table_path, schema, table_schema->PartitionKeys(),
                core_options.GetPartitionDefaultName(),
                /*identifier=*/"orc", core_options.DataFilePrefix(),
                core_options.LegacyPartitionNameEnabled(), external_paths,
                global_index_external_path,
                /*index_file_in_data_file_dir=*/core_options.IndexFileInDataFileDir(),
                memory_pool_));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<IndexManifestFile> index_manifest_file,
                               IndexManifestFile::Create(core_options.GetFileSystem(),
                                                         core_options.GetManifestFormat(),
                                                         core_options.GetManifestCompression(),
                                                         path_factory, memory_pool_, core_options));
        auto path_factories = std::make_shared<IndexFilePathFactories>(path_factory);
        return std::make_unique<IndexFileHandler>(std::move(index_manifest_file), path_factories);
    }
    std::shared_ptr<MemoryPool> memory_pool_;
};

TEST_F(IndexFileHandlerTest, TestFilePath) {
    std::string table_path = paimon::test::GetDataDir() +
                             "/orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality/";
    {
        // test without external path & index-file-in-data-file-dir" = false
        ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap({}));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<IndexFileHandler> index_file_handler,
                             CreateIndexFileHandler(table_path, core_options));
        auto index_file_meta = std::make_shared<IndexFileMeta>(
            /*index_type=*/"DELETION_VECTOR", /*file_name=*/"deletion-file", /*file_size=*/1000,
            /*row_count=*/100, /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt);
        auto partition = BinaryRowGenerator::GenerateRow({10}, memory_pool_.get());
        ASSERT_OK_AND_ASSIGN(std::string file_path, index_file_handler->FilePath(
                                                        partition, /*bucket=*/1, index_file_meta));
        ASSERT_EQ(file_path, table_path + "index/deletion-file");
    }
    {
        // test with external path & index-file-in-data-file-dir" = false
        ASSERT_OK_AND_ASSIGN(
            CoreOptions core_options,
            CoreOptions::FromMap({{Options::DATA_FILE_EXTERNAL_PATHS, "FILE:///tmp/"}}));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<IndexFileHandler> index_file_handler,
                             CreateIndexFileHandler(table_path, core_options));
        auto index_file_meta = std::make_shared<IndexFileMeta>(
            /*index_type=*/"DELETION_VECTOR", /*file_name=*/"deletion-file", /*file_size=*/1000,
            /*row_count=*/100, /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt);
        auto partition = BinaryRowGenerator::GenerateRow({10}, memory_pool_.get());
        ASSERT_OK_AND_ASSIGN(std::string file_path, index_file_handler->FilePath(
                                                        partition, /*bucket=*/1, index_file_meta));
        ASSERT_EQ(file_path, table_path + "index/deletion-file");
    }
    {
        // test without external path & index-file-in-data-file-dir" = true
        ASSERT_OK_AND_ASSIGN(
            CoreOptions core_options,
            CoreOptions::FromMap({{Options::INDEX_FILE_IN_DATA_FILE_DIR, "true"}}));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<IndexFileHandler> index_file_handler,
                             CreateIndexFileHandler(table_path, core_options));
        auto index_file_meta = std::make_shared<IndexFileMeta>(
            /*index_type=*/"DELETION_VECTOR", /*file_name=*/"deletion-file", /*file_size=*/1000,
            /*row_count=*/100, /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt);
        auto partition = BinaryRowGenerator::GenerateRow({10}, memory_pool_.get());
        ASSERT_OK_AND_ASSIGN(std::string file_path, index_file_handler->FilePath(
                                                        partition, /*bucket=*/1, index_file_meta));
        ASSERT_EQ(file_path, table_path + "f1=10/bucket-1/deletion-file");
    }
    {
        // test with external path & index-file-in-data-file-dir" = true
        ASSERT_OK_AND_ASSIGN(
            CoreOptions core_options,
            CoreOptions::FromMap({{Options::INDEX_FILE_IN_DATA_FILE_DIR, "true"},
                                  {Options::DATA_FILE_EXTERNAL_PATHS, "FILE:///tmp/"}}));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<IndexFileHandler> index_file_handler,
                             CreateIndexFileHandler(table_path, core_options));
        auto index_file_meta = std::make_shared<IndexFileMeta>(
            /*index_type=*/"DELETION_VECTOR", /*file_name=*/"deletion-file", /*file_size=*/1000,
            /*row_count=*/100, /*dv_ranges=*/std::nullopt,
            /*external_path=*/"FILE:///tmp/f1=10/bucket-1/deletion-file");
        auto partition = BinaryRowGenerator::GenerateRow({10}, memory_pool_.get());
        ASSERT_OK_AND_ASSIGN(std::string file_path, index_file_handler->FilePath(
                                                        partition, /*bucket=*/1, index_file_meta));
        ASSERT_EQ(file_path, "FILE:///tmp/f1=10/bucket-1/deletion-file");
    }
}
TEST_F(IndexFileHandlerTest, TestScan) {
    std::string table_path = paimon::test::GetDataDir() +
                             "/orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality/";

    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::MANIFEST_FORMAT, "orc"}}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<IndexFileHandler> index_file_handler,
                         CreateIndexFileHandler(table_path, core_options));

    SnapshotManager snapshot_manager(core_options.GetFileSystem(), table_path);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot, snapshot_manager.LoadSnapshot(/*snapshot_id=*/4));

    auto partition = BinaryRowGenerator::GenerateRow({10}, memory_pool_.get());
    std::unordered_set<BinaryRow> partitions = {partition};
    ASSERT_OK_AND_ASSIGN(
        auto index_file_metas,
        index_file_handler->Scan(
            snapshot, std::string(DeletionVectorsIndexFile::DELETION_VECTORS_INDEX), partitions));
    ASSERT_EQ(2, index_file_metas.size());

    // check index metas equal
    LinkedHashMap<std::string, DeletionVectorMeta> dv_meta_p10_b0;
    dv_meta_p10_b0.insert_or_assign(
        "data-0d0f29cc-63c6-4fab-a594-71bd7d06fcde-0.orc",
        DeletionVectorMeta("data-0d0f29cc-63c6-4fab-a594-71bd7d06fcde-0.orc", /*offset=*/1,
                           /*length=*/22, /*cardinality=*/1));
    std::vector<std::shared_ptr<IndexFileMeta>> index_meta_p10_b0 = {
        std::make_shared<IndexFileMeta>(
            "DELETION_VECTORS", "index-86356766-3238-46e6-990b-656cd7409eaa-0",
            /*file_size=*/31, /*row_count=*/1, dv_meta_p10_b0, /*external_path=*/std::nullopt)};

    LinkedHashMap<std::string, DeletionVectorMeta> dv_meta_p10_b1;
    dv_meta_p10_b1.insert_or_assign(
        "data-2ffe7ae9-2cf7-41e9-944b-2065585cde31-0.orc",
        DeletionVectorMeta("data-2ffe7ae9-2cf7-41e9-944b-2065585cde31-0.orc", /*offset=*/1,
                           /*length=*/24, /*cardinality=*/2));
    std::vector<std::shared_ptr<IndexFileMeta>> index_meta_p10_b1 = {
        std::make_shared<IndexFileMeta>(
            "DELETION_VECTORS", "index-86356766-3238-46e6-990b-656cd7409eaa-1",
            /*file_size=*/33, /*row_count=*/1, dv_meta_p10_b1, /*external_path=*/std::nullopt)};
    ASSERT_TRUE(
        ObjectUtils::Equal(index_file_metas[std::make_pair(partition, 0)], index_meta_p10_b0));
    ASSERT_TRUE(
        ObjectUtils::Equal(index_file_metas[std::make_pair(partition, 1)], index_meta_p10_b1));

    // test FilePath
    ASSERT_OK_AND_ASSIGN(auto index_file_path, index_file_handler->FilePath(partition, /*bucket=*/0,
                                                                            index_meta_p10_b0[0]));
    ASSERT_EQ(index_file_path,
              PathUtil::JoinPath(table_path, "/index/" + index_meta_p10_b0[0]->FileName()));
}

TEST_F(IndexFileHandlerTest, Test09VersionScan) {
    std::string table_path = paimon::test::GetDataDir() + "/orc/pk_09.db/pk_09/";
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::MANIFEST_FORMAT, "orc"}}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<IndexFileHandler> index_file_handler,
                         CreateIndexFileHandler(table_path, core_options));

    SnapshotManager snapshot_manager(core_options.GetFileSystem(), table_path);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot, snapshot_manager.LoadSnapshot(/*snapshot_id=*/6));

    auto partition = BinaryRowGenerator::GenerateRow({10}, memory_pool_.get());
    std::unordered_set<BinaryRow> partitions = {partition};
    ASSERT_OK_AND_ASSIGN(
        auto index_file_metas,
        index_file_handler->Scan(
            snapshot, std::string(DeletionVectorsIndexFile::DELETION_VECTORS_INDEX), partitions));

    ASSERT_EQ(2, index_file_metas.size());

    // check index metas equal
    LinkedHashMap<std::string, DeletionVectorMeta> dv_meta_p10_b0;
    dv_meta_p10_b0.insert_or_assign(
        "data-1c7a85f1-55bd-424f-b503-34a33be0fb96-0.orc",
        DeletionVectorMeta("data-1c7a85f1-55bd-424f-b503-34a33be0fb96-0.orc", /*offset=*/1,
                           /*length=*/22, /*cardinality=*/std::nullopt));
    dv_meta_p10_b0.insert_or_assign(
        "data-980e82b4-2345-4976-bc1d-ea989fcdbffa-0.orc",
        DeletionVectorMeta("data-980e82b4-2345-4976-bc1d-ea989fcdbffa-0.orc", /*offset=*/31,
                           /*length=*/22, /*cardinality=*/std::nullopt));
    std::vector<std::shared_ptr<IndexFileMeta>> index_meta_p10_b0 = {
        std::make_shared<IndexFileMeta>(
            "DELETION_VECTORS", "index-7badd250-6c0b-49e9-8e40-2449ae9a2539-0",
            /*file_size=*/61, /*row_count=*/2, dv_meta_p10_b0, /*external_path=*/std::nullopt)};

    LinkedHashMap<std::string, DeletionVectorMeta> dv_meta_p10_b1;
    dv_meta_p10_b1.insert_or_assign(
        "data-6871b960-edd9-40fc-9859-aaca9ea205cf-0.orc",
        DeletionVectorMeta("data-6871b960-edd9-40fc-9859-aaca9ea205cf-0.orc", /*offset=*/1,
                           /*length=*/22, /*cardinality=*/std::nullopt));
    std::vector<std::shared_ptr<IndexFileMeta>> index_meta_p10_b1 = {
        std::make_shared<IndexFileMeta>(
            "DELETION_VECTORS", "index-7badd250-6c0b-49e9-8e40-2449ae9a2539-1",
            /*file_size=*/31, /*row_count=*/1, dv_meta_p10_b1, /*external_path=*/std::nullopt)};
    ASSERT_TRUE(
        ObjectUtils::Equal(index_file_metas[std::make_pair(partition, 0)], index_meta_p10_b0));
    ASSERT_TRUE(
        ObjectUtils::Equal(index_file_metas[std::make_pair(partition, 1)], index_meta_p10_b1));
}

}  // namespace paimon::test
