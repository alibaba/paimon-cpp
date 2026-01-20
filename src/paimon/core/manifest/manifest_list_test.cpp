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

#include "paimon/core/manifest/manifest_list.h"

#include <map>
#include <variant>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class ManifestListTest : public testing::Test {
 public:
    std::unique_ptr<ManifestList> CreateManifestList(
        const std::string& file_format_str, const std::string& root_path,
        const std::shared_ptr<MemoryPool>& pool) const {
        std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<FileFormat> file_format,
                             FileFormatFactory::Get(file_format_str, {}));
        auto unused_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", arrow::utf8())}));
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<FileStorePathFactory> path_factory,
                             FileStorePathFactory::Create(
                                 root_path, unused_schema, /*partition_keys=*/{},
                                 /*default_part_value=*/"", file_format->Identifier(),
                                 /*data_file_prefix=*/"data-",
                                 /*legacy_partition_name_enabled=*/true, /*external_paths=*/{},
                                 /*global_index_external_path=*/std::nullopt,
                                 /*index_file_in_data_file_dir=*/false, pool));
        EXPECT_OK_AND_ASSIGN(auto manifest_list, ManifestList::Create(file_system, file_format,
                                                                      "zstd", path_factory, pool));
        return manifest_list;
    }

    std::vector<ManifestFileMeta> ReadManifestFileMeta(
        const std::string& file_format_str, const std::string& root_path,
        const std::string& file_name, const std::shared_ptr<MemoryPool>& pool) const {
        auto manifest_list = CreateManifestList(file_format_str, root_path, pool);
        std::vector<ManifestFileMeta> manifest_file_metas;
        EXPECT_OK(manifest_list->Read(file_name, /*filter=*/nullptr, &manifest_file_metas));
        return manifest_file_metas;
    }
};

TEST_F(ManifestListTest, TestSimple) {
    auto pool = GetDefaultPool();
    auto manifest_file_metas =
        ReadManifestFileMeta("orc", paimon::test::GetDataDir() + "/orc/append_09.db/append_09",
                             "manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-2", pool);
    ASSERT_EQ(manifest_file_metas.size(), 4);

    std::vector<ManifestFileMeta> expected_manifest_file_metas;
    auto expected_meta1 =
        ManifestFileMeta("manifest-f8b15cfc-437a-4d21-a6a0-e45b639ae7ed-0", /*file_size=*/2666,
                         /*num_added_files=*/3, /*num_deleted_files=*/0,
                         BinaryRowGenerator::GenerateStats({10}, {20}, {0}, pool.get()),
                         /*schema_id=*/0, /*min_bucket=*/std::nullopt, /*max_bucket=*/std::nullopt,
                         /*min_level=*/std::nullopt, /*max_level=*/std::nullopt,
                         /*min_row_id=*/std::nullopt, /*max_row_id=*/std::nullopt);
    auto expected_meta2 =
        ManifestFileMeta("manifest-3a44a0da-1008-463c-914e-28d271375e24-0", /*file_size=*/2617,
                         /*num_added_files=*/2, /*num_deleted_files=*/0,
                         BinaryRowGenerator::GenerateStats({10}, {20}, {0}, pool.get()),
                         /*schema_id=*/0, /*min_bucket=*/std::nullopt, /*max_bucket=*/std::nullopt,
                         /*min_level=*/std::nullopt, /*max_level=*/std::nullopt,
                         /*min_row_id=*/std::nullopt, /*max_row_id=*/std::nullopt);
    auto expected_meta3 =
        ManifestFileMeta("manifest-c5904353-0236-46a2-891f-62a326dd8e5e-0", /*file_size=*/2360,
                         /*num_added_files=*/1, /*num_deleted_files=*/0,
                         BinaryRowGenerator::GenerateStats({10}, {10}, {0}, pool.get()),
                         /*schema_id=*/0, /*min_bucket=*/std::nullopt, /*max_bucket=*/std::nullopt,
                         /*min_level=*/std::nullopt, /*max_level=*/std::nullopt,
                         /*min_row_id=*/std::nullopt, /*max_row_id=*/std::nullopt);
    auto expected_meta4 =
        ManifestFileMeta("manifest-3ea5ee21-d399-4f1c-a749-2fc63dbf0852-0", /*file_size=*/2366,
                         /*num_added_files=*/1, /*num_deleted_files=*/0,
                         BinaryRowGenerator::GenerateStats({10}, {10}, {0}, pool.get()),
                         /*schema_id=*/0, /*min_bucket=*/std::nullopt, /*max_bucket=*/std::nullopt,
                         /*min_level=*/std::nullopt, /*max_level=*/std::nullopt,
                         /*min_row_id=*/std::nullopt, /*max_row_id=*/std::nullopt);
    expected_manifest_file_metas.emplace_back(expected_meta1);
    expected_manifest_file_metas.emplace_back(expected_meta2);
    expected_manifest_file_metas.emplace_back(expected_meta3);
    expected_manifest_file_metas.emplace_back(expected_meta4);
    ASSERT_EQ(manifest_file_metas, expected_manifest_file_metas);
}

TEST_F(ManifestListTest, TestReadWithBucketsAndLevel) {
    auto pool = GetDefaultPool();
    auto manifest_file_metas =
        ReadManifestFileMeta("orc",
                             paimon::test::GetDataDir() +
                                 "/orc/pk_table_with_total_buckets.db/pk_table_with_total_buckets",
                             "manifest-list-673be11d-f405-4921-84dc-6f53028c55ea-1", pool);
    ASSERT_EQ(manifest_file_metas.size(), 1);

    std::vector<ManifestFileMeta> expected_manifest_file_metas;
    auto expected_meta1 =
        ManifestFileMeta("manifest-2026dc88-7f67-4944-8c33-ea775d34108c-0", /*file_size=*/3046,
                         /*num_added_files=*/2, /*num_deleted_files=*/0,
                         BinaryRowGenerator::GenerateStats({10}, {10}, {0}, pool.get()),
                         /*schema_id=*/0, /*min_bucket=*/0, /*max_bucket=*/1,
                         /*min_level=*/0, /*max_level=*/0,
                         /*min_row_id=*/std::nullopt, /*max_row_id=*/std::nullopt);
    expected_manifest_file_metas.emplace_back(expected_meta1);
    ASSERT_EQ(manifest_file_metas, expected_manifest_file_metas);
}

TEST_F(ManifestListTest, TestEmptyManifestList) {
    auto pool = GetDefaultPool();
    auto manifest_file_metas =
        ReadManifestFileMeta("orc", paimon::test::GetDataDir() + "/orc/append_09.db/append_09",
                             "manifest-list-616d1847-a02c-495f-9cca-2c8b7def0fec-0", pool);
    ASSERT_EQ(manifest_file_metas.size(), 0);
}

TEST_F(ManifestListTest, TestManifestListCompatibleWithJavaPaimon09) {
    auto pool = GetDefaultPool();
    auto manifest_file_metas = ReadManifestFileMeta("avro", paimon::test::GetDataDir() + "/avro",
                                                    "avro_manifest_list_09", pool);
    ASSERT_EQ(manifest_file_metas.size(), 1);

    std::vector<ManifestFileMeta> expected_manifest_file_metas;
    auto expected_meta =
        ManifestFileMeta("manifest-b09d5588-5614-46e2-b441-f196d29e60dc-0", /*file_size=*/2010,
                         /*num_added_files=*/1, /*num_deleted_files=*/0, SimpleStats::EmptyStats(),
                         /*schema_id=*/0, /*min_bucket=*/std::nullopt, /*max_bucket=*/std::nullopt,
                         /*min_level=*/std::nullopt, /*max_level=*/std::nullopt,
                         /*min_row_id=*/std::nullopt, /*max_row_id=*/std::nullopt);
    expected_manifest_file_metas.emplace_back(expected_meta);
    ASSERT_EQ(manifest_file_metas, expected_manifest_file_metas);
}

TEST_F(ManifestListTest, TestManifestListCompatibleWithJavaPaimon11) {
    auto pool = GetDefaultPool();
    auto manifest_file_metas = ReadManifestFileMeta("avro", paimon::test::GetDataDir() + "/avro",
                                                    "avro_manifest_list_11", pool);
    ASSERT_EQ(manifest_file_metas.size(), 1);

    std::vector<ManifestFileMeta> expected_manifest_file_metas;
    auto expected_meta = ManifestFileMeta(
        "manifest-3c977409-ebff-4d68-8265-86d237e24e9a-0", /*file_size=*/2081,
        /*num_added_files=*/1, /*num_deleted_files=*/0, SimpleStats::EmptyStats(),
        /*schema_id=*/0, /*min_bucket=*/0, /*max_bucket=*/0, /*min_level=*/0, /*max_level=*/0,
        /*min_row_id=*/std::nullopt, /*max_row_id=*/std::nullopt);
    expected_manifest_file_metas.emplace_back(expected_meta);
    ASSERT_EQ(manifest_file_metas, expected_manifest_file_metas);
}

TEST_F(ManifestListTest, TestReadWithMinAndMaxRowId) {
    auto pool = GetDefaultPool();
    // test read meta from java
    auto manifest_file_metas = ReadManifestFileMeta(
        "orc",
        paimon::test::GetDataDir() + "orc/append_with_global_index.db/append_with_global_index/",
        "manifest-list-2bccccf8-9f5e-48f2-b706-5b33f8c3bfc0-0", pool);
    ASSERT_EQ(manifest_file_metas.size(), 1);

    std::vector<ManifestFileMeta> expected_manifest_file_metas;
    auto expected_meta =
        ManifestFileMeta("manifest-65b0d403-a1bc-4157-b242-bff73c46596d-0", /*file_size=*/2779,
                         /*num_added_files=*/1, /*num_deleted_files=*/0, SimpleStats::EmptyStats(),
                         /*schema_id=*/0, /*min_bucket=*/0, /*max_bucket=*/0,
                         /*min_level=*/0, /*max_level=*/0,
                         /*min_row_id=*/0, /*max_row_id=*/7);
    expected_manifest_file_metas.emplace_back(expected_meta);
    ASSERT_EQ(manifest_file_metas, expected_manifest_file_metas);

    // test write meta
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto manifest_list = CreateManifestList("orc", dir->Str(), pool);
    std::pair<std::string, int64_t> file_meta;
    ASSERT_OK_AND_ASSIGN(file_meta, manifest_list->Write({expected_meta}));
    // test read meta from C++
    auto manifest_file_metas2 = ReadManifestFileMeta("orc", dir->Str(), file_meta.first, pool);
    ASSERT_EQ(manifest_file_metas2.size(), 1);
    ASSERT_EQ(manifest_file_metas2, expected_manifest_file_metas);
}

}  // namespace paimon::test
