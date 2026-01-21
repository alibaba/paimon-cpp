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

#include "paimon/core/manifest/manifest_file.h"

#include <functional>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <variant>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/data_define.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class ManifestFileTest : public testing::Test {
 public:
    std::vector<ManifestEntry> ReadManifestEntry(const std::string& file_format_str,
                                                 const std::string& root_path,
                                                 const std::string& file_name,
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
        EXPECT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{Options::FILE_FORMAT, "orc"},
                                                   {Options::MANIFEST_FORMAT, file_format_str}}));
        EXPECT_OK_AND_ASSIGN(
            std::unique_ptr<ManifestFile> manifest_file,
            ManifestFile::Create(file_system, file_format, "zstd", path_factory,
                                 /*target_file_size=*/1024, pool, options, unused_schema));
        std::vector<ManifestEntry> manifest_entries;
        EXPECT_OK(manifest_file->Read(file_name, /*filter=*/nullptr, &manifest_entries));

        return manifest_entries;
    }
};

TEST_F(ManifestFileTest, TestSimple) {
    auto pool = GetDefaultPool();
    auto manifest_entries =
        ReadManifestEntry("orc", paimon::test::GetDataDir() + "/orc/append_09.db/append_09",
                          "manifest-3ea5ee21-d399-4f1c-a749-2fc63dbf0852-1", pool);
    ASSERT_EQ(manifest_entries.size(), 5);
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-4e30d6c0-f109-4300-a010-4ba03047dd9d-0.orc", /*file_size=*/575, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Bob"), 10, 0, 12.1},
                                          {std::string("Tony"), 10, 0, 14.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1721643142456ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    auto manifest_entry1 =
        ManifestEntry(FileKind::Delete(), BinaryRowGenerator::GenerateRow({10}, pool.get()),
                      /*bucket=*/1, /*total_buckets=*/2, file_meta1);

    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-10b9eea8-241d-4e4b-8ab8-2a82d72d79a2-0.orc", /*file_size=*/589, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Alex"), 10, 0, 12.1},
                                          {std::string("Emily"), 10, 0, 16.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/3, /*max_sequence_number=*/5, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1721643267385ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    auto manifest_entry2 =
        ManifestEntry(FileKind::Delete(), BinaryRowGenerator::GenerateRow({10}, pool.get()),
                      /*bucket=*/1, /*total_buckets=*/2, file_meta2);

    auto file_meta3 = std::make_shared<DataFileMeta>(
        "data-e2bb59ee-ae25-4e5b-9bcc-257250bc5fdd-0.orc", /*file_size=*/541, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("David"), 10, 0, 17.1},
                                          {std::string("David"), 10, 0, 17.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/6, /*max_sequence_number=*/6, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1721643314161ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    auto manifest_entry3 =
        ManifestEntry(FileKind::Delete(), BinaryRowGenerator::GenerateRow({10}, pool.get()),
                      /*bucket=*/1, /*total_buckets=*/2, file_meta3);

    auto file_meta4 = std::make_shared<DataFileMeta>(
        "data-2d5ea1ea-77c1-47ff-bb87-19a509962a37-0.orc", /*file_size=*/538, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Lily"), 10, 0, 17.1},
                                          {std::string("Lily"), 10, 0, 17.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/7, /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1721643834400ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    auto manifest_entry4 =
        ManifestEntry(FileKind::Delete(), BinaryRowGenerator::GenerateRow({10}, pool.get()),
                      /*bucket=*/1, /*total_buckets=*/2, file_meta4);

    auto file_meta5 = std::make_shared<DataFileMeta>(
        "data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc", /*file_size=*/640, /*row_count=*/8,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Alex"), 10, 0, 12.1},
                                          {std::string("Tony"), 10, 0, 17.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1721643834472ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Compact(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    auto manifest_entry5 =
        ManifestEntry(FileKind::Add(), BinaryRowGenerator::GenerateRow({10}, pool.get()),
                      /*bucket=*/1, /*total_buckets=*/2, file_meta5);

    std::vector<ManifestEntry> expected_manifest_entries;
    expected_manifest_entries.emplace_back(manifest_entry1);
    expected_manifest_entries.emplace_back(manifest_entry2);
    expected_manifest_entries.emplace_back(manifest_entry3);
    expected_manifest_entries.emplace_back(manifest_entry4);
    expected_manifest_entries.emplace_back(manifest_entry5);
    ASSERT_EQ(expected_manifest_entries, manifest_entries);
}

TEST_F(ManifestFileTest, TestWithNullCount) {
    auto pool = GetDefaultPool();
    auto manifest_entries =
        ReadManifestEntry("orc", paimon::test::GetDataDir() + "/orc/append_09.db/append_09",
                          "manifest-3a44a0da-1008-463c-914e-28d271375e24-0", pool);
    ASSERT_EQ(manifest_entries.size(), 2);
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-10b9eea8-241d-4e4b-8ab8-2a82d72d79a2-0.orc", /*file_size=*/589, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Alex"), 10, 0, 12.1},
                                          {std::string("Emily"), 10, 0, 16.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/3, /*max_sequence_number=*/5, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1721643267385ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    auto manifest_entry1 =
        ManifestEntry(FileKind::Add(), BinaryRowGenerator::GenerateRow({10}, pool.get()),
                      /*bucket=*/1, /*total_buckets=*/2, file_meta1);

    ASSERT_EQ(manifest_entries[0].Kind(), FileKind::Add());
    ASSERT_EQ(manifest_entries[0].Partition(), BinaryRowGenerator::GenerateRow({10}, pool.get()));
    ASSERT_EQ(manifest_entries[0].Bucket(), 1);
    ASSERT_EQ(manifest_entries[0].Level(), 0);
    ASSERT_EQ(manifest_entries[0].FileName(), "data-10b9eea8-241d-4e4b-8ab8-2a82d72d79a2-0.orc");
    ASSERT_EQ(manifest_entries[0].MinKey(), BinaryRow::EmptyRow());
    ASSERT_EQ(manifest_entries[0].MaxKey(), BinaryRow::EmptyRow());
    ASSERT_EQ(manifest_entries[0].CreateIdentifier(), manifest_entry1.CreateIdentifier());

    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc", /*file_size=*/506, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Paul"), 20, 1, NullType()},
                                          {std::string("Paul"), 20, 1, NullType()}, {0, 0, 0, 1},
                                          pool.get()),
        /*min_sequence_number=*/1, /*max_sequence_number=*/1, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1721643267404ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    auto manifest_entry2 =
        ManifestEntry(FileKind::Add(), BinaryRowGenerator::GenerateRow({20}, pool.get()),
                      /*bucket=*/0, /*total_buckets=*/2, file_meta2);
    std::vector<ManifestEntry> expected_manifest_entries;
    expected_manifest_entries.emplace_back(manifest_entry1);
    expected_manifest_entries.emplace_back(manifest_entry2);
    ASSERT_EQ(expected_manifest_entries, manifest_entries);
}

TEST_F(ManifestFileTest, TestManifestFileCompatibleWithJavaPaimon09) {
    auto pool = GetDefaultPool();
    auto manifest_entries =
        ReadManifestEntry("avro", paimon::test::GetDataDir() + "/avro", "avro_manifest_09", pool);
    ASSERT_EQ(manifest_entries.size(), 1);
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-dd28db13-0f8f-43a5-a0df-684e7dc93c55-0.avro", /*file_size=*/1625, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        ::paimon::test::BinaryRowGenerator::GenerateStats(
            {false, static_cast<int8_t>(-128), static_cast<int16_t>(-32768),
             static_cast<int32_t>(-2147483648), -9999999999999, -1234.56f, -1234567890.0987654321,
             std::string("aa"), NullType(), NullType(), NullType(),
             TimestampType(Timestamp(123123, 123000), 9), 2456, Decimal(2, 2, -22),
             Decimal(10, 10, -1234567890), Decimal(19, 19, 1234567890987654321)},
            {true, static_cast<int8_t>(127), static_cast<int16_t>(32767),
             static_cast<int32_t>(2147483647), 9999999999999, 1234.56f, 1234567890.0987654321,
             std::string("aa"), NullType(), NullType(), NullType(),
             TimestampType(Timestamp(999999, 999000), 9), 2456, Decimal(2, 2, 22),
             Decimal(10, 10, 1234567890), Decimal(19, 19, 1234567890987654321)},
            {1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 2, 1, 1, 2}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1754496160777ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    auto manifest_entry = ManifestEntry(FileKind::Add(), /*partition=*/BinaryRow::EmptyRow(),
                                        /*bucket=*/0, /*total_buckets=*/-1, file_meta);

    std::vector<ManifestEntry> expected_manifest_entries;
    expected_manifest_entries.emplace_back(manifest_entry);
    ASSERT_EQ(expected_manifest_entries, manifest_entries);
}

TEST_F(ManifestFileTest, TestManifestFileCompatibleWithJavaPaimon11) {
    auto pool = GetDefaultPool();
    auto manifest_entries =
        ReadManifestEntry("avro", paimon::test::GetDataDir() + "/avro", "avro_manifest_11", pool);
    ASSERT_EQ(manifest_entries.size(), 1);
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-0ff223ba-0d95-4c43-a25f-bcee3c051e58-0.avro", /*file_size=*/1615, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        ::paimon::test::BinaryRowGenerator::GenerateStats(
            {false, static_cast<int8_t>(-128), static_cast<int16_t>(-32768),
             static_cast<int32_t>(-2147483648), -9999999999999, -1234.56f, -1234567890.0987654321,
             std::string("aa"), NullType(), NullType(), NullType(),
             TimestampType(Timestamp(123123, 123000), 9), 2456, Decimal(2, 2, -22),
             Decimal(10, 10, -1234567890), Decimal(19, 19, 1234567890987654321)},
            {true, static_cast<int8_t>(127), static_cast<int16_t>(32767),
             static_cast<int32_t>(2147483647), 9999999999999, 1234.56f, 1234567890.0987654321,
             std::string("aa"), NullType(), NullType(), NullType(),
             TimestampType(Timestamp(999999, 999000), 9), 2456, Decimal(2, 2, 22),
             Decimal(10, 10, 1234567890), Decimal(19, 19, 1234567890987654321)},
            {1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 2, 1, 1, 2}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1754048761150ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    auto manifest_entry = ManifestEntry(FileKind::Add(), /*partition=*/BinaryRow::EmptyRow(),
                                        /*bucket=*/0, /*total_buckets=*/-1, file_meta);

    std::vector<ManifestEntry> expected_manifest_entries;
    expected_manifest_entries.emplace_back(manifest_entry);
    ASSERT_EQ(expected_manifest_entries, manifest_entries);
}

}  // namespace paimon::test
