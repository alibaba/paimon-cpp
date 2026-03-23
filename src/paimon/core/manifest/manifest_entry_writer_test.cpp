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

#include "paimon/core/manifest/manifest_entry_writer.h"

#include <map>
#include <optional>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/meta_to_arrow_array_converter.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_entry_serializer.h"
#include "paimon/core/utils/versioned_object_serializer.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon {
class WriterBuilder;
}  // namespace paimon

namespace paimon::test {
class ManifestEntryWriterTest : public ::testing::Test {
 public:
    ManifestEntry CreateEntry(const std::optional<int64_t>& first_row_id, int64_t row_count) const {
        auto meta = std::make_shared<DataFileMeta>(
            "data-d7725088-6bd4-4e70-9ce6-714ae93b47cc-0.orc", /*file_size=*/863, row_count,
            /*min_key=*/BinaryRow::EmptyRow(),
            /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/
            SimpleStats::EmptyStats(),
            /*value_stats=*/SimpleStats::EmptyStats(),
            /*min_sequence_number=*/0, /*max_sequence_number=*/row_count, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1743525392885ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt, first_row_id,
            /*write_cols=*/std::nullopt);
        return {FileKind::Add(), BinaryRowGenerator::GenerateRow({10}, pool_.get()), /*bucket=*/0,
                /*total_buckets=*/-1, meta};
    }

    std::unique_ptr<ManifestEntryWriter> CreateEntryWriter(
        const std::string& entry_file_name) const {
        std::shared_ptr<arrow::Schema> part_type =
            arrow::schema(arrow::FieldVector({arrow::field("f1", arrow::int32())}));
        auto serializer = std::make_shared<ManifestEntrySerializer>(pool_);
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<MetaToArrowArrayConverter> to_array_converter,
                             MetaToArrowArrayConverter::Create(serializer->GetDataType(), pool_));

        auto converter = [serializer, to_array_converter](ManifestEntry entry,
                                                          ::ArrowArray* dest) -> Status {
            PAIMON_ASSIGN_OR_RAISE(BinaryRow entry_row, serializer->ToRow(entry));
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> array,
                                   to_array_converter->NextBatch({entry_row}));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, dest));
            return Status::OK();
        };
        auto writer = std::make_unique<ManifestEntryWriter>("zstd", converter, pool_, part_type);

        EXPECT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{Options::FILE_FORMAT, "orc"}}));
        auto file_format = options.GetWriteFileFormat(/*level=*/0);
        std::shared_ptr<arrow::DataType> data_type =
            VersionedObjectSerializer<ManifestEntry>::VersionType(ManifestEntry::DataType());
        ArrowSchema arrow_schema;
        EXPECT_TRUE(arrow::ExportType(*data_type, &arrow_schema).ok());
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<WriterBuilder> writer_builder,
                             file_format->CreateWriterBuilder(&arrow_schema, /*batch_size=*/100));
        EXPECT_OK(writer->Init(options.GetFileSystem(), entry_file_name, writer_builder));
        return writer;
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(ManifestEntryWriterTest, TestSimple) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string entry_file_name = dir->Str() + "/manifest/my-manifest-file-name";
    auto writer = CreateEntryWriter(entry_file_name);

    auto meta1 = std::make_shared<DataFileMeta>(
        "data-d7725088-6bd4-4e70-9ce6-714ae93b47cc-0.orc", /*file_size=*/863, /*row_count=*/1,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alice", 1}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({"Alice", 1}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 1}, {"Alice", 1}, {0, 0}, pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 10, 1, 11.1}, {"Alice", 10, 1, 11.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/4, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1743525392885ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    auto meta2 = std::make_shared<DataFileMeta>(
        "data-5858a84b-7081-4618-b828-ae3918c5e1f6-0.orc", /*file_size=*/943, /*row_count=*/4,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alex", 0}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({"Tony", 0}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({"Alex", 0}, {"Tony", 0}, {0, 0}, pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alex", 20, 0, 12.1}, {"Tony", 20, 0, 16.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/3, /*schema_id=*/0,
        /*level=*/5, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1743525392921ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    auto entry1 = ManifestEntry(FileKind::Add(), BinaryRowGenerator::GenerateRow({10}, pool_.get()),
                                0, 2, meta1);
    auto entry2 = ManifestEntry(FileKind::Add(), BinaryRowGenerator::GenerateRow({20}, pool_.get()),
                                1, 2, meta2);
    ASSERT_OK(writer->Write(entry1));
    ASSERT_OK(writer->Write(entry2));
    ASSERT_EQ(writer->min_bucket_, 0);
    ASSERT_EQ(writer->max_bucket_, 1);
    ASSERT_EQ(writer->min_level_, 4);
    ASSERT_EQ(writer->max_level_, 5);

    // check partition stats
    ASSERT_OK_AND_ASSIGN(ManifestFileMeta meta, writer->GetResult());
    auto partition_stats = meta.PartitionStats();
    ASSERT_EQ(partition_stats, BinaryRowGenerator::GenerateStats({10}, {20}, {0}, pool_.get()));
}

TEST_F(ManifestEntryWriterTest, TestWithRowIds) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string entry_file_name = dir->Str() + "/manifest/my-manifest-file-name";
    auto writer = CreateEntryWriter(entry_file_name);
    // [10, 30)
    auto entry1 = CreateEntry(/*first_row_id=*/10, /*row_count=*/20);
    ASSERT_OK(writer->Write(entry1));
    // [50, 80)
    auto entry2 = CreateEntry(/*first_row_id=*/50, /*row_count=*/30);
    ASSERT_OK(writer->Write(entry2));
    ASSERT_TRUE(writer->row_id_stats_);
    ASSERT_EQ(writer->row_id_stats_.value().min_row_id, 10);
    ASSERT_EQ(writer->row_id_stats_.value().max_row_id, 79);
    // null first row id forces row_id_stats_ to be always null
    auto entry3 = CreateEntry(/*first_row_id=*/std::nullopt, /*row_count=*/30);
    ASSERT_OK(writer->Write(entry3));
    // [100, 110)
    auto entry4 = CreateEntry(/*first_row_id=*/100, /*row_count=*/10);
    ASSERT_OK(writer->Write(entry4));
    ASSERT_FALSE(writer->row_id_stats_);

    ASSERT_OK_AND_ASSIGN(ManifestFileMeta meta, writer->GetResult());
    ASSERT_FALSE(meta.MinRowId());
    ASSERT_FALSE(meta.MaxRowId());
}
}  // namespace paimon::test
