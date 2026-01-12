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

#include "paimon/core/operation/manifest_file_merger.h"

#include <cassert>
#include <functional>
#include <limits>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "arrow/type.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/data/timestamp.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class ManifestFileMergerTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        dir_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        test_root_ = dir_->Str();
        partition_type_ = arrow::int64();
        CreateManifestFile(test_root_);
        assert(manifest_file_);
    }

    void TearDown() override {}

    ManifestEntry MakeEntry(FileKind kind, const std::string& file_name) {
        return MakeEntry(kind, file_name, 0);
    }

    ManifestEntry MakeEntry(FileKind kind, const std::string& file_name,
                            std::optional<int32_t> partition) {
        BinaryRow binary_row = BinaryRow::EmptyRow();
        if (partition != std::nullopt) {
            binary_row = BinaryRow(1);
            BinaryRowWriter writer(&binary_row, 0, pool_.get());
            writer.WriteInt(0, partition.value());
            writer.Complete();
        }

        return ManifestEntry(
            kind, binary_row,
            0,  // not used
            0,  // not used
            std::make_shared<DataFileMeta>(
                file_name,
                0,                          // not used
                0,                          // not used
                binary_row,                 // not used
                binary_row,                 // not used
                SimpleStats::EmptyStats(),  // not used
                SimpleStats::EmptyStats(),  // not used
                0,                          // not used
                0,                          // not used
                0,                          // not used
                0,                          // not used
                /*extra_files=*/std::vector<std::optional<std::string>>(), Timestamp(200000, 0),
                0,        // not used
                nullptr,  // not used
                FileSource::Append(), /*value_stats_cols=*/std::nullopt,
                /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
                /*write_cols=*/std::nullopt));
    }

    ManifestFileMeta MakeManifest(const std::vector<ManifestEntry>& entries) {
        EXPECT_OK_AND_ASSIGN(std::vector<ManifestFileMeta> manifest_file_metas,
                             manifest_file_->Write(entries));
        // force the file size of each manifest to be 3000
        manifest_file_metas[0].file_size_ = 3000;
        return manifest_file_metas[0];
    }

 private:
    void CreateManifestFile(const std::string& path_str) {
        auto file_system = std::make_shared<LocalFileSystem>();
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FileFormat> file_format,
            FileFormatFactory::Get("parquet", std::map<std::string, std::string>()));
        auto schema = arrow::schema(arrow::FieldVector({arrow::field("f0", partition_type_)}));
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
        ASSERT_OK_AND_ASSIGN(std::vector<std::string> external_paths,
                             options.CreateExternalPaths());
        ASSERT_OK_AND_ASSIGN(std::optional<std::string> global_index_external_path,
                             options.CreateGlobalIndexExternalPath());

        ASSERT_OK_AND_ASSIGN(
            static std::shared_ptr<FileStorePathFactory> path_factory,
            FileStorePathFactory::Create(
                path_str, schema, /*partition_keys=*/{"f0"}, options.GetPartitionDefaultName(),
                options.GetWriteFileFormat()->Identifier(), options.DataFilePrefix(),
                options.LegacyPartitionNameEnabled(), external_paths, global_index_external_path,
                options.IndexFileInDataFileDir(), pool_));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Schema> partition_schema,
                             FieldMapping::GetPartitionSchema(schema, {"f0"}));
        ASSERT_OK_AND_ASSIGN(manifest_file_,
                             ManifestFile::Create(file_system, file_format, "zstd", path_factory,
                                                  /*target_file_size=*/1024 * 1024, pool_, options,
                                                  partition_schema));
    }

    void ContainSameEntryFile(
        const std::vector<ManifestFileMeta>& metas,
        const std::vector<std::pair<std::string, FileKind>>& entry_file_name_expected) {
        std::vector<ManifestEntry> entries;
        for (const auto& meta : metas) {
            ASSERT_OK(manifest_file_->Read(meta.FileName(), /*filter=*/nullptr, &entries));
        }
        std::vector<std::pair<std::string, FileKind>> entry_file_name_actual;
        for (const auto& entry : entries) {
            entry_file_name_actual.emplace_back(entry.FileName(), entry.Kind());
        }
        ASSERT_EQ(entry_file_name_expected, entry_file_name_actual);
    }

    void AssertEquivalentEntries(const std::vector<ManifestFileMeta>& lhs,
                                 const std::vector<ManifestFileMeta>& rhs) {
        ASSERT_EQ(lhs.size(), rhs.size());
        for (uint32_t i = 0; i < lhs.size(); i++) {
            ASSERT_EQ(lhs[i].ToString(), rhs[i].ToString());
        }
    }

    static constexpr int64_t MAX_LONG_VALUE = std::numeric_limits<int64_t>::max();
    static constexpr int32_t MAX_INT_VALUE = std::numeric_limits<int32_t>::max();

    std::unique_ptr<UniqueTestDirectory> dir_;
    std::string test_root_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<ManifestFile> manifest_file_;
    std::shared_ptr<arrow::DataType> partition_type_;
};

TEST_F(ManifestFileMergerTest, TestMergeWithoutCompaction) {
    std::vector<ManifestEntry> entries;
    for (int i = 0; i < 16; i++) {
        entries.push_back(MakeEntry(FileKind::Add(), std::to_string(i)));
    }
    std::vector<ManifestFileMeta> input;
    // base manifest
    input.push_back(MakeManifest(entries));

    // delta manifest
    input.push_back(MakeManifest({MakeEntry(FileKind::Add(), "A"), MakeEntry(FileKind::Add(), "B"),
                                  MakeEntry(FileKind::Add(), "C")}));
    input.push_back(MakeManifest({MakeEntry(FileKind::Add(), "D")}));
    input.push_back(
        MakeManifest({MakeEntry(FileKind::Delete(), "A"), MakeEntry(FileKind::Delete(), "B"),
                      MakeEntry(FileKind::Add(), "F")}));
    input.push_back(
        MakeManifest({MakeEntry(FileKind::Delete(), "14"), MakeEntry(FileKind::Delete(), "15")}));
    input.push_back(
        MakeManifest({MakeEntry(FileKind::Delete(), "C"), MakeEntry(FileKind::Delete(), "D"),
                      MakeEntry(FileKind::Delete(), "F"), MakeEntry(FileKind::Add(), "G")}));
    // every file is larger than target manifest file size 500
    ASSERT_OK_AND_ASSIGN(
        std::vector<ManifestFileMeta> actual,
        ManifestFileMerger::Merge(input, 500, 3, MAX_LONG_VALUE, manifest_file_.get()));
    AssertEquivalentEntries(input, actual);
}

TEST_F(ManifestFileMergerTest, TestMergeWithoutDeleteFile) {
    // entries are All Add().
    std::vector<ManifestFileMeta> input;
    // base
    for (int j = 0; j < 6; j++) {
        std::vector<ManifestEntry> entries;
        for (int i = 1; i < 16; i++) {
            entries.push_back(MakeEntry(FileKind::Add(), fmt::format("{}-{}", j, i), j));
        }
        input.push_back(MakeManifest(entries));
    }
    // delta
    input.push_back(MakeManifest({MakeEntry(FileKind::Add(), "A")}));
    input.push_back(MakeManifest({MakeEntry(FileKind::Add(), "B")}));
    input.push_back(MakeManifest({MakeEntry(FileKind::Add(), "C")}));
    input.push_back(MakeManifest({MakeEntry(FileKind::Add(), "D")}));
    input.push_back(MakeManifest({MakeEntry(FileKind::Add(), "E")}));
    input.push_back(MakeManifest({MakeEntry(FileKind::Add(), "F")}));
    input.push_back(MakeManifest({MakeEntry(FileKind::Add(), "G")}));

    ASSERT_OK_AND_ASSIGN(std::vector<ManifestFileMeta> merged,
                         ManifestFileMerger::Merge(input, 500, 3, 200, manifest_file_.get()));
    AssertEquivalentEntries(input, merged);
}

TEST_F(ManifestFileMergerTest, TestTriggerMinorCompaction) {
    std::vector<ManifestEntry> entries;
    for (int i = 0; i < 16; i++) {
        entries.push_back(MakeEntry(FileKind::Add(), std::to_string(i)));
    }
    std::vector<ManifestFileMeta> input;
    // base manifest
    input.push_back(MakeManifest(entries));

    // delta manifest
    input.push_back(MakeManifest({MakeEntry(FileKind::Add(), "A"), MakeEntry(FileKind::Add(), "B"),
                                  MakeEntry(FileKind::Add(), "C")}));
    input.push_back(MakeManifest({MakeEntry(FileKind::Add(), "D")}));
    input.push_back(
        MakeManifest({MakeEntry(FileKind::Delete(), "A"), MakeEntry(FileKind::Delete(), "B"),
                      MakeEntry(FileKind::Add(), "F")}));
    input.push_back(
        MakeManifest({MakeEntry(FileKind::Delete(), "14"), MakeEntry(FileKind::Delete(), "15")}));
    input.push_back(
        MakeManifest({MakeEntry(FileKind::Delete(), "C"), MakeEntry(FileKind::Delete(), "D"),
                      MakeEntry(FileKind::Delete(), "F"), MakeEntry(FileKind::Add(), "G")}));

    std::vector<ManifestFileMeta> new_metas;
    // trigger minor compaction
    ASSERT_OK_AND_ASSIGN(std::vector<ManifestFileMeta> merged,
                         ManifestFileMerger::TryMinorCompaction(
                             input, /*manifest_target_file_size=*/5000,
                             /*merge_min_count=*/30, manifest_file_.get(), &new_metas));
    ASSERT_EQ(3, new_metas.size());

    std::vector<std::pair<std::string, FileKind>> entry_file_expected;
    for (int i = 0; i < 16; i++) {
        entry_file_expected.emplace_back(std::to_string(i), FileKind::Add());
    }
    entry_file_expected.emplace_back("A", FileKind::Add());
    entry_file_expected.emplace_back("B", FileKind::Add());
    entry_file_expected.emplace_back("C", FileKind::Add());
    entry_file_expected.emplace_back("D", FileKind::Add());
    entry_file_expected.emplace_back("A", FileKind::Delete());
    entry_file_expected.emplace_back("B", FileKind::Delete());
    entry_file_expected.emplace_back("F", FileKind::Add());
    entry_file_expected.emplace_back("14", FileKind::Delete());
    entry_file_expected.emplace_back("15", FileKind::Delete());
    entry_file_expected.emplace_back("C", FileKind::Delete());
    entry_file_expected.emplace_back("D", FileKind::Delete());
    entry_file_expected.emplace_back("F", FileKind::Delete());
    entry_file_expected.emplace_back("G", FileKind::Add());
    ContainSameEntryFile(merged, entry_file_expected);
}

TEST_F(ManifestFileMergerTest, TestTriggerMinorCompactionWithLastBit) {
    std::vector<ManifestEntry> entries;
    for (int i = 0; i < 16; i++) {
        entries.push_back(MakeEntry(FileKind::Add(), std::to_string(i)));
    }
    std::vector<ManifestFileMeta> input;
    // base manifest
    input.push_back(MakeManifest(entries));

    // delta manifest
    input.push_back(MakeManifest({MakeEntry(FileKind::Add(), "A"), MakeEntry(FileKind::Add(), "B"),
                                  MakeEntry(FileKind::Add(), "C")}));
    input.push_back(MakeManifest({MakeEntry(FileKind::Add(), "D")}));
    input.push_back(
        MakeManifest({MakeEntry(FileKind::Delete(), "A"), MakeEntry(FileKind::Delete(), "B"),
                      MakeEntry(FileKind::Add(), "F")}));
    input.push_back(
        MakeManifest({MakeEntry(FileKind::Delete(), "14"), MakeEntry(FileKind::Delete(), "15")}));
    input.push_back(
        MakeManifest({MakeEntry(FileKind::Delete(), "C"), MakeEntry(FileKind::Delete(), "D"),
                      MakeEntry(FileKind::Delete(), "F"), MakeEntry(FileKind::Add(), "G")}));

    std::vector<ManifestFileMeta> new_metas;
    // trigger minor compaction
    ASSERT_OK_AND_ASSIGN(std::vector<ManifestFileMeta> merged,
                         ManifestFileMerger::TryMinorCompaction(
                             input, /*manifest_target_file_size=*/10000,
                             /*merge_min_count=*/2, manifest_file_.get(), &new_metas));
    ASSERT_EQ(2, new_metas.size());

    std::vector<std::pair<std::string, FileKind>> entry_file_expected;
    for (int i = 0; i < 16; i++) {
        entry_file_expected.emplace_back(std::to_string(i), FileKind::Add());
    }
    entry_file_expected.emplace_back("C", FileKind::Add());
    entry_file_expected.emplace_back("D", FileKind::Add());
    entry_file_expected.emplace_back("F", FileKind::Add());

    entry_file_expected.emplace_back("14", FileKind::Delete());
    entry_file_expected.emplace_back("15", FileKind::Delete());
    entry_file_expected.emplace_back("C", FileKind::Delete());
    entry_file_expected.emplace_back("D", FileKind::Delete());
    entry_file_expected.emplace_back("F", FileKind::Delete());
    entry_file_expected.emplace_back("G", FileKind::Add());
    ContainSameEntryFile(merged, entry_file_expected);
}

TEST_F(ManifestFileMergerTest, TestTriggerFullCompaction) {
    std::vector<ManifestEntry> entries;
    for (int i = 0; i < 16; i++) {
        entries.push_back(MakeEntry(FileKind::Add(), std::to_string(i)));
    }

    std::vector<ManifestFileMeta> input;

    // base manifest
    input.push_back(MakeManifest(entries));

    // delta manifest
    input.push_back(MakeManifest({MakeEntry(FileKind::Add(), "A"), MakeEntry(FileKind::Add(), "B"),
                                  MakeEntry(FileKind::Add(), "C")}));
    input.push_back(MakeManifest({MakeEntry(FileKind::Add(), "D")}));
    input.push_back(
        MakeManifest({MakeEntry(FileKind::Delete(), "A"), MakeEntry(FileKind::Delete(), "B"),
                      MakeEntry(FileKind::Add(), "F")}));
    input.push_back(
        MakeManifest({MakeEntry(FileKind::Delete(), "14"), MakeEntry(FileKind::Delete(), "15")}));
    input.push_back(
        MakeManifest({MakeEntry(FileKind::Delete(), "C"), MakeEntry(FileKind::Delete(), "D"),
                      MakeEntry(FileKind::Delete(), "F"), MakeEntry(FileKind::Add(), "G")}));

    // no trigger for delta size
    std::vector<ManifestFileMeta> new_metas;
    ASSERT_OK_AND_ASSIGN(
        std::optional<std::vector<ManifestFileMeta>> full_compacted,
        ManifestFileMerger::TryFullCompaction(input, /*manifest_target_file_size=*/500,
                                              /*full_compaction_file_size=*/MAX_INT_VALUE,
                                              manifest_file_.get(), &new_metas));
    ASSERT_EQ(std::nullopt, full_compacted);
    ASSERT_EQ(0, new_metas.size());
    new_metas.clear();

    // trigger full compaction
    ASSERT_OK_AND_ASSIGN(std::optional<std::vector<ManifestFileMeta>> merged,
                         ManifestFileMerger::TryFullCompaction(
                             input, /*manifest_target_file_size=*/5000,
                             /*full_compaction_file_size=*/100, manifest_file_.get(), &new_metas));
    ASSERT_NE(std::nullopt, merged);
    ASSERT_GT(new_metas.size(), 0);

    std::vector<std::pair<std::string, FileKind>> entry_file_expected;
    for (int i = 0; i < 14; i++) {
        entry_file_expected.emplace_back(std::to_string(i), FileKind::Add());
    }
    entry_file_expected.emplace_back("G", FileKind::Add());
    ContainSameEntryFile(merged.value(), entry_file_expected);
}

}  // namespace paimon::test
