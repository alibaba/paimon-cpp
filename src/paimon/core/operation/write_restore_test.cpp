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

#include "paimon/core/operation/write_restore.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/core/manifest/file_kind.h"

namespace paimon::test {

namespace {

std::shared_ptr<DataFileMeta> CreateDataFileMeta(const std::string& file_name) {
    return std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/128, /*row_count=*/10, DataFileMeta::EmptyMinKey(),
        DataFileMeta::EmptyMaxKey(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
        /*min_sequence_number=*/1, /*max_sequence_number=*/1, /*schema_id=*/1,
        /*level=*/DataFileMeta::DUMMY_LEVEL,
        /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(0, 0), /*delete_row_count=*/std::nullopt,
        /*embedded_index=*/nullptr, /*file_source=*/std::nullopt,
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
}

ManifestEntry CreateManifestEntry(int32_t total_buckets, const std::string& file_name) {
    return ManifestEntry(FileKind::Add(), BinaryRow::EmptyRow(), /*bucket=*/0, total_buckets,
                         CreateDataFileMeta(file_name));
}

}  // namespace

TEST(WriteRestoreTest, ExtractDataFilesEmptyEntries) {
    std::vector<ManifestEntry> entries;
    std::vector<std::shared_ptr<DataFileMeta>> data_files;

    auto result = WriteRestore::ExtractDataFiles(entries, &data_files);

    ASSERT_TRUE(result.ok()) << result.status().ToString();
    ASSERT_FALSE(result.value().has_value());
    ASSERT_TRUE(data_files.empty());
}

TEST(WriteRestoreTest, ExtractDataFilesConsistentTotalBuckets) {
    std::vector<ManifestEntry> entries = {
        CreateManifestEntry(/*total_buckets=*/4, "file-1.parquet"),
        CreateManifestEntry(/*total_buckets=*/4, "file-2.parquet"),
        CreateManifestEntry(/*total_buckets=*/4, "file-3.parquet")};
    std::vector<std::shared_ptr<DataFileMeta>> data_files;

    auto result = WriteRestore::ExtractDataFiles(entries, &data_files);

    ASSERT_TRUE(result.ok()) << result.status().ToString();
    ASSERT_TRUE(result.value().has_value());
    ASSERT_EQ(result.value().value(), 4);
    ASSERT_EQ(data_files.size(), 3);
    ASSERT_EQ(data_files[0]->file_name, "file-1.parquet");
    ASSERT_EQ(data_files[1]->file_name, "file-2.parquet");
    ASSERT_EQ(data_files[2]->file_name, "file-3.parquet");
}

TEST(WriteRestoreTest, ExtractDataFilesInconsistentTotalBuckets) {
    std::vector<ManifestEntry> entries = {
        CreateManifestEntry(/*total_buckets=*/2, "file-1.parquet"),
        CreateManifestEntry(/*total_buckets=*/3, "file-2.parquet")};
    std::vector<std::shared_ptr<DataFileMeta>> data_files;

    auto result = WriteRestore::ExtractDataFiles(entries, &data_files);

    ASSERT_FALSE(result.ok());
    ASSERT_NE(result.status().ToString().find("different total bucket number"), std::string::npos);
    ASSERT_EQ(data_files.size(), 1);
    ASSERT_EQ(data_files[0]->file_name, "file-1.parquet");
}

}  // namespace paimon::test
