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

#include "paimon/core/io/data_file_path_factory.h"

#include <optional>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/fs/external_path_provider.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class DataFilePathFactoryTest : public ::testing::Test {
 protected:
    void SetUp() override {
        // Initialize the DataFilePathFactory with a parent directory and format identifier
        ASSERT_OK(factory_.Init(/*parent=*/"/tmp", /*format_identifier=*/"txt",
                                /*data_file_prefix=*/"data-", /*external_path_provider=*/nullptr));
    }

    DataFilePathFactory factory_;
};

TEST_F(DataFilePathFactoryTest, TestNewPath) {
    std::string path1 = factory_.NewPath();
    std::string path2 = factory_.NewPath();

    // Ensure that the paths are unique
    ASSERT_NE(path1, path2);
    ASSERT_TRUE(path1.find("/tmp/data-") != std::string::npos);
    ASSERT_TRUE(path2.find("/tmp/data-") != std::string::npos);
    // test Parent() and NewPathFromName()
    ASSERT_EQ(factory_.Parent(), "/tmp");
    ASSERT_EQ(factory_.NewPathFromName("index-file"), "/tmp/index-file");
}

TEST_F(DataFilePathFactoryTest, TestNewExternalStorageBlobPath) {
    std::string blob_path1 = factory_.NewExternalStorageBlobPath("/tmp/external_blob");
    std::string blob_path2 = factory_.NewExternalStorageBlobPath("/tmp/external_blob");

    // Paths are unique (counter increments)
    ASSERT_NE(blob_path1, blob_path2);
    // Both start with the external storage path joined with the data file prefix
    ASSERT_TRUE(StringUtils::StartsWith(blob_path1, "/tmp/external_blob/data-"));
    ASSERT_TRUE(StringUtils::StartsWith(blob_path2, "/tmp/external_blob/data-"));
    // Both end with .blob extension
    ASSERT_TRUE(StringUtils::EndsWith(blob_path1, ".blob"));
    ASSERT_TRUE(StringUtils::EndsWith(blob_path2, ".blob"));
}

TEST_F(DataFilePathFactoryTest, TestNewPathWithDataFilePrefixAndExternalPath) {
    DataFilePathFactory factory;
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ExternalPathProvider> external_path_provider,
        ExternalPathProvider::Create({"/tmp/external_path/"}, "p0=1/p1=0/bucket-0"));

    ASSERT_OK(factory_.Init(/*parent=*/"/tmp/p0=1/p1=0/bucket-0/", /*format_identifier=*/"txt",
                            /*data_file_prefix=*/"test-data-", std::move(external_path_provider)));
    std::string path1 = factory_.NewPath();
    std::string path2 = factory_.NewPath();

    // Ensure that the paths are unique
    ASSERT_NE(path1, path2);
    ASSERT_TRUE(path1.find("/tmp/external_path/p0=1/p1=0/bucket-0/test-data-") !=
                std::string::npos);
    ASSERT_TRUE(path2.find("/tmp/external_path/p0=1/p1=0/bucket-0/test-data-") !=
                std::string::npos);

    // test Parent() and NewPathFromName()
    ASSERT_EQ(factory_.Parent(), "/tmp/p0=1/p1=0/bucket-0/");
    ASSERT_EQ(factory_.NewPathFromName("index-file"),
              "/tmp/external_path/p0=1/p1=0/bucket-0/index-file");
}

TEST_F(DataFilePathFactoryTest, TestToPath) {
    std::string file_name = "example.txt";

    ASSERT_EQ(factory_.ToPath(file_name), "/tmp/example.txt");

    auto file_meta = std::make_shared<DataFileMeta>(
        "example.txt", /*file_size=*/645, /*row_count=*/5, BinaryRow::EmptyRow(),
        BinaryRow::EmptyRow(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
        /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1737111915429ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/"file:/test/bucket-0/example.txt", /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    ASSERT_EQ(factory_.ToPath(file_meta), "file:/test/bucket-0/example.txt");
}

TEST_F(DataFilePathFactoryTest, TestToFileIndexPath) {
    std::string file_path = "/tmp/example.txt";
    std::string index_path = factory_.ToFileIndexPath(file_path);

    ASSERT_EQ(index_path, "/tmp/example.txt.index");
}

TEST_F(DataFilePathFactoryTest, TestToAlignedPath) {
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-0.txt", /*file_size=*/645, /*row_count=*/5, BinaryRow::EmptyRow(),
        BinaryRow::EmptyRow(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
        /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1737111915429ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/"file:/test/bucket-0/data-0.txt", /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);

    ASSERT_EQ(factory_.ToAlignedPath("index-0", file_meta), "file:/test/bucket-0/index-0");

    file_meta->external_path = std::nullopt;
    ASSERT_EQ(factory_.ToAlignedPath("index-0", file_meta), "/tmp/index-0");
}

TEST_F(DataFilePathFactoryTest, TestCollectFiles) {
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-0.txt", /*file_size=*/645, /*row_count=*/5, BinaryRow::EmptyRow(),
        BinaryRow::EmptyRow(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
        /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1737111915429ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    ASSERT_EQ(factory_.CollectFiles(file_meta), std::vector<std::string>({"/tmp/data-0.txt"}));

    file_meta->extra_files = {"data-0.index", "data-1.index"};
    ASSERT_EQ(
        factory_.CollectFiles(file_meta),
        std::vector<std::string>({"/tmp/data-0.txt", "/tmp/data-0.index", "/tmp/data-1.index"}));
}
}  // namespace paimon::test
