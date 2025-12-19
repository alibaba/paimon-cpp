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

#include "paimon/core/io/data_file_meta.h"

#include "gtest/gtest.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(DataFileMetaTest, TestAddRowCount) {
    DataFileMeta file_meta("data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0.orc", /*file_size=*/645,
                           /*row_count=*/5, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
                           SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
                           /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
                           /*level=*/0, /*extra_files=*/{},
                           /*creation_time=*/Timestamp(1737111915429ll, 0),
                           /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
                           /*value_stats_cols=*/std::nullopt,
                           /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
                           /*write_cols=*/std::nullopt);
    ASSERT_EQ(3, file_meta.AddRowCount().value());
    // test null delete row count
    file_meta.delete_row_count = std::nullopt;
    ASSERT_EQ(std::nullopt, file_meta.AddRowCount());
}

TEST(DataFileMetaTest, TestFileFormat) {
    DataFileMeta file_meta("data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0.orc", /*file_size=*/645,
                           /*row_count=*/5, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
                           SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
                           /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
                           /*level=*/0, /*extra_files=*/{},
                           /*creation_time=*/Timestamp(1737111915429ll, 0),
                           /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
                           /*value_stats_cols=*/std::nullopt,
                           /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
                           /*write_cols=*/std::nullopt);
    ASSERT_OK_AND_ASSIGN(auto file_format, file_meta.FileFormat());
    ASSERT_EQ("orc", file_format);
    file_meta.file_name = "data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0.parquet";
    ASSERT_OK_AND_ASSIGN(file_format, file_meta.FileFormat());
    ASSERT_EQ("parquet", file_format);
    // test invalid data file name
    file_meta.file_name = "data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0";
    ASSERT_NOK_WITH_MSG(file_meta.FileFormat(),
                        "cannot find format from file data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0");
}

TEST(DataFileMetaTest, TestExternalPathDir) {
    DataFileMeta file_meta(
        "data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0.orc", /*file_size=*/645, /*row_count=*/5,
        BinaryRow::EmptyRow(), BinaryRow::EmptyRow(), SimpleStats::EmptyStats(),
        SimpleStats::EmptyStats(),
        /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/{}, /*creation_time=*/Timestamp(1737111915429ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/"file:/tmp/bucket-0/data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0.orc",
        /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    ASSERT_EQ("file:/tmp/bucket-0", file_meta.ExternalPathDir().value());
    file_meta.external_path = std::nullopt;
    ASSERT_EQ(std::nullopt, file_meta.ExternalPathDir());
}

TEST(DataFileMetaTest, TestGetMaxSequenceNumber) {
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0.orc", /*file_size=*/645,
        /*row_count=*/5, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(), SimpleStats::EmptyStats(),
        SimpleStats::EmptyStats(),
        /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1737111915429ll, 0),
        /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-80110e15-97b5-4bcf-ac09-6ca2659a4950-1.orc", /*file_size=*/645,
        /*row_count=*/5, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(), SimpleStats::EmptyStats(),
        SimpleStats::EmptyStats(),
        /*min_sequence_number=*/5, /*max_sequence_number=*/10, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1737111915429ll, 0),
        /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    ASSERT_EQ(4, DataFileMeta::GetMaxSequenceNumber({file_meta1}));
    ASSERT_EQ(10, DataFileMeta::GetMaxSequenceNumber({file_meta1, file_meta2}));
    ASSERT_EQ(-1, DataFileMeta::GetMaxSequenceNumber({}));
}

TEST(DataFileMetaTest, TestNonNullFirstRowId) {
    {
        auto file_meta = std::make_shared<DataFileMeta>(
            "data-0.orc", /*file_size=*/645,
            /*row_count=*/5, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
            SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
            /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1737111915429ll, 0),
            /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt, /*first_row_id=*/100, /*write_cols=*/std::nullopt);
        ASSERT_OK_AND_ASSIGN(int64_t first_row_id, file_meta->NonNullFirstRowId());
        ASSERT_EQ(100, first_row_id);
    }
    {
        auto file_meta = std::make_shared<DataFileMeta>(
            "data-1.orc", /*file_size=*/645,
            /*row_count=*/5, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
            SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
            /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1737111915429ll, 0),
            /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        ASSERT_NOK_WITH_MSG(file_meta->NonNullFirstRowId(),
                            "First row id of data-1.orc should not be null.");
    }
}

TEST(DataFileMetaTest, TestToFileSelection) {
    // row id range [100, 110)
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-0.orc", /*file_size=*/645,
        /*row_count=*/10, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(), SimpleStats::EmptyStats(),
        SimpleStats::EmptyStats(),
        /*min_sequence_number=*/100, /*max_sequence_number=*/109, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1737111915429ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/100, /*write_cols=*/std::nullopt);

    {
        ASSERT_OK_AND_ASSIGN(std::optional<RoaringBitmap32> result,
                             file_meta->ToFileSelection(std::nullopt));
        ASSERT_FALSE(result);
    }
    {
        std::vector<Range> ranges = {Range(10l, 20l), Range(105l, 107l), Range(500l, 520l)};
        ASSERT_OK_AND_ASSIGN(std::optional<RoaringBitmap32> result,
                             file_meta->ToFileSelection(ranges));
        ASSERT_TRUE(result);
        ASSERT_EQ(result.value().ToString(), "{5,6,7}");
    }
    {
        std::vector<Range> ranges = {};
        ASSERT_OK_AND_ASSIGN(std::optional<RoaringBitmap32> result,
                             file_meta->ToFileSelection(ranges));
        ASSERT_TRUE(result);
        ASSERT_EQ(result.value().ToString(), "{}");
    }
    {
        std::vector<Range> ranges = {Range(100l, 109l)};
        ASSERT_OK_AND_ASSIGN(std::optional<RoaringBitmap32> result,
                             file_meta->ToFileSelection(ranges));
        ASSERT_FALSE(result);
    }
}

}  // namespace paimon::test
