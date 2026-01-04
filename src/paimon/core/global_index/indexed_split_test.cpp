/*
 * Copyright 2025-present Alibaba Inc.
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

#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/data_define.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(IndexedSplitTest, TestSimple) {
    std::string file_name = paimon::test::GetDataDir() + "/global_index/indexed_split-01";
    auto file_system = std::make_unique<LocalFileSystem>();

    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);

    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Split> result,
        Split::Deserialize(reinterpret_cast<char*>(split_bytes.data()), split_bytes.size(), pool));

    auto result_indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(result);

    auto meta1 = std::make_shared<DataFileMeta>(
        "file1.orc", 100l, 200l, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
        SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), 50l, 249l, 0, 0,
        std::vector<std::optional<std::string>>(), Timestamp(1765535214349l, 0), 0, nullptr,
        FileSource::Append(), std::nullopt, std::nullopt, 50l, std::nullopt);
    auto meta2 = std::make_shared<DataFileMeta>(
        "file2.orc", 101l, 100l, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
        SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), 250l, 349l, 0, 0,
        std::vector<std::optional<std::string>>(), Timestamp(1765535214349l, 0), 0, nullptr,
        FileSource::Append(), std::nullopt, std::nullopt, 250l, std::nullopt);
    auto meta3 = std::make_shared<DataFileMeta>(
        "file3.orc", 102l, 200l, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
        SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), 1000l, 1199l, 0, 0,
        std::vector<std::optional<std::string>>(), Timestamp(1765535214349l, 0), 0, nullptr,
        FileSource::Append(), std::nullopt, std::nullopt, 1000l, std::nullopt);

    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRow::EmptyRow(),
        /*bucket=*/0, /*bucket_path=*/
        "data/test_table/bucket-0",
        std::vector<std::shared_ptr<DataFileMeta>>({meta1, meta2, meta3}));

    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(
        builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build().value());

    std::vector<Range> ranges = {Range(55, 56), Range(270, 270), Range(1001, 1002)};
    auto expected_indexed_split = std::make_shared<IndexedSplitImpl>(expected_data_split, ranges);

    ASSERT_EQ(*result_indexed_split, *expected_indexed_split) << result_indexed_split->ToString();
    ASSERT_OK_AND_ASSIGN(std::string serialize_bytes, Split::Serialize(result_indexed_split, pool));
    ASSERT_EQ(serialize_bytes,
              std::string(reinterpret_cast<char*>(split_bytes.data()), split_bytes.size()));
}

TEST(IndexedSplitTest, TestIndexedSplitWithScore) {
    std::string file_name = paimon::test::GetDataDir() + "/global_index/indexed_split-02";
    auto file_system = std::make_unique<LocalFileSystem>();

    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    std::vector<char> split_bytes(input_stream->Length().value_or(0), 0);

    ASSERT_OK(input_stream->Read(split_bytes.data(), split_bytes.size()));
    ASSERT_OK(input_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Split> result,
        Split::Deserialize(reinterpret_cast<char*>(split_bytes.data()), split_bytes.size(), pool));

    auto result_indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(result);

    auto meta1 = std::make_shared<DataFileMeta>(
        "file1.orc", 100l, 200l, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
        SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), 50l, 249l, 0, 0,
        std::vector<std::optional<std::string>>(), Timestamp(1765549435648l, 0), 0, nullptr,
        FileSource::Append(), std::nullopt, std::nullopt, 50l, std::nullopt);
    auto meta2 = std::make_shared<DataFileMeta>(
        "file2.orc", 101l, 100l, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
        SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), 250l, 349l, 0, 0,
        std::vector<std::optional<std::string>>(), Timestamp(1765549435649l, 0), 0, nullptr,
        FileSource::Append(), std::nullopt, std::nullopt, 250l, std::nullopt);
    auto meta3 = std::make_shared<DataFileMeta>(
        "file3.orc", 102l, 200l, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
        SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), 1000l, 1199l, 0, 0,
        std::vector<std::optional<std::string>>(), Timestamp(1765549435649l, 0), 0, nullptr,
        FileSource::Append(), std::nullopt, std::nullopt, 1000l, std::nullopt);

    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRow::EmptyRow(),
        /*bucket=*/0, /*bucket_path=*/
        "data/test_table/bucket-0",
        std::vector<std::shared_ptr<DataFileMeta>>({meta1, meta2, meta3}));

    auto expected_data_split = std::dynamic_pointer_cast<DataSplitImpl>(
        builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build().value());

    std::vector<Range> ranges = {Range(55, 56), Range(270, 270), Range(1001, 1002)};
    std::vector<float> scores = {1.01f, 2.10f, -1.32f, 4.23f, 50.74f};
    auto expected_indexed_split =
        std::make_shared<IndexedSplitImpl>(expected_data_split, ranges, scores);

    ASSERT_EQ(*result_indexed_split, *expected_indexed_split) << result_indexed_split->ToString();
    ASSERT_TRUE(
        result_indexed_split->ToString().find(
            "rowRanges=[[55, 56],[270, 270],[1001, 1002]], scores=[1.01,2.1,-1.32,4.23,50.74]") !=
        std::string::npos);
    ASSERT_OK_AND_ASSIGN(std::string serialize_bytes, Split::Serialize(result_indexed_split, pool));
    ASSERT_EQ(serialize_bytes,
              std::string(reinterpret_cast<char*>(split_bytes.data()), split_bytes.size()));
}

TEST(IndexedSplitTest, TestValidate) {
    auto meta = std::make_shared<DataFileMeta>(
        "file.orc", 1l, 200l, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
        SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), 1000l, 1199l, 0, 0,
        std::vector<std::optional<std::string>>(), Timestamp(0l, 0), 0, nullptr,
        FileSource::Append(), std::nullopt, std::nullopt, 1000l, std::nullopt);

    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRow::EmptyRow(),
        /*bucket=*/0, /*bucket_path=*/
        "data/test_table/bucket-0", std::vector<std::shared_ptr<DataFileMeta>>({meta}));

    auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(
        builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build().value());

    {
        std::vector<Range> row_ranges = {Range(10, 20), Range(30, 40)};
        IndexedSplitImpl split(data_split, row_ranges);
        ASSERT_OK(split.Validate());
    }
    {
        std::vector<Range> row_ranges = {Range(10, 12), Range(30, 31)};
        std::vector<float> scores = {10.01f, 10.11f, 10.21f, -30.01f, -30.11f};
        IndexedSplitImpl split(data_split, row_ranges, scores);
        ASSERT_OK(split.Validate());
    }
    {
        std::vector<Range> row_ranges = {Range(10, 12), Range(30, 31)};
        std::vector<float> scores = {10.01f, 10.11f, 10.21f, -30.01f};
        IndexedSplitImpl split(data_split, row_ranges, scores);
        ASSERT_NOK_WITH_MSG(split.Validate(),
                            "Scores length does not match row ranges in indexed split.");
    }
    {
        std::vector<Range> row_ranges = {};
        IndexedSplitImpl split(data_split, row_ranges);
        ASSERT_NOK_WITH_MSG(split.Validate(),
                            "Invalid IndexedSplit: row ranges mismatch data files.");
    }
    {
        std::vector<Range> row_ranges = {Range(10, 12)};
        DataSplitImpl::Builder empty_builder(
            /*partition=*/BinaryRow::EmptyRow(),
            /*bucket=*/0, /*bucket_path=*/
            "data/test_table/bucket-0", std::vector<std::shared_ptr<DataFileMeta>>({}));
        auto empty_data_split = std::dynamic_pointer_cast<DataSplitImpl>(
            empty_builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build().value());

        IndexedSplitImpl split(empty_data_split, row_ranges);
        ASSERT_NOK_WITH_MSG(split.Validate(),
                            "Invalid IndexedSplit: row ranges mismatch data files.");
    }
}
}  // namespace paimon::test
