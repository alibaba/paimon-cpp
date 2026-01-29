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

#include "paimon/format/orc/read_range_generator.h"

#include <cstddef>
#include <iostream>
#include <sstream>

#include "gtest/gtest.h"
#include "orc/MemoryPool.hh"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "orc/Writer.hh"
#include "orc/orc-config.hh"
#include "paimon/format/orc/orc_format_defs.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::orc::test {

class ReadRangeGeneratorTest : public ::testing::Test {
 public:
    bool CheckEqual(const std::vector<std::pair<uint64_t, uint64_t>>& expected,
                    const std::vector<std::pair<uint64_t, uint64_t>>& actual) const {
        if (expected.size() != actual.size()) {
            std::cout << "expected: " << ToString(expected) << std::endl;
            std::cout << "actual: " << ToString(actual) << std::endl;
            return false;
        }
        for (size_t i = 0; i < expected.size(); i++) {
            if (expected[i] != actual[i]) {
                std::cout << "expected: " << ToString(expected) << std::endl;
                std::cout << "actual: " << ToString(actual) << std::endl;
                return false;
            }
        }
        return true;
    }

    std::string ToString(const std::vector<std::pair<uint64_t, uint64_t>>& ranges) const {
        std::stringstream ss;
        for (size_t i = 0; i < ranges.size(); i++) {
            ss << "[ " << ranges[i].first << "," << ranges[i].second << " ] ";
            if (i % 5 == 0 && i != 0) {
                ss << std::endl;
            }
        }
        ss << std::endl;
        return ss.str();
    }
};

TEST_F(ReadRangeGeneratorTest, EmptyFile) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    std::string file_path = dir->Str() + "/empty_file.orc";
    std::unique_ptr<::orc::Type> schema = ::orc::createStructType();
    schema->addStructField("id", ::orc::createPrimitiveType(::orc::TypeKind::INT));
    schema->addStructField("name", ::orc::createPrimitiveType(::orc::TypeKind::STRING));
    schema->addStructField("is_active", ::orc::createPrimitiveType(::orc::TypeKind::BOOLEAN));
    schema->addStructField("value", ::orc::createPrimitiveType(::orc::TypeKind::DOUBLE));
    ::orc::WriterOptions options;
    ORC_UNIQUE_PTR<::orc::OutputStream> output_stream = ::orc::writeLocalFile(file_path);
    std::unique_ptr<::orc::Writer> writer =
        ::orc::createWriter(*schema, output_stream.get(), options);
    writer->close();
    auto input_stream = ::orc::readLocalFile(file_path);
    auto reader = createReader(std::move(input_stream), ::orc::ReaderOptions());
    ASSERT_OK_AND_ASSIGN(auto range_generator,
                         ReadRangeGenerator::Create(reader.get(), 1024 * 1024, {}));
    bool need_prefetch = false;
    ASSERT_OK_AND_ASSIGN(auto read_ranges, range_generator->GenReadRanges(
                                               /*target_column_ids=*/{1, 2}, /*begin_row_num=*/0,
                                               /*end_row_num=*/100, &need_prefetch));
    ASSERT_FALSE(need_prefetch);
    std::vector<std::pair<uint64_t, uint64_t>> empty_ranges;
    ASSERT_EQ(read_ranges, empty_ranges);
}

TEST_F(ReadRangeGeneratorTest, Simple) {
    /*
{ "name": "/tmp/paimon_test_a6daa513-04df-423f-98d1-7eebc52510ae/simple.orc",
  "type": "struct<name:string,id:bigint>",
  "attributes": {},
  "rows": 300000,
  "stripe count": 1,
  "format": "0.12", "writer version": "ORC-135", "software version": "ORC C++ 2.1.1",
  "compression": "zstd", "compression block": 65536,
  "file length": 3763,
  "content": 3528, "stripe stats": 64, "footer": 143, "postscript": 24,
  "row index stride": 10000,
  "user metadata": {
  },
  "stripes": [
    { "stripe": 0, "rows": 300000,
      "offset": 3, "length": 3528,
      "index": 929, "data": 2512, "footer": 87,
      "encodings": [
         { "column": 0, "encoding": "direct" },
         { "column": 1, "encoding": "dictionary rle2", "count": 100 },
         { "column": 2, "encoding": "direct rle2" }
      ],
      "streams": [
        { "id": 0, "column": 0, "kind": "index", "offset": 3, "length": 29 },
        { "id": 1, "column": 1, "kind": "index", "offset": 32, "length": 291 },
        { "id": 2, "column": 2, "kind": "index", "offset": 323, "length": 609 },
        { "id": 3, "column": 1, "kind": "data", "offset": 932, "length": 1682 },
        { "id": 4, "column": 1, "kind": "dictionary", "offset": 2614, "length": 251 },
        { "id": 5, "column": 1, "kind": "length", "offset": 2865, "length": 26 },
        { "id": 6, "column": 2, "kind": "data", "offset": 2891, "length": 553 }
      ],
      "timezone": "GMT"
    }
  ]
}
     */
    auto dir = paimon::test::UniqueTestDirectory::Create();
    std::string file_path = dir->Str() + "/simple.orc";
    std::unique_ptr<::orc::Type> schema = ::orc::createStructType();
    schema->addStructField("name", ::orc::createPrimitiveType(::orc::TypeKind::STRING));
    schema->addStructField("id", ::orc::createPrimitiveType(::orc::TypeKind::LONG));
    ::orc::WriterOptions options;
    options.setDictionaryKeySizeThreshold(1);
    ORC_UNIQUE_PTR<::orc::OutputStream> output_stream = ::orc::writeLocalFile(file_path);
    std::unique_ptr<::orc::Writer> writer =
        ::orc::createWriter(*schema, output_stream.get(), options);
    const int64_t num_rows = 300000;
    for (int64_t i = 0; i < num_rows; ++i) {
        auto batch = writer->createRowBatch(1);
        auto struct_batch = dynamic_cast<::orc::StructVectorBatch*>(batch.get());
        auto* string_batch = dynamic_cast<::orc::StringVectorBatch*>(struct_batch->fields[0]);
        auto* int64_batch = dynamic_cast<::orc::LongVectorBatch*>(struct_batch->fields[1]);
        std::string str = "Row" + std::to_string(i % 100);
        string_batch->data[0] = str.data();
        string_batch->length[0] = str.size();
        int64_batch->data[0] = i;
        batch->numElements = 1;
        writer->add(*struct_batch);
    }
    writer->close();
    auto input_stream = ::orc::readLocalFile(file_path);
    auto reader = createReader(std::move(input_stream), ::orc::ReaderOptions());
    ASSERT_OK_AND_ASSIGN(auto range_generator,
                         ReadRangeGenerator::Create(reader.get(), /*natural_read_size=*/1024,
                                                    {{ENABLE_PREFETCH_READ_SIZE_THRESHOLD, "0"}}));
    bool need_prefetch = false;
    ASSERT_OK_AND_ASSIGN(auto read_ranges,
                         range_generator->GenReadRanges(
                             /*target_column_ids=*/{1, 2}, /*begin_row_num=*/0,
                             /*end_row_num=*/reader->getNumberOfRows(), &need_prefetch));
    ASSERT_TRUE(need_prefetch);
    std::vector<std::pair<uint64_t, uint64_t>> expect_ranges = {
        {0, 140000}, {140000, 280000}, {280000, 300000}};
    EXPECT_EQ(read_ranges, expect_ranges);
    EXPECT_EQ(140000, range_generator->SuggestRowCount({1}));
    EXPECT_EQ(140000, range_generator->SuggestRowCount({1, 2}));
    EXPECT_EQ(270000, range_generator->SuggestRowCount({2}));
}

TEST_F(ReadRangeGeneratorTest, SuggestRowCountWithFewRows) {
    // struct<f0:string,f1:int,f2:int,f3:double>
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/f1=10/bucket-1/"
                            "data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc";
    auto input_stream = ::orc::readLocalFile(file_name);
    auto reader = createReader(std::move(input_stream), ::orc::ReaderOptions());
    ASSERT_OK_AND_ASSIGN(auto range_generator,
                         ReadRangeGenerator::Create(reader.get(), 1024 * 1024, {}));
    EXPECT_EQ(8, range_generator->SuggestRowCount({1}));
    EXPECT_EQ(8, range_generator->SuggestRowCount({4}));
}

TEST_F(ReadRangeGeneratorTest, TestBytesPerGroup) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/f1=10/bucket-1/"
                            "data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc";

    std::unique_ptr<::orc::Reader> reader =
        ::orc::createReader(::orc::readLocalFile(file_name), ::orc::ReaderOptions());
    ASSERT_OK_AND_ASSIGN(auto range_generator,
                         ReadRangeGenerator::Create(reader.get(), 1024 * 1024, {}));
    EXPECT_EQ(37500, range_generator->BytesPerGroup({2}));
    EXPECT_EQ(96250, range_generator->BytesPerGroup({1, 2}));
    EXPECT_EQ(96250, range_generator->BytesPerGroup({1}));
}

TEST_F(ReadRangeGeneratorTest, TestGenReadRanges) {
    ReadRangeGenerator::ReaderMeta meta;
    meta.rows_per_group = 10000;
    meta.rows_per_stripes = {20480, 20480, 20480, 20480, 2080};
    auto read_ranges_vec = ReadRangeGenerator::DoGenReadRanges(
        /*begin_row_num=*/0, /*end_row_num=*/84000, /*range_size=*/10000, meta);
    ASSERT_TRUE(CheckEqual({{0, 10000},
                            {10000, 20000},
                            {20000, 20480},
                            {20480, 30480},
                            {30480, 40480},
                            {40480, 40960},
                            {40960, 50960},
                            {50960, 60960},
                            {60960, 61440},
                            {61440, 71440},
                            {71440, 81440},
                            {81440, 81920},
                            {81920, 84000}},
                           read_ranges_vec));
}

TEST_F(ReadRangeGeneratorTest, TestGenReadRangesBasic) {
    ReadRangeGenerator::ReaderMeta meta;
    meta.rows_per_group = 3;
    meta.rows_per_stripes = {4, 6};
    auto read_ranges_vec = ReadRangeGenerator::DoGenReadRanges(
        /*begin_row_num=*/0, /*end_row_num=*/10, /*range_size=*/3, meta);
    ASSERT_TRUE(CheckEqual({{0, 3}, {3, 4}, {4, 7}, {7, 10}}, read_ranges_vec));
}

TEST_F(ReadRangeGeneratorTest, TestGenReadRangesMultiStripes) {
    ReadRangeGenerator::ReaderMeta meta;
    meta.rows_per_group = 4;
    meta.rows_per_stripes = {10, 15};
    auto read_ranges_vec = ReadRangeGenerator::DoGenReadRanges(
        /*begin_row_num=*/5, /*end_row_num=*/25, /*range_size=*/5, meta);
    ASSERT_TRUE(CheckEqual({{5, 10}, {10, 15}, {15, 20}, {20, 25}}, read_ranges_vec));
}

TEST_F(ReadRangeGeneratorTest, TestGenReadRangesInvalid) {
    ReadRangeGenerator::ReaderMeta meta;
    meta.rows_per_group = 4;
    meta.rows_per_stripes = {10, 15};
    auto read_ranges_vec = ReadRangeGenerator::DoGenReadRanges(
        /*begin_row_num=*/3, /*end_row_num=*/2, /*range_size=*/5, meta);
    ASSERT_TRUE(CheckEqual({}, read_ranges_vec));
}

TEST_F(ReadRangeGeneratorTest, TestGenReadRangesStripeBound) {
    ReadRangeGenerator::ReaderMeta meta;
    meta.rows_per_group = 5;
    meta.rows_per_stripes = {10, 10};
    auto read_ranges_vec = ReadRangeGenerator::DoGenReadRanges(
        /*begin_row_num=*/3, /*end_row_num=*/18, /*range_size=*/5, meta);
    ASSERT_TRUE(CheckEqual({{3, 8}, {8, 10}, {10, 15}, {15, 18}}, read_ranges_vec));
}

TEST_F(ReadRangeGeneratorTest, TestGenReadRangesNotMatch) {
    ReadRangeGenerator::ReaderMeta meta;
    meta.rows_per_group = 2;
    meta.rows_per_stripes = {4, 6};
    auto read_ranges_vec = ReadRangeGenerator::DoGenReadRanges(
        /*begin_row_num=*/10, /*end_row_num=*/10, /*range_size=*/2, meta);
    ASSERT_TRUE(CheckEqual({}, read_ranges_vec));
}

TEST_F(ReadRangeGeneratorTest, CreateWithNullReader) {
    ASSERT_NOK(ReadRangeGenerator::Create(nullptr, 1024 * 1024, {}));
}

TEST_F(ReadRangeGeneratorTest, BytesPerGroupWithEmptyColumns) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    std::string file_path = dir->Str() + "/empty_columns.orc";
    std::unique_ptr<::orc::Type> schema = ::orc::createStructType();
    schema->addStructField("id", ::orc::createPrimitiveType(::orc::TypeKind::INT));
    ::orc::WriterOptions options;
    ORC_UNIQUE_PTR<::orc::OutputStream> output_stream = ::orc::writeLocalFile(file_path);
    std::unique_ptr<::orc::Writer> writer =
        ::orc::createWriter(*schema, output_stream.get(), options);
    writer->close();
    auto input_stream = ::orc::readLocalFile(file_path);
    auto reader = createReader(std::move(input_stream), ::orc::ReaderOptions());
    ASSERT_OK_AND_ASSIGN(auto range_generator,
                         ReadRangeGenerator::Create(reader.get(), 1024 * 1024, {}));
    EXPECT_EQ(1, range_generator->BytesPerGroup({}));
}

}  // namespace paimon::orc::test
