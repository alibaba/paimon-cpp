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

#include "paimon/format/parquet/column_index_filter.h"

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/defs.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/format/parquet/parquet_format_writer.h"
#include "paimon/format/parquet/row_ranges.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/testharness.h"
#include "parquet/file_reader.h"

namespace paimon::parquet::test {

// =====================================================================
// RowRanges unit tests
// =====================================================================

class RowRangesTest : public ::testing::Test {
 protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(RowRangesTest, TestCreateSingle) {
    RowRanges ranges = RowRanges::CreateSingle(100);
    EXPECT_FALSE(ranges.IsEmpty());
    EXPECT_EQ(100, ranges.RowCount());
    EXPECT_EQ(1, ranges.GetRanges().size());
    EXPECT_EQ(0, ranges.GetRanges()[0].from);
    EXPECT_EQ(99, ranges.GetRanges()[0].to);
}

TEST_F(RowRangesTest, TestCreateEmpty) {
    RowRanges ranges = RowRanges::CreateEmpty();
    EXPECT_TRUE(ranges.IsEmpty());
    EXPECT_EQ(0, ranges.RowCount());
    EXPECT_EQ(0, ranges.GetRanges().size());
}

TEST_F(RowRangesTest, TestAddRange) {
    RowRanges ranges;
    ranges.Add(RowRanges::Range(10, 20));
    EXPECT_FALSE(ranges.IsEmpty());
    EXPECT_EQ(11, ranges.RowCount());
    EXPECT_EQ(1, ranges.GetRanges().size());
}

TEST_F(RowRangesTest, TestAddOverlappingRanges) {
    RowRanges ranges;
    ranges.Add(RowRanges::Range(10, 20));
    ranges.Add(RowRanges::Range(15, 25));  // overlaps with [10, 20]
    EXPECT_EQ(1, ranges.GetRanges().size());
    EXPECT_EQ(10, ranges.GetRanges()[0].from);
    EXPECT_EQ(25, ranges.GetRanges()[0].to);
    EXPECT_EQ(16, ranges.RowCount());
}

TEST_F(RowRangesTest, TestAddAdjacentRanges) {
    RowRanges ranges;
    ranges.Add(RowRanges::Range(10, 20));
    ranges.Add(RowRanges::Range(21, 30));  // adjacent to [10, 20]
    EXPECT_EQ(1, ranges.GetRanges().size());
    EXPECT_EQ(10, ranges.GetRanges()[0].from);
    EXPECT_EQ(30, ranges.GetRanges()[0].to);
    EXPECT_EQ(21, ranges.RowCount());
}

TEST_F(RowRangesTest, TestAddNonOverlappingRanges) {
    RowRanges ranges;
    ranges.Add(RowRanges::Range(10, 20));
    ranges.Add(RowRanges::Range(30, 40));
    EXPECT_EQ(2, ranges.GetRanges().size());
    EXPECT_EQ(10, ranges.GetRanges()[0].from);
    EXPECT_EQ(20, ranges.GetRanges()[0].to);
    EXPECT_EQ(30, ranges.GetRanges()[1].from);
    EXPECT_EQ(40, ranges.GetRanges()[1].to);
    EXPECT_EQ(22, ranges.RowCount());
}

TEST_F(RowRangesTest, TestUnion) {
    RowRanges left;
    left.Add(RowRanges::Range(10, 20));
    left.Add(RowRanges::Range(40, 50));

    RowRanges right;
    right.Add(RowRanges::Range(15, 25));
    right.Add(RowRanges::Range(60, 70));

    RowRanges result = RowRanges::Union(left, right);
    EXPECT_EQ(3, result.GetRanges().size());
    EXPECT_EQ(10, result.GetRanges()[0].from);
    EXPECT_EQ(25, result.GetRanges()[0].to);
    EXPECT_EQ(40, result.GetRanges()[1].from);
    EXPECT_EQ(50, result.GetRanges()[1].to);
    EXPECT_EQ(60, result.GetRanges()[2].from);
    EXPECT_EQ(70, result.GetRanges()[2].to);
}

TEST_F(RowRangesTest, TestUnionWithOverlap) {
    RowRanges left;
    left.Add(RowRanges::Range(10, 30));

    RowRanges right;
    right.Add(RowRanges::Range(20, 40));

    RowRanges result = RowRanges::Union(left, right);
    EXPECT_EQ(1, result.GetRanges().size());
    EXPECT_EQ(10, result.GetRanges()[0].from);
    EXPECT_EQ(40, result.GetRanges()[0].to);
}

TEST_F(RowRangesTest, TestIntersection) {
    RowRanges left;
    left.Add(RowRanges::Range(10, 30));
    left.Add(RowRanges::Range(50, 70));

    RowRanges right;
    right.Add(RowRanges::Range(20, 40));
    right.Add(RowRanges::Range(60, 80));

    RowRanges result = RowRanges::Intersection(left, right);
    EXPECT_EQ(2, result.GetRanges().size());
    EXPECT_EQ(20, result.GetRanges()[0].from);
    EXPECT_EQ(30, result.GetRanges()[0].to);
    EXPECT_EQ(60, result.GetRanges()[1].from);
    EXPECT_EQ(70, result.GetRanges()[1].to);
}

TEST_F(RowRangesTest, TestIntersectionNoOverlap) {
    RowRanges left;
    left.Add(RowRanges::Range(10, 20));

    RowRanges right;
    right.Add(RowRanges::Range(30, 40));

    RowRanges result = RowRanges::Intersection(left, right);
    EXPECT_TRUE(result.IsEmpty());
}

TEST_F(RowRangesTest, TestIntersectionEmptyLeft) {
    RowRanges left = RowRanges::CreateEmpty();

    RowRanges right;
    right.Add(RowRanges::Range(10, 20));

    RowRanges result = RowRanges::Intersection(left, right);
    EXPECT_TRUE(result.IsEmpty());
}

TEST_F(RowRangesTest, TestIsOverlapping) {
    RowRanges ranges;
    ranges.Add(RowRanges::Range(10, 20));
    ranges.Add(RowRanges::Range(30, 40));

    EXPECT_TRUE(ranges.IsOverlapping(10, 20));
    EXPECT_TRUE(ranges.IsOverlapping(15, 25));
    EXPECT_TRUE(ranges.IsOverlapping(30, 40));
    EXPECT_FALSE(ranges.IsOverlapping(21, 29));
    EXPECT_FALSE(ranges.IsOverlapping(5, 9));
    EXPECT_FALSE(ranges.IsOverlapping(41, 50));
}

TEST_F(RowRangesTest, TestRowCount) {
    RowRanges ranges;
    ranges.Add(RowRanges::Range(0, 9));
    ranges.Add(RowRanges::Range(20, 29));
    EXPECT_EQ(20, ranges.RowCount());

    ranges.Add(RowRanges::Range(10, 19));  // Fill the gap
    EXPECT_EQ(30, ranges.RowCount());
}

TEST_F(RowRangesTest, TestToString) {
    RowRanges ranges;
    ranges.Add(RowRanges::Range(10, 20));
    ranges.Add(RowRanges::Range(30, 40));
    EXPECT_EQ("[[10, 20], [30, 40]]", ranges.ToString());
}

TEST_F(RowRangesTest, TestRangeOperations) {
    RowRanges::Range r1(10, 20);
    RowRanges::Range r2(30, 40);
    RowRanges::Range r3(15, 25);

    // r1 lies entirely before r2; r3 overlaps r1.
    EXPECT_TRUE(r1.to < r2.from);
    EXPECT_FALSE(r1.from > r2.to);
    EXPECT_FALSE(r1.to < r3.from);
    EXPECT_FALSE(r1.from > r3.to);
    EXPECT_EQ(11, r1.Count());
}

// =====================================================================
// ColumnIndexFilter integration tests
// =====================================================================

/// Test fixture that creates real Parquet files with page index for testing
/// ColumnIndexFilter::CalculateRowRanges end-to-end.
///
/// Data layout: 100 rows, 10 pages of 10 rows each.
///   Page 0: val [0, 9]
///   Page 1: val [10, 19]
///   ...
///   Page 9: val [90, 99]
class ColumnIndexFilterTest : public ::testing::Test {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
        arrow_pool_ = GetArrowPool(pool_);
        dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        fs_ = dir_->GetFileSystem();

        // Write the test file once for all tests
        file_name_ = dir_->Str() + "/col_index_filter.parquet";
        auto data = MakeSequentialIntData(100);
        WriteTestFile(file_name_, data, /*write_batch_size=*/10, /*max_row_group_length=*/100);

        // Open as raw ParquetFileReader
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_name_));
        ASSERT_OK_AND_ASSIGN(uint64_t length, in->Length());
        auto in_stream = std::make_shared<ArrowInputStreamAdapter>(in, arrow_pool_, length);
        parquet_reader_ = ::parquet::ParquetFileReader::Open(in_stream);
        ASSERT_TRUE(parquet_reader_);

        page_index_reader_ = parquet_reader_->GetPageIndexReader();
        ASSERT_TRUE(page_index_reader_);

        column_name_to_index_["val"] = 0;
        row_group_row_count_ = parquet_reader_->metadata()->RowGroup(0)->num_rows();
    }

    static std::shared_ptr<arrow::StructArray> MakeSequentialIntData(int32_t num_rows) {
        arrow::Int32Builder builder;
        EXPECT_TRUE(builder.Reserve(num_rows).ok());
        for (int32_t i = 0; i < num_rows; ++i) {
            builder.UnsafeAppend(i);
        }
        auto array = builder.Finish().ValueOrDie();
        auto field = arrow::field("val", arrow::int32());
        return arrow::StructArray::Make({array}, {field}).ValueOrDie();
    }

    void WriteTestFile(const std::string& file_name,
                       const std::shared_ptr<arrow::StructArray>& struct_array,
                       int32_t write_batch_size, int64_t max_row_group_length) {
        auto data_type = struct_array->struct_type();
        auto data_schema = arrow::schema(data_type->fields());
        auto data_arrow_array = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*struct_array, data_arrow_array.get()).ok());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                             fs_->Create(file_name, /*overwrite=*/false));
        ::parquet::WriterProperties::Builder wp_builder;
        wp_builder.write_batch_size(write_batch_size);
        wp_builder.max_row_group_length(max_row_group_length);
        wp_builder.disable_dictionary();
        wp_builder.enable_write_page_index();
        wp_builder.data_pagesize(1);
        auto writer_properties = wp_builder.build();
        ASSERT_OK_AND_ASSIGN(
            auto format_writer,
            ParquetFormatWriter::Create(out, data_schema, writer_properties,
                                        DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE, arrow_pool_));
        ASSERT_OK(format_writer->AddBatch(data_arrow_array.get()));
        ASSERT_OK(format_writer->Finish());
        ASSERT_OK(out->Close());
    }

    Result<RowRanges> Filter(const std::shared_ptr<Predicate>& predicate) {
        return ColumnIndexFilter::CalculateRowRanges(predicate, page_index_reader_,
                                                     column_name_to_index_, /*row_group_index=*/0,
                                                     row_group_row_count_);
    }

    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<FileSystem> fs_;
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::string file_name_;
    std::unique_ptr<::parquet::ParquetFileReader> parquet_reader_;
    std::shared_ptr<::parquet::PageIndexReader> page_index_reader_;
    std::map<std::string, int32_t> column_name_to_index_;
    int64_t row_group_row_count_ = 0;
};

/// EQUAL: val = 55 → should match only page 5 (rows [50,59])
TEST_F(ColumnIndexFilterTest, EqualMatchSinglePage) {
    auto pred =
        PredicateBuilder::Equal(0, "val", FieldType::INT, Literal(static_cast<int32_t>(55)));
    ASSERT_OK_AND_ASSIGN(auto ranges, Filter(pred));
    EXPECT_FALSE(ranges.IsEmpty());
    // Page 5 covers rows [50, 59]
    EXPECT_EQ(10, ranges.RowCount());
    EXPECT_EQ(50, ranges.GetRanges()[0].from);
    EXPECT_EQ(59, ranges.GetRanges()[0].to);
}

/// EQUAL: val = 0 → should match page 0 (rows [0,9])
TEST_F(ColumnIndexFilterTest, EqualMatchFirstPage) {
    auto pred = PredicateBuilder::Equal(0, "val", FieldType::INT, Literal(static_cast<int32_t>(0)));
    ASSERT_OK_AND_ASSIGN(auto ranges, Filter(pred));
    EXPECT_FALSE(ranges.IsEmpty());
    EXPECT_EQ(10, ranges.RowCount());
    EXPECT_EQ(0, ranges.GetRanges()[0].from);
    EXPECT_EQ(9, ranges.GetRanges()[0].to);
}

/// EQUAL: val = 999 → should match no pages (value out of range)
TEST_F(ColumnIndexFilterTest, EqualNoMatch) {
    auto pred =
        PredicateBuilder::Equal(0, "val", FieldType::INT, Literal(static_cast<int32_t>(999)));
    ASSERT_OK_AND_ASSIGN(auto ranges, Filter(pred));
    EXPECT_TRUE(ranges.IsEmpty());
}

/// LESS_THAN: val < 25 → should match pages 0,1,2 (rows [0,29])
/// Page 0: [0,9], Page 1: [10,19], Page 2: [20,29] — page 2 has min=20 < 25
TEST_F(ColumnIndexFilterTest, LessThanMatchMultiplePages) {
    auto pred =
        PredicateBuilder::LessThan(0, "val", FieldType::INT, Literal(static_cast<int32_t>(25)));
    ASSERT_OK_AND_ASSIGN(auto ranges, Filter(pred));
    EXPECT_FALSE(ranges.IsEmpty());
    // Pages 0-2 match (min < 25)
    EXPECT_EQ(30, ranges.RowCount());
    EXPECT_EQ(0, ranges.GetRanges()[0].from);
    EXPECT_EQ(29, ranges.GetRanges()[0].to);
}

/// LESS_THAN: val < 0 → no pages match (min of page 0 is 0, which is not < 0)
TEST_F(ColumnIndexFilterTest, LessThanNoMatch) {
    auto pred =
        PredicateBuilder::LessThan(0, "val", FieldType::INT, Literal(static_cast<int32_t>(0)));
    ASSERT_OK_AND_ASSIGN(auto ranges, Filter(pred));
    EXPECT_TRUE(ranges.IsEmpty());
}

/// GREATER_THAN: val > 85 → should match pages 8,9
/// Page 8: max=89 > 85, Page 9: max=99 > 85
TEST_F(ColumnIndexFilterTest, GreaterThanMatchLastPages) {
    auto pred =
        PredicateBuilder::GreaterThan(0, "val", FieldType::INT, Literal(static_cast<int32_t>(85)));
    ASSERT_OK_AND_ASSIGN(auto ranges, Filter(pred));
    EXPECT_FALSE(ranges.IsEmpty());
    EXPECT_EQ(20, ranges.RowCount());
    EXPECT_EQ(80, ranges.GetRanges()[0].from);
    EXPECT_EQ(99, ranges.GetRanges()[0].to);
}

/// GREATER_THAN: val > 99 → no pages match
TEST_F(ColumnIndexFilterTest, GreaterThanNoMatch) {
    auto pred =
        PredicateBuilder::GreaterThan(0, "val", FieldType::INT, Literal(static_cast<int32_t>(99)));
    ASSERT_OK_AND_ASSIGN(auto ranges, Filter(pred));
    EXPECT_TRUE(ranges.IsEmpty());
}

/// LESS_OR_EQUAL: val <= 9 → page 0 only (max=9 <= 9, but page 1 min=10 > 9)
TEST_F(ColumnIndexFilterTest, LessOrEqualBoundary) {
    auto pred =
        PredicateBuilder::LessOrEqual(0, "val", FieldType::INT, Literal(static_cast<int32_t>(9)));
    ASSERT_OK_AND_ASSIGN(auto ranges, Filter(pred));
    EXPECT_EQ(10, ranges.RowCount());
    EXPECT_EQ(0, ranges.GetRanges()[0].from);
    EXPECT_EQ(9, ranges.GetRanges()[0].to);
}

/// GREATER_OR_EQUAL: val >= 90 → page 9 only
TEST_F(ColumnIndexFilterTest, GreaterOrEqualBoundary) {
    auto pred = PredicateBuilder::GreaterOrEqual(0, "val", FieldType::INT,
                                                 Literal(static_cast<int32_t>(90)));
    ASSERT_OK_AND_ASSIGN(auto ranges, Filter(pred));
    EXPECT_EQ(10, ranges.RowCount());
    EXPECT_EQ(90, ranges.GetRanges()[0].from);
    EXPECT_EQ(99, ranges.GetRanges()[0].to);
}

/// IN: val IN (5, 55, 95) → pages 0, 5, 9
TEST_F(ColumnIndexFilterTest, InMatchMultiplePages) {
    auto pred =
        PredicateBuilder::In(0, "val", FieldType::INT,
                             {Literal(static_cast<int32_t>(5)), Literal(static_cast<int32_t>(55)),
                              Literal(static_cast<int32_t>(95))});
    ASSERT_OK_AND_ASSIGN(auto ranges, Filter(pred));
    EXPECT_FALSE(ranges.IsEmpty());
    // Pages 0, 5, 9
    EXPECT_EQ(3, ranges.GetRanges().size());
    EXPECT_EQ(0, ranges.GetRanges()[0].from);
    EXPECT_EQ(9, ranges.GetRanges()[0].to);
    EXPECT_EQ(50, ranges.GetRanges()[1].from);
    EXPECT_EQ(59, ranges.GetRanges()[1].to);
    EXPECT_EQ(90, ranges.GetRanges()[2].from);
    EXPECT_EQ(99, ranges.GetRanges()[2].to);
}

/// IN: val IN (999) → no match
TEST_F(ColumnIndexFilterTest, InNoMatch) {
    auto pred =
        PredicateBuilder::In(0, "val", FieldType::INT, {Literal(static_cast<int32_t>(999))});
    ASSERT_OK_AND_ASSIGN(auto ranges, Filter(pred));
    EXPECT_TRUE(ranges.IsEmpty());
}

/// IS_NOT_NULL on non-nullable column → all pages match
TEST_F(ColumnIndexFilterTest, IsNotNullAllPages) {
    auto pred = PredicateBuilder::IsNotNull(0, "val", FieldType::INT);
    ASSERT_OK_AND_ASSIGN(auto ranges, Filter(pred));
    EXPECT_EQ(row_group_row_count_, ranges.RowCount());
}

/// AND: val >= 30 AND val < 50 → pages 3, 4
TEST_F(ColumnIndexFilterTest, AndCompound) {
    auto ge = PredicateBuilder::GreaterOrEqual(0, "val", FieldType::INT,
                                               Literal(static_cast<int32_t>(30)));
    auto lt =
        PredicateBuilder::LessThan(0, "val", FieldType::INT, Literal(static_cast<int32_t>(50)));
    ASSERT_OK_AND_ASSIGN(auto pred, PredicateBuilder::And({ge, lt}));
    ASSERT_OK_AND_ASSIGN(auto ranges, Filter(pred));
    EXPECT_EQ(20, ranges.RowCount());
    EXPECT_EQ(30, ranges.GetRanges()[0].from);
    EXPECT_EQ(49, ranges.GetRanges()[0].to);
}

/// OR: val < 10 OR val >= 90 → pages 0, 9
TEST_F(ColumnIndexFilterTest, OrCompound) {
    auto lt =
        PredicateBuilder::LessThan(0, "val", FieldType::INT, Literal(static_cast<int32_t>(10)));
    auto ge = PredicateBuilder::GreaterOrEqual(0, "val", FieldType::INT,
                                               Literal(static_cast<int32_t>(90)));
    ASSERT_OK_AND_ASSIGN(auto pred, PredicateBuilder::Or({lt, ge}));
    ASSERT_OK_AND_ASSIGN(auto ranges, Filter(pred));
    EXPECT_EQ(2, ranges.GetRanges().size());
    EXPECT_EQ(0, ranges.GetRanges()[0].from);
    EXPECT_EQ(9, ranges.GetRanges()[0].to);
    EXPECT_EQ(90, ranges.GetRanges()[1].from);
    EXPECT_EQ(99, ranges.GetRanges()[1].to);
}

/// Predicates referencing fields absent from the data file are stripped upstream
/// by FieldMappingBuilder, so reaching ColumnIndexFilter with such a predicate is
/// a contract violation and surfaces as an error.
TEST_F(ColumnIndexFilterTest, UnknownColumnReturnsError) {
    auto pred = PredicateBuilder::Equal(0, "nonexistent", FieldType::INT,
                                        Literal(static_cast<int32_t>(42)));
    EXPECT_FALSE(Filter(pred).ok());
}

/// Null predicate → all rows
TEST_F(ColumnIndexFilterTest, NullPredicateReturnsAllRows) {
    ASSERT_OK_AND_ASSIGN(auto ranges, Filter(nullptr));
    EXPECT_EQ(row_group_row_count_, ranges.RowCount());
}

}  // namespace paimon::parquet::test
