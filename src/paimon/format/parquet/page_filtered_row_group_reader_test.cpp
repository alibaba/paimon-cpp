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

#include "paimon/format/parquet/page_filtered_row_group_reader.h"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/defs.h"
#include "paimon/format/parquet/parquet_file_batch_reader.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/format/parquet/parquet_format_writer.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"
#include "parquet/properties.h"

namespace paimon {
class Predicate;
}  // namespace paimon

namespace paimon::parquet::test {

/// Test fixture for page-level filtering.
/// Creates Parquet files with multiple row groups and small page sizes to ensure
/// multiple pages per row group, enabling page-level filtering tests.
class PageFilteredRowGroupReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        arrow_pool_ = GetArrowPool(pool_);
        dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        fs_ = dir_->GetFileSystem();
    }

    /// Write a Parquet file with controlled page boundaries.
    /// @param file_name Output file name
    /// @param struct_array Data to write
    /// @param write_batch_size Controls page size (number of rows per page)
    /// @param max_row_group_length Controls row group size
    void WriteTestFile(const std::string& file_name,
                       const std::shared_ptr<arrow::StructArray>& struct_array,
                       int32_t write_batch_size, int64_t max_row_group_length,
                       bool enable_dictionary = false) {
        auto data_type = struct_array->struct_type();
        auto data_schema = arrow::schema(data_type->fields());
        auto data_arrow_array = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*struct_array, data_arrow_array.get()).ok());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                             fs_->Create(file_name, /*overwrite=*/false));
        ::parquet::WriterProperties::Builder builder;
        builder.write_batch_size(write_batch_size);
        builder.max_row_group_length(max_row_group_length);
        if (enable_dictionary) {
            builder.enable_dictionary();
        } else {
            builder.disable_dictionary();  // Ensure page index min/max are meaningful
        }
        builder.enable_write_page_index();  // Enable page index for page-level filtering
        // Set data page size to 1 byte to force a new page after every write_batch_size rows.
        // The writer flushes a page when accumulated data exceeds data_pagesize, so setting
        // it to 1 ensures each batch of write_batch_size rows becomes exactly one page.
        builder.data_pagesize(1);
        auto writer_properties = builder.build();
        ASSERT_OK_AND_ASSIGN(
            auto format_writer,
            ParquetFormatWriter::Create(out, data_schema, writer_properties,
                                        DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE, arrow_pool_));
        ASSERT_OK(format_writer->AddBatch(data_arrow_array.get()));
        ASSERT_OK(format_writer->Finish());
        ASSERT_OK(out->Close());
    }

    /// Read back a Parquet file with an optional predicate and page index filter enabled.
    /// Returns the collected result as a ChunkedArray.
    void ReadWithPredicateImpl(const std::string& file_name,
                               const std::shared_ptr<arrow::Schema>& read_schema,
                               const std::shared_ptr<Predicate>& predicate,
                               std::shared_ptr<arrow::ChunkedArray>* out,
                               int32_t batch_size = 1024) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_name));
        ASSERT_OK_AND_ASSIGN(int64_t length, in->Length());
        auto in_stream = std::make_shared<ArrowInputStreamAdapter>(in, arrow_pool_, length);

        std::map<std::string, std::string> options;
        options[PARQUET_READ_ENABLE_PAGE_INDEX_FILTER] = "true";
        ASSERT_OK_AND_ASSIGN(
            auto batch_reader,
            ParquetFileBatchReader::Create(std::move(in_stream), arrow_pool_, options, batch_size));
        auto c_schema = std::make_unique<ArrowSchema>();
        ASSERT_TRUE(arrow::ExportSchema(*read_schema, c_schema.get()).ok());
        ASSERT_OK(batch_reader->SetReadSchema(c_schema.get(), predicate,
                                              /*selection_bitmap=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(*out,
                             paimon::test::ReadResultCollector::CollectResult(batch_reader.get()));
    }

 protected:
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<FileSystem> fs_;
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
};

// Helper: build a StructArray with N rows of int32 "val" column with sequential values.
// val[i] = i for i in [0, N).
static std::shared_ptr<arrow::StructArray> MakeSequentialIntData(int32_t num_rows) {
    arrow::Int32Builder val_builder;
    EXPECT_TRUE(val_builder.Reserve(num_rows).ok());
    for (int32_t i = 0; i < num_rows; ++i) {
        val_builder.UnsafeAppend(i);
    }
    auto val_array = val_builder.Finish().ValueOrDie();
    auto field = arrow::field("val", arrow::int32());
    return arrow::StructArray::Make({val_array}, {field}).ValueOrDie();
}

// Helper: build a StructArray with two int32 columns: "a" and "b".
// a[i] = i, b[i] = i * 10, for i in [0, N).
static std::shared_ptr<arrow::StructArray> MakeTwoColumnData(int32_t num_rows) {
    arrow::Int32Builder a_builder, b_builder;
    EXPECT_TRUE(a_builder.Reserve(num_rows).ok());
    EXPECT_TRUE(b_builder.Reserve(num_rows).ok());
    for (int32_t i = 0; i < num_rows; ++i) {
        a_builder.UnsafeAppend(i);
        b_builder.UnsafeAppend(i * 10);
    }
    auto a_array = a_builder.Finish().ValueOrDie();
    auto b_array = b_builder.Finish().ValueOrDie();
    auto field_a = arrow::field("a", arrow::int32());
    auto field_b = arrow::field("b", arrow::int32());
    return arrow::StructArray::Make({a_array, b_array}, {field_a, field_b}).ValueOrDie();
}

/// Test: page-level filtering correctly skips non-matching pages.
///
/// Scenario: 100 rows, 10 rows per page, 1 row group.
/// val[i] = i. Predicate: val >= 50. Pages 0-4 (rows 0-49) should be skipped,
/// pages 5-9 (rows 50-99) should be read.
TEST_F(PageFilteredRowGroupReaderTest, SingleRowGroupPartialPageMatch) {
    std::string file_name = dir_->Str() + "/single_rg_partial.parquet";
    auto data = MakeSequentialIntData(100);
    WriteTestFile(file_name, data, /*write_batch_size=*/10, /*max_row_group_length=*/100);

    auto read_schema = arrow::schema({arrow::field("val", arrow::int32())});
    auto predicate = PredicateBuilder::GreaterOrEqual(
        /*field_index=*/0, /*field_name=*/"val", FieldType::INT, Literal(50));

    std::shared_ptr<arrow::ChunkedArray> result;
    ReadWithPredicateImpl(file_name, read_schema, predicate, &result);

    // Should get rows 50-99 = 50 rows
    ASSERT_TRUE(result);
    ASSERT_EQ(50, result->length());

    // Verify actual values
    auto flat = result->chunk(0);
    auto struct_arr = std::dynamic_pointer_cast<arrow::StructArray>(flat);
    ASSERT_TRUE(struct_arr);
    auto val_arr = std::dynamic_pointer_cast<arrow::Int32Array>(struct_arr->field(0));
    ASSERT_TRUE(val_arr);
    for (int32_t i = 0; i < 50; ++i) {
        ASSERT_EQ(50 + i, val_arr->Value(i)) << "Mismatch at index " << i;
    }
}

/// Test: predicate matches all pages → same as unfiltered read.
TEST_F(PageFilteredRowGroupReaderTest, AllPagesMatch) {
    std::string file_name = dir_->Str() + "/all_match.parquet";
    auto data = MakeSequentialIntData(100);
    WriteTestFile(file_name, data, /*write_batch_size=*/10, /*max_row_group_length=*/100);

    auto read_schema = arrow::schema({arrow::field("val", arrow::int32())});
    auto predicate = PredicateBuilder::GreaterOrEqual(
        /*field_index=*/0, /*field_name=*/"val", FieldType::INT, Literal(0));

    std::shared_ptr<arrow::ChunkedArray> result;
    ReadWithPredicateImpl(file_name, read_schema, predicate, &result);
    ASSERT_TRUE(result);
    ASSERT_EQ(100, result->length());
}

/// Test: predicate matches no pages → empty result.
TEST_F(PageFilteredRowGroupReaderTest, NoPagesMatch) {
    std::string file_name = dir_->Str() + "/no_match.parquet";
    auto data = MakeSequentialIntData(100);
    WriteTestFile(file_name, data, /*write_batch_size=*/10, /*max_row_group_length=*/100);

    auto read_schema = arrow::schema({arrow::field("val", arrow::int32())});
    auto predicate = PredicateBuilder::GreaterThan(
        /*field_index=*/0, /*field_name=*/"val", FieldType::INT, Literal(999));

    std::shared_ptr<arrow::ChunkedArray> result;
    ReadWithPredicateImpl(file_name, read_schema, predicate, &result);
    // No matching rows; result should be null (empty)
    ASSERT_FALSE(result);
}

/// Test: multiple row groups, page filtering active on some.
///
/// 200 rows, 10 rows per page, 50 rows per row group → 4 row groups.
/// Predicate: val >= 150. Row groups 0-2 (rows 0-149) should be eliminated entirely.
/// Row group 3 (rows 150-199): all pages match → full read, no page filtering.
TEST_F(PageFilteredRowGroupReaderTest, MultipleRowGroupsFullElimination) {
    std::string file_name = dir_->Str() + "/multi_rg_elim.parquet";
    auto data = MakeSequentialIntData(200);
    WriteTestFile(file_name, data, /*write_batch_size=*/10, /*max_row_group_length=*/50);

    auto read_schema = arrow::schema({arrow::field("val", arrow::int32())});
    auto predicate = PredicateBuilder::GreaterOrEqual(
        /*field_index=*/0, /*field_name=*/"val", FieldType::INT, Literal(150));

    std::shared_ptr<arrow::ChunkedArray> result;
    ReadWithPredicateImpl(file_name, read_schema, predicate, &result);
    ASSERT_TRUE(result);
    ASSERT_EQ(50, result->length());

    // Verify values are 150-199
    auto flat = result->chunk(0);
    auto struct_arr = std::dynamic_pointer_cast<arrow::StructArray>(flat);
    ASSERT_TRUE(struct_arr);
    auto val_arr = std::dynamic_pointer_cast<arrow::Int32Array>(struct_arr->field(0));
    for (int32_t i = 0; i < 50; ++i) {
        ASSERT_EQ(150 + i, val_arr->Value(i));
    }
}

/// Test: multiple row groups, partial page match within a row group.
///
/// 200 rows, 10 rows per page, 100 rows per row group → 2 row groups.
/// Predicate: val >= 50 AND val < 150.
/// Row group 0 (rows 0-99): pages 0-4 skipped, pages 5-9 read → 50 rows
/// Row group 1 (rows 100-199): pages 0-4 read, pages 5-9 skipped → 50 rows
/// Total: 100 rows
TEST_F(PageFilteredRowGroupReaderTest, MultipleRowGroupsPartialPageMatch) {
    std::string file_name = dir_->Str() + "/multi_rg_partial.parquet";
    auto data = MakeSequentialIntData(200);
    WriteTestFile(file_name, data, /*write_batch_size=*/10, /*max_row_group_length=*/100);

    auto read_schema = arrow::schema({arrow::field("val", arrow::int32())});
    ASSERT_OK_AND_ASSIGN(
        auto predicate,
        PredicateBuilder::And(
            {PredicateBuilder::GreaterOrEqual(/*field_index=*/0, /*field_name=*/"val",
                                              FieldType::INT, Literal(50)),
             PredicateBuilder::LessThan(/*field_index=*/0, /*field_name=*/"val", FieldType::INT,
                                        Literal(150))}));

    std::shared_ptr<arrow::ChunkedArray> result;
    ReadWithPredicateImpl(file_name, read_schema, predicate, &result);
    ASSERT_TRUE(result);
    ASSERT_EQ(100, result->length());

    // Collect all values and verify they are 50-149
    int64_t offset = 0;
    for (int i = 0; i < result->num_chunks(); ++i) {
        auto struct_arr = std::dynamic_pointer_cast<arrow::StructArray>(result->chunk(i));
        ASSERT_TRUE(struct_arr);
        auto val_arr = std::dynamic_pointer_cast<arrow::Int32Array>(struct_arr->field(0));
        for (int64_t j = 0; j < val_arr->length(); ++j) {
            ASSERT_EQ(50 + offset, val_arr->Value(j)) << "Mismatch at offset " << offset;
            ++offset;
        }
    }
    ASSERT_EQ(100, offset);
}

/// Test: two columns remain aligned after page-level filtering.
///
/// 100 rows, a[i] = i, b[i] = i*10. 10 rows per page.
/// Predicate on "a": a >= 50. After filtering, b should be b[50..99] = {500, 510, ..., 990}.
TEST_F(PageFilteredRowGroupReaderTest, MultiColumnAlignment) {
    std::string file_name = dir_->Str() + "/multi_col.parquet";
    auto data = MakeTwoColumnData(100);
    WriteTestFile(file_name, data, /*write_batch_size=*/10, /*max_row_group_length=*/100);

    auto read_schema =
        arrow::schema({arrow::field("a", arrow::int32()), arrow::field("b", arrow::int32())});
    auto predicate = PredicateBuilder::GreaterOrEqual(
        /*field_index=*/0, /*field_name=*/"a", FieldType::INT, Literal(50));

    std::shared_ptr<arrow::ChunkedArray> result;
    ReadWithPredicateImpl(file_name, read_schema, predicate, &result);
    ASSERT_TRUE(result);
    ASSERT_EQ(50, result->length());

    auto struct_arr = std::dynamic_pointer_cast<arrow::StructArray>(result->chunk(0));
    ASSERT_TRUE(struct_arr);
    auto a_arr = std::dynamic_pointer_cast<arrow::Int32Array>(struct_arr->field(0));
    auto b_arr = std::dynamic_pointer_cast<arrow::Int32Array>(struct_arr->field(1));
    for (int32_t i = 0; i < 50; ++i) {
        ASSERT_EQ(50 + i, a_arr->Value(i));
        ASSERT_EQ((50 + i) * 10, b_arr->Value(i));
    }
}

/// Test: predicate matches pages in the middle of a row group.
///
/// 100 rows, 10 rows per page. Predicate: val >= 30 AND val < 70.
/// Pages 0-2 (rows 0-29) skipped, pages 3-6 (rows 30-69) read, pages 7-9 (rows 70-99) skipped.
TEST_F(PageFilteredRowGroupReaderTest, MiddlePagesMatch) {
    std::string file_name = dir_->Str() + "/middle_pages.parquet";
    auto data = MakeSequentialIntData(100);
    WriteTestFile(file_name, data, /*write_batch_size=*/10, /*max_row_group_length=*/100);

    auto read_schema = arrow::schema({arrow::field("val", arrow::int32())});
    ASSERT_OK_AND_ASSIGN(
        auto predicate,
        PredicateBuilder::And(
            {PredicateBuilder::GreaterOrEqual(/*field_index=*/0, /*field_name=*/"val",
                                              FieldType::INT, Literal(30)),
             PredicateBuilder::LessThan(/*field_index=*/0, /*field_name=*/"val", FieldType::INT,
                                        Literal(70))}));

    std::shared_ptr<arrow::ChunkedArray> result;
    ReadWithPredicateImpl(file_name, read_schema, predicate, &result);
    ASSERT_TRUE(result);
    ASSERT_EQ(40, result->length());

    int64_t offset = 0;
    for (int i = 0; i < result->num_chunks(); ++i) {
        auto struct_arr = std::dynamic_pointer_cast<arrow::StructArray>(result->chunk(i));
        auto val_arr = std::dynamic_pointer_cast<arrow::Int32Array>(struct_arr->field(0));
        for (int64_t j = 0; j < val_arr->length(); ++j) {
            ASSERT_EQ(30 + offset, val_arr->Value(j));
            ++offset;
        }
    }
    ASSERT_EQ(40, offset);
}

/// Test: no predicate → all data returned (no filtering).
TEST_F(PageFilteredRowGroupReaderTest, NoPredicate) {
    std::string file_name = dir_->Str() + "/no_predicate.parquet";
    auto data = MakeSequentialIntData(100);
    WriteTestFile(file_name, data, /*write_batch_size=*/10, /*max_row_group_length=*/100);

    auto read_schema = arrow::schema({arrow::field("val", arrow::int32())});

    std::shared_ptr<arrow::ChunkedArray> result;
    ReadWithPredicateImpl(file_name, read_schema, /*predicate=*/nullptr, &result);
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(100, result->length());
}

/// Test: page filtering with EQUAL predicate that matches a single page.
///
/// 100 rows, 10 rows per page. Predicate: val == 55.
/// Only page 5 (rows 50-59) should match, containing value 55.
TEST_F(PageFilteredRowGroupReaderTest, EqualPredicateSinglePageMatch) {
    std::string file_name = dir_->Str() + "/equal_single_page.parquet";
    auto data = MakeSequentialIntData(100);
    WriteTestFile(file_name, data, /*write_batch_size=*/10, /*max_row_group_length=*/100);

    auto read_schema = arrow::schema({arrow::field("val", arrow::int32())});
    auto predicate = PredicateBuilder::Equal(
        /*field_index=*/0, /*field_name=*/"val", FieldType::INT, Literal(55));

    std::shared_ptr<arrow::ChunkedArray> result;
    ReadWithPredicateImpl(file_name, read_schema, predicate, &result);
    ASSERT_TRUE(result);
    // Page 5 has rows 50-59, which includes 55. The entire page is returned.
    ASSERT_EQ(10, result->length());

    auto struct_arr = std::dynamic_pointer_cast<arrow::StructArray>(result->chunk(0));
    auto val_arr = std::dynamic_pointer_cast<arrow::Int32Array>(struct_arr->field(0));
    for (int32_t i = 0; i < 10; ++i) {
        ASSERT_EQ(50 + i, val_arr->Value(i));
    }
}

/// Test: page filtering with LessThan predicate.
///
/// 100 rows, 10 rows per page. Predicate: val < 25.
/// Pages 0-2 (rows 0-29) match (page 2 has min=20 < 25).
/// Pages 3-9 don't match.
TEST_F(PageFilteredRowGroupReaderTest, LessThanPredicatePageMatch) {
    std::string file_name = dir_->Str() + "/less_than.parquet";
    auto data = MakeSequentialIntData(100);
    WriteTestFile(file_name, data, /*write_batch_size=*/10, /*max_row_group_length=*/100);

    auto read_schema = arrow::schema({arrow::field("val", arrow::int32())});
    auto predicate = PredicateBuilder::LessThan(
        /*field_index=*/0, /*field_name=*/"val", FieldType::INT, Literal(25));

    std::shared_ptr<arrow::ChunkedArray> result;
    ReadWithPredicateImpl(file_name, read_schema, predicate, &result);
    ASSERT_TRUE(result);
    // Pages 0 (0-9), 1 (10-19), 2 (20-29) match because their min < 25.
    // Page 2 has min=20, max=29, and 20 < 25, so it matches.
    ASSERT_EQ(30, result->length());

    auto struct_arr = std::dynamic_pointer_cast<arrow::StructArray>(result->chunk(0));
    auto val_arr = std::dynamic_pointer_cast<arrow::Int32Array>(struct_arr->field(0));
    for (int32_t i = 0; i < 30; ++i) {
        ASSERT_EQ(i, val_arr->Value(i));
    }
}

/// Test: large data with multiple row groups and page filtering.
///
/// 1000 rows, 10 rows per page, 200 rows per row group → 5 row groups.
/// Predicate: val >= 500 AND val < 700.
/// Row groups 0,1 (rows 0-399): all pages eliminated
/// Row group 2 (rows 400-599): pages 0-9 (400-499) eliminated, pages 10-19 (500-599) read
/// Row group 3 (rows 600-799): pages 0-9 (600-699) read, pages 10-19 (700-799) eliminated
/// Row group 4 (rows 800-999): all pages eliminated
/// Total: 200 rows (500-699)
TEST_F(PageFilteredRowGroupReaderTest, LargeDataMultiRowGroupPageFilter) {
    std::string file_name = dir_->Str() + "/large_data.parquet";
    auto data = MakeSequentialIntData(1000);
    WriteTestFile(file_name, data, /*write_batch_size=*/10, /*max_row_group_length=*/200);

    auto read_schema = arrow::schema({arrow::field("val", arrow::int32())});
    ASSERT_OK_AND_ASSIGN(
        auto predicate,
        PredicateBuilder::And(
            {PredicateBuilder::GreaterOrEqual(/*field_index=*/0, /*field_name=*/"val",
                                              FieldType::INT, Literal(500)),
             PredicateBuilder::LessThan(/*field_index=*/0, /*field_name=*/"val", FieldType::INT,
                                        Literal(700))}));

    std::shared_ptr<arrow::ChunkedArray> result;
    ReadWithPredicateImpl(file_name, read_schema, predicate, &result);
    ASSERT_TRUE(result);
    ASSERT_EQ(200, result->length());

    // Verify values are 500-699
    int64_t offset = 0;
    for (int i = 0; i < result->num_chunks(); ++i) {
        auto struct_arr = std::dynamic_pointer_cast<arrow::StructArray>(result->chunk(i));
        auto val_arr = std::dynamic_pointer_cast<arrow::Int32Array>(struct_arr->field(0));
        for (int64_t j = 0; j < val_arr->length(); ++j) {
            ASSERT_EQ(500 + offset, val_arr->Value(j)) << "Mismatch at offset " << offset;
            ++offset;
        }
    }
    ASSERT_EQ(200, offset);
}

/// Test: string column page filtering.
///
/// Write 40 rows with string values: "aaa_00", "aaa_01", ..., "aaa_09",
/// "bbb_10", ..., "bbb_19", "ccc_20", ..., "ccc_29", "ddd_30", ..., "ddd_39".
/// 10 rows per page → 4 pages. Predicate: val >= "ccc" should match pages 2-3.
TEST_F(PageFilteredRowGroupReaderTest, StringColumnPageFilter) {
    std::string file_name = dir_->Str() + "/string_filter.parquet";

    arrow::StringBuilder str_builder;
    ASSERT_TRUE(str_builder.Reserve(40).ok());
    std::vector<std::string> prefixes = {"aaa", "bbb", "ccc", "ddd"};
    for (int32_t i = 0; i < 40; ++i) {
        std::string val = prefixes[i / 10] + "_" + (i < 10 ? "0" : "") + std::to_string(i);
        ASSERT_TRUE(str_builder.Append(val).ok());
    }
    auto str_array = str_builder.Finish().ValueOrDie();
    auto field = arrow::field("val", arrow::utf8());
    auto struct_arr = arrow::StructArray::Make({str_array}, {field}).ValueOrDie();

    WriteTestFile(file_name, struct_arr, /*write_batch_size=*/10, /*max_row_group_length=*/40);

    auto read_schema = arrow::schema({field});
    auto predicate = PredicateBuilder::GreaterOrEqual(
        /*field_index=*/0, /*field_name=*/"val", FieldType::STRING,
        Literal(FieldType::STRING, "ccc", 3));

    std::shared_ptr<arrow::ChunkedArray> result;
    ReadWithPredicateImpl(file_name, read_schema, predicate, &result);
    ASSERT_TRUE(result);
    // Pages 2 (ccc_20..ccc_29) and 3 (ddd_30..ddd_39) should match.
    ASSERT_EQ(20, result->length());
}

/// Test: ComputePageRanges returns only matching page byte ranges.
///
/// 100 rows, 10 rows per page, 1 row group with page index enabled.
/// RowRanges = [50, 59] (page 5 only). Should return exactly 1 page range per column.
TEST_F(PageFilteredRowGroupReaderTest, ComputePageRangesPartialMatch) {
    std::string file_name = dir_->Str() + "/compute_ranges_partial.parquet";
    auto data = MakeSequentialIntData(100);
    WriteTestFile(file_name, data, /*write_batch_size=*/10, /*max_row_group_length=*/100);

    // Open as raw ParquetFileReader
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_name));
    ASSERT_OK_AND_ASSIGN(int64_t length, in->Length());
    auto in_stream = std::make_shared<ArrowInputStreamAdapter>(in, arrow_pool_, length);
    auto parquet_reader = ::parquet::ParquetFileReader::Open(in_stream);
    ASSERT_TRUE(parquet_reader);

    // Single page match: rows [50, 59] = page 5
    RowRanges row_ranges;
    row_ranges.Add(RowRanges::Range(50, 59));

    auto ranges = PageFilteredRowGroupReader::ComputePageRanges(
        parquet_reader.get(), /*row_group_index=*/0, row_ranges, /*column_indices=*/{0});

    // Should have exactly 1 range (page 5 of column 0, no dictionary since disabled)
    ASSERT_EQ(1, ranges.size());
    ASSERT_GT(ranges[0].offset, 0);
    ASSERT_GT(ranges[0].length, 0);
}

/// Test: ComputePageRanges returns all page ranges when RowRanges covers entire row group.
TEST_F(PageFilteredRowGroupReaderTest, ComputePageRangesAllMatch) {
    std::string file_name = dir_->Str() + "/compute_ranges_all.parquet";
    auto data = MakeSequentialIntData(100);
    WriteTestFile(file_name, data, /*write_batch_size=*/10, /*max_row_group_length=*/100);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_name));
    ASSERT_OK_AND_ASSIGN(int64_t length, in->Length());
    auto in_stream = std::make_shared<ArrowInputStreamAdapter>(in, arrow_pool_, length);
    auto parquet_reader = ::parquet::ParquetFileReader::Open(in_stream);

    // All rows match
    RowRanges row_ranges;
    row_ranges.Add(RowRanges::Range(0, 99));

    auto ranges =
        PageFilteredRowGroupReader::ComputePageRanges(parquet_reader.get(), 0, row_ranges, {0});

    // 10 pages, all matching
    ASSERT_EQ(10, ranges.size());
    for (const auto& r : ranges) {
        ASSERT_GT(r.offset, 0);
        ASSERT_GT(r.length, 0);
    }
}

/// Test: ComputePageRanges returns no page ranges for empty RowRanges.
TEST_F(PageFilteredRowGroupReaderTest, ComputePageRangesNoMatch) {
    std::string file_name = dir_->Str() + "/compute_ranges_none.parquet";
    auto data = MakeSequentialIntData(100);
    WriteTestFile(file_name, data, /*write_batch_size=*/10, /*max_row_group_length=*/100);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_name));
    ASSERT_OK_AND_ASSIGN(int64_t length, in->Length());
    auto in_stream = std::make_shared<ArrowInputStreamAdapter>(in, arrow_pool_, length);
    auto parquet_reader = ::parquet::ParquetFileReader::Open(in_stream);

    RowRanges row_ranges;  // empty

    auto ranges =
        PageFilteredRowGroupReader::ComputePageRanges(parquet_reader.get(), 0, row_ranges, {0});

    ASSERT_EQ(0, ranges.size());
}

/// Test: ComputePageRanges with multiple columns returns ranges for each column.
TEST_F(PageFilteredRowGroupReaderTest, ComputePageRangesMultiColumn) {
    std::string file_name = dir_->Str() + "/compute_ranges_multi_col.parquet";
    auto data = MakeTwoColumnData(100);
    WriteTestFile(file_name, data, /*write_batch_size=*/10, /*max_row_group_length=*/100);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_name));
    ASSERT_OK_AND_ASSIGN(int64_t length, in->Length());
    auto in_stream = std::make_shared<ArrowInputStreamAdapter>(in, arrow_pool_, length);
    auto parquet_reader = ::parquet::ParquetFileReader::Open(in_stream);

    // Match page 5 only (rows 50-59)
    RowRanges row_ranges;
    row_ranges.Add(RowRanges::Range(50, 59));

    auto ranges =
        PageFilteredRowGroupReader::ComputePageRanges(parquet_reader.get(), 0, row_ranges, {0, 1});

    // 1 matching page per column = 2 ranges total
    ASSERT_EQ(2, ranges.size());
    // Ranges should be at different offsets (different columns)
    ASSERT_NE(ranges[0].offset, ranges[1].offset);
}

/// Test: ComputePageRanges with multiple matching pages.
///
/// 100 rows, 10 per page. RowRanges = [20,29] + [70,79] = pages 2 and 7.
TEST_F(PageFilteredRowGroupReaderTest, ComputePageRangesMultiplePages) {
    std::string file_name = dir_->Str() + "/compute_ranges_multi_page.parquet";
    auto data = MakeSequentialIntData(100);
    WriteTestFile(file_name, data, /*write_batch_size=*/10, /*max_row_group_length=*/100);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_name));
    ASSERT_OK_AND_ASSIGN(int64_t length, in->Length());
    auto in_stream = std::make_shared<ArrowInputStreamAdapter>(in, arrow_pool_, length);
    auto parquet_reader = ::parquet::ParquetFileReader::Open(in_stream);

    RowRanges row_ranges;
    row_ranges.Add(RowRanges::Range(20, 29));
    row_ranges.Add(RowRanges::Range(70, 79));

    auto ranges =
        PageFilteredRowGroupReader::ComputePageRanges(parquet_reader.get(), 0, row_ranges, {0});

    // 2 matching pages for 1 column
    ASSERT_EQ(2, ranges.size());
    // Pages should be at increasing offsets
    ASSERT_LT(ranges[0].offset, ranges[1].offset);
}

/// Test: variable-length columns are streamed across multiple zero-copy-sliced
/// RecordBatches when batch_size is smaller than the matched row count, instead of
/// being concatenated into a single RecordBatch via CombineChunks.
///
/// This verifies the alignment with Arrow's standard TableBatchReader path:
/// multi-chunk binary/string columns split along chunk + batch_size boundaries,
/// with no deep copy. Asserts both correctness (total rows + full content order) and
/// the multi-batch shape (more than one chunk in the collected ChunkedArray).
TEST_F(PageFilteredRowGroupReaderTest, StringColumnMultiBatchStreaming) {
    std::string file_name = dir_->Str() + "/string_multi_batch.parquet";

    arrow::StringBuilder str_builder;
    ASSERT_TRUE(str_builder.Reserve(60).ok());
    // 6 pages of 10 rows each: prefix "p0_".."p5_" so each page has a distinct min/max.
    for (int32_t i = 0; i < 60; ++i) {
        std::string val =
            "p" + std::to_string(i / 10) + "_" + (i < 10 ? "0" : "") + std::to_string(i);
        ASSERT_TRUE(str_builder.Append(val).ok());
    }
    auto str_array = str_builder.Finish().ValueOrDie();
    auto field = arrow::field("val", arrow::utf8());
    auto struct_arr = arrow::StructArray::Make({str_array}, {field}).ValueOrDie();

    WriteTestFile(file_name, struct_arr, /*write_batch_size=*/10, /*max_row_group_length=*/60);

    // Predicate matches pages 2..5 (40 rows: "p2_20".."p5_59"). batch_size=7 forces
    // the wrapper to surface multiple batches per page-filtered RG.
    auto read_schema = arrow::schema({field});
    auto predicate = PredicateBuilder::GreaterOrEqual(
        /*field_index=*/0, /*field_name=*/"val", FieldType::STRING,
        Literal(FieldType::STRING, "p2", 2));

    std::shared_ptr<arrow::ChunkedArray> result;
    ReadWithPredicateImpl(file_name, read_schema, predicate, &result, /*batch_size=*/7);
    ASSERT_TRUE(result);
    ASSERT_EQ(40, result->length());

    // Multi-batch shape: with 40 matched rows and batch_size=7 we expect at least
    // ceil(40/7)=6 chunks. Anything > 1 already proves we did not collapse to a single
    // post-CombineChunks RecordBatch.
    ASSERT_GT(result->num_chunks(), 1);

    // Content correctness: rows arrive in the original page order, "p2_20" through "p5_59".
    int64_t seen = 0;
    for (int i = 0; i < result->num_chunks(); ++i) {
        auto struct_chunk = std::dynamic_pointer_cast<arrow::StructArray>(result->chunk(i));
        ASSERT_TRUE(struct_chunk);
        auto str_chunk = std::dynamic_pointer_cast<arrow::StringArray>(struct_chunk->field(0));
        ASSERT_TRUE(str_chunk);
        for (int64_t j = 0; j < str_chunk->length(); ++j) {
            int32_t row = 20 + static_cast<int32_t>(seen);
            std::string expected =
                "p" + std::to_string(row / 10) + "_" + (row < 10 ? "0" : "") + std::to_string(row);
            ASSERT_EQ(expected, str_chunk->GetString(j));
            ++seen;
        }
    }
    ASSERT_EQ(40, seen);
}

/// Test: end-to-end page-filtered read produces correct results when using page-level PreBuffer.
///
/// This exercises the full path: ComputePageRanges → PreBufferRanges → CachedInputStream →
/// ReadFilteredRowGroup with page_ranges.
TEST_F(PageFilteredRowGroupReaderTest, EndToEndPageLevelPreBuffer) {
    std::string file_name = dir_->Str() + "/e2e_page_prebuffer.parquet";
    auto data = MakeSequentialIntData(100);
    WriteTestFile(file_name, data, /*write_batch_size=*/10, /*max_row_group_length=*/100);

    // Read via the standard ParquetFileBatchReader path (page index enabled)
    auto read_schema = arrow::schema({arrow::field("val", arrow::int32())});
    auto predicate = PredicateBuilder::Equal(
        /*field_index=*/0, /*field_name=*/"val", FieldType::INT, Literal(55));

    // Use small batch_size to verify batched consumption of page-filtered results
    std::shared_ptr<arrow::ChunkedArray> result;
    ReadWithPredicateImpl(file_name, read_schema, predicate, &result, /*batch_size=*/3);
    ASSERT_TRUE(result);
    // Page 5 (rows 50-59) matches, should return 10 rows
    ASSERT_EQ(10, result->length());

    // Verify actual values across chunks
    int64_t offset = 0;
    for (int i = 0; i < result->num_chunks(); ++i) {
        auto struct_arr = std::dynamic_pointer_cast<arrow::StructArray>(result->chunk(i));
        ASSERT_TRUE(struct_arr);
        auto val_arr = std::dynamic_pointer_cast<arrow::Int32Array>(struct_arr->field(0));
        for (int64_t j = 0; j < val_arr->length(); ++j) {
            ASSERT_EQ(50 + offset, val_arr->Value(j));
            ++offset;
        }
    }
    ASSERT_EQ(10, offset);
}

/// Test: ComputePageRanges with dictionary encoding produces correct chunk_end.
///
/// When dictionary encoding is enabled, the column chunk layout is:
///   [Dictionary Page] [Data Page 0] [Data Page 1] ... [Data Page N]
/// And total_compressed_size covers the entire chunk starting from dictionary_page_offset.
///
/// The bug: chunk_end = data_page_offset + total_compressed_size is wrong because
/// total_compressed_size already includes the dictionary page size. The correct
/// chunk_end should be dictionary_page_offset + total_compressed_size.
///
/// This test verifies that:
/// 1. No range exceeds the true chunk boundary (overshoot regression).
/// 2. At least one non-dictionary data-page range is present (not truncated).
/// 3. The maximum range_end equals true_chunk_end when requesting all rows.
/// 4. End-to-end reads with page-level filtering return correct query results.
TEST_F(PageFilteredRowGroupReaderTest, ComputePageRangesWithDictionaryEncoding) {
    std::string file_name = dir_->Str() + "/compute_ranges_dict.parquet";

    // Use low-cardinality data to ensure dictionary encoding is actually used.
    // 100 rows with values cycling through 0..9 → dictionary will have 10 entries.
    arrow::Int32Builder val_builder;
    ASSERT_TRUE(val_builder.Reserve(100).ok());
    for (int32_t i = 0; i < 100; ++i) {
        val_builder.UnsafeAppend(i % 10);
    }
    auto val_array = val_builder.Finish().ValueOrDie();
    auto field = arrow::field("val", arrow::int32());
    auto struct_array = arrow::StructArray::Make({val_array}, {field}).ValueOrDie();

    // Write with dictionary encoding enabled and 1 row per page.
    // Each page has min==max==val for that row, enabling precise page-level skipping.
    WriteTestFile(file_name, struct_array, /*write_batch_size=*/1,
                  /*max_row_group_length=*/100, /*enable_dictionary=*/true);

    // Open the file and verify metadata confirms dictionary page presence
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_name));
    ASSERT_OK_AND_ASSIGN(uint64_t length, in->Length());
    auto in_stream = std::make_shared<ArrowInputStreamAdapter>(in, arrow_pool_, length);
    auto parquet_reader = ::parquet::ParquetFileReader::Open(in_stream);
    ASSERT_TRUE(parquet_reader);

    auto file_metadata = parquet_reader->metadata();
    auto rg_metadata = file_metadata->RowGroup(0);
    auto col_chunk = rg_metadata->ColumnChunk(0);

    // Precondition: dictionary page must exist for this test to be meaningful
    ASSERT_TRUE(col_chunk->has_dictionary_page());

    int64_t dict_offset = col_chunk->dictionary_page_offset();
    int64_t data_page_offset = col_chunk->data_page_offset();
    int64_t total_compressed_size = col_chunk->total_compressed_size();

    // The true chunk end is dict_offset + total_compressed_size
    int64_t true_chunk_end = dict_offset + total_compressed_size;
    // The buggy chunk end would be data_page_offset + total_compressed_size
    int64_t buggy_chunk_end = data_page_offset + total_compressed_size;

    // Sanity: dict page is before data pages, so buggy end > true end
    ASSERT_LT(dict_offset, data_page_offset);
    ASSERT_GT(buggy_chunk_end, true_chunk_end);
    // Now call ComputePageRanges with all rows matching
    RowRanges row_ranges;
    row_ranges.Add(RowRanges::Range(0, 99));

    auto ranges = PageFilteredRowGroupReader::ComputePageRanges(
        parquet_reader.get(), /*row_group_index=*/0, row_ranges, /*column_indices=*/{0});

    ASSERT_FALSE(ranges.empty());

    // --- Check 1: No range should extend beyond the true chunk end ---
    // With the bug, the last data page's range would use chunk_end = data_page_offset +
    // total_compressed_size, which overshoots by the dictionary page size.
    for (auto& range : ranges) {
        int64_t range_end = range.offset + range.length;
        ASSERT_LE(range_end, true_chunk_end);
    }

    // --- Check 2: At least one non-dictionary data-page range is present ---
    // Guards against truncation: if only the dictionary range is returned, the test
    // would still pass the overshoot check but miss that data pages are lost.
    int data_page_range_count = 0;
    for (const auto& range : ranges) {
        if (range.offset >= data_page_offset) {
            ++data_page_range_count;
        }
    }
    ASSERT_GE(data_page_range_count, 1);

    // --- Check 3: Maximum range_end equals true_chunk_end when requesting all rows ---
    int64_t max_range_end = 0;
    for (const auto& range : ranges) {
        int64_t range_end = range.offset + range.length;
        max_range_end = std::max(max_range_end, range_end);
    }
    ASSERT_EQ(max_range_end, true_chunk_end);

    // --- Check 4: No range exceeds file size ---
    for (const auto& range : ranges) {
        ASSERT_LE(range.offset + range.length, static_cast<int64_t>(length));
    }

    // --- End-to-end check 1: read all rows (no predicate filtering) ---
    // Verifies that reading a dictionary-encoded file with page index enabled
    // returns all 100 rows with correct values.
    auto read_schema = arrow::schema({field});
    auto predicate_all = PredicateBuilder::GreaterOrEqual(
        /*field_index=*/0, /*field_name=*/"val", FieldType::INT, Literal(0));
    std::shared_ptr<arrow::ChunkedArray> result_all;
    ReadWithPredicateImpl(file_name, read_schema, predicate_all, &result_all);
    ASSERT_TRUE(result_all);
    ASSERT_EQ(100, result_all->length());

    // --- End-to-end check 2: full range query with page level skipping ---
    // Build expected array: val = i % 10 for i in [0, 100), wrapped in a struct.
    // Concatenate all chunks and compare with Equals
    auto actual_struct_arr = arrow::Concatenate(result_all->chunks()).ValueOrDie();
    ASSERT_TRUE(actual_struct_arr->Equals(struct_array));

    // --- End-to-end check 3: partial-row query with page-level skipping ---
    // Predicate val >= 7 skips pages where val < 7, keeping only val in {7,8,9}.
    // Out of 100 rows, 30 rows satisfy val >= 7 (3 per cycle × 10 cycles).
    auto predicate_partial = PredicateBuilder::GreaterOrEqual(
        /*field_index=*/0, /*field_name=*/"val", FieldType::INT, Literal(7));
    std::shared_ptr<arrow::ChunkedArray> result_partial;
    ReadWithPredicateImpl(file_name, read_schema, predicate_partial, &result_partial);
    ASSERT_TRUE(result_partial);

    // Build expected StructArray and compare with Equals
    arrow::Int32Builder expected_builder;
    ASSERT_TRUE(expected_builder.Reserve(30).ok());
    for (int32_t i = 0; i < 100; ++i) {
        if (i % 10 >= 7) {
            expected_builder.UnsafeAppend(i % 10);
        }
    }
    auto expected_array = expected_builder.Finish().ValueOrDie();
    auto expected_struct = arrow::StructArray::Make({expected_array}, {field}).ValueOrDie();
    auto partial_concat = arrow::Concatenate(result_partial->chunks()).ValueOrDie();
    ASSERT_TRUE(partial_concat->Equals(expected_struct));
}

}  // namespace paimon::parquet::test
