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

#include "paimon/format/parquet/file_reader_wrapper.h"

#include <map>
#include <string>

#include "arrow/api.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/io/caching.h"
#include "arrow/memory_pool.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/format/parquet/parquet_field_id_converter.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/format/parquet/parquet_format_writer.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/testing/utils/testharness.h"
#include "parquet/arrow/reader.h"
#include "parquet/properties.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace paimon::parquet::test {

class FileReaderWrapperTest : public ::testing::Test {
 public:
    void SetUp() override {
        dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        fs_ = std::make_shared<LocalFileSystem>();
        pool_ = GetDefaultPool();
        arrow_pool_ = GetArrowPool(pool_);
        batch_size_ = 512;
    }
    void TearDown() override {}

    std::pair<std::shared_ptr<arrow::Schema>, std::shared_ptr<arrow::DataType>> PrepareArrowSchema()
        const {
        auto string_field = arrow::field(
            "col1", arrow::utf8(),
            arrow::KeyValueMetadata::Make({ParquetFieldIdConverter::PARQUET_FIELD_ID}, {"0"}));
        auto int_field = arrow::field(
            "col2", arrow::int32(),
            arrow::KeyValueMetadata::Make({ParquetFieldIdConverter::PARQUET_FIELD_ID}, {"1"}));
        auto bool_field = arrow::field(
            "col3", arrow::boolean(),
            arrow::KeyValueMetadata::Make({ParquetFieldIdConverter::PARQUET_FIELD_ID}, {"2"}));
        auto struct_type = arrow::struct_({string_field, int_field, bool_field});
        return std::make_pair(
            arrow::schema(arrow::FieldVector({string_field, int_field, bool_field})), struct_type);
    }

    std::shared_ptr<arrow::Array> PrepareArray(const std::shared_ptr<arrow::DataType>& data_type,
                                               int32_t record_batch_size,
                                               int32_t offset = 0) const {
        arrow::StructBuilder struct_builder(
            data_type, arrow::default_memory_pool(),
            {std::make_shared<arrow::StringBuilder>(), std::make_shared<arrow::Int32Builder>(),
             std::make_shared<arrow::BooleanBuilder>()});
        auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
        auto int_builder = static_cast<arrow::Int32Builder*>(struct_builder.field_builder(1));
        auto bool_builder = static_cast<arrow::BooleanBuilder*>(struct_builder.field_builder(2));
        for (int32_t i = 0 + offset; i < record_batch_size + offset; ++i) {
            EXPECT_TRUE(struct_builder.Append().ok());
            EXPECT_TRUE(string_builder->Append("str_" + std::to_string(i)).ok());
            if (i % 3 == 0) {
                // test null
                EXPECT_TRUE(int_builder->AppendNull().ok());
            } else {
                EXPECT_TRUE(int_builder->Append(i).ok());
            }
            EXPECT_TRUE(bool_builder->Append(static_cast<bool>(i % 2)).ok());
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(struct_builder.Finish(&array).ok());
        return array;
    }

    void AddRecordBatchOnce(const std::shared_ptr<ParquetFormatWriter>& format_writer,
                            const std::shared_ptr<arrow::DataType>& struct_type,
                            int32_t record_batch_size, int32_t offset) const {
        auto array = PrepareArray(struct_type, record_batch_size, offset);
        auto arrow_array = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*array, arrow_array.get()).ok());
        auto batch = std::make_shared<RecordBatch>(
            /*partition=*/std::map<std::string, std::string>(), /*bucket=*/-1,
            /*row_kinds=*/std::vector<RecordBatch::RowKind>(), arrow_array.get());
        ASSERT_OK(format_writer->AddBatch(batch->GetData()));
    }

    Result<std::unique_ptr<FileReaderWrapper>> PrepareReaderWrapper(
        const std::string& file_path, int64_t wrapper_batch_size = 0) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> in, fs_->Open(file_path));
        PAIMON_ASSIGN_OR_RAISE(int64_t file_length, in->Length());
        auto input_stream = std::make_unique<ArrowInputStreamAdapter>(in, arrow_pool_, file_length);
        ::parquet::arrow::FileReaderBuilder file_reader_builder;
        ::parquet::ReaderProperties reader_properties;
        reader_properties.enable_buffered_stream();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            file_reader_builder.Open(std::move(input_stream), reader_properties));

        ::parquet::ArrowReaderProperties arrow_reader_props;
        arrow_reader_props.set_pre_buffer(true);
        arrow_reader_props.set_batch_size(static_cast<int64_t>(batch_size_));
        arrow_reader_props.set_use_threads(true);
        arrow_reader_props.set_cache_options(arrow::io::CacheOptions::Defaults());
        std::unique_ptr<::parquet::arrow::FileReader> file_reader;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(file_reader_builder.memory_pool(arrow_pool_.get())
                                            ->properties(arrow_reader_props)
                                            ->Build(&file_reader));
        return FileReaderWrapper::Create(std::move(file_reader), wrapper_batch_size, arrow_pool_);
    }

    void PrepareParquetFile(const std::string& file_path, int32_t row_count,
                            bool enable_page_index = false, int32_t write_batch_size = 10) {
        auto schema_pair = PrepareArrowSchema();
        const auto& arrow_schema = schema_pair.first;
        const auto& struct_type = schema_pair.second;

        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                             fs_->Create(file_path, /*overwrite=*/false));
        ::parquet::WriterProperties::Builder builder;
        builder.write_batch_size(write_batch_size);
        builder.max_row_group_length(1000);
        builder.enable_store_decimal_as_integer();
        if (enable_page_index) {
            builder.enable_write_page_index();
            builder.disable_dictionary();
            builder.data_pagesize(1);
        }
        auto writer_properties = builder.build();
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<ParquetFormatWriter> format_writer,
            ParquetFormatWriter::Create(out, arrow_schema, writer_properties,
                                        DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE, arrow_pool_));

        AddRecordBatchOnce(format_writer, struct_type, /*record_batch_size=*/row_count,
                           /*offset=*/0);
        ASSERT_OK(format_writer->Flush());
        ASSERT_OK(format_writer->Finish());
        ASSERT_OK(out->Flush());
        ASSERT_OK(out->Close());
    }

 private:
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    int32_t batch_size_;
};

TEST_F(FileReaderWrapperTest, EmptyFile) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "test.parquet");
    PrepareParquetFile(file_path, /*row_count=*/0);
    ASSERT_OK_AND_ASSIGN(auto reader_wrapper, PrepareReaderWrapper(file_path));
    ASSERT_EQ(0, reader_wrapper->GetNumberOfRows());
    ASSERT_EQ(0, reader_wrapper->GetNumberOfRowGroups());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
              reader_wrapper->GetPreviousBatchFirstRowNumber().value());
    ASSERT_OK_AND_ASSIGN(auto batch, reader_wrapper->Next());
    ASSERT_EQ(0, reader_wrapper->GetPreviousBatchFirstRowNumber().value());
    ASSERT_EQ(0, reader_wrapper->GetNextRowToRead());
    ASSERT_TRUE(reader_wrapper->GetAllRowGroupRanges().empty());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::RecordBatch> record_batch, reader_wrapper->Next());
    ASSERT_FALSE(record_batch);
}

TEST_F(FileReaderWrapperTest, NullFileReader) {
    ASSERT_NOK_WITH_MSG(FileReaderWrapper::Create(nullptr,
                                                  /*batch_size=*/0,
                                                  /*pool=*/arrow_pool_),
                        "file reader wrapper create failed. file reader is nullptr");
}

TEST_F(FileReaderWrapperTest, Simple) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "test.parquet");
    PrepareParquetFile(file_path, /*row_count=*/5500);
    ASSERT_OK_AND_ASSIGN(auto reader_wrapper, PrepareReaderWrapper(file_path));
    ASSERT_EQ(5500, reader_wrapper->GetNumberOfRows());
    ASSERT_EQ(6, reader_wrapper->GetNumberOfRowGroups());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
              reader_wrapper->GetPreviousBatchFirstRowNumber().value());
    std::vector<std::pair<uint64_t, uint64_t>> expected_all_row_group_ranges = {
        {0, 1000}, {1000, 2000}, {2000, 3000}, {3000, 4000}, {4000, 5000}, {5000, 5500}};
    ASSERT_EQ(expected_all_row_group_ranges, reader_wrapper->GetAllRowGroupRanges());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::RecordBatch> record_batch, reader_wrapper->Next());
    ASSERT_TRUE(record_batch);
    ASSERT_EQ(512, record_batch->num_rows());
    ASSERT_EQ(512, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(0, reader_wrapper->GetPreviousBatchFirstRowNumber().value());
    ASSERT_OK_AND_ASSIGN(record_batch, reader_wrapper->Next());
    ASSERT_TRUE(record_batch);
    ASSERT_EQ(488, record_batch->num_rows());
    ASSERT_EQ(1000, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(512, reader_wrapper->GetPreviousBatchFirstRowNumber().value());
    ASSERT_OK_AND_ASSIGN(record_batch, reader_wrapper->Next());
    ASSERT_TRUE(record_batch);
    ASSERT_EQ(512, record_batch->num_rows());
    ASSERT_EQ(1512, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(1000, reader_wrapper->GetPreviousBatchFirstRowNumber().value());
    ASSERT_NOK_WITH_MSG(reader_wrapper->SeekToRow(1001),
                        "should not be in the middle of readable range");
    ASSERT_OK(reader_wrapper->SeekToRow(1000));
    ASSERT_OK_AND_ASSIGN(record_batch, reader_wrapper->Next());
    ASSERT_TRUE(record_batch);
    ASSERT_EQ(512, record_batch->num_rows());
    ASSERT_EQ(1512, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(1000, reader_wrapper->GetPreviousBatchFirstRowNumber().value());

    ASSERT_OK(reader_wrapper->SeekToRow(5600));
    ASSERT_EQ(5500, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(6, reader_wrapper->current_row_group_idx_);
    ASSERT_EQ(1000, reader_wrapper->GetPreviousBatchFirstRowNumber().value());
    ASSERT_OK_AND_ASSIGN(record_batch, reader_wrapper->Next());
    ASSERT_FALSE(record_batch);
    ASSERT_EQ(5500, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(5500, reader_wrapper->GetPreviousBatchFirstRowNumber().value());
}

/// Regression: when batch_size_ is 0 (the default) and a row group is consumed via
/// the page-filtered streaming path, we must not pass 0 to TableBatchReader::set_chunksize
/// — that would make ReadNext spin forever on zero-row batches. The wrapper now
/// translates 0 to int64_max so the reader produces one batch covering all matched rows.
TEST_F(FileReaderWrapperTest, PageFilteredZeroBatchSizeDoesNotHang) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "page_zero_batch.parquet");
    PrepareParquetFile(file_path, /*row_count=*/200, /*enable_page_index=*/true);
    ASSERT_OK_AND_ASSIGN(auto reader_wrapper, PrepareReaderWrapper(file_path));
    ASSERT_EQ(1, reader_wrapper->GetNumberOfRowGroups());

    // Inject a per-RG RowRanges to drive the page-filtered streaming path. Two non-
    // contiguous ranges keep the test honest about RowRanges semantics; the actual
    // numbers don't matter as long as their total falls inside the row group.
    RowRanges rr({RowRanges::Range(0, 49), RowRanges::Range(100, 149)});

    std::vector<int32_t> all_columns = {0, 1, 2};
    ASSERT_OK(reader_wrapper->PrepareForReading(
        {TargetRowGroup(/*rg_index=*/0, /*is_partially_matched=*/true, /*ranges=*/rr)},
        all_columns));
    int64_t total = 0;
    int64_t batch_count = 0;
    while (true) {
        ASSERT_OK_AND_ASSIGN(auto batch, reader_wrapper->Next());
        if (!batch) break;
        total += batch->num_rows();
        ++batch_count;
        ASSERT_LT(batch_count, 1000) << "Next() did not converge — likely an infinite loop";
    }
    ASSERT_EQ(100, total);
    ASSERT_GE(batch_count, 1);
}

/// SeekToRow back to a previously-consumed page-filtered row group must rebuild the
/// per-RG streaming reader from row_group_row_ranges_ and re-yield the same rows.
/// The page-filter path holds no per-RG cache that consumption could destroy; the
/// reader is constructed on demand each time, mirroring Arrow's stateless
/// GetRecordBatchReader for the fully-matched path.
TEST_F(FileReaderWrapperTest, SeekBackToConsumedPageFilteredRowGroup) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "seek_back.parquet");
    // 2000 rows produces 2 row groups (max_row_group_length=1000) with page index enabled.
    PrepareParquetFile(file_path, /*row_count=*/2000, /*enable_page_index=*/true);
    ASSERT_OK_AND_ASSIGN(auto reader_wrapper, PrepareReaderWrapper(file_path));
    ASSERT_EQ(2, reader_wrapper->GetNumberOfRowGroups());

    // Both RGs page-filtered. RowRanges are RG-local: RG0 keeps 40 rows, RG1 keeps 50.
    std::map<int32_t, RowRanges> row_ranges_map;
    row_ranges_map[0] = RowRanges(RowRanges::Range(10, 49));
    row_ranges_map[1] = RowRanges(RowRanges::Range(100, 149));

    std::vector<int32_t> all_columns = {0, 1, 2};
    ASSERT_OK(reader_wrapper->PrepareForReading(
        {TargetRowGroup(/*rg_index=*/0, /*is_partially_matched=*/true,
                        /*ranges=*/row_ranges_map[0]),
         TargetRowGroup(/*rg_index=*/1, /*is_partially_matched=*/true,
                        /*ranges=*/row_ranges_map[1])},
        all_columns));

    auto count_all_rows = [&](int64_t* out_total) {
        int64_t total = 0;
        while (true) {
            auto next = reader_wrapper->Next();
            if (!next.ok()) return next.status();
            auto batch = std::move(next).value();
            if (!batch) break;
            total += batch->num_rows();
        }
        *out_total = total;
        return Status::OK();
    };

    int64_t first_total = 0;
    ASSERT_OK(count_all_rows(&first_total));
    ASSERT_EQ(90, first_total);  // 40 + 50

    // Seek back to row 0 (start of RG0). The on-demand reader construction means RG0
    // is read again from scratch, producing the same 90 rows total.
    ASSERT_OK(reader_wrapper->SeekToRow(0));

    int64_t second_total = 0;
    ASSERT_OK(count_all_rows(&second_total));
    ASSERT_EQ(90, second_total);
}

/// When the page-level predicate matches more rows than the wrapper's batch_size,
/// the page-filtered streaming path must split the filtered rows across multiple
/// Next() calls. Pages are written 3 rows wide (write_batch_size=3 with
/// data_pagesize=1) so that filtered rows span multiple page-sized chunks; the
/// emitted batches must (a) sum to the RowRanges row count and (b) never exceed
/// the configured batch_size — TableBatchReader additionally caps each batch at
/// the underlying chunk boundary, which is fine as long as the cap holds.
TEST_F(FileReaderWrapperTest, PageFilteredRespectsBatchSize) {
    constexpr int32_t kRowCount = 60;
    constexpr int32_t kPageRowCount = 3;
    constexpr int64_t kExpectedTotal = 30;

    std::string file_path = PathUtil::JoinPath(dir_->Str(), "page_split.parquet");
    PrepareParquetFile(file_path, kRowCount, /*enable_page_index=*/true,
                       /*write_batch_size=*/kPageRowCount);

    // Keep rows [0, 29] — the first 10 pages of the row group.
    RowRanges rr({RowRanges::Range(0, kExpectedTotal - 1)});

    for (int64_t batch_size : {int64_t{1}, int64_t{2}, int64_t{3}, int64_t{5}, int64_t{10}}) {
        SCOPED_TRACE("batch_size=" + std::to_string(batch_size));
        ASSERT_OK_AND_ASSIGN(auto reader_wrapper, PrepareReaderWrapper(file_path, batch_size));
        ASSERT_OK(reader_wrapper->PrepareForReading(
            {TargetRowGroup(/*rg_index=*/0, /*is_partially_matched=*/true, /*ranges=*/rr)},
            {0, 1, 2}));

        int64_t total = 0;
        int64_t batch_count = 0;
        while (true) {
            ASSERT_OK_AND_ASSIGN(auto batch, reader_wrapper->Next());
            if (!batch) break;
            ASSERT_GT(batch->num_rows(), 0);
            ASSERT_LE(batch->num_rows(), batch_size);
            total += batch->num_rows();
            ++batch_count;
        }
        ASSERT_EQ(kExpectedTotal, total);
        const int64_t min_batches = (kExpectedTotal + batch_size - 1) / batch_size;
        ASSERT_GE(batch_count, min_batches);
    }
}

TEST_F(FileReaderWrapperTest, GetRowGroupRanges) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "test.parquet");
    PrepareParquetFile(file_path, /*row_count=*/5500);
    ASSERT_OK_AND_ASSIGN(auto reader_wrapper, PrepareReaderWrapper(file_path));
    ASSERT_OK_AND_ASSIGN(auto ranges, reader_wrapper->GetRowGroupRanges({0, 3, 5}));
    std::vector<std::pair<uint64_t, uint64_t>> expected_read_ranges = {
        {0, 1000}, {3000, 4000}, {5000, 5500}};
    ASSERT_EQ(expected_read_ranges, ranges);
    ASSERT_NOK_WITH_MSG(reader_wrapper->GetRowGroupRanges({0, 3, 6}), "out of bound");
    ASSERT_OK_AND_ASSIGN(ranges, reader_wrapper->GetRowGroupRanges({}));
    ASSERT_TRUE(ranges.empty());
}

TEST_F(FileReaderWrapperTest, ApplyReadRanges) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "test.parquet");
    PrepareParquetFile(file_path, /*row_count=*/5500);
    ASSERT_OK_AND_ASSIGN(auto reader_wrapper, PrepareReaderWrapper(file_path));

    // Prepare with a subset of row groups: {0, 1, 2, 4, 5}
    std::vector<TargetRowGroup> initial_targets = {
        TargetRowGroup(/*rg_index=*/0, /*is_partially_matched=*/false,
                       /*ranges=*/RowRanges()),
        TargetRowGroup(/*rg_index=*/1, /*is_partially_matched=*/false,
                       /*ranges=*/RowRanges()),
        TargetRowGroup(/*rg_index=*/2, /*is_partially_matched=*/false,
                       /*ranges=*/RowRanges()),
        TargetRowGroup(/*rg_index=*/4, /*is_partially_matched=*/false,
                       /*ranges=*/RowRanges()),
        TargetRowGroup(/*rg_index=*/5, /*is_partially_matched=*/false,
                       /*ranges=*/RowRanges())};
    std::vector<int32_t> all_columns = {0, 1, 2};
    ASSERT_OK(reader_wrapper->PrepareForReadingLazy(initial_targets, all_columns));

    // Apply read ranges that match RG 0, 3, 5. Only 0 and 5 are in initial targets.
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges = {
        {0, 1000}, {3000, 4000}, {5000, 5500}};
    ASSERT_OK(reader_wrapper->ApplyReadRanges(read_ranges));

    // Verify: reading should only produce rows from RG 0 (1000 rows) and RG 5 (500 rows).
    int64_t total_rows = 0;
    while (true) {
        ASSERT_OK_AND_ASSIGN(auto batch, reader_wrapper->Next());
        if (!batch) {
            break;
        }
        total_rows += batch->num_rows();
    }
    ASSERT_EQ(1500, total_rows);

    // Apply empty read ranges should result in no data.
    ASSERT_OK(reader_wrapper->PrepareForReadingLazy(initial_targets, all_columns));
    ASSERT_OK(reader_wrapper->ApplyReadRanges({}));
    ASSERT_OK_AND_ASSIGN(auto batch, reader_wrapper->Next());
    ASSERT_FALSE(batch);
}

TEST_F(FileReaderWrapperTest, ApplyReadRangesWiderSecondCall) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "test.parquet");
    PrepareParquetFile(file_path, /*row_count=*/5500);
    ASSERT_OK_AND_ASSIGN(auto reader_wrapper, PrepareReaderWrapper(file_path));

    // Prepare with row groups: {0, 1, 2, 4, 5}
    std::vector<TargetRowGroup> initial_targets = {
        TargetRowGroup(/*rg_index=*/0, /*is_partially_matched=*/false,
                       /*ranges=*/RowRanges()),
        TargetRowGroup(/*rg_index=*/1, /*is_partially_matched=*/false,
                       /*ranges=*/RowRanges()),
        TargetRowGroup(/*rg_index=*/2, /*is_partially_matched=*/false,
                       /*ranges=*/RowRanges()),
        TargetRowGroup(/*rg_index=*/4, /*is_partially_matched=*/false,
                       /*ranges=*/RowRanges()),
        TargetRowGroup(/*rg_index=*/5, /*is_partially_matched=*/false,
                       /*ranges=*/RowRanges())};
    std::vector<int32_t> all_columns = {0, 1, 2};
    ASSERT_OK(reader_wrapper->PrepareForReadingLazy(initial_targets, all_columns));

    // First ApplyReadRanges: narrow to RG 0 only.
    ASSERT_OK(reader_wrapper->ApplyReadRanges({{0, 1000}}));

    // Second ApplyReadRanges: widen to RG 0, 1, 2. Previously excluded RG 1, 2 should restore.
    ASSERT_OK(reader_wrapper->ApplyReadRanges({{0, 1000}, {1000, 2000}, {2000, 3000}}));

    // Verify: reading should produce rows from RG 0 + 1 + 2 = 3000 rows.
    int64_t total_rows = 0;
    while (true) {
        ASSERT_OK_AND_ASSIGN(auto batch, reader_wrapper->Next());
        if (!batch) break;
        total_rows += batch->num_rows();
    }
    ASSERT_EQ(3000, total_rows);
}

TEST_F(FileReaderWrapperTest, PrepareForReading) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "test.parquet");
    PrepareParquetFile(file_path, /*row_count=*/5500);
    ASSERT_OK_AND_ASSIGN(auto reader_wrapper, PrepareReaderWrapper(file_path));
    ASSERT_OK(reader_wrapper->PrepareForReading(
        /*target_row_groups=*/{TargetRowGroup(/*rg_index=*/1, /*is_partially_matched=*/false,
                                              /*ranges=*/RowRanges())},
        /*column_indices=*/{0}));
    // seek before actual read range
    ASSERT_OK(reader_wrapper->SeekToRow(0));
    ASSERT_EQ(1000, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
              reader_wrapper->GetPreviousBatchFirstRowNumber().value());
    ASSERT_OK_AND_ASSIGN(auto record_batch, reader_wrapper->Next());
    ASSERT_EQ(512, record_batch->num_rows());
    ASSERT_EQ(1, record_batch->num_columns());
    ASSERT_EQ(1512, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(1000, reader_wrapper->GetPreviousBatchFirstRowNumber().value());
    ASSERT_OK_AND_ASSIGN(record_batch, reader_wrapper->Next());
    ASSERT_TRUE(record_batch);
    ASSERT_EQ(488, record_batch->num_rows());
    ASSERT_EQ(5500, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(1512, reader_wrapper->GetPreviousBatchFirstRowNumber().value());
    ASSERT_OK_AND_ASSIGN(record_batch, reader_wrapper->Next());
    ASSERT_FALSE(record_batch);

    // empty column indices
    ASSERT_OK(reader_wrapper->PrepareForReading(
        /*target_row_groups=*/{TargetRowGroup(/*rg_index=*/0, /*is_partially_matched=*/false,
                                              /*ranges=*/RowRanges()),
                               TargetRowGroup(/*rg_index=*/1, /*is_partially_matched=*/false,
                                              /*ranges=*/RowRanges())},
        /*column_indices=*/{}));
    ASSERT_EQ(0, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
              reader_wrapper->GetPreviousBatchFirstRowNumber().value());
    ASSERT_OK_AND_ASSIGN(record_batch, reader_wrapper->Next());
    ASSERT_EQ(512, record_batch->num_rows());
    ASSERT_EQ(0, record_batch->num_columns());

    // empty row group indices
    ASSERT_OK(reader_wrapper->PrepareForReading(
        /*target_row_groups=*/{},
        /*column_indices=*/{0}));
    ASSERT_EQ(5500, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
              reader_wrapper->GetPreviousBatchFirstRowNumber().value());
}

}  // namespace paimon::parquet::test
