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

#include "paimon/common/reader/prefetch_file_batch_reader_impl.h"

#include <set>

#include "arrow/compute/api.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/executor.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/format_writer.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/mock/mock_format_reader_builder.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/read_ahead_cache.h"

namespace paimon::test {

struct TestParam {
    std::string file_format;
    PrefetchCacheMode cache_mode;
};

class PrefetchFileBatchReaderImplTest : public ::testing::Test,
                                        public ::testing::WithParamInterface<TestParam> {
 public:
    void SetUp() override {
        fields_ = {arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int64()),
                   arrow::field("f2", arrow::boolean())};
        data_type_ = arrow::struct_(fields_);
        mock_fs_ = std::make_shared<MockFileSystem>();
        local_fs_ = std::make_shared<LocalFileSystem>();
        executor_ = CreateDefaultExecutor(/*thread_count=*/2);
        dir_ = ::paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
    }
    void TearDown() override {}

    std::shared_ptr<arrow::Array> PrepareArray(int32_t length, int32_t offset = 0) {
        arrow::StructBuilder struct_builder(
            data_type_, arrow::default_memory_pool(),
            {std::make_shared<arrow::StringBuilder>(), std::make_shared<arrow::Int64Builder>(),
             std::make_shared<arrow::BooleanBuilder>()});
        auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
        auto big_int_builder = static_cast<arrow::Int64Builder*>(struct_builder.field_builder(1));
        auto bool_builder = static_cast<arrow::BooleanBuilder*>(struct_builder.field_builder(2));
        for (int32_t i = 0 + offset; i < length + offset; ++i) {
            EXPECT_TRUE(struct_builder.Append().ok());
            EXPECT_TRUE(string_builder->Append("str_" + std::to_string(i)).ok());
            EXPECT_TRUE(big_int_builder->Append(i).ok());
            EXPECT_TRUE(bool_builder->Append(static_cast<bool>(i % 2)).ok());
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(struct_builder.Finish(&array).ok());
        return array;
    }

    void PrepareTestData(const std::string& file_format_str,
                         const std::shared_ptr<arrow::Array>& array, int32_t stripe_row_count,
                         int32_t row_index_stride) const {
        // for simple case, assume that array.length() %  row_index_stride == 0
        ASSERT_EQ(array->length() % row_index_stride, 0);
        arrow::Schema schema(array->type()->fields());
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(schema, &c_schema).ok());
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FileFormat> file_format,
            FileFormatFactory::Get(
                file_format_str,
                {{"parquet.write.max-row-group-length", std::to_string(row_index_stride)},
                 {"orc.row.index.stride", std::to_string(row_index_stride)}}));

        ASSERT_OK_AND_ASSIGN(auto writer_builder,
                             file_format->CreateWriterBuilder(&c_schema, 1024));
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<OutputStream> out,
            local_fs_->Create(PathUtil::JoinPath(dir_->Str(), "file." + file_format_str),
                              /*overwrite=*/false));
        ASSERT_OK_AND_ASSIGN(auto writer, writer_builder->Build(out, "zstd"));

        int32_t write_batch_count = array->length() / row_index_stride;
        for (int32_t i = 0; i < write_batch_count; i++) {
            auto slice = array->Slice(i * row_index_stride, row_index_stride);
            auto copied_array = arrow::Concatenate({slice}).ValueOr(nullptr);
            ASSERT_TRUE(copied_array);
            ::ArrowArray c_array;
            ASSERT_TRUE(arrow::ExportArray(*copied_array, &c_array).ok());
            ASSERT_OK(writer->AddBatch(&c_array));
        }
        ASSERT_OK(writer->Flush());
        ASSERT_OK(writer->Finish());
        ASSERT_OK(out->Flush());
        ASSERT_OK(out->Close());
    }

    std::unique_ptr<PrefetchFileBatchReaderImpl> PreparePrefetchReader(
        const std::string& file_format_str, const arrow::Schema* read_schema,
        const std::shared_ptr<Predicate>& predicate,
        const std::optional<RoaringBitmap32>& selection_bitmap, int32_t batch_size,
        int32_t prefetch_max_parallel_num, PrefetchCacheMode cache_mode) const {
        EXPECT_OK_AND_ASSIGN(std::unique_ptr<FileFormat> file_format,
                             FileFormatFactory::Get(file_format_str, {}));
        EXPECT_OK_AND_ASSIGN(auto reader_builder, file_format->CreateReaderBuilder(batch_size));
        EXPECT_OK_AND_ASSIGN(
            std::unique_ptr<PrefetchFileBatchReaderImpl> reader,
            PrefetchFileBatchReaderImpl::Create(
                PathUtil::JoinPath(dir_->Str(), "file." + file_format->Identifier()),
                reader_builder.get(), local_fs_, prefetch_max_parallel_num, batch_size,
                prefetch_max_parallel_num * 2, /*enable_adaptive_prefetch_strategy=*/false,
                CreateDefaultExecutor(prefetch_max_parallel_num - 1),
                /*initialize_read_ranges=*/false, cache_mode, CacheConfig(), GetDefaultPool()));
        std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
        auto arrow_status = arrow::ExportSchema(*read_schema, c_schema.get());
        EXPECT_TRUE(arrow_status.ok());
        EXPECT_OK(reader->SetReadSchema(c_schema.get(), predicate, selection_bitmap));
        return reader;
    }

    bool HasValue(const std::vector<
                  std::unique_ptr<ThreadsafeQueue<PrefetchFileBatchReaderImpl::PrefetchBatch>>>&
                      prefetch_queues) {
        for (const auto& queue : prefetch_queues) {
            if (!queue->empty()) {
                return true;
            }
        }
        return false;
    }

    bool CheckEqual(const std::shared_ptr<arrow::ChunkedArray>& lhs,
                    const std::shared_ptr<arrow::ChunkedArray>& rhs) {
        std::string lhs_str, rhs_str;
        auto print_option = arrow::PrettyPrintOptions::Defaults();
        print_option.window = 1000;
        print_option.container_window = 1000;
        EXPECT_TRUE(arrow::PrettyPrint(*lhs, print_option, &lhs_str).ok());
        EXPECT_TRUE(arrow::PrettyPrint(*rhs, print_option, &rhs_str).ok());
        bool is_equal = lhs->Equals(rhs);
        if (!is_equal) {
            std::cout << "lhs array: " << lhs_str << ", rhs array: " << rhs_str;
        }
        return is_equal;
    }

 private:
    arrow::FieldVector fields_;
    std::shared_ptr<arrow::DataType> data_type_;
    std::shared_ptr<FileSystem> mock_fs_;
    std::shared_ptr<FileSystem> local_fs_;
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<Executor> executor_;
};

std::vector<TestParam> PrepareTestParam() {
    std::vector<TestParam> values = {
        TestParam{"parquet", PrefetchCacheMode::ALWAYS},
        TestParam{"parquet", PrefetchCacheMode::EXCLUDE_BITMAP},
        TestParam{"parquet", PrefetchCacheMode::EXCLUDE_PREDICATE},
        TestParam{"parquet", PrefetchCacheMode::EXCLUDE_BITMAP_OR_PREDICATE},
        TestParam{"parquet", PrefetchCacheMode::NEVER}};
#ifdef PAIMON_ENABLE_ORC
    values.emplace_back(TestParam{"orc", PrefetchCacheMode::ALWAYS});
    values.emplace_back(TestParam{"orc", PrefetchCacheMode::EXCLUDE_BITMAP});
    values.emplace_back(TestParam{"orc", PrefetchCacheMode::EXCLUDE_PREDICATE});
    values.emplace_back(TestParam{"orc", PrefetchCacheMode::EXCLUDE_BITMAP_OR_PREDICATE});
    values.emplace_back(TestParam{"orc", PrefetchCacheMode::NEVER});
#endif
    return values;
}

INSTANTIATE_TEST_SUITE_P(TestParam, PrefetchFileBatchReaderImplTest,
                         ::testing::ValuesIn(PrepareTestParam()));

TEST_F(PrefetchFileBatchReaderImplTest, TestSimple) {
    auto data_array = PrepareArray(101);
    int32_t batch_size = 10;
    for (auto prefetch_max_parallel_num : {1, 2, 3, 5, 8, 10}) {
        MockFormatReaderBuilder reader_builder(data_array, data_type_, batch_size);
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            PrefetchFileBatchReaderImpl::Create(
                /*data_file_path=*/"", &reader_builder, mock_fs_, prefetch_max_parallel_num,
                batch_size, prefetch_max_parallel_num * 2,
                /*enable_adaptive_prefetch_strategy=*/false, executor_,
                /*initialize_read_ranges=*/true, /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS,
                CacheConfig(), GetDefaultPool()));
        ASSERT_EQ(reader->GetPreviousBatchFirstRowNumber(), -1);
        ASSERT_OK_AND_ASSIGN(auto result_array,
                             ReadResultCollector::CollectResult(
                                 reader.get(), /*max simulated data processing time*/ 100));
        ASSERT_EQ(reader->GetPreviousBatchFirstRowNumber(), 101);
        auto expected_array = std::make_shared<arrow::ChunkedArray>(data_array);
        ASSERT_TRUE(result_array->Equals(expected_array));
    }
}

TEST_F(PrefetchFileBatchReaderImplTest, TestReadWithLimits) {
    auto data_array = PrepareArray(101);
    int32_t batch_size = 10;
    int32_t prefetch_max_parallel_num = 12;

    MockFormatReaderBuilder reader_builder(data_array, data_type_, batch_size);
    ASSERT_OK_AND_ASSIGN(
        auto reader,
        PrefetchFileBatchReaderImpl::Create(
            /*data_file_path=*/"", &reader_builder, mock_fs_, prefetch_max_parallel_num, batch_size,
            prefetch_max_parallel_num * 2, /*enable_adaptive_prefetch_strategy=*/false, executor_,
            /*initialize_read_ranges=*/true, /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS,
            CacheConfig(), GetDefaultPool()));
    // simulate read limits, only read 8 batches
    for (int32_t i = 0; i < 8; i++) {
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                             reader->NextBatchWithBitmap());
        auto& [batch, bitmap] = batch_with_bitmap;
        ASSERT_EQ(batch.first->length, bitmap.Cardinality());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array,
                             ReadResultCollector::GetArray(std::move(batch)));
        ASSERT_TRUE(array);
    }
    reader->Close();
    // test metrics
    auto read_metrics = reader->GetReaderMetrics();
    ASSERT_TRUE(read_metrics);
}

TEST_F(PrefetchFileBatchReaderImplTest, TestReadWithoutInitializeReadRanges) {
    auto data_array = PrepareArray(101);
    int32_t batch_size = 10;
    int32_t prefetch_max_parallel_num = 12;

    MockFormatReaderBuilder reader_builder(data_array, data_type_, batch_size);
    ASSERT_OK_AND_ASSIGN(
        auto reader,
        PrefetchFileBatchReaderImpl::Create(
            /*data_file_path=*/"", &reader_builder, mock_fs_, prefetch_max_parallel_num, batch_size,
            prefetch_max_parallel_num * 2, /*enable_adaptive_prefetch_strategy=*/false, executor_,
            /*initialize_read_ranges=*/false, /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS,
            CacheConfig(), GetDefaultPool()));
    // simulate read limits, only read 8 batches
    ASSERT_NOK_WITH_MSG(reader->NextBatchWithBitmap(),
                        "prefetch reader read ranges are not initialized");
    reader->Close();
}

TEST_F(PrefetchFileBatchReaderImplTest, FilterReadRangesWithoutBitmap) {
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges = {
        {0, 1000},    {1000, 2000}, {2000, 3000}, {3000, 4000}, {4000, 5000},
        {5000, 6000}, {6000, 7000}, {7000, 8000}, {8000, 9000}, {9000, 10000}};
    auto filtered_ranges = PrefetchFileBatchReaderImpl::FilterReadRanges(read_ranges, std::nullopt);
    ASSERT_EQ(filtered_ranges, read_ranges);
}

TEST_F(PrefetchFileBatchReaderImplTest, FilterReadRangesWithAllZeroBitmap) {
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges = {
        {0, 1000},    {1000, 2000}, {2000, 3000}, {3000, 4000}, {4000, 5000},
        {5000, 6000}, {6000, 7000}, {7000, 8000}, {8000, 9000}, {9000, 10000}};
    auto bitmap = RoaringBitmap32::From({});
    auto filtered_ranges = PrefetchFileBatchReaderImpl::FilterReadRanges(read_ranges, bitmap);
    ASSERT_TRUE(filtered_ranges.empty());
}

TEST_F(PrefetchFileBatchReaderImplTest, FilterReadRangesWithBitmap) {
    auto data_array = PrepareArray(10000);
    std::set<int32_t> valid_row_ids;
    for (int32_t i = 1000; i < 2000; i++) {
        valid_row_ids.insert(i);
    }
    for (int32_t i = 3000; i < 6500; i++) {
        valid_row_ids.insert(i);
    }
    std::vector<int32_t> bitmap_data(valid_row_ids.begin(), valid_row_ids.end());
    auto bitmap = RoaringBitmap32::From(bitmap_data);
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges = {
        {0, 1000},    {1000, 2000}, {2000, 3000}, {3000, 4000}, {4000, 5000},
        {5000, 6000}, {6000, 7000}, {7000, 8000}, {8000, 9000}, {9000, 10000}};
    auto filtered_ranges = PrefetchFileBatchReaderImpl::FilterReadRanges(read_ranges, bitmap);
    std::vector<std::pair<uint64_t, uint64_t>> expected_filtered_ranges = {
        {1000, 2000}, {3000, 4000}, {4000, 5000}, {5000, 6000}, {6000, 7000}};
    ASSERT_EQ(expected_filtered_ranges, filtered_ranges);
}

TEST_F(PrefetchFileBatchReaderImplTest, DispatchReadRangesEmpty) {
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges;
    auto read_ranges_in_group = PrefetchFileBatchReaderImpl::DispatchReadRanges(read_ranges, 3);
    ASSERT_EQ(read_ranges_in_group.size(), 3);
    ASSERT_TRUE(read_ranges_in_group[0].empty());
    ASSERT_TRUE(read_ranges_in_group[1].empty());
    ASSERT_TRUE(read_ranges_in_group[2].empty());
}

TEST_F(PrefetchFileBatchReaderImplTest, DispatchReadRanges) {
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges = {
        {0, 10000}, {10000, 20000}, {20000, 30000}, {30000, 40000}};
    auto read_ranges_in_group = PrefetchFileBatchReaderImpl::DispatchReadRanges(read_ranges, 3);
    std::vector<std::pair<uint64_t, uint64_t>> expected_group_0 = {{0, 10000}, {30000, 40000}};
    ASSERT_EQ(read_ranges_in_group[0], expected_group_0);
    std::vector<std::pair<uint64_t, uint64_t>> expected_group_1 = {{10000, 20000}};
    ASSERT_EQ(read_ranges_in_group[1], expected_group_1);
    std::vector<std::pair<uint64_t, uint64_t>> expected_group_2 = {{20000, 30000}};
    ASSERT_EQ(read_ranges_in_group[2], expected_group_2);
}

TEST_F(PrefetchFileBatchReaderImplTest, RefreshReadRanges) {
    auto data_array = PrepareArray(101);
    int32_t batch_size = 30;
    int32_t prefetch_max_parallel_num = 3;
    MockFormatReaderBuilder reader_builder(data_array, data_type_, batch_size);
    ASSERT_OK_AND_ASSIGN(
        auto reader,
        PrefetchFileBatchReaderImpl::Create(
            /*data_file_path=*/"", &reader_builder, mock_fs_, prefetch_max_parallel_num, batch_size,
            prefetch_max_parallel_num * 2, /*enable_adaptive_prefetch_strategy=*/false, executor_,
            /*initialize_read_ranges=*/false, /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS,
            CacheConfig(), GetDefaultPool()));
    auto prefetch_reader = dynamic_cast<PrefetchFileBatchReaderImpl*>(reader.get());
    ASSERT_OK(prefetch_reader->RefreshReadRanges());
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges_0 = {{0, 30}, {90, 101}};
    auto mock_reader_0 = dynamic_cast<MockFileBatchReader*>(prefetch_reader->readers_[0].get());
    ASSERT_EQ(mock_reader_0->GetReadRanges(), read_ranges_0);
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges_1 = {{30, 60}};
    auto mock_reader_1 = dynamic_cast<MockFileBatchReader*>(prefetch_reader->readers_[1].get());
    ASSERT_EQ(mock_reader_1->GetReadRanges(), read_ranges_1);
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges_2 = {{60, 90}};
    auto mock_reader_2 = dynamic_cast<MockFileBatchReader*>(prefetch_reader->readers_[2].get());
    ASSERT_EQ(mock_reader_2->GetReadRanges(), read_ranges_2);
}

TEST_F(PrefetchFileBatchReaderImplTest, SetReadRanges) {
    auto data_array = PrepareArray(400);
    int32_t batch_size = 30;
    int32_t prefetch_max_parallel_num = 3;
    MockFormatReaderBuilder reader_builder(data_array, data_type_, batch_size);
    ASSERT_OK_AND_ASSIGN(
        auto reader,
        PrefetchFileBatchReaderImpl::Create(
            /*data_file_path=*/"", &reader_builder, mock_fs_, prefetch_max_parallel_num, batch_size,
            prefetch_max_parallel_num * 2, /*enable_adaptive_prefetch_strategy=*/false, executor_,
            /*initialize_read_ranges=*/false, /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS,
            CacheConfig(), GetDefaultPool()));
    auto prefetch_reader = dynamic_cast<PrefetchFileBatchReaderImpl*>(reader.get());
    ASSERT_FALSE(prefetch_reader->need_prefetch_);
    prefetch_reader->need_prefetch_ = true;
    std::vector<std::pair<uint64_t, uint64_t>> ranges = {
        {0, 100}, {100, 200}, {200, 300}, {300, 400}};
    ASSERT_OK(prefetch_reader->SetReadRanges(ranges));
    auto& read_ranges_queue = prefetch_reader->read_ranges_;
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges;
    for (auto& iter : read_ranges_queue) {
        read_ranges.push_back(iter);
    }
    ranges.emplace_back(400, 401);
    ASSERT_EQ(read_ranges, ranges);

    std::vector<std::pair<uint64_t, uint64_t>> read_ranges_0 = {{0, 100}, {300, 400}};
    auto mock_reader_0 = dynamic_cast<MockFileBatchReader*>(prefetch_reader->readers_[0].get());
    ASSERT_EQ(mock_reader_0->GetReadRanges(), read_ranges_0);
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges_1 = {{100, 200}};
    auto mock_reader_1 = dynamic_cast<MockFileBatchReader*>(prefetch_reader->readers_[1].get());
    ASSERT_EQ(mock_reader_1->GetReadRanges(), read_ranges_1);
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges_2 = {{200, 300}};
    auto mock_reader_2 = dynamic_cast<MockFileBatchReader*>(prefetch_reader->readers_[2].get());
    ASSERT_EQ(mock_reader_2->GetReadRanges(), read_ranges_2);
}

TEST_F(PrefetchFileBatchReaderImplTest, TestReadWithLargeBatchSize) {
    auto data_array = PrepareArray(101);
    int32_t batch_size = 150;
    int32_t prefetch_max_parallel_num = 3;
    MockFormatReaderBuilder reader_builder(data_array, data_type_, batch_size);
    ASSERT_OK_AND_ASSIGN(
        auto reader,
        PrefetchFileBatchReaderImpl::Create(
            /*data_file_path=*/"", &reader_builder, mock_fs_, prefetch_max_parallel_num, batch_size,
            prefetch_max_parallel_num * 2, /*enable_adaptive_prefetch_strategy=*/false, executor_,
            /*initialize_read_ranges=*/true, /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS,
            CacheConfig(), GetDefaultPool()));
    ASSERT_EQ(reader->GetPreviousBatchFirstRowNumber(), -1);
    ASSERT_OK_AND_ASSIGN(auto result_array,
                         ReadResultCollector::CollectResult(
                             reader.get(), /*max simulated data processing time*/ 100));
    ASSERT_EQ(reader->GetPreviousBatchFirstRowNumber(), 101);
    auto expected_array = std::make_shared<arrow::ChunkedArray>(data_array);
    ASSERT_TRUE(result_array->Equals(expected_array));
}

TEST_F(PrefetchFileBatchReaderImplTest, TestPartialReaderSuccessRead) {
    auto data_array = PrepareArray(101);
    int32_t batch_size = 10;
    int32_t prefetch_max_parallel_num = 3;
    MockFormatReaderBuilder reader_builder(data_array, data_type_, batch_size);
    ASSERT_OK_AND_ASSIGN(
        auto reader,
        PrefetchFileBatchReaderImpl::Create(
            /*data_file_path=*/"", &reader_builder, mock_fs_, prefetch_max_parallel_num, batch_size,
            prefetch_max_parallel_num, /*enable_adaptive_prefetch_strategy=*/false, executor_,
            /*initialize_read_ranges=*/true, /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS,
            CacheConfig(), GetDefaultPool()));
    auto prefetch_reader = dynamic_cast<PrefetchFileBatchReaderImpl*>(reader.get());
    for (int32_t i = 0; i < prefetch_max_parallel_num; i++) {
        dynamic_cast<MockFileBatchReader*>(prefetch_reader->readers_[i].get())
            ->EnableRandomizeBatchSize(false);
    }

    arrow::ArrayVector result_array_vector;
    ASSERT_EQ(reader->GetPreviousBatchFirstRowNumber(), -1);
    ASSERT_OK_AND_ASSIGN(auto batch_with_bitmap, reader->NextBatchWithBitmap());
    auto& [batch, bitmap] = batch_with_bitmap;
    ASSERT_EQ(batch.first->length, bitmap.Cardinality());
    ASSERT_EQ(reader->GetPreviousBatchFirstRowNumber(), 0);
    ASSERT_OK_AND_ASSIGN(auto array, ReadResultCollector::GetArray(std::move(batch)));
    result_array_vector.push_back(array);
    ASSERT_OK(prefetch_reader->GetReadStatus());
    usleep(100000);  // sleep 100ms to ensure that the other data has been pushed
    ASSERT_TRUE(HasValue(prefetch_reader->prefetch_queues_));

    // Set IOError for reader[1] after the first NextBatch().
    // Now the data in prefetch_queues_[0] is [30,39], prefetch_queues_[1] is [10,19],
    // prefetch_queues_[2] is [20,29],
    // So, the IOError will occur at [40,49].
    dynamic_cast<MockFileBatchReader*>(prefetch_reader->readers_[1].get())
        ->SetNextBatchStatus(Status::IOError("mock error"));
    usleep(100000);
    // pop [10,19]
    ASSERT_OK_AND_ASSIGN(batch_with_bitmap, reader->NextBatchWithBitmap());
    // now reader1 fetch [40,49] and set error status.
    usleep(100000);
    ASSERT_NOK(reader->NextBatchWithBitmap());
    ReaderUtils::ReleaseReadBatch(std::move(batch_with_bitmap.first));
}

TEST_F(PrefetchFileBatchReaderImplTest, TestAllReaderFailedWithIOError) {
    auto data_array = PrepareArray(101);
    int32_t batch_size = 10;
    int32_t prefetch_max_parallel_num = 3;
    MockFormatReaderBuilder reader_builder(data_array, data_type_, batch_size);
    ASSERT_OK_AND_ASSIGN(
        auto reader,
        PrefetchFileBatchReaderImpl::Create(
            /*data_file_path=*/"", &reader_builder, mock_fs_, prefetch_max_parallel_num, batch_size,
            prefetch_max_parallel_num * 2, /*enable_adaptive_prefetch_strategy=*/false, executor_,
            /*initialize_read_ranges=*/true, /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS,
            CacheConfig(), GetDefaultPool()));

    auto prefetch_reader = dynamic_cast<PrefetchFileBatchReaderImpl*>(reader.get());
    for (int32_t i = 0; i < prefetch_max_parallel_num; i++) {
        dynamic_cast<MockFileBatchReader*>(prefetch_reader->readers_[i].get())
            ->SetNextBatchStatus(Status::IOError("mock error"));
    }

    ASSERT_EQ(reader->GetPreviousBatchFirstRowNumber(), -1);
    auto batch_result = reader->NextBatchWithBitmap();
    ASSERT_EQ(reader->GetPreviousBatchFirstRowNumber(), -1);
    ASSERT_FALSE(batch_result.ok());
    ASSERT_TRUE(batch_result.status().IsIOError());
    ASSERT_FALSE(prefetch_reader->is_shutdown_);
    ASSERT_NOK(prefetch_reader->GetReadStatus());
    ASSERT_FALSE(HasValue(prefetch_reader->prefetch_queues_));

    // call NextBatch again, will still return error status
    auto batch_result2 = reader->NextBatchWithBitmap();
    ASSERT_EQ(reader->GetPreviousBatchFirstRowNumber(), -1);
    ASSERT_FALSE(batch_result2.ok());
    ASSERT_TRUE(batch_result2.status().IsIOError());
}

TEST_F(PrefetchFileBatchReaderImplTest, TestPrefetchWithEmptyData) {
    auto data_array = PrepareArray(0);
    int32_t batch_size = 10;
    int32_t prefetch_max_parallel_num = 3;
    MockFormatReaderBuilder reader_builder(data_array, data_type_, batch_size);
    ASSERT_OK_AND_ASSIGN(
        auto reader,
        PrefetchFileBatchReaderImpl::Create(
            /*data_file_path=*/"", &reader_builder, mock_fs_, prefetch_max_parallel_num, batch_size,
            prefetch_max_parallel_num * 2, /*enable_adaptive_prefetch_strategy=*/false, executor_,
            /*initialize_read_ranges=*/true, /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS,
            CacheConfig(), GetDefaultPool()));
    ASSERT_EQ(reader->GetPreviousBatchFirstRowNumber(), -1);
    ASSERT_OK_AND_ASSIGN(auto result_array,
                         ReadResultCollector::CollectResult(
                             reader.get(), /*max simulated data processing time*/ 100));
    ASSERT_EQ(reader->GetPreviousBatchFirstRowNumber(), 0);
    ASSERT_FALSE(result_array);
}

TEST_F(PrefetchFileBatchReaderImplTest, TestCallNextBatchAfterReadingEof) {
    auto data_array = PrepareArray(10);
    int32_t batch_size = 10;
    int32_t prefetch_max_parallel_num = 6;
    MockFormatReaderBuilder reader_builder(data_array, data_type_, batch_size);
    ASSERT_OK_AND_ASSIGN(
        auto reader,
        PrefetchFileBatchReaderImpl::Create(
            /*data_file_path=*/"", &reader_builder, mock_fs_, prefetch_max_parallel_num, batch_size,
            prefetch_max_parallel_num * 2, /*enable_adaptive_prefetch_strategy=*/false, executor_,
            /*initialize_read_ranges=*/true, /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS,
            CacheConfig(), GetDefaultPool()));
    ASSERT_EQ(reader->GetPreviousBatchFirstRowNumber(), -1);
    ASSERT_OK_AND_ASSIGN(auto result_array,
                         ReadResultCollector::CollectResult(
                             reader.get(), /*max simulated data processing time*/ 100));
    ASSERT_EQ(reader->GetPreviousBatchFirstRowNumber(), 10);
    auto expected_array = std::make_shared<arrow::ChunkedArray>(data_array);
    ASSERT_TRUE(result_array->Equals(expected_array));

    // continue to call NextBatch() after reading eof
    ASSERT_OK_AND_ASSIGN(auto batch_with_bitmap, reader->NextBatchWithBitmap());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch_with_bitmap));
}

TEST_F(PrefetchFileBatchReaderImplTest, TestCreateReaderWithoutNextBatch) {
    auto data_array = PrepareArray(101);
    int32_t batch_size = 10;
    int32_t prefetch_max_parallel_num = 3;
    MockFormatReaderBuilder reader_builder(data_array, data_type_, batch_size);
    ASSERT_OK_AND_ASSIGN(
        auto reader,
        PrefetchFileBatchReaderImpl::Create(
            /*data_file_path=*/"", &reader_builder, mock_fs_, prefetch_max_parallel_num, batch_size,
            prefetch_max_parallel_num * 2, /*enable_adaptive_prefetch_strategy=*/false, executor_,
            /*initialize_read_ranges=*/true, /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS,
            CacheConfig(), GetDefaultPool()));
}

TEST_F(PrefetchFileBatchReaderImplTest, TestInvalidCase) {
    auto data_array = PrepareArray(101);
    int32_t batch_size = 10;
    int32_t prefetch_max_parallel_num = 3;
    std::string data_file_path = "";
    MockFormatReaderBuilder reader_builder(data_array, data_type_, batch_size);
    {
        ASSERT_NOK(PrefetchFileBatchReaderImpl::Create(
            data_file_path, &reader_builder, mock_fs_,
            /*prefetch_max_parallel_num=*/0, batch_size, 2,
            /*enable_adaptive_prefetch_strategy=*/false, executor_,
            /*initialize_read_ranges=*/true, /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS,
            CacheConfig(), GetDefaultPool()));
    }
    {
        ASSERT_NOK(PrefetchFileBatchReaderImpl::Create(
            data_file_path, &reader_builder, mock_fs_, prefetch_max_parallel_num, /*batch_size=*/-1,
            prefetch_max_parallel_num * 2, /*enable_adaptive_prefetch_strategy=*/false, executor_,
            /*initialize_read_ranges=*/true, /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS,
            CacheConfig(), GetDefaultPool()));
    }
    {
        ASSERT_NOK(PrefetchFileBatchReaderImpl::Create(
            data_file_path, &reader_builder, mock_fs_, prefetch_max_parallel_num, batch_size,
            prefetch_max_parallel_num * 2,
            /*enable_adaptive_prefetch_strategy=*/false,
            /*executor=*/nullptr, /*initialize_read_ranges=*/true,
            /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS, CacheConfig(), GetDefaultPool()));
    }
    {
        ASSERT_NOK(PrefetchFileBatchReaderImpl::Create(
            data_file_path, /*reader_builder=*/nullptr, mock_fs_, prefetch_max_parallel_num,
            batch_size, prefetch_max_parallel_num * 2,
            /*enable_adaptive_prefetch_strategy=*/false, executor_,
            /*initialize_read_ranges=*/true, /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS,
            CacheConfig(), GetDefaultPool()));
    }
    {
        ASSERT_NOK(PrefetchFileBatchReaderImpl::Create(
            data_file_path, &reader_builder,
            /*fs=*/nullptr, prefetch_max_parallel_num, batch_size, prefetch_max_parallel_num * 2,
            /*enable_adaptive_prefetch_strategy=*/false, executor_,
            /*initialize_read_ranges=*/true, /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS,
            CacheConfig(), GetDefaultPool()));
    }
    {
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            PrefetchFileBatchReaderImpl::Create(
                data_file_path, &reader_builder, mock_fs_, prefetch_max_parallel_num, batch_size,
                prefetch_max_parallel_num * 2,
                /*enable_adaptive_prefetch_strategy=*/false, executor_,
                /*initialize_read_ranges=*/true, /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS,
                CacheConfig(), GetDefaultPool()));
        ASSERT_NOK_WITH_MSG(reader->SeekToRow(/*row_number=*/101),
                            "not support seek to row for prefetch reader");
    }
}

/// There are three stripes: [0,30), [30,60), [60,90). After predicate pushdown, the stripe
/// [30,60) will be filtered out.
/// The read range is [0,30), [30,60), [60,90). So, expected results is [0,30), [60,90)
TEST_P(PrefetchFileBatchReaderImplTest, TestPrefetchWithPredicatePushdownWithCompleteFiltering) {
    auto [file_format, cache_mode] = GetParam();
    auto data_array = PrepareArray(90);
    PrepareTestData(file_format, data_array, /*stripe_row_count=*/30, /*row_index_stride=*/30);
    auto schema = arrow::schema(fields_);
    ASSERT_OK_AND_ASSIGN(auto predicate,
                         PredicateBuilder::Or({
                             PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                        FieldType::BIGINT, Literal(20l)),
                             PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                           FieldType::BIGINT, Literal(70l)),
                         }));

    auto reader =
        PreparePrefetchReader(file_format, schema.get(), predicate,
                              /*selection_bitmap=*/std::nullopt,
                              /*batch_size=*/10, /*prefetch_max_parallel_num=*/3, cache_mode);
    ASSERT_EQ(reader->GetPreviousBatchFirstRowNumber(), -1);
    ASSERT_OK_AND_ASSIGN(auto result_array,
                         ReadResultCollector::CollectResult(
                             reader.get(), /*max simulated data processing time*/ 100));
    ASSERT_EQ(reader->GetPreviousBatchFirstRowNumber(), 90);

    arrow::ArrayVector expected_array_vector;
    expected_array_vector.push_back(data_array->Slice(0, 30));
    expected_array_vector.push_back(data_array->Slice(60, 30));
    auto expected_array = std::make_shared<arrow::ChunkedArray>(expected_array_vector);
    ASSERT_TRUE(CheckEqual(expected_array, result_array));
}

/// There are three stripes: [0,30), [30,60), [60,90). Each stripe has 3 row groups.
/// After predicate pushdown, the row group [0, 20), [70, 90) will be remained.
/// The read range is [0,30), [30,60), [60,90).
TEST_P(PrefetchFileBatchReaderImplTest,
       TestPrefetchWithOrcPredicatePushdownWithRowGroupGranularity) {
    auto [file_format, cache_mode] = GetParam();
    auto data_array = PrepareArray(90);
    PrepareTestData(file_format, data_array, /*stripe_row_count=*/30, /*row_index_stride=*/10);

    auto schema = arrow::schema(fields_);
    ASSERT_OK_AND_ASSIGN(auto predicate,
                         PredicateBuilder::Or({
                             PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                        FieldType::BIGINT, Literal(20l)),
                             PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                           FieldType::BIGINT, Literal(70l)),
                         }));

    auto reader =
        PreparePrefetchReader(file_format, schema.get(), predicate,
                              /*selection_bitmap=*/std::nullopt,
                              /*batch_size=*/10, /*prefetch_max_parallel_num=*/3, cache_mode);
    ASSERT_OK(reader->RefreshReadRanges());
    ASSERT_EQ(reader->GetPreviousBatchFirstRowNumber(), -1);
    ASSERT_OK_AND_ASSIGN(auto result_array,
                         ReadResultCollector::CollectResult(
                             reader.get(), /*max simulated data processing time*/ 100));
    ASSERT_EQ(reader->GetPreviousBatchFirstRowNumber(), 90);

    arrow::ArrayVector expected_array_vector;
    expected_array_vector.push_back(data_array->Slice(0, 20));
    expected_array_vector.push_back(data_array->Slice(70, 20));
    auto expected_array = std::make_shared<arrow::ChunkedArray>(expected_array_vector);
    ASSERT_TRUE(CheckEqual(expected_array, result_array));
}

TEST_F(PrefetchFileBatchReaderImplTest, TestPrefetchWithBitmap) {
    auto data_array = PrepareArray(10000);
    std::set<int32_t> valid_row_ids;
    for (int32_t i = 0; i < 5120; i++) {
        valid_row_ids.insert(paimon::test::RandomNumber(0, data_array->length() - 1));
    }
    std::vector<int32_t> bitmap_data(valid_row_ids.begin(), valid_row_ids.end());
    auto bitmap = RoaringBitmap32::From(bitmap_data);
    MockFormatReaderBuilder reader_builder(data_array, data_type_, bitmap,
                                           /*read_batch_size=*/100);
    int32_t prefetch_max_parallel_num = 3;
    ASSERT_OK_AND_ASSIGN(
        auto reader,
        PrefetchFileBatchReaderImpl::Create(
            /*data_file_path=*/"", &reader_builder, mock_fs_, prefetch_max_parallel_num,
            /*batch_size=*/100, prefetch_max_parallel_num * 2,
            /*enable_adaptive_prefetch_strategy=*/false, executor_,
            /*initialize_read_ranges=*/true, /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS,
            CacheConfig(), GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(auto result_chunk_array, ReadResultCollector::CollectResult(reader.get()));

    ASSERT_OK_AND_ASSIGN(auto data_batch, ReadResultCollector::GetReadBatch(data_array));
    ASSERT_OK_AND_ASSIGN(auto expected_batch, ReaderUtils::ApplyBitmapToReadBatch(
                                                  std::make_pair(std::move(data_batch), bitmap),
                                                  arrow::default_memory_pool()));
    ASSERT_OK_AND_ASSIGN(auto expected_array,
                         ReadResultCollector::GetArray(std::move(expected_batch)));
    auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_array);
    ASSERT_TRUE(result_chunk_array->Equals(expected_chunk_array));
}

}  // namespace paimon::test
