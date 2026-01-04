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

#include "paimon/format/orc/orc_file_batch_reader.h"

#include <list>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/defs.h"
#include "paimon/format/orc/orc_adapter.h"
#include "paimon/format/orc/orc_format_defs.h"
#include "paimon/format/orc/orc_format_writer.h"
#include "paimon/format/orc/orc_input_stream_impl.h"
#include "paimon/format/orc/orc_memory_pool.h"
#include "paimon/format/orc/orc_metrics.h"
#include "paimon/format/orc/orc_output_stream_impl.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"

namespace paimon::orc::test {

struct TestParam {
    uint64_t natural_read_size;
    bool enable_tz;
};

class OrcFileBatchReaderTest : public ::testing::Test,
                               public ::testing::WithParamInterface<TestParam> {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        batch_size_ = 10;

        arrow::FieldVector fields = {
            arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
            arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};

        struct_array_ = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        ["Bob", 10, 0, 12.1], ["Emily", 10, 0, 13.1], ["Tony", 10, 0, 14.1], ["Emily", 10, 0, 15.1],
        ["Bob", 10, 0, 12.1], ["Alex", 10, 0, 16.1], ["David", 10, 0, 17.1], ["Lily", 10, 0, 17.1]
    ])")
                .ValueOrDie());
    }
    void TearDown() override {}

    std::pair<std::unique_ptr<OrcFileBatchReader>, std::shared_ptr<arrow::ChunkedArray>>
    ReadBatchWithCustomizedData(const std::shared_ptr<arrow::Array>& src_array,
                                int32_t write_batch_size, int32_t write_stripe_size,
                                int32_t write_row_index_stride, const arrow::Schema* read_schema,
                                const std::shared_ptr<Predicate>& predicate,
                                const std::optional<RoaringBitmap32>& selection_bitmap,
                                int32_t read_batch_size, double dict_key_size_threshold,
                                bool enable_lazy_decoding) const {
        arrow::Schema src_schema(src_array->type()->fields());
        EXPECT_OK_AND_ASSIGN(std::unique_ptr<::orc::Type> orc_type,
                             OrcAdapter::GetOrcType(src_schema));
        EXPECT_TRUE(orc_type);
        auto dir = paimon::test::UniqueTestDirectory::Create();
        EXPECT_TRUE(dir);
        auto fs = dir->GetFileSystem();
        std::string data_path = dir->Str() + "/test.data";
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<paimon::OutputStream> output_stream,
                             fs->Create(data_path, /*overwrite=*/true));
        EXPECT_TRUE(output_stream);
        EXPECT_OK_AND_ASSIGN(auto orc_output_stream, OrcOutputStreamImpl::Create(output_stream));
        EXPECT_TRUE(orc_output_stream);
        ::orc::WriterOptions writer_options;
        writer_options.setDictionaryKeySizeThreshold(dict_key_size_threshold);
        if (write_stripe_size != -1) {
            writer_options.setStripeSize(write_stripe_size);
        }
        if (write_row_index_stride != -1) {
            writer_options.setRowIndexStride(write_row_index_stride);
        }
        std::unique_ptr<::orc::Writer> writer =
            ::orc::createWriter(*orc_type, orc_output_stream.get(), writer_options);
        // for simple case, assume that src_array.length() %  write_batch_size == 0
        EXPECT_EQ(src_array->length() % write_batch_size, 0);
        int32_t write_batch_count = src_array->length() / write_batch_size;
        for (int32_t i = 0; i < write_batch_count; i++) {
            auto src_slice = src_array->Slice(i * write_batch_size, write_batch_size);
            auto write_batch = writer->createRowBatch(src_slice->length());
            // Convert from arrow array to orc batch
            EXPECT_OK(OrcAdapter::WriteBatch(src_slice, write_batch.get()));
            writer->add(*write_batch);
        }
        writer->close();
        EXPECT_OK(output_stream->Close());

        EXPECT_OK_AND_ASSIGN(std::shared_ptr<paimon::InputStream> input_stream,
                             fs->Open(data_path));
        EXPECT_TRUE(input_stream);
        EXPECT_OK_AND_ASSIGN(auto orc_input_stream,
                             OrcInputStreamImpl::Create(input_stream, DEFAULT_NATURAL_READ_SIZE));
        EXPECT_TRUE(orc_input_stream);
        std::string enable_lazy_decoding_str = enable_lazy_decoding ? "true" : "false";
        std::map<std::string, std::string> options = {
            {ORC_READ_ENABLE_LAZY_DECODING, enable_lazy_decoding_str}};
        auto orc_batch_reader =
            PrepareOrcFileBatchReader(std::move(orc_input_stream), options, read_schema, predicate,
                                      selection_bitmap, read_batch_size);
        EXPECT_OK_AND_ASSIGN(
            auto result, paimon::test::ReadResultCollector::CollectResult(orc_batch_reader.get()));
        return std::make_pair(std::move(orc_batch_reader), result);
    }

    std::unique_ptr<OrcFileBatchReader> PrepareOrcFileBatchReader(
        const std::string& file_name, const arrow::Schema* read_schema, int32_t batch_size,
        uint64_t natural_read_size) const {
        std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             file_system->Open(file_name));
        EXPECT_TRUE(input_stream);
        EXPECT_OK_AND_ASSIGN(auto in_stream,
                             OrcInputStreamImpl::Create(input_stream, natural_read_size));
        EXPECT_TRUE(in_stream);
        std::map<std::string, std::string> options = {{ORC_READ_ENABLE_LAZY_DECODING, "true"},
                                                      {ORC_READ_ENABLE_METRICS, "true"},
                                                      {"orc.timestamp-ltz.legacy.type", "false"}};
        return PrepareOrcFileBatchReader(std::move(in_stream), options, read_schema,
                                         /*predicate=*/nullptr, /*selection_bitmap=*/std::nullopt,
                                         batch_size);
    }

    std::unique_ptr<paimon::orc::OrcFileBatchReader> PrepareOrcFileBatchReader(
        std::unique_ptr<::orc::InputStream>&& in_stream,
        const std::map<std::string, std::string>& options, const arrow::Schema* read_schema,
        const std::shared_ptr<Predicate>& predicate,
        const std::optional<RoaringBitmap32>& selection_bitmap, int32_t batch_size) const {
        EXPECT_OK_AND_ASSIGN(
            auto orc_batch_reader,
            OrcFileBatchReader::Create(std::move(in_stream), pool_, options, batch_size))
        EXPECT_TRUE(orc_batch_reader);
        std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
        auto arrow_status = arrow::ExportSchema(*read_schema, c_schema.get());
        EXPECT_TRUE(arrow_status.ok());
        EXPECT_OK(orc_batch_reader->SetReadSchema(c_schema.get(), predicate, selection_bitmap));
        return orc_batch_reader;
    }

    void WriteArray(const std::shared_ptr<FileSystem>& fs, const std::string& file_path,
                    const std::shared_ptr<arrow::Array>& src_array,
                    const std::shared_ptr<arrow::Schema>& arrow_schema,
                    const std::map<std::string, std::string>& options) const {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                             fs->Create(file_path, /*overwrite=*/true));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<OrcOutputStreamImpl> output_stream,
                             OrcOutputStreamImpl::Create(out));
        ASSERT_OK_AND_ASSIGN(
            auto format_writer,
            OrcFormatWriter::Create(std::move(output_stream), *arrow_schema, options, "zstd",
                                    /*batch_size=*/src_array->length(), pool_));
        auto arrow_array = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*src_array, arrow_array.get()).ok());
        ASSERT_OK(format_writer->AddBatch(arrow_array.get()));
        ASSERT_OK(format_writer->Flush());
        ASSERT_OK(format_writer->Finish());
        ASSERT_OK(out->Flush());
        ASSERT_OK(out->Close());
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    int32_t batch_size_;
    std::shared_ptr<arrow::StructArray> struct_array_;
};

INSTANTIATE_TEST_SUITE_P(TestParam, OrcFileBatchReaderTest,
                         ::testing::Values(TestParam{128 * 1024, false}, TestParam{16, false},
                                           TestParam{16, true}));

TEST_F(OrcFileBatchReaderTest, TestSetReadSchema) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/f1=10/bucket-1/"
                            "data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc";
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, file_system->Open(file_name));
    ASSERT_OK_AND_ASSIGN(auto in_stream,
                         OrcInputStreamImpl::Create(input_stream, DEFAULT_NATURAL_READ_SIZE));
    std::map<std::string, std::string> options = {{ORC_READ_ENABLE_LAZY_DECODING, "true"}};
    ASSERT_OK_AND_ASSIGN(
        auto orc_batch_reader,
        OrcFileBatchReader::Create(std::move(in_stream), pool_, options, batch_size_));
    // test GetFileSchema()
    ASSERT_OK_AND_ASSIGN(auto c_file_schema, orc_batch_reader->GetFileSchema());
    auto arrow_file_schema = arrow::ImportSchema(c_file_schema.get()).ValueOrDie();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    arrow::Schema file_schema(fields);
    ASSERT_TRUE(arrow_file_schema->Equals(file_schema));
    // NextBatch() without SetReadSchema(), will return data with
    // file schema
    ASSERT_OK_AND_ASSIGN(auto result_with_file_schema,
                         paimon::test::ReadResultCollector::CollectResult(orc_batch_reader.get()));
    auto expected_file_chunk_array = std::make_shared<arrow::ChunkedArray>(struct_array_);
    ASSERT_TRUE(result_with_file_schema->Equals(expected_file_chunk_array));
    // NextBatch() with SetReadSchema(), will return data with read schema
    arrow::Schema read_schema({fields[0], fields[2]});
    std::unique_ptr<ArrowSchema> c_read_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(read_schema, c_read_schema.get()).ok());
    ASSERT_OK(orc_batch_reader->SetReadSchema(c_read_schema.get(),
                                              /*predicate=*/nullptr,
                                              /*selection_bitmap=*/std::nullopt));
    auto expected_read_array =
        arrow::StructArray::Make({struct_array_->field(0), struct_array_->field(2)},
                                 {fields[0], fields[2]})
            .ValueOrDie();
    auto expected_read_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_read_array);
    ASSERT_OK_AND_ASSIGN(auto result_with_read_schema,
                         paimon::test::ReadResultCollector::CollectResult(orc_batch_reader.get()));
    ASSERT_TRUE(result_with_read_schema->Equals(expected_read_chunk_array));

    // NextBatch() with predicate
    auto predicate = PredicateBuilder::GreaterThan(
        /*field_index=*/3, /*field_name=*/"f2", FieldType::INT, Literal(2));
    c_read_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(read_schema, c_read_schema.get()).ok());
    ASSERT_OK(orc_batch_reader->SetReadSchema(c_read_schema.get(), predicate,
                                              /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(result_with_read_schema,
                         paimon::test::ReadResultCollector::CollectResult(orc_batch_reader.get()));
    ASSERT_FALSE(result_with_read_schema);
}

TEST_F(OrcFileBatchReaderTest, TestCreateRowReaderOptions) {
    auto orc_pool = std::make_shared<OrcMemoryPool>(pool_);
    {
        // read all fields && default options
        std::map<std::string, std::string> options;
        std::string orc_schema = "struct<col1:int,col2:double,col3:string>";
        std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(orc_schema);
        std::unique_ptr<::orc::Type> target_type = ::orc::Type::buildTypeFromString(orc_schema);
        ASSERT_OK_AND_ASSIGN(auto row_reader_option, OrcFileBatchReader::CreateRowReaderOptions(
                                                         src_type.get(), target_type.get(),
                                                         /*search_arg=*/nullptr, options));
        ASSERT_EQ(std::list<std::string>({"col1", "col2", "col3"}),
                  row_reader_option.getIncludeNames());
        ASSERT_EQ(row_reader_option.getEnableLazyDecoding(), false);
    }
    {
        // read partial fields && set options
        std::map<std::string, std::string> options = {{ORC_READ_ENABLE_LAZY_DECODING, "true"}};
        std::string src_orc_schema = "struct<col1:int,col2:double,col3:string>";
        std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(src_orc_schema);
        std::string target_orc_schema = "struct<col1:int,col3:string>";
        std::unique_ptr<::orc::Type> target_type =
            ::orc::Type::buildTypeFromString(target_orc_schema);
        ASSERT_OK_AND_ASSIGN(auto row_reader_option, OrcFileBatchReader::CreateRowReaderOptions(
                                                         src_type.get(), target_type.get(),
                                                         /*search_arg=*/nullptr, options));
        ASSERT_EQ(std::list<std::string>({"col1", "col3"}), row_reader_option.getIncludeNames());
        ASSERT_EQ(row_reader_option.getEnableLazyDecoding(), true);
    }
    {
        // read partial fields, sequence mismatch
        std::map<std::string, std::string> options;
        std::string src_orc_schema = "struct<col1:int,col2:double,col3:string>";
        std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(src_orc_schema);
        std::string target_orc_schema = "struct<col3:string,col1:int>";
        std::unique_ptr<::orc::Type> target_type =
            ::orc::Type::buildTypeFromString(target_orc_schema);
        ASSERT_NOK_WITH_MSG(
            OrcFileBatchReader::CreateRowReaderOptions(src_type.get(), target_type.get(),
                                                       /*search_arg=*/nullptr, options),
            "The column id of the target field should be monotonically increasing in format "
            "reader");
    }
    {
        // read non exist column
        std::map<std::string, std::string> options;
        std::string src_orc_schema = "struct<col1:int,col2:double,col3:string>";
        std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(src_orc_schema);
        std::string target_orc_schema = "struct<col1:int,non_exist_col:double>";
        std::unique_ptr<::orc::Type> target_type =
            ::orc::Type::buildTypeFromString(target_orc_schema);
        ASSERT_NOK_WITH_MSG(
            OrcFileBatchReader::CreateRowReaderOptions(src_type.get(), target_type.get(),
                                                       /*search_arg=*/nullptr, options),
            "field non_exist_col not in file schema");
    }
    {
        std::map<std::string, std::string> options;
        std::string src_orc_schema =
            "struct<col1:struct<sub1:int,sub2:array<array<double>>,sub3:int>,col2:double,col3:"
            "map<string,int>>";
        std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(src_orc_schema);
        std::string target_orc_schema =
            "struct<col1:struct<sub1:int,sub2:array<array<double>>,sub3:int>,col3:map<string,int>>";
        std::unique_ptr<::orc::Type> target_type =
            ::orc::Type::buildTypeFromString(target_orc_schema);
        ASSERT_OK_AND_ASSIGN(auto row_reader_option, OrcFileBatchReader::CreateRowReaderOptions(
                                                         src_type.get(), target_type.get(),
                                                         /*search_arg=*/nullptr, options));
        ASSERT_EQ(std::list<std::string>({"col1", "col3"}), row_reader_option.getIncludeNames());
    }
    {
        // read with type mismatch
        std::map<std::string, std::string> options;
        std::string src_orc_schema =
            "struct<col1:struct<sub1:int,sub2:int,sub3:int>,col2:double,col3:string>";
        std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(src_orc_schema);
        std::string target_orc_schema =
            "struct<col1:struct<sub1:int,sub2:double,sub3:int>,col2:double,col3:string>";
        std::unique_ptr<::orc::Type> target_type =
            ::orc::Type::buildTypeFromString(target_orc_schema);

        ASSERT_NOK_WITH_MSG(
            OrcFileBatchReader::CreateRowReaderOptions(src_type.get(), target_type.get(),
                                                       /*search_arg=*/nullptr, options),
            "target_type "
            "struct<col1:struct<sub1:int,sub2:double,sub3:int>,col2:double,col3:string> not match "
            "src_type struct<col1:struct<sub1:int,sub2:int,sub3:int>,col2:double,col3:string>, "
            "mismatch field name col1");
    }
}

TEST_P(OrcFileBatchReaderTest, TestNextBatchSimple) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/f1=10/bucket-1/"
                            "data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc";
    auto data_type = struct_array_->struct_type();
    arrow::Schema read_schema(data_type->fields());
    auto [natural_read_size, _] = GetParam();
    for (auto batch_size : {1, 2, 3, 5, 8, 10}) {
        auto orc_batch_reader =
            PrepareOrcFileBatchReader(file_name, &read_schema, batch_size, natural_read_size);
        ASSERT_EQ(orc_batch_reader->GetPreviousBatchFirstRowNumber(), -1);
        ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                    orc_batch_reader.get()));
        ASSERT_EQ(orc_batch_reader->GetPreviousBatchFirstRowNumber(), 8);
        orc_batch_reader->Close();
        auto expected_array = std::make_shared<arrow::ChunkedArray>(struct_array_);
        ASSERT_TRUE(result_array->Equals(expected_array));
        // test metrics
        auto reader_metrics = orc_batch_reader->GetReaderMetrics();
        ASSERT_OK_AND_ASSIGN(uint64_t io_count,
                             reader_metrics->GetCounter(OrcMetrics::READ_IO_COUNT));
        ASSERT_GT(io_count, 0);
        ASSERT_OK_AND_ASSIGN(uint64_t latency,
                             reader_metrics->GetCounter(OrcMetrics::READ_INCLUSIVE_LATENCY_US));
        ASSERT_GT(latency, 0);
    }
}

TEST_P(OrcFileBatchReaderTest, TestNextBatchWithTargetSchema) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/f1=10/bucket-1/"
                            "data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc";
    // read without f2
    auto data_type = struct_array_->struct_type();
    arrow::FieldVector fields = {data_type->GetFieldByName("f0"), data_type->GetFieldByName("f1"),
                                 data_type->GetFieldByName("f3")};
    auto [natural_read_size, _] = GetParam();
    arrow::Schema read_schema(fields);

    auto orc_batch_reader =
        PrepareOrcFileBatchReader(file_name, &read_schema, batch_size_, natural_read_size);
    ASSERT_OK_AND_ASSIGN(auto result_array,
                         paimon::test::ReadResultCollector::CollectResult(orc_batch_reader.get()));
    auto expected_array = std::make_shared<arrow::ChunkedArray>(
        arrow::StructArray::Make(
            {struct_array_->GetFieldByName("f0"), struct_array_->GetFieldByName("f1"),
             struct_array_->GetFieldByName("f3")},
            fields)
            .ValueOrDie());
    ASSERT_TRUE(result_array->Equals(expected_array));
}

TEST_F(OrcFileBatchReaderTest, TestNextBatchWithOutofOrderTargetSchema) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/f1=10/bucket-1/"
                            "data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc";
    // file type is f0, f1, f2, f3
    // read with f3, f1, f0
    auto data_type = struct_array_->struct_type();
    arrow::FieldVector fields = {data_type->GetFieldByName("f3"), data_type->GetFieldByName("f1"),
                                 data_type->GetFieldByName("f0")};
    arrow::Schema read_schema(fields);
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, file_system->Open(file_name));
    ASSERT_OK_AND_ASSIGN(auto in_stream,
                         OrcInputStreamImpl::Create(input_stream, DEFAULT_NATURAL_READ_SIZE));
    std::map<std::string, std::string> options = {{ORC_READ_ENABLE_LAZY_DECODING, "true"}};
    ASSERT_OK_AND_ASSIGN(
        auto orc_batch_reader,
        OrcFileBatchReader::Create(std::move(in_stream), pool_, options, batch_size_));
    std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(read_schema, c_schema.get()).ok());
    ASSERT_NOK_WITH_MSG(orc_batch_reader->SetReadSchema(c_schema.get(), /*predicate=*/nullptr,
                                                        /*selection_bitmap=*/std::nullopt),
                        "The column id of the target field should be "
                        "monotonically increasing in "
                        "format reader");
}

TEST_P(OrcFileBatchReaderTest, TestNextBatchWithNullValue) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/f1=20/bucket-0/"
                            "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc";
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto arrow_data_type = arrow::struct_(fields);
    arrow::Schema read_schema(fields);
    auto [natural_read_size, _] = GetParam();
    auto orc_batch_reader =
        PrepareOrcFileBatchReader(file_name, &read_schema, batch_size_, natural_read_size);
    ASSERT_OK_AND_ASSIGN(auto result_array,
                         paimon::test::ReadResultCollector::CollectResult(orc_batch_reader.get()));
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    std::string json = R"([
      ["Paul", 20, 1, null]
    ])";
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {json}, &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(expected_array));
}

TEST_F(OrcFileBatchReaderTest, TestNextBatchWithDictionary) {
    auto f0 = arrow::field("f0", arrow::list(arrow::utf8()));
    auto f1 = arrow::field("f1", arrow::map(arrow::utf8(), arrow::binary()));
    auto f2 = arrow::field(
        "f2", arrow::struct_({field("sub1", arrow::int64()), field("sub2", arrow::binary()),
                              field("sub3", arrow::utf8())}));

    arrow::FieldVector fields = {f0, f1, f2};
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [["a", "a", "b"], [["a", "q"], ["b", "w"]],             [10, "q", "a"]],
        [["a", "c"],      [["a", "e"], ["b", "r"], ["c", "e"]], [20, "w", "a"]],
        [["a", "d"],      [["d", "r"], ["e", "t"]],             [null, "e", "b"]],
        [["a"],           [["a", "q"]],                         [null, "w", "c"]],
        [null,            [["a", "w"], ["f", "y"]],             [50, "r", null]],
        [["a"],           null,                                 [60, "r", "b"]],
        [["a", "b", "e"], [["a", null], ["b", "w"]],            null]
    ])")
            .ValueOrDie());

    arrow::FieldVector read_fields = {f0, f1, f2};
    auto read_schema = arrow::schema(read_fields);
    auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(src_array);
    auto check_result = [&](double dict_key_size_threshold, bool enable_lazy_decoding) {
        auto [orc_reader_holder, target_array] = ReadBatchWithCustomizedData(
            src_array, /*write_batch_size=*/src_array->length(), /*write_stripe_size=*/-1,
            /*write_row_index_stride=*/-1, read_schema.get(), /*predicate=*/nullptr,
            /*selection_bitmap=*/std::nullopt, 10, dict_key_size_threshold, enable_lazy_decoding);
        ASSERT_TRUE(target_array->Equals(expected_chunk_array));
        ASSERT_TRUE(expected_chunk_array->Equals(target_array));
    };
    // touch all conditions
    check_result(0.9, true);
    check_result(0.1, true);
    check_result(0.9, false);
    check_result(0.1, false);
}

TEST_P(OrcFileBatchReaderTest, TestComplexType) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_complex_data.db/append_complex_data/f1=10/bucket-0/"
                            "data-14a30421-7650-486c-9876-66a1fa4356ff-0.orc";
    arrow::FieldVector fields = {arrow::field("f1", arrow::int32()),
                                 arrow::field("f2", arrow::int32()),
                                 arrow::field("f3", arrow::date32()),
                                 arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)),
                                 arrow::field("f5", arrow::decimal128(23, 5)),
                                 arrow::field("f6", arrow::binary())};
    auto arrow_data_type = arrow::struct_(fields);
    arrow::Schema read_schema(fields);
    auto [natural_read_size, _] = GetParam();
    auto orc_batch_reader =
        PrepareOrcFileBatchReader(file_name, &read_schema, batch_size_, natural_read_size);
    ASSERT_OK_AND_ASSIGN(auto result_array,
                         paimon::test::ReadResultCollector::CollectResult(orc_batch_reader.get()));
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [10, 1, 1234,  "2033-05-18 03:33:20.0",         "123456789987654321.45678", "add"],
        [10, 1, 19909, "2033-05-18 03:33:20.000001001", "12.30000", "cat"],
        [10, 1, 0,     "2008-12-28 00:00:00.000123456", null, "dad"],
        [10, 1, 100,   "2008-12-28 00:00:00.00012345",  "-123.45000", "eat"],
        [10, 1, null,  "1899-01-01 00:59:20.001001001", "0.00000", "fat"],
        [10, 1, 20006, "2024-10-10 10:10:10.1001001",   "1728551410100.10010", null]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(*expected_array));
}

TEST_F(OrcFileBatchReaderTest, TestGetFileSchemaWithFieldId) {
    auto get_file_schema = [&](const std::string& file_name) -> std::shared_ptr<arrow::Schema> {
        std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             file_system->Open(file_name));
        EXPECT_TRUE(input_stream);
        EXPECT_OK_AND_ASSIGN(auto in_stream,
                             OrcInputStreamImpl::Create(input_stream, DEFAULT_NATURAL_READ_SIZE));
        EXPECT_TRUE(in_stream);
        std::map<std::string, std::string> options = {{ORC_READ_ENABLE_LAZY_DECODING, "true"}};
        EXPECT_OK_AND_ASSIGN(
            auto orc_batch_reader,
            OrcFileBatchReader::Create(std::move(in_stream), pool_, options, batch_size_));
        EXPECT_TRUE(orc_batch_reader);
        auto c_file_schema = orc_batch_reader->GetFileSchema();
        EXPECT_TRUE(c_file_schema.ok());
        auto arrow_file_schema = arrow::ImportSchema(c_file_schema.value().get());
        EXPECT_TRUE(arrow_file_schema.ok());
        return arrow_file_schema.ValueOrDie();
    };
    {
        // test file without field id
        std::string file_name = paimon::test::GetDataDir() +
                                "/orc/append_09.db/append_09/f1=10/bucket-1/"
                                "data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc";
        auto file_schema = get_file_schema(file_name);
        ASSERT_NOK(DataField::ConvertArrowSchemaToDataFields(file_schema));
    }
    {
        // test file with field id, simple schema
        std::string file_name = paimon::test::GetDataDir() +
                                "/orc/append_table_with_alter_table_build_in_fieldid.db/"
                                "append_table_with_alter_table_build_in_fieldid/key0=0/"
                                "key1=1/bucket-0/"
                                "data-35e7027e-b12a-4ebf-ae15-4c0fe8d6a895-0.orc";
        auto file_schema = get_file_schema(file_name);
        ASSERT_OK_AND_ASSIGN(auto data_fields,
                             DataField::ConvertArrowSchemaToDataFields(file_schema));
        std::vector<DataField> expected_data_fields = {
            DataField(0, arrow::field("key0", arrow::int32())),
            DataField(1, arrow::field("key1", arrow::int32())),
            DataField(2, arrow::field("a", arrow::int32())),
            DataField(3, arrow::field("b", arrow::int32())),
            DataField(4, arrow::field("c", arrow::int32())),
            DataField(5, arrow::field("d", arrow::int32())),
            DataField(6, arrow::field("k", arrow::int32()))};
        ASSERT_EQ(data_fields, expected_data_fields);
    }
    {
        // test file with field id, complex schema
        std::string file_name = paimon::test::GetDataDir() +
                                "/orc/append_complex_build_in_fieldid.db/"
                                "append_complex_build_in_fieldid/bucket-0/"
                                "data-6dac9052-36d8-4950-8f74-b2bbc082e489-0.orc";
        auto file_schema = get_file_schema(file_name);
        ASSERT_OK_AND_ASSIGN(auto data_fields,
                             DataField::ConvertArrowSchemaToDataFields(file_schema));
        // map type will lose meta info in c <-> c++ bridge (see
        // TestBridgeForMapType in OrcAdapterTest
        auto map_type = arrow::map(arrow::int8(), arrow::int16());
        auto list_type = arrow::list(DataField::ConvertDataFieldToArrowField(
            DataField(536871936, arrow::field("item", arrow::float32()))));
        std::vector<DataField> struct_fields = {DataField(3, arrow::field("f0", arrow::boolean())),
                                                DataField(4, arrow::field("f1", arrow::int64()))};
        auto struct_type = DataField::ConvertDataFieldsToArrowStructType(struct_fields);
        std::vector<DataField> expected_data_fields = {
            DataField(0, arrow::field("f1", map_type)),
            DataField(1, arrow::field("f2", list_type)),
            DataField(2, arrow::field("f3", struct_type)),
            DataField(5, arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO))),
            DataField(6, arrow::field("f5", arrow::date32())),
            DataField(7, arrow::field("f6", arrow::decimal128(2, 2)))};
        ASSERT_EQ(data_fields, expected_data_fields);
    }
}

TEST_F(OrcFileBatchReaderTest, TestDictionaryWithMultiStripe) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    auto arrow_type = arrow::struct_(fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_type, R"([
        ["abc"],
        ["abc"],
        ["abc"],
        ["de"],
        ["de"],
        ["de"]
    ])")
            .ValueOrDie());
    auto src_schema = arrow::schema(fields);
    // force generate two stripe in one file
    auto [orc_reader_holder, target_array] = ReadBatchWithCustomizedData(
        src_array, /*write_batch_size=*/3, /*write_stripe_size=*/3,
        /*write_row_index_stride=*/3, /*read_schema=*/src_schema.get(), /*predicate=*/nullptr,
        /*selection_bitmap=*/std::nullopt, /*read_batch_size=*/10,
        /*dict_key_size_threshold=*/0.9,
        /*enable_lazy_decoding=*/true);
    auto expected_array = arrow::ChunkedArray::Make({src_array}).ValueOrDie();
    ASSERT_TRUE(target_array->Equals(expected_array));
    ASSERT_TRUE(expected_array->Equals(target_array));
}

TEST_F(OrcFileBatchReaderTest, TestReadNoField) {
    // if only read partition fields, format reader will set empty read schema
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/f1=10/bucket-1/"
                            "data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc";
    // read no field
    arrow::Schema read_schema({});
    auto orc_batch_reader = PrepareOrcFileBatchReader(file_name, &read_schema, /*batch_size=*/3,
                                                      /*natural_read_size=*/10);
    // read 3 rows
    ASSERT_EQ(orc_batch_reader->GetPreviousBatchFirstRowNumber(), -1);
    ASSERT_OK_AND_ASSIGN(auto batch1, orc_batch_reader->NextBatch());
    ASSERT_EQ(orc_batch_reader->GetPreviousBatchFirstRowNumber(), 0);
    // read 3 rows
    ASSERT_OK_AND_ASSIGN(auto batch2, orc_batch_reader->NextBatch());
    ASSERT_EQ(orc_batch_reader->GetPreviousBatchFirstRowNumber(), 3);
    // read 2 rows
    ASSERT_OK_AND_ASSIGN(auto batch3, orc_batch_reader->NextBatch());
    ASSERT_EQ(orc_batch_reader->GetPreviousBatchFirstRowNumber(), 6);
    // read rows with eof
    ASSERT_OK_AND_ASSIGN(auto batch4, orc_batch_reader->NextBatch());
    ASSERT_EQ(orc_batch_reader->GetPreviousBatchFirstRowNumber(), 8);
    ASSERT_TRUE(BatchReader::IsEofBatch(batch4));
    orc_batch_reader->Close();

    arrow::FieldVector fields;
    auto arrow_type = arrow::struct_(fields);
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_type, R"([
[],
[],
[]
    ])")
            .ValueOrDie());

    auto check_batch = [](BatchReader::ReadBatch&& result_batch,
                          const std::shared_ptr<arrow::Array>& expected_array) {
        auto& [c_array, c_schema] = result_batch;
        auto result_array = arrow::ImportArray(c_array.get(), c_schema.get()).ValueOr(nullptr);
        ASSERT_TRUE(result_array);
        ASSERT_TRUE(result_array->Equals(expected_array));
    };

    check_batch(std::move(batch1), expected_array);
    check_batch(std::move(batch2), expected_array);
    check_batch(std::move(batch3), expected_array->Slice(0, 2));
}

TEST_P(OrcFileBatchReaderTest, TestTimestampType) {
    auto [natural_read_size, enable_tz] = GetParam();
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    std::string timezone_str = enable_tz ? "Asia/Tokyo" : timezone;
    paimon::test::TimezoneGuard tz_guard(timezone_str);
    arrow::FieldVector fields = {
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, timezone_str)),
        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, timezone_str)),
        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, timezone_str)),
        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, timezone_str)),
    };

    auto array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
["1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001", "1970-01-01 00:00:02", "1970-01-01 00:00:00.002", "1970-01-01 00:00:00.000002", "1970-01-01 00:00:00.000000002"],
["1970-01-01 00:00:03", "1970-01-01 00:00:00.003", null, "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000004"],
["1970-01-01 00:00:05", "1970-01-01 00:00:00.005", null, null, "1970-01-01 00:00:06", null, "1970-01-01 00:00:00.000006", null]
    ])")
            .ValueOrDie());
    auto arrow_schema = arrow::schema(fields);
    std::shared_ptr<arrow::ChunkedArray> expected_array =
        std::make_shared<arrow::ChunkedArray>(array);
    {
        // read data generated by Java Paimon
        std::string file_name = paimon::test::GetDataDir() +
                                "/orc/append_with_multiple_ts_precision_and_timezone.db/"
                                "append_with_multiple_ts_precision_and_timezone/bucket-0/"
                                "data-3f58c403-1672-49a3-93c0-d90cfff9bd8a-0.orc";
        auto orc_batch_reader = PrepareOrcFileBatchReader(file_name, arrow_schema.get(),
                                                          batch_size_, natural_read_size);
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::ChunkedArray> result_array,
            paimon::test::ReadResultCollector::CollectResult(orc_batch_reader.get()));
        ASSERT_TRUE(result_array->Equals(*expected_array)) << result_array->ToString() << std::endl
                                                           << expected_array->ToString();
    }
    {
        // read data generated by C++ Paimon
        auto dir = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        std::string data_path = dir->Str() + "/test.data";
        WriteArray(dir->GetFileSystem(), data_path, array, arrow_schema,
                   /*options=*/{{"orc.timestamp-ltz.legacy.type", "false"}});
        auto orc_batch_reader = PrepareOrcFileBatchReader(data_path, arrow_schema.get(),
                                                          batch_size_, natural_read_size);
        // check file schema
        ASSERT_OK_AND_ASSIGN(auto c_file_schema, orc_batch_reader->GetFileSchema());
        auto result_file_schema = arrow::ImportSchema(c_file_schema.get()).ValueOr(nullptr);
        ASSERT_TRUE(result_file_schema);

        arrow::FieldVector expected_fields = {
            arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::NANO)),
            arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::NANO)),
            arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::NANO)),
            arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
            arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::NANO, timezone_str)),
            arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::NANO, timezone_str)),
            arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::NANO, timezone_str)),
            arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, timezone_str)),
        };
        auto expected_file_schema = arrow::schema(expected_fields);
        ASSERT_TRUE(result_file_schema->Equals(expected_file_schema));

        // check array
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::ChunkedArray> result_array,
            paimon::test::ReadResultCollector::CollectResult(orc_batch_reader.get()));
        ASSERT_TRUE(result_array->Equals(expected_array)) << result_array->ToString();
    }
}

// TODO(liancheng.lsz): TestBitmapPushDownWithMultiRowGroups, TestPredicateAndBitmapPushDown
// TODO(liancheng.lsz): TestGenReadRanges
}  // namespace paimon::orc::test
