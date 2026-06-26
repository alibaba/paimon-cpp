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

#include "paimon/format/parquet/parquet_file_batch_reader.h"

#include <functional>
#include <iostream>
#include <limits>
#include <string>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/io/caching.h"
#include "arrow/io/interfaces.h"
#include "arrow/ipc/api.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/defs.h"
#include "paimon/format/parquet/parquet_field_id_converter.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/format/parquet/parquet_format_writer.h"
#include "paimon/format/parquet/parquet_reader_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/testing/utils/counting_cache_test_utils.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"
#include "paimon/utils/roaring_bitmap32.h"
#include "parquet/properties.h"

namespace paimon {
class Predicate;
}  // namespace paimon

namespace paimon::parquet::test {

std::string SerializeSchemaToString(const std::shared_ptr<arrow::Schema>& schema) {
    std::shared_ptr<arrow::Buffer> serialized = arrow::ipc::SerializeSchema(*schema).ValueOrDie();
    return std::string(reinterpret_cast<const char*>(serialized->data()),
                       static_cast<size_t>(serialized->size()));
}

class FailedUriInputStream : public InputStream {
 public:
    explicit FailedUriInputStream(const std::shared_ptr<InputStream>& input) : input_(input) {}

    Status Seek(int64_t offset, SeekOrigin origin) override {
        return input_->Seek(offset, origin);
    }

    Result<int64_t> GetPos() const override {
        return input_->GetPos();
    }

    Result<int64_t> Read(char* buffer, int64_t size) override {
        return input_->Read(buffer, size);
    }

    Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
        return input_->Read(buffer, size, offset);
    }

    void ReadAsync(char* buffer, int64_t size, int64_t offset,
                   std::function<void(Status)>&& callback) override {
        return input_->ReadAsync(buffer, size, offset, std::move(callback));
    }

    Result<std::string> GetUri() const override {
        return Status::Invalid("failed to get uri");
    }

    Result<int64_t> Length() const override {
        return input_->Length();
    }

    Status Close() override {
        return input_->Close();
    }

 private:
    std::shared_ptr<InputStream> input_;
};

class ParquetFileBatchReaderTest : public ::testing::Test,
                                   public ::testing::WithParamInterface<bool> {
 public:
    static std::shared_ptr<arrow::Field> WithMapSelectedKeys(
        const std::shared_ptr<arrow::Field>& field, const std::string& selected_keys) {
        auto metadata =
            field->metadata() ? field->metadata()->Copy() : arrow::key_value_metadata({});
        auto set_status = metadata->Set(DataField::MAP_SELECTED_KEYS, selected_keys);
        EXPECT_TRUE(set_status.ok()) << set_status.ToString();
        return field->WithMetadata(metadata);
    }

    static std::shared_ptr<arrow::Schema> MakeReadSchema(const arrow::FieldVector& fields) {
        return arrow::schema(fields);
    }

    void SetUp() override {
        dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        fs_ = std::make_shared<LocalFileSystem>();
        pool_ = GetArrowPool(GetDefaultPool());
        batch_size_ = 10;
        file_path_ = PathUtil::JoinPath(dir_->Str(), "test.parquet");

        arrow::FieldVector fields = {
            arrow::field("f1", arrow::boolean()),
            arrow::field("f2", arrow::int8()),
            arrow::field("f3", arrow::int16()),
            arrow::field("f4", arrow::int32()),
            arrow::field("f5", arrow::int64()),
            arrow::field("f6", arrow::float32()),
            arrow::field("f7", arrow::float64()),
            arrow::field("f8", arrow::utf8()),
            arrow::field("f9", arrow::binary()),
            arrow::field("f10", arrow::map(arrow::list(arrow::float32()),
                                           arrow::struct_({arrow::field("f0", arrow::boolean()),
                                                           arrow::field("f1", arrow::int64())}))),
            arrow::field("f11", arrow::timestamp(arrow::TimeUnit::NANO)),
            arrow::field("f12", arrow::date32()),
            arrow::field("f13", arrow::decimal128(2, 2))};

        schema_ = arrow::schema(fields);
        struct_array_ = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
[true, 31, null, 300001, 3000000001, 3.1, 300.000000001, "s31", "a31", [[[5.11, 5.21], [true, 61]]], "1970-01-01 00:00:00.000071", 81, "0.91"],
[false, 32, 302, 300002, 3000000002, 3.2, 300.000000002, "s32", "a32", [[[5.12, 5.22], [false, 62]]], "1970-01-01 00:00:00.000072", 82, "0.92"],
[true, 33, 303, 300003, 3000000003, 3.3, 300.000000003, "s33", "a33", null, "1970-01-01 00:00:00.000073", 83, "0.93"],
[false, 34, 304, 300004, 3000000004, 3.4, 300.000000004, "s34", "a34", [[[5.141, 5.241], [false, 641]], [[5.14, 5.24], [false, 64]]], "1970-01-01 00:00:00.000074", 84, "0.94"],
[true, 35, 305, 300005, 3000000005, 3.5, 300.000000005, "s35", "a35", [[[5.15, 5.25], [true, 65]]], "1970-01-01 00:00:00.000075", 85, "0.95"],
[false, 36, 306, 300006, 3000000006, 3.6, 300.000000006, "s36", "a36", [[[5.16, 5.26], null]], "1970-01-01 00:00:00.000076", 86, "0.96"]
    ])")
                .ValueOrDie());
    }

    void TearDown() override {}

    void WriteArray(const std::string& file_path, const std::shared_ptr<arrow::Array>& src_array,
                    const std::shared_ptr<arrow::Schema>& arrow_schema, int64_t write_batch_size,
                    bool enable_dictionary, int64_t max_row_group_length,
                    int64_t max_page_size = 1024 * 1024 * 1024) const {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                             fs_->Create(file_path, /*overwrite=*/true));
        ::parquet::WriterProperties::Builder builder;
        builder.write_batch_size(write_batch_size);
        builder.max_row_group_length(max_row_group_length);
        builder.data_pagesize(max_page_size);
        builder.enable_write_page_index();
        enable_dictionary ? builder.enable_dictionary() : builder.disable_dictionary();
        auto writer_properties = builder.build();
        ASSERT_OK_AND_ASSIGN(auto format_writer, ParquetFormatWriter::Create(
                                                     out, arrow_schema, writer_properties,
                                                     DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE, pool_));

        auto arrow_array = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*src_array, arrow_array.get()).ok());
        ASSERT_OK(format_writer->AddBatch(arrow_array.get()));
        ASSERT_OK(format_writer->Flush());
        ASSERT_OK(format_writer->Finish());
        ASSERT_OK(out->Flush());
        ASSERT_OK(out->Close());
    }

    std::unique_ptr<ParquetFileBatchReader> PrepareParquetFileBatchReader(
        const std::string& file_name, const std::shared_ptr<arrow::Schema>& read_schema,
        const std::shared_ptr<Predicate>& predicate,
        const std::optional<RoaringBitmap32>& selection_bitmap, int32_t batch_size) const {
        EXPECT_OK_AND_ASSIGN(auto input_stream, fs_->Open(file_name));
        auto length = fs_->GetFileStatus(file_name).value()->GetLen();
        auto in_stream =
            std::make_unique<ArrowInputStreamAdapter>(std::move(input_stream), pool_, length);
        std::map<std::string, std::string> options = {};
        return PrepareParquetFileBatchReader(std::move(in_stream), options, read_schema, predicate,
                                             selection_bitmap, batch_size);
    }

    std::unique_ptr<paimon::parquet::ParquetFileBatchReader> PrepareParquetFileBatchReader(
        std::unique_ptr<arrow::io::RandomAccessFile>&& in_stream,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<arrow::Schema>& read_schema,
        const std::shared_ptr<Predicate>& predicate,
        const std::optional<RoaringBitmap32>& selection_bitmap, int32_t batch_size) const {
        EXPECT_OK_AND_ASSIGN(
            auto parquet_batch_reader,
            ParquetFileBatchReader::Create(std::move(in_stream), options, batch_size,
                                           /*file_metadata=*/nullptr, pool_));
        std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
        auto arrow_status = arrow::ExportSchema(*read_schema, c_schema.get());
        EXPECT_TRUE(arrow_status.ok());
        EXPECT_OK(parquet_batch_reader->SetReadSchema(c_schema.get(), predicate, selection_bitmap));
        return parquet_batch_reader;
    }

 protected:
    std::string file_path_;
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<arrow::MemoryPool> pool_;
    int32_t batch_size_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<arrow::StructArray> struct_array_;
};

static std::shared_ptr<arrow::StructArray> MakeSequentialIntData(int32_t num_rows) {
    arrow::Int32Builder val_builder;
    EXPECT_TRUE(val_builder.Reserve(num_rows).ok());
    for (int32_t i = 0; i < num_rows; ++i) {
        val_builder.UnsafeAppend(i);
    }
    auto val_array = val_builder.Finish().ValueOrDie();
    auto field = arrow::field("f0", arrow::int32());
    return arrow::StructArray::Make({val_array}, {field}).ValueOrDie();
}

TEST_F(ParquetFileBatchReaderTest, TestParquetMetadataCacheReusesSerializedFooter) {
    WriteArray(file_path_, struct_array_, schema_, /*write_batch_size=*/struct_array_->length(),
               /*enable_dictionary=*/false,
               /*max_row_group_length=*/struct_array_->length());

    auto cache = std::make_shared<paimon::test::CountingRoutingCache>(CacheKind::DATA_FILE_FOOTER,
                                                                      128 * 1024 * 1024);
    auto open_reader = [&]() -> Result<std::unique_ptr<ParquetFileBatchReader>> {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input_stream, fs_->Open(file_path_));
        std::map<std::string, std::string> options;
        ParquetReaderBuilder builder(options, batch_size_);
        builder.WithMemoryPool(GetDefaultPool())->WithCache(cache);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileBatchReader> reader,
                               builder.Build(input_stream));
        auto parquet_reader = dynamic_cast<ParquetFileBatchReader*>(reader.release());
        if (parquet_reader == nullptr) {
            return Status::Invalid("failed to cast FileBatchReader to ParquetFileBatchReader");
        }
        return std::unique_ptr<ParquetFileBatchReader>(parquet_reader);
    };

    ASSERT_OK_AND_ASSIGN(auto reader1, open_reader());
    ASSERT_OK_AND_ASSIGN(auto schema1, reader1->GetFileSchema());
    ASSERT_TRUE(schema1);
    ASSERT_TRUE(schema1->release);
    schema1->release(schema1.get());
    ASSERT_EQ(1, cache->GetCount());
    ASSERT_EQ(1, cache->SupplierCallCount());
    ASSERT_EQ(1, cache->Size());
    ASSERT_EQ(CacheKind::DATA_FILE_FOOTER, cache->LastKind());

    ASSERT_OK_AND_ASSIGN(auto reader2, open_reader());
    ASSERT_OK_AND_ASSIGN(auto schema2, reader2->GetFileSchema());
    ASSERT_TRUE(schema2);
    ASSERT_TRUE(schema2->release);
    schema2->release(schema2.get());
    ASSERT_EQ(2, cache->GetCount());
    ASSERT_EQ(1, cache->SupplierCallCount());
    ASSERT_EQ(1, cache->Size());
    ASSERT_EQ(CacheKind::DATA_FILE_FOOTER, cache->LastKind());
}

TEST_F(ParquetFileBatchReaderTest, TestParquetMetadataCacheBypassesWhenGetUriFails) {
    WriteArray(file_path_, struct_array_, schema_, /*write_batch_size=*/struct_array_->length(),
               /*enable_dictionary=*/false,
               /*max_row_group_length=*/struct_array_->length());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, fs_->Open(file_path_));
    auto failed_uri_input_stream = std::make_shared<FailedUriInputStream>(input_stream);
    auto cache = std::make_shared<paimon::test::CountingRoutingCache>(CacheKind::DATA_FILE_FOOTER,
                                                                      128 * 1024 * 1024);

    std::map<std::string, std::string> options;
    ParquetReaderBuilder builder(options, batch_size_);
    builder.WithMemoryPool(GetDefaultPool())->WithCache(cache);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader,
                         builder.Build(failed_uri_input_stream));
    auto parquet_reader = dynamic_cast<ParquetFileBatchReader*>(reader.get());
    ASSERT_TRUE(parquet_reader);
    ASSERT_OK_AND_ASSIGN(auto file_schema, parquet_reader->GetFileSchema());
    ASSERT_TRUE(file_schema);
    ASSERT_TRUE(file_schema->release);
    file_schema->release(file_schema.get());

    ASSERT_EQ(0, cache->GetCount());
    ASSERT_EQ(0, cache->SupplierCallCount());
    ASSERT_EQ(0, cache->Size());
}

TEST_F(ParquetFileBatchReaderTest, TestReadBinaryWrittenFromBinaryAndLargeBinary) {
    auto check_binary_read_result = [&](const std::shared_ptr<arrow::DataType>& write_type,
                                        const std::string& file_name) {
        std::string data_json = R"([
        ["descriptor-1"],
        [""],
        [null],
        ["descriptor-2"]
    ])";
        auto write_field = arrow::field("f0", write_type);
        auto write_schema = arrow::schema({write_field});
        auto write_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({write_field}), data_json)
                .ValueOrDie());

        std::string file_path = PathUtil::JoinPath(dir_->Str(), file_name);
        WriteArray(file_path, write_array, write_schema, /*write_batch_size=*/write_array->length(),
                   /*enable_dictionary=*/false, /*max_row_group_length=*/write_array->length());

        auto read_field = arrow::field("f0", arrow::binary());
        auto read_schema = arrow::schema({read_field});
        auto parquet_batch_reader =
            PrepareParquetFileBatchReader(file_path, read_schema, /*predicate=*/nullptr,
                                          /*selection_bitmap=*/std::nullopt, batch_size_);

        ASSERT_OK_AND_ASSIGN(auto c_file_schema, parquet_batch_reader->GetFileSchema());
        auto file_schema = arrow::ImportSchema(c_file_schema.get()).ValueOrDie();
        ASSERT_TRUE(file_schema->Equals(*read_schema));

        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({read_field}), data_json)
                .ValueOrDie());
        auto expected_chunked_array = std::make_shared<arrow::ChunkedArray>(expected_array);
        ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                    parquet_batch_reader.get()));
        ASSERT_TRUE(result_array->Equals(expected_chunked_array));
    };

    check_binary_read_result(arrow::binary(), "binary.parquet");
    check_binary_read_result(arrow::large_binary(), "large-binary.parquet");
}

TEST_F(ParquetFileBatchReaderTest, TestSimple) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/parquet/parquet_append_table.db/parquet_append_table/bucket-0/"
                            "data-9ea62f34-1dca-49c1-bf7a-d37303d8fb76-0.parquet";
    auto parquet_batch_reader = PrepareParquetFileBatchReader(
        file_name, schema_, /*predicate=*/nullptr, /*selection_bitmap=*/std::nullopt, batch_size_);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::ChunkedArray> result_array,
        paimon::test::ReadResultCollector::CollectResult(parquet_batch_reader.get()));
    std::shared_ptr<arrow::ChunkedArray> expected_array =
        std::make_shared<arrow::ChunkedArray>(struct_array_);
    ASSERT_TRUE(result_array->Equals(*expected_array,
                                     arrow::EqualOptions::Defaults().diff_sink(&std::cout)));
}

TEST_F(ParquetFileBatchReaderTest, TestSetReadSchema) {
    std::string file_name = paimon::test::GetDataDir() +
                            "parquet/parquet_append_table.db/parquet_append_table/bucket-0/"
                            "data-9ea62f34-1dca-49c1-bf7a-d37303d8fb76-0.parquet";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, fs_->Open(file_name));
    auto length = fs_->GetFileStatus(file_name).value()->GetLen();
    auto in_stream =
        std::make_unique<ArrowInputStreamAdapter>(std::move(input_stream), pool_, length);
    std::map<std::string, std::string> options;
    ASSERT_OK_AND_ASSIGN(auto parquet_batch_reader,
                         ParquetFileBatchReader::Create(std::move(in_stream), options, batch_size_,
                                                        /*file_metadata=*/nullptr, pool_));
    // test GetFileSchema()
    ASSERT_OK_AND_ASSIGN(auto c_file_schema, parquet_batch_reader->GetFileSchema());
    auto arrow_file_schema = arrow::ImportSchema(c_file_schema.get()).ValueOrDie();
    ASSERT_TRUE(arrow_file_schema->Equals(*schema_));

    // NextBatch() without SetReadSchema(), will return data with
    // file schema
    ASSERT_OK_AND_ASSIGN(
        auto result_with_file_schema,
        paimon::test::ReadResultCollector::CollectResult(parquet_batch_reader.get()));
    auto expected_file_chunk_array = std::make_shared<arrow::ChunkedArray>(struct_array_);
    ASSERT_TRUE(result_with_file_schema->Equals(expected_file_chunk_array));
    // NextBatch() with SetReadSchema(), will return data with read schema
    arrow::Schema read_schema({schema_->field(0), schema_->field(9), schema_->field(12)});
    std::unique_ptr<ArrowSchema> c_read_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(read_schema, c_read_schema.get()).ok());
    ASSERT_OK(parquet_batch_reader->SetReadSchema(c_read_schema.get(),
                                                  /*predicate=*/nullptr,
                                                  /*selection_bitmap=*/std::nullopt));
    auto expected_read_array =
        arrow::StructArray::Make(
            {struct_array_->field(0), struct_array_->field(9), struct_array_->field(12)},
            read_schema.fields())
            .ValueOrDie();
    auto expected_read_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_read_array);
    ASSERT_OK_AND_ASSIGN(
        auto result_with_read_schema,
        paimon::test::ReadResultCollector::CollectResult(parquet_batch_reader.get()));
    ASSERT_TRUE(result_with_read_schema->Equals(expected_read_chunk_array));

    // NextBatch() with predicate
    auto predicate = PredicateBuilder::IsNull(
        /*field_index=*/0, /*field_name=*/"f1", FieldType::BOOLEAN);
    c_read_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(read_schema, c_read_schema.get()).ok());
    ASSERT_OK(parquet_batch_reader->SetReadSchema(c_read_schema.get(), predicate,
                                                  /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(result_with_read_schema, paimon::test::ReadResultCollector::CollectResult(
                                                      parquet_batch_reader.get()));
    ASSERT_FALSE(result_with_read_schema);
}

TEST_F(ParquetFileBatchReaderTest, TestSetReadSchemaWithLegacyParquetMissingFieldIds) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/parquet/append_09.db/append_09/f1=20/bucket-0/"
                            "data-b446f78a-2cfb-4b3b-add8-31295d24a277-0.parquet";

    std::vector<DataField> read_fields = {
        DataField(0, arrow::field("f0", arrow::utf8())),
        DataField(2, arrow::field("f2", arrow::int32())),
        DataField(3, arrow::field("f3", arrow::float64())),
    };
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto parquet_batch_reader =
        PrepareParquetFileBatchReader(file_name, read_schema, /*predicate=*/nullptr,
                                      /*selection_bitmap=*/std::nullopt, batch_size_);

    ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                parquet_batch_reader.get()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    arrow::struct_(read_schema->fields()), {R"([
        ["Lucy", 1, 14.1]
    ])"},
                    &expected_array)
                    .ok());
    ASSERT_TRUE(result_array->Equals(expected_array))
        << "expected: " << expected_array->ToString() << "\nactual: " << result_array->ToString();
}

TEST_F(ParquetFileBatchReaderTest, TestNextBatchSimple) {
    std::string file_name = paimon::test::GetDataDir() +
                            "parquet/parquet_append_table.db/parquet_append_table/bucket-0/"
                            "data-9ea62f34-1dca-49c1-bf7a-d37303d8fb76-0.parquet";
    for (auto batch_size : {1, 2, 3, 5, 8, 10}) {
        auto parquet_batch_reader =
            PrepareParquetFileBatchReader(file_name, schema_, /*predicate=*/nullptr,
                                          /*selection_bitmap=*/std::nullopt, batch_size);
        ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                    parquet_batch_reader.get()));
        parquet_batch_reader->Close();
        auto expected_array = std::make_shared<arrow::ChunkedArray>(struct_array_);
        ASSERT_TRUE(result_array->Equals(expected_array));
        // test metrics
        auto read_metrics = parquet_batch_reader->GetReaderMetrics();
        ASSERT_TRUE(read_metrics);
        // TODO(jinli.zjw): test metrics
        // ASSERT_TRUE(read_metrics->GetCounter(ParquetMetrics::READ_BYTES) > 0);
        // ASSERT_TRUE(read_metrics->GetCounter(ParquetMetrics::READ_RAW_BYTES) > 0);
    }
}

TEST_F(ParquetFileBatchReaderTest, TestNextBatchWithTargetSchema) {
    std::string file_name = paimon::test::GetDataDir() +
                            "parquet/parquet_append_table.db/parquet_append_table/bucket-0/"
                            "data-9ea62f34-1dca-49c1-bf7a-d37303d8fb76-0.parquet";
    auto read_schema = arrow::schema(arrow::FieldVector(
        {schema_->field(4), schema_->field(9), schema_->field(10), schema_->field(12)}));
    auto parquet_batch_reader =
        PrepareParquetFileBatchReader(file_name, read_schema, /*predicate=*/nullptr,
                                      /*selection_bitmap=*/std::nullopt, batch_size_);
    ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                parquet_batch_reader.get()));
    parquet_batch_reader->Close();
    auto expected_read_array =
        arrow::StructArray::Make({struct_array_->field(4), struct_array_->field(9),
                                  struct_array_->field(10), struct_array_->field(12)},
                                 read_schema->fields())
            .ValueOrDie();
    auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_read_array);
    ASSERT_TRUE(result_array->Equals(expected_chunk_array));
}

TEST_F(ParquetFileBatchReaderTest, TestNextBatchWithOutofOrderTargetSchema) {
    std::string file_name = paimon::test::GetDataDir() +
                            "parquet/parquet_append_table.db/parquet_append_table/bucket-0/"
                            "data-9ea62f34-1dca-49c1-bf7a-d37303d8fb76-0.parquet";
    // read with f13, f11, f10, f5
    auto read_schema = arrow::schema(arrow::FieldVector(
        {schema_->field(12), schema_->field(10), schema_->field(9), schema_->field(4)}));
    auto parquet_batch_reader =
        PrepareParquetFileBatchReader(file_name, read_schema, /*predicate=*/nullptr,
                                      /*selection_bitmap=*/std::nullopt, batch_size_);
    ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                parquet_batch_reader.get()));
    parquet_batch_reader->Close();
    auto expected_read_array =
        arrow::StructArray::Make({struct_array_->field(12), struct_array_->field(10),
                                  struct_array_->field(9), struct_array_->field(4)},
                                 read_schema->fields())
            .ValueOrDie();
    auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_read_array);
    ASSERT_TRUE(result_array->Equals(expected_chunk_array));
}

TEST_F(ParquetFileBatchReaderTest, TestNextBatchWithDictionary) {
    auto f0 = arrow::field("f0", arrow::list(arrow::utf8()));
    auto f1 = arrow::field("f1", arrow::map(arrow::utf8(), arrow::binary()));
    auto f2 = arrow::field(
        "f2", arrow::struct_({field("sub1", arrow::int64()), field("sub2", arrow::binary()),
                              field("sub3", arrow::utf8())}));

    arrow::FieldVector fields = {f0, f1, f2};
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [["a", "a", "b"], [["a", "q"], ["b", "w"]],             [10, "q", "a"]],
        [["a", "c"],      [["a", "e"], ["b", "r"], ["c", "e"]], [20, "w", "a"]],
        [["a", "d"],      [["d", "r"], ["e", "t"]],             [null, "e", "b"]],
        [["a"],           [["a", "q"]],                         [null, "w", "c"]],
        [null,            [["a", "w"], ["f", "y"]],             [50, "r", null]],
        [["a"],           null,                                 [60, "r", "b"]],
        [["a", "b", "e"], [["a", null], ["b", "w"]],            null]
    ])")
            .ValueOrDie());
    auto src_schema = arrow::schema(fields);
    auto arrow_schema = arrow::schema(fields);
    auto expected_array = arrow::ChunkedArray::Make({src_array}).ValueOrDie();

    auto check_result = [&](bool enable_dictionary) {
        WriteArray(file_path_, src_array, arrow_schema, /*write_batch_size=*/1, enable_dictionary,
                   /*max_row_group_length=*/3);
        auto parquet_batch_reader =
            PrepareParquetFileBatchReader(file_path_, arrow_schema, /*predicate=*/nullptr,
                                          /*selection_bitmap=*/std::nullopt, /*batch_size=*/2);
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::ChunkedArray> result_array,
            paimon::test::ReadResultCollector::CollectResult(parquet_batch_reader.get()));
        ASSERT_TRUE(result_array->Equals(expected_array)) << result_array->ToString();
    };
    check_result(true);
    check_result(false);
}

TEST_F(ParquetFileBatchReaderTest, TestNestedStructChildProjectionRecall) {
    auto f0 = arrow::field("f0", arrow::int32());
    auto f1 = arrow::field(
        "f1", arrow::struct_({arrow::field("c0", arrow::int64()), arrow::field("c1", arrow::utf8()),
                              arrow::field("c2", arrow::float64())}));
    auto f2 = arrow::field("f2", arrow::utf8());

    auto write_schema = arrow::schema({f0, f1, f2});
    auto write_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(write_schema->fields()), R"([
        [1, [100, "a", 1.1], "x"],
        [2, [200, "b", 2.2], "y"],
        [3, [300, null, 3.3], "z"]
    ])")
            .ValueOrDie());

    WriteArray(file_path_, write_array, write_schema,
               /*write_batch_size=*/write_array->length(),
               /*enable_dictionary=*/false, /*max_row_group_length=*/write_array->length());

    auto read_schema = MakeReadSchema({
        f0,
        arrow::field("f1", arrow::struct_({arrow::field("c1", arrow::utf8())})),
    });

    auto parquet_batch_reader =
        PrepareParquetFileBatchReader(file_path_, read_schema, /*predicate=*/nullptr,
                                      /*selection_bitmap=*/std::nullopt, /*batch_size=*/2);

    ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                parquet_batch_reader.get()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    arrow::struct_(read_schema->fields()), {R"([
        [1, ["a"]],
        [2, ["b"]],
        [3, [null]]
    ])"},
                    &expected_array)
                    .ok());

    ASSERT_TRUE(result_array->Equals(expected_array))
        << "expected: " << expected_array->ToString() << "\nactual: " << result_array->ToString();
}

TEST_F(ParquetFileBatchReaderTest, TestReadSchemaWithMapSelectedKeysMetadata) {
    auto id_field = arrow::field("id", arrow::int32());
    auto map_field = arrow::field("m", arrow::map(arrow::utf8(), arrow::int32()));

    auto write_schema = arrow::schema({id_field, map_field});
    auto write_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(write_schema->fields()), R"([
        [1, [["k1", 10], ["k2", 20], ["k3", 30]]],
        [2, [["k2", 200]]],
        [3, null]
    ])")
            .ValueOrDie());

    WriteArray(file_path_, write_array, write_schema,
               /*write_batch_size=*/write_array->length(),
               /*enable_dictionary=*/false, /*max_row_group_length=*/write_array->length());

    // selected-keys metadata is consumed by upper-level field mapping; format reader should
    // still accept the schema and read data correctly.
    auto read_schema = MakeReadSchema(
        {id_field, WithMapSelectedKeys(map_field, "k1,k3")});  // NOLINT(whitespace/comma)

    auto parquet_batch_reader =
        PrepareParquetFileBatchReader(file_path_, read_schema, /*predicate=*/nullptr,
                                      /*selection_bitmap=*/std::nullopt, /*batch_size=*/2);

    ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                parquet_batch_reader.get()));
    auto expected_array = arrow::ChunkedArray::Make({write_array}).ValueOrDie();
    ASSERT_TRUE(result_array->Equals(expected_array))
        << "expected: " << expected_array->ToString() << "\nactual: " << result_array->ToString();
}

TEST_F(ParquetFileBatchReaderTest, TestGetFileSchemaWithFieldId) {
    std::string file_name = paimon::test::GetDataDir() +
                            "parquet/parquet_append_table.db/parquet_append_table/bucket-0/"
                            "data-9ea62f34-1dca-49c1-bf7a-d37303d8fb76-0.parquet";
    auto parquet_batch_reader = PrepareParquetFileBatchReader(
        file_name, schema_, /*predicate=*/nullptr, /*selection_bitmap=*/std::nullopt, batch_size_);
    ASSERT_OK_AND_ASSIGN(auto c_file_schema, parquet_batch_reader->GetFileSchema());
    auto arrow_file_schema = arrow::ImportSchema(c_file_schema.get()).ValueOr(nullptr);
    ASSERT_TRUE(arrow_file_schema);
    ASSERT_OK_AND_ASSIGN(auto data_fields,
                         DataField::ConvertArrowSchemaToDataFields(arrow_file_schema));

    auto list_type = arrow::list(DataField::ConvertDataFieldToArrowField(
        DataField(536880129, arrow::field("item", arrow::float32()))));
    std::vector<DataField> struct_fields = {DataField(10, arrow::field("f0", arrow::boolean())),
                                            DataField(11, arrow::field("f1", arrow::int64()))};
    auto struct_type = DataField::ConvertDataFieldsToArrowStructType(struct_fields);
    auto map_type = arrow::map(list_type, struct_type);

    std::vector<DataField> expected_data_fields = {
        DataField(0, arrow::field("f1", arrow::boolean())),
        DataField(1, arrow::field("f2", arrow::int8())),
        DataField(2, arrow::field("f3", arrow::int16())),
        DataField(3, arrow::field("f4", arrow::int32())),
        DataField(4, arrow::field("f5", arrow::int64())),
        DataField(5, arrow::field("f6", arrow::float32())),
        DataField(6, arrow::field("f7", arrow::float64())),
        DataField(7, arrow::field("f8", arrow::utf8())),
        DataField(8, arrow::field("f9", arrow::binary())),
        DataField(9, arrow::field("f10", map_type)),
        DataField(12, arrow::field("f11", arrow::timestamp(arrow::TimeUnit::NANO))),
        DataField(13, arrow::field("f12", arrow::date32())),
        DataField(14, arrow::field("f13", arrow::decimal128(2, 2))),
    };
    ASSERT_EQ(data_fields, expected_data_fields);
}

TEST_F(ParquetFileBatchReaderTest, TestCreateReaderProperties) {
    {
        // test default options
        std::map<std::string, std::string> options;
        ASSERT_OK_AND_ASSIGN(auto reader_properties,
                             ParquetFileBatchReader::CreateReaderProperties(pool_, options));
        ASSERT_EQ(reader_properties.is_buffered_stream_enabled(), true);
    }
}

TEST_F(ParquetFileBatchReaderTest, TestCreateArrowReaderProperties) {
    {
        // test default options
        std::map<std::string, std::string> options;
        int32_t batch_size = 1024;
        ASSERT_OK_AND_ASSIGN(
            auto arrow_reader_properties,
            ParquetFileBatchReader::CreateArrowReaderProperties(pool_, options, batch_size));
        ASSERT_EQ(arrow_reader_properties.pre_buffer(), true);
        ASSERT_EQ(arrow_reader_properties.batch_size(), 1024);
        ASSERT_EQ(arrow_reader_properties.use_threads(), true);
        ASSERT_EQ(arrow::GetCpuThreadPoolCapacity(), 3);
        ASSERT_EQ(arrow_reader_properties.cache_options(), arrow::io::CacheOptions::Defaults());
    }
    {
        std::map<std::string, std::string> options = {{PARQUET_READ_EXECUTOR_THREAD_COUNT, "0"}};
        int32_t batch_size = 1024;
        ASSERT_OK_AND_ASSIGN(
            auto arrow_reader_properties,
            ParquetFileBatchReader::CreateArrowReaderProperties(pool_, options, batch_size));
        ASSERT_EQ(arrow_reader_properties.use_threads(), false);
    }
    {
        std::map<std::string, std::string> options = {{PARQUET_READ_EXECUTOR_THREAD_COUNT, "6"}};
        int32_t batch_size = 1024;
        ASSERT_OK_AND_ASSIGN(
            auto arrow_reader_properties,
            ParquetFileBatchReader::CreateArrowReaderProperties(pool_, options, batch_size));
        ASSERT_EQ(arrow_reader_properties.use_threads(), true);
        ASSERT_EQ(arrow::GetCpuThreadPoolCapacity(), 6);
    }
}

TEST_F(ParquetFileBatchReaderTest, TestBitmapPushDownWithMultiRowGroups) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::int32())};
    auto arrow_type = arrow::struct_(fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_type, R"([
        [0],
        [1],
        [2],
        [3],
        [4],
        [5],
        [6],
        [7],
        [8],
        [9],
        [10],
        [11]
    ])")
            .ValueOrDie());
    auto src_schema = arrow::schema(fields);
    std::optional<RoaringBitmap32> bitmap = RoaringBitmap32::From({3, 5});
    // data in file rowGroup0:[0, 1, 2, 3, 4, 5] | rowGroup1:[6, 7, 8, 9, 10, 11]

    auto arrow_schema = arrow::schema(fields);
    WriteArray(file_path_, src_array, arrow_schema, /*write_batch_size=*/12,
               /*enable_dictionary=*/true,
               /*max_row_group_length=*/6);

    auto parquet_batch_reader = PrepareParquetFileBatchReader(
        file_path_, arrow_schema, /*predicate=*/nullptr, bitmap, /*batch_size=*/12);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::ChunkedArray> result_array,
        paimon::test::ReadResultCollector::CollectResult(parquet_batch_reader.get()));

    auto expected_array = arrow::ChunkedArray::Make({src_array->Slice(0, 6)}).ValueOrDie();
    ASSERT_TRUE(result_array->Equals(expected_array)) << result_array->ToString();
}

TEST_F(ParquetFileBatchReaderTest, TestPredicateAndBitmapPushDown) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::int32())};
    auto arrow_type = arrow::struct_(fields);
    arrow::StructBuilder struct_builder(arrow_type, arrow::default_memory_pool(),
                                        {std::make_shared<arrow::Int32Builder>()});
    auto int_builder = static_cast<arrow::Int32Builder*>(struct_builder.field_builder(0));
    int32_t length = 1024;
    for (int32_t i = 0; i < length; ++i) {
        ASSERT_TRUE(struct_builder.Append().ok());
        ASSERT_TRUE(int_builder->Append(i).ok());
    }
    // data file:
    // rowGroup0: [0, 256)
    // rowGroup1: [256, 512)
    // rowGroup2: [512, 768)
    // rowGroup3: [768, 1024)
    std::shared_ptr<arrow::Array> src_array;
    ASSERT_TRUE(struct_builder.Finish(&src_array).ok());
    auto src_schema = arrow::schema(fields);
    auto arrow_schema = arrow::schema(fields);
    WriteArray(file_path_, src_array, arrow_schema, /*write_batch_size=*/1024,
               /*enable_dictionary=*/true,
               /*max_row_group_length=*/256);
    {
        // simple case
        std::optional<RoaringBitmap32> bitmap = RoaringBitmap32::From({100, 400, 600});
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::Or(
                {PredicateBuilder::LessThan(/*field_index=*/0, /*field_name=*/"f0", FieldType::INT,
                                            Literal(255)),
                 PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f0",
                                               FieldType::INT, Literal(600))}));
        auto parquet_batch_reader = PrepareParquetFileBatchReader(
            file_path_, arrow_schema, predicate, bitmap, /*batch_size=*/length);
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::ChunkedArray> result_array,
            paimon::test::ReadResultCollector::CollectResult(parquet_batch_reader.get()));

        auto expected_array =
            arrow::ChunkedArray::Make({src_array->Slice(0, 256), src_array->Slice(512, 256)})
                .ValueOrDie();
        ASSERT_TRUE(result_array->Equals(expected_array)) << result_array->ToString();
    }
    {
        // test all data has been filtered out with predicate and bitmap pushdown
        std::optional<RoaringBitmap32> bitmap = RoaringBitmap32::From({100, 400, 600});
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f0",
                                                       FieldType::INT, Literal(800));
        auto parquet_batch_reader = PrepareParquetFileBatchReader(
            file_path_, arrow_schema, predicate, bitmap, /*batch_size=*/length);
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::ChunkedArray> result_array,
            paimon::test::ReadResultCollector::CollectResult(parquet_batch_reader.get()));
        ASSERT_FALSE(result_array);
    }
}

TEST_F(ParquetFileBatchReaderTest, TestReadNoField) {
    // if only read partition fields, format reader will set empty read schema
    std::string file_name = paimon::test::GetDataDir() +
                            "parquet/parquet_append_table.db/parquet_append_table/bucket-0/"
                            "data-9ea62f34-1dca-49c1-bf7a-d37303d8fb76-0.parquet";
    // read no field
    auto read_schema = arrow::schema(arrow::FieldVector());
    auto parquet_batch_reader =
        PrepareParquetFileBatchReader(file_name, read_schema, /*predicate=*/nullptr,
                                      /*selection_bitmap=*/std::nullopt, /*batch_size=*/2);
    uint64_t global_row_id = 0;
    // read 2 rows
    ASSERT_NOK(parquet_batch_reader->GetPreviousBatchGlobalRowId(0));
    ASSERT_OK_AND_ASSIGN(auto batch1, parquet_batch_reader->NextBatch());
    ASSERT_OK_AND_ASSIGN(global_row_id, parquet_batch_reader->GetPreviousBatchGlobalRowId(0));
    ASSERT_EQ(global_row_id, 0);
    //  read 2 rows
    ASSERT_OK_AND_ASSIGN(auto batch2, parquet_batch_reader->NextBatch());
    ASSERT_OK_AND_ASSIGN(global_row_id, parquet_batch_reader->GetPreviousBatchGlobalRowId(0));
    ASSERT_EQ(global_row_id, 2);
    // read 2 rows
    ASSERT_OK_AND_ASSIGN(auto batch3, parquet_batch_reader->NextBatch());
    ASSERT_OK_AND_ASSIGN(global_row_id, parquet_batch_reader->GetPreviousBatchGlobalRowId(0));
    ASSERT_EQ(global_row_id, 4);
    // read rows with eof
    ASSERT_OK_AND_ASSIGN(auto batch4, parquet_batch_reader->NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch4));
    ASSERT_NOK(parquet_batch_reader->GetPreviousBatchGlobalRowId(0));
    parquet_batch_reader->Close();

    arrow::FieldVector fields;
    auto arrow_type = arrow::struct_(fields);
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_type, R"([
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
    check_batch(std::move(batch3), expected_array);
}

TEST_P(ParquetFileBatchReaderTest, TestTimestampType) {
    auto enable_tz = GetParam();
    std::string timezone_str = enable_tz ? "Asia/Tokyo" : DateTimeUtils::GetLocalTimezoneName();
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
    std::shared_ptr<arrow::ChunkedArray> expected_array =
        std::make_shared<arrow::ChunkedArray>(array);

    {
        // read data generated by Java Paimon
        std::string file_name = paimon::test::GetDataDir() +
                                "/parquet/append_with_multiple_ts_precision_and_timezone.db/"
                                "append_with_multiple_ts_precision_and_timezone/bucket-0/"
                                "data-9b8abdde-df4d-4655-bb4c-ffda164ef9d4-0.parquet";
        auto parquet_batch_reader = PrepareParquetFileBatchReader(
            file_name, std::make_shared<arrow::Schema>(fields),
            /*predicate=*/nullptr, /*selection_bitmap=*/std::nullopt, batch_size_);
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::ChunkedArray> result_array,
            paimon::test::ReadResultCollector::CollectResult(parquet_batch_reader.get()));
        ASSERT_TRUE(result_array->Equals(*expected_array)) << result_array->ToString();
    }
    {
        // read data generated by C++ Paimon
        auto arrow_schema = std::make_shared<arrow::Schema>(fields);
        WriteArray(file_path_, array, arrow_schema,
                   /*write_batch_size=*/1, /*enable_dictionary=*/true,
                   /*max_row_group_length=*/1);
        auto parquet_batch_reader =
            PrepareParquetFileBatchReader(file_path_, arrow_schema, /*predicate=*/nullptr,
                                          /*selection_bitmap=*/std::nullopt, /*batch_size=*/2);

        // check file schema
        ASSERT_OK_AND_ASSIGN(auto c_file_schema, parquet_batch_reader->GetFileSchema());
        auto result_file_schema = arrow::ImportSchema(c_file_schema.get()).ValueOr(nullptr);
        ASSERT_TRUE(result_file_schema);

        arrow::FieldVector expected_fields = {
            arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::MILLI)),
            arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
            arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
            arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
            arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::MILLI, timezone_str)),
            arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, timezone_str)),
            arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, timezone_str)),
            arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        };
        auto expected_file_schema = arrow::schema(expected_fields);
        ASSERT_TRUE(result_file_schema->Equals(expected_file_schema))
            << result_file_schema->ToString();

        // check array
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::ChunkedArray> result_array,
            paimon::test::ReadResultCollector::CollectResult(parquet_batch_reader.get()));
        ASSERT_TRUE(result_array->Equals(expected_array)) << result_array->ToString();
    }
}

INSTANTIATE_TEST_SUITE_P(TestParam, ParquetFileBatchReaderTest, ::testing::Values(false, true));

TEST_F(ParquetFileBatchReaderTest, TestAddMetadataPerFieldMetadata) {
    // Write a simple parquet file, call AddMetadata to inject per-field metadata
    // before Finish, then read back and verify the file schema carries the metadata.
    auto write_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("name", arrow::utf8()),
        arrow::field("score", arrow::float64()),
    });

    std::string file_path = PathUtil::JoinPath(dir_->Str(), "update_schema_test.parquet");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs_->Create(file_path, /*overwrite=*/true));

    ::parquet::WriterProperties::Builder builder;
    builder.write_batch_size(10);
    auto writer_properties = builder.build();
    ASSERT_OK_AND_ASSIGN(auto format_writer,
                         ParquetFormatWriter::Create(out, write_schema, writer_properties,
                                                     DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE, pool_));

    // Write one batch of data.
    auto data = arrow::ipc::internal::json::ArrayFromJSON(
                    arrow::struct_(write_schema->fields()),
                    R"([[1, "alice", 95.5], [2, "bob", 88.0], [3, "charlie", 72.3]])")
                    .ValueOrDie();
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*data, &c_array).ok());
    ASSERT_OK(format_writer->AddBatch(&c_array));
    ASSERT_OK(format_writer->Flush());

    // Build an updated schema with per-field metadata on "name" and "score".
    auto name_meta = std::make_shared<arrow::KeyValueMetadata>();
    name_meta->Append("shredding.field_mapping", "0:alice,1:bob,2:charlie");
    name_meta->Append("shredding.num_columns", "3");
    auto score_meta = std::make_shared<arrow::KeyValueMetadata>();
    score_meta->Append("custom.unit", "percent");

    auto updated_schema = arrow::schema({
        write_schema->field(0),                            // id — no metadata
        write_schema->field(1)->WithMetadata(name_meta),   // name — shredding metadata
        write_schema->field(2)->WithMetadata(score_meta),  // score — custom metadata
    });

    // AddMetadata must be called before Finish.
    ASSERT_OK(format_writer->AddMetadata(
        {{ArrowUtils::kArrowSchemaMetadataKey, SerializeSchemaToString(updated_schema)}}));
    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());

    // Read back: GetFileSchema should reflect the updated per-field metadata.
    auto parquet_batch_reader =
        PrepareParquetFileBatchReader(file_path, write_schema, /*predicate=*/nullptr,
                                      /*selection_bitmap=*/std::nullopt, batch_size_);

    ASSERT_OK_AND_ASSIGN(auto c_file_schema, parquet_batch_reader->GetFileSchema());
    auto file_schema = arrow::ImportSchema(c_file_schema.get()).ValueOrDie();

    // Field 0 "id": no metadata.
    ASSERT_EQ("id", file_schema->field(0)->name());

    // Field 1 "name": should have the shredding metadata we set.
    ASSERT_EQ("name", file_schema->field(1)->name());
    auto read_name_meta = file_schema->field(1)->metadata();
    ASSERT_NE(nullptr, read_name_meta);
    auto field_mapping_val = read_name_meta->Get("shredding.field_mapping").ValueOrDie();
    ASSERT_EQ("0:alice,1:bob,2:charlie", field_mapping_val);
    auto num_columns_val = read_name_meta->Get("shredding.num_columns").ValueOrDie();
    ASSERT_EQ("3", num_columns_val);

    // Field 2 "score": should have the custom metadata.
    ASSERT_EQ("score", file_schema->field(2)->name());
    auto read_score_meta = file_schema->field(2)->metadata();
    ASSERT_NE(nullptr, read_score_meta);
    auto unit_val = read_score_meta->Get("custom.unit").ValueOrDie();
    ASSERT_EQ("percent", unit_val);

    // Also verify data integrity — read it back and compare content.
    ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                parquet_batch_reader.get()));
    ASSERT_EQ(result_array->num_chunks(), 1);
    ASSERT_TRUE(data->Equals(*result_array->chunk(0))) << result_array->ToString();
}

TEST_F(ParquetFileBatchReaderTest, TestRowMappingSimple) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::int32())};
    auto src_array = MakeSequentialIntData(12);
    // data in file rowGroup0:[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    // one row per page
    auto arrow_schema = arrow::schema(fields);
    WriteArray(file_path_, src_array, arrow_schema, /*write_batch_size=*/1,
               /*enable_dictionary=*/true, /*max_row_group_length=*/12, /*max_page_size=*/1);

    // 1<=f0<=3 || 5<=f0<=6
    ASSERT_OK_AND_ASSIGN(
        auto predicate,
        PredicateBuilder::Or({PredicateBuilder::Between(/*field_index=*/0, /*field_name=*/"f0",
                                                        FieldType::INT, Literal(1), Literal(3)),
                              PredicateBuilder::Between(/*field_index=*/0, /*field_name=*/"f0",
                                                        FieldType::INT, Literal(5), Literal(6))}));

    auto parquet_batch_reader = PrepareParquetFileBatchReader(
        file_path_, arrow_schema, /*predicate=*/predicate, std::nullopt, /*batch_size=*/2);

    uint64_t global_row_id = 0;
    ASSERT_NOK(parquet_batch_reader->GetPreviousBatchGlobalRowId(0));
    ASSERT_OK_AND_ASSIGN(auto batch1, paimon::test::ReadResultCollector::CollectResultOneBatch(
                                          parquet_batch_reader.get()));
    auto expected_batch1 = src_array->Slice(1, 2);
    ASSERT_TRUE(batch1->chunk(0)->Equals(expected_batch1)) << batch1->ToString();
    ASSERT_OK_AND_ASSIGN(global_row_id, parquet_batch_reader->GetPreviousBatchGlobalRowId(0));
    ASSERT_EQ(global_row_id, 1);
    ASSERT_OK_AND_ASSIGN(global_row_id, parquet_batch_reader->GetPreviousBatchGlobalRowId(1));
    ASSERT_EQ(global_row_id, 2);
    // out of bound return invalid
    ASSERT_NOK(parquet_batch_reader->GetPreviousBatchGlobalRowId(2));

    // Not adjacent pages
    ASSERT_OK_AND_ASSIGN(auto batch2, paimon::test::ReadResultCollector::CollectResultOneBatch(
                                          parquet_batch_reader.get()));
    auto expected_batch2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
[3],
[5]
    ])")
            .ValueOrDie());
    ASSERT_TRUE(batch2->chunk(0)->Equals(expected_batch2)) << batch2->ToString();
    ASSERT_OK_AND_ASSIGN(global_row_id, parquet_batch_reader->GetPreviousBatchGlobalRowId(0));
    ASSERT_EQ(global_row_id, 3);
    ASSERT_OK_AND_ASSIGN(global_row_id, parquet_batch_reader->GetPreviousBatchGlobalRowId(1));
    ASSERT_EQ(global_row_id, 5);

    // Only one record read
    ASSERT_OK_AND_ASSIGN(auto batch3, paimon::test::ReadResultCollector::CollectResultOneBatch(
                                          parquet_batch_reader.get()));
    auto expected_batch3 = src_array->Slice(6, 1);
    ASSERT_TRUE(batch3->chunk(0)->Equals(expected_batch3)) << batch3->ToString();
    ASSERT_OK_AND_ASSIGN(global_row_id, parquet_batch_reader->GetPreviousBatchGlobalRowId(0));
    ASSERT_EQ(global_row_id, 6);
    ASSERT_NOK(parquet_batch_reader->GetPreviousBatchGlobalRowId(1));

    ASSERT_OK_AND_ASSIGN(auto eof_batch, paimon::test::ReadResultCollector::CollectResultOneBatch(
                                             parquet_batch_reader.get()));
    ASSERT_EQ(nullptr, eof_batch);
    // previous batch is eof, return invalid.
    ASSERT_NOK(parquet_batch_reader->GetPreviousBatchGlobalRowId(0));
}

TEST_F(ParquetFileBatchReaderTest, TestRowMappingFullyAndPartially) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::int32())};
    auto src_array = MakeSequentialIntData(12);
    // data in file RowGroup0:[0, 1, 2] | RowGroup1:[3, 4, 5] | RowGroup2:[6, 7, 8] | RowGroup3:[9,
    // 10, 11] one row per page
    auto arrow_schema = arrow::schema(fields);
    WriteArray(file_path_, src_array, arrow_schema, /*write_batch_size=*/1,
               /*enable_dictionary=*/true, /*max_row_group_length=*/3, /*max_page_size=*/1);

    // 3<=f0<=5 || f0==6 || f0==8
    // RowGroup 1 is fully matched, RowGroup 2 is partially matched, RowGroup 0 and RowGroup 3 are
    // not matched.
    ASSERT_OK_AND_ASSIGN(
        auto predicate,
        PredicateBuilder::Or({PredicateBuilder::Between(/*field_index=*/0, /*field_name=*/"f0",
                                                        FieldType::INT, Literal(3), Literal(5)),
                              PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                      FieldType::INT, Literal(6)),
                              PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                      FieldType::INT, Literal(8))}));

    auto parquet_batch_reader = PrepareParquetFileBatchReader(
        file_path_, arrow_schema, /*predicate=*/predicate, std::nullopt, /*batch_size=*/3);

    uint64_t global_row_id = 0;
    ASSERT_NOK(parquet_batch_reader->GetPreviousBatchGlobalRowId(0));
    ASSERT_OK_AND_ASSIGN(auto batch1, paimon::test::ReadResultCollector::CollectResultOneBatch(
                                          parquet_batch_reader.get()));
    ASSERT_OK_AND_ASSIGN(global_row_id, parquet_batch_reader->GetPreviousBatchGlobalRowId(0));
    ASSERT_EQ(global_row_id, 3);
    ASSERT_OK_AND_ASSIGN(global_row_id, parquet_batch_reader->GetPreviousBatchGlobalRowId(2));
    ASSERT_EQ(global_row_id, 5);

    ASSERT_OK_AND_ASSIGN(auto batch2, paimon::test::ReadResultCollector::CollectResultOneBatch(
                                          parquet_batch_reader.get()));
    ASSERT_OK_AND_ASSIGN(global_row_id, parquet_batch_reader->GetPreviousBatchGlobalRowId(0));
    ASSERT_EQ(global_row_id, 6);
    ASSERT_OK_AND_ASSIGN(global_row_id, parquet_batch_reader->GetPreviousBatchGlobalRowId(1));
    ASSERT_EQ(global_row_id, 8);
}

TEST_F(ParquetFileBatchReaderTest, TestRowMappingSetReadSchemaTwice) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::int32())};
    auto src_array = MakeSequentialIntData(12);
    // data in file RowGroup0:[0, 1, 2] | RowGroup1:[3, 4, 5] | RowGroup2:[6, 7, 8] | RowGroup3:[9,
    // 10, 11] one row per page
    auto arrow_schema = arrow::schema(fields);
    WriteArray(file_path_, src_array, arrow_schema, /*write_batch_size=*/1,
               /*enable_dictionary=*/true, /*max_row_group_length=*/3, /*max_page_size=*/1);

    // 1<=f0<=3 || 6<=f0<=7
    ASSERT_OK_AND_ASSIGN(
        auto predicate,
        PredicateBuilder::Or({PredicateBuilder::Between(/*field_index=*/0, /*field_name=*/"f0",
                                                        FieldType::INT, Literal(1), Literal(3)),
                              PredicateBuilder::Between(/*field_index=*/0, /*field_name=*/"f0",
                                                        FieldType::INT, Literal(6), Literal(7))}));

    auto parquet_batch_reader = PrepareParquetFileBatchReader(
        file_path_, arrow_schema, /*predicate=*/predicate, std::nullopt, /*batch_size=*/3);

    uint64_t global_row_id = 0;
    ASSERT_NOK(parquet_batch_reader->GetPreviousBatchGlobalRowId(0));
    ASSERT_OK_AND_ASSIGN(auto batch1, paimon::test::ReadResultCollector::CollectResultOneBatch(
                                          parquet_batch_reader.get()));
    ASSERT_OK_AND_ASSIGN(global_row_id, parquet_batch_reader->GetPreviousBatchGlobalRowId(0));
    ASSERT_EQ(global_row_id, 1);
    ASSERT_OK_AND_ASSIGN(global_row_id, parquet_batch_reader->GetPreviousBatchGlobalRowId(1));
    ASSERT_EQ(global_row_id, 2);

    ASSERT_OK_AND_ASSIGN(auto batch2, paimon::test::ReadResultCollector::CollectResultOneBatch(
                                          parquet_batch_reader.get()));
    ASSERT_OK_AND_ASSIGN(global_row_id, parquet_batch_reader->GetPreviousBatchGlobalRowId(0));
    ASSERT_EQ(global_row_id, 3);
    ASSERT_NOK(parquet_batch_reader->GetPreviousBatchGlobalRowId(1));

    ASSERT_OK_AND_ASSIGN(
        predicate,
        PredicateBuilder::Or({PredicateBuilder::Between(/*field_index=*/0, /*field_name=*/"f0",
                                                        FieldType::INT, Literal(3), Literal(5))}));

    std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*arrow_schema, c_schema.get()).ok());
    ASSERT_OK(
        parquet_batch_reader->SetReadSchema(c_schema.get(), /*predicate=*/predicate, std::nullopt));
    ASSERT_NOK(parquet_batch_reader->GetPreviousBatchGlobalRowId(0));
    ASSERT_OK_AND_ASSIGN(auto batch3, paimon::test::ReadResultCollector::CollectResultOneBatch(
                                          parquet_batch_reader.get()));
    ASSERT_OK_AND_ASSIGN(global_row_id, parquet_batch_reader->GetPreviousBatchGlobalRowId(0));
    ASSERT_EQ(global_row_id, 3);
    ASSERT_OK_AND_ASSIGN(global_row_id, parquet_batch_reader->GetPreviousBatchGlobalRowId(2));
    ASSERT_EQ(global_row_id, 5);
}

}  // namespace paimon::parquet::test
