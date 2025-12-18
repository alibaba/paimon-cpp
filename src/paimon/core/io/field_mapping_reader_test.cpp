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

#include "paimon/core/io/field_mapping_reader.h"

#include <map>
#include <ostream>
#include <utility>
#include <variant>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/util.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/util/checked_cast.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/format_writer.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon {
class Predicate;
}  // namespace paimon

namespace paimon::test {
class FieldMappingReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        partition_ = BinaryRowGenerator::GenerateRow({10, 0}, pool_.get());
    }
    void TearDown() override {}

    std::unique_ptr<FileBatchReader> PrepareFileBatchReader(
        const std::string& file_name, const std::shared_ptr<FileSystem>& fs,
        const std::map<std::string, std::string>& options, const arrow::Schema* read_schema,
        const std::shared_ptr<Predicate>& predicate, int32_t batch_size) const {
        EXPECT_OK_AND_ASSIGN(auto file_format, FileFormatFactory::Get("orc", options));
        EXPECT_OK_AND_ASSIGN(auto reader_builder, file_format->CreateReaderBuilder(batch_size));
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, fs->Open(file_name));
        EXPECT_OK_AND_ASSIGN(auto orc_batch_reader, reader_builder->Build(input_stream));
        std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
        auto arrow_status = arrow::ExportSchema(*read_schema, c_schema.get());
        EXPECT_TRUE(arrow_status.ok());
        EXPECT_OK(orc_batch_reader->SetReadSchema(c_schema.get(), predicate,
                                                  /*selection_bitmap=*/std::nullopt));
        return orc_batch_reader;
    }

    std::unique_ptr<FileBatchReader> PrepareFileBatchReaderWithCustomizedData(
        const std::string& data_path, const std::shared_ptr<FileSystem>& fs,
        const arrow::Schema* src_schema, const std::shared_ptr<arrow::Array>& src_array,
        const arrow::Schema* read_schema, const std::shared_ptr<Predicate>& predicate,
        int32_t batch_size) const {
        std::map<std::string, std::string> write_options = {
            {"orc.dictionary-key-size-threshold", "1.0"}};
        EXPECT_OK_AND_ASSIGN(auto file_format, FileFormatFactory::Get("orc", write_options));
        std::unique_ptr<ArrowSchema> write_schema = std::make_unique<ArrowSchema>();
        EXPECT_TRUE(arrow::ExportSchema(*src_schema, write_schema.get()).ok());
        EXPECT_OK_AND_ASSIGN(auto writer_builder,
                             file_format->CreateWriterBuilder(write_schema.get(), batch_size));
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<paimon::OutputStream> output_stream,
                             fs->Create(data_path, /*overwrite=*/true));
        EXPECT_TRUE(output_stream);
        EXPECT_OK_AND_ASSIGN(auto writer, writer_builder->Build(output_stream, "zstd"));

        ::ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*src_array, &c_array).ok());
        EXPECT_OK(writer->AddBatch(&c_array));
        EXPECT_OK(writer->Flush());
        EXPECT_OK(writer->Finish());
        EXPECT_OK(output_stream->Flush());
        EXPECT_OK(output_stream->Close());

        EXPECT_OK_AND_ASSIGN(std::shared_ptr<paimon::InputStream> input_stream,
                             fs->Open(data_path));
        EXPECT_TRUE(input_stream);
        std::map<std::string, std::string> read_options = {
            {"orc.read.enable-lazy-decoding", "true"}};
        return PrepareFileBatchReader(data_path, fs, read_options, read_schema, predicate,
                                      batch_size);
    }

    void CheckResult(const std::shared_ptr<arrow::Schema>& read_schema,
                     const std::shared_ptr<Predicate>& predicate,
                     const std::shared_ptr<arrow::Array>& expect_array) {
        std::string file_name = paimon::test::GetDataDir() +
                                "/orc/multi_partition_append_table.db/"
                                "multi_partition_append_table/f1=10/f2=0/"
                                "bucket-0/"
                                "data-1c547e5f-48b2-4917-a996-71d306377661-0.orc";

        ASSERT_OK_AND_ASSIGN(auto mapping_builder,
                             FieldMappingBuilder::Create(read_schema, partition_keys_, predicate));
        ASSERT_OK_AND_ASSIGN(auto mapping, mapping_builder->CreateFieldMapping(data_fields_));

        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(
            mapping->non_partition_info.non_partition_data_schema);
        std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
        auto orc_batch_reader =
            PrepareFileBatchReader(file_name, fs, /*options=*/{}, arrow_schema.get(),
                                   mapping->non_partition_info.non_partition_filter,
                                   /*batch_size=*/1);

        auto reader = std::make_shared<FieldMappingReader>(
            /*field_count=*/read_schema->num_fields(), std::move(orc_batch_reader), partition_,
            std::move(mapping), pool_);
        ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(reader.get()));
        if (expect_array == nullptr && result_array == nullptr) {
            // expect empty result
            return;
        }
        auto expected_chunk_array =
            std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector({expect_array}));

        ASSERT_TRUE(result_array->type()->Equals(expected_chunk_array->type()))
            << expected_chunk_array->type()->ToString() << std::endl
            << result_array->type()->ToString();
        ASSERT_TRUE(result_array->Equals(expected_chunk_array))
            << result_array->ToString() << expected_chunk_array->ToString();
    }

    void CheckResult(const std::shared_ptr<arrow::Schema>& data_schema,
                     const std::shared_ptr<arrow::Array>& data_array,
                     const std::shared_ptr<arrow::Schema>& read_schema,
                     const std::shared_ptr<Predicate>& predicate,
                     const std::vector<std::string>& partition_keys, const BinaryRow& partition,
                     const std::shared_ptr<arrow::Array>& expect_array) const {
        auto dir = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        auto fs = dir->GetFileSystem();
        std::string data_path = dir->Str() + "/test.data";

        ASSERT_OK_AND_ASSIGN(auto mapping_builder,
                             FieldMappingBuilder::Create(read_schema, partition_keys, predicate));
        ASSERT_OK_AND_ASSIGN(auto mapping, mapping_builder->CreateFieldMapping(data_schema));

        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(
            mapping->non_partition_info.non_partition_data_schema);
        auto orc_batch_reader = PrepareFileBatchReaderWithCustomizedData(
            data_path, fs, data_schema.get(), data_array, arrow_schema.get(),
            /*predicate=*/mapping->non_partition_info.non_partition_filter, /*batch_size=*/1);

        auto reader = std::make_shared<FieldMappingReader>(
            /*field_count=*/read_schema->num_fields(), std::move(orc_batch_reader), partition,
            std::move(mapping), pool_);
        ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(reader.get()));
        if (expect_array == nullptr && result_array == nullptr) {
            // expect empty result
            return;
        }
        auto expected_chunk_array =
            std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector({expect_array}));

        ASSERT_TRUE(result_array->Equals(expected_chunk_array))
            << result_array->ToString() << expected_chunk_array->ToString();
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::vector<DataField> data_fields_ = {DataField(0, arrow::field("f0", arrow::utf8())),
                                           DataField(1, arrow::field("f1", arrow::int32())),
                                           DataField(2, arrow::field("f2", arrow::int32())),
                                           DataField(3, arrow::field("f3", arrow::float64()))};
    std::shared_ptr<arrow::Array> f0_ =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["Emily", "Bob", "Alex"])")
            .ValueOrDie();
    std::shared_ptr<arrow::Array> f1_ =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([10, 10, 10])").ValueOrDie();
    std::shared_ptr<arrow::Array> f2_ =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([0, 0, 0])").ValueOrDie();
    std::shared_ptr<arrow::Array> f3_ =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::float64(), R"([15.1, 12.1, 16.1])")
            .ValueOrDie();
    std::vector<std::string> partition_keys_ = {"f1", "f2"};
    BinaryRow partition_ = BinaryRow::EmptyRow();
};

TEST_F(FieldMappingReaderTest, TestGenerateSinglePartitionArray) {
    PartitionInfo partition_info;
    // read schema: p9-p0
    // partition key: p0-p9
    partition_info.partition_read_schema = {DataField(9, arrow::field("p9", arrow::date32())),
                                            DataField(8, arrow::field("p8", arrow::binary())),
                                            DataField(7, arrow::field("p7", arrow::utf8())),
                                            DataField(6, arrow::field("p6", arrow::float64())),
                                            DataField(5, arrow::field("p5", arrow::float32())),
                                            DataField(4, arrow::field("p4", arrow::int64())),
                                            DataField(3, arrow::field("p3", arrow::int32())),
                                            DataField(2, arrow::field("p2", arrow::int16())),
                                            DataField(1, arrow::field("p1", arrow::int8())),
                                            DataField(0, arrow::field("p0", arrow::boolean()))};
    partition_info.idx_in_target_read_schema = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    partition_info.idx_in_partition = {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

    NonPartitionInfo non_part_info;
    auto field_mapping = std::make_unique<FieldMapping>(partition_info, non_part_info,
                                                        /*non_exist_field_info=*/std::nullopt);
    auto partition = BinaryRowGenerator::GenerateRow(
        {false, static_cast<int8_t>(1), static_cast<int16_t>(2), static_cast<int32_t>(3),
         static_cast<int64_t>(4), static_cast<float>(5.1), 6.21, "7",
         std::make_shared<Bytes>("8", pool_.get()), 100},
        pool_.get());
    auto mapping_reader = std::make_unique<FieldMappingReader>(
        /*field_count=*/10, /*reader=*/nullptr, partition, std::move(field_mapping), pool_);

    {
        ASSERT_OK_AND_ASSIGN(auto p9_array, mapping_reader->GenerateSinglePartitionArray(
                                                /*idx in read schema*/ 0, /*batch_size=*/2));
        ASSERT_EQ(p9_array->length(), 2);
        ASSERT_EQ(arrow::internal::checked_cast<arrow::Date32Array*>(p9_array.get())->Value(0),
                  100);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto p8_array, mapping_reader->GenerateSinglePartitionArray(
                                                /*idx in read schema*/ 1, /*batch_size=*/2));
        ASSERT_EQ(p8_array->length(), 2);
        ASSERT_EQ(arrow::internal::checked_cast<arrow::BinaryArray*>(p8_array.get())->Value(0),
                  "8");
    }
    {
        ASSERT_OK_AND_ASSIGN(auto p7_array, mapping_reader->GenerateSinglePartitionArray(
                                                /*idx in read schema*/ 2, /*batch_size=*/1));
        ASSERT_EQ(p7_array->length(), 1);
        ASSERT_EQ(arrow::internal::checked_cast<arrow::StringArray*>(p7_array.get())->Value(0),
                  "7");
    }
    {
        ASSERT_OK_AND_ASSIGN(auto p6_array, mapping_reader->GenerateSinglePartitionArray(
                                                /*idx in read schema*/ 3, /*batch_size=*/1));
        ASSERT_EQ(
            arrow::internal::checked_cast<arrow::NumericArray<arrow::DoubleType>*>(p6_array.get())
                ->Value(0),
            static_cast<double>(6.21));
    }
    {
        ASSERT_OK_AND_ASSIGN(auto p5_array, mapping_reader->GenerateSinglePartitionArray(
                                                /*idx in read schema*/ 4, /*batch_size=*/1));
        ASSERT_EQ(
            arrow::internal::checked_cast<arrow::NumericArray<arrow::FloatType>*>(p5_array.get())
                ->Value(0),
            static_cast<float>(5.1));
    }
    {
        ASSERT_OK_AND_ASSIGN(auto p4_array, mapping_reader->GenerateSinglePartitionArray(
                                                /*idx in read schema*/ 5, /*batch_size=*/1));
        ASSERT_EQ(
            arrow::internal::checked_cast<arrow::NumericArray<arrow::Int64Type>*>(p4_array.get())
                ->Value(0),
            static_cast<int64_t>(4));
    }
    {
        ASSERT_OK_AND_ASSIGN(auto p3_array, mapping_reader->GenerateSinglePartitionArray(
                                                /*idx in read schema*/ 6, /*batch_size=*/1));
        ASSERT_EQ(
            arrow::internal::checked_cast<arrow::NumericArray<arrow::Int32Type>*>(p3_array.get())
                ->Value(0),
            static_cast<int32_t>(3));
    }
    {
        ASSERT_OK_AND_ASSIGN(auto p2_array, mapping_reader->GenerateSinglePartitionArray(
                                                /*idx in read schema*/ 7, /*batch_size=*/1));
        ASSERT_EQ(
            arrow::internal::checked_cast<arrow::NumericArray<arrow::Int16Type>*>(p2_array.get())
                ->Value(0),
            static_cast<int16_t>(2));
    }
    {
        ASSERT_OK_AND_ASSIGN(auto p1_array, mapping_reader->GenerateSinglePartitionArray(
                                                /*idx in read schema*/ 8, /*batch_size=*/1));
        ASSERT_EQ(
            arrow::internal::checked_cast<arrow::NumericArray<arrow::Int8Type>*>(p1_array.get())
                ->Value(0),
            static_cast<int64_t>(1));
    }
    {
        ASSERT_OK_AND_ASSIGN(auto p0_array, mapping_reader->GenerateSinglePartitionArray(
                                                /*idx in read schema*/ 9, /*batch_size=*/1));
        ASSERT_EQ(arrow::internal::checked_cast<arrow::BooleanArray*>(p0_array.get())->Value(0),
                  false);
    }
}

TEST_F(FieldMappingReaderTest, TestReadWithOnlyPartitionField) {
    std::vector<DataField> read_fields = {DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(1, arrow::field("f1", arrow::int32()))};
    std::shared_ptr<arrow::Schema> read_schema =
        DataField::ConvertDataFieldsToArrowSchema(read_fields);

    std::shared_ptr<arrow::Array> expect_data =
        arrow::StructArray::Make({f2_, f1_}, read_schema->fields()).ValueOrDie();
    CheckResult(read_schema, /*predicate=*/nullptr, expect_data);
}

TEST_F(FieldMappingReaderTest, TestReadWithOnlyNonExistField) {
    std::vector<DataField> read_fields = {DataField(10, arrow::field("non-exist", arrow::int32()))};
    std::shared_ptr<arrow::Schema> read_schema =
        DataField::ConvertDataFieldsToArrowSchema(read_fields);
    auto null_array =
        arrow::MakeArrayOfNull(arrow::list(arrow::int32()), /*length=*/3).ValueOrDie();
    std::shared_ptr<arrow::Array> expect_data =
        arrow::StructArray::Make({null_array}, read_schema->fields()).ValueOrDie();
    CheckResult(read_schema, /*predicate=*/nullptr, expect_data);
}

TEST_F(FieldMappingReaderTest, TestReadWithPartitionField) {
    std::vector<DataField> read_fields = {DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(1, arrow::field("f1", arrow::int32()))};
    std::shared_ptr<arrow::Schema> read_schema =
        DataField::ConvertDataFieldsToArrowSchema(read_fields);

    std::shared_ptr<arrow::Array> expect_data =
        arrow::StructArray::Make({f2_, f0_, f3_, f1_}, read_schema->fields()).ValueOrDie();
    CheckResult(read_schema, /*predicate=*/nullptr, expect_data);
}

TEST_F(FieldMappingReaderTest, TestReadWithNonExistField) {
    std::vector<DataField> read_fields = {
        DataField(2, arrow::field("f2", arrow::int32())),
        DataField(0, arrow::field("f0", arrow::utf8())),
        DataField(100, arrow::field("non-exist", arrow::list(arrow::utf8()))),
        DataField(3, arrow::field("f3", arrow::float64())),
        DataField(1, arrow::field("f1", arrow::int32()))};
    std::shared_ptr<arrow::Schema> read_schema =
        DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto null_array = arrow::MakeArrayOfNull(arrow::list(arrow::utf8()), /*length=*/3).ValueOrDie();
    std::shared_ptr<arrow::Array> expect_data =
        arrow::StructArray::Make({f2_, f0_, null_array, f3_, f1_}, read_schema->fields())
            .ValueOrDie();
    CheckResult(read_schema, /*predicate=*/nullptr, expect_data);
}

TEST_F(FieldMappingReaderTest, TestReadWithAllPartitionField) {
    std::vector<DataField> read_fields = {
        DataField(2, arrow::field("f2", arrow::int32())),
        DataField(100, arrow::field("non-exist", arrow::list(arrow::utf8()))),
        DataField(1, arrow::field("f1", arrow::int32()))};
    std::shared_ptr<arrow::Schema> read_schema =
        DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto null_array = arrow::MakeArrayOfNull(arrow::list(arrow::utf8()), /*length=*/3).ValueOrDie();
    std::shared_ptr<arrow::Array> expect_data =
        arrow::StructArray::Make({f2_, null_array, f1_}, read_schema->fields()).ValueOrDie();
    CheckResult(read_schema, /*predicate=*/nullptr, expect_data);
}

TEST_F(FieldMappingReaderTest, TestReadWithAllNonPartitionField) {
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    std::shared_ptr<arrow::Schema> read_schema =
        DataField::ConvertDataFieldsToArrowSchema(read_fields);

    std::shared_ptr<arrow::Array> expect_data =
        arrow::StructArray::Make({f3_, f0_}, read_schema->fields()).ValueOrDie();
    CheckResult(read_schema, /*predicate=*/nullptr, expect_data);
}

TEST_F(FieldMappingReaderTest, TestReadWithSchemaEvolution) {
    std::vector<DataField> read_fields = {
        DataField(2, arrow::field("f2", arrow::int32())),
        DataField(0, arrow::field("f3", arrow::utf8())),
        DataField(100, arrow::field("non-exist", arrow::list(arrow::utf8()))),
        DataField(3, arrow::field("f0", arrow::float64())),
        DataField(1, arrow::field("f1", arrow::int32()))};
    std::shared_ptr<arrow::Schema> read_schema =
        DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto null_array = arrow::MakeArrayOfNull(arrow::list(arrow::utf8()), /*length=*/3).ValueOrDie();
    std::shared_ptr<arrow::Array> expect_data =
        arrow::StructArray::Make({f2_, f0_, null_array, f3_, f1_}, read_schema->fields())
            .ValueOrDie();

    CheckResult(read_schema, /*predicate=*/nullptr, expect_data);
}

TEST_F(FieldMappingReaderTest, TestDictionaryTypeWithSchemaEvolution) {
    std::vector<DataField> data_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::float32())),
                                          DataField(2, arrow::field("f2", arrow::float64())),
                                          DataField(3, arrow::field("f3", arrow::int8())),
                                          DataField(4, arrow::field("f4", arrow::int16())),
                                          DataField(5, arrow::field("f5", arrow::int32())),
                                          DataField(6, arrow::field("f6", arrow::int64())),
                                          DataField(7, arrow::field("f7", arrow::boolean()))};
    std::shared_ptr<arrow::Schema> data_schema =
        DataField::ConvertDataFieldsToArrowSchema(data_fields);
    auto data_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({data_schema->fields()}), R"([
        ["apple", 4.0, 5.1, 10, 100, 1000, 10000, true],
        ["banana", 4.1, 6.2, 10, 200, 1000, 20000, null],
        [null, 4.2, null, 10, 300, 1000, 30000, true],
        ["data", 4.3, 8.4, 10, 400, 1000, 40000, false],
        ["data", 4.5, 8.4, 10, 400, 1000, 40000, false],
        ["data", 4.0, 8.0, 10, 500, 1000, 40000, true]
    ])")
            .ValueOrDie());

    std::vector<DataField> read_fields = {DataField(6, arrow::field("ff6", arrow::int64())),
                                          DataField(2, arrow::field("ff2", arrow::float64())),
                                          DataField(0, arrow::field("ff0", arrow::utf8())),
                                          DataField(1, arrow::field("ff1", arrow::float32())),
                                          DataField(5, arrow::field("f5", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::int8())),
                                          DataField(8, arrow::field("non-exist", arrow::utf8())),
                                          DataField(7, arrow::field("ff7", arrow::boolean())),
                                          DataField(4, arrow::field("ff4", arrow::int16()))};
    std::shared_ptr<arrow::Schema> read_schema =
        DataField::ConvertDataFieldsToArrowSchema(read_fields);

    std::vector<std::string> partition_keys = {"f3", "f5"};
    BinaryRow partition = BinaryRowGenerator::GenerateRow({10, 1000}, pool_.get());
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({read_schema->fields()}), R"([
        [10000, 5.1, "apple", 4.0, 1000, 10, null, true, 100],
        [20000, 6.2, "banana", 4.1, 1000, 10, null, null, 200],
        [30000, null, null, 4.2, 1000,  10, null, true, 300],
        [40000, 8.4, "data", 4.3, 1000, 10, null, false, 400],
        [40000, 8.4, "data", 4.5, 1000, 10, null, false, 400],
        [40000, 8.0, "data", 4.0, 1000, 10, null, true, 500]
    ])")
            .ValueOrDie());
    CheckResult(data_schema, data_array, read_schema, /*predicate=*/nullptr, partition_keys,
                partition, expected_array);
}

TEST_F(FieldMappingReaderTest, TestSchemaEvolutionWithModifyType) {
    std::vector<DataField> data_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::float32())),
                                          DataField(2, arrow::field("f2", arrow::float64())),
                                          DataField(3, arrow::field("f3", arrow::int8()))};
    std::shared_ptr<arrow::Schema> data_schema =
        DataField::ConvertDataFieldsToArrowSchema(data_fields);
    auto data_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({data_schema->fields()}), R"([
        ["true", 4.0, 5.1, 10],
        ["False", 4.1, 6.2, 10],
        [null, 4.2, null, 10],
        ["yes", 4.3, 8.4, 10],
        ["nO", 4.5, 8.4, 10],
        ["1", 4.0, 8.0, 10]
    ])")
            .ValueOrDie());

    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::boolean())),
                                          DataField(1, arrow::field("f1", arrow::utf8())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::int8()))};

    std::shared_ptr<arrow::Schema> read_schema =
        DataField::ConvertDataFieldsToArrowSchema(read_fields);

    std::vector<std::string> partition_keys = {"f3"};
    BinaryRow partition = BinaryRowGenerator::GenerateRow({10}, pool_.get());
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({read_schema->fields()}), R"([
        [true, "4", 5, 10],
        [false, "4.1", 6, 10],
        [null, "4.2", null,  10],
        [true, "4.3", 8, 10],
        [false, "4.5", 8, 10],
        [true, "4", 8, 10]
    ])")
            .ValueOrDie());
    CheckResult(data_schema, data_array, read_schema, /*predicate=*/nullptr, partition_keys,
                partition, expected_array);
}

TEST_F(FieldMappingReaderTest, TestSchemaEvolutionWithModifyTypeWithDict) {
    std::vector<DataField> data_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::float32())),
                                          DataField(2, arrow::field("f2", arrow::float64())),
                                          DataField(3, arrow::field("f3", arrow::int8()))};
    std::shared_ptr<arrow::Schema> data_schema =
        DataField::ConvertDataFieldsToArrowSchema(data_fields);
    auto data_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({data_schema->fields()}), R"([
        ["true", 4.0, 5.1, 10],
        ["false", 4.1, 6.2, 10],
        [null, 4.2, null, 10],
        ["true", 4.3, 8.4, 10],
        ["false", 4.5, 8.4, 10],
        ["1", 4.0, 8.0, 10]
    ])")
            .ValueOrDie());

    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::boolean())),
                                          DataField(1, arrow::field("f1", arrow::utf8())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::int8()))};

    std::shared_ptr<arrow::Schema> read_schema =
        DataField::ConvertDataFieldsToArrowSchema(read_fields);

    std::vector<std::string> partition_keys = {"f3"};
    BinaryRow partition = BinaryRowGenerator::GenerateRow({10}, pool_.get());
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({read_schema->fields()}), R"([
        [true, "4", 5, 10],
        [false, "4.1", 6, 10],
        [null, "4.2", null,  10],
        [true, "4.3", 8, 10],
        [false, "4.5", 8, 10],
        [true, "4", 8, 10]
    ])")
            .ValueOrDie());
    CheckResult(data_schema, data_array, read_schema, /*predicate=*/nullptr, partition_keys,
                partition, expected_array);
}

TEST_F(FieldMappingReaderTest, TestSchemaEvolutionWithModifyTypeWithPredicate) {
    std::vector<DataField> data_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::float32())),
                                          DataField(2, arrow::field("f2", arrow::int16())),
                                          DataField(3, arrow::field("f3", arrow::int8()))};
    std::shared_ptr<arrow::Schema> data_schema =
        DataField::ConvertDataFieldsToArrowSchema(data_fields);
    auto data_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({data_schema->fields()}), R"([
        ["true", 4.0, 5, 10],
        ["False", 4.1, 6, 10],
        [null, 4.2, null, 10],
        ["yes", 4.3, 8, 10],
        ["nO", 4.5, 8, 10],
        ["1", 4.0, 9, 10]
    ])")
            .ValueOrDie());

    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::boolean())),
                                          DataField(1, arrow::field("f1", arrow::utf8())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::int8()))};

    std::shared_ptr<arrow::Schema> read_schema =
        DataField::ConvertDataFieldsToArrowSchema(read_fields);
    auto predicate = PredicateBuilder::GreaterThan(
        /*field_index=*/2, /*field_name=*/"f2", FieldType::INT, {Literal(10)});

    std::vector<std::string> partition_keys = {"f3"};
    BinaryRow partition = BinaryRowGenerator::GenerateRow({10}, pool_.get());
    CheckResult(data_schema, data_array, read_schema, predicate, partition_keys, partition,
                /*expect_array=*/nullptr);
}

TEST_F(FieldMappingReaderTest, TestReadWithSchemaEvolutionWithRenameAndModifyType) {
    // field_0 and field_3 are rename and modify type
    std::vector<DataField> read_fields = {
        DataField(2, arrow::field("f2", arrow::int32())),
        DataField(0, arrow::field("f3", arrow::binary())),
        DataField(100, arrow::field("non-exist", arrow::list(arrow::utf8()))),
        DataField(3, arrow::field("f0", arrow::utf8())),
        DataField(1, arrow::field("f1", arrow::int32()))};
    std::shared_ptr<arrow::Schema> read_schema =
        DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto null_array = arrow::MakeArrayOfNull(arrow::list(arrow::utf8()), /*length=*/3).ValueOrDie();
    std::shared_ptr<arrow::Array> f0_new =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::binary(), R"(["Emily", "Bob", "Alex"])")
            .ValueOrDie();
    std::shared_ptr<arrow::Array> f3_new =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["15.1", "12.1", "16.1"])")
            .ValueOrDie();
    std::shared_ptr<arrow::Array> expect_data =
        arrow::StructArray::Make({f2_, f0_new, null_array, f3_new, f1_}, read_schema->fields())
            .ValueOrDie();
    CheckResult(read_schema, /*predicate=*/nullptr, expect_data);
}

TEST_F(FieldMappingReaderTest, TestReadWithSchemaEvolutionWithRenameAndModifyTypeAndPredicate) {
    // field_0 and field_3 are rename and modify type
    // result is not filtered by predicate, as DOUBLE->STRING alter table does not support predicate
    // push down
    std::vector<DataField> read_fields = {
        DataField(2, arrow::field("f2", arrow::int32())),
        DataField(0, arrow::field("f3", arrow::binary())),
        DataField(100, arrow::field("non-exist", arrow::list(arrow::utf8()))),
        DataField(3, arrow::field("f0", arrow::utf8())),
        DataField(1, arrow::field("f1", arrow::int32()))};
    std::shared_ptr<arrow::Schema> read_schema =
        DataField::ConvertDataFieldsToArrowSchema(read_fields);
    std::string literal_str = "17";
    auto predicate = PredicateBuilder::GreaterOrEqual(
        /*field_index=*/3, /*field_name=*/"f0", FieldType::STRING,
        Literal(FieldType::STRING, literal_str.data(), literal_str.size()));
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({read_schema->fields()}), R"([
        [0, "Emily", null, "15.1", 10],
        [0, "Bob", null, "12.1", 10],
        [0, "Alex", null, "16.1", 10]
    ])")
            .ValueOrDie());
    CheckResult(read_schema, predicate, expected_array);
}

TEST_F(FieldMappingReaderTest, TestSchemaEvolutionWithDictType) {
    std::vector<DataField> data_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::float32())),
                                          DataField(2, arrow::field("f2", arrow::float64())),
                                          DataField(3, arrow::field("f3", arrow::int8()))};
    std::shared_ptr<arrow::Schema> data_schema =
        DataField::ConvertDataFieldsToArrowSchema(data_fields);
    auto data_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({data_schema->fields()}), R"([
        ["Bob", 4.0, 5.1, 10],
        ["Emily", 4.1, 6.2, 10],
        ["Alice", 4.2, null, 10],
        ["Bob", 4.3, 8.4, 10],
        ["Emily", 4.5, 8.4, 10],
        ["Bob", 4.0, 8.0, 10]
    ])")
            .ValueOrDie());

    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::int8())),
                                          DataField(1, arrow::field("f1", arrow::utf8())),
                                          DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(2, arrow::field("f2", arrow::int32()))};

    std::shared_ptr<arrow::Schema> read_schema =
        DataField::ConvertDataFieldsToArrowSchema(read_fields);

    std::vector<std::string> partition_keys = {"f3"};
    BinaryRow partition = BinaryRowGenerator::GenerateRow({10}, pool_.get());
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({read_schema->fields()}), R"([
        [10, "4", "Bob", 5],
        [10, "4.1", "Emily", 6],
        [10, "4.2", "Alice", null],
        [10, "4.3", "Bob", 8],
        [10, "4.5", "Emily", 8],
        [10, "4", "Bob", 8]
    ])")
            .ValueOrDie());
    CheckResult(data_schema, data_array, read_schema, /*predicate=*/nullptr, partition_keys,
                partition, expected_array);
}

}  // namespace paimon::test
