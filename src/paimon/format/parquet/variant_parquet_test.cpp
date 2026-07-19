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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/io/file.h"
#include "gtest/gtest.h"
#include "paimon/common/data/shredding/shredding_file_reader.h"
#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/common/data/variant/variant_defs.h"
#include "paimon/common/data/variant/variant_schema.h"
#include "paimon/common/data/variant/variant_shredding_batch_converter.h"
#include "paimon/common/data/variant/variant_shredding_read_plan_factory.h"
#include "paimon/common/data/variant/variant_shredding_utils.h"
#include "paimon/common/data/variant/variant_shredding_write_plan.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/data/variant.h"
#include "paimon/format/parquet/parquet_field_id_converter.h"
#include "paimon/format/parquet/parquet_file_batch_reader.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/format/parquet/parquet_format_writer.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/variant_test_data.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/properties.h"
#include "parquet/schema.h"

namespace paimon::parquet::test {

class VariantParquetTest : public ::testing::Test {
 public:
    void SetUp() override {
        dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        fs_ = std::make_shared<LocalFileSystem>();
        pool_ = GetDefaultPool();
        arrow_pool_ = GetArrowPool(pool_);
        file_path_ = PathUtil::JoinPath(dir_->Str(), "variant.parquet");

        std::vector<DataField> fields = {DataField(1, arrow::field("id", arrow::int32())),
                                         DataField(2, VariantTypeUtils::ToArrowField("v"))};
        paimon_schema_ = DataField::ConvertDataFieldsToArrowSchema(fields);
    }

    std::shared_ptr<arrow::StructArray> BuildArray(const std::vector<const char*>& jsons) {
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> batch,
                             paimon::test::VariantTestData::BuildVariantBatch(
                                 paimon_schema_->field(0), paimon_schema_->field(1), jsons, pool_));
        return batch;
    }

    // Writes one batch with the given logical schema through the production parquet write path
    // (mapping paimon field ids to parquet field ids).
    void WriteFile(const std::shared_ptr<arrow::Schema>& schema, ArrowArray* c_array) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Schema> write_schema,
                             ParquetFieldIdConverter::AddParquetIdsFromPaimonIds(schema));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                             fs_->Create(file_path_, /*overwrite=*/true));
        ::parquet::WriterProperties::Builder builder;
        auto writer_properties = builder.build();
        ASSERT_OK_AND_ASSIGN(
            auto format_writer,
            ParquetFormatWriter::Create(out, write_schema, writer_properties,
                                        DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE, arrow_pool_));
        ASSERT_OK(format_writer->AddBatch(c_array));
        ASSERT_OK(format_writer->Flush());
        ASSERT_OK(format_writer->Finish());
        ASSERT_OK(out->Flush());
        ASSERT_OK(out->Close());
    }

    void WriteFile(const std::shared_ptr<arrow::StructArray>& array) {
        auto arrow_array = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*array, arrow_array.get()).ok());
        WriteFile(paimon_schema_, arrow_array.get());
    }

    // Writes `jsons` shredded according to the configured ROW-type shredding schema JSON.
    void WriteShreddedFile(const std::vector<const char*>& jsons,
                           const char* shredding_schema_json) {
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<VariantShreddingWritePlan> plan,
            VariantShreddingWritePlan::FromConfiguredSchema(paimon_schema_, shredding_schema_json));
        ASSERT_NE(plan, nullptr);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<VariantShreddingBatchConverter> converter,
                             VariantShreddingBatchConverter::Create(plan, pool_));
        auto logical = BuildArray(jsons);
        auto c_logical = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*logical, c_logical.get()).ok());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ArrowArray> c_physical,
                             converter->Convert(c_logical.get()));
        WriteFile(converter->GetPhysicalSchema(), c_physical.get());
    }

    // Opens the written file and returns the reader plus its imported file schema.
    void OpenFile(std::unique_ptr<FileBatchReader>* file_reader,
                  std::shared_ptr<arrow::Schema>* file_schema) {
        ASSERT_OK_AND_ASSIGN(auto input_stream, fs_->Open(file_path_));
        auto length = fs_->GetFileStatus(file_path_).value()->GetLen();
        auto in_stream =
            std::make_unique<ArrowInputStreamAdapter>(std::move(input_stream), arrow_pool_, length);
        std::map<std::string, std::string> options = {};
        ASSERT_OK_AND_ASSIGN(auto parquet_reader, ParquetFileBatchReader::Create(
                                                      std::move(in_stream), options,
                                                      /*batch_size=*/1024,
                                                      /*file_metadata=*/nullptr, arrow_pool_));
        *file_reader = std::move(parquet_reader);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<::ArrowSchema> c_file_schema,
                             (*file_reader)->GetFileSchema());
        auto imported = arrow::ImportSchema(c_file_schema.get());
        ASSERT_TRUE(imported.ok()) << imported.status().ToString();
        *file_schema = imported.ValueOrDie();
    }

    // Builds a variant-access projection field for column "v" via the public builder.
    std::shared_ptr<arrow::Field> BuildAccessField(
        const std::vector<std::pair<std::shared_ptr<arrow::DataType>, std::string>>& accesses) {
        VariantAccessBuilder builder;
        for (const auto& [type, path] : accesses) {
            auto c_target = std::make_unique<ArrowSchema>();
            EXPECT_TRUE(arrow::ExportField(arrow::Field("t", type), c_target.get()).ok());
            EXPECT_OK(builder.AddField(c_target.get(), path, /*fail_on_error=*/false));
        }
        auto c_field = builder.Build("v");
        EXPECT_TRUE(c_field.ok()) << c_field.status().ToString();
        auto imported = arrow::ImportField(c_field.value().get());
        EXPECT_TRUE(imported.ok()) << imported.status().ToString();
        return imported.ValueOrDie();
    }

    // Reads the whole file through the shredding reader with the given read schema and returns
    // the `v` column.
    void ReadVariantColumn(const std::shared_ptr<arrow::Schema>& read_schema,
                           std::shared_ptr<arrow::StructArray>* v_column) {
        std::unique_ptr<FileBatchReader> file_reader;
        std::shared_ptr<arrow::Schema> file_schema;
        OpenFile(&file_reader, &file_schema);
        ASSERT_OK_AND_ASSIGN(auto plans, VariantShreddingReadPlanFactory::CreateReadPlans(
                                             read_schema, file_schema, pool_));
        ASSERT_EQ(plans.size(), 1);
        auto shredding_reader =
            std::make_unique<ShreddingFileReader>(std::move(file_reader), std::move(plans), pool_);
        auto c_read_schema = std::make_unique<ArrowSchema>();
        ASSERT_TRUE(arrow::ExportSchema(*read_schema, c_read_schema.get()).ok());
        ASSERT_OK(shredding_reader->SetReadSchema(c_read_schema.get(), /*predicate=*/nullptr,
                                                  /*selection_bitmap=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(auto batch_with_bitmap, shredding_reader->NextBatchWithBitmap());
        ASSERT_FALSE(BatchReader::IsEofBatch(batch_with_bitmap));
        auto& [read_batch, bitmap] = batch_with_bitmap;
        auto imported = arrow::ImportArray(read_batch.first.get(), read_batch.second.get());
        ASSERT_TRUE(imported.ok()) << imported.status().ToString();
        auto result_struct = std::static_pointer_cast<arrow::StructArray>(imported.ValueOrDie());
        *v_column = std::static_pointer_cast<arrow::StructArray>(result_struct->field(1));
        shredding_reader->Close();
        // The assembled arrays borrow the reader's memory pool; keep the reader alive until the
        // fixture is torn down (fixture members outlive test-body locals).
        live_readers_.push_back(std::move(shredding_reader));
    }

 protected:
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::string file_path_;
    std::shared_ptr<arrow::Schema> paimon_schema_;
    std::vector<std::unique_ptr<FileBatchReader>> live_readers_;
};

namespace {

constexpr const char* kAgeCityShreddingSchema = R"({
    "type": "ROW",
    "fields": [ {
        "id": 0,
        "name": "v",
        "type": {
            "type": "ROW",
            "fields": [
                {"id": 1, "name": "age", "type": "INT"},
                {"id": 2, "name": "city", "type": "STRING"}
            ]
        }
    } ]
})";

}  // namespace

TEST_F(VariantParquetTest, PhysicalLayoutMatchesJava) {
    auto array = BuildArray({R"({"a": 1, "b": "hello"})", nullptr, "[1,2,3]"});
    WriteFile(array);

    // The on-disk layout must match the Java ParquetSchemaConverter: an (unannotated) group
    // with two REQUIRED BINARY fields `value` (id 0) and `metadata` (id 1). The raw parquet
    // reader is required because these parquet-level properties (repetition, physical types,
    // field ids, the absence of a logical-type annotation) are not visible in the Arrow schema
    // surfaced by the paimon reader.
    auto file = arrow::io::ReadableFile::Open(file_path_, arrow_pool_.get());
    ASSERT_TRUE(file.ok());
    std::unique_ptr<::parquet::arrow::FileReader> reader;
    auto status = ::parquet::arrow::OpenFile(file.ValueOrDie(), arrow_pool_.get(), &reader);
    ASSERT_TRUE(status.ok()) << status.ToString();
    const ::parquet::SchemaDescriptor* schema = reader->parquet_reader()->metadata()->schema();
    ASSERT_EQ(schema->num_columns(), 3);
    const auto* root = schema->group_node();
    ASSERT_EQ(root->field_count(), 2);
    const auto& variant_group_node = root->field(1);
    ASSERT_TRUE(variant_group_node->is_group());
    ASSERT_EQ(variant_group_node->name(), "v");
    ASSERT_EQ(variant_group_node->field_id(), 2);
    ASSERT_EQ(variant_group_node->logical_type()->type(), ::parquet::LogicalType::Type::NONE);
    const auto* variant_group =
        static_cast<const ::parquet::schema::GroupNode*>(variant_group_node.get());
    ASSERT_EQ(variant_group->field_count(), 2);
    const auto& value_node = variant_group->field(0);
    ASSERT_EQ(value_node->name(), "value");
    ASSERT_TRUE(value_node->is_primitive());
    ASSERT_TRUE(value_node->is_required());
    ASSERT_EQ(value_node->field_id(), 0);
    ASSERT_EQ(
        static_cast<const ::parquet::schema::PrimitiveNode*>(value_node.get())->physical_type(),
        ::parquet::Type::BYTE_ARRAY);
    const auto& metadata_node = variant_group->field(1);
    ASSERT_EQ(metadata_node->name(), "metadata");
    ASSERT_TRUE(metadata_node->is_primitive());
    ASSERT_TRUE(metadata_node->is_required());
    ASSERT_EQ(metadata_node->field_id(), 1);
}

TEST_F(VariantParquetTest, WriteAndReadRoundTrip) {
    std::vector<const char*> jsons = {
        R"({"a": 1, "b": "hello"})",
        nullptr,
        "[1,2,3]",
        "{\"nested\": {\"x\": [true, null, 1.5]}, \"s\": \"中文\"}",
        "12345678901234",
        "100.99",
    };
    auto array = BuildArray(jsons);
    WriteFile(array);

    {
        // Sanity-check the raw file through the plain parquet-arrow reader: the struct child
        // arrays must align with the logical rows.
        auto file = arrow::io::ReadableFile::Open(file_path_, arrow_pool_.get());
        ASSERT_TRUE(file.ok());
        std::unique_ptr<::parquet::arrow::FileReader> raw_reader;
        ASSERT_TRUE(
            ::parquet::arrow::OpenFile(file.ValueOrDie(), arrow_pool_.get(), &raw_reader).ok());
        std::shared_ptr<arrow::Table> table;
        ASSERT_TRUE(raw_reader->ReadTable(&table).ok());
        auto raw_variant = std::static_pointer_cast<arrow::StructArray>(table->column(1)->chunk(0));
        auto raw_value = std::static_pointer_cast<arrow::BinaryArray>(raw_variant->field(0));
        for (size_t i = 0; i < jsons.size(); ++i) {
            SCOPED_TRACE("raw row " + std::to_string(i));
            if (jsons[i] != nullptr) {
                ASSERT_FALSE(raw_variant->IsNull(i));
                ASSERT_GT(raw_value->GetView(i).size(), 0);
            } else {
                ASSERT_TRUE(raw_variant->IsNull(i));
            }
        }
    }

    ASSERT_OK_AND_ASSIGN(auto input_stream, fs_->Open(file_path_));
    auto length = fs_->GetFileStatus(file_path_).value()->GetLen();
    auto in_stream =
        std::make_unique<ArrowInputStreamAdapter>(std::move(input_stream), arrow_pool_, length);
    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(auto batch_reader,
                         ParquetFileBatchReader::Create(std::move(in_stream), options,
                                                        /*batch_size=*/1024,
                                                        /*file_metadata=*/nullptr, arrow_pool_));
    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*paimon_schema_, c_schema.get()).ok());
    ASSERT_OK(batch_reader->SetReadSchema(c_schema.get(), /*predicate=*/nullptr,
                                          /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto result_chunked,
                         paimon::test::ReadResultCollector::CollectResult(batch_reader.get()));
    batch_reader->Close();
    ASSERT_EQ(result_chunked->length(), static_cast<int64_t>(jsons.size()));
    ASSERT_EQ(result_chunked->num_chunks(), 1);
    auto result_struct = std::static_pointer_cast<arrow::StructArray>(result_chunked->chunk(0));

    auto variant_column = std::static_pointer_cast<arrow::StructArray>(result_struct->field(1));
    ASSERT_EQ(variant_column->length(), static_cast<int64_t>(jsons.size()));
    ASSERT_EQ(variant_column->field(0)->length(), variant_column->length());
    auto value_column = std::static_pointer_cast<arrow::BinaryArray>(variant_column->field(0));
    auto metadata_column = std::static_pointer_cast<arrow::BinaryArray>(variant_column->field(1));
    for (size_t i = 0; i < jsons.size(); ++i) {
        SCOPED_TRACE("row " + std::to_string(i));
        if (jsons[i] == nullptr) {
            ASSERT_TRUE(variant_column->IsNull(i));
            continue;
        }
        ASSERT_FALSE(variant_column->IsNull(i));
        auto value_view = value_column->GetView(i);
        auto metadata_view = metadata_column->GetView(i);
        SCOPED_TRACE("value size " + std::to_string(value_view.size()) + ", metadata size " +
                     std::to_string(metadata_view.size()));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> variant,
                             GenericVariant::Create(value_view, metadata_view, pool_));
        ASSERT_OK_AND_ASSIGN(std::string actual_json, variant->ToJson());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> expected,
                             GenericVariant::FromJson(jsons[i], pool_));
        ASSERT_OK_AND_ASSIGN(std::string expected_json, expected->ToJson());
        ASSERT_EQ(actual_json, expected_json);
    }
}

TEST_F(VariantParquetTest, ShreddedWriteAndReadRoundTrip) {
    std::vector<const char*> jsons = {
        R"({"age": 35, "city": "Hangzhou"})",
        nullptr,
        R"({"age": "not a number", "extra": [1, 2]})",
        "[\"top level array\"]",
    };
    WriteShreddedFile(jsons, kAgeCityShreddingSchema);

    {
        std::unique_ptr<FileBatchReader> file_reader;
        std::shared_ptr<arrow::Schema> file_schema;
        OpenFile(&file_reader, &file_schema);
        auto file_variant_field = file_schema->GetFieldByName("v");
        ASSERT_NE(file_variant_field, nullptr);
        ASSERT_TRUE(VariantShreddingUtils::IsShreddedFileType(file_variant_field->type()))
            << file_variant_field->type()->ToString();
        file_reader->Close();
    }

    // Reading the column as a plain VARIANT reassembles every physical shape back to the
    // original logical value.
    std::shared_ptr<arrow::StructArray> variant_column;
    ReadVariantColumn(paimon_schema_, &variant_column);
    ASSERT_EQ(variant_column->length(), static_cast<int64_t>(jsons.size()));
    auto value_column = std::static_pointer_cast<arrow::BinaryArray>(variant_column->field(0));
    auto metadata_column = std::static_pointer_cast<arrow::BinaryArray>(variant_column->field(1));
    for (size_t i = 0; i < jsons.size(); ++i) {
        SCOPED_TRACE("row " + std::to_string(i));
        if (jsons[i] == nullptr) {
            ASSERT_TRUE(variant_column->IsNull(i));
            continue;
        }
        ASSERT_FALSE(variant_column->IsNull(i));
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<GenericVariant> variant,
            GenericVariant::Create(value_column->GetView(i), metadata_column->GetView(i), pool_));
        ASSERT_OK_AND_ASSIGN(std::string actual_json, variant->ToJson());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> expected,
                             GenericVariant::FromJson(jsons[i], pool_));
        ASSERT_OK_AND_ASSIGN(std::string expected_json, expected->ToJson());
        ASSERT_EQ(actual_json, expected_json);
    }
}

TEST_F(VariantParquetTest, VariantAccessReadMixedTypedAndBinary) {
    std::vector<const char*> jsons = {R"({"age": 35, "city": "Chicago"})",
                                      R"({"age": 25, "other": "Hello"})", nullptr};
    WriteShreddedFile(jsons, kAgeCityShreddingSchema);

    auto access_field = BuildAccessField(
        {{arrow::int64(), "$.age"}, {arrow::utf8(), "$.other"}, {arrow::utf8(), "$.missing"}});
    auto read_schema = arrow::schema({paimon_schema_->field(0), access_field});

    // The plan prunes `typed_value` to the requested keys and keeps `value` because `$.other`
    // and `$.missing` are not shredded.
    {
        std::unique_ptr<FileBatchReader> file_reader;
        std::shared_ptr<arrow::Schema> file_schema;
        OpenFile(&file_reader, &file_schema);
        ASSERT_OK_AND_ASSIGN(auto plans, VariantShreddingReadPlanFactory::CreateReadPlans(
                                             read_schema, file_schema, pool_));
        ASSERT_EQ(plans.size(), 1);
        const auto& physical_type =
            static_cast<const arrow::StructType&>(*plans.at("v")->PhysicalField()->type());
        ASSERT_NE(physical_type.GetFieldByName(VariantDefs::kMetadataFieldName), nullptr);
        ASSERT_NE(physical_type.GetFieldByName(VariantDefs::kValueFieldName), nullptr);
        auto typed_value = physical_type.GetFieldByName(VariantDefs::kTypedValueFieldName);
        ASSERT_NE(typed_value, nullptr);
        const auto& typed_struct = static_cast<const arrow::StructType&>(*typed_value->type());
        ASSERT_EQ(typed_struct.num_fields(), 1);
        ASSERT_NE(typed_struct.GetFieldByName("age"), nullptr);
        file_reader->Close();
    }

    std::shared_ptr<arrow::StructArray> v_column;
    ReadVariantColumn(read_schema, &v_column);
    ASSERT_EQ(v_column->length(), 3);
    const auto& age = static_cast<const arrow::Int64Array&>(*v_column->field(0));
    const auto& other = static_cast<const arrow::StringArray&>(*v_column->field(1));
    const auto& missing = static_cast<const arrow::StringArray&>(*v_column->field(2));
    ASSERT_EQ(age.Value(0), 35);
    ASSERT_EQ(age.Value(1), 25);
    ASSERT_TRUE(v_column->IsNull(2));
    ASSERT_TRUE(other.IsNull(0));
    ASSERT_EQ(other.GetString(1), "Hello");
    ASSERT_TRUE(missing.IsNull(0));
    ASSERT_TRUE(missing.IsNull(1));
}

TEST_F(VariantParquetTest, VariantAccessReadTypedOnlyPrunesValue) {
    std::vector<const char*> jsons = {R"({"age": 35, "city": "Chicago"})",
                                      R"({"age": 25, "other": "Hello"})"};
    WriteShreddedFile(jsons, kAgeCityShreddingSchema);

    auto access_field = BuildAccessField({{arrow::int64(), "$.age"}, {arrow::utf8(), "$.city"}});
    auto read_schema = arrow::schema({paimon_schema_->field(0), access_field});

    // All requested keys are shredded: neither `value` nor the unrequested typed keys are read.
    {
        std::unique_ptr<FileBatchReader> file_reader;
        std::shared_ptr<arrow::Schema> file_schema;
        OpenFile(&file_reader, &file_schema);
        ASSERT_OK_AND_ASSIGN(auto plans, VariantShreddingReadPlanFactory::CreateReadPlans(
                                             read_schema, file_schema, pool_));
        const auto& physical_type =
            static_cast<const arrow::StructType&>(*plans.at("v")->PhysicalField()->type());
        ASSERT_EQ(physical_type.GetFieldByName(VariantDefs::kValueFieldName), nullptr);
        auto typed_value = physical_type.GetFieldByName(VariantDefs::kTypedValueFieldName);
        ASSERT_NE(typed_value, nullptr);
        ASSERT_EQ(typed_value->type()->num_fields(), 2);
        file_reader->Close();
    }

    std::shared_ptr<arrow::StructArray> v_column;
    ReadVariantColumn(read_schema, &v_column);
    const auto& age = static_cast<const arrow::Int64Array&>(*v_column->field(0));
    const auto& city = static_cast<const arrow::StringArray&>(*v_column->field(1));
    ASSERT_EQ(age.Value(0), 35);
    ASSERT_EQ(age.Value(1), 25);
    ASSERT_EQ(city.GetString(0), "Chicago");
    // Row 1 has no "city" key: the shredded field is missing, which reads as null.
    ASSERT_TRUE(city.IsNull(1));
}

TEST_F(VariantParquetTest, VariantAccessReadUnshreddedFile) {
    std::vector<const char*> jsons = {R"({"age": 35, "city": "Chicago"})",
                                      R"({"age": 25, "other": "Hello"})", nullptr};
    WriteFile(BuildArray(jsons));

    auto access_field = BuildAccessField({{arrow::int64(), "$.age"}, {arrow::utf8(), "$.other"}});
    auto read_schema = arrow::schema({paimon_schema_->field(0), access_field});

    std::shared_ptr<arrow::StructArray> v_column;
    ReadVariantColumn(read_schema, &v_column);
    ASSERT_EQ(v_column->length(), 3);
    const auto& age = static_cast<const arrow::Int64Array&>(*v_column->field(0));
    const auto& other = static_cast<const arrow::StringArray&>(*v_column->field(1));
    ASSERT_EQ(age.Value(0), 35);
    ASSERT_EQ(age.Value(1), 25);
    ASSERT_TRUE(v_column->IsNull(2));
    ASSERT_TRUE(other.IsNull(0));
    ASSERT_EQ(other.GetString(1), "Hello");
}

TEST_F(VariantParquetTest, VariantAccessReadSemicolonKey) {
    // Object keys may contain the description delimiter; the description parser anchors on the
    // trailing failOnError/timeZoneId tokens instead of splitting on every delimiter.
    std::vector<const char*> jsons = {R"({"a;b": 7})"};
    WriteFile(BuildArray(jsons));
    auto access_field = BuildAccessField({{arrow::int64(), "$['a;b']"}});
    auto read_schema = arrow::schema({paimon_schema_->field(0), access_field});
    std::shared_ptr<arrow::StructArray> v_column;
    ReadVariantColumn(read_schema, &v_column);
    ASSERT_EQ(static_cast<const arrow::Int64Array&>(*v_column->field(0)).Value(0), 7);
}

TEST_F(VariantParquetTest, VariantAccessReadVariantTarget) {
    std::vector<const char*> jsons = {R"({"user": {"name": "Paimon", "age": 1}})",
                                      R"({"user": "flat"})"};
    WriteFile(BuildArray(jsons));

    // A variant-marked target re-encodes the extracted sub-variant instead of casting it to a
    // plain struct; the marker on the target field must survive AddField.
    VariantAccessBuilder builder;
    ASSERT_OK_AND_ASSIGN(auto variant_target, Variant::ArrowField("t"));
    ASSERT_OK(builder.AddField(variant_target.get(), "$.user"));
    ASSERT_OK_AND_ASSIGN(auto c_access_field, builder.Build("v"));
    auto imported = arrow::ImportField(c_access_field.get());
    ASSERT_TRUE(imported.ok()) << imported.status().ToString();
    auto read_schema = arrow::schema({paimon_schema_->field(0), imported.ValueOrDie()});

    std::shared_ptr<arrow::StructArray> v_column;
    ReadVariantColumn(read_schema, &v_column);
    const auto& user = static_cast<const arrow::StructArray&>(*v_column->field(0));
    const auto& value_column = static_cast<const arrow::BinaryArray&>(*user.field(0));
    const auto& metadata_column = static_cast<const arrow::BinaryArray&>(*user.field(1));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<GenericVariant> row0,
        GenericVariant::Create(value_column.GetView(0), metadata_column.GetView(0), pool_));
    ASSERT_OK_AND_ASSIGN(std::string row0_json, row0->ToJson());
    ASSERT_EQ(row0_json, R"({"age":1,"name":"Paimon"})");
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<GenericVariant> row1,
        GenericVariant::Create(value_column.GetView(1), metadata_column.GetView(1), pool_));
    ASSERT_OK_AND_ASSIGN(std::string row1_json, row1->ToJson());
    ASSERT_EQ(row1_json, R"("flat")");
}

TEST_F(VariantParquetTest, VariantAccessReadNestedPath) {
    const char* nested_shredding_schema = R"({
        "type": "ROW",
        "fields": [ {
            "id": 0,
            "name": "v",
            "type": {
                "type": "ROW",
                "fields": [ {
                    "id": 1,
                    "name": "address",
                    "type": {
                        "type": "ROW",
                        "fields": [ {"id": 2, "name": "city", "type": "STRING"} ]
                    }
                } ]
            }
        } ]
    })";
    std::vector<const char*> jsons = {R"({"address": {"city": "Hangzhou"}})",
                                      R"({"address": "oops"})", R"({"address": {"zip": 12345}})"};
    WriteShreddedFile(jsons, nested_shredding_schema);

    auto access_field = BuildAccessField({{arrow::utf8(), "$.address.city"}});
    auto read_schema = arrow::schema({paimon_schema_->field(0), access_field});

    std::shared_ptr<arrow::StructArray> v_column;
    ReadVariantColumn(read_schema, &v_column);
    const auto& city = static_cast<const arrow::StringArray&>(*v_column->field(0));
    ASSERT_EQ(city.GetString(0), "Hangzhou");
    // Row 1's address is not an object; row 2's address has no "city" key.
    ASSERT_TRUE(city.IsNull(1));
    ASSERT_TRUE(city.IsNull(2));
}

}  // namespace paimon::parquet::test
