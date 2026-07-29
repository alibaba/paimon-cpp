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

        // `[id, s: struct<nv: VARIANT, t: STRING>]`: a variant nested inside a ROW column, next to
        // a plain sibling that must survive the physical substitution and the reassembly.
        nested_sibling_field_ = arrow::field("t", arrow::utf8());
        std::vector<DataField> nested_fields = {
            DataField(1, arrow::field("id", arrow::int32())),
            DataField(2, arrow::field("s", arrow::struct_({VariantTypeUtils::ToArrowField("nv"),
                                                           nested_sibling_field_})))};
        nested_schema_ = DataField::ConvertDataFieldsToArrowSchema(nested_fields);
        nested_variant_field_ = nested_schema_->field(1)->type()->field(0);

        // `[id, arr: ARRAY<VARIANT>]` and `[id, m: MAP<STRING, VARIANT>]`: variants inside
        // repeated groups, which are never shredded but can still be read as a projection.
        list_element_field_ = VariantTypeUtils::ToArrowField("element");
        std::vector<DataField> list_fields = {
            DataField(1, arrow::field("id", arrow::int32())),
            DataField(2, arrow::field("arr", arrow::list(list_element_field_)))};
        list_schema_ = DataField::ConvertDataFieldsToArrowSchema(list_fields);
        list_element_field_ = list_schema_->field(1)->type()->field(0);

        map_item_field_ = VariantTypeUtils::ToArrowField("value");
        std::vector<DataField> map_fields = {
            DataField(1, arrow::field("id", arrow::int32())),
            DataField(2, arrow::field("m", arrow::map(arrow::utf8(), map_item_field_)))};
        map_schema_ = DataField::ConvertDataFieldsToArrowSchema(map_fields);
        map_item_field_ = map_schema_->field(1)->type()->field(0)->type()->field(1);

        // `[id, arr2: ARRAY<ROW<v: VARIANT, t: STRING>>]`: a variant one struct level below a
        // repeated group, where the read and file children must line up field by field.
        std::vector<DataField> list_struct_fields = {
            DataField(1, arrow::field("id", arrow::int32())),
            DataField(
                2, arrow::field("arr2",
                                arrow::list(arrow::field(
                                    "element", arrow::struct_({VariantTypeUtils::ToArrowField("v"),
                                                               nested_sibling_field_})))))};
        list_struct_schema_ = DataField::ConvertDataFieldsToArrowSchema(list_struct_fields);
        list_struct_variant_field_ =
            list_struct_schema_->field(1)->type()->field(0)->type()->field(0);
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

    void WriteShreddedFile(const std::vector<const char*>& jsons,
                           const std::shared_ptr<VariantShreddingWritePlan>& plan) {
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

    // Writes `jsons` shredded according to the configured ROW-type shredding schema JSON.
    void WriteShreddedFile(const std::vector<const char*>& jsons,
                           const char* shredding_schema_json) {
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<VariantShreddingWritePlan> plan,
            VariantShreddingWritePlan::FromConfiguredSchema(paimon_schema_, shredding_schema_json));
        WriteShreddedFile(jsons, plan);
    }

    // Writes `jsons` using the given inferred shredding type for the top-level Variant column.
    void WriteShreddedFile(const std::vector<const char*>& jsons,
                           const std::shared_ptr<arrow::DataType>& shredding_type) {
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<VariantShreddingWritePlan> plan,
            VariantShreddingWritePlan::Create(paimon_schema_, {{"v", shredding_type}}));
        WriteShreddedFile(jsons, plan);
    }

    static std::string NestedSiblingValue(size_t row) {
        return "t" + std::to_string(row);
    }

    std::shared_ptr<arrow::ArrayData> BuildIdColumn(size_t rows) {
        arrow::Int32Builder id_builder;
        for (size_t i = 0; i < rows; ++i) {
            EXPECT_TRUE(id_builder.Append(static_cast<int32_t>(i)).ok());
        }
        std::shared_ptr<arrow::Array> ids;
        EXPECT_TRUE(id_builder.Finish(&ids).ok());
        return ids->data();
    }

    // Builds the offsets buffer of a repeated column, flattening its elements into `flat`.
    std::shared_ptr<arrow::Buffer> BuildOffsetsBuffer(
        const std::vector<std::vector<const char*>>& rows, std::vector<const char*>* flat) {
        arrow::Int32Builder offset_builder;
        for (const auto& row : rows) {
            EXPECT_TRUE(offset_builder.Append(static_cast<int32_t>(flat->size())).ok());
            flat->insert(flat->end(), row.begin(), row.end());
        }
        EXPECT_TRUE(offset_builder.Append(static_cast<int32_t>(flat->size())).ok());
        std::shared_ptr<arrow::Array> offsets;
        EXPECT_TRUE(offset_builder.Finish(&offsets).ok());
        return offsets->data()->buffers[1];
    }

    // Builds a `[id, arr: list<VARIANT>]` batch: row `i` holds the variants of `rows[i]`.
    //
    // The repeated columns here are assembled at the `ArrayData` level on purpose: Arrow's
    // `FromArrays` helpers `checked_cast` their arguments, which is a `dynamic_cast` in a debug
    // build and fails across the test binary / libpaimon boundary for templated array classes.
    std::shared_ptr<arrow::StructArray> BuildListArray(
        const std::vector<std::vector<const char*>>& rows) {
        std::vector<const char*> flat;
        auto offsets_buffer = BuildOffsetsBuffer(rows, &flat);
        // The element variants are built as one flat batch that the offsets slice into rows.
        auto elements = paimon::test::VariantTestData::BuildVariantBatch(
            list_schema_->field(0), list_element_field_, flat, pool_);
        EXPECT_TRUE(elements.ok()) << elements.status().ToString();
        auto list_data = arrow::ArrayData::Make(
            list_schema_->field(1)->type(), static_cast<int64_t>(rows.size()),
            {nullptr, offsets_buffer}, {elements.value()->field(1)->data()}, /*null_count=*/0);
        auto batch_data = arrow::ArrayData::Make(
            arrow::struct_(list_schema_->fields()), static_cast<int64_t>(rows.size()), {nullptr},
            {BuildIdColumn(rows.size()), list_data}, /*null_count=*/0);
        return std::make_shared<arrow::StructArray>(batch_data);
    }

    // Writes `rows` into the unshredded `arr: list<VARIANT>` column.
    void WriteListFile(const std::vector<std::vector<const char*>>& rows) {
        auto arrow_array = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*BuildListArray(rows), arrow_array.get()).ok());
        WriteFile(list_schema_, arrow_array.get());
    }

    // Builds a `[id, m: map<STRING, VARIANT>]` batch: row `i` maps `keys[i][k]` to the variant of
    // `rows[i][k]`.
    std::shared_ptr<arrow::StructArray> BuildMapArray(
        const std::vector<std::vector<std::string>>& keys,
        const std::vector<std::vector<const char*>>& rows) {
        std::vector<const char*> flat;
        arrow::StringBuilder key_builder;
        EXPECT_EQ(keys.size(), rows.size());
        for (size_t i = 0; i < keys.size() && i < rows.size(); ++i) {
            EXPECT_EQ(keys[i].size(), rows[i].size()) << "row " << i;
            for (const auto& key : keys[i]) {
                EXPECT_TRUE(key_builder.Append(key).ok());
            }
        }
        auto offsets_buffer = BuildOffsetsBuffer(rows, &flat);
        std::shared_ptr<arrow::Array> map_keys;
        EXPECT_TRUE(key_builder.Finish(&map_keys).ok());
        auto elements = paimon::test::VariantTestData::BuildVariantBatch(
            map_schema_->field(0), map_item_field_, flat, pool_);
        EXPECT_TRUE(elements.ok()) << elements.status().ToString();
        // A map array is a list of `struct<key, value>` entries.
        auto entries_data = arrow::ArrayData::Make(
            map_schema_->field(1)->type()->field(0)->type(), static_cast<int64_t>(flat.size()),
            {nullptr}, {map_keys->data(), elements.value()->field(1)->data()}, /*null_count=*/0);
        auto map_data =
            arrow::ArrayData::Make(map_schema_->field(1)->type(), static_cast<int64_t>(rows.size()),
                                   {nullptr, offsets_buffer}, {entries_data}, /*null_count=*/0);
        auto batch_data = arrow::ArrayData::Make(
            arrow::struct_(map_schema_->fields()), static_cast<int64_t>(rows.size()), {nullptr},
            {BuildIdColumn(rows.size()), map_data}, /*null_count=*/0);
        return std::make_shared<arrow::StructArray>(batch_data);
    }

    // Writes `rows` into the unshredded `m: map<STRING, VARIANT>` column.
    void WriteMapFile(const std::vector<std::vector<std::string>>& keys,
                      const std::vector<std::vector<const char*>>& rows) {
        auto arrow_array = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*BuildMapArray(keys, rows), arrow_array.get()).ok());
        WriteFile(map_schema_, arrow_array.get());
    }

    // Builds a `[id, arr2: list<struct<v: VARIANT, t: STRING>>]` batch. The sibling of the
    // `k`-th element over the whole column is `NestedSiblingValue(k)`.
    std::shared_ptr<arrow::StructArray> BuildListStructArray(
        const std::vector<std::vector<const char*>>& rows) {
        std::vector<const char*> flat;
        auto offsets_buffer = BuildOffsetsBuffer(rows, &flat);
        auto elements = paimon::test::VariantTestData::BuildVariantBatch(
            list_struct_schema_->field(0), list_struct_variant_field_, flat, pool_);
        EXPECT_TRUE(elements.ok()) << elements.status().ToString();
        arrow::StringBuilder sibling_builder;
        for (size_t i = 0; i < flat.size(); ++i) {
            EXPECT_TRUE(sibling_builder.Append(NestedSiblingValue(i)).ok());
        }
        std::shared_ptr<arrow::Array> sibling;
        EXPECT_TRUE(sibling_builder.Finish(&sibling).ok());
        auto element_data = arrow::ArrayData::Make(
            list_struct_schema_->field(1)->type()->field(0)->type(),
            static_cast<int64_t>(flat.size()), {nullptr},
            {elements.value()->field(1)->data(), sibling->data()}, /*null_count=*/0);
        auto list_data = arrow::ArrayData::Make(
            list_struct_schema_->field(1)->type(), static_cast<int64_t>(rows.size()),
            {nullptr, offsets_buffer}, {element_data}, /*null_count=*/0);
        auto batch_data = arrow::ArrayData::Make(
            arrow::struct_(list_struct_schema_->fields()), static_cast<int64_t>(rows.size()),
            {nullptr}, {BuildIdColumn(rows.size()), list_data}, /*null_count=*/0);
        return std::make_shared<arrow::StructArray>(batch_data);
    }

    // Writes `rows` into the unshredded `arr2: list<struct<v: VARIANT, t: STRING>>` column.
    void WriteListStructFile(const std::vector<std::vector<const char*>>& rows) {
        auto arrow_array = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*BuildListStructArray(rows), arrow_array.get()).ok());
        WriteFile(list_struct_schema_, arrow_array.get());
    }

    // Builds a `[id, s: struct<nv, t>]` batch holding the variant encodings of `jsons`.
    std::shared_ptr<arrow::StructArray> BuildNestedArray(const std::vector<const char*>& jsons) {
        auto batch = paimon::test::VariantTestData::BuildVariantBatch(
            nested_schema_->field(0), nested_variant_field_, jsons, pool_);
        EXPECT_TRUE(batch.ok()) << batch.status().ToString();
        arrow::StringBuilder sibling_builder;
        for (size_t i = 0; i < jsons.size(); ++i) {
            EXPECT_TRUE(sibling_builder.Append(NestedSiblingValue(i)).ok());
        }
        std::shared_ptr<arrow::Array> sibling;
        EXPECT_TRUE(sibling_builder.Finish(&sibling).ok());
        auto struct_column = arrow::StructArray::Make(
            {batch.value()->field(1), sibling}, {nested_variant_field_, nested_sibling_field_});
        EXPECT_TRUE(struct_column.ok()) << struct_column.status().ToString();
        auto nested =
            arrow::StructArray::Make({batch.value()->field(0), struct_column.ValueOrDie()},
                                     {nested_schema_->field(0), nested_schema_->field(1)});
        EXPECT_TRUE(nested.ok()) << nested.status().ToString();
        return nested.ValueOrDie();
    }

    // Writes `jsons` into `s.nv` unshredded.
    void WriteNestedFile(const std::vector<const char*>& jsons) {
        auto arrow_array = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*BuildNestedArray(jsons), arrow_array.get()).ok());
        WriteFile(nested_schema_, arrow_array.get());
    }

    // Writes `jsons` into `s.nv` shredded by `shredding_type` (the nested variant is addressed by
    // its field-index path `{1, 0}`).
    void WriteShreddedNestedFile(const std::vector<const char*>& jsons,
                                 const std::shared_ptr<arrow::DataType>& shredding_type) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<VariantShreddingWritePlan> plan,
                             VariantShreddingWritePlan::CreateFromPaths(
                                 nested_schema_, {{std::vector<int32_t>{1, 0}, shredding_type}}));
        ASSERT_NE(plan, nullptr);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<VariantShreddingBatchConverter> converter,
                             VariantShreddingBatchConverter::Create(plan, pool_));
        auto c_logical = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*BuildNestedArray(jsons), c_logical.get()).ok());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ArrowArray> c_physical,
                             converter->Convert(c_logical.get()));
        WriteFile(converter->GetPhysicalSchema(), c_physical.get());
    }

    // Builds the read schema projecting `s.nv` as the given variant-access projection, keeping
    // the plain sibling `s.t`.
    std::shared_ptr<arrow::Schema> BuildNestedAccessReadSchema(
        const std::vector<std::pair<std::shared_ptr<arrow::DataType>, std::string>>& accesses) {
        auto access_field = BuildAccessField(accesses, "nv");
        auto read_struct = nested_schema_->field(1)->WithType(
            arrow::struct_({access_field, nested_sibling_field_}));
        return arrow::schema({nested_schema_->field(0), read_struct});
    }

    // Asserts that the plain sibling column of `s` round-tripped unchanged.
    void ExpectNestedSibling(const std::shared_ptr<arrow::StructArray>& s_column) {
        const auto& sibling = static_cast<const arrow::StringArray&>(*s_column->field(1));
        for (int64_t i = 0; i < s_column->length(); ++i) {
            EXPECT_EQ(sibling.GetString(i), NestedSiblingValue(static_cast<size_t>(i)));
        }
    }

    // Opens the written file and returns the reader plus its imported file schema.
    void OpenFile(std::unique_ptr<FileBatchReader>* file_reader,
                  std::shared_ptr<arrow::Schema>* file_schema) {
        ASSERT_OK_AND_ASSIGN(auto input_stream, fs_->Open(file_path_));
        auto length = fs_->GetFileStatus(file_path_).value()->GetLen();
        auto in_stream =
            std::make_unique<ArrowInputStreamAdapter>(std::move(input_stream), length, arrow_pool_);
        std::map<std::string, std::string> options = {};
        ASSERT_OK_AND_ASSIGN(auto parquet_reader, ParquetFileBatchReader::Create(
                                                      std::move(in_stream), options,
                                                      /*batch_size=*/1024,
                                                      /*file_metadata=*/nullptr,
                                                      /*storage_read_bytes=*/nullptr, arrow_pool_));
        *file_reader = std::move(parquet_reader);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<::ArrowSchema> c_file_schema,
                             (*file_reader)->GetFileSchema());
        auto imported = arrow::ImportSchema(c_file_schema.get());
        ASSERT_TRUE(imported.ok()) << imported.status().ToString();
        *file_schema = imported.ValueOrDie();
    }

    // Builds a variant-access projection field for a variant column via the public builder.
    std::shared_ptr<arrow::Field> BuildAccessField(
        const std::vector<std::pair<std::shared_ptr<arrow::DataType>, std::string>>& accesses,
        const std::string& field_name = "v") {
        VariantAccessBuilder builder;
        for (const auto& [type, path] : accesses) {
            auto c_target = std::make_unique<ArrowSchema>();
            EXPECT_TRUE(arrow::ExportField(arrow::Field("t", type), c_target.get()).ok());
            EXPECT_OK(builder.AddField(c_target.get(), path, /*fail_on_error=*/false));
        }
        auto c_field = builder.Build(field_name);
        EXPECT_TRUE(c_field.ok()) << c_field.status().ToString();
        auto imported = arrow::ImportField(c_field.value().get());
        EXPECT_TRUE(imported.ok()) << imported.status().ToString();
        return imported.ValueOrDie();
    }

    // Reads the whole file through the shredding reader with the given read schema and returns
    // the second (variant) column.
    void ReadColumn(const std::shared_ptr<arrow::Schema>& read_schema,
                    std::shared_ptr<arrow::Array>* column) {
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
        *column = result_struct->field(1);
        shredding_reader->Close();
        // The assembled arrays borrow the reader's memory pool; keep the reader alive until the
        // fixture is torn down (fixture members outlive test-body locals).
        live_readers_.push_back(std::move(shredding_reader));
    }

    // `ReadColumn` for the cases whose second column is a struct.
    void ReadVariantColumn(const std::shared_ptr<arrow::Schema>& read_schema,
                           std::shared_ptr<arrow::StructArray>* v_column) {
        std::shared_ptr<arrow::Array> column;
        ReadColumn(read_schema, &column);
        ASSERT_EQ(column->type_id(), arrow::Type::STRUCT);
        *v_column = std::static_pointer_cast<arrow::StructArray>(column);
    }

 protected:
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::string file_path_;
    std::shared_ptr<arrow::Schema> paimon_schema_;
    std::shared_ptr<arrow::Schema> nested_schema_;
    std::shared_ptr<arrow::Field> nested_variant_field_;
    std::shared_ptr<arrow::Field> nested_sibling_field_;
    std::shared_ptr<arrow::Schema> list_schema_;
    std::shared_ptr<arrow::Field> list_element_field_;
    std::shared_ptr<arrow::Schema> map_schema_;
    std::shared_ptr<arrow::Field> map_item_field_;
    std::shared_ptr<arrow::Schema> list_struct_schema_;
    std::shared_ptr<arrow::Field> list_struct_variant_field_;
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
        std::make_unique<ArrowInputStreamAdapter>(std::move(input_stream), length, arrow_pool_);
    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(auto batch_reader, ParquetFileBatchReader::Create(
                                                std::move(in_stream), options,
                                                /*batch_size=*/1024,
                                                /*file_metadata=*/nullptr,
                                                /*storage_read_bytes=*/nullptr, arrow_pool_));
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

TEST_F(VariantParquetTest, UntypedPhysicalVariantWriteAndReadRoundTrip) {
    std::vector<const char*> jsons = {
        R"({"a": 1, "b": "hello"})",
        nullptr,
        "[1,2,3]",
    };
    WriteShreddedFile(jsons, arrow::null());

    {
        std::unique_ptr<FileBatchReader> file_reader;
        std::shared_ptr<arrow::Schema> file_schema;
        OpenFile(&file_reader, &file_schema);
        auto file_variant_field = file_schema->GetFieldByName("v");
        ASSERT_NE(file_variant_field, nullptr);
        const auto& physical_type =
            static_cast<const arrow::StructType&>(*file_variant_field->type());
        ASSERT_EQ(physical_type.num_fields(), 2);
        ASSERT_EQ(physical_type.field(0)->name(), VariantDefs::kMetadataFieldName);
        ASSERT_EQ(physical_type.field(1)->name(), VariantDefs::kValueFieldName);
        ASSERT_FALSE(VariantShreddingUtils::IsShreddedFileType(file_variant_field->type()));
        ASSERT_TRUE(
            VariantShreddingUtils::IsUntypedPhysicalVariantType(file_variant_field->type()));
        file_reader->Close();
    }

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

TEST_F(VariantParquetTest, NestedVariantPlainReadReassembles) {
    std::vector<const char*> jsons = {R"({"age": 35, "city": "Chicago"})", nullptr};
    WriteShreddedNestedFile(jsons, arrow::struct_({arrow::field("age", arrow::int32()),
                                                   arrow::field("city", arrow::utf8())}));

    // Read as a plain nested VARIANT: the shredded sub-columns are reassembled back into
    // `struct<value, metadata>`.
    std::shared_ptr<arrow::StructArray> s_column;
    ReadVariantColumn(nested_schema_, &s_column);
    const auto& nv = static_cast<const arrow::StructArray&>(*s_column->field(0));
    ASSERT_TRUE(nv.type()->Equals(*nested_variant_field_->type()));
    const auto& value_column = static_cast<const arrow::BinaryArray&>(*nv.field(0));
    const auto& metadata_column = static_cast<const arrow::BinaryArray&>(*nv.field(1));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<GenericVariant> row0,
        GenericVariant::Create(value_column.GetView(0), metadata_column.GetView(0), pool_));
    ASSERT_OK_AND_ASSIGN(std::string row0_json, row0->ToJson());
    ASSERT_EQ(row0_json, R"({"age":35,"city":"Chicago"})");
    ASSERT_TRUE(nv.IsNull(1));
    ExpectNestedSibling(s_column);
}

TEST_F(VariantParquetTest, NestedVariantAccessReadShreddedFile) {
    std::vector<const char*> jsons = {R"({"age": 35, "city": "Chicago"})",
                                      R"({"age": 25, "other": "Hello"})", nullptr};
    WriteShreddedNestedFile(jsons, arrow::struct_({arrow::field("age", arrow::int32()),
                                                   arrow::field("city", arrow::utf8())}));

    auto read_schema =
        BuildNestedAccessReadSchema({{arrow::int64(), "$.age"}, {arrow::utf8(), "$.other"}});

    // A nested projection prunes the scan the same way a top-level one does: `typed_value` is
    // narrowed to `age`, and `value` is kept because `$.other` is not shredded.
    {
        std::unique_ptr<FileBatchReader> file_reader;
        std::shared_ptr<arrow::Schema> file_schema;
        OpenFile(&file_reader, &file_schema);
        ASSERT_OK_AND_ASSIGN(auto plans, VariantShreddingReadPlanFactory::CreateReadPlans(
                                             read_schema, file_schema, pool_));
        ASSERT_EQ(plans.size(), 1);
        const auto& physical_struct =
            static_cast<const arrow::StructType&>(*plans.at("s")->PhysicalField()->type());
        auto physical_nv = physical_struct.GetFieldByName("nv");
        ASSERT_NE(physical_nv, nullptr);
        const auto& physical_nv_type = static_cast<const arrow::StructType&>(*physical_nv->type());
        ASSERT_NE(physical_nv_type.GetFieldByName(VariantDefs::kMetadataFieldName), nullptr);
        ASSERT_NE(physical_nv_type.GetFieldByName(VariantDefs::kValueFieldName), nullptr);
        auto typed_value = physical_nv_type.GetFieldByName(VariantDefs::kTypedValueFieldName);
        ASSERT_NE(typed_value, nullptr);
        const auto& typed_struct = static_cast<const arrow::StructType&>(*typed_value->type());
        ASSERT_EQ(typed_struct.num_fields(), 1);
        ASSERT_NE(typed_struct.GetFieldByName("age"), nullptr);
        file_reader->Close();
    }

    std::shared_ptr<arrow::StructArray> s_column;
    ReadVariantColumn(read_schema, &s_column);
    ASSERT_EQ(s_column->length(), 3);
    const auto& nv = static_cast<const arrow::StructArray&>(*s_column->field(0));
    const auto& age = static_cast<const arrow::Int64Array&>(*nv.field(0));
    const auto& other = static_cast<const arrow::StringArray&>(*nv.field(1));
    // The extracted values must match the requested access struct, not `struct<value, metadata>`.
    ASSERT_EQ(nv.num_fields(), 2);
    ASSERT_EQ(age.Value(0), 35);
    ASSERT_EQ(age.Value(1), 25);
    ASSERT_TRUE(other.IsNull(0));
    ASSERT_EQ(other.GetString(1), "Hello");
    // Row 2's nested variant is null, so the whole projection is null.
    ASSERT_TRUE(nv.IsNull(2));
    ExpectNestedSibling(s_column);
}

TEST_F(VariantParquetTest, NestedVariantAccessReadUnshreddedFile) {
    std::vector<const char*> jsons = {R"({"age": 35, "city": "Chicago"})",
                                      R"({"age": 25, "other": "Hello"})", nullptr};
    WriteNestedFile(jsons);

    // The nested column is stored unshredded: the paths are extracted from the `value` binary,
    // which still requires a read plan (the projection type must never reach the format reader).
    auto read_schema =
        BuildNestedAccessReadSchema({{arrow::int64(), "$.age"}, {arrow::utf8(), "$.other"}});
    std::shared_ptr<arrow::StructArray> s_column;
    ReadVariantColumn(read_schema, &s_column);
    ASSERT_EQ(s_column->length(), 3);
    const auto& nv = static_cast<const arrow::StructArray&>(*s_column->field(0));
    const auto& age = static_cast<const arrow::Int64Array&>(*nv.field(0));
    const auto& other = static_cast<const arrow::StringArray&>(*nv.field(1));
    ASSERT_EQ(age.Value(0), 35);
    ASSERT_EQ(age.Value(1), 25);
    ASSERT_TRUE(other.IsNull(0));
    ASSERT_EQ(other.GetString(1), "Hello");
    ASSERT_TRUE(nv.IsNull(2));
    ExpectNestedSibling(s_column);
}

TEST_F(VariantParquetTest, ListVariantPlainReadNeedsNoPlan) {
    // A variant inside a repeated group is never shredded, so a plain read of it still needs no
    // plan at all: the logical type is exactly what the file stores.
    WriteListFile({{R"({"x": 1})", R"({"x": 2})"}, {R"({"x": 3})"}});
    std::unique_ptr<FileBatchReader> file_reader;
    std::shared_ptr<arrow::Schema> file_schema;
    OpenFile(&file_reader, &file_schema);
    ASSERT_OK_AND_ASSIGN(auto plans, VariantShreddingReadPlanFactory::CreateReadPlans(
                                         list_schema_, file_schema, pool_));
    ASSERT_TRUE(plans.empty());
    file_reader->Close();
}

TEST_F(VariantParquetTest, ListVariantAccessRead) {
    // A variant-access projection inside an ARRAY extracts the paths per element (as in Java's
    // testReadNestedVariantInArray). The empty row and the null element cover the offsets and
    // the element validity being carried over by the reassembly.
    WriteListFile(
        {{R"({"x": 1, "y": 2})", R"({"x": 3, "y": 4})"}, {}, {R"({"x": 5, "y": 6})", nullptr}});

    auto access_field = BuildAccessField({{arrow::int64(), "$.x"}}, "element");
    auto read_schema = arrow::schema(
        {list_schema_->field(0), list_schema_->field(1)->WithType(arrow::list(access_field))});

    // The parquet reader rejects partial projection inside a repeated group, so the whole file
    // subtree is pushed down and only reassembled back.
    {
        std::unique_ptr<FileBatchReader> file_reader;
        std::shared_ptr<arrow::Schema> file_schema;
        OpenFile(&file_reader, &file_schema);
        ASSERT_OK_AND_ASSIGN(auto plans, VariantShreddingReadPlanFactory::CreateReadPlans(
                                             read_schema, file_schema, pool_));
        ASSERT_EQ(plans.size(), 1);
        ASSERT_TRUE(plans.at("arr")->PhysicalField()->type()->Equals(
            *file_schema->GetFieldByName("arr")->type()));
        file_reader->Close();
    }

    std::shared_ptr<arrow::Array> arr_column;
    ReadColumn(read_schema, &arr_column);
    ASSERT_NE(arr_column, nullptr);
    const auto& list = static_cast<const arrow::ListArray&>(*arr_column);
    ASSERT_EQ(list.length(), 3);
    ASSERT_EQ(list.value_length(0), 2);
    ASSERT_EQ(list.value_length(1), 0);
    ASSERT_EQ(list.value_length(2), 2);
    const auto& elements = static_cast<const arrow::StructArray&>(*list.values());
    const auto& x = static_cast<const arrow::Int64Array&>(*elements.field(0));
    ASSERT_EQ(x.Value(list.value_offset(0)), 1);
    ASSERT_EQ(x.Value(list.value_offset(0) + 1), 3);
    ASSERT_EQ(x.Value(list.value_offset(2)), 5);
    // The null element's whole projection is null.
    ASSERT_TRUE(elements.IsNull(list.value_offset(2) + 1));
}

TEST_F(VariantParquetTest, ListOfStructVariantAccessRead) {
    // A variant one struct level below the ARRAY: the plan descends list -> struct -> variant
    // and must leave the struct's plain sibling untouched.
    WriteListStructFile({{R"({"x": 1, "y": 2})", R"({"x": 3})"}, {R"({"x": 5})"}});

    auto access_field = BuildAccessField({{arrow::int64(), "$.x"}}, "v");
    auto element_field =
        arrow::field("element", arrow::struct_({access_field, nested_sibling_field_}));
    auto read_schema =
        arrow::schema({list_struct_schema_->field(0),
                       list_struct_schema_->field(1)->WithType(arrow::list(element_field))});

    std::shared_ptr<arrow::Array> arr_column;
    ReadColumn(read_schema, &arr_column);
    ASSERT_NE(arr_column, nullptr);
    const auto& list = static_cast<const arrow::ListArray&>(*arr_column);
    ASSERT_EQ(list.length(), 2);
    const auto& elements = static_cast<const arrow::StructArray&>(*list.values());
    // Each element is `struct<v: <access projection>, t: STRING>`, so the extracted path sits one
    // struct level below the element.
    const auto& access = static_cast<const arrow::StructArray&>(*elements.field(0));
    const auto& x = static_cast<const arrow::Int64Array&>(*access.field(0));
    const auto& sibling = static_cast<const arrow::StringArray&>(*elements.field(1));
    ASSERT_EQ(x.Value(0), 1);
    ASSERT_EQ(x.Value(1), 3);
    ASSERT_EQ(x.Value(2), 5);
    for (int64_t i = 0; i < 3; ++i) {
        EXPECT_EQ(sibling.GetString(i), NestedSiblingValue(static_cast<size_t>(i)));
    }
}

TEST_F(VariantParquetTest, ListPartialProjectionNeedsNoPlan) {
    // Projecting a subset of a struct inside a repeated group is unsupported, so no plan is
    // built: the read fails in the reader instead of assembling a mistyped column.
    WriteListStructFile({{R"({"x": 1})"}});

    auto access_field = BuildAccessField({{arrow::int64(), "$.x"}}, "v");
    auto element_field = arrow::field("element", arrow::struct_({access_field}));
    auto read_schema =
        arrow::schema({list_struct_schema_->field(0),
                       list_struct_schema_->field(1)->WithType(arrow::list(element_field))});

    std::unique_ptr<FileBatchReader> file_reader;
    std::shared_ptr<arrow::Schema> file_schema;
    OpenFile(&file_reader, &file_schema);
    ASSERT_OK_AND_ASSIGN(auto plans, VariantShreddingReadPlanFactory::CreateReadPlans(
                                         read_schema, file_schema, pool_));
    ASSERT_TRUE(plans.empty());
    file_reader->Close();
}

TEST_F(VariantParquetTest, MapVariantAccessRead) {
    // The same for a variant value inside a MAP: the plan descends through the map entries
    // struct and rewrites only the item child.
    WriteMapFile({{"a", "b"}, {"c", "d"}},
                 {{R"({"x": 1, "y": 2})", R"({"x": 3})"}, {R"({"x": 5, "y": 6})", nullptr}});

    auto access_field = BuildAccessField({{arrow::int64(), "$.x"}}, "value");
    auto read_schema =
        arrow::schema({map_schema_->field(0),
                       map_schema_->field(1)->WithType(arrow::map(arrow::utf8(), access_field))});

    std::shared_ptr<arrow::Array> m_column;
    ReadColumn(read_schema, &m_column);
    ASSERT_NE(m_column, nullptr);
    const auto& map = static_cast<const arrow::MapArray&>(*m_column);
    ASSERT_EQ(map.length(), 2);
    ASSERT_EQ(map.value_length(0), 2);
    ASSERT_EQ(map.value_length(1), 2);
    // The keys must survive untouched next to the rewritten values.
    const auto& keys = static_cast<const arrow::StringArray&>(*map.keys());
    ASSERT_EQ(keys.GetString(map.value_offset(0)), "a");
    ASSERT_EQ(keys.GetString(map.value_offset(0) + 1), "b");
    ASSERT_EQ(keys.GetString(map.value_offset(1)), "c");
    ASSERT_EQ(keys.GetString(map.value_offset(1) + 1), "d");
    const auto& items = static_cast<const arrow::StructArray&>(*map.items());
    const auto& x = static_cast<const arrow::Int64Array&>(*items.field(0));
    ASSERT_EQ(x.Value(map.value_offset(0)), 1);
    ASSERT_EQ(x.Value(map.value_offset(0) + 1), 3);
    ASSERT_EQ(x.Value(map.value_offset(1)), 5);
    // A null map value projects to a null row.
    ASSERT_TRUE(items.IsNull(map.value_offset(1) + 1));
}

}  // namespace paimon::parquet::test
