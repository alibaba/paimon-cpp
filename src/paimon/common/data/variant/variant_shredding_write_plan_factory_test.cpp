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

#include "paimon/common/data/variant/variant_shredding_write_plan_factory.h"

#include <map>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/core_options.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/variant_test_data.h"

namespace paimon::test {

class VariantShreddingWritePlanFactoryTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        std::vector<DataField> fields = {DataField(1, arrow::field("id", arrow::int32())),
                                         DataField(2, VariantTypeUtils::ToArrowField("v"))};
        schema_ = DataField::ConvertDataFieldsToArrowSchema(fields);
    }

    Result<CoreOptions> MakeOptions(std::map<std::string, std::string> options) const {
        // Keep the manifest format resolvable in test binaries without the avro plugin.
        options.emplace("manifest.format", "parquet");
        return CoreOptions::FromMap(options);
    }

    std::shared_ptr<arrow::Array> BuildBatch(const std::vector<const char*>& jsons) {
        auto result =
            VariantTestData::BuildVariantBatch(schema_->field(0), schema_->field(1), jsons, pool_);
        EXPECT_TRUE(result.ok()) << result.status().ToString();
        return std::move(result).value();
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::Schema> schema_;
};

TEST_F(VariantShreddingWritePlanFactoryTest, InactiveWithoutOptions) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options, MakeOptions({}));
    auto factory = VariantShreddingWritePlanFactory::Create(options, schema_, pool_);
    ASSERT_FALSE(factory->ShouldCreateWritePlan());
    ASSERT_FALSE(factory->ShouldInferWritePlan());
}

TEST_F(VariantShreddingWritePlanFactoryTest, ConfiguredSchema) {
    const char* shredding_schema_json = R"({
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
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         MakeOptions({{"variant.shreddingSchema", shredding_schema_json}}));
    auto factory = VariantShreddingWritePlanFactory::Create(options, schema_, pool_);
    ASSERT_TRUE(factory->ShouldCreateWritePlan());
    ASSERT_FALSE(factory->ShouldInferWritePlan());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ShreddingBatchConverter> converter,
                         factory->CreateConverter("parquet", {}));
    ASSERT_NE(converter, nullptr);
    auto variant_field = converter->GetPhysicalSchema()->GetFieldByName("v");
    ASSERT_NE(variant_field, nullptr);
    const auto& physical_type = static_cast<const arrow::StructType&>(*variant_field->type());
    ASSERT_NE(physical_type.GetFieldByName("typed_value"), nullptr);
    // Variant shredding only supports the parquet format.
    ASSERT_TRUE(factory->CreateConverter("orc", {}).status().IsNotImplemented());
}

TEST_F(VariantShreddingWritePlanFactoryTest, InferredSchema) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         MakeOptions({{"variant.inferShreddingSchema", "true"}}));
    auto factory = VariantShreddingWritePlanFactory::Create(options, schema_, pool_);
    ASSERT_TRUE(factory->ShouldCreateWritePlan());
    ASSERT_TRUE(factory->ShouldInferWritePlan());
    ASSERT_EQ(factory->InferBufferRowCount(), 4096);

    std::vector<std::shared_ptr<arrow::Array>> samples = {
        BuildBatch({R"({"age": 35, "city": "Chicago"})", R"({"age": 25, "city": "Hangzhou"})"}),
        BuildBatch({R"({"age": 18, "city": "Beijing"})", nullptr})};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ShreddingBatchConverter> converter,
                         factory->CreateConverter("parquet", samples));
    ASSERT_NE(converter, nullptr);
    auto variant_field = converter->GetPhysicalSchema()->GetFieldByName("v");
    ASSERT_NE(variant_field, nullptr);
    const auto& physical_type = static_cast<const arrow::StructType&>(*variant_field->type());
    auto typed_value = physical_type.GetFieldByName("typed_value");
    ASSERT_NE(typed_value, nullptr);
    const auto& typed_struct = static_cast<const arrow::StructType&>(*typed_value->type());
    ASSERT_NE(typed_struct.GetFieldByName("age"), nullptr);
    ASSERT_NE(typed_struct.GetFieldByName("city"), nullptr);
}

TEST_F(VariantShreddingWritePlanFactoryTest, InferredSchemaWithoutSamples) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         MakeOptions({{"variant.inferShreddingSchema", "true"}}));
    auto factory = VariantShreddingWritePlanFactory::Create(options, schema_, pool_);
    // With no useful samples the file stays unshredded.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ShreddingBatchConverter> converter,
                         factory->CreateConverter("parquet", {}));
    ASSERT_EQ(converter, nullptr);
}

TEST_F(VariantShreddingWritePlanFactoryTest, SharedWidthBudgetAcrossColumns) {
    // Two variant columns share one maxSchemaWidth budget (as in Java): with a small limit the
    // first column consumes it and the second column stays unshredded.
    std::vector<DataField> fields = {DataField(1, arrow::field("id", arrow::int32())),
                                     DataField(2, VariantTypeUtils::ToArrowField("v1")),
                                     DataField(3, VariantTypeUtils::ToArrowField("v2"))};
    auto schema = DataField::ConvertDataFieldsToArrowSchema(fields);
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         MakeOptions({{"variant.inferShreddingSchema", "true"},
                                      {"variant.shredding.maxSchemaWidth", "3"}}));
    auto factory = VariantShreddingWritePlanFactory::Create(options, schema, pool_);
    ASSERT_TRUE(factory->ShouldInferWritePlan());

    auto build_batch = [&](const std::vector<const char*>& v1_jsons,
                           const std::vector<const char*>& v2_jsons) {
        auto v1 =
            VariantTestData::BuildVariantBatch(schema->field(0), schema->field(1), v1_jsons, pool_);
        EXPECT_TRUE(v1.ok()) << v1.status().ToString();
        auto v2 =
            VariantTestData::BuildVariantBatch(schema->field(0), schema->field(2), v2_jsons, pool_);
        EXPECT_TRUE(v2.ok()) << v2.status().ToString();
        auto batch = arrow::StructArray::Make(
                         {v1.value()->field(0), v1.value()->field(1), v2.value()->field(1)},
                         {schema->field(0), schema->field(1), schema->field(2)})
                         .ValueOrDie();
        return std::shared_ptr<arrow::Array>(batch);
    };
    std::vector<std::shared_ptr<arrow::Array>> samples = {
        build_batch({R"({"a": 1, "b": 2})"}, {R"({"c": 3})"})};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ShreddingBatchConverter> converter,
                         factory->CreateConverter("parquet", samples));
    ASSERT_NE(converter, nullptr);
    const auto& physical = converter->GetPhysicalSchema();
    // v1 got the budget and is shredded; v2 exceeded it and stays unshredded.
    const auto& v1_type =
        static_cast<const arrow::StructType&>(*physical->GetFieldByName("v1")->type());
    ASSERT_NE(v1_type.GetFieldByName("typed_value"), nullptr);
    const auto& v2_type =
        static_cast<const arrow::StructType&>(*physical->GetFieldByName("v2")->type());
    ASSERT_EQ(v2_type.GetFieldByName("typed_value"), nullptr);
}

TEST_F(VariantShreddingWritePlanFactoryTest, NestedVariantInsideStruct) {
    // A VARIANT nested inside a ROW column is discovered, inferred and shredded (as in Java);
    // top-level detection also recurses through structs.
    auto nested_variant = VariantTypeUtils::ToArrowField("nv");
    auto struct_field = arrow::field("s", arrow::struct_({nested_variant}));
    std::vector<DataField> fields = {DataField(1, arrow::field("id", arrow::int32())),
                                     DataField(2, struct_field)};
    auto schema = DataField::ConvertDataFieldsToArrowSchema(fields);
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         MakeOptions({{"variant.inferShreddingSchema", "true"}}));
    auto factory = VariantShreddingWritePlanFactory::Create(options, schema, pool_);
    ASSERT_TRUE(factory->ShouldCreateWritePlan());
    ASSERT_TRUE(factory->ShouldInferWritePlan());

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::StructArray> variant_batch,
        VariantTestData::BuildVariantBatch(schema->field(0), schema->field(1)->type()->field(0),
                                           {R"({"age": 35, "city": "Chicago"})"}, pool_));
    std::shared_ptr<arrow::StructArray> struct_column =
        arrow::StructArray::Make({variant_batch->field(1)}, {schema->field(1)->type()->field(0)})
            .ValueOrDie();
    std::shared_ptr<arrow::StructArray> batch =
        arrow::StructArray::Make({variant_batch->field(0), struct_column},
                                 {schema->field(0), schema->field(1)})
            .ValueOrDie();
    std::vector<std::shared_ptr<arrow::Array>> samples = {batch};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ShreddingBatchConverter> converter,
                         factory->CreateConverter("parquet", samples));
    ASSERT_NE(converter, nullptr);
    const auto& physical = converter->GetPhysicalSchema();
    const auto& physical_struct =
        static_cast<const arrow::StructType&>(*physical->GetFieldByName("s")->type());
    const auto& physical_variant =
        static_cast<const arrow::StructType&>(*physical_struct.GetFieldByName("nv")->type());
    auto typed_value = physical_variant.GetFieldByName("typed_value");
    ASSERT_NE(typed_value, nullptr);
    const auto& typed_struct = static_cast<const arrow::StructType&>(*typed_value->type());
    ASSERT_NE(typed_struct.GetFieldByName("age"), nullptr);
    ASSERT_NE(typed_struct.GetFieldByName("city"), nullptr);

    // Child slot contents under a null struct row are unspecified in Arrow: a row whose parent
    // struct is null must be skipped by sampling and shredded to null by conversion, without
    // decoding the (here invalid) variant bytes underneath.
    arrow::BinaryBuilder garbage_builder;
    ASSERT_TRUE(garbage_builder.Append("not a variant").ok());
    std::shared_ptr<arrow::Array> garbage;
    ASSERT_TRUE(garbage_builder.Finish(&garbage).ok());
    std::shared_ptr<arrow::StructArray> garbage_variant =
        arrow::StructArray::Make({garbage, garbage}, {nested_variant->type()->field(0),
                                                      nested_variant->type()->field(1)})
            .ValueOrDie();
    // Mark the single parent row null (a zeroed bitmap) while the child slots keep their bytes.
    auto null_struct_column = std::make_shared<arrow::StructArray>(
        struct_field->type(), 1, arrow::ArrayVector{garbage_variant},
        arrow::AllocateEmptyBitmap(1).ValueOrDie(), 1);
    std::shared_ptr<arrow::StructArray> garbage_batch =
        arrow::StructArray::Make({variant_batch->field(0), null_struct_column},
                                 {schema->field(0), schema->field(1)})
            .ValueOrDie();

    // Sampling sees no usable value: the file stays unshredded.
    std::vector<std::shared_ptr<arrow::Array>> garbage_samples = {garbage_batch};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ShreddingBatchConverter> unshredded_converter,
                         factory->CreateConverter("parquet", garbage_samples));
    ASSERT_EQ(unshredded_converter, nullptr);

    // Conversion with the previously inferred plan shreds the row to null instead of failing.
    auto c_garbage_batch = std::make_unique<ArrowArray>();
    ASSERT_TRUE(arrow::ExportArray(*garbage_batch, c_garbage_batch.get()).ok());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ArrowArray> c_physical,
                         converter->Convert(c_garbage_batch.get()));
    auto physical_struct_type = arrow::struct_(physical->fields());
    std::shared_ptr<arrow::Array> physical_array =
        arrow::ImportArray(c_physical.get(), physical_struct_type).ValueOrDie();
    const auto& physical_row = static_cast<const arrow::StructArray&>(*physical_array);
    const auto& converted_struct = static_cast<const arrow::StructArray&>(*physical_row.field(1));
    ASSERT_TRUE(converted_struct.IsNull(0));
}

TEST_F(VariantShreddingWritePlanFactoryTest, ConfiguredSchemaMatchingNoColumn) {
    // A configured schema naming no variant column writes the file unshredded (as in Java)
    // instead of failing.
    const char* shredding_schema_json = R"({
        "type": "ROW",
        "fields": [ {
            "id": 0,
            "name": "not_a_column",
            "type": {"type": "ROW", "fields": [{"id": 1, "name": "a", "type": "INT"}]}
        } ]
    })";
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         MakeOptions({{"variant.shreddingSchema", shredding_schema_json}}));
    auto factory = VariantShreddingWritePlanFactory::Create(options, schema_, pool_);
    ASSERT_TRUE(factory->ShouldCreateWritePlan());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ShreddingBatchConverter> converter,
                         factory->CreateConverter("parquet", {}));
    ASSERT_EQ(converter, nullptr);
}

}  // namespace paimon::test
