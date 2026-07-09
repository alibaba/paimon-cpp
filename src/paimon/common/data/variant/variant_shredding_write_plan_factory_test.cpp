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

    Result<CoreOptions> MakeOptions(std::map<std::string, std::string> options) {
        // Keep the manifest format resolvable in test binaries without the avro plugin.
        options.emplace("manifest.format", "parquet");
        return CoreOptions::FromMap(options);
    }

    std::shared_ptr<arrow::Array> BuildBatch(const std::vector<const char*>& jsons) {
        auto result = BuildVariantBatch(schema_->field(0), schema_->field(1), jsons, pool_);
        EXPECT_TRUE(result.ok()) << result.status().ToString();
        return std::move(result).value();
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::Schema> schema_;
};

TEST_F(VariantShreddingWritePlanFactoryTest, InactiveWithoutOptions) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options, MakeOptions({}));
    VariantShreddingWritePlanFactory factory(options, schema_, pool_);
    ASSERT_FALSE(factory.ShouldCreateWritePlan());
    ASSERT_FALSE(factory.ShouldInferWritePlan());
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
    VariantShreddingWritePlanFactory factory(options, schema_, pool_);
    ASSERT_TRUE(factory.ShouldCreateWritePlan());
    ASSERT_FALSE(factory.ShouldInferWritePlan());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ShreddingBatchConverter> converter,
                         factory.CreateConverter("parquet", {}));
    ASSERT_NE(converter, nullptr);
    auto variant_field = converter->GetPhysicalSchema()->GetFieldByName("v");
    ASSERT_NE(variant_field, nullptr);
    const auto& physical_type = static_cast<const arrow::StructType&>(*variant_field->type());
    ASSERT_NE(physical_type.GetFieldByName("typed_value"), nullptr);
    // Variant shredding only supports the parquet format.
    ASSERT_TRUE(factory.CreateConverter("orc", {}).status().IsNotImplemented());
}

TEST_F(VariantShreddingWritePlanFactoryTest, InferredSchema) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         MakeOptions({{"variant.inferShreddingSchema", "true"}}));
    VariantShreddingWritePlanFactory factory(options, schema_, pool_);
    ASSERT_TRUE(factory.ShouldCreateWritePlan());
    ASSERT_TRUE(factory.ShouldInferWritePlan());
    ASSERT_EQ(factory.InferBufferRowCount(), 4096);

    std::vector<std::shared_ptr<arrow::Array>> samples = {
        BuildBatch({R"({"age": 35, "city": "Chicago"})", R"({"age": 25, "city": "Hangzhou"})"}),
        BuildBatch({R"({"age": 18, "city": "Beijing"})", nullptr})};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ShreddingBatchConverter> converter,
                         factory.CreateConverter("parquet", samples));
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
    VariantShreddingWritePlanFactory factory(options, schema_, pool_);
    // With no useful samples the file stays unshredded.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ShreddingBatchConverter> converter,
                         factory.CreateConverter("parquet", {}));
    ASSERT_EQ(converter, nullptr);
}

}  // namespace paimon::test
