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
#include "paimon/common/data/variant/variant_shredding_utils.h"
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

    static std::shared_ptr<arrow::DataType> ShreddedTypedValue(
        const arrow::StructType& typed_object, const std::string& field_name) {
        auto field = typed_object.GetFieldByName(field_name);
        if (field == nullptr || field->type()->id() != arrow::Type::STRUCT) {
            return nullptr;
        }
        return std::static_pointer_cast<arrow::StructType>(field->type())
            ->GetFieldByName("typed_value")
            ->type();
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

// Verifies that adaptive inference returns a complete physical row schema to the write-plan
// factory, and that committed evidence is reused only after the preceding file completes.
TEST_F(VariantShreddingWritePlanFactoryTest, AdaptiveInferenceReturnsCompletePhysicalSchema) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         MakeOptions({{"variant.inferShreddingSchema", "true"},
                                      {"variant.shredding.inferenceMode", "adaptive"}}));
    auto factory = VariantShreddingWritePlanFactory::Create(options, schema_, pool_);
    std::vector<std::shared_ptr<arrow::Array>> samples = {
        BuildBatch({R"({"age": 35, "city": "Chicago"})"})};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::DataType> expected_variant_type,
        VariantShreddingUtils::VariantShreddingSchema(arrow::struct_(
            {arrow::field("age", arrow::int64()), arrow::field("city", arrow::utf8())})));
    auto expected_schema =
        arrow::schema({schema_->field(0), schema_->field(1)->WithType(expected_variant_type)},
                      schema_->metadata());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ShreddingBatchConverter> first,
                         factory->CreateConverter("parquet", samples));
    ASSERT_NE(first, nullptr);
    ASSERT_TRUE(first->GetPhysicalSchema()->Equals(*expected_schema))
        << first->GetPhysicalSchema()->ToString();

    ASSERT_OK(factory->OnFileCompleted(first));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ShreddingBatchConverter> second,
                         factory->CreateConverter("parquet", {}));
    ASSERT_NE(second, nullptr);
    ASSERT_TRUE(second->GetPhysicalSchema()->Equals(*expected_schema))
        << second->GetPhysicalSchema()->ToString();
    ASSERT_OK(factory->OnFileCompleted(second));
}

TEST_F(VariantShreddingWritePlanFactoryTest, AdaptiveInferenceUsesAdmissionAndRetentionThresholds) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         MakeOptions({{"variant.inferShreddingSchema", "true"},
                                      {"variant.shredding.inferenceMode", "adaptive"},
                                      {"variant.shredding.maxInferBufferRow", "10"},
                                      {"variant.shredding.adaptive.maxInferBufferRow", "10"},
                                      {"variant.shredding.minFieldCardinalityRatio", "0.4"},
                                      {"variant.shredding.adaptive.retentionRatio", "0.2"}}));
    auto factory = VariantShreddingWritePlanFactory::Create(options, schema_, pool_);
    auto typed_value = [](const std::shared_ptr<ShreddingBatchConverter>& converter) {
        const auto& variant_type = static_cast<const arrow::StructType&>(
            *converter->GetPhysicalSchema()->GetFieldByName("v")->type());
        return variant_type.GetFieldByName("typed_value");
    };

    std::vector<std::shared_ptr<arrow::Array>> first_samples = {
        BuildBatch({R"({"legacy":"v","stable":1})", R"({"legacy":"v","stable":1})",
                    R"({"legacy":"v","stable":1})", R"({"legacy":"v","stable":1})",
                    R"({"legacy":"v","stable":1})", R"({"stable":1})", R"({"stable":1})",
                    R"({"stable":1})", R"({"stable":1})", R"({"stable":1})"})};
    ASSERT_OK_AND_ASSIGN(auto first, factory->CreateConverter("parquet", first_samples));
    auto first_typed = typed_value(first);
    ASSERT_NE(nullptr, first_typed);
    const auto& first_struct = static_cast<const arrow::StructType&>(*first_typed->type());
    ASSERT_TRUE(ShreddedTypedValue(first_struct, "legacy")->Equals(*arrow::utf8()));
    ASSERT_TRUE(ShreddedTypedValue(first_struct, "stable")->Equals(*arrow::int64()));
    ASSERT_OK(factory->OnFileCompleted(first));

    std::vector<std::shared_ptr<arrow::Array>> second_samples = {
        BuildBatch({R"({"emerging":true,"stable":2})", R"({"emerging":true,"stable":2})",
                    R"({"emerging":true,"stable":2})", R"({"emerging":true,"stable":2})",
                    R"({"emerging":true,"stable":2})", R"({"emerging":true,"stable":2})",
                    R"({"emerging":true,"stable":2})", R"({"emerging":true,"stable":2})",
                    R"({"emerging":true,"stable":2})", R"({"stable":2})"})};
    ASSERT_OK_AND_ASSIGN(auto second, factory->CreateConverter("parquet", second_samples));
    auto second_typed = typed_value(second);
    ASSERT_NE(nullptr, second_typed);
    const auto& second_struct = static_cast<const arrow::StructType&>(*second_typed->type());
    ASSERT_TRUE(ShreddedTypedValue(second_struct, "emerging")->Equals(*arrow::boolean()));
    ASSERT_TRUE(ShreddedTypedValue(second_struct, "legacy")->Equals(*arrow::utf8()));
    ASSERT_TRUE(ShreddedTypedValue(second_struct, "stable")->Equals(*arrow::int64()));
    ASSERT_OK(factory->OnFileCompleted(second));

    std::vector<std::shared_ptr<arrow::Array>> third_samples = {
        BuildBatch({R"({"stable":3})", R"({"stable":3})", R"({"stable":3})", R"({"stable":3})",
                    R"({"stable":3})", R"({"stable":3})", R"({"stable":3})", R"({"stable":3})",
                    R"({"stable":3})", R"({"stable":3})"})};
    ASSERT_OK_AND_ASSIGN(auto third, factory->CreateConverter("parquet", third_samples));
    auto third_typed = typed_value(third);
    ASSERT_NE(nullptr, third_typed);
    const auto& third_struct = static_cast<const arrow::StructType&>(*third_typed->type());
    ASSERT_TRUE(ShreddedTypedValue(third_struct, "emerging")->Equals(*arrow::boolean()));
    ASSERT_EQ(nullptr, third_struct.GetFieldByName("legacy"));
    ASSERT_TRUE(ShreddedTypedValue(third_struct, "stable")->Equals(*arrow::int64()));
    ASSERT_OK(factory->OnFileCompleted(third));
}

TEST_F(VariantShreddingWritePlanFactoryTest, AdaptiveInferenceOnShortRolledFile) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         MakeOptions({{"variant.inferShreddingSchema", "true"},
                                      {"variant.shredding.inferenceMode", "adaptive"},
                                      {"variant.shredding.maxInferBufferRow", "4"},
                                      {"variant.shredding.adaptive.maxInferBufferRow", "4"},
                                      {"variant.shredding.minFieldCardinalityRatio", "0.4"},
                                      {"variant.shredding.adaptive.retentionRatio", "0.2"}}));
    auto factory = VariantShreddingWritePlanFactory::Create(options, schema_, pool_);

    std::vector<std::shared_ptr<arrow::Array>> first_samples = {
        BuildBatch({R"({"legacy":"a","stable":1})", R"({"legacy":"b","stable":2})",
                    R"({"legacy":"c","stable":3})", R"({"legacy":"d","stable":4})"})};
    ASSERT_OK_AND_ASSIGN(auto first, factory->CreateConverter("parquet", first_samples));
    ASSERT_OK(factory->OnFileCompleted(first));

    std::vector<std::shared_ptr<arrow::Array>> short_samples = {
        BuildBatch({R"({"emerging":true,"stable":5})", R"({"emerging":false,"stable":6})",
                    R"({"emerging":true,"stable":7})"})};
    ASSERT_OK_AND_ASSIGN(auto second, factory->CreateConverter("parquet", short_samples));
    const auto& variant_type = static_cast<const arrow::StructType&>(
        *second->GetPhysicalSchema()->GetFieldByName("v")->type());
    const auto& typed_object =
        static_cast<const arrow::StructType&>(*variant_type.GetFieldByName("typed_value")->type());
    ASSERT_TRUE(ShreddedTypedValue(typed_object, "emerging")->Equals(*arrow::boolean()));
    ASSERT_TRUE(ShreddedTypedValue(typed_object, "legacy")->Equals(*arrow::utf8()));
    ASSERT_TRUE(ShreddedTypedValue(typed_object, "stable")->Equals(*arrow::int64()));
    ASSERT_OK(factory->OnFileCompleted(second));
}

TEST_F(VariantShreddingWritePlanFactoryTest,
       AdaptiveInferenceWidensScalarSelectedFromPriorEvidence) {
    std::vector<DataField> fields = {DataField(1, arrow::field("id", arrow::int32())),
                                     DataField(2, VariantTypeUtils::ToArrowField("first")),
                                     DataField(3, VariantTypeUtils::ToArrowField("second"))};
    auto schema = DataField::ConvertDataFieldsToArrowSchema(fields);
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         MakeOptions({{"variant.inferShreddingSchema", "true"},
                                      {"variant.shredding.inferenceMode", "adaptive"},
                                      {"variant.shredding.maxSchemaWidth", "6"}}));
    auto factory = VariantShreddingWritePlanFactory::Create(options, schema, pool_);
    auto build_batch = [&](const std::vector<const char*>& first_jsons,
                           const std::vector<const char*>& second_jsons) {
        auto first = VariantTestData::BuildVariantBatch(schema->field(0), schema->field(1),
                                                        first_jsons, pool_);
        EXPECT_TRUE(first.ok()) << first.status().ToString();
        auto second = VariantTestData::BuildVariantBatch(schema->field(0), schema->field(2),
                                                         second_jsons, pool_);
        EXPECT_TRUE(second.ok()) << second.status().ToString();
        return std::shared_ptr<arrow::Array>(
            arrow::StructArray::Make(
                {first.value()->field(0), first.value()->field(1), second.value()->field(1)},
                schema->fields())
                .ValueOrDie());
    };

    std::vector<std::shared_ptr<arrow::Array>> first_samples = {
        build_batch({R"({"a":1,"b":2})"}, {R"({"historical":12345})"})};
    ASSERT_OK_AND_ASSIGN(auto initial, factory->CreateConverter("parquet", first_samples));
    const auto& initial_first = static_cast<const arrow::StructType&>(
        *initial->GetPhysicalSchema()->GetFieldByName("first")->type());
    const auto& initial_first_typed =
        static_cast<const arrow::StructType&>(*initial_first.GetFieldByName("typed_value")->type());
    ASSERT_NE(nullptr, initial_first_typed.GetFieldByName("a"));
    ASSERT_NE(nullptr, initial_first_typed.GetFieldByName("b"));
    const auto& initial_second = static_cast<const arrow::StructType&>(
        *initial->GetPhysicalSchema()->GetFieldByName("second")->type());
    ASSERT_EQ(nullptr, initial_second.GetFieldByName("typed_value"));
    ASSERT_OK(factory->OnFileCompleted(initial));

    std::vector<std::shared_ptr<arrow::Array>> adaptive_samples = {build_batch({"1"}, {nullptr})};
    ASSERT_OK_AND_ASSIGN(auto adaptive, factory->CreateConverter("parquet", adaptive_samples));
    const auto& adaptive_first = static_cast<const arrow::StructType&>(
        *adaptive->GetPhysicalSchema()->GetFieldByName("first")->type());
    ASSERT_TRUE(adaptive_first.GetFieldByName("typed_value")->type()->Equals(*arrow::int64()));
    const auto& adaptive_second = static_cast<const arrow::StructType&>(
        *adaptive->GetPhysicalSchema()->GetFieldByName("second")->type());
    const auto& adaptive_second_typed = static_cast<const arrow::StructType&>(
        *adaptive_second.GetFieldByName("typed_value")->type());
    ASSERT_TRUE(ShreddedTypedValue(adaptive_second_typed, "historical")->Equals(*arrow::int64()));
    ASSERT_OK(factory->OnFileCompleted(adaptive));
}

TEST_F(VariantShreddingWritePlanFactoryTest, AdaptiveInferenceWithNestedVariant) {
    auto nested_variant = VariantTypeUtils::ToArrowField("payload");
    auto nested_field = arrow::field(
        "nested", arrow::struct_({arrow::field("label", arrow::utf8()), nested_variant}));
    std::vector<DataField> fields = {DataField(1, arrow::field("id", arrow::int32())),
                                     DataField(2, nested_field)};
    auto schema = DataField::ConvertDataFieldsToArrowSchema(fields);
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         MakeOptions({{"variant.inferShreddingSchema", "true"},
                                      {"variant.shredding.inferenceMode", "adaptive"},
                                      {"variant.shredding.maxInferBufferRow", "2"},
                                      {"variant.shredding.adaptive.maxInferBufferRow", "2"},
                                      {"variant.shredding.minFieldCardinalityRatio", "0.4"},
                                      {"variant.shredding.adaptive.retentionRatio", "0.2"}}));
    auto factory = VariantShreddingWritePlanFactory::Create(options, schema, pool_);
    auto build_nested_batch = [&](const std::vector<const char*>& labels,
                                  const std::vector<const char*>& jsons) {
        auto variants =
            VariantTestData::BuildVariantBatch(schema->field(0), nested_variant, jsons, pool_);
        EXPECT_TRUE(variants.ok()) << variants.status().ToString();
        arrow::StringBuilder label_builder;
        for (const char* label : labels) {
            EXPECT_TRUE(label_builder.Append(label).ok());
        }
        std::shared_ptr<arrow::Array> label_array;
        EXPECT_TRUE(label_builder.Finish(&label_array).ok());
        auto nested = arrow::StructArray::Make({label_array, variants.value()->field(1)},
                                               nested_field->type()->fields())
                          .ValueOrDie();
        return std::shared_ptr<arrow::Array>(
            arrow::StructArray::Make({variants.value()->field(0), nested}, schema->fields())
                .ValueOrDie());
    };
    auto nested_typed_object = [](const std::shared_ptr<ShreddingBatchConverter>& converter) {
        const auto& physical_nested = static_cast<const arrow::StructType&>(
            *converter->GetPhysicalSchema()->GetFieldByName("nested")->type());
        const auto& physical_variant = static_cast<const arrow::StructType&>(
            *physical_nested.GetFieldByName("payload")->type());
        return std::static_pointer_cast<arrow::StructType>(
            physical_variant.GetFieldByName("typed_value")->type());
    };

    std::vector<std::shared_ptr<arrow::Array>> first_samples = {build_nested_batch(
        {"first", "second"}, {R"({"legacy":"a","stable":1})", R"({"legacy":"b","stable":2})"})};
    ASSERT_OK_AND_ASSIGN(auto first, factory->CreateConverter("parquet", first_samples));
    auto first_typed = nested_typed_object(first);
    ASSERT_TRUE(ShreddedTypedValue(*first_typed, "legacy")->Equals(*arrow::utf8()));
    ASSERT_TRUE(ShreddedTypedValue(*first_typed, "stable")->Equals(*arrow::int64()));
    ASSERT_OK(factory->OnFileCompleted(first));

    std::vector<std::shared_ptr<arrow::Array>> second_samples = {build_nested_batch(
        {"third", "fourth"},
        {R"({"emerging":true,"stable":3})", R"({"emerging":false,"stable":4})"})};
    ASSERT_OK_AND_ASSIGN(auto second, factory->CreateConverter("parquet", second_samples));
    auto second_typed = nested_typed_object(second);
    ASSERT_TRUE(ShreddedTypedValue(*second_typed, "emerging")->Equals(*arrow::boolean()));
    ASSERT_TRUE(ShreddedTypedValue(*second_typed, "legacy")->Equals(*arrow::utf8()));
    ASSERT_TRUE(ShreddedTypedValue(*second_typed, "stable")->Equals(*arrow::int64()));
    ASSERT_OK(factory->OnFileCompleted(second));
}

TEST_F(VariantShreddingWritePlanFactoryTest, InferredSchemaWithoutSamples) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         MakeOptions({{"variant.inferShreddingSchema", "true"}}));
    auto factory = VariantShreddingWritePlanFactory::Create(options, schema_, pool_);
    // With no useful samples the complete physical plan keeps the Variant untyped.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ShreddingBatchConverter> converter,
                         factory->CreateConverter("parquet", {}));
    ASSERT_NE(converter, nullptr);
    const auto& physical_type = static_cast<const arrow::StructType&>(
        *converter->GetPhysicalSchema()->GetFieldByName("v")->type());
    ASSERT_EQ(physical_type.GetFieldByName("typed_value"), nullptr);
}

TEST_F(VariantShreddingWritePlanFactoryTest, MultipleVariantFieldsInferredIndependently) {
    std::vector<DataField> fields = {DataField(1, arrow::field("id", arrow::int32())),
                                     DataField(2, VariantTypeUtils::ToArrowField("v1")),
                                     DataField(3, VariantTypeUtils::ToArrowField("v2"))};
    auto schema = DataField::ConvertDataFieldsToArrowSchema(fields);
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         MakeOptions({{"variant.inferShreddingSchema", "true"},
                                      {"variant.shredding.inferenceMode", "adaptive"},
                                      {"variant.shredding.maxInferBufferRow", "2"},
                                      {"variant.shredding.adaptive.maxInferBufferRow", "2"},
                                      {"variant.shredding.minFieldCardinalityRatio", "0.4"},
                                      {"variant.shredding.adaptive.retentionRatio", "0.2"}}));
    auto factory = VariantShreddingWritePlanFactory::Create(options, schema, pool_);

    auto v1 = VariantTestData::BuildVariantBatch(
        schema->field(0), schema->field(1), {R"({"name":"Alice"})", R"({"name":"Bob"})"}, pool_);
    ASSERT_TRUE(v1.ok()) << v1.status().ToString();
    auto v2 = VariantTestData::BuildVariantBatch(schema->field(0), schema->field(2),
                                                 {R"({"age":30})", R"({"age":25})"}, pool_);
    ASSERT_TRUE(v2.ok()) << v2.status().ToString();
    std::shared_ptr<arrow::Array> batch =
        arrow::StructArray::Make({v1.value()->field(0), v1.value()->field(1), v2.value()->field(1)},
                                 schema->fields())
            .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto converter, factory->CreateConverter("parquet", {batch}));

    const auto& v1_physical = static_cast<const arrow::StructType&>(
        *converter->GetPhysicalSchema()->GetFieldByName("v1")->type());
    const auto& v1_typed =
        static_cast<const arrow::StructType&>(*v1_physical.GetFieldByName("typed_value")->type());
    ASSERT_TRUE(ShreddedTypedValue(v1_typed, "name")->Equals(*arrow::utf8()));

    const auto& v2_physical = static_cast<const arrow::StructType&>(
        *converter->GetPhysicalSchema()->GetFieldByName("v2")->type());
    const auto& v2_typed =
        static_cast<const arrow::StructType&>(*v2_physical.GetFieldByName("typed_value")->type());
    ASSERT_TRUE(ShreddedTypedValue(v2_typed, "age")->Equals(*arrow::int64()));
    ASSERT_OK(factory->OnFileCompleted(converter));

    auto next_v1 = VariantTestData::BuildVariantBatch(
        schema->field(0), schema->field(1),
        {R"({"emerging":true,"name":"Carol"})", R"({"emerging":false,"name":"Dave"})"}, pool_);
    ASSERT_TRUE(next_v1.ok()) << next_v1.status().ToString();
    auto next_v2 = VariantTestData::BuildVariantBatch(schema->field(0), schema->field(2),
                                                      {R"({"age":40})", R"({"age":45})"}, pool_);
    ASSERT_TRUE(next_v2.ok()) << next_v2.status().ToString();
    std::shared_ptr<arrow::Array> next_batch =
        arrow::StructArray::Make(
            {next_v1.value()->field(0), next_v1.value()->field(1), next_v2.value()->field(1)},
            schema->fields())
            .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto adaptive, factory->CreateConverter("parquet", {next_batch}));
    const auto& adaptive_v1 = static_cast<const arrow::StructType&>(
        *adaptive->GetPhysicalSchema()->GetFieldByName("v1")->type());
    const auto& adaptive_v1_typed =
        static_cast<const arrow::StructType&>(*adaptive_v1.GetFieldByName("typed_value")->type());
    ASSERT_TRUE(ShreddedTypedValue(adaptive_v1_typed, "emerging")->Equals(*arrow::boolean()));
    ASSERT_TRUE(ShreddedTypedValue(adaptive_v1_typed, "name")->Equals(*arrow::utf8()));
    const auto& adaptive_v2 = static_cast<const arrow::StructType&>(
        *adaptive->GetPhysicalSchema()->GetFieldByName("v2")->type());
    const auto& adaptive_v2_typed =
        static_cast<const arrow::StructType&>(*adaptive_v2.GetFieldByName("typed_value")->type());
    ASSERT_TRUE(ShreddedTypedValue(adaptive_v2_typed, "age")->Equals(*arrow::int64()));
    ASSERT_OK(factory->OnFileCompleted(adaptive));
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

    // Sampling sees no usable value: the complete physical plan keeps the Variant untyped.
    std::vector<std::shared_ptr<arrow::Array>> garbage_samples = {garbage_batch};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ShreddingBatchConverter> unshredded_converter,
                         factory->CreateConverter("parquet", garbage_samples));
    ASSERT_NE(unshredded_converter, nullptr);
    const auto& unshredded_struct = static_cast<const arrow::StructType&>(
        *unshredded_converter->GetPhysicalSchema()->GetFieldByName("s")->type());
    const auto& unshredded_variant =
        static_cast<const arrow::StructType&>(*unshredded_struct.GetFieldByName("nv")->type());
    ASSERT_EQ(unshredded_variant.GetFieldByName("typed_value"), nullptr);

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
    ASSERT_NE(converter, nullptr);
    ASSERT_TRUE(converter->GetPhysicalSchema()->Equals(*schema_));
}

}  // namespace paimon::test
