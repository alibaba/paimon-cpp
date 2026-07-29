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

#include "paimon/common/data/variant/variant_shredding_read_plan_factory.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/util/key_value_metadata.h"
#include "gtest/gtest.h"
#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/common/data/variant/variant_access_utils.h"
#include "paimon/common/data/variant/variant_defs.h"
#include "paimon/common/data/variant/variant_shredding_utils.h"
#include "paimon/common/data/variant/variant_shredding_writer.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/types/data_field.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class VariantShreddingReadPlanFactoryTest : public ::testing::Test {
 public:
    std::shared_ptr<GenericVariant> Variant(const std::string& json) {
        auto result = GenericVariant::FromJson(json, pool_);
        EXPECT_TRUE(result.ok()) << result.status().ToString();
        return result.value();
    }

    // The full shredded StructArray (struct{metadata, value, typed_value}) for one variant, using
    // the production writer.
    void MakeFullShredded(const std::shared_ptr<arrow::DataType>& logical,
                          const std::shared_ptr<GenericVariant>& variant,
                          std::shared_ptr<arrow::DataType>* physical_type,
                          std::shared_ptr<arrow::StructArray>* array) {
        ASSERT_OK_AND_ASSIGN(*physical_type,
                             VariantShreddingUtils::VariantShreddingSchema(logical));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<VariantSchema> schema,
                             VariantShreddingUtils::BuildVariantSchema(*physical_type));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<VariantShreddedColumnWriter> writer,
                             VariantShreddedColumnWriter::Create(schema, *physical_type,
                                                                 arrow::default_memory_pool()));
        ASSERT_OK(writer->Append(*variant));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> arr, writer->Finish());
        *array = std::static_pointer_cast<arrow::StructArray>(arr);
    }

    // The unshredded physical StructArray (struct{value, metadata}) for one variant.
    std::shared_ptr<arrow::StructArray> MakeUnshredded(const std::shared_ptr<GenericVariant>& v) {
        auto value = v->Value();
        EXPECT_TRUE(value.ok()) << value.status().ToString();
        return MakeUnshreddedRaw(value.value(), v->Metadata(), /*metadata_null=*/false);
    }

    std::shared_ptr<arrow::StructArray> MakeUnshreddedRaw(std::string_view value,
                                                          std::string_view metadata,
                                                          bool metadata_null) {
        arrow::BinaryBuilder value_builder;
        EXPECT_TRUE(value_builder.Append(value).ok());
        std::shared_ptr<arrow::Array> value_array;
        EXPECT_TRUE(value_builder.Finish(&value_array).ok());
        arrow::BinaryBuilder metadata_builder;
        if (metadata_null) {
            EXPECT_TRUE(metadata_builder.AppendNull().ok());
        } else {
            EXPECT_TRUE(metadata_builder.Append(metadata).ok());
        }
        std::shared_ptr<arrow::Array> metadata_array;
        EXPECT_TRUE(metadata_builder.Finish(&metadata_array).ok());
        auto made = arrow::StructArray::Make({value_array, metadata_array},
                                             std::vector<std::string>{"value", "metadata"});
        EXPECT_TRUE(made.ok()) << made.status().ToString();
        return made.ValueOrDie();
    }

    // A variant-access child: an arrow field carrying a `__VARIANT_METADATA` description.
    static std::shared_ptr<arrow::Field> AccessChild(const std::string& name,
                                                     const std::shared_ptr<arrow::DataType>& type,
                                                     const std::string& path) {
        return arrow::field(name, type, /*nullable=*/true,
                            arrow::key_value_metadata({DataField::DESCRIPTION},
                                                      {VariantAccessUtils::BuildVariantMetadata(
                                                          path, /*fail_on_error=*/false, "UTC")}));
    }

    std::shared_ptr<ShreddingColumnReadPlan> CreatePlan(
        const std::shared_ptr<arrow::Field>& read_field,
        const std::shared_ptr<arrow::Field>& file_field) {
        auto plans = VariantShreddingReadPlanFactory::CreateReadPlans(
            arrow::schema({read_field}), arrow::schema({file_field}), pool_);
        EXPECT_TRUE(plans.ok()) << plans.status().ToString();
        auto it = plans.value().find(read_field->name());
        EXPECT_NE(it, plans.value().end());
        return it->second;
    }

    static std::shared_ptr<arrow::Array> Int32Array(int32_t value) {
        arrow::Int32Builder builder;
        EXPECT_TRUE(builder.Append(value).ok());
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(builder.Finish(&array).ok());
        return array;
    }

 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(VariantShreddingReadPlanFactoryTest, FullVariantReadOfShreddedFile) {
    std::shared_ptr<GenericVariant> variant = Variant(R"({"a": 5})");
    std::shared_ptr<arrow::DataType> physical;
    std::shared_ptr<arrow::StructArray> shredded;
    MakeFullShredded(arrow::struct_({arrow::field("a", arrow::int64())}), variant, &physical,
                     &shredded);
    ASSERT_FALSE(HasFatalFailure());

    auto read_field = VariantTypeUtils::ToArrowField("v");
    auto file_field = arrow::field("v", physical);
    std::shared_ptr<ShreddingColumnReadPlan> plan = CreatePlan(read_field, file_field);
    ASSERT_NE(plan, nullptr);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> assembled,
                         plan->Assemble(shredded, arrow::default_memory_pool()));
    auto assembled_struct = std::static_pointer_cast<arrow::StructArray>(assembled);
    auto value_column = std::static_pointer_cast<arrow::BinaryArray>(assembled_struct->field(0));
    auto metadata_column = std::static_pointer_cast<arrow::BinaryArray>(assembled_struct->field(1));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<GenericVariant> rebuilt,
        GenericVariant::Create(value_column->GetView(0), metadata_column->GetView(0), pool_));
    ASSERT_OK_AND_ASSIGN(std::string json, rebuilt->ToJson());
    EXPECT_EQ(json, R"({"a":5})");

    // Assembling a non-struct physical array is an error.
    auto ints = Int32Array(1);
    ASSERT_NOK(plan->Assemble(ints, arrow::default_memory_pool()));
}

TEST_F(VariantShreddingReadPlanFactoryTest, FullVariantReadOfUntypedPhysicalFile) {
    std::shared_ptr<GenericVariant> variant = Variant(R"({"a": 5})");
    std::shared_ptr<arrow::DataType> physical;
    std::shared_ptr<arrow::StructArray> shredded;
    MakeFullShredded(arrow::null(), variant, &physical, &shredded);
    ASSERT_FALSE(HasFatalFailure());

    const auto& physical_struct = static_cast<const arrow::StructType&>(*physical);
    ASSERT_EQ(physical_struct.num_fields(), 2);
    ASSERT_EQ(physical_struct.field(0)->name(), VariantDefs::kMetadataFieldName);
    ASSERT_EQ(physical_struct.field(1)->name(), VariantDefs::kValueFieldName);
    ASSERT_FALSE(VariantShreddingUtils::IsShreddedFileType(physical));
    ASSERT_TRUE(VariantShreddingUtils::IsUntypedPhysicalVariantType(physical));

    auto read_field = VariantTypeUtils::ToArrowField("v");
    auto file_field = arrow::field("v", physical);
    std::shared_ptr<ShreddingColumnReadPlan> plan = CreatePlan(read_field, file_field);
    ASSERT_NE(plan, nullptr);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> assembled,
                         plan->Assemble(shredded, arrow::default_memory_pool()));
    auto assembled_struct = std::static_pointer_cast<arrow::StructArray>(assembled);
    ASSERT_EQ(assembled_struct->data()->child_data[0], shredded->data()->child_data[1]);
    ASSERT_EQ(assembled_struct->data()->child_data[1], shredded->data()->child_data[0]);
    auto value_column = std::static_pointer_cast<arrow::BinaryArray>(assembled_struct->field(0));
    auto metadata_column = std::static_pointer_cast<arrow::BinaryArray>(assembled_struct->field(1));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<GenericVariant> rebuilt,
        GenericVariant::Create(value_column->GetView(0), metadata_column->GetView(0), pool_));
    ASSERT_OK_AND_ASSIGN(std::string json, rebuilt->ToJson());
    EXPECT_EQ(json, R"({"a":5})");
}

TEST_F(VariantShreddingReadPlanFactoryTest, AccessProjectionOnUnshreddedFile) {
    std::shared_ptr<GenericVariant> variant = Variant(R"({"a": 5, "b": [10, 20], "c": "hi"})");
    std::shared_ptr<arrow::StructArray> unshredded = MakeUnshredded(variant);
    ASSERT_FALSE(HasFatalFailure());

    auto read_field =
        arrow::field("v", arrow::struct_({AccessChild("a", arrow::int64(), "$.a"),
                                          AccessChild("c", arrow::utf8(), "$.c"),
                                          AccessChild("b0", arrow::int64(), "$.b[0]"),
                                          AccessChild("missing", arrow::utf8(), "$.missing")}));
    // Unshredded file column: struct{value, metadata}.
    auto file_field = arrow::field("v", VariantTypeUtils::UnshreddedStructType());
    std::shared_ptr<ShreddingColumnReadPlan> plan = CreatePlan(read_field, file_field);
    ASSERT_NE(plan, nullptr);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> assembled,
                         plan->Assemble(unshredded, arrow::default_memory_pool()));
    auto row = std::static_pointer_cast<arrow::StructArray>(assembled);
    ASSERT_EQ(row->length(), 1);
    EXPECT_EQ(static_cast<const arrow::Int64Array&>(*row->field(0)).Value(0), 5);
    EXPECT_EQ(static_cast<const arrow::StringArray&>(*row->field(1)).GetString(0), "hi");
    EXPECT_EQ(static_cast<const arrow::Int64Array&>(*row->field(2)).Value(0), 10);
    EXPECT_TRUE(row->field(3)->IsNull(0));

    // A non-struct physical array is an error.
    auto ints = Int32Array(1);
    ASSERT_NOK(plan->Assemble(ints, arrow::default_memory_pool()));
}

TEST_F(VariantShreddingReadPlanFactoryTest, AccessProjectionRejectsNullMetadata) {
    std::shared_ptr<GenericVariant> variant = Variant(R"({"a": 5})");
    ASSERT_OK_AND_ASSIGN(std::string_view value, variant->Value());
    std::shared_ptr<arrow::StructArray> bad =
        MakeUnshreddedRaw(value, variant->Metadata(), /*metadata_null=*/true);
    ASSERT_FALSE(HasFatalFailure());

    auto read_field = arrow::field("v", arrow::struct_({AccessChild("a", arrow::int64(), "$.a")}));
    auto file_field = arrow::field("v", VariantTypeUtils::UnshreddedStructType());
    std::shared_ptr<ShreddingColumnReadPlan> plan = CreatePlan(read_field, file_field);
    ASSERT_NE(plan, nullptr);
    ASSERT_NOK(plan->Assemble(bad, arrow::default_memory_pool()));
}

TEST_F(VariantShreddingReadPlanFactoryTest, AccessProjectionOnShreddedFile) {
    // Extracting shredded object keys reads the typed sub-columns directly (with pruning).
    std::shared_ptr<GenericVariant> variant = Variant(R"({"a": 5, "b": "hi"})");
    std::shared_ptr<arrow::DataType> physical;
    std::shared_ptr<arrow::StructArray> shredded;
    MakeFullShredded(
        arrow::struct_({arrow::field("a", arrow::int64()), arrow::field("b", arrow::utf8())}),
        variant, &physical, &shredded);
    ASSERT_FALSE(HasFatalFailure());

    auto read_field = arrow::field("v", arrow::struct_({AccessChild("a", arrow::int64(), "$.a"),
                                                        AccessChild("b", arrow::utf8(), "$.b")}));
    auto file_field = arrow::field("v", physical);
    std::shared_ptr<ShreddingColumnReadPlan> plan = CreatePlan(read_field, file_field);
    ASSERT_NE(plan, nullptr);

    // With both keys shredded and requested, pruning keeps {metadata, typed_value} and drops the
    // top-level `value` column; project the full array down to match the pushed-down physical.
    auto made = arrow::StructArray::Make({shredded->field(0), shredded->field(2)},
                                         {std::string(VariantDefs::kMetadataFieldName),
                                          std::string(VariantDefs::kTypedValueFieldName)});
    ASSERT_TRUE(made.ok()) << made.status().ToString();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> assembled,
                         plan->Assemble(made.ValueOrDie(), arrow::default_memory_pool()));
    auto row = std::static_pointer_cast<arrow::StructArray>(assembled);
    EXPECT_EQ(static_cast<const arrow::Int64Array&>(*row->field(0)).Value(0), 5);
    EXPECT_EQ(static_cast<const arrow::StringArray&>(*row->field(1)).GetString(0), "hi");
}

TEST_F(VariantShreddingReadPlanFactoryTest, NestedVariantColumnAndTypeMismatch) {
    // A struct column holding a variant child: the plan rebuilds the struct around the reassembled
    // variant.
    std::shared_ptr<GenericVariant> variant = Variant(R"({"a": 5})");
    std::shared_ptr<arrow::DataType> physical;
    std::shared_ptr<arrow::StructArray> shredded;
    MakeFullShredded(arrow::struct_({arrow::field("a", arrow::int64())}), variant, &physical,
                     &shredded);
    ASSERT_FALSE(HasFatalFailure());

    auto read_field = arrow::field("s", arrow::struct_({arrow::field("x", arrow::int32()),
                                                        VariantTypeUtils::ToArrowField("v")}));
    auto file_field = arrow::field(
        "s", arrow::struct_({arrow::field("x", arrow::int32()), arrow::field("v", physical)}));
    // A sibling plain struct with no variant exercises `ContainsNestedVariant` returning false.
    auto plain_field = arrow::field("plain", arrow::struct_({arrow::field("y", arrow::int32())}));
    auto plans_result = VariantShreddingReadPlanFactory::CreateReadPlans(
        arrow::schema({plain_field, read_field}), arrow::schema({plain_field, file_field}), pool_);
    ASSERT_OK_AND_ASSIGN(auto plans, std::move(plans_result));
    ASSERT_EQ(plans.count("plain"), 0);
    ASSERT_EQ(plans.count("s"), 1);
    std::shared_ptr<ShreddingColumnReadPlan> plan = plans["s"];

    auto x_array = Int32Array(7);
    auto made = arrow::StructArray::Make({x_array, shredded}, std::vector<std::string>{"x", "v"});
    ASSERT_TRUE(made.ok()) << made.status().ToString();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> assembled,
                         plan->Assemble(made.ValueOrDie(), arrow::default_memory_pool()));
    auto row = std::static_pointer_cast<arrow::StructArray>(assembled);
    EXPECT_EQ(static_cast<const arrow::Int32Array&>(*row->field(0)).Value(0), 7);
    ASSERT_EQ(row->field(1)->type_id(), arrow::Type::STRUCT);

    // Assembling a physical array whose type differs from the logical struct is an error.
    auto ints = Int32Array(1);
    ASSERT_NOK(plan->Assemble(ints, arrow::default_memory_pool()));
}

TEST_F(VariantShreddingReadPlanFactoryTest, ColumnAbsentInFileSkipped) {
    // Schema evolution: the variant column is missing from the file, so no plan is produced.
    auto read_field = VariantTypeUtils::ToArrowField("v");
    auto other = arrow::field("other", arrow::int32());
    ASSERT_OK_AND_ASSIGN(auto plans,
                         VariantShreddingReadPlanFactory::CreateReadPlans(
                             arrow::schema({read_field}), arrow::schema({other}), pool_));
    EXPECT_TRUE(plans.empty());
}

}  // namespace paimon::test
