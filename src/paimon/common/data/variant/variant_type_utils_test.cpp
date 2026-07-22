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

#include "paimon/common/data/variant/variant_type_utils.h"

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/variant/variant_defs.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(VariantTypeUtilsTest, ToArrowFieldAndDetection) {
    auto field = VariantTypeUtils::ToArrowField("v");
    ASSERT_EQ(field->name(), "v");
    ASSERT_TRUE(field->nullable());
    ASSERT_TRUE(VariantTypeUtils::IsVariantField(field));
    ASSERT_TRUE(VariantTypeUtils::IsVariantMetadata(field->metadata()));
    ASSERT_OK(VariantTypeUtils::ValidateVariantShape(field));

    auto struct_type = std::static_pointer_cast<arrow::StructType>(field->type());
    ASSERT_EQ(struct_type->num_fields(), 2);
    ASSERT_EQ(struct_type->field(0)->name(), VariantDefs::kValueFieldName);
    ASSERT_EQ(struct_type->field(0)->type()->id(), arrow::Type::BINARY);
    ASSERT_FALSE(struct_type->field(0)->nullable());
    ASSERT_EQ(struct_type->field(1)->name(), VariantDefs::kMetadataFieldName);
    ASSERT_EQ(struct_type->field(1)->type()->id(), arrow::Type::BINARY);
    ASSERT_FALSE(struct_type->field(1)->nullable());
    // The children carry paimon field ids 0/1 (mapped to parquet field ids on write).
    ASSERT_EQ(struct_type->field(0)->metadata()->Get("paimon.id").ValueOr(""), "0");
    ASSERT_EQ(struct_type->field(1)->metadata()->Get("paimon.id").ValueOr(""), "1");

    auto plain_struct = arrow::field("s", VariantTypeUtils::UnshreddedStructType());
    ASSERT_FALSE(VariantTypeUtils::IsVariantField(plain_struct));
    std::unordered_map<std::string, std::string> metadata = {
        {VariantDefs::kExtensionTypeKey, VariantDefs::kExtensionTypeValue}};
    auto marked_binary = arrow::field("b", arrow::binary(), true,
                                      std::make_shared<arrow::KeyValueMetadata>(metadata));
    ASSERT_FALSE(VariantTypeUtils::IsVariantField(marked_binary));
}

TEST(VariantTypeUtilsTest, ValidateVariantShapeRejectsWrongShape) {
    std::unordered_map<std::string, std::string> metadata = {
        {VariantDefs::kExtensionTypeKey, VariantDefs::kExtensionTypeValue}};
    auto arrow_metadata = std::make_shared<arrow::KeyValueMetadata>(metadata);
    auto one_child = arrow::field(
        "v", arrow::struct_({arrow::field("value", arrow::binary(), false)}), true, arrow_metadata);
    ASSERT_NOK(VariantTypeUtils::ValidateVariantShape(one_child));
    auto nullable_children =
        arrow::field("v",
                     arrow::struct_({arrow::field("value", arrow::binary(), true),
                                     arrow::field("metadata", arrow::binary(), true)}),
                     true, arrow_metadata);
    ASSERT_NOK(VariantTypeUtils::ValidateVariantShape(nullable_children));
    auto wrong_type =
        arrow::field("v",
                     arrow::struct_({arrow::field("value", arrow::utf8(), false),
                                     arrow::field("metadata", arrow::binary(), false)}),
                     true, arrow_metadata);
    ASSERT_NOK(VariantTypeUtils::ValidateVariantShape(wrong_type));
}

TEST(VariantTypeUtilsTest, ContainsVariantField) {
    auto variant_field = VariantTypeUtils::ToArrowField("v");
    auto plain = arrow::schema({arrow::field("a", arrow::int32())});
    ASSERT_FALSE(VariantTypeUtils::ContainsVariantField(plain));
    auto top_level = arrow::schema({arrow::field("a", arrow::int32()), variant_field});
    ASSERT_TRUE(VariantTypeUtils::ContainsVariantField(top_level));
    auto nested =
        arrow::schema({arrow::field("row", arrow::struct_({arrow::field("inner", arrow::int32()),
                                                           VariantTypeUtils::ToArrowField("v")}))});
    ASSERT_TRUE(VariantTypeUtils::ContainsVariantField(nested));
    auto in_list =
        arrow::schema({arrow::field("l", arrow::list(VariantTypeUtils::ToArrowField("item")))});
    ASSERT_TRUE(VariantTypeUtils::ContainsVariantField(in_list));
}

TEST(VariantTypeUtilsTest, FieldTypeConversion) {
    auto variant_field = VariantTypeUtils::ToArrowField("v");
    ASSERT_OK_AND_ASSIGN(FieldType field_type, FieldTypeUtils::ConvertToFieldType(variant_field));
    ASSERT_EQ(field_type, FieldType::VARIANT);
    ASSERT_EQ(FieldTypeUtils::FieldTypeToString(FieldType::VARIANT), "VARIANT");
    auto plain_struct = arrow::field("s", VariantTypeUtils::UnshreddedStructType());
    ASSERT_OK_AND_ASSIGN(FieldType plain_type, FieldTypeUtils::ConvertToFieldType(plain_struct));
    ASSERT_EQ(plain_type, FieldType::STRUCT);
}

}  // namespace paimon::test
