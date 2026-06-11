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

#include "paimon/common/data/blob_utils.h"

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/catalog/identifier.h"
#include "paimon/common/data/blob_defs.h"
#include "paimon/common/data/blob_descriptor.h"
#include "paimon/common/data/blob_view_struct.h"
#include "paimon/common/types/data_field.h"
#include "paimon/data/blob.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class BlobUtilsTest : public ::testing::Test {
 public:
    std::shared_ptr<arrow::KeyValueMetadata> CreateBlobMetadata() {
        std::unordered_map<std::string, std::string> blob_metadata_map = {
            {BlobDefs::kExtensionTypeKey, BlobDefs::kExtensionTypeValue}};
        return std::make_shared<arrow::KeyValueMetadata>(blob_metadata_map);
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(BlobUtilsTest, IsBlobMetadata) {
    auto correct_metadata = CreateBlobMetadata();
    ASSERT_TRUE(BlobUtils::IsBlobMetadata(correct_metadata));
    ASSERT_FALSE(BlobUtils::IsBlobMetadata(nullptr));
    std::unordered_map<std::string, std::string> wrong_metadata_map = {
        {BlobDefs::kExtensionTypeKey, "paimon.type.varchar"}};
    auto wrong_metadata = std::make_shared<arrow::KeyValueMetadata>(wrong_metadata_map);
    ASSERT_FALSE(BlobUtils::IsBlobMetadata(wrong_metadata));
    std::unordered_map<std::string, std::string> no_extension_metadata_map = {
        {"other_key", BlobDefs::kExtensionTypeValue}};
    auto no_extension_metadata =
        std::make_shared<arrow::KeyValueMetadata>(no_extension_metadata_map);
    ASSERT_FALSE(BlobUtils::IsBlobMetadata(no_extension_metadata));
}

TEST_F(BlobUtilsTest, IsBlobField) {
    std::shared_ptr<arrow::Field> blob_field = BlobUtils::ToArrowField("f1", true);
    ASSERT_TRUE(BlobUtils::IsBlobField(blob_field));

    auto int_field = arrow::field("i_int", arrow::int32());
    ASSERT_FALSE(BlobUtils::IsBlobField(int_field));

    auto binary_field_no_meta = arrow::field("b_no_meta", arrow::large_binary());
    ASSERT_FALSE(BlobUtils::IsBlobField(binary_field_no_meta));

    auto wrong_meta = std::make_shared<arrow::KeyValueMetadata>(
        std::unordered_map<std::string, std::string>{{"other_key", "value"}});
    auto binary_field_wrong_meta =
        arrow::field("b_wrong_meta", arrow::large_binary(), false, wrong_meta);
    ASSERT_FALSE(BlobUtils::IsBlobField(binary_field_wrong_meta));
}

TEST_F(BlobUtilsTest, SeparateBlobSchema) {
    auto int_field = arrow::field("f1_int", arrow::int32());
    auto string_field = arrow::field("f2_string", arrow::utf8());
    std::shared_ptr<arrow::Field> blob_field_1 = BlobUtils::ToArrowField("f3_blob_1", true);
    {
        std::shared_ptr<arrow::Schema> original_schema =
            arrow::schema({int_field, string_field, blob_field_1});

        BlobUtils::SeparatedSchemas schemas =
            BlobUtils::SeparateBlobSchema(original_schema, /*inline_fields=*/{});

        std::shared_ptr<arrow::Schema> expected_main_schema =
            arrow::schema({int_field, string_field});
        ASSERT_TRUE(schemas.main_schema->Equals(*expected_main_schema));

        std::shared_ptr<arrow::Schema> expected_blob_schema = arrow::schema({blob_field_1});
        ASSERT_TRUE(schemas.blob_schema->Equals(*expected_blob_schema));
    }
    {
        std::shared_ptr<arrow::Schema> no_blob_schema = arrow::schema({int_field, string_field});
        BlobUtils::SeparatedSchemas no_blob_schemas =
            BlobUtils::SeparateBlobSchema(no_blob_schema, /*inline_fields=*/{});
        ASSERT_TRUE(no_blob_schemas.main_schema->Equals(*no_blob_schema));
        ASSERT_EQ(no_blob_schemas.blob_schema->num_fields(), 0);
    }
    {
        std::shared_ptr<arrow::Schema> only_blob_schema = arrow::schema({blob_field_1});
        BlobUtils::SeparatedSchemas only_blob_schemas =
            BlobUtils::SeparateBlobSchema(only_blob_schema, /*inline_fields=*/{});
        ASSERT_TRUE(only_blob_schemas.blob_schema->Equals(*only_blob_schema));
        ASSERT_EQ(only_blob_schemas.main_schema->num_fields(), 0);
    }
    {
        // Inline blob field stays in main_schema instead of going to blob_schema
        auto blob_field_2 = BlobUtils::ToArrowField("f4_blob_2", false);
        std::shared_ptr<arrow::Schema> schema =
            arrow::schema({int_field, blob_field_1, blob_field_2, string_field});

        BlobUtils::SeparatedSchemas schemas =
            BlobUtils::SeparateBlobSchema(schema, /*inline_fields=*/{"f3_blob_1"});

        // f3_blob_1 is inline -> stays in main; f4_blob_2 goes to blob
        std::shared_ptr<arrow::Schema> expected_main =
            arrow::schema({int_field, blob_field_1, string_field});
        ASSERT_TRUE(schemas.main_schema->Equals(*expected_main));

        std::shared_ptr<arrow::Schema> expected_blob = arrow::schema({blob_field_2});
        ASSERT_TRUE(schemas.blob_schema->Equals(*expected_blob));
    }
    {
        // All blob fields are inline -> blob_schema is empty
        std::shared_ptr<arrow::Schema> schema =
            arrow::schema({int_field, blob_field_1, string_field});

        BlobUtils::SeparatedSchemas schemas =
            BlobUtils::SeparateBlobSchema(schema, /*inline_fields=*/{"f3_blob_1"});

        ASSERT_TRUE(schemas.main_schema->Equals(*schema));
        ASSERT_EQ(schemas.blob_schema->num_fields(), 0);
    }
}

TEST_F(BlobUtilsTest, SeparateBlobArray) {
    auto int_field = arrow::field("f1_int", arrow::int32());
    std::shared_ptr<arrow::Field> blob_field = BlobUtils::ToArrowField("f2_blob", false);
    auto string_field = arrow::field("f3_string", arrow::utf8());
    auto schema = arrow::schema({int_field, blob_field, string_field});

    arrow::Int32Builder int_builder;
    ASSERT_TRUE(int_builder.AppendValues({1, 2, 3}).ok());
    auto int_array = int_builder.Finish().ValueOrDie();

    arrow::StringBuilder string_builder;
    ASSERT_TRUE(string_builder.AppendValues({"a", "b", "c"}).ok());
    auto string_array = string_builder.Finish().ValueOrDie();

    arrow::LargeBinaryBuilder blob_builder;
    ASSERT_TRUE(blob_builder.Append("1", 1).ok());
    ASSERT_TRUE(blob_builder.Append("2", 1).ok());
    ASSERT_TRUE(blob_builder.Append("3", 1).ok());
    auto blob_array_data = blob_builder.Finish().ValueOrDie();

    auto raw_struct_array =
        arrow::StructArray::Make({int_array, blob_array_data, string_array}, schema->fields())
            .ValueOrDie();

    std::shared_ptr<arrow::StructArray> struct_array =
        std::static_pointer_cast<arrow::StructArray>(raw_struct_array);

    ASSERT_OK_AND_ASSIGN(auto separated,
                         BlobUtils::SeparateBlobArray(struct_array, /*inline_fields=*/{}));

    std::shared_ptr<arrow::DataType> expected_main_type = arrow::struct_({int_field, string_field});
    ASSERT_TRUE(separated.main_array->type()->Equals(*expected_main_type));
    ASSERT_EQ(separated.main_array->num_fields(), 2);
    ASSERT_TRUE(separated.main_array->field(0)->Equals(*int_array));
    ASSERT_TRUE(separated.main_array->field(1)->Equals(*string_array));

    std::shared_ptr<arrow::DataType> expected_blob_type = arrow::struct_({blob_field});
    ASSERT_TRUE(separated.blob_array->type()->Equals(*expected_blob_type));
    ASSERT_EQ(separated.blob_array->num_fields(), 1);
    ASSERT_TRUE(separated.blob_array->field(0)->Equals(*blob_array_data));

    // All blob fields are inline -> should return error (no blob field to separate)
    ASSERT_NOK_WITH_MSG(
        BlobUtils::SeparateBlobArray(struct_array, /*inline_fields=*/{"f2_blob"}),
        "SeparateBlobArray expects at least one non-inline blob field, but got none.");

    // All fields are blob with no inline -> no main field -> should return error
    auto all_blob_struct = arrow::StructArray::Make({blob_array_data}, {blob_field}).ValueOrDie();
    auto all_blob_sa = std::dynamic_pointer_cast<arrow::StructArray>(all_blob_struct);
    ASSERT_NOK_WITH_MSG(BlobUtils::SeparateBlobArray(all_blob_sa, /*inline_fields=*/{}),
                        "SeparateBlobArray expects at least one main field, but got none.");
}

TEST_F(BlobUtilsTest, SeparateBlobArrayWithPartialInline) {
    auto int_field = arrow::field("f1_int", arrow::int32());
    std::shared_ptr<arrow::Field> blob_field_1 = BlobUtils::ToArrowField("f2_blob_1", false);
    std::shared_ptr<arrow::Field> blob_field_2 = BlobUtils::ToArrowField("f3_blob_2", true);
    auto schema = arrow::schema({int_field, blob_field_1, blob_field_2});

    arrow::Int32Builder int_builder;
    ASSERT_TRUE(int_builder.AppendValues({1, 2}).ok());
    auto int_array = int_builder.Finish().ValueOrDie();

    arrow::LargeBinaryBuilder blob_builder_1;
    ASSERT_TRUE(blob_builder_1.Append("a", 1).ok());
    ASSERT_TRUE(blob_builder_1.Append("b", 1).ok());
    auto blob_array_1 = blob_builder_1.Finish().ValueOrDie();

    arrow::LargeBinaryBuilder blob_builder_2;
    ASSERT_TRUE(blob_builder_2.Append("x", 1).ok());
    ASSERT_TRUE(blob_builder_2.AppendNull().ok());
    auto blob_array_2 = blob_builder_2.Finish().ValueOrDie();

    auto raw_struct_array =
        arrow::StructArray::Make({int_array, blob_array_1, blob_array_2}, schema->fields())
            .ValueOrDie();
    auto struct_array = std::static_pointer_cast<arrow::StructArray>(raw_struct_array);

    // f2_blob_1 is inline, f3_blob_2 goes to blob
    ASSERT_OK_AND_ASSIGN(auto separated, BlobUtils::SeparateBlobArray(
                                             struct_array, /*inline_fields=*/{"f2_blob_1"}));

    std::shared_ptr<arrow::DataType> expected_main_type = arrow::struct_({int_field, blob_field_1});
    ASSERT_TRUE(separated.main_array->type()->Equals(*expected_main_type));
    ASSERT_EQ(separated.main_array->num_fields(), 2);
    ASSERT_TRUE(separated.main_array->field(0)->Equals(*int_array));
    ASSERT_TRUE(separated.main_array->field(1)->Equals(*blob_array_1));

    std::shared_ptr<arrow::DataType> expected_blob_type = arrow::struct_({blob_field_2});
    ASSERT_TRUE(separated.blob_array->type()->Equals(*expected_blob_type));
    ASSERT_EQ(separated.blob_array->num_fields(), 1);
    ASSERT_TRUE(separated.blob_array->field(0)->Equals(*blob_array_2));
}

TEST_F(BlobUtilsTest, ValidateInlineBlobDescriptorsEmptyFields) {
    // Empty inline_descriptor_fields -> always OK
    arrow::LargeBinaryBuilder builder;
    ASSERT_TRUE(builder.Append("random_data").ok());
    auto array = builder.Finish().ValueOrDie();
    auto struct_array =
        arrow::StructArray::Make({array}, {BlobUtils::ToArrowField("b0")}).ValueOrDie();
    auto sa = std::dynamic_pointer_cast<arrow::StructArray>(struct_array);
    ASSERT_OK(BlobUtils::ValidateBlobInlineFields(sa, {}, "blob-descriptor-field"));
}

TEST_F(BlobUtilsTest, ValidateInlineBlobDescriptorsFieldNotPresent) {
    // Field not in struct_array -> skip, OK
    arrow::Int32Builder int_builder;
    ASSERT_TRUE(int_builder.Append(42).ok());
    auto int_array = int_builder.Finish().ValueOrDie();
    auto struct_array =
        arrow::StructArray::Make({int_array}, {arrow::field("f0", arrow::int32())}).ValueOrDie();
    auto sa = std::dynamic_pointer_cast<arrow::StructArray>(struct_array);
    // "b0" does not exist in the struct -> should pass
    ASSERT_OK(BlobUtils::ValidateBlobInlineFields(sa, {"b0"}, "blob-descriptor-field"));
}

TEST_F(BlobUtilsTest, ValidateInlineBlobDescriptorsWithValidDescriptor) {
    // Valid BlobDescriptor bytes -> OK
    ASSERT_OK_AND_ASSIGN(auto descriptor, BlobDescriptor::Create("file:///tmp/test.bin", 0, 100));
    auto serialized = descriptor->Serialize(pool_);

    arrow::LargeBinaryBuilder builder;
    ASSERT_TRUE(builder.Append(serialized->data(), serialized->size()).ok());
    auto blob_array = builder.Finish().ValueOrDie();
    auto struct_array =
        arrow::StructArray::Make({blob_array}, {BlobUtils::ToArrowField("b0")}).ValueOrDie();
    auto sa = std::dynamic_pointer_cast<arrow::StructArray>(struct_array);
    ASSERT_OK(BlobUtils::ValidateBlobInlineFields(sa, {"b0"}, "blob-descriptor-field"));
}

TEST_F(BlobUtilsTest, ValidateInlineBlobDescriptorsWithNullValue) {
    // Null values in blob column -> skip, OK
    arrow::LargeBinaryBuilder builder;
    ASSERT_TRUE(builder.AppendNull().ok());
    auto blob_array = builder.Finish().ValueOrDie();
    auto struct_array =
        arrow::StructArray::Make({blob_array}, {BlobUtils::ToArrowField("b0")}).ValueOrDie();
    auto sa = std::dynamic_pointer_cast<arrow::StructArray>(struct_array);
    ASSERT_OK(BlobUtils::ValidateBlobInlineFields(sa, {"b0"}, "blob-descriptor-field"));
}

TEST_F(BlobUtilsTest, ValidateInlineBlobDescriptorsWithRawBytes) {
    // Raw bytes (not a descriptor) -> error
    arrow::LargeBinaryBuilder builder;
    ASSERT_TRUE(builder.Append("not_a_descriptor_just_raw_data").ok());
    auto blob_array = builder.Finish().ValueOrDie();
    auto struct_array =
        arrow::StructArray::Make({blob_array}, {BlobUtils::ToArrowField("b0")}).ValueOrDie();
    auto sa = std::dynamic_pointer_cast<arrow::StructArray>(struct_array);
    ASSERT_NOK_WITH_MSG(BlobUtils::ValidateBlobInlineFields(sa, {"b0"}, "blob-descriptor-field"),
                        "BLOB inline field b0 require values to be set as corresponding type.");
}

TEST_F(BlobUtilsTest, ValidateInlineBlobDescriptorsMixedValidAndInvalid) {
    // First row is valid descriptor, second row is raw bytes -> error on row 1
    ASSERT_OK_AND_ASSIGN(auto descriptor, BlobDescriptor::Create("file:///tmp/test.bin", 0, 100));
    auto serialized = descriptor->Serialize(pool_);
    arrow::LargeBinaryBuilder builder;
    ASSERT_TRUE(builder.Append(serialized->data(), serialized->size()).ok());
    ASSERT_TRUE(builder.Append("raw_bytes_not_descriptor").ok());
    auto blob_array = builder.Finish().ValueOrDie();
    auto struct_array =
        arrow::StructArray::Make({blob_array}, {BlobUtils::ToArrowField("b0")}).ValueOrDie();
    auto sa = std::dynamic_pointer_cast<arrow::StructArray>(struct_array);
    ASSERT_NOK_WITH_MSG(BlobUtils::ValidateBlobInlineFields(sa, {"b0"}, "blob-descriptor-field"),
                        "BLOB inline field b0 require values to be set as corresponding type.");
}

TEST_F(BlobUtilsTest, ValidateInlineBlobDescriptorsMultipleFields) {
    // Two inline fields: b0 is valid, b1 has raw bytes -> error on b1
    ASSERT_OK_AND_ASSIGN(auto descriptor, BlobDescriptor::Create("file:///tmp/test.bin", 0, 100));
    auto serialized = descriptor->Serialize(pool_);

    arrow::LargeBinaryBuilder b0_builder;
    ASSERT_TRUE(b0_builder.Append(serialized->data(), serialized->size()).ok());
    auto b0_array = b0_builder.Finish().ValueOrDie();

    arrow::LargeBinaryBuilder b1_builder;
    ASSERT_TRUE(b1_builder.Append("invalid_raw_data").ok());
    auto b1_array = b1_builder.Finish().ValueOrDie();

    auto struct_array =
        arrow::StructArray::Make({b0_array, b1_array},
                                 {BlobUtils::ToArrowField("b0"), BlobUtils::ToArrowField("b1")})
            .ValueOrDie();
    auto sa = std::dynamic_pointer_cast<arrow::StructArray>(struct_array);
    ASSERT_NOK_WITH_MSG(
        BlobUtils::ValidateBlobInlineFields(sa, {"b0", "b1"}, "blob-descriptor-field"),
        "BLOB inline field b1 require values to be set as corresponding type.");
}

TEST_F(BlobUtilsTest, ValidateBlobViewFieldsEmptyFields) {
    // Empty view_fields -> always OK
    arrow::LargeBinaryBuilder builder;
    ASSERT_TRUE(builder.Append("random_data").ok());
    auto array = builder.Finish().ValueOrDie();
    auto struct_array =
        arrow::StructArray::Make({array}, {BlobUtils::ToArrowField("view")}).ValueOrDie();
    auto sa = std::dynamic_pointer_cast<arrow::StructArray>(struct_array);
    ASSERT_OK(BlobUtils::ValidateBlobInlineFields(sa, {}, "blob-view-field"));
}

TEST_F(BlobUtilsTest, ValidateBlobViewFieldsFieldNotPresent) {
    // Field not in struct_array -> skip, OK
    arrow::Int32Builder int_builder;
    ASSERT_TRUE(int_builder.Append(42).ok());
    auto int_array = int_builder.Finish().ValueOrDie();
    auto struct_array =
        arrow::StructArray::Make({int_array}, {arrow::field("f0", arrow::int32())}).ValueOrDie();
    auto sa = std::dynamic_pointer_cast<arrow::StructArray>(struct_array);
    ASSERT_OK(BlobUtils::ValidateBlobInlineFields(sa, {"view"}, "blob-view-field"));
}

TEST_F(BlobUtilsTest, ValidateBlobViewFieldsWithValidViewStruct) {
    // A BlobViewStruct value is accepted for a view field.
    BlobViewStruct view_struct(Identifier("db", "tbl"), /*field_id=*/2, /*row_id=*/5);
    auto serialized = view_struct.Serialize(pool_);

    arrow::LargeBinaryBuilder builder;
    ASSERT_TRUE(builder.Append(serialized->data(), serialized->size()).ok());
    auto blob_array = builder.Finish().ValueOrDie();
    auto struct_array =
        arrow::StructArray::Make({blob_array}, {BlobUtils::ToArrowField("view")}).ValueOrDie();
    auto sa = std::dynamic_pointer_cast<arrow::StructArray>(struct_array);
    ASSERT_OK(BlobUtils::ValidateBlobInlineFields(sa, {"view"}, "blob-view-field"));
}

TEST_F(BlobUtilsTest, ValidateBlobViewFieldsWithNullValue) {
    // Null values in view column -> skip, OK
    arrow::LargeBinaryBuilder builder;
    ASSERT_TRUE(builder.AppendNull().ok());
    auto blob_array = builder.Finish().ValueOrDie();
    auto struct_array =
        arrow::StructArray::Make({blob_array}, {BlobUtils::ToArrowField("view")}).ValueOrDie();
    auto sa = std::dynamic_pointer_cast<arrow::StructArray>(struct_array);
    ASSERT_OK(BlobUtils::ValidateBlobInlineFields(sa, {"view"}, "blob-view-field"));
}

TEST_F(BlobUtilsTest, ValidateBlobViewFieldsWithRawBytes) {
    // Raw bytes -> error
    arrow::LargeBinaryBuilder builder;
    ASSERT_TRUE(builder.Append("raw_bytes_not_view").ok());
    auto blob_array = builder.Finish().ValueOrDie();
    auto struct_array =
        arrow::StructArray::Make({blob_array}, {BlobUtils::ToArrowField("view")}).ValueOrDie();
    auto sa = std::dynamic_pointer_cast<arrow::StructArray>(struct_array);
    ASSERT_NOK_WITH_MSG(BlobUtils::ValidateBlobInlineFields(sa, {"view"}, "blob-view-field"),
                        "BLOB inline field view require values to be set as corresponding type.");
}

TEST_F(BlobUtilsTest, ValidateBlobViewFieldsRejectsBlobDescriptor) {
    // A BlobDescriptor value is NOT accepted for a view field.
    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(auto descriptor, BlobDescriptor::Create("file:///tmp/test.bin", 0, 100));
    auto serialized = descriptor->Serialize(pool);

    arrow::LargeBinaryBuilder builder;
    ASSERT_TRUE(builder.Append(serialized->data(), serialized->size()).ok());
    auto blob_array = builder.Finish().ValueOrDie();
    auto struct_array =
        arrow::StructArray::Make({blob_array}, {BlobUtils::ToArrowField("view")}).ValueOrDie();
    auto sa = std::dynamic_pointer_cast<arrow::StructArray>(struct_array);
    ASSERT_NOK_WITH_MSG(BlobUtils::ValidateBlobInlineFields(sa, {"view"}, "blob-view-field"),
                        "BLOB inline field view require values to be set as corresponding type.");
}

TEST_F(BlobUtilsTest, TestConvertBlobInlineDataFields) {
    // Schema with a blob field (large_binary with blob metadata) and normal fields.
    auto blob_field = BlobUtils::ToArrowField("blob_col", /*nullable=*/true);
    std::vector<DataField> data_fields = {DataField(0, arrow::field("int_col", arrow::int32())),
                                          DataField(1, blob_field),
                                          DataField(2, arrow::field("str_col", arrow::utf8()))};

    // Without inline fields — blob_col stays as large_binary
    {
        auto result = BlobUtils::ConvertBlobInlineDataFields(data_fields, {});
        ASSERT_EQ(result.size(), 3);
        ASSERT_EQ(result[1].ArrowField()->type()->id(), arrow::Type::LARGE_BINARY);
    }

    // With inline fields — blob_col should be converted from large_binary to binary
    {
        auto result = BlobUtils::ConvertBlobInlineDataFields(data_fields, {"blob_col"});
        ASSERT_EQ(result.size(), 3);
        ASSERT_EQ(result[1].ArrowField()->type()->id(), arrow::Type::BINARY);
        ASSERT_EQ(result[1].Name(), "blob_col");
        ASSERT_EQ(result[1].Nullable(), true);
        // Other fields unchanged
        ASSERT_EQ(result[0].ArrowField()->type()->id(), arrow::Type::INT32);
        ASSERT_EQ(result[2].ArrowField()->type()->id(), arrow::Type::STRING);
    }

    // Non-matching inline field name — no conversion should happen
    {
        auto result = BlobUtils::ConvertBlobInlineDataFields(data_fields, {"non_existent_field"});
        ASSERT_EQ(result[1].ArrowField()->type()->id(), arrow::Type::LARGE_BINARY);
    }
}

}  // namespace paimon::test
