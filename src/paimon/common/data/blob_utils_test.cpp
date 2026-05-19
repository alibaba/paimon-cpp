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
#include "paimon/common/data/blob_defs.h"
#include "paimon/data/blob.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class BlobUtilsTest : public ::testing::Test {
 private:
    std::shared_ptr<arrow::KeyValueMetadata> CreateBlobMetadata() {
        std::unordered_map<std::string, std::string> blob_metadata_map = {
            {BlobDefs::kExtensionTypeKey, BlobDefs::kExtensionTypeValue}};
        return std::make_shared<arrow::KeyValueMetadata>(blob_metadata_map);
    }
};

TEST_F(BlobUtilsTest, IsBlobMetadata) {
    auto correct_metadata = CreateBlobMetadata();
    EXPECT_TRUE(BlobUtils::IsBlobMetadata(correct_metadata));
    EXPECT_FALSE(BlobUtils::IsBlobMetadata(nullptr));
    std::unordered_map<std::string, std::string> wrong_metadata_map = {
        {BlobDefs::kExtensionTypeKey, "paimon.type.varchar"}};
    auto wrong_metadata = std::make_shared<arrow::KeyValueMetadata>(wrong_metadata_map);
    EXPECT_FALSE(BlobUtils::IsBlobMetadata(wrong_metadata));
    std::unordered_map<std::string, std::string> no_extension_metadata_map = {
        {"other_key", BlobDefs::kExtensionTypeValue}};
    auto no_extension_metadata =
        std::make_shared<arrow::KeyValueMetadata>(no_extension_metadata_map);
    EXPECT_FALSE(BlobUtils::IsBlobMetadata(no_extension_metadata));
}

TEST_F(BlobUtilsTest, IsBlobField) {
    std::shared_ptr<arrow::Field> blob_field = BlobUtils::ToArrowField("f1", true);
    EXPECT_TRUE(BlobUtils::IsBlobField(blob_field));

    auto int_field = arrow::field("i_int", arrow::int32());
    EXPECT_FALSE(BlobUtils::IsBlobField(int_field));

    auto binary_field_no_meta = arrow::field("b_no_meta", arrow::large_binary());
    EXPECT_FALSE(BlobUtils::IsBlobField(binary_field_no_meta));

    auto wrong_meta = std::make_shared<arrow::KeyValueMetadata>(
        std::unordered_map<std::string, std::string>{{"other_key", "value"}});
    auto binary_field_wrong_meta =
        arrow::field("b_wrong_meta", arrow::large_binary(), false, wrong_meta);
    EXPECT_FALSE(BlobUtils::IsBlobField(binary_field_wrong_meta));
}

TEST_F(BlobUtilsTest, SeparateBlobSchema) {
    auto int_field = arrow::field("f1_int", arrow::int32());
    auto string_field = arrow::field("f2_string", arrow::utf8());
    std::shared_ptr<arrow::Field> blob_field_1 = BlobUtils::ToArrowField("f3_blob_1", true);
    {
        std::shared_ptr<arrow::Schema> original_schema =
            arrow::schema({int_field, string_field, blob_field_1});

        BlobUtils::SeparatedSchemas schemas = BlobUtils::SeparateBlobSchema(original_schema);

        std::shared_ptr<arrow::Schema> expected_main_schema =
            arrow::schema({int_field, string_field});
        ASSERT_TRUE(schemas.main_schema->Equals(*expected_main_schema));

        std::shared_ptr<arrow::Schema> expected_blob_schema = arrow::schema({blob_field_1});
        ASSERT_TRUE(schemas.blob_schema->Equals(*expected_blob_schema));
    }
    {
        std::shared_ptr<arrow::Schema> no_blob_schema = arrow::schema({int_field, string_field});
        BlobUtils::SeparatedSchemas no_blob_schemas = BlobUtils::SeparateBlobSchema(no_blob_schema);
        ASSERT_TRUE(no_blob_schemas.main_schema->Equals(*no_blob_schema));
        ASSERT_EQ(no_blob_schemas.blob_schema->num_fields(), 0);
    }
    {
        std::shared_ptr<arrow::Schema> only_blob_schema = arrow::schema({blob_field_1});
        BlobUtils::SeparatedSchemas only_blob_schemas =
            BlobUtils::SeparateBlobSchema(only_blob_schema);
        ASSERT_TRUE(only_blob_schemas.blob_schema->Equals(*only_blob_schema));
        ASSERT_EQ(only_blob_schemas.main_schema->num_fields(), 0);
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

    ASSERT_OK_AND_ASSIGN(auto separated, BlobUtils::SeparateBlobArray(struct_array));

    std::shared_ptr<arrow::DataType> expected_main_type = arrow::struct_({int_field, string_field});
    ASSERT_TRUE(separated.main_array->type()->Equals(*expected_main_type));
    ASSERT_EQ(separated.main_array->num_fields(), 2);
    ASSERT_TRUE(separated.main_array->field(0)->Equals(*int_array));
    ASSERT_TRUE(separated.main_array->field(1)->Equals(*string_array));

    std::shared_ptr<arrow::DataType> expected_blob_type = arrow::struct_({blob_field});
    ASSERT_TRUE(separated.blob_array->type()->Equals(*expected_blob_type));
    ASSERT_EQ(separated.blob_array->num_fields(), 1);
    ASSERT_TRUE(separated.blob_array->field(0)->Equals(*blob_array_data));
}

}  // namespace paimon::test
