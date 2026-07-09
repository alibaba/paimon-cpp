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

#include "paimon/core/schema/arrow_schema_validator.h"

#include <string>
#include <vector>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/data/variant/variant_access_utils.h"
#include "paimon/common/data/variant/variant_defs.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(ArrowSchemaValidatorTest, TestSimple) {
    auto col1_field = arrow::field("col1", arrow::int64());
    auto col2_field = arrow::field("col2", arrow::int32());
    auto col3_field = arrow::field("col3", arrow::int16());
    auto col4_field = arrow::field("col4", arrow::int8());
    auto col5_field = arrow::field("col5", arrow::float64());
    auto col6_field = arrow::field("col6", arrow::float32());
    auto col7_field = arrow::field("col7", arrow::boolean());
    auto col8_field = arrow::field("col8", arrow::utf8());
    auto col9_field = arrow::field("col9", arrow::binary());
    auto col10_field = arrow::field("col10", arrow::date32());
    auto col11_field = arrow::field("col11", arrow::decimal128(20, 4));
    auto col12_field = arrow::field("col12", arrow::decimal128(18, 5));
    auto col13_field = arrow::field("col13", arrow::list(arrow::int64()));
    auto col14_field = arrow::field("col14", arrow::map(arrow::utf8(), arrow::int64()));
    auto col15_field = arrow::field("col15", arrow::timestamp(arrow::TimeUnit::NANO));
    auto col16_field = arrow::field(
        "col16",
        arrow::struct_({arrow::field("sub1", arrow::int8()), arrow::field("sub2", arrow::int16()),
                        arrow::field("sub3", arrow::int64())}));

    auto arrow_schema = arrow::schema(
        arrow::FieldVector({col1_field, col2_field, col3_field, col4_field, col5_field, col6_field,
                            col7_field, col8_field, col9_field, col10_field, col11_field,
                            col12_field, col13_field, col14_field, col15_field, col16_field}));
    ASSERT_OK(ArrowSchemaValidator::ValidateSchema(*arrow_schema));
}

TEST(ArrowSchemaValidatorTest, TestValidateNoRedundantFields) {
    auto col1_field = arrow::field("col1", arrow::int64());
    auto col2_field = arrow::field("col2", arrow::int32());
    auto col3_field = arrow::field("col3", arrow::int16());
    {
        auto arrow_schema = arrow::schema(arrow::FieldVector({col1_field, col2_field, col3_field}));
        ASSERT_OK(ArrowSchemaValidator::ValidateNoRedundantFields(arrow_schema->fields()));
    }
    {
        auto arrow_schema =
            arrow::schema(arrow::FieldVector({col1_field, col2_field, col3_field, col2_field}));
        ASSERT_NOK_WITH_MSG(ArrowSchemaValidator::ValidateNoRedundantFields(arrow_schema->fields()),
                            "validate schema failed: read schema has duplicate field col2");
    }
    {
        auto col4_field =
            arrow::field("col4", arrow::struct_({arrow::field("sub1", arrow::int8()),
                                                 arrow::field("sub1", arrow::int16())}));
        auto arrow_schema =
            arrow::schema(arrow::FieldVector({col1_field, col2_field, col3_field, col4_field}));
        ASSERT_NOK_WITH_MSG(ArrowSchemaValidator::ValidateNoRedundantFields(arrow_schema->fields()),
                            "validate schema failed: read schema has duplicate field sub1");
    }
}

TEST(ArrowSchemaValidatorTest, TestValidateNoWhitespaceOnlyFields) {
    auto col1_field = arrow::field("col1", arrow::int64());
    auto col2_field = arrow::field("col2", arrow::int32());
    auto col3_field = arrow::field("col3", arrow::int16());
    {
        auto arrow_schema = arrow::schema(arrow::FieldVector({col1_field, col2_field, col3_field}));
        ASSERT_OK(ArrowSchemaValidator::ValidateNoWhitespaceOnlyFields(arrow_schema->fields()));
    }
    {
        auto col4_field = arrow::field(" ", arrow::int16());
        auto arrow_schema =
            arrow::schema(arrow::FieldVector({col1_field, col2_field, col3_field, col4_field}));
        ASSERT_NOK_WITH_MSG(
            ArrowSchemaValidator::ValidateNoWhitespaceOnlyFields(arrow_schema->fields()),
            "validate schema failed: read schema has whitespace-only field");
    }
    {
        auto col4_field = arrow::field("col4", arrow::struct_({arrow::field("sub1", arrow::int8()),
                                                               arrow::field(" ", arrow::int16())}));
        auto arrow_schema =
            arrow::schema(arrow::FieldVector({col1_field, col2_field, col3_field, col4_field}));
        ASSERT_NOK_WITH_MSG(
            ArrowSchemaValidator::ValidateNoWhitespaceOnlyFields(arrow_schema->fields()),
            "validate schema failed: read schema has whitespace-only field");
    }
}

TEST(ArrowSchemaValidatorTest, TestInvalidDataType) {
    {
        auto col1_field = arrow::field("col1", arrow::large_utf8());
        auto arrow_schema = arrow::schema(arrow::FieldVector({col1_field}));
        ASSERT_NOK_WITH_MSG(ArrowSchemaValidator::ValidateSchema(*arrow_schema),
                            "Unknown or unsupported arrow type: large_string");
    }
    {
        auto col1_field = arrow::field("col1", arrow::large_binary());
        auto arrow_schema = arrow::schema(arrow::FieldVector({col1_field}));
        ASSERT_NOK_WITH_MSG(ArrowSchemaValidator::ValidateSchema(*arrow_schema),
                            "Unknown or unsupported arrow type: large_binary");
    }
    {
        auto col1_field = arrow::field("col1", arrow::uint32());
        auto arrow_schema = arrow::schema(arrow::FieldVector({col1_field}));
        ASSERT_NOK_WITH_MSG(ArrowSchemaValidator::ValidateSchema(*arrow_schema),
                            "Unknown or unsupported arrow type: uint32");
    }
    {
        auto union_type = arrow::sparse_union(
            {arrow::field("_union_0", arrow::int32()), arrow::field("_union_1", arrow::utf8())});
        auto col1_field = arrow::field("col1", union_type);
        auto arrow_schema = arrow::schema(arrow::FieldVector({col1_field}));
        ASSERT_NOK_WITH_MSG(ArrowSchemaValidator::ValidateSchema(*arrow_schema),
                            "Unknown or unsupported arrow type: sparse_union<_union_0: int32=0, "
                            "_union_1: string=1>");
    }
    {
        auto col1_field = arrow::field("col1", arrow::date64());
        auto arrow_schema = arrow::schema(arrow::FieldVector({col1_field}));
        ASSERT_NOK_WITH_MSG(ArrowSchemaValidator::ValidateSchema(*arrow_schema),
                            "Unknown or unsupported arrow type: date64[ms]");
    }
    {
        auto col1_field = arrow::field("col1", arrow::decimal256(10, 5));
        auto arrow_schema = arrow::schema(arrow::FieldVector({col1_field}));
        ASSERT_NOK_WITH_MSG(ArrowSchemaValidator::ValidateSchema(*arrow_schema),
                            "Unknown or unsupported arrow type: decimal256(10, 5)");
    }
}

TEST(ArrowSchemaValidatorTest, ValidateDataTypeWithFieldId) {
    {
        std::vector<DataField> fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                         DataField(0, arrow::field("f0", arrow::utf8())),
                                         DataField(1, arrow::field("f1", arrow::int32()))};
        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(fields);
        ASSERT_OK(ArrowSchemaValidator::ValidateSchemaWithFieldId(*arrow_schema));
    }
    {
        std::vector<DataField> sub_fields0 = {
            DataField(1, arrow::field("sub_f1", arrow::float64())),
            DataField(2, arrow::field("sub_f2", arrow::utf8())),
            DataField(3, arrow::field("sub_f3", arrow::int32()))};
        std::vector<DataField> sub_fields1 = {
            DataField(5, arrow::field("sub_f5", arrow::float64())),
            DataField(6, arrow::field("sub_f6", arrow::utf8())),
            DataField(7, arrow::field("sub_f7", arrow::int32()))};

        DataField field0 = DataField(
            0, arrow::field("f0", DataField::ConvertDataFieldsToArrowStructType(sub_fields0)));
        DataField field1 = DataField(
            4, arrow::field(
                   "f1", arrow::map(arrow::utf8(),
                                    DataField::ConvertDataFieldsToArrowStructType(sub_fields1))));
        DataField field2 =
            DataField(8, arrow::field("f2", arrow::map(arrow::int8(), arrow::int16())));
        std::vector<DataField> fields = {field0, field1, field2};
        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(fields);
        ASSERT_OK(ArrowSchemaValidator::ValidateSchemaWithFieldId(*arrow_schema))
            << ArrowSchemaValidator::ValidateSchemaWithFieldId(*arrow_schema).ToString();
    }
    {
        std::vector<DataField> fields = {DataField(0, arrow::field("f3", arrow::float64())),
                                         DataField(0, arrow::field("f0", arrow::utf8())),
                                         DataField(1, arrow::field("f1", arrow::int32()))};
        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(fields);
        ASSERT_NOK_WITH_MSG(ArrowSchemaValidator::ValidateSchemaWithFieldId(*arrow_schema),
                            "field id must be unique, duplicate field id 0");
    }
    {
        arrow::FieldVector fields = {arrow::field("f3", arrow::float64()),
                                     arrow::field("f0", arrow::utf8()),
                                     arrow::field("f1", arrow::int32())};
        auto arrow_schema = arrow::schema(fields);
        ASSERT_NOK_WITH_MSG(ArrowSchemaValidator::ValidateSchemaWithFieldId(*arrow_schema),
                            "invalid read schema, lack of metadata of field id");
    }
    {
        std::vector<DataField> sub_fields0 = {
            DataField(1, arrow::field("sub_f1", arrow::float64())),
            DataField(2, arrow::field("sub_f2", arrow::utf8())),
            DataField(3, arrow::field("sub_f3", arrow::int32()))};
        std::vector<DataField> sub_fields1 = {
            DataField(5, arrow::field("sub_f5", arrow::float64())),
            DataField(6, arrow::field("sub_f6", arrow::utf8())),
            DataField(7, arrow::field("sub_f7", arrow::int32()))};

        DataField field0 = DataField(
            0, arrow::field("f0", DataField::ConvertDataFieldsToArrowStructType(sub_fields0)));
        DataField field1 = DataField(
            4, arrow::field(
                   "f1", arrow::list(DataField::ConvertDataFieldsToArrowStructType(sub_fields1))));
        DataField field2 =
            DataField(8, arrow::field("f2", arrow::map(arrow::int8(), arrow::int16())));
        std::vector<DataField> fields = {field0, field1, field2};
        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(fields);
        ASSERT_OK(ArrowSchemaValidator::ValidateSchemaWithFieldId(*arrow_schema))
            << ArrowSchemaValidator::ValidateSchemaWithFieldId(*arrow_schema).ToString();
    }
    {
        std::vector<DataField> sub_fields0 = {
            DataField(1, arrow::field("sub_f1", arrow::float64())),
            DataField(2, arrow::field("sub_f2", arrow::utf8())),
            DataField(3, arrow::field("sub_f3", arrow::int32()))};
        std::vector<DataField> sub_fields1 = {
            DataField(5, arrow::field("sub_f5", arrow::float64())),
            DataField(4, arrow::field("sub_f6", arrow::utf8())),
            DataField(7, arrow::field("sub_f7", arrow::int32()))};

        DataField field0 = DataField(
            0, arrow::field("f0", DataField::ConvertDataFieldsToArrowStructType(sub_fields0)));
        DataField field1 = DataField(
            4, arrow::field(
                   "f1", arrow::list(DataField::ConvertDataFieldsToArrowStructType(sub_fields1))));
        DataField field2 =
            DataField(8, arrow::field("f2", arrow::map(arrow::int8(), arrow::int16())));
        std::vector<DataField> fields = {field0, field1, field2};
        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(fields);
        ASSERT_NOK_WITH_MSG(ArrowSchemaValidator::ValidateSchemaWithFieldId(*arrow_schema),
                            "field id must be unique, duplicate field id 4");
    }
    {
        std::vector<DataField> sub_fields0 = {
            DataField(1, arrow::field("sub_f1", arrow::float64())),
            DataField(2, arrow::field("sub_f2", arrow::utf8())),
            DataField(3, arrow::field("sub_f3", arrow::int32()))};
        arrow::FieldVector invalid_sub_fields = {arrow::field("sub_f4", arrow::float64()),
                                                 arrow::field("sub_f5", arrow::utf8()),
                                                 arrow::field("sub_f6", arrow::int32())};
        DataField field0 = DataField(
            0, arrow::field("f0", DataField::ConvertDataFieldsToArrowStructType(sub_fields0)));
        DataField field1 =
            DataField(4, arrow::field("f1", arrow::list(arrow::struct_(invalid_sub_fields))));
        DataField field2 =
            DataField(5, arrow::field("f2", arrow::map(arrow::int8(), arrow::int16())));
        std::vector<DataField> fields = {field0, field1, field2};
        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(fields);
        ASSERT_NOK_WITH_MSG(ArrowSchemaValidator::ValidateSchemaWithFieldId(*arrow_schema),
                            "invalid read schema, lack of metadata of field id");
    }
    {
        std::vector<DataField> fields = {DataField(0, arrow::field("f0", arrow::float64())),
                                         DataField(1, arrow::field("f1", arrow::large_utf8())),
                                         DataField(2, arrow::field("f2", arrow::int32()))};
        auto struct_type = DataField::ConvertDataFieldsToArrowStructType(fields);
        std::set<int32_t> field_id_set;
        ASSERT_NOK_WITH_MSG(ArrowSchemaValidator::ValidateDataTypeWithFieldId(
                                struct_type, /*key_value_metadata=*/nullptr, &field_id_set),
                            "Unknown or unsupported arrow type: large_string");
    }
}

TEST(ArrowSchemaValidatorTest, ValidateVariantField) {
    // A variant field validates as a leaf: its fixed child ids 0/1 do not join the global field
    // id uniqueness check, even when top-level fields use ids 0/1.
    {
        std::vector<DataField> fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                         DataField(1, VariantTypeUtils::ToArrowField("v1")),
                                         DataField(2, VariantTypeUtils::ToArrowField("v2"))};
        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(fields);
        ASSERT_OK(ArrowSchemaValidator::ValidateSchemaWithFieldId(*arrow_schema))
            << ArrowSchemaValidator::ValidateSchemaWithFieldId(*arrow_schema).ToString();
    }
    // A variant-access projection (variant marker + description-carrying children) validates
    // as a leaf instead of being shape-checked.
    {
        auto age_child =
            arrow::field("0", arrow::int64(), true,
                         arrow::KeyValueMetadata::Make(
                             {DataField::DESCRIPTION},
                             {VariantAccessUtils::BuildVariantMetadata("$.age", true, "UTC")}));
        std::unordered_map<std::string, std::string> metadata = {
            {VariantDefs::kExtensionTypeKey, VariantDefs::kExtensionTypeValue}};
        auto access_field = arrow::field("v", arrow::struct_({age_child}), true,
                                         std::make_shared<arrow::KeyValueMetadata>(metadata));
        auto arrow_schema = arrow::schema({access_field});
        ASSERT_OK(ArrowSchemaValidator::ValidateSchema(*arrow_schema))
            << ArrowSchemaValidator::ValidateSchema(*arrow_schema).ToString();
    }
    // A variant-marked struct with the wrong physical shape is rejected.
    {
        std::unordered_map<std::string, std::string> metadata = {
            {VariantDefs::kExtensionTypeKey, VariantDefs::kExtensionTypeValue}};
        auto bad_variant =
            arrow::field("v",
                         arrow::struct_({arrow::field("value", arrow::binary(), true),
                                         arrow::field("metadata", arrow::binary(), true)}),
                         true, std::make_shared<arrow::KeyValueMetadata>(metadata));
        auto arrow_schema = arrow::schema({bad_variant});
        ASSERT_NOK(ArrowSchemaValidator::ValidateSchema(*arrow_schema));
    }
}

TEST(ArrowSchemaValidatorTest, ContainTimestampWithTimezone) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    {
        std::vector<DataField> fields = {
            DataField(0, arrow::field("f0", arrow::float64())),
            DataField(1, arrow::field("f1", arrow::utf8())),
            DataField(2, arrow::field("f2", arrow::timestamp(arrow::TimeUnit::NANO)))};
        std::shared_ptr<arrow::DataType> arrow_data_type =
            DataField::ConvertDataFieldsToArrowStructType(fields);
        ASSERT_FALSE(ArrowSchemaValidator::ContainTimestampWithTimezone(*arrow_data_type));
    }
    {
        std::vector<DataField> fields = {
            DataField(0, arrow::field("f0", arrow::float64())),
            DataField(1, arrow::field("f1", arrow::utf8())),
            DataField(2, arrow::field("f2", arrow::timestamp(arrow::TimeUnit::NANO, timezone)))};
        std::shared_ptr<arrow::DataType> arrow_data_type =
            DataField::ConvertDataFieldsToArrowStructType(fields);
        ASSERT_TRUE(ArrowSchemaValidator::ContainTimestampWithTimezone(*arrow_data_type));
    }
    {
        std::vector<DataField> sub_fields0 = {
            DataField(1, arrow::field("sub_f1", arrow::float64())),
            DataField(2, arrow::field("sub_f2", arrow::utf8())),
            DataField(3,
                      arrow::field("sub_f3", arrow::timestamp(arrow::TimeUnit::NANO, timezone)))};
        DataField field0 = DataField(
            0, arrow::field("f0", DataField::ConvertDataFieldsToArrowStructType(sub_fields0)));
        std::vector<DataField> fields = {field0, DataField(4, arrow::field("f1", arrow::utf8()))};
        std::shared_ptr<arrow::DataType> arrow_data_type =
            DataField::ConvertDataFieldsToArrowStructType(fields);
        ASSERT_TRUE(ArrowSchemaValidator::ContainTimestampWithTimezone(*arrow_data_type));
    }
    {
        std::vector<DataField> sub_fields0 = {
            DataField(1, arrow::field("sub_f1", arrow::float64())),
            DataField(2, arrow::field("sub_f2", arrow::utf8())),
            DataField(3, arrow::field("sub_f3", arrow::decimal128(20, 5)))};
        DataField field0 = DataField(
            0, arrow::field("f0", DataField::ConvertDataFieldsToArrowStructType(sub_fields0)));
        std::vector<DataField> fields = {field0, DataField(4, arrow::field("f1", arrow::utf8()))};
        std::shared_ptr<arrow::DataType> arrow_data_type =
            DataField::ConvertDataFieldsToArrowStructType(fields);
        ASSERT_FALSE(ArrowSchemaValidator::ContainTimestampWithTimezone(*arrow_data_type));
    }
    {
        DataField field0 = DataField(
            0, arrow::field("f0", arrow::map(arrow::int8(),
                                             arrow::timestamp(arrow::TimeUnit::NANO, timezone))));
        std::vector<DataField> fields = {field0, DataField(1, arrow::field("f1", arrow::utf8()))};
        std::shared_ptr<arrow::DataType> arrow_data_type =
            DataField::ConvertDataFieldsToArrowStructType(fields);
        ASSERT_TRUE(ArrowSchemaValidator::ContainTimestampWithTimezone(*arrow_data_type));
    }
    {
        DataField field0 = DataField(
            0, arrow::field("f0", arrow::map(arrow::timestamp(arrow::TimeUnit::NANO, timezone),
                                             arrow::int8())));
        std::vector<DataField> fields = {field0, DataField(1, arrow::field("f1", arrow::utf8()))};
        std::shared_ptr<arrow::DataType> arrow_data_type =
            DataField::ConvertDataFieldsToArrowStructType(fields);
        ASSERT_TRUE(ArrowSchemaValidator::ContainTimestampWithTimezone(*arrow_data_type));
    }
    {
        DataField field0 = DataField(
            0, arrow::field("f0", arrow::list(arrow::timestamp(arrow::TimeUnit::NANO, timezone))));
        std::vector<DataField> fields = {field0, DataField(1, arrow::field("f1", arrow::utf8()))};
        std::shared_ptr<arrow::DataType> arrow_data_type =
            DataField::ConvertDataFieldsToArrowStructType(fields);
        ASSERT_TRUE(ArrowSchemaValidator::ContainTimestampWithTimezone(*arrow_data_type));
    }
}
}  // namespace paimon::test
