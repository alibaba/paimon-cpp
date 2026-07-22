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

#include "paimon/common/types/data_type.h"

#include <stdexcept>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/rapidjson_util.h"

namespace paimon::test {

TEST(DataTypeTest, Create) {
    auto int32_type = arrow::int32();
    auto nullable_int32 = DataType::Create(int32_type, /*nullable=*/true, /*metadata=*/nullptr);
    ASSERT_TRUE(nullable_int32);

    auto non_nullable_int32 =
        DataType::Create(int32_type, /*nullable=*/false, /*metadata=*/nullptr);
    ASSERT_TRUE(non_nullable_int32);
}

TEST(DataTypeTest, WithNullable) {
    auto int32_type = arrow::int32();
    DataType nullable_int32(int32_type, true, /*metadata=*/nullptr);
    ASSERT_EQ(nullable_int32.WithNullable("INT"), "INT");

    DataType non_nullable_int32(int32_type, false, /*metadata=*/nullptr);
    ASSERT_EQ(non_nullable_int32.WithNullable("INT"), "INT NOT NULL");
}

TEST(DataTypeTest, DataTypeToString) {
    DataType dummy_data_type(arrow::null(), true,
                             /*metadata=*/nullptr);  // Need a DataType instance to call the method

    ASSERT_EQ(dummy_data_type.DataTypeToString(arrow::boolean()), "BOOLEAN");
    ASSERT_EQ(dummy_data_type.DataTypeToString(arrow::int8()), "TINYINT");
    ASSERT_EQ(dummy_data_type.DataTypeToString(arrow::int16()), "SMALLINT");
    ASSERT_EQ(dummy_data_type.DataTypeToString(arrow::int32()), "INT");
    ASSERT_EQ(dummy_data_type.DataTypeToString(arrow::int64()), "BIGINT");
    ASSERT_EQ(dummy_data_type.DataTypeToString(arrow::float32()), "FLOAT");
    ASSERT_EQ(dummy_data_type.DataTypeToString(arrow::float64()), "DOUBLE");
    ASSERT_EQ(dummy_data_type.DataTypeToString(arrow::utf8()), "STRING");
    ASSERT_EQ(dummy_data_type.DataTypeToString(arrow::binary()), "BYTES");
    {
        std::shared_ptr<arrow::Field> blob_field = BlobUtils::ToArrowField("f2_blob", false);
        DataType blob_type(blob_field->type(), blob_field->nullable(), blob_field->metadata());
        ASSERT_EQ(blob_type.DataTypeToString(blob_field->type()), "BLOB");
    }
    {
        std::shared_ptr<arrow::Field> variant_field = VariantTypeUtils::ToArrowField("f3_variant");
        DataType variant_type(variant_field->type(), variant_field->nullable(),
                              variant_field->metadata());
        ASSERT_EQ(variant_type.DataTypeToString(variant_field->type()), "VARIANT");
        // A variant field is a scalar VARIANT type, not a ROW type.
        auto created = DataType::Create(variant_field->type(), variant_field->nullable(),
                                        variant_field->metadata());
        rapidjson::Document doc;
        auto json_value = created->ToJson(&doc.GetAllocator());
        ASSERT_TRUE(json_value.IsString());
        ASSERT_EQ(std::string(json_value.GetString()), "VARIANT");
    }
    ASSERT_EQ(dummy_data_type.DataTypeToString(arrow::date32()), "DATE");

    auto decimal_type1 = arrow::decimal128(10, 2);
    ASSERT_EQ(dummy_data_type.DataTypeToString(decimal_type1), "DECIMAL(10, 2)");

    auto decimal_type2 = arrow::decimal128(18, 6);
    ASSERT_EQ(dummy_data_type.DataTypeToString(decimal_type2), "DECIMAL(18, 6)");

    auto nano_timestamp_type = arrow::timestamp(arrow::TimeUnit::NANO);
    ASSERT_EQ(dummy_data_type.DataTypeToString(nano_timestamp_type), "TIMESTAMP(9)");

    auto micro_timestamp_type = arrow::timestamp(arrow::TimeUnit::MICRO);
    ASSERT_EQ(dummy_data_type.DataTypeToString(micro_timestamp_type), "TIMESTAMP(6)");

    auto milli_timestamp_type = arrow::timestamp(arrow::TimeUnit::MILLI);
    ASSERT_EQ(dummy_data_type.DataTypeToString(milli_timestamp_type), "TIMESTAMP(3)");

    auto second_timestamp_type = arrow::timestamp(arrow::TimeUnit::SECOND);
    ASSERT_EQ(dummy_data_type.DataTypeToString(second_timestamp_type), "TIMESTAMP(0)");

    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    auto tz_timestamp_type = arrow::timestamp(arrow::TimeUnit::NANO, timezone);
    ASSERT_EQ(dummy_data_type.DataTypeToString(tz_timestamp_type),
              "TIMESTAMP(9) WITH LOCAL TIME ZONE");

    tz_timestamp_type = arrow::timestamp(arrow::TimeUnit::NANO, "Asia/Shanghai");
    ASSERT_EQ(dummy_data_type.DataTypeToString(tz_timestamp_type),
              "TIMESTAMP(9) WITH LOCAL TIME ZONE");

    auto unknown_type = arrow::date64();
    ASSERT_THROW(dummy_data_type.DataTypeToString(unknown_type), std::invalid_argument);
}

TEST(DataTypeTest, NestedTypeSerializationUsesChildMetadata) {
    // ARRAY and MAP must carry their children's metadata into the serialized type, because that
    // is what marks an extension type. Dropping it serialized a VARIANT as its physical
    // `struct<value, metadata>` ROW, whose fixed child ids 0/1 then broke reading the schema back.
    auto to_json = [](const std::shared_ptr<arrow::Field>& field) {
        auto data_type = DataType::Create(field->type(), field->nullable(), field->metadata());
        rapidjson::Document doc;
        auto value = data_type->ToJson(&doc.GetAllocator());
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        value.Accept(writer);
        return std::string(buffer.GetString());
    };

    auto variant_field = VariantTypeUtils::ToArrowField("element");
    auto array_field = arrow::field("arr", arrow::list(variant_field));
    ASSERT_EQ(to_json(array_field), R"({"type":"ARRAY","element":"VARIANT"})");

    auto map_field =
        arrow::field("m", arrow::map(arrow::utf8(), VariantTypeUtils::ToArrowField("value")));
    ASSERT_EQ(to_json(map_field), R"({"type":"MAP","key":"STRING NOT NULL","value":"VARIANT"})");

    // BLOB is marked the same way and was degraded to its physical BYTES the same way.
    auto blob_array = arrow::field("b", arrow::list(BlobUtils::ToArrowField("element", true)));
    ASSERT_EQ(to_json(blob_array), R"({"type":"ARRAY","element":"BLOB"})");

    // Child metadata only ever selects an extension type, so ordinary elements are unaffected by
    // it being carried over.
    auto plain_child = arrow::field("element", arrow::int32(), /*nullable=*/true,
                                    arrow::KeyValueMetadata::Make({DataField::FIELD_ID}, {"7"}));
    ASSERT_EQ(to_json(arrow::field("a", arrow::list(plain_child))),
              R"({"type":"ARRAY","element":"INT"})");
}

}  // namespace paimon::test
