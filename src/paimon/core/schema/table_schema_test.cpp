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

#include "paimon/core/schema/table_schema.h"

#include <utility>

#include "arrow/api.h"
#include "arrow/util/checked_cast.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class TableSchemaTest : public ::testing::Test {
 public:
    arrow::FieldVector MakeArrowField(int32_t id) const {
        arrow::FieldVector arrow_fields;
        arrow_fields.reserve(5);
        auto meta =
            arrow::KeyValueMetadata::Make({std::string(DataField::FIELD_ID)}, {std::to_string(id)});
        arrow_fields.push_back(std::make_shared<arrow::Field>("sub1", arrow::date32(),
                                                              /*nullable=*/true, meta));
        meta = arrow::KeyValueMetadata::Make({std::string(DataField::FIELD_ID)},
                                             {std::to_string(id + 1)});
        arrow_fields.push_back(
            std::make_shared<arrow::Field>("sub2", arrow::timestamp(arrow::TimeUnit::type::NANO),
                                           /*nullable=*/true, meta));
        meta = arrow::KeyValueMetadata::Make({std::string(DataField::FIELD_ID)},
                                             {std::to_string(id + 2)});
        arrow_fields.push_back(std::make_shared<arrow::Field>("sub3", arrow::decimal128(23, 5),
                                                              /*nullable=*/true, meta));
        meta = arrow::KeyValueMetadata::Make({std::string(DataField::FIELD_ID)},
                                             {std::to_string(id + 3)});
        arrow_fields.push_back(std::make_shared<arrow::Field>("sub4", arrow::binary(),
                                                              /*nullable=*/true, meta));
        meta = arrow::KeyValueMetadata::Make({std::string(DataField::FIELD_ID)},
                                             {std::to_string(id + 4)});
        arrow_fields.push_back(std::make_shared<arrow::Field>("sub5", arrow::binary(),
                                                              /*nullable=*/true, meta));
        return arrow_fields;
    }

    std::string ReplaceAll(const std::string& str) {
        std::string replaced_str = StringUtils::Replace(str, " ", "");
        replaced_str = StringUtils::Replace(replaced_str, "\t", "");
        replaced_str = StringUtils::Replace(replaced_str, "\n", "");
        return replaced_str;
    }
};

TEST_F(TableSchemaTest, TestCreateWithAllFieldsNotHaveFieldId) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),  arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),     arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),    arrow::field("f5", arrow::int32()),
        arrow::field("f6", arrow::int32()),    arrow::field("f7", arrow::int64()),
        arrow::field("f8", arrow::int64()),    arrow::field("f9", arrow::float32()),
        arrow::field("f10", arrow::float64()), arrow::field("f11", arrow::utf8()),
        arrow::field("f12", arrow::binary()),  arrow::field("non-partition-field", arrow::int32())};
    auto schema = arrow::schema(fields);
    std::vector<std::string> partition_keys = {"f1", "f2"};
    std::vector<std::string> primary_keys = {"f3", "f4"};
    std::map<std::string, std::string> options;
    ASSERT_OK_AND_ASSIGN(
        auto table_schema,
        TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
    ASSERT_TRUE(table_schema);

    std::vector<DataField> expected_data_fields = {
        DataField(0, arrow::field("f0", arrow::boolean())),
        DataField(1, arrow::field("f1", arrow::int8())),
        DataField(2, arrow::field("f2", arrow::int8())),
        DataField(3, arrow::field("f3", arrow::int16(), /*nullable=*/false)),
        DataField(4, arrow::field("f4", arrow::int16(), /*nullable=*/false)),
        DataField(5, arrow::field("f5", arrow::int32())),
        DataField(6, arrow::field("f6", arrow::int32())),
        DataField(7, arrow::field("f7", arrow::int64())),
        DataField(8, arrow::field("f8", arrow::int64())),
        DataField(9, arrow::field("f9", arrow::float32())),
        DataField(10, arrow::field("f10", arrow::float64())),
        DataField(11, arrow::field("f11", arrow::utf8())),
        DataField(12, arrow::field("f12", arrow::binary())),
        DataField(13, arrow::field("non-partition-field", arrow::int32()))};

    ASSERT_EQ(table_schema->Fields(), expected_data_fields);
    ASSERT_EQ(table_schema->Id(), 0);
    ASSERT_EQ(table_schema->HighestFieldId(), 13);
    ASSERT_EQ(table_schema->PrimaryKeys(), primary_keys);
    ASSERT_EQ(table_schema->PartitionKeys(), partition_keys);
    ASSERT_EQ(table_schema->Options(), options);
}

TEST_F(TableSchemaTest, TestCreateWithAllFieldsHaveFieldId) {
    arrow::FieldVector fields = MakeArrowField(0);
    auto schema = arrow::schema(fields);
    std::vector<std::string> partition_keys = {"sub1", "sub2"};
    std::vector<std::string> primary_keys = {"sub3", "sub4"};
    std::map<std::string, std::string> options;
    ASSERT_OK_AND_ASSIGN(
        auto table_schema,
        TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
    ASSERT_TRUE(table_schema);

    std::vector<DataField> expected_data_fields = {
        DataField(0, arrow::field("sub1", arrow::date32())),
        DataField(1, arrow::field("sub2", arrow::timestamp(arrow::TimeUnit::type::NANO))),
        DataField(2, arrow::field("sub3", arrow::decimal128(23, 5), /*nullable=*/false)),
        DataField(3, arrow::field("sub4", arrow::binary(), /*nullable=*/false)),
        DataField(4, arrow::field("sub5", arrow::binary()))};
    ASSERT_EQ(table_schema->Fields(), expected_data_fields);
    ASSERT_EQ(table_schema->Id(), 0);
    ASSERT_EQ(table_schema->HighestFieldId(), 4);
    ASSERT_EQ(table_schema->PrimaryKeys(), primary_keys);
    ASSERT_EQ(table_schema->PartitionKeys(), partition_keys);
    ASSERT_EQ(table_schema->Options(), options);
}

TEST_F(TableSchemaTest, TestInvalidCreate) {
    // partial fields have field id
    arrow::FieldVector fields = MakeArrowField(3);
    fields.push_back(arrow::field("f0", arrow::boolean()));
    auto schema = arrow::schema(fields);
    std::vector<std::string> partition_keys = {"sub1", "sub2"};
    std::vector<std::string> primary_keys = {"sub3", "sub4"};
    std::map<std::string, std::string> options;
    ASSERT_NOK(TableSchema::Create(/*schema_id=*/1, schema, partition_keys, primary_keys, options));
}

TEST_F(TableSchemaTest, TestDeserializeAppendTableSchema) {
    auto fs = std::make_shared<LocalFileSystem>();

    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/schema/schema-0";
    std::string content;
    ASSERT_OK(fs->ReadFile(path, &content));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> result_schema,
                         TableSchema::CreateFromJson(content));
    TableSchema expected_schema;
    expected_schema.version_ = 3;
    expected_schema.id_ = 0;

    auto field1 = DataField(0, arrow::field("f0", arrow::utf8()));
    auto field2 = DataField(1, arrow::field("f1", arrow::int32()));
    auto field3 = DataField(2, arrow::field("f2", arrow::int32()));
    auto field4 = DataField(3, arrow::field("f3", arrow::float64()));
    expected_schema.fields_ = {field1, field2, field3, field4};

    expected_schema.highest_field_id_ = 3;
    expected_schema.partition_keys_ = {"f1"};
    expected_schema.primary_keys_ = {};
    expected_schema.options_ = {
        {"bucket", "2"}, {"bucket-key", "f2"}, {"manifest.format", "orc"}, {"file.format", "orc"}};
    expected_schema.time_millis_ = 1721614341162;

    ASSERT_EQ(*result_schema, expected_schema);

    ASSERT_EQ(std::vector<std::string>({"f0", "f1", "f2", "f3"}), result_schema->FieldNames());
    ASSERT_EQ(field2, result_schema->GetField("f1").value());
    ASSERT_EQ(field2, result_schema->GetField(1).value());
    ASSERT_NOK_WITH_MSG(result_schema->GetField("non-exist"),
                        "Get field non-exist failed: not exist in table schema");
    ASSERT_NOK_WITH_MSG(result_schema->GetField(100),
                        "Get field with id 100 failed: not exist in table schema");
}

TEST_F(TableSchemaTest, TestDeserializePkTableSchema) {
    auto fs = std::make_shared<LocalFileSystem>();

    std::string path = paimon::test::GetDataDir() + "/orc/pk_09.db/pk_09/schema/schema-0";
    std::string content;
    ASSERT_OK(fs->ReadFile(path, &content));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> result_schema,
                         TableSchema::CreateFromJson(content));
    TableSchema expected_schema;
    expected_schema.version_ = 3;
    expected_schema.id_ = 0;

    auto field1 = DataField(0, arrow::field("f0", arrow::utf8(), /*nullable=*/false));
    auto field2 = DataField(1, arrow::field("f1", arrow::int32(), /*nullable=*/false));
    auto field3 = DataField(2, arrow::field("f2", arrow::int32(), /*nullable=*/false));
    auto field4 = DataField(3, arrow::field("f3", arrow::float64(), /*nullable=*/true));
    expected_schema.fields_ = {field1, field2, field3, field4};

    expected_schema.highest_field_id_ = 3;
    expected_schema.partition_keys_ = {"f1"};
    expected_schema.primary_keys_ = {"f0", "f1", "f2"};
    expected_schema.options_ = {{"bucket", "2"},
                                {"bucket-key", "f2"},
                                {"manifest.format", "orc"},
                                {"file.format", "orc"},
                                {"deletion-vectors.enabled", "true"},
                                {"commit.force-compact", "true"}};
    expected_schema.time_millis_ = 1725534144802;

    ASSERT_EQ(*result_schema, expected_schema);
    ASSERT_EQ(std::vector<std::string>({"f0", "f1", "f2", "f3"}), result_schema->FieldNames());
    ASSERT_OK_AND_ASSIGN(auto trimmed_primary_keys, result_schema->TrimmedPrimaryKeys());
    ASSERT_EQ(trimmed_primary_keys, std::vector<std::string>({"f0", "f2"}));
    ASSERT_OK_AND_ASSIGN(std::vector<DataField> trimmed_primary_key_fields,
                         result_schema->GetFields(trimmed_primary_keys));
    ASSERT_EQ(2, trimmed_primary_key_fields.size());
    ASSERT_EQ(trimmed_primary_key_fields[0], field1);
    ASSERT_EQ(trimmed_primary_key_fields[1], field3);
}

TEST_F(TableSchemaTest, TestDeserializeAppendTableSchemaWithTimestamp) {
    auto fs = std::make_shared<LocalFileSystem>();

    std::string path = paimon::test::GetDataDir() +
                       "/orc/append_with_multiple_ts_precision_and_timezone.db/"
                       "append_with_multiple_ts_precision_and_timezone/schema/schema-0";
    std::string content;
    ASSERT_OK(fs->ReadFile(path, &content));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> result_schema,
                         TableSchema::CreateFromJson(content));
    TableSchema expected_schema;
    expected_schema.version_ = 3;
    expected_schema.id_ = 0;

    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    std::vector<DataField> fields = {
        DataField(0, arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND))),
        DataField(1, arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI))),
        DataField(2, arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO))),
        DataField(3, arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO))),
        DataField(4,
                  arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, timezone))),
        DataField(5,
                  arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, timezone))),
        DataField(6,
                  arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, timezone))),
        DataField(7,
                  arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, timezone)))};
    expected_schema.fields_ = fields;

    expected_schema.highest_field_id_ = 7;
    expected_schema.partition_keys_ = {};
    expected_schema.primary_keys_ = {};
    expected_schema.options_ = {{"bucket", "-1"},
                                {"manifest.format", "orc"},
                                {"file.format", "orc"},
                                {"orc.timestamp-ltz.legacy.type", "false"}};
    expected_schema.time_millis_ = 1757927622210;

    ASSERT_EQ(*result_schema, expected_schema);

    ASSERT_EQ(std::vector<std::string>({"ts_sec", "ts_milli", "ts_micro", "ts_nano", "ts_tz_sec",
                                        "ts_tz_milli", "ts_tz_micro", "ts_tz_nano"}),
              result_schema->FieldNames());
}

TEST_F(TableSchemaTest, TestDeserializeWithNestedDataType) {
    auto fs = std::make_shared<LocalFileSystem>();
    std::string path = paimon::test::GetDataDir() +
                       "/orc/append_table_with_nested_type.db/append_table_with_nested_type/schema/"
                       "schema-0";
    std::string content;
    ASSERT_OK(fs->ReadFile(path, &content));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> result_schema,
                         TableSchema::CreateFromJson(content));
    TableSchema expected_schema;
    expected_schema.version_ = 3;
    expected_schema.id_ = 0;

    auto field1 = DataField(0, arrow::field("f0", arrow::struct_(MakeArrowField(1))));
    auto field2 = DataField(6, arrow::field("f1", arrow::list(arrow::struct_(MakeArrowField(7)))));
    auto field3 = DataField(12, arrow::field("f2", arrow::map(arrow::struct_(MakeArrowField(13)),
                                                              arrow::struct_(MakeArrowField(18)))));
    expected_schema.fields_ = {field1, field2, field3};

    expected_schema.highest_field_id_ = 22;
    expected_schema.partition_keys_ = {};
    expected_schema.primary_keys_ = {};
    expected_schema.options_ = {{"manifest.format", "orc"}, {"file.format", "orc"}};
    expected_schema.time_millis_ = 1729759141146;

    ASSERT_EQ(*result_schema, expected_schema);
    ASSERT_EQ(std::vector<std::string>({"f0", "f1", "f2"}), result_schema->FieldNames());
}

TEST_F(TableSchemaTest, TestEmptyBucketKey) {
    // empty bucket key, will use trimmed pk as bucket key
    {
        std::string table_schema_str = R"({
        "version" : 3,
        "id" : 0,
        "fields" : [ {
                "id" : 0,
                "name" : "f0",
                "type" : "STRING"
        }, {
                "id" : 1,
                "name" : "f1",
                "type" : "INT"
        }, {
                "id" : 2,
                "name" : "f2",
                "type" : "INT"
        }, {
                "id" : 3,
                "name" : "f3",
                "type" : "DOUBLE"
        } ],
        "highestFieldId" : 3,
        "partitionKeys" : [ "f1"],
        "primaryKeys" : [ "f1", "f2" ],
        "options" : {
                "bucket" : "2",
                "bucket-key" : "",
                "manifest.format" : "orc",
                "file.format" : "orc"
        },
        "timeMillis" : 1721614341162
    })";
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> schema_result,
                             TableSchema::CreateFromJson(table_schema_str));
        ASSERT_EQ(std::vector<std::string>({"f2"}), schema_result->bucket_keys_);
    }
    {
        std::string table_schema_str = R"({
        "version" : 3,
        "id" : 0,
        "fields" : [ {
                "id" : 0,
                "name" : "f0",
                "type" : "STRING"
        }, {
                "id" : 1,
                "name" : "f1",
                "type" : "INT"
        }, {
                "id" : 2,
                "name" : "f2",
                "type" : "INT"
        }, {
                "id" : 3,
                "name" : "f3",
                "type" : "DOUBLE"
        } ],
        "highestFieldId" : 3,
        "partitionKeys" : [ "f1"],
        "primaryKeys" : [ "f1", "f2" ],
        "options" : {
                "bucket" : "2",
                "manifest.format" : "orc",
                "file.format" : "orc"
        },
        "timeMillis" : 1721614341162
    })";
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> schema_result,
                             TableSchema::CreateFromJson(table_schema_str));
        ASSERT_EQ(std::vector<std::string>({"f2"}), schema_result->bucket_keys_);
    }
}

TEST_F(TableSchemaTest, TestToJson) {
    auto field1 = DataField(0, arrow::field("f0", arrow::utf8()));
    auto field2 = DataField(1, arrow::field("f1", arrow::int32()));
    auto field3 = DataField(2, arrow::field("f2", arrow::int32()));
    auto field4 = DataField(3, arrow::field("f3", arrow::float64()));
    auto data_fields = {field1, field2, field3, field4};
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(data_fields);
    ASSERT_OK_AND_ASSIGN(
        auto schema, TableSchema::Create(/*schema_id=*/0, arrow_schema, /*partition_keys=*/{"f1"},
                                         /*primary_keys=*/{},
                                         {{"manifest.format", "orc"}, {"file.format", "orc"}}));
    schema->time_millis_ = 1721614341162;
    ASSERT_OK_AND_ASSIGN(std::string json_str, schema->ToJsonString());
    std::string table_schema_str = R"({
    "version": 3,
    "id": 0,
    "fields": [
        {
            "id": 0,
            "name": "f0",
            "type": "STRING"
        },
        {
            "id": 1,
            "name": "f1",
            "type": "INT"
        },
        {
            "id": 2,
            "name": "f2",
            "type": "INT"
        },
        {
            "id": 3,
            "name": "f3",
            "type": "DOUBLE"
        }
    ],
    "highestFieldId": 3,
    "partitionKeys": [
        "f1"
    ],
    "primaryKeys": [],
    "options": {
        "file.format": "orc",
        "manifest.format": "orc"
    },
    "timeMillis": 1721614341162
})";
    ASSERT_EQ(ReplaceAll(table_schema_str), ReplaceAll(json_str));
}

TEST_F(TableSchemaTest, TestToJson2) {
    std::string table_schema_str = R"({
        "version" : 3,
        "id" : 0,
        "fields" : [ {
                "id" : 0,
                "name" : "f0",
                "type" : "STRING"
        }, {
                "id" : 1,
                "name" : "f1",
                "type" : "INT"
        }, {
                "id" : 2,
                "name" : "f2",
                "type" : "INT"
        }, {
                "id" : 3,
                "name" : "f3",
                "type" : "DOUBLE"
        } ],
        "highestFieldId" : 3,
        "partitionKeys" : [ "f1"],
        "primaryKeys" : [ "f1", "f2" ],
        "options" : {
                "bucket" : "2",
                "file.format" : "orc",
                "manifest.format" : "orc"
        },
        "timeMillis" : 1721614341162
    })";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> schema_result,
                         TableSchema::CreateFromJson(table_schema_str));
    ASSERT_OK_AND_ASSIGN(std::string json_str, schema_result->ToJsonString());
    ASSERT_EQ(ReplaceAll(table_schema_str), ReplaceAll(json_str));
}

TEST_F(TableSchemaTest, TestToJson3) {
    auto fs = std::make_shared<LocalFileSystem>();
    std::string path =
        paimon::test::GetDataDir() +
        "/orc/append_complex_build_in_fieldid.db/append_complex_build_in_fieldid/schema/"
        "schema-0";
    std::string content;
    ASSERT_OK(fs->ReadFile(path, &content));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> st, TableSchema::CreateFromJson(content));
    ASSERT_OK_AND_ASSIGN(std::string json_str, st->ToJsonString());
    std::string expected_str = R"==({
    "version": 3,
    "id": 0,
    "fields": [
        {
            "id": 0,
            "name": "f1",
            "type": {
                "type": "MAP",
                "key": "TINYINT NOT NULL",
                "value": "SMALLINT"
            }
        },
        {
            "id": 1,
            "name": "f2",
            "type": {
                "type": "ARRAY",
                "element": "FLOAT"
            }
        },
        {
            "id": 2,
            "name": "f3",
            "type": {
                "type": "ROW",
                "fields": [
                    {
                        "id": 3,
                        "name": "f0",
                        "type": "BOOLEAN"
                    },
                    {
                        "id": 4,
                        "name": "f1",
                        "type": "BIGINT"
                    }
                ]
            }
        },
        {
            "id": 5,
            "name": "f4",
            "type": "TIMESTAMP(9)"
        },
        {
            "id": 6,
            "name": "f5",
            "type": "DATE"
        },
        {
            "id": 7,
            "name": "f6",
            "type": "DECIMAL(2, 2)"
        }
    ],
    "highestFieldId": 7,
    "partitionKeys": [],
    "primaryKeys": [],
    "options": {
        "file.format": "orc",
        "manifest.format": "orc"
    },
    "timeMillis": 1732079677062
})==";
    ASSERT_EQ(ReplaceAll(expected_str), ReplaceAll(json_str));
}

TEST_F(TableSchemaTest, TestToJsonWithNestedField0) {
    auto map_type = arrow::map(arrow::int8(), arrow::int16());
    auto list_type = arrow::list(DataField::ConvertDataFieldToArrowField(
        DataField(536871936, arrow::field("item", arrow::float32()))));
    std::vector<DataField> struct_fields = {DataField(3, arrow::field("f0", arrow::boolean())),
                                            DataField(4, arrow::field("f1", arrow::int64()))};
    auto struct_type = DataField::ConvertDataFieldsToArrowStructType(struct_fields);
    auto data_fields = {DataField(0, arrow::field("f1", map_type)),
                        DataField(1, arrow::field("f2", list_type)),
                        DataField(2, arrow::field("f3", struct_type)),
                        DataField(5, arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO))),
                        DataField(6, arrow::field("f5", arrow::date32())),
                        DataField(7, arrow::field("f6", arrow::decimal128(2, 2)))};

    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(data_fields);
    ASSERT_OK_AND_ASSIGN(auto schema,
                         TableSchema::Create(/*schema_id=*/0, arrow_schema, /*partition_keys=*/{},
                                             /*primary_keys=*/{},
                                             {{"manifest.format", "orc"}, {"file.format", "orc"}}));

    schema->time_millis_ = 1721614341162;
    ASSERT_OK_AND_ASSIGN(std::string json_str, schema->ToJsonString());
    std::string expected_schema = R"==({
    "version": 3,
    "id": 0,
    "fields": [
        {
            "id": 0,
            "name": "f1",
            "type": {
                "type": "MAP",
                "key": "TINYINT NOT NULL",
                "value": "SMALLINT"
            }
        },
        {
            "id": 1,
            "name": "f2",
            "type": {
                "type": "ARRAY",
                "element": "FLOAT"
            }
        },
        {
            "id": 2,
            "name": "f3",
            "type": {
                "type": "ROW",
                "fields": [
                    {
                        "id": 3,
                        "name": "f0",
                        "type": "BOOLEAN"
                    },
                    {
                        "id": 4,
                        "name": "f1",
                        "type": "BIGINT"
                    }
                ]
            }
        },
        {
            "id": 5,
            "name": "f4",
            "type": "TIMESTAMP(9)"
        },
        {
            "id": 6,
            "name": "f5",
            "type": "DATE"
        },
        {
            "id": 7,
            "name": "f6",
            "type": "DECIMAL(2, 2)"
        }
    ],
    "highestFieldId": 7,
    "partitionKeys": [],
    "primaryKeys": [],
    "options": {
        "file.format": "orc",
        "manifest.format": "orc"
    },
    "timeMillis": 1721614341162
})==";
    ASSERT_EQ(ReplaceAll(expected_schema), ReplaceAll(json_str));
}

TEST_F(TableSchemaTest, TestToJsonWithNestedField1) {
    auto fs = std::make_shared<LocalFileSystem>();
    std::string path = paimon::test::GetDataDir() +
                       "/orc/append_table_with_nested_type.db/append_table_with_nested_type/schema/"
                       "schema-0";
    std::string content;
    ASSERT_OK(fs->ReadFile(path, &content));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> st, TableSchema::CreateFromJson(content));
    ASSERT_OK_AND_ASSIGN(std::string json_str, st->ToJsonString());
    std::string expected_str = R"==({
    "version": 3,
    "id": 0,
    "fields": [
        {
            "id": 0,
            "name": "f0",
            "type": {
                "type": "ROW",
                "fields": [
                    {
                        "id": 1,
                        "name": "sub1",
                        "type": "DATE"
                    },
                    {
                        "id": 2,
                        "name": "sub2",
                        "type": "TIMESTAMP(9)"
                    },
                    {
                        "id": 3,
                        "name": "sub3",
                        "type": "DECIMAL(23, 5)"
                    },
                    {
                        "id": 4,
                        "name": "sub4",
                        "type": "BYTES"
                    },
                    {
                        "id": 5,
                        "name": "sub5",
                        "type": "BYTES"
                    }
                ]
            }
        },
        {
            "id": 6,
            "name": "f1",
            "type": {
                "type": "ARRAY",
                "element": {
                    "type": "ROW",
                    "fields": [
                        {
                            "id": 7,
                            "name": "sub1",
                            "type": "DATE"
                        },
                        {
                            "id": 8,
                            "name": "sub2",
                            "type": "TIMESTAMP(9)"
                        },
                        {
                            "id": 9,
                            "name": "sub3",
                            "type": "DECIMAL(23, 5)"
                        },
                        {
                            "id": 10,
                            "name": "sub4",
                            "type": "BYTES"
                        },
                        {
                            "id": 11,
                            "name": "sub5",
                            "type": "BYTES"
                        }
                    ]
                }
            }
        },
        {
            "id": 12,
            "name": "f2",
            "type": {
                "type": "MAP",
                "key": {
                    "type": "ROW NOT NULL",
                    "fields": [
                        {
                            "id": 13,
                            "name": "sub1",
                            "type": "DATE"
                        },
                        {
                            "id": 14,
                            "name": "sub2",
                            "type": "TIMESTAMP(9)"
                        },
                        {
                            "id": 15,
                            "name": "sub3",
                            "type": "DECIMAL(23, 5)"
                        },
                        {
                            "id": 16,
                            "name": "sub4",
                            "type": "BYTES"
                        },
                        {
                            "id": 17,
                            "name": "sub5",
                            "type": "BYTES"
                        }
                    ]
                },
                "value": {
                    "type": "ROW",
                    "fields": [
                        {
                            "id": 18,
                            "name": "sub1",
                            "type": "DATE"
                        },
                        {
                            "id": 19,
                            "name": "sub2",
                            "type": "TIMESTAMP(9)"
                        },
                        {
                            "id": 20,
                            "name": "sub3",
                            "type": "DECIMAL(23, 5)"
                        },
                        {
                            "id": 21,
                            "name": "sub4",
                            "type": "BYTES"
                        },
                        {
                            "id": 22,
                            "name": "sub5",
                            "type": "BYTES"
                        }
                    ]
                }
            }
        }
    ],
    "highestFieldId": 22,
    "partitionKeys": [],
    "primaryKeys": [],
    "options": {
        "file.format": "orc",
        "manifest.format": "orc"
    },
    "timeMillis": 1729759141146
})==";

    ASSERT_EQ(ReplaceAll(expected_str), ReplaceAll(json_str));
    // test create from arrow schema, test set field id
    auto f0 = arrow::field(
        "f0", arrow::struct_({arrow::field("sub1", arrow::date32()),
                              arrow::field("sub2", arrow::timestamp(arrow::TimeUnit::NANO)),
                              arrow::field("sub3", arrow::decimal128(23, 5)),
                              arrow::field("sub4", arrow::binary()),
                              arrow::field("sub5", arrow::binary())}));
    auto f1 = arrow::field("f1", arrow::list(f0));
    auto f2 = arrow::field("f2", arrow::map(f0->type(), f0->type()));
    arrow::FieldVector fields = {f0, f1, f2};
    auto arrow_schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(
        auto new_table_schema,
        TableSchema::Create(0, arrow_schema, /*partition_keys=*/{}, /*primary_keys=*/{},
                            /*options=*/{{"file.format", "orc"}, {"manifest.format", "orc"}}));
    new_table_schema->time_millis_ = 1729759141146;
    ASSERT_OK_AND_ASSIGN(std::string new_json_str, new_table_schema->ToJsonString());
    ASSERT_EQ(expected_str, new_json_str) << new_json_str;
}

TEST_F(TableSchemaTest, TestInvalidSchema) {
    {
        // partition key same as primary key
        std::string table_schema_str = R"({
        "version" : 3,
        "id" : 0,
        "fields" : [ {
                "id" : 0,
                "name" : "f0",
                "type" : "STRING"
        }, {
                "id" : 1,
                "name" : "f1",
                "type" : "INT"
        }, {
                "id" : 2,
                "name" : "f2",
                "type" : "INT"
        }, {
                "id" : 3,
                "name" : "f3",
                "type" : "DOUBLE"
        } ],
        "highestFieldId" : 3,
        "partitionKeys" : [ "f1", "f2" ],
        "primaryKeys" : [ "f1", "f2" ],
        "options" : {
                "bucket" : "2",
                "bucket-key" : "f2",
                "manifest.format" : "orc",
                "file.format" : "orc"
        },
        "timeMillis" : 1721614341162
    })";
        ASSERT_NOK_WITH_MSG(TableSchema::CreateFromJson(table_schema_str),
                            "this will result in only one record in a partition");
    }
    {
        // bucket key is non-exist in fields
        std::string table_schema_str = R"({
        "version" : 3,
        "id" : 0,
        "fields" : [ {
                "id" : 0,
                "name" : "f0",
                "type" : "STRING"
        }, {
                "id" : 1,
                "name" : "f1",
                "type" : "INT"
        }, {
                "id" : 2,
                "name" : "f2",
                "type" : "INT"
        }, {
                "id" : 3,
                "name" : "f3",
                "type" : "DOUBLE"
        } ],
        "highestFieldId" : 3,
        "partitionKeys" : [ "f1"],
        "primaryKeys" : [ "f1", "f2" ],
        "options" : {
                "bucket" : "2",
                "bucket-key" : "non-exist",
                "manifest.format" : "orc",
                "file.format" : "orc"
        },
        "timeMillis" : 1721614341162
    })";
        ASSERT_NOK_WITH_MSG(TableSchema::CreateFromJson(table_schema_str),
                            "should contain all bucket keys");
    }
    {
        // bucket key in partition key
        std::string table_schema_str = R"({
        "version" : 3,
        "id" : 0,
        "fields" : [ {
                "id" : 0,
                "name" : "f0",
                "type" : "STRING"
        }, {
                "id" : 1,
                "name" : "f1",
                "type" : "INT"
        }, {
                "id" : 2,
                "name" : "f2",
                "type" : "INT"
        }, {
                "id" : 3,
                "name" : "f3",
                "type" : "DOUBLE"
        } ],
        "highestFieldId" : 3,
        "partitionKeys" : [ "f1"],
        "primaryKeys" : [ "f1", "f2" ],
        "options" : {
                "bucket" : "2",
                "bucket-key" : "f1,f3",
                "manifest.format" : "orc",
                "file.format" : "orc"
        },
        "timeMillis" : 1721614341162
    })";
        ASSERT_NOK_WITH_MSG(TableSchema::CreateFromJson(table_schema_str),
                            "should not in partition keys");
    }
    {
        // bucket key is not a subset of primary key
        std::string table_schema_str = R"({
        "version" : 3,
        "id" : 0,
        "fields" : [ {
                "id" : 0,
                "name" : "f0",
                "type" : "STRING"
        }, {
                "id" : 1,
                "name" : "f1",
                "type" : "INT"
        }, {
                "id" : 2,
                "name" : "f2",
                "type" : "INT"
        }, {
                "id" : 3,
                "name" : "f3",
                "type" : "DOUBLE"
        } ],
        "highestFieldId" : 3,
        "partitionKeys" : [ "f1"],
        "primaryKeys" : [ "f1", "f2" ],
        "options" : {
                "bucket" : "2",
                "bucket-key" : "f3",
                "manifest.format" : "orc",
                "file.format" : "orc"
        },
        "timeMillis" : 1721614341162
    })";
        ASSERT_NOK_WITH_MSG(TableSchema::CreateFromJson(table_schema_str),
                            "should contain all bucket keys");
    }
}

TEST_F(TableSchemaTest, SetFieldIdBasicType) {
    auto field = arrow::field("column1", arrow::int32());
    {
        int32_t field_id = 0;
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::Field> new_field,
            TableSchema::AssignFieldIdsRecursively(field, /*set_field_id=*/true, &field_id));
        ASSERT_TRUE(new_field->metadata());
        ASSERT_EQ(new_field->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "0");
        ASSERT_EQ(field_id, 1);
    }
    {
        int32_t field_id = 0;
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::Field> new_field,
            TableSchema::AssignFieldIdsRecursively(field, /*set_field_id=*/false, &field_id));
        ASSERT_FALSE(new_field->metadata());
        ASSERT_EQ(field_id, 0);
    }
}

TEST_F(TableSchemaTest, SetFieldIdStructType) {
    auto child1 = arrow::field("child1", arrow::int32());
    auto child2 = arrow::field("child2", arrow::float64());
    auto struct_field = arrow::field("parent", arrow::struct_({child1, child2}));
    {
        int32_t field_id = 0;
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::Field> new_field,
            TableSchema::AssignFieldIdsRecursively(struct_field, /*set_field_id=*/true, &field_id));
        auto struct_type = std::static_pointer_cast<arrow::StructType>(new_field->type());
        ASSERT_EQ(struct_type->num_fields(), 2);
        ASSERT_EQ(new_field->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "0");
        ASSERT_EQ(struct_type->field(0)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "1");
        ASSERT_EQ(struct_type->field(1)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "2");
        ASSERT_EQ(field_id, 3);
    }
    {
        int32_t field_id = 0;
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> new_field,
                             TableSchema::AssignFieldIdsRecursively(
                                 struct_field, /*set_field_id=*/false, &field_id));
        auto struct_type = std::static_pointer_cast<arrow::StructType>(new_field->type());
        ASSERT_EQ(struct_type->num_fields(), 2);
        ASSERT_FALSE(new_field->metadata());
        ASSERT_EQ(struct_type->field(0)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "0");
        ASSERT_EQ(struct_type->field(1)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "1");
        ASSERT_EQ(field_id, 2);
    }
}

TEST_F(TableSchemaTest, SetFieldIdListType) {
    auto value_field = arrow::field("values", arrow::int32());
    auto list_field = arrow::field("list_column", arrow::list(value_field));
    {
        int32_t field_id = 0;
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::Field> new_field,
            TableSchema::AssignFieldIdsRecursively(list_field, /*set_field_id=*/true, &field_id));
        ASSERT_EQ(new_field->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "0");
        auto list_type = arrow::internal::checked_pointer_cast<arrow::ListType>(new_field->type());
        ASSERT_FALSE(list_type->value_field()->metadata());
        ASSERT_EQ(field_id, 1);
    }
    {
        int32_t field_id = 0;
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::Field> new_field,
            TableSchema::AssignFieldIdsRecursively(list_field, /*set_field_id=*/false, &field_id));
        ASSERT_FALSE(new_field->metadata());
        auto list_type = arrow::internal::checked_pointer_cast<arrow::ListType>(new_field->type());
        ASSERT_FALSE(list_type->value_field()->metadata());
        ASSERT_EQ(field_id, 0);
    }
}

TEST_F(TableSchemaTest, SetFieldIdMapType) {
    auto map_field = arrow::field("map_column", arrow::map(arrow::utf8(), arrow::int32()));
    {
        int32_t field_id = 0;
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::Field> new_field,
            TableSchema::AssignFieldIdsRecursively(map_field, /*set_field_id=*/true, &field_id));
        ASSERT_EQ(new_field->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "0");
        auto map_type = arrow::internal::checked_pointer_cast<arrow::MapType>(new_field->type());
        std::shared_ptr<arrow::Field> key_field = map_type->key_field();
        std::shared_ptr<arrow::Field> value_field = map_type->item_field();
        ASSERT_FALSE(key_field->metadata());
        ASSERT_FALSE(value_field->metadata());
        ASSERT_EQ(field_id, 1);
    }
    {
        int32_t field_id = 0;
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::Field> new_field,
            TableSchema::AssignFieldIdsRecursively(map_field, /*set_field_id=*/false, &field_id));
        ASSERT_FALSE(new_field->metadata());
        auto map_type = arrow::internal::checked_pointer_cast<arrow::MapType>(new_field->type());
        std::shared_ptr<arrow::Field> key_field = map_type->key_field();
        std::shared_ptr<arrow::Field> value_field = map_type->item_field();
        ASSERT_FALSE(key_field->metadata());
        ASSERT_FALSE(value_field->metadata());
        ASSERT_EQ(field_id, 0);
    }
}

TEST_F(TableSchemaTest, SetFieldIdMapWithStruct) {
    auto inner_child1 = arrow::field("inner1", arrow::int32());
    auto inner_child2 = arrow::field("inner2", arrow::float64());
    auto map_field =
        arrow::field("map_column", arrow::map(arrow::struct_({inner_child1, inner_child2}),
                                              arrow::struct_({inner_child1, inner_child2})));
    {
        int32_t field_id = 0;
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::Field> new_field,
            TableSchema::AssignFieldIdsRecursively(map_field, /*set_field_id=*/true, &field_id));
        ASSERT_EQ(new_field->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "0");
        auto map_type = arrow::internal::checked_pointer_cast<arrow::MapType>(new_field->type());
        std::shared_ptr<arrow::Field> key_field = map_type->key_field();
        std::shared_ptr<arrow::Field> value_field = map_type->item_field();
        ASSERT_FALSE(key_field->metadata());
        auto key_inner_type = std::static_pointer_cast<arrow::StructType>(key_field->type());
        ASSERT_EQ(key_inner_type->field(0)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "1");
        ASSERT_EQ(key_inner_type->field(1)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "2");
        ASSERT_FALSE(value_field->metadata());
        auto value_inner_type = std::static_pointer_cast<arrow::StructType>(value_field->type());
        ASSERT_EQ(value_inner_type->field(0)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(),
                  "3");
        ASSERT_EQ(value_inner_type->field(1)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(),
                  "4");
        ASSERT_EQ(field_id, 5);
    }
    {
        int32_t field_id = 0;
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::Field> new_field,
            TableSchema::AssignFieldIdsRecursively(map_field, /*set_field_id=*/false, &field_id));
        ASSERT_FALSE(new_field->metadata());
        auto map_type = arrow::internal::checked_pointer_cast<arrow::MapType>(new_field->type());
        std::shared_ptr<arrow::Field> key_field = map_type->key_field();
        std::shared_ptr<arrow::Field> value_field = map_type->item_field();
        ASSERT_FALSE(key_field->metadata());
        auto key_inner_type = std::static_pointer_cast<arrow::StructType>(key_field->type());
        ASSERT_EQ(key_inner_type->field(0)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "0");
        ASSERT_EQ(key_inner_type->field(1)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "1");
        ASSERT_FALSE(value_field->metadata());
        auto value_inner_type = std::static_pointer_cast<arrow::StructType>(value_field->type());
        ASSERT_EQ(value_inner_type->field(0)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(),
                  "2");
        ASSERT_EQ(value_inner_type->field(1)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(),
                  "3");
        ASSERT_EQ(field_id, 4);
    }
}

TEST_F(TableSchemaTest, SetFieldIdNestedStruct) {
    auto inner_child1 = arrow::field("inner1", arrow::int32());
    auto inner_child2 = arrow::field("inner2", arrow::float64());
    auto inner_struct = arrow::field("inner_struct", arrow::struct_({inner_child1, inner_child2}));
    auto outer_struct = arrow::field("outer_struct", arrow::struct_({inner_struct}));
    {
        int32_t field_id = 0;
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::Field> new_field,
            TableSchema::AssignFieldIdsRecursively(outer_struct, /*set_field_id=*/true, &field_id));
        ASSERT_EQ(new_field->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "0");
        auto outer_type = std::static_pointer_cast<arrow::StructType>(new_field->type());
        ASSERT_EQ(outer_type->field(0)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "1");
        auto inner_type = std::static_pointer_cast<arrow::StructType>(outer_type->field(0)->type());
        ASSERT_EQ(inner_type->field(0)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "2");
        ASSERT_EQ(inner_type->field(1)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "3");
        ASSERT_EQ(field_id, 4);
    }
    {
        int32_t field_id = 0;
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> new_field,
                             TableSchema::AssignFieldIdsRecursively(
                                 outer_struct, /*set_field_id=*/false, &field_id));
        ASSERT_FALSE(new_field->metadata());
        auto outer_type = std::static_pointer_cast<arrow::StructType>(new_field->type());
        ASSERT_EQ(outer_type->field(0)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "0");
        auto inner_type = std::static_pointer_cast<arrow::StructType>(outer_type->field(0)->type());
        ASSERT_EQ(inner_type->field(0)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "1");
        ASSERT_EQ(inner_type->field(1)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "2");
        ASSERT_EQ(field_id, 3);
    }
}

TEST_F(TableSchemaTest, SetFieldIdNestedListInStruct) {
    auto list_value_field = arrow::field("list_values", arrow::int32());
    auto list_field = arrow::field("list_column", arrow::list(list_value_field));
    auto struct_field = arrow::field("struct_with_list", arrow::struct_({list_field}));
    {
        int32_t field_id = 0;
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::Field> new_field,
            TableSchema::AssignFieldIdsRecursively(struct_field, /*set_field_id=*/true, &field_id));
        auto struct_type = std::static_pointer_cast<arrow::StructType>(new_field->type());
        ASSERT_EQ(new_field->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "0");
        ASSERT_EQ(struct_type->field(0)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "1");
        auto list_type =
            arrow::internal::checked_pointer_cast<arrow::ListType>(struct_type->field(0)->type());
        ASSERT_FALSE(list_type->value_field()->metadata());
        ASSERT_EQ(field_id, 2);
    }
    {
        int32_t field_id = 0;
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> new_field,
                             TableSchema::AssignFieldIdsRecursively(
                                 struct_field, /*set_field_id=*/false, &field_id));
        auto struct_type = std::static_pointer_cast<arrow::StructType>(new_field->type());
        ASSERT_FALSE(new_field->metadata());
        ASSERT_EQ(struct_type->field(0)->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "0");
        auto list_type =
            arrow::internal::checked_pointer_cast<arrow::ListType>(struct_type->field(0)->type());
        ASSERT_FALSE(list_type->value_field()->metadata());
        ASSERT_EQ(field_id, 1);
    }
}

TEST_F(TableSchemaTest, CrossPartitionUpdate) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),    arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),   arrow::field("f5", arrow::int32())};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f1", "f2"};
    std::vector<std::string> partition_keys = {"f2", "f3"};
    std::map<std::string, std::string> options;
    ASSERT_OK_AND_ASSIGN(
        auto table_schema,
        TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
    ASSERT_TRUE(table_schema);
    ASSERT_TRUE(table_schema->CrossPartitionUpdate());
}

TEST_F(TableSchemaTest, CrossPartitionUpdate2) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),    arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),   arrow::field("f5", arrow::int32())};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f1", "f2", "f3"};
    std::vector<std::string> partition_keys = {"f2", "f3"};
    std::map<std::string, std::string> options;
    ASSERT_OK_AND_ASSIGN(
        auto table_schema,
        TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
    ASSERT_TRUE(table_schema);
    ASSERT_FALSE(table_schema->CrossPartitionUpdate());
}

}  // namespace paimon::test
