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

#include "paimon/format/avro/avro_schema_converter.h"

#include "arrow/api.h"
#include "avro/Compiler.hh"
#include "avro/ValidSchema.hh"
#include "gtest/gtest.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/utils/versioned_object_serializer.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::avro::test {

TEST(AvroSchemaConverterTest, TestSimple) {
    // Test a basic record with primitive types
    std::string schema_json = R"({
        "type": "record",
        "namespace": "org.apache.paimon.avro.generated",
        "name": "record",
        "fields": [
            {"name": "f_bool", "type": "boolean"},
            {"name": "f_int", "type": "int"},
            {"name": "f_long", "type": "long"},
            {"name": "f_float", "type": "float"},
            {"name": "f_double", "type": "double"},
            {"name": "f_string", "type": "string"},
            {"name": "f_bytes", "type": "bytes"}
        ]
    })";

    auto avro_schema = ::avro::compileJsonSchemaFromString(schema_json);

    ASSERT_OK_AND_ASSIGN(auto arrow_type,
                         AvroSchemaConverter::AvroSchemaToArrowDataType(avro_schema));

    // Expected Arrow Schema
    auto expected_fields = {
        arrow::field("f_bool", arrow::boolean(), false),
        arrow::field("f_int", arrow::int32(), false),
        arrow::field("f_long", arrow::int64(), false),
        arrow::field("f_float", arrow::float32(), false),
        arrow::field("f_double", arrow::float64(), false),
        arrow::field("f_string", arrow::utf8(), false),
        arrow::field("f_bytes", arrow::binary(), false),
    };
    auto arrow_schema = arrow::schema(expected_fields);

    // The converted type should be a StructType
    ASSERT_EQ(arrow_type->id(), arrow::Type::STRUCT);
    ASSERT_TRUE(arrow_type->Equals(arrow::struct_(arrow_schema->fields())));

    ASSERT_OK_AND_ASSIGN(auto expected_avro_schema,
                         AvroSchemaConverter::ArrowSchemaToAvroSchema(arrow_schema));
    ASSERT_EQ(expected_avro_schema.toJson(), avro_schema.toJson());
}

TEST(AvroSchemaConverterTest, TestAvroSchemaToArrowDataTypeWithNullableAndComplexType) {
    // Test a schema with nullable types and nested types (array, record)
    std::string schema_json = R"({
    "type": "record",
    "namespace": "org.apache.paimon.avro.generated",
    "name": "record",
    "fields": [
        {
            "name": "_VERSION",
            "type": "int"
        },
        {
            "name": "_FILE_NAME",
            "type": "string"
        },
        {
            "name": "_FILE_SIZE",
            "type": "long"
        },
        {
            "name": "_NUM_ADDED_FILES",
            "type": "long"
        },
        {
            "name": "_NUM_DELETED_FILES",
            "type": "long"
        },
        {
            "name": "_PARTITION_STATS",
            "type": {
                "type": "record",
                "namespace": "org.apache.paimon.avro.generated",
                "name": "record__PARTITION_STATS",
                "fields": [
                    {
                        "name": "_MIN_VALUES",
                        "type": "bytes"
                    },
                    {
                        "name": "_MAX_VALUES",
                        "type": "bytes"
                    },
                    {
                        "name": "_NULL_COUNTS",
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items": [
                                    "null",
                                    "long"
                                ]
                            }
                        ],
                        "default": null
                    }
                ]
            }
        },
        {
            "name": "_SCHEMA_ID",
            "type": "long"
        },
        {
            "name": "_MIN_BUCKET",
            "type": [
                "null",
                "int"
            ],
            "default": null
        },
        {
            "name": "_MAX_BUCKET",
            "type": [
                "null",
                "int"
            ],
            "default": null
        },
        {
            "name": "_MIN_LEVEL",
            "type": [
                "null",
                "int"
            ],
            "default": null
        },
        {
            "name": "_MAX_LEVEL",
            "type": [
                "null",
                "int"
            ],
            "default": null
        },
        {
            "name": "_MIN_ROW_ID",
            "type": [
                "null",
                "long"
            ],
            "default": null
        },
        {
            "name": "_MAX_ROW_ID",
            "type": [
                "null",
                "long"
            ],
            "default": null
        }
    ]
})";

    auto avro_schema = ::avro::compileJsonSchemaFromString(schema_json);
    ASSERT_OK_AND_ASSIGN(auto arrow_type,
                         AvroSchemaConverter::AvroSchemaToArrowDataType(avro_schema));

    ASSERT_EQ(arrow_type->id(), arrow::Type::STRUCT);
    ASSERT_TRUE(arrow_type->Equals(
        VersionedObjectSerializer<ManifestFileMeta>::VersionType(ManifestFileMeta::DataType())));
}

TEST(AvroSchemaConverterTest, TestAvroSchemaToArrowDataTypeWithTimestampType) {
    std::string schema_json = R"({
    "type": "record",
    "namespace": "org.apache.paimon.avro.generated",
    "name": "record",
    "fields": [
        {
            "name": "ts_milli",
            "type": { "type": "long", "logicalType": "timestamp-millis"}
        },
        {
            "name": "ts_micro",
            "type": { "type": "long", "logicalType": "timestamp-micros"}
        },
        {
            "name": "ts_nano",
            "type": { "type": "long", "logicalType": "timestamp-nanos"}
        },
        {
            "name": "ts_milli_tz",
            "type": { "type": "long", "logicalType": "local-timestamp-millis"}
        },
        {
            "name": "ts_micro_tz",
            "type": { "type": "long", "logicalType": "local-timestamp-micros"}
        },
        {
            "name": "ts_nano_tz",
            "type": { "type": "long", "logicalType": "local-timestamp-nanos"}
        }
    ]
})";

    auto avro_schema = ::avro::compileJsonSchemaFromString(schema_json);
    ASSERT_OK_AND_ASSIGN(auto arrow_type,
                         AvroSchemaConverter::AvroSchemaToArrowDataType(avro_schema));

    ASSERT_EQ(arrow_type->id(), arrow::Type::STRUCT);
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    auto expected_fields = {
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI), false),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO), false),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO), false),
        arrow::field("ts_milli_tz", arrow::timestamp(arrow::TimeUnit::MILLI, timezone), false),
        arrow::field("ts_micro_tz", arrow::timestamp(arrow::TimeUnit::MICRO, timezone), false),
        arrow::field("ts_nano_tz", arrow::timestamp(arrow::TimeUnit::NANO, timezone), false),
    };
    ASSERT_TRUE(arrow_type->Equals(arrow::struct_(expected_fields))) << arrow_type->ToString();
}

}  // namespace paimon::avro::test
